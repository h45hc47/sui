// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generic Roaring-bitmap inverted-index handler.
//!
//! Parallel to [`crate::handlers::handler`]: defines a [`BitmapIndexProcessor`]
//! trait and a generic [`BitmapIndexHandler`] wrapper that owns the shared
//! batching (single-row RoaringBitmap per batch) and read-modify-write CAS
//! commit loop. Concrete per-index logic lives in sibling modules (see
//! [`super::transaction_processor`] and [`super::event_processor`]).

use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use roaring::RoaringBitmap;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::concurrent::BatchStatus;
use sui_indexer_alt_framework::pipeline::concurrent::Handler;
use sui_indexer_alt_framework_store_traits::Store;
use sui_types::full_checkpoint_content::Checkpoint;

use crate::bigtable::client::BigTableClient;
use crate::bigtable::proto::bigtable::v2::Mutation;
use crate::bigtable::proto::bigtable::v2::RowFilter;
use crate::bigtable::proto::bigtable::v2::ValueRange;
use crate::bigtable::proto::bigtable::v2::mutation::SetCell;
use crate::bigtable::proto::bigtable::v2::row_filter;
use crate::bigtable::proto::bigtable::v2::row_filter::Filter;
use crate::bigtable::proto::bigtable::v2::value_range::EndValue;
use crate::bigtable::proto::bigtable::v2::value_range::StartValue;
use crate::bigtable::store::BigTableStore;
use crate::config::ConcurrentLayer;
use crate::rate_limiter::CompositeRateLimiter;
use crate::tables;

/// CAS retries are handled here rather than relying on the framework's commit retry
/// (100ms–1s exponential backoff) because CAS contention between 3 writers resolves in
/// microseconds. An in-handler retry also targets only the contested row key rather than
/// re-reading and re-writing the entire batch.
const MAX_CAS_RETRIES: usize = 10;

/// Predicate mode for the CAS write loop.
enum CasPredicate {
    /// Row does not exist yet. Uses PassAllFilter: matched=true means the row was
    /// created by another writer, matched=false means it's still empty and our
    /// write (in false_mutations) was applied.
    RowEmpty,
    /// Row exists with a known value. Uses ValueRangeFilter on the exact bytes:
    /// matched=true means the value is unchanged and our write (in true_mutations)
    /// was applied, matched=false means another writer updated it.
    ValueEquals(Bytes),
}

/// Output of `process()`: identifies a single bit to set in the bitmap index.
pub struct BitmapIndexValue {
    pub row_key: Bytes,
    pub bit_position: u32,
}

/// Batch that accumulates a RoaringBitmap for a single row key.
#[derive(Default)]
pub struct BitmapIndexBatch {
    inner: Option<(Bytes, Arc<RoaringBitmap>)>,
}

/// Extension of `Processor` that targets a Roaring-bitmap inverted index table.
///
/// Implementors extract per-checkpoint `(row_key, bit_position)` pairs; the shared
/// [`BitmapIndexHandler`] wrapper accumulates them into a single-row RoaringBitmap
/// and commits via a read-modify-write CAS loop.
pub trait BitmapIndexProcessor: Processor<Value = BitmapIndexValue> {
    /// The BigTable table that holds this index.
    const TABLE: &'static str;
    /// The column qualifier that holds the serialized bitmap.
    const COLUMN: &'static str;
}

/// Generic wrapper that implements `concurrent::Handler` for any [`BitmapIndexProcessor`].
///
/// Owns the shared batching invariant (a batch spans a single row key) and the
/// read-modify-write CAS commit loop. Unlike [`crate::handlers::BigTableHandler`],
/// this handler maintains a single-row RoaringBitmap per batch and merges into
/// existing cell values.
pub struct BitmapIndexHandler<P> {
    processor: P,
    rate_limiter: Arc<CompositeRateLimiter>,
}

impl<P> BitmapIndexHandler<P> {
    pub(crate) fn new(
        processor: P,
        _config: &ConcurrentLayer,
        rate_limiter: Arc<CompositeRateLimiter>,
    ) -> Self {
        Self {
            processor,
            rate_limiter,
        }
    }
}

#[async_trait::async_trait]
impl<P> Processor for BitmapIndexHandler<P>
where
    P: BitmapIndexProcessor + Send + Sync,
{
    const NAME: &'static str = P::NAME;
    type Value = BitmapIndexValue;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        self.processor.process(checkpoint).await
    }
}

#[async_trait::async_trait]
impl<P> Handler for BitmapIndexHandler<P>
where
    P: BitmapIndexProcessor + Send + Sync,
{
    type Store = BigTableStore;
    type Batch = BitmapIndexBatch;

    fn batch(
        &self,
        batch: &mut Self::Batch,
        values: &mut std::vec::IntoIter<Self::Value>,
    ) -> BatchStatus {
        loop {
            let next = match values.as_slice().first() {
                Some(v) => v,
                None => return BatchStatus::Pending,
            };

            match &mut batch.inner {
                Some((key, bitmap)) => {
                    if *key != next.row_key {
                        return BatchStatus::Ready;
                    }
                    let v = values.next().unwrap();
                    Arc::make_mut(bitmap).insert(v.bit_position);
                }
                None => {
                    let v = values.next().unwrap();
                    let mut bm = RoaringBitmap::new();
                    bm.insert(v.bit_position);
                    batch.inner = Some((v.row_key, Arc::new(bm)));
                }
            }
        }
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        conn: &mut <Self::Store as Store>::Connection<'a>,
    ) -> anyhow::Result<usize> {
        let (row_key, delta) = match &batch.inner {
            Some((k, bm)) => (k.clone(), bm.clone()),
            None => return Ok(0),
        };

        let mut client = conn.client().clone();
        cas_write_row(
            &mut client,
            &self.rate_limiter,
            &row_key,
            &delta,
            P::TABLE,
            P::COLUMN,
        )
        .await?;
        Ok(1)
    }
}

/// Read-merge-CAS loop for a single row key. On success, returns Ok(()).
/// On contention, re-reads the current value and retries.
async fn cas_write_row(
    client: &mut BigTableClient,
    rate_limiter: &CompositeRateLimiter,
    row_key: &Bytes,
    delta: &Arc<RoaringBitmap>,
    table_name: &'static str,
    column: &'static str,
) -> anyhow::Result<()> {
    for _attempt in 0..MAX_CAS_RETRIES {
        rate_limiter.acquire(1).await;
        let current_bytes = read_bitmap_cell(client, row_key, table_name, column).await?;
        let (predicate, write_bytes) = match current_bytes {
            None => {
                let mut to_write = (**delta).clone();
                to_write.optimize();
                let mut buf = Vec::new();
                to_write
                    .serialize_into(&mut buf)
                    .context("serializing new bitmap")?;
                (CasPredicate::RowEmpty, Bytes::from(buf))
            }
            Some(ref existing_bytes) => {
                let existing_bm = RoaringBitmap::deserialize_from(existing_bytes.as_ref())
                    .context("deserializing existing bitmap")?;

                if delta.is_subset(&existing_bm) {
                    return Ok(());
                }

                let mut merged = &existing_bm | delta.as_ref();
                merged.optimize();
                let mut buf = Vec::new();
                merged
                    .serialize_into(&mut buf)
                    .context("serializing merged bitmap")?;
                (
                    CasPredicate::ValueEquals(existing_bytes.clone()),
                    Bytes::from(buf),
                )
            }
        };

        let set_cell = Mutation {
            mutation: Some(
                crate::bigtable::proto::bigtable::v2::mutation::Mutation::SetCell(SetCell {
                    family_name: tables::FAMILY.to_string(),
                    column_qualifier: Bytes::from(column),
                    timestamp_micros: -1,
                    value: write_bytes,
                }),
            ),
        };

        let (predicate_filter, true_mutations, false_mutations) = match &predicate {
            CasPredicate::RowEmpty => (
                Some(RowFilter {
                    filter: Some(Filter::PassAllFilter(true)),
                }),
                vec![],         // row exists → don't write, re-read below
                vec![set_cell], // row empty → write our delta
            ),
            CasPredicate::ValueEquals(expected) => (
                // Chain CellsPerColumnLimitFilter(1) → ValueRangeFilter to ensure we
                // compare against the latest cell version only. Without this, stale
                // versions (not yet GC'd under maxversions=1) could match the predicate
                // and cause a false CAS success.
                Some(RowFilter {
                    filter: Some(Filter::Chain(row_filter::Chain {
                        filters: vec![
                            RowFilter {
                                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                            },
                            RowFilter {
                                filter: Some(Filter::ValueRangeFilter(ValueRange {
                                    start_value: Some(StartValue::StartValueClosed(
                                        expected.clone(),
                                    )),
                                    end_value: Some(EndValue::EndValueClosed(expected.clone())),
                                })),
                            },
                        ],
                    })),
                }),
                vec![set_cell], // value unchanged → write merged
                vec![],         // value changed → re-read below
            ),
        };

        let matched = client
            .check_and_mutate_row(
                table_name,
                row_key.clone(),
                predicate_filter,
                // Applied when the predicate matches
                true_mutations,
                // Applied when the predicate fails
                false_mutations,
            )
            .await?;

        let wrote = match &predicate {
            CasPredicate::RowEmpty => !matched,
            CasPredicate::ValueEquals(_) => matched,
        };
        if wrote {
            return Ok(());
        }
    }

    anyhow::bail!("CAS retry limit ({MAX_CAS_RETRIES}) exceeded for bitmap index row");
}

/// Read the bitmap column for a single row key, returning None if the row doesn't exist.
async fn read_bitmap_cell(
    client: &mut BigTableClient,
    row_key: &Bytes,
    table_name: &'static str,
    column: &'static str,
) -> anyhow::Result<Option<Bytes>> {
    let rows = client
        .multi_get(table_name, vec![row_key.to_vec()], None)
        .await?;

    Ok(rows
        .into_iter()
        .flat_map(|(_, cells)| cells)
        .find(|(col, _)| col.as_ref() == column.as_bytes())
        .map(|(_, v)| v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bigtable::mock_server::MockBigtableServer;
    use crate::bigtable::store::BigTableStore;
    use crate::tables::transaction_bitmap_index;

    const TABLE: &str = transaction_bitmap_index::NAME;
    const FAMILY: &str = tables::FAMILY;
    const COL: &str = transaction_bitmap_index::col::BITMAP;

    /// Minimal processor used only to exercise the generic handler's batch/commit
    /// logic. Concrete processors live in sibling modules.
    struct TestProcessor;

    #[async_trait::async_trait]
    impl Processor for TestProcessor {
        const NAME: &'static str = "test_bitmap";
        type Value = BitmapIndexValue;

        async fn process(&self, _: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
            Ok(vec![])
        }
    }

    impl BitmapIndexProcessor for TestProcessor {
        const TABLE: &'static str = transaction_bitmap_index::NAME;
        const COLUMN: &'static str = transaction_bitmap_index::col::BITMAP;
    }

    async fn setup() -> (MockBigtableServer, BigTableStore) {
        let mock = MockBigtableServer::new();
        let (addr, _handle) = mock.start().await.unwrap();
        let client = BigTableClient::new_for_host(addr.to_string(), "test".to_string(), "test")
            .await
            .unwrap();
        // Leak the handle so the server lives for the test's duration.
        std::mem::forget(_handle);
        (mock, BigTableStore::new(client))
    }

    fn handler() -> BitmapIndexHandler<TestProcessor> {
        BitmapIndexHandler::new(
            TestProcessor,
            &ConcurrentLayer::default(),
            Arc::new(CompositeRateLimiter::noop()),
        )
    }

    fn make_values(row_key: &[u8], bits: &[u32]) -> Vec<BitmapIndexValue> {
        bits.iter()
            .map(|&bit_position| BitmapIndexValue {
                row_key: Bytes::copy_from_slice(row_key),
                bit_position,
            })
            .collect()
    }

    async fn read_stored_bitmap(
        mock: &MockBigtableServer,
        row_key: &[u8],
    ) -> Option<RoaringBitmap> {
        let bytes = mock
            .get_cell(TABLE, row_key, FAMILY, COL.as_bytes())
            .await?;
        Some(RoaringBitmap::deserialize_from(bytes.as_ref()).unwrap())
    }

    #[tokio::test]
    async fn test_cas_write_to_empty_row() {
        let (mock, store) = setup().await;
        let handler = handler();
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";
        let values = make_values(row_key, &[0, 5, 99]);
        let mut batch = BitmapIndexBatch::default();
        handler.batch(&mut batch, &mut values.into_iter());

        let count = handler.commit(&batch, &mut conn).await.unwrap();
        assert_eq!(count, 1);

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert!(bm.contains(0));
        assert!(bm.contains(5));
        assert!(bm.contains(99));
        assert_eq!(bm.len(), 3);
    }

    #[tokio::test]
    async fn test_cas_merge_with_existing_row() {
        let (mock, store) = setup().await;
        let handler = handler();
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";

        // Pre-seed the row with bits {10, 20}.
        let mut existing = RoaringBitmap::new();
        existing.insert(10);
        existing.insert(20);
        let mut buf = Vec::new();
        existing.serialize_into(&mut buf).unwrap();
        mock.put_cell(TABLE, row_key, FAMILY, COL.as_bytes(), Bytes::from(buf))
            .await;

        // Write bits {20, 30} — should merge to {10, 20, 30}.
        let values = make_values(row_key, &[20, 30]);
        let mut batch = BitmapIndexBatch::default();
        handler.batch(&mut batch, &mut values.into_iter());

        let count = handler.commit(&batch, &mut conn).await.unwrap();
        assert_eq!(count, 1);

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert_eq!(bm.len(), 3);
        assert!(bm.contains(10));
        assert!(bm.contains(20));
        assert!(bm.contains(30));
    }

    #[tokio::test]
    async fn test_cas_skip_when_already_subset() {
        let (mock, store) = setup().await;
        let handler = handler();
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";

        // Pre-seed with bits {1, 2, 3}.
        let mut existing = RoaringBitmap::new();
        existing.insert(1);
        existing.insert(2);
        existing.insert(3);
        let mut buf = Vec::new();
        existing.serialize_into(&mut buf).unwrap();
        let original_bytes = Bytes::from(buf);
        mock.put_cell(
            TABLE,
            row_key,
            FAMILY,
            COL.as_bytes(),
            original_bytes.clone(),
        )
        .await;

        // Write bits {1, 2} — already a subset, should be a no-op.
        let values = make_values(row_key, &[1, 2]);
        let mut batch = BitmapIndexBatch::default();
        handler.batch(&mut batch, &mut values.into_iter());

        let count = handler.commit(&batch, &mut conn).await.unwrap();
        assert_eq!(count, 1);

        // Value should be unchanged.
        let stored = mock
            .get_cell(TABLE, row_key, FAMILY, COL.as_bytes())
            .await
            .unwrap();
        assert_eq!(stored, original_bytes);
    }

    #[tokio::test]
    async fn test_batch_splits_on_different_row_key() {
        let handler = handler();

        let key_a = b"v1#dim#0000000000";
        let key_b = b"v1#dim#0000000001";
        let mut values: Vec<BitmapIndexValue> = make_values(key_a, &[1, 2]);
        values.extend(make_values(key_b, &[3, 4]));

        let mut batch = BitmapIndexBatch::default();
        let status = handler.batch(&mut batch, &mut values.into_iter());

        // Should return Ready when it encounters key_b.
        assert!(matches!(status, BatchStatus::Ready));

        // Batch should contain only key_a's bits.
        let (key, bm) = batch.inner.as_ref().unwrap();
        assert_eq!(key.as_ref(), key_a);
        assert!(bm.contains(1));
        assert!(bm.contains(2));
        assert_eq!(bm.len(), 2);
    }

    #[tokio::test]
    async fn test_batch_pending_when_all_same_key() {
        let handler = handler();

        let key = b"v1#dim#0000000000";
        let values = make_values(key, &[10, 20, 30]);

        let mut batch = BitmapIndexBatch::default();
        let status = handler.batch(&mut batch, &mut values.into_iter());

        assert!(matches!(status, BatchStatus::Pending));

        let (_, bm) = batch.inner.as_ref().unwrap();
        assert_eq!(bm.len(), 3);
    }
}
