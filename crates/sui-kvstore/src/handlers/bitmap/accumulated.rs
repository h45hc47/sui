// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Accumulated bitmap state, owned by a [`super::BitmapIndexHandler`] and
//! mutated exclusively by `commit()`.
//!
//! BigTable cells for the bitmap tables are written with `maxversions=1`, so each
//! flush writes the full accumulated bitmap (not a delta). That requires keeping
//! the bits OR'd in over the lifetime of this process in memory.
//! `AccumulatedState` holds that per-row state; `Batch` (see [`super::batch`])
//! is the per-commit delta that gets OR'd into `AccumulatedState` on each commit.
//!
//! ## Correctness
//!
//! The sequential pipeline guarantees in-order commits and retains the same
//! `&batch` on failure. Bitmap OR is idempotent and monotonic, so re-applying
//! the same batch on retry is safe. Cell timestamps are the checkpoint
//! timestamp, which is monotonically non-decreasing per row; with
//! `maxversions=1`, later accumulated writes always supersede earlier ones and
//! are always supersets.
//!
//! On restart, `startup_tx_hi` is seeded from the persisted committer
//! watermark. Buckets straddling that watermark lazy-load their pre-restart
//! cell on first commit and OR it in, reconciling any partial pre-crash state.

use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use roaring::RoaringBitmap;
use sui_indexer_alt_framework_store_traits::CommitterWatermark;
use tracing::debug;
use tracing::error;

use crate::bigtable::client::BigTableClient;
use crate::bigtable::client::PartialWriteError;
use crate::bigtable::proto::bigtable::v2::mutate_rows_request::Entry;
use crate::handlers::bitmap::BitmapIndexProcessor;
use crate::handlers::bitmap::BitmapIndexValue;
use crate::handlers::bitmap::batch::BitmapIndexBatch;
use crate::rate_limiter::CompositeRateLimiter;
use crate::tables;

const PRE_RESTART_LOAD_CHUNK_SIZE: usize = 100;
const PRE_RESTART_LOAD_CONCURRENCY: usize = 10;

/// Accumulated bitmap state for a single pipeline: bits OR'd in over the
/// lifetime of this process (plus, for the one straddler bucket, a lazy
/// one-shot load of pre-restart state from BigTable).
///
/// Holds the same row shape as [`BitmapIndexBatch`] — a batch is
/// effectively a short-lived accumulator that gets OR'd into this long-lived
/// one each commit. The two side sets track per-row flags that only matter
/// for the accumulated view (whether a row needs flushing, whether we still
/// owe it a pre-restart DB load).
pub struct AccumulatedState {
    table: &'static str,
    column: &'static str,
    seal_fn: fn(u64) -> u64,
    /// Persisted committer watermark's `tx_hi` at startup. Identifies the
    /// single bucket (if any) that straddles the startup watermark — the
    /// only bucket whose rows could have pre-restart bits already in
    /// BigTable. All other buckets' rows skip the DB load.
    startup_tx_hi: u64,
    /// Max entries per BigTable write RPC during `flush`.
    flush_write_chunk_size: usize,
    /// Max parallel BigTable write RPCs during `flush`.
    flush_write_concurrency: usize,
    /// Accumulated bits per row.
    rows: HashMap<Bytes, BitmapIndexValue>,
    /// Row keys modified since the last successful flush — the set written
    /// on the next flush.
    dirty: HashSet<Bytes>,
    /// Row keys in the straddler bucket whose pre-restart BigTable cell
    /// hasn't yet been OR'd into `rows`. Cleared per-row as soon as its DB
    /// state is loaded.
    needs_load_from_db: HashSet<Bytes>,
}

impl AccumulatedState {
    pub fn new(
        table: &'static str,
        column: &'static str,
        seal_fn: fn(u64) -> u64,
        startup_tx_hi: u64,
        flush_write_chunk_size: usize,
        flush_write_concurrency: usize,
    ) -> Self {
        Self {
            table,
            column,
            seal_fn,
            startup_tx_hi,
            flush_write_chunk_size,
            flush_write_concurrency,
            rows: HashMap::new(),
            dirty: HashSet::new(),
            needs_load_from_db: HashSet::new(),
        }
    }

    /// Construct from a [`BitmapIndexProcessor`]'s associated constants.
    pub fn for_processor<P: BitmapIndexProcessor>(
        startup_tx_hi: u64,
        flush_write_chunk_size: usize,
        flush_write_concurrency: usize,
    ) -> Self {
        Self::new(
            P::TABLE,
            P::COLUMN,
            P::seal_tx_hi_exclusive,
            startup_tx_hi,
            flush_write_chunk_size,
            flush_write_concurrency,
        )
    }

    /// OR the batch into accumulated state. Idempotent: re-applying the
    /// same batch (as happens on sequential retry after a partial failure)
    /// adds no new bits, so rows that were successfully written on the
    /// previous attempt won't be re-marked dirty.
    pub fn merge_batch(&mut self, batch: &BitmapIndexBatch) {
        for (row_key, incoming) in batch.rows() {
            let is_new = !self.rows.contains_key(row_key);
            if is_new && self.bucket_straddles_startup(incoming.bucket_id) {
                self.needs_load_from_db.insert(row_key.clone());
            }
            let row = self
                .rows
                .entry(row_key.clone())
                .or_insert_with(|| BitmapIndexValue {
                    row_key: row_key.clone(),
                    bucket_id: incoming.bucket_id,
                    bitmap: RoaringBitmap::new(),
                    max_cp: 0,
                    max_ts_ms: 0,
                });
            let bits_before = row.bitmap.len();
            row.bitmap |= &incoming.bitmap;
            if incoming.max_cp > row.max_cp {
                row.max_cp = incoming.max_cp;
                row.max_ts_ms = incoming.max_ts_ms;
            }
            // Only mark dirty if OR'ing the incoming bits actually grew the
            // row's bitmap. Preserves retry-skip correctness: re-merging a
            // previously-flushed batch is a no-op on `dirty`.
            if row.bitmap.len() > bits_before {
                self.dirty.insert(row_key.clone());
            }
        }
    }

    /// Write every dirty row to BigTable and evict rows whose buckets are
    /// sealed by `watermark.tx_hi`. Call after `merge_batch`. Rate-limits on
    /// the actual row count written to BigTable (not the processor-output
    /// bit count) so the limiter reflects real BigTable mutation cost.
    pub async fn flush(
        &mut self,
        client: &mut BigTableClient,
        watermark: &CommitterWatermark,
        rate_limiter: &CompositeRateLimiter,
    ) -> anyhow::Result<usize> {
        self.load_pre_restart_state(client).await?;

        let table = self.table;
        let column = self.column;
        let entries: Vec<Entry> = self
            .dirty
            .iter()
            .map(|row_key| {
                let row = self
                    .rows
                    .get_mut(row_key)
                    .expect("dirty row must be present in rows");
                row.bitmap.optimize();
                let mut buf = Vec::with_capacity(row.bitmap.serialized_size());
                row.bitmap
                    .serialize_into(&mut buf)
                    .expect("serialize into Vec is infallible");
                tables::make_entry(
                    row_key.clone(),
                    [(column, Bytes::from(buf))],
                    Some(row.max_ts_ms),
                )
            })
            .collect();

        let entry_count = entries.len();
        if entry_count == 0 {
            return Ok(0);
        }

        let chunk_size = self.flush_write_chunk_size;
        let chunk_concurrency = self.flush_write_concurrency;
        let chunk_count = entry_count.div_ceil(chunk_size);
        let write_chunks = entries
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();
        // Wait for every chunk to finish, not just the first failure.
        // Short-circuiting with `?` would drop the remaining in-flight RPCs
        // (since `buffer_unordered` cancels on stream-drop), so we collect
        // all results and handle them below.
        let results: Vec<(Vec<Bytes>, anyhow::Result<()>)> = stream::iter(write_chunks)
            .map(|chunk| {
                let mut client = client.clone();
                async move {
                    rate_limiter.acquire(chunk.len()).await;
                    let chunk_keys: Vec<Bytes> = chunk.iter().map(|e| e.row_key.clone()).collect();
                    let res = client.write_entries(table, chunk).await;
                    (chunk_keys, res)
                }
            })
            .buffer_unordered(chunk_concurrency)
            .collect()
            .await;

        // For each successful chunk (and for per-entry successes within a
        // `PartialWriteError`) remove the written row keys from `self.dirty`
        // so the framework's retry doesn't re-send them.
        let mut first_error: Option<anyhow::Error> = None;
        for (chunk_keys, result) in results {
            match result {
                Ok(()) => {
                    for k in &chunk_keys {
                        self.dirty.remove(k);
                    }
                }
                Err(e) => {
                    if let Some(partial) = e.downcast_ref::<PartialWriteError>() {
                        let failed: HashSet<&Bytes> =
                            partial.failed_keys.iter().map(|f| &f.key).collect();
                        for k in &chunk_keys {
                            if !failed.contains(k) {
                                self.dirty.remove(k);
                            }
                        }
                    }
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        debug!(
            table,
            rows = entry_count,
            chunks = chunk_count,
            chunk_size,
            chunk_concurrency,
            remaining_dirty = self.dirty.len(),
            "Flushed bitmap rows to BigTable",
        );

        // Evict rows we no longer need in memory: clean (bits safely in
        // BigTable) AND not awaiting a pre-restart DB load AND sealed by
        // the watermark (no future checkpoint can add bits). Safe to run
        // before returning the error: dirty rows — including any that
        // failed this flush — are preserved by the `dirty.contains(k)`
        // check so their in-memory bits aren't lost.
        let seal_fn = self.seal_fn;
        let dirty = &self.dirty;
        let needs_load_from_db = &self.needs_load_from_db;
        self.rows.retain(|k, r| {
            dirty.contains(k)
                || needs_load_from_db.contains(k)
                || seal_fn(r.bucket_id) > watermark.tx_hi
        });

        if let Some(e) = first_error {
            return Err(e);
        }

        Ok(entry_count)
    }

    /// `true` iff bucket `B`'s tx range contains `startup_tx_hi`. For
    /// bucket B, the low-tx-end is `seal_fn(B-1)` (or `0` for `B = 0`);
    /// the high-tx-end is `seal_fn(B)`. Every row we ever see has
    /// `seal_fn(B) > startup_tx_hi` (otherwise the bucket is fully
    /// pre-restart and wouldn't be touched by replay), so the straddling
    /// test collapses to `bucket_low < startup_tx_hi`.
    fn bucket_straddles_startup(&self, bucket_id: u64) -> bool {
        let bucket_low = if bucket_id == 0 {
            0
        } else {
            (self.seal_fn)(bucket_id - 1)
        };
        bucket_low < self.startup_tx_hi
    }

    async fn load_pre_restart_state(&mut self, client: &mut BigTableClient) -> anyhow::Result<()> {
        let need_load: Vec<Vec<u8>> = self
            .dirty
            .iter()
            .filter(|k| self.needs_load_from_db.contains(*k))
            .map(|k| k.to_vec())
            .collect();

        if need_load.is_empty() {
            return Ok(());
        }

        let table = self.table;
        let column = self.column;
        let need_load_len = need_load.len();
        debug!(
            table,
            rows = need_load_len,
            "Loading pre-restart bitmap rows from BigTable",
        );

        let chunk_count = need_load_len.div_ceil(PRE_RESTART_LOAD_CHUNK_SIZE);
        let mut fetched_rows = 0usize;
        let chunked_keys = need_load
            .chunks(PRE_RESTART_LOAD_CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();
        let mut chunks = stream::iter(chunked_keys)
            .map(|chunk| {
                let mut client = client.clone();
                async move {
                    let chunk_len = chunk.len();
                    let fetched = client.multi_get(table, chunk.clone(), None).await;
                    (chunk_len, chunk, fetched)
                }
            })
            .buffer_unordered(PRE_RESTART_LOAD_CONCURRENCY);

        while let Some((chunk_len, attempted_keys, fetched)) = chunks.next().await {
            let fetched = match fetched {
                Ok(fetched) => fetched,
                Err(e) => {
                    error!(
                        table,
                        rows = need_load_len,
                        chunk_rows = chunk_len,
                        chunk_size = PRE_RESTART_LOAD_CHUNK_SIZE,
                        chunk_concurrency = PRE_RESTART_LOAD_CONCURRENCY,
                        error = %e,
                        error_debug = ?e,
                        "Failed loading pre-restart bitmap rows from BigTable",
                    );
                    return Err(e).context("loading pre-existing bitmap rows from BigTable");
                }
            };

            fetched_rows += fetched.len();

            let mut db_bitmaps: HashMap<Bytes, RoaringBitmap> = HashMap::new();
            for (row_key, cells) in fetched {
                for (col, val) in cells {
                    if col.as_ref() == column.as_bytes() {
                        let bm = RoaringBitmap::deserialize_from(val.as_ref())
                            .context("deserializing existing bitmap from BigTable")?;
                        db_bitmaps.insert(row_key.clone(), bm);
                        break;
                    }
                }
            }

            for attempted in attempted_keys {
                self.needs_load_from_db.remove(attempted.as_slice());
            }
            for (row_key, db_bm) in db_bitmaps {
                if let Some(row) = self.rows.get_mut(&row_key) {
                    row.bitmap |= db_bm;
                }
            }
        }

        debug!(
            table,
            rows = need_load_len,
            chunks = chunk_count,
            fetched = fetched_rows,
            chunk_size = PRE_RESTART_LOAD_CHUNK_SIZE,
            chunk_concurrency = PRE_RESTART_LOAD_CONCURRENCY,
            "Loaded pre-restart bitmap rows from BigTable",
        );

        Ok(())
    }
}

#[cfg(test)]
impl AccumulatedState {
    pub fn contains_row(&self, row_key: &[u8]) -> bool {
        self.rows.contains_key(row_key)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use roaring::RoaringBitmap;
    use sui_indexer_alt_framework::pipeline::Processor;
    use sui_indexer_alt_framework_store_traits::CommitterWatermark;
    use sui_indexer_alt_framework_store_traits::Store;
    use sui_types::full_checkpoint_content::Checkpoint;

    use super::AccumulatedState;
    use crate::bigtable::client::BigTableClient;
    use crate::bigtable::mock_server::MockBigtableServer;
    use crate::bigtable::store::BigTableStore;
    use crate::handlers::bitmap::BitmapIndexProcessor;
    use crate::handlers::bitmap::BitmapIndexValue;
    use crate::handlers::bitmap::batch::BitmapIndexBatch;
    use crate::rate_limiter::CompositeRateLimiter;
    use crate::tables;
    use crate::tables::transaction_bitmap_index;

    const TABLE: &str = transaction_bitmap_index::NAME;
    const FAMILY: &str = tables::FAMILY;
    const COL: &str = transaction_bitmap_index::col::BITMAP;
    const TEST_FLUSH_WRITE_CHUNK_SIZE: usize = 100;
    const TEST_FLUSH_WRITE_CONCURRENCY: usize = 4;

    struct TestProcessor;

    #[async_trait]
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

        fn seal_tx_hi_exclusive(bucket_id: u64) -> u64 {
            (bucket_id + 1) * transaction_bitmap_index::BUCKET_SIZE
        }
    }

    async fn setup() -> (MockBigtableServer, BigTableStore) {
        let mock = MockBigtableServer::new();
        let (addr, handle) = mock.start().await.unwrap();
        let client = BigTableClient::new_for_host(addr.to_string(), "test".to_string(), "test")
            .await
            .unwrap();
        std::mem::forget(handle);
        (mock, BigTableStore::new(client))
    }

    fn accumulated(startup_tx_hi: u64) -> AccumulatedState {
        AccumulatedState::for_processor::<TestProcessor>(
            startup_tx_hi,
            TEST_FLUSH_WRITE_CHUNK_SIZE,
            TEST_FLUSH_WRITE_CONCURRENCY,
        )
    }

    /// Build a `BitmapIndexBatch` from flat values, mimicking what the
    /// handler's `batch()` would produce.
    fn make_batch(values: Vec<BitmapIndexValue>) -> BitmapIndexBatch {
        let mut batch = BitmapIndexBatch::default();
        batch.extend(values);
        batch
    }

    fn make_values(
        row_key: &[u8],
        bucket_id: u64,
        bits: &[u32],
        max_cp: u64,
        max_ts_ms: u64,
    ) -> Vec<BitmapIndexValue> {
        let mut bitmap = RoaringBitmap::new();
        for &bit in bits {
            bitmap.insert(bit);
        }
        vec![BitmapIndexValue {
            row_key: Bytes::copy_from_slice(row_key),
            bucket_id,
            bitmap,
            max_cp,
            max_ts_ms,
        }]
    }

    fn watermark(cp: u64, tx_hi: u64, ts_ms: u64) -> CommitterWatermark {
        CommitterWatermark {
            epoch_hi_inclusive: 0,
            checkpoint_hi_inclusive: cp,
            tx_hi,
            timestamp_ms_hi_inclusive: ts_ms,
        }
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
    async fn merge_and_flush_writes_bits() {
        let (mock, store) = setup().await;
        let mut state = accumulated(0);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";
        state.merge_batch(&make_batch(make_values(row_key, 0, &[0, 5, 99], 0, 1000)));

        state
            .flush(
                conn.client(),
                &watermark(0, 3, 1000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert!(bm.contains(0));
        assert!(bm.contains(5));
        assert!(bm.contains(99));
        assert_eq!(bm.len(), 3);
    }

    #[tokio::test]
    async fn multiple_batches_merge_before_flush() {
        let (mock, store) = setup().await;
        let mut state = accumulated(0);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";
        state.merge_batch(&make_batch(make_values(row_key, 0, &[5], 1, 2000)));
        state.merge_batch(&make_batch(make_values(row_key, 0, &[1], 0, 1000)));

        state
            .flush(
                conn.client(),
                &watermark(1, 6, 2000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert!(bm.contains(1));
        assert!(bm.contains(5));
        assert_eq!(bm.len(), 2);
    }

    #[tokio::test]
    async fn incremental_flush_supersedes_with_growing_set() {
        let (mock, store) = setup().await;
        let mut state = accumulated(0);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";

        state.merge_batch(&make_batch(make_values(row_key, 0, &[10], 0, 1000)));
        state
            .flush(
                conn.client(),
                &watermark(0, 3, 1000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();
        assert_eq!(read_stored_bitmap(&mock, row_key).await.unwrap().len(), 1);

        state.merge_batch(&make_batch(make_values(row_key, 0, &[20], 1, 2000)));
        state
            .flush(
                conn.client(),
                &watermark(1, 6, 2000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert_eq!(bm.len(), 2);
        assert!(bm.contains(10));
        assert!(bm.contains(20));
    }

    #[tokio::test]
    async fn repeat_flush_is_noop_when_nothing_dirty() {
        let (mock, store) = setup().await;
        let mut state = accumulated(0);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";
        state.merge_batch(&make_batch(make_values(row_key, 0, &[0, 5], 0, 1000)));

        state
            .flush(
                conn.client(),
                &watermark(0, 3, 1000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();
        let first = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert_eq!(first.len(), 2);

        state
            .flush(
                conn.client(),
                &watermark(0, 3, 1000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();
        let second = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert_eq!(second.len(), 2);
    }

    #[tokio::test]
    async fn checkpoint_spans_two_buckets_evicts_sealed_one() {
        let (mock, store) = setup().await;
        let mut state = accumulated(0);
        let mut conn = store.connect().await.unwrap();

        let bucket_size = transaction_bitmap_index::BUCKET_SIZE;
        let row_lo = b"v1#dim#0000000000";
        let row_hi = b"v1#dim#0000000001";
        let tx_hi_excl = bucket_size + 1;

        let mut lo_bm = RoaringBitmap::new();
        lo_bm.insert((bucket_size - 1) as u32);
        let mut hi_bm = RoaringBitmap::new();
        hi_bm.insert(0);
        let values = vec![
            BitmapIndexValue {
                row_key: Bytes::copy_from_slice(row_lo),
                bucket_id: 0,
                bitmap: lo_bm,
                max_cp: 0,
                max_ts_ms: 1000,
            },
            BitmapIndexValue {
                row_key: Bytes::copy_from_slice(row_hi),
                bucket_id: 1,
                bitmap: hi_bm,
                max_cp: 0,
                max_ts_ms: 1000,
            },
        ];

        state.merge_batch(&make_batch(values));
        state
            .flush(
                conn.client(),
                &watermark(0, tx_hi_excl, 1000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();

        assert_eq!(read_stored_bitmap(&mock, row_lo).await.unwrap().len(), 1);
        assert_eq!(read_stored_bitmap(&mock, row_hi).await.unwrap().len(), 1);

        assert!(
            !state.contains_row(row_lo),
            "bucket 0 should be evicted (sealed and clean)"
        );
        assert!(
            state.contains_row(row_hi),
            "bucket 1 should remain (not yet sealed)"
        );
    }

    #[tokio::test]
    async fn restart_loads_existing_bitmap_from_db() {
        let (mock, store) = setup().await;
        let mut state = accumulated(10);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";

        let mut pre = RoaringBitmap::new();
        pre.insert(1);
        pre.insert(5);
        let mut pre_bytes = Vec::new();
        pre.serialize_into(&mut pre_bytes).unwrap();
        conn.client()
            .write_entries(
                TABLE,
                vec![tables::make_entry(
                    Bytes::copy_from_slice(row_key),
                    [(COL, Bytes::from(pre_bytes))],
                    Some(500),
                )],
            )
            .await
            .unwrap();

        state.merge_batch(&make_batch(make_values(row_key, 0, &[10], 5, 2000)));
        state
            .flush(
                conn.client(),
                &watermark(5, 18, 2000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert!(bm.contains(1), "pre-restart bit 1 lost");
        assert!(bm.contains(5), "pre-restart bit 5 lost");
        assert!(bm.contains(10), "new bit 10 missing");
        assert_eq!(bm.len(), 3);
    }

    #[tokio::test]
    async fn straddler_row_with_no_db_state_still_writes_correctly() {
        let (mock, store) = setup().await;
        let mut state = accumulated(10);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";
        state.merge_batch(&make_batch(make_values(row_key, 0, &[7], 5, 2000)));
        state
            .flush(
                conn.client(),
                &watermark(5, 18, 2000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert_eq!(bm.len(), 1);
        assert!(bm.contains(7));
    }

    #[tokio::test]
    async fn second_flush_does_not_reread_db() {
        let (mock, store) = setup().await;
        let mut state = accumulated(10);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";

        let mut pre = RoaringBitmap::new();
        pre.insert(1);
        pre.insert(5);
        let mut pre_bytes = Vec::new();
        pre.serialize_into(&mut pre_bytes).unwrap();
        conn.client()
            .write_entries(
                TABLE,
                vec![tables::make_entry(
                    Bytes::copy_from_slice(row_key),
                    [(COL, Bytes::from(pre_bytes))],
                    Some(500),
                )],
            )
            .await
            .unwrap();

        state.merge_batch(&make_batch(make_values(row_key, 0, &[10], 5, 2000)));
        state
            .flush(
                conn.client(),
                &watermark(5, 18, 2000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();

        let mut tamper = RoaringBitmap::new();
        tamper.insert(99);
        let mut tamper_bytes = Vec::new();
        tamper.serialize_into(&mut tamper_bytes).unwrap();
        conn.client()
            .write_entries(
                TABLE,
                vec![tables::make_entry(
                    Bytes::copy_from_slice(row_key),
                    [(COL, Bytes::from(tamper_bytes))],
                    Some(3000),
                )],
            )
            .await
            .unwrap();

        state.merge_batch(&make_batch(make_values(row_key, 0, &[20], 6, 4000)));
        state
            .flush(
                conn.client(),
                &watermark(6, 20, 4000),
                &CompositeRateLimiter::noop(),
            )
            .await
            .unwrap();

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert!(bm.contains(1));
        assert!(bm.contains(5));
        assert!(bm.contains(10));
        assert!(bm.contains(20));
        assert!(
            !bm.contains(99),
            "DB was re-read between flushes — bit 99 leaked in"
        );
        assert_eq!(bm.len(), 4);
    }
}
