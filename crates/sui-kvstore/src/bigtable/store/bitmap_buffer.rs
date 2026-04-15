// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! In-memory bitmap accumulator that the [`super::BigTableStore`] flushes to
//! BigTable inside `set_committer_watermark`.
//!
//! Ownership model: three disjoint single-owner regions, no sharing of
//! bitmaps across threads.
//!
//! 1. **Per-batch (commit task)**: [`BitmapIndexBatchInner`] is a
//!    `HashMap<Bytes, BatchedRow>` populated by the handler's `batch()`.
//!    Values with the same row key merge locally — first-level dedup.
//! 2. **In-flight on an `mpsc` channel**: the handler's `commit()` calls
//!    `mem::take` on the batch's map and sends it as a
//!    [`BatchMessage`] to the buffer. Ownership transfers to the channel.
//! 3. **Canonical cumulative state**: [`FlushState`] holds the
//!    `HashMap<Bytes, CanonicalRow>` that represents every bit ever
//!    contributed to any row for this pipeline. Mutated only by
//!    [`BitmapBuffer::flush_through`] — never contended.
//!
//! The framework serializes `set_committer_watermark` per pipeline, so
//! `flush_through` is never called concurrently with itself. `FlushState`
//! is wrapped in a `Mutex` so the buffer can expose `&self` methods, but
//! the lock is uncontended: commit-side code only touches the mpsc sender.
//!
//! ## Correctness
//!
//! The framework only invokes `set_committer_watermark(cp_hi)` once every
//! `commit()` for `cp ≤ cp_hi` has returned `Ok`. `commit()` sends the
//! batch on the channel before returning, so by the time
//! `set_committer_watermark` runs, every relevant batch is either already
//! in the channel or already drained into canonical. `flush_through`
//! drains the channel, merges into canonical, writes to BigTable, and
//! returns — only then does the watermark advance in BigTable.
//!
//! BigTable cells are written with `maxversions=1`, so each flush writes
//! the full cumulative bitmap (not a delta). The cell timestamp is the
//! checkpoint timestamp of the latest contributing checkpoint; since
//! `max_cp` is monotonically non-decreasing per row across flushes, later
//! writes always supersede earlier ones and are always supersets.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use roaring::RoaringBitmap;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework_store_traits::CommitterWatermark;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;

use crate::bigtable::client::BigTableClient;
use crate::bigtable::proto::bigtable::v2::mutate_rows_request::Entry;
use crate::tables;

const PRE_RESTART_LOAD_CHUNK_SIZE: usize = 100;
const PRE_RESTART_LOAD_CONCURRENCY: usize = 10;
const FLUSH_WRITE_CHUNK_SIZE: usize = 5_000;
const FLUSH_WRITE_CONCURRENCY: usize = 10;

/// One bit to set in a bitmap-index row, plus the checkpoint metadata the
/// buffer needs to determine cell timestamps and seal-for-eviction conditions.
pub struct BitmapIndexValue {
    pub row_key: Bytes,
    pub bucket_id: u64,
    pub bit_position: u32,
    pub checkpoint_seq: u64,
    /// Checkpoint wall-clock timestamp (ms). Used as the BigTable cell
    /// version on flush so cumulative re-writes monotonically supersede
    /// earlier ones under `maxversions=1`.
    pub timestamp_ms: u64,
}

/// Extension of `Processor` that targets a Roaring-bitmap inverted index.
pub trait BitmapIndexProcessor: Processor<Value = BitmapIndexValue> {
    /// The BigTable table that holds this index.
    const TABLE: &'static str;
    /// The column qualifier that holds the serialized bitmap.
    const COLUMN: &'static str;

    /// Smallest `tx_hi_exclusive` that, once covered by the persisted
    /// committer watermark's `tx_hi`, guarantees no future checkpoint can
    /// contribute a bit to the bucket. Used for canonical eviction.
    fn seal_tx_hi_exclusive(bucket_id: u64) -> u64;
}

/// Per-batch accumulator owned by a commit task. Handler's `batch()` OR's
/// values into `rows` and appends checkpoint metadata to `cps`. `commit()`
/// drains both into a [`BatchMessage`] and sends on the buffer's channel.
#[derive(Default)]
pub struct BitmapIndexBatch {
    inner: Mutex<BatchInner>,
}

#[derive(Default)]
struct BatchInner {
    rows: HashMap<Bytes, BatchedRow>,
    cps: BTreeMap<u64, u64>,
}

pub struct BatchedRow {
    pub bucket_id: u64,
    pub bitmap: RoaringBitmap,
    pub max_cp: u64,
}

/// Ownership-transfer unit between `commit()` and `flush_through`.
pub struct BatchMessage {
    pub rows: HashMap<Bytes, BatchedRow>,
    /// `cp_seq -> timestamp_ms` for checkpoints represented in `rows`.
    pub cps: BTreeMap<u64, u64>,
}

impl BitmapIndexBatch {
    /// Merge a batch of values into this per-task accumulator.
    pub fn extend(&self, values: impl IntoIterator<Item = BitmapIndexValue>) {
        let mut inner = self.inner.lock().unwrap();
        let mut last_checkpoint_seq = None;
        for v in values {
            if last_checkpoint_seq != Some(v.checkpoint_seq) {
                inner.cps.entry(v.checkpoint_seq).or_insert(v.timestamp_ms);
                last_checkpoint_seq = Some(v.checkpoint_seq);
            }
            let row = inner.rows.entry(v.row_key).or_insert(BatchedRow {
                bucket_id: v.bucket_id,
                bitmap: RoaringBitmap::new(),
                max_cp: 0,
            });
            row.bitmap.insert(v.bit_position);
            if v.checkpoint_seq > row.max_cp {
                row.max_cp = v.checkpoint_seq;
            }
        }
    }

    /// Drain the accumulator into a [`BatchMessage`] ready to send.
    pub fn take(&self) -> BatchMessage {
        let mut inner = self.inner.lock().unwrap();
        BatchMessage {
            rows: std::mem::take(&mut inner.rows),
            cps: std::mem::take(&mut inner.cps),
        }
    }
}

/// Per-pipeline buffer owned by [`super::BigTableStore`]. Holds the mpsc
/// sender that commit tasks send to, and the `FlushState` that
/// `flush_through` drains the receiver into.
pub struct BitmapBuffer {
    table: &'static str,
    column: &'static str,
    seal_fn: fn(u64) -> u64,
    /// Persisted committer watermark's `tx_hi` at buffer-creation time.
    /// Identifies the single bucket (if any) that straddles the startup
    /// watermark — the only bucket whose rows could have pre-restart bits
    /// already in BigTable. All other buckets' rows skip the DB load.
    startup_tx_hi: u64,
    sender: mpsc::UnboundedSender<BatchMessage>,
    flush_state: Mutex<FlushState>,
}

struct FlushState {
    receiver: mpsc::UnboundedReceiver<BatchMessage>,
    /// Cumulative bits per row. Owned exclusively by the flush thread.
    canonical: HashMap<Bytes, CanonicalRow>,
    /// `cp_seq -> timestamp_ms`. Populated as batches drain in; looked up
    /// at write time to pick each row's cell timestamp from its `max_cp`.
    cps: BTreeMap<u64, u64>,
}

struct CanonicalRow {
    bucket_id: u64,
    bitmap: RoaringBitmap,
    max_cp: u64,
    /// `true` if bits were added since the last successful flush.
    dirty: bool,
    /// `true` iff this row's bucket straddles the startup watermark and
    /// we haven't yet OR'd the pre-restart BigTable cell into `bitmap`.
    needs_load_from_db: bool,
}

impl BitmapBuffer {
    pub fn new(
        table: &'static str,
        column: &'static str,
        seal_fn: fn(u64) -> u64,
        startup_tx_hi: u64,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            table,
            column,
            seal_fn,
            startup_tx_hi,
            sender,
            flush_state: Mutex::new(FlushState {
                receiver,
                canonical: HashMap::new(),
                cps: BTreeMap::new(),
            }),
        }
    }

    /// Construct from a [`BitmapIndexProcessor`]'s associated constants.
    pub fn for_processor<P: BitmapIndexProcessor>(startup_tx_hi: u64) -> Self {
        Self::new(P::TABLE, P::COLUMN, P::seal_tx_hi_exclusive, startup_tx_hi)
    }

    /// Enqueue a batch for the flush thread to merge. Panics if the
    /// receiver has been dropped (which would mean the buffer itself was
    /// dropped while commit tasks still hold references — a wiring bug).
    pub fn send(&self, msg: BatchMessage) {
        self.sender
            .send(msg)
            .unwrap_or_else(|_| panic!("bitmap buffer receiver dropped"));
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

    /// Called from `BigTableConnection::set_committer_watermark` before
    /// the watermark write. Drains any pending batches into canonical,
    /// writes every dirty row to BigTable, and evicts rows whose buckets
    /// are sealed by `watermark.tx_hi`.
    pub async fn flush_through(
        &self,
        client: &mut BigTableClient,
        watermark: &CommitterWatermark,
    ) -> anyhow::Result<()> {
        // 1. Drain all pending batches into canonical. Second-level dedup
        // happens here: rows that appeared in multiple batches merge into
        // one canonical row.
        {
            let mut state = self.flush_state.lock().unwrap();
            while let Ok(msg) = state.receiver.try_recv() {
                self.merge_into_canonical(&mut state, msg);
            }
        }

        // 2. Load pre-restart DB state for dirty straddler-bucket rows.
        self.load_pre_restart_state(client).await?;

        // 3. Serialize every dirty row in place (we own canonical
        // exclusively) and collect entries to write.
        let entries: Vec<Entry> = {
            let mut state = self.flush_state.lock().unwrap();
            let FlushState { canonical, cps, .. } = &mut *state;
            canonical
                .iter_mut()
                .filter(|(_, r)| r.dirty)
                .map(|(row_key, row)| {
                    row.bitmap.optimize();
                    let mut buf = Vec::with_capacity(row.bitmap.serialized_size());
                    row.bitmap
                        .serialize_into(&mut buf)
                        .expect("serialize into Vec is infallible");
                    let ts_ms = *cps.get(&row.max_cp).expect("max_cp must exist in cps");
                    tables::make_entry(
                        row_key.clone(),
                        [(self.column, Bytes::from(buf))],
                        Some(ts_ms),
                    )
                })
                .collect::<Vec<_>>()
        };

        if entries.is_empty() {
            return Ok(());
        }

        // 4. Write outside the lock. Commits keep sending to the channel;
        // canonical doesn't change until the next flush drains.
        let entry_count = entries.len();
        let chunk_count = entry_count.div_ceil(FLUSH_WRITE_CHUNK_SIZE);
        let write_chunks = entries
            .chunks(FLUSH_WRITE_CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();
        let mut writes = stream::iter(write_chunks)
            .map(|chunk| {
                let mut client = client.clone();
                async move { client.write_entries(self.table, chunk).await }
            })
            .buffer_unordered(FLUSH_WRITE_CONCURRENCY);
        while let Some(result) = writes.next().await {
            result?;
        }
        debug!(
            table = self.table,
            rows = entry_count,
            chunks = chunk_count,
            chunk_size = FLUSH_WRITE_CHUNK_SIZE,
            chunk_concurrency = FLUSH_WRITE_CONCURRENCY,
            "Flushed bitmap rows to BigTable",
        );

        // 5. Mark every dirty row clean (nothing else touched canonical
        // during IO) and evict sealed rows.
        let mut state = self.flush_state.lock().unwrap();
        let seal_fn = self.seal_fn;
        for row in state.canonical.values_mut() {
            row.dirty = false;
        }
        state.canonical.retain(|_k, r| {
            !(!r.needs_load_from_db && !r.dirty && seal_fn(r.bucket_id) <= watermark.tx_hi)
        });

        Ok(())
    }

    /// OR a single drained [`BatchMessage`] into the canonical state.
    /// Creates new canonical rows with `needs_load_from_db` set based on
    /// whether the row's bucket straddles the startup watermark.
    fn merge_into_canonical(&self, state: &mut FlushState, msg: BatchMessage) {
        for (cp, ts_ms) in msg.cps {
            state.cps.entry(cp).or_insert(ts_ms);
        }
        for (row_key, batched) in msg.rows {
            let needs_load_from_db = self.bucket_straddles_startup(batched.bucket_id);
            let row = state.canonical.entry(row_key).or_insert(CanonicalRow {
                bucket_id: batched.bucket_id,
                bitmap: RoaringBitmap::new(),
                max_cp: 0,
                dirty: false,
                needs_load_from_db,
            });
            row.bitmap |= batched.bitmap;
            if batched.max_cp > row.max_cp {
                row.max_cp = batched.max_cp;
            }
            row.dirty = true;
        }
    }

    /// Fetch any pre-restart BigTable cells for dirty straddler-bucket
    /// rows and OR them into canonical. After this call,
    /// `needs_load_from_db` is `false` for the rows that were loaded.
    async fn load_pre_restart_state(&self, client: &mut BigTableClient) -> anyhow::Result<()> {
        let need_load: Vec<Vec<u8>> = {
            let state = self.flush_state.lock().unwrap();
            state
                .canonical
                .iter()
                .filter(|(_, r)| r.dirty && r.needs_load_from_db)
                .map(|(k, _)| k.to_vec())
                .collect()
        };

        if need_load.is_empty() {
            return Ok(());
        }

        let need_load_len = need_load.len();
        debug!(
            table = self.table,
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
                    let fetched = client.multi_get(self.table, chunk.clone(), None).await;
                    (chunk_len, chunk, fetched)
                }
            })
            .buffer_unordered(PRE_RESTART_LOAD_CONCURRENCY);

        while let Some((chunk_len, attempted_keys, fetched)) = chunks.next().await {
            let fetched = match fetched {
                Ok(fetched) => fetched,
                Err(e) => {
                    error!(
                        table = self.table,
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
                    if col.as_ref() == self.column.as_bytes() {
                        let bm = RoaringBitmap::deserialize_from(val.as_ref())
                            .context("deserializing existing bitmap from BigTable")?;
                        db_bitmaps.insert(row_key.clone(), bm);
                        break;
                    }
                }
            }

            let mut state = self.flush_state.lock().unwrap();
            for attempted in attempted_keys {
                if let Some(row) = state.canonical.get_mut(attempted.as_slice()) {
                    row.needs_load_from_db = false;
                }
            }
            for (row_key, db_bm) in db_bitmaps {
                if let Some(row) = state.canonical.get_mut(&row_key) {
                    row.bitmap |= db_bm;
                }
            }
        }

        debug!(
            table = self.table,
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
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use roaring::RoaringBitmap;
    use sui_indexer_alt_framework::pipeline::Processor;
    use sui_indexer_alt_framework_store_traits::CommitterWatermark;
    use sui_indexer_alt_framework_store_traits::Store;
    use sui_types::full_checkpoint_content::Checkpoint;

    use super::BatchMessage;
    use super::BatchedRow;
    use super::BitmapBuffer;
    use super::BitmapIndexProcessor;
    use super::BitmapIndexValue;
    use crate::bigtable::client::BigTableClient;
    use crate::bigtable::mock_server::MockBigtableServer;
    use crate::bigtable::store::BigTableStore;
    use crate::tables;
    use crate::tables::transaction_bitmap_index;

    const TABLE: &str = transaction_bitmap_index::NAME;
    const FAMILY: &str = tables::FAMILY;
    const COL: &str = transaction_bitmap_index::col::BITMAP;

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

    fn buffer(startup_tx_hi: u64) -> BitmapBuffer {
        BitmapBuffer::for_processor::<TestProcessor>(startup_tx_hi)
    }

    /// Build a `BatchMessage` from flat `BitmapIndexValue`s, mimicking
    /// what the handler's `batch()` + `commit()` would produce.
    fn batch_message(values: Vec<BitmapIndexValue>) -> BatchMessage {
        let mut rows: std::collections::HashMap<Bytes, BatchedRow> = Default::default();
        let mut cps = BTreeMap::new();
        for v in values {
            cps.entry(v.checkpoint_seq).or_insert(v.timestamp_ms);
            let row = rows.entry(v.row_key).or_insert(BatchedRow {
                bucket_id: v.bucket_id,
                bitmap: RoaringBitmap::new(),
                max_cp: 0,
            });
            row.bitmap.insert(v.bit_position);
            if v.checkpoint_seq > row.max_cp {
                row.max_cp = v.checkpoint_seq;
            }
        }
        BatchMessage { rows, cps }
    }

    fn make_values(
        row_key: &[u8],
        bucket_id: u64,
        bits: &[u32],
        checkpoint_seq: u64,
        timestamp_ms: u64,
    ) -> Vec<BitmapIndexValue> {
        bits.iter()
            .map(|&bit_position| BitmapIndexValue {
                row_key: Bytes::copy_from_slice(row_key),
                bucket_id,
                bit_position,
                checkpoint_seq,
                timestamp_ms,
            })
            .collect()
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
    async fn send_does_not_write_until_flush() {
        let (mock, store) = setup().await;
        let buf = buffer(0);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";
        buf.send(batch_message(make_values(row_key, 0, &[0, 5, 99], 0, 1000)));
        assert!(read_stored_bitmap(&mock, row_key).await.is_none());

        buf.flush_through(conn.client(), &watermark(0, 3, 1000))
            .await
            .unwrap();

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert!(bm.contains(0));
        assert!(bm.contains(5));
        assert!(bm.contains(99));
        assert_eq!(bm.len(), 3);
    }

    #[tokio::test]
    async fn multiple_queued_batches_collapse_into_one_flush() {
        // Two batches for the same row queue up on the channel; flush
        // drains both and writes the union.
        let (mock, store) = setup().await;
        let buf = buffer(0);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";
        buf.send(batch_message(make_values(row_key, 0, &[5], 1, 2000)));
        buf.send(batch_message(make_values(row_key, 0, &[1], 0, 1000)));

        buf.flush_through(conn.client(), &watermark(1, 6, 2000))
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
        let buf = buffer(0);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";

        buf.send(batch_message(make_values(row_key, 0, &[10], 0, 1000)));
        buf.flush_through(conn.client(), &watermark(0, 3, 1000))
            .await
            .unwrap();
        assert_eq!(read_stored_bitmap(&mock, row_key).await.unwrap().len(), 1);

        buf.send(batch_message(make_values(row_key, 0, &[20], 1, 2000)));
        buf.flush_through(conn.client(), &watermark(1, 6, 2000))
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
        let buf = buffer(0);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";
        buf.send(batch_message(make_values(row_key, 0, &[0, 5], 0, 1000)));

        buf.flush_through(conn.client(), &watermark(0, 3, 1000))
            .await
            .unwrap();
        let first = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert_eq!(first.len(), 2);

        buf.flush_through(conn.client(), &watermark(0, 3, 1000))
            .await
            .unwrap();
        let second = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert_eq!(second.len(), 2);
    }

    #[tokio::test]
    async fn checkpoint_spans_two_buckets_evicts_sealed_one() {
        let (mock, store) = setup().await;
        let buf = buffer(0);
        let mut conn = store.connect().await.unwrap();

        let bucket_size = transaction_bitmap_index::BUCKET_SIZE;
        let row_lo = b"v1#dim#0000000000";
        let row_hi = b"v1#dim#0000000001";
        let tx_hi_excl = bucket_size + 1;

        let mut values = vec![BitmapIndexValue {
            row_key: Bytes::copy_from_slice(row_lo),
            bucket_id: 0,
            bit_position: (bucket_size - 1) as u32,
            checkpoint_seq: 0,
            timestamp_ms: 1000,
        }];
        values.push(BitmapIndexValue {
            row_key: Bytes::copy_from_slice(row_hi),
            bucket_id: 1,
            bit_position: 0,
            checkpoint_seq: 0,
            timestamp_ms: 1000,
        });

        buf.send(batch_message(values));
        buf.flush_through(conn.client(), &watermark(0, tx_hi_excl, 1000))
            .await
            .unwrap();

        assert_eq!(read_stored_bitmap(&mock, row_lo).await.unwrap().len(), 1);
        assert_eq!(read_stored_bitmap(&mock, row_hi).await.unwrap().len(), 1);

        let state = buf.flush_state.lock().unwrap();
        assert!(
            !state
                .canonical
                .contains_key(&Bytes::copy_from_slice(row_lo)),
            "bucket 0 should be evicted (sealed and clean)"
        );
        assert!(
            state
                .canonical
                .contains_key(&Bytes::copy_from_slice(row_hi)),
            "bucket 1 should remain (not yet sealed)"
        );
    }

    #[tokio::test]
    async fn restart_loads_existing_bitmap_from_db() {
        // Persisted tx_hi at startup is 10, which straddles bucket 0
        // (low-end 0 < startup_tx_hi). The buffer should lazy-load the
        // pre-restart bitmap from BigTable on the first flush.
        let (mock, store) = setup().await;
        let buf = buffer(10);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";

        // Pre-seed DB with bits {1, 5} as if a prior run had flushed them.
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

        // Fresh buffer merges new bit 10 from cp 5.
        buf.send(batch_message(make_values(row_key, 0, &[10], 5, 2000)));
        buf.flush_through(conn.client(), &watermark(5, 18, 2000))
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
        let buf = buffer(10);
        let mut conn = store.connect().await.unwrap();

        let row_key = b"v1#dim#0000000000";
        buf.send(batch_message(make_values(row_key, 0, &[7], 5, 2000)));
        buf.flush_through(conn.client(), &watermark(5, 18, 2000))
            .await
            .unwrap();

        let bm = read_stored_bitmap(&mock, row_key).await.unwrap();
        assert_eq!(bm.len(), 1);
        assert!(bm.contains(7));
    }

    #[tokio::test]
    async fn second_flush_does_not_reread_db() {
        // Once a straddler row's pre-restart state is loaded into canonical,
        // subsequent flushes must not re-read the DB. Verified by tampering
        // with the DB cell between flushes and asserting the tampered bit
        // doesn't leak into the written bitmap.
        let (mock, store) = setup().await;
        let buf = buffer(10);
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

        buf.send(batch_message(make_values(row_key, 0, &[10], 5, 2000)));
        buf.flush_through(conn.client(), &watermark(5, 18, 2000))
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

        buf.send(batch_message(make_values(row_key, 0, &[20], 6, 4000)));
        buf.flush_through(conn.client(), &watermark(6, 20, 4000))
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
