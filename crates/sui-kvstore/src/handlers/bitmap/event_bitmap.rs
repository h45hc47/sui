// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Event-keyed Roaring bitmap inverted index processor.
//!
//! Parallel to [`super::transaction_processor`], but bit positions
//! correspond to packed `event_seq`s (see [`crate::tables::event_bitmap_index`])
//! rather than `tx_sequence_number`s. Enables `list_events` to resolve matches
//! directly in event-space with no over-fetch.

use std::sync::Arc;

use bytes::Bytes;
use sui_index_dimensions::encode_dimension_key;
use sui_index_dimensions::extract_event_dimensions;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_types::full_checkpoint_content::Checkpoint;

use crate::tables::event_bitmap_index;

use crate::bigtable::store::BitmapIndexProcessor;
use crate::bigtable::store::BitmapIndexValue;

// Compile-time check that BUCKET_SIZE fits in u32 (required for RoaringBitmap bit positions).
const _: () = assert!(event_bitmap_index::BUCKET_SIZE <= u32::MAX as u64);

/// Event-keyed bitmap index: one bit per (dimension, packed event_seq).
pub struct EventBitmapProcessor;

#[async_trait::async_trait]
impl Processor for EventBitmapProcessor {
    const NAME: &'static str = "kvstore_event_bitmap_index";
    type Value = BitmapIndexValue;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let cp = checkpoint.summary.data();
        let checkpoint_seq = cp.sequence_number;
        let tx_hi_exclusive = cp.network_total_transactions;
        let timestamp_ms = cp.timestamp_ms;
        // network_total_transactions is cumulative *including* this checkpoint,
        // so tx_lo is the first tx_seq in this checkpoint.
        let tx_lo = tx_hi_exclusive - checkpoint.transactions.len() as u64;

        let mut values = Vec::new();
        for (i, tx) in checkpoint.transactions.iter().enumerate() {
            let tx_seq = tx_lo + i as u64;
            for (dim, value, event_idx) in extract_event_dimensions(tx) {
                let event_seq = event_bitmap_index::encode_event_seq(tx_seq, event_idx);
                let bucket_id = event_seq / event_bitmap_index::BUCKET_SIZE;
                let bit_position = (event_seq % event_bitmap_index::BUCKET_SIZE) as u32;
                let dim_key = encode_dimension_key(dim, &value);
                let row_key = event_bitmap_index::encode_row_key(
                    event_bitmap_index::SCHEMA_VERSION,
                    &dim_key,
                    bucket_id,
                );
                values.push(BitmapIndexValue {
                    row_key: Bytes::from(row_key),
                    bucket_id,
                    bit_position,
                    checkpoint_seq,
                    tx_hi_exclusive,
                    timestamp_ms,
                });
            }
        }
        Ok(values)
    }
}

impl BitmapIndexProcessor for EventBitmapProcessor {
    const TABLE: &'static str = event_bitmap_index::NAME;
    const COLUMN: &'static str = event_bitmap_index::col::BITMAP;

    fn seal_tx_hi_exclusive(bucket_id: u64) -> u64 {
        // Bucket B is sealed once every future tx's smallest event_seq
        // (`event_seq_lo(tx) = tx * MAX_EVENTS_PER_TX`) is past bucket B's
        // upper end. Solve for the smallest tx satisfying that.
        ((bucket_id + 1) * event_bitmap_index::BUCKET_SIZE)
            .div_ceil(event_bitmap_index::MAX_EVENTS_PER_TX as u64)
    }
}
