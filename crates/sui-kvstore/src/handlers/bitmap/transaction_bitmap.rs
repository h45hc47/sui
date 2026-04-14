// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Transaction-keyed Roaring bitmap inverted index processor.
//!
//! Emits one bit per `(dimension, tx_seq)` pair. Bits within a bucket row
//! correspond to `tx_sequence_number`s; see [`crate::tables::transaction_bitmap_index`].

use std::sync::Arc;

use bytes::Bytes;
use sui_index_dimensions::encode_dimension_key;
use sui_index_dimensions::extract_transaction_dimensions;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_types::full_checkpoint_content::Checkpoint;

use crate::tables::transaction_bitmap_index;

use crate::bigtable::store::BitmapIndexProcessor;
use crate::bigtable::store::BitmapIndexValue;

// Compile-time check that BUCKET_SIZE fits in u32 (required for RoaringBitmap bit positions).
const _: () = assert!(transaction_bitmap_index::BUCKET_SIZE <= u32::MAX as u64);

/// Tx-keyed bitmap index: one bit per (dimension, tx_seq).
pub struct TransactionBitmapProcessor;

#[async_trait::async_trait]
impl Processor for TransactionBitmapProcessor {
    const NAME: &'static str = "kvstore_transaction_bitmap_index";
    type Value = BitmapIndexValue;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let cp = checkpoint.summary.data();
        let checkpoint_seq = cp.sequence_number;
        let tx_hi_exclusive = cp.network_total_transactions;
        let timestamp_ms = cp.timestamp_ms;
        // network_total_transactions is the cumulative count *including* this
        // checkpoint's transactions, so tx_lo is the first tx_seq in this checkpoint.
        let tx_lo = tx_hi_exclusive - checkpoint.transactions.len() as u64;

        let mut values = Vec::new();
        for (i, tx) in checkpoint.transactions.iter().enumerate() {
            let tx_seq = tx_lo + i as u64;
            let bucket_id = tx_seq / transaction_bitmap_index::BUCKET_SIZE;
            let bit_position = (tx_seq % transaction_bitmap_index::BUCKET_SIZE) as u32;

            for (dim, value) in extract_transaction_dimensions(tx) {
                let dim_key = encode_dimension_key(dim, &value);
                let row_key = transaction_bitmap_index::encode_row_key(
                    transaction_bitmap_index::SCHEMA_VERSION,
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

impl BitmapIndexProcessor for TransactionBitmapProcessor {
    const TABLE: &'static str = transaction_bitmap_index::NAME;
    const COLUMN: &'static str = transaction_bitmap_index::col::BITMAP;

    fn seal_tx_hi_exclusive(bucket_id: u64) -> u64 {
        (bucket_id + 1) * transaction_bitmap_index::BUCKET_SIZE
    }
}
