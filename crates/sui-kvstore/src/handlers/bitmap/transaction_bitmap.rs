// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Transaction-keyed Roaring bitmap inverted index processor.
//!
//! Emits one bit per `(dimension, tx_seq)` pair. Bits within a bucket row
//! correspond to `tx_sequence_number`s; see [`crate::tables::transaction_bitmap_index`].

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use sui_index_dimensions::for_each_transaction_dimension;
use sui_index_dimensions::write_dimension_key;
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
        let timestamp_ms = cp.timestamp_ms;
        // network_total_transactions is the cumulative count *including* this
        // checkpoint's transactions, so tx_lo is the first tx_seq in this checkpoint.
        let tx_lo = cp.network_total_transactions - checkpoint.transactions.len() as u64;

        let mut values = Vec::new();
        let mut dimension_key = Vec::new();
        let mut row_key = Vec::new();
        let mut seen = HashSet::new();
        for (i, tx) in checkpoint.transactions.iter().enumerate() {
            let tx_seq = tx_lo + i as u64;
            let bucket_id = tx_seq / transaction_bitmap_index::BUCKET_SIZE;
            let bit_position = (tx_seq % transaction_bitmap_index::BUCKET_SIZE) as u32;
            seen.clear();

            for_each_transaction_dimension(tx, |dim, value| {
                write_dimension_key(&mut dimension_key, dim, value);
                transaction_bitmap_index::encode_row_key_into(
                    &mut row_key,
                    transaction_bitmap_index::SCHEMA_VERSION,
                    &dimension_key,
                    bucket_id,
                );
                let row_key = Bytes::copy_from_slice(&row_key);
                if !seen.insert(row_key.clone()) {
                    return;
                }

                values.push(BitmapIndexValue {
                    row_key,
                    bucket_id,
                    bit_position,
                    checkpoint_seq,
                    timestamp_ms,
                });
            });
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use move_core_types::ident_str;
    use sui_types::base_types::ObjectID;
    use sui_types::event::Event;
    use sui_types::gas_coin::GAS;
    use sui_types::test_checkpoint_data_builder::TestCheckpointBuilder;

    use super::*;

    #[tokio::test]
    async fn deduplicates_repeated_tx_dimensions_within_a_transaction() {
        let checkpoint = TestCheckpointBuilder::new(0)
            .start_transaction(1)
            .create_coin_object(10, 2, 10, GAS::type_tag())
            .create_coin_object(11, 2, 20, GAS::type_tag())
            .add_move_call(ObjectID::ZERO, "dup", "call")
            .add_move_call(ObjectID::ZERO, "dup", "call")
            .with_events(vec![
                Event::new(
                    &ObjectID::ZERO,
                    ident_str!("dup_mod"),
                    TestCheckpointBuilder::derive_address(1),
                    GAS::type_(),
                    vec![],
                ),
                Event::new(
                    &ObjectID::ZERO,
                    ident_str!("dup_mod"),
                    TestCheckpointBuilder::derive_address(1),
                    GAS::type_(),
                    vec![],
                ),
            ])
            .finish_transaction()
            .build_checkpoint();

        let values = TransactionBitmapProcessor
            .process(&Arc::new(checkpoint))
            .await
            .unwrap();

        let unique_rows: HashSet<_> = values.iter().map(|v| v.row_key.clone()).collect();
        assert!(!values.is_empty());
        assert_eq!(values.len(), unique_rows.len());
    }
}
