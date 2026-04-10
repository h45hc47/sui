// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Direct mapping from `tx_sequence_number → (TransactionDigest, checkpoint_seq, event_count)`.
//!
//! Row key is the tx_sequence_number (big-endian u64). One row per transaction.
//! Resolving a batch of tx_seqs is a single `multi_get` — no range scan or
//! checkpoint-content join needed.
//!
//! `event_count` lets readers enumerate a transaction's event_seqs without
//! reading the tx row itself — used by unfiltered event listing to bound
//! the walk to exactly the events contributing to a page.
//!
//! Rows written before the `event_count` column was added decode as
//! `event_count = 0`, which is silently wrong for unfiltered event listing.
//! Re-index from genesis (or backfill) before relying on it.

use anyhow::{Context, Result};
use bytes::Bytes;
use sui_types::digests::TransactionDigest;

pub const NAME: &str = "tx_seq_digest";

pub mod col {
    /// Raw 32-byte TransactionDigest.
    pub const DIGEST: &str = "d";
    /// BCS-encoded u64 checkpoint sequence number.
    pub const CHECKPOINT_SEQ: &str = "c";
    /// BCS-encoded u32 count of events emitted by this transaction.
    pub const EVENT_COUNT: &str = "e";
}

/// Row key: tx_sequence_number, big-endian u64.
pub fn encode_key(tx_seq: u64) -> Vec<u8> {
    tx_seq.to_be_bytes().to_vec()
}

pub fn encode(
    digest: &TransactionDigest,
    checkpoint_seq: u64,
    event_count: u32,
) -> [(&'static str, Bytes); 3] {
    [
        (col::DIGEST, Bytes::from(digest.inner().to_vec())),
        (
            col::CHECKPOINT_SEQ,
            Bytes::from(bcs::to_bytes(&checkpoint_seq).unwrap()),
        ),
        (
            col::EVENT_COUNT,
            Bytes::from(bcs::to_bytes(&event_count).unwrap()),
        ),
    ]
}

pub fn decode(cells: &[(Bytes, Bytes)]) -> Result<(TransactionDigest, u64, u32)> {
    let mut digest: Option<TransactionDigest> = None;
    let mut cp_seq: Option<u64> = None;
    let mut event_count: u32 = 0;
    for (column, value) in cells {
        if column.as_ref() == col::DIGEST.as_bytes() {
            let bytes: [u8; 32] = value
                .as_ref()
                .try_into()
                .context("tx_seq_digest digest not 32 bytes")?;
            digest = Some(TransactionDigest::from(bytes));
        } else if column.as_ref() == col::CHECKPOINT_SEQ.as_bytes() {
            cp_seq = Some(bcs::from_bytes(value).context("invalid checkpoint_seq BCS")?);
        } else if column.as_ref() == col::EVENT_COUNT.as_bytes() {
            event_count = bcs::from_bytes(value).context("invalid event_count BCS")?;
        }
    }
    Ok((
        digest.context("tx_seq_digest missing digest column")?,
        cp_seq.context("tx_seq_digest missing checkpoint_seq column")?,
        event_count,
    ))
}
