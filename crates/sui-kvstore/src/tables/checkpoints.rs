// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Checkpoints table: stores full checkpoint data indexed by sequence number.
//!
//! Row key is the bit-reversed sequence number (big-endian u64). Bit reversal
//! is a bijection that maps the monotonically-increasing checkpoint_seq input
//! uniformly across the keyspace, so sequential writes land on random tablets
//! instead of funneling into the trailing one. The table is point-lookup only
//! (no range scans), so losing locality has no cost: writers and readers both
//! recompute `cp_seq.reverse_bits()` to build keys.

use anyhow::Result;
use bytes::Bytes;
use sui_types::crypto::AuthorityStrongQuorumSignInfo;
use sui_types::messages_checkpoint::{
    CheckpointContents, CheckpointSequenceNumber, CheckpointSummary,
};

use crate::CheckpointData;

pub mod col {
    pub const SUMMARY: &str = "s";
    pub const SIGNATURES: &str = "sg";
    pub const CONTENTS: &str = "c";
}

pub const NAME: &str = "checkpoints";

pub fn encode_key(sequence_number: CheckpointSequenceNumber) -> Vec<u8> {
    sequence_number.reverse_bits().to_be_bytes().to_vec()
}

pub fn encode(
    summary: &CheckpointSummary,
    signatures: &AuthorityStrongQuorumSignInfo,
    contents: &CheckpointContents,
) -> Result<[(&'static str, Bytes); 3]> {
    Ok([
        (col::SUMMARY, Bytes::from(bcs::to_bytes(summary)?)),
        (col::SIGNATURES, Bytes::from(bcs::to_bytes(signatures)?)),
        (col::CONTENTS, Bytes::from(bcs::to_bytes(contents)?)),
    ])
}

pub fn decode(row: &[(Bytes, Bytes)]) -> Result<CheckpointData> {
    let mut summary = None;
    let mut contents = None;
    let mut signatures = None;

    for (column, value) in row {
        match column.as_ref() {
            b"s" => summary = Some(bcs::from_bytes(value)?),
            b"c" => contents = Some(bcs::from_bytes(value)?),
            b"sg" => signatures = Some(bcs::from_bytes(value)?),
            _ => {}
        }
    }

    Ok(CheckpointData {
        summary,
        contents,
        signatures,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_key_is_bijective() {
        for cp_seq in [0u64, 1, 2, 42, 1_000_000, u64::MAX - 1, u64::MAX] {
            let k = encode_key(cp_seq);
            assert_eq!(k.len(), 8);
            let round_trip = u64::from_be_bytes(k.as_slice().try_into().unwrap()).reverse_bits();
            assert_eq!(round_trip, cp_seq);
        }
    }

    #[test]
    fn consecutive_inputs_land_on_opposite_halves() {
        // N and N+1 always differ in bit 0, which maps to the top bit of the
        // reversed key — so consecutive checkpoints always land on opposite
        // halves of the keyspace (top bit of first byte flips every step).
        for n in 0u64..1000 {
            let a_top = encode_key(n)[0] & 0x80;
            let b_top = encode_key(n + 1)[0] & 0x80;
            assert_ne!(a_top, b_top, "top bit of {n} and {} should differ", n + 1);
        }
    }

    #[test]
    fn first_byte_covers_full_range_for_1k_inputs() {
        let mut seen = std::collections::HashSet::new();
        for cp_seq in 0u64..1024 {
            seen.insert(encode_key(cp_seq)[0]);
        }
        // 1024 cp_seqs = 10 low bits varying; reversed, those become the top 10
        // bits of the key. The first byte is 8 of those — so we expect all 256
        // distinct values.
        assert_eq!(seen.len(), 256);
    }
}
