// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Per-commit accumulator for [`super::BitmapIndexHandler`].
//!
//! Values arriving via `Handler::batch()` are OR'd into `BitmapIndexBatch`
//! row-by-row. On `Handler::commit()`, the contents are merged (by reference)
//! into the handler's accumulated state. The sequential framework retains
//! `&batch` on failure; on success it resets `batch` to default. Since
//! `AccumulatedState::merge_batch` is idempotent under OR, a retried commit
//! is safe.

use std::collections::HashMap;

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::handlers::bitmap::BitmapIndexValue;

/// Per-commit accumulator. Not shared across threads — the sequential
/// committer task owns it.
#[derive(Default)]
pub struct BitmapIndexBatch {
    rows: HashMap<Bytes, BitmapIndexValue>,
}

impl BitmapIndexBatch {
    /// Merge a batch of values into this accumulator. Called by the
    /// handler's `batch()`, once per checkpoint — every `v` in `values`
    /// shares the same `max_cp` / `max_ts_ms`. Each value already carries a
    /// row-sized bitmap from the processor; here we just OR it into the
    /// matching accumulator row.
    pub fn extend(&mut self, values: impl IntoIterator<Item = BitmapIndexValue>) {
        for v in values {
            let row = self
                .rows
                .entry(v.row_key.clone())
                .or_insert(BitmapIndexValue {
                    row_key: v.row_key,
                    bucket_id: v.bucket_id,
                    bitmap: RoaringBitmap::new(),
                    max_cp: 0,
                    max_ts_ms: 0,
                });
            row.bitmap |= v.bitmap;
            if v.max_cp > row.max_cp {
                row.max_cp = v.max_cp;
                row.max_ts_ms = v.max_ts_ms;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn rows(&self) -> &HashMap<Bytes, BitmapIndexValue> {
        &self.rows
    }
}
