// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generic Roaring-bitmap inverted-index handler.
//!
//! Thin glue between `concurrent::Handler` and
//! [`BitmapBuffer`](crate::bigtable::store::BitmapBuffer). `batch()` OR's
//! incoming values into a per-task [`BitmapIndexBatch`]; `commit()`
//! `mem::take`s the batch and hands it off to the buffer's mpsc channel
//! via [`BigTableConnection::send_bitmap_batch`](crate::bigtable::store::BigTableConnection::send_bitmap_batch).
//! The actual BigTable write happens inside
//! `BigTableConnection::set_committer_watermark`, which drains the channel
//! into the canonical cumulative state and flushes. See
//! `bigtable/store/bitmap_buffer.rs` for correctness details.

use std::sync::Arc;

use async_trait::async_trait;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::concurrent::BatchStatus;
use sui_indexer_alt_framework::pipeline::concurrent::Handler;
use sui_indexer_alt_framework_store_traits::Store;
use sui_types::full_checkpoint_content::Checkpoint;

use crate::bigtable::store::BigTableStore;
use crate::bigtable::store::BitmapIndexBatch;
use crate::bigtable::store::BitmapIndexProcessor;
use crate::bigtable::store::BitmapIndexValue;
use crate::config::ConcurrentLayer;

/// Generic wrapper that implements `concurrent::Handler` for any
/// [`BitmapIndexProcessor`].
pub struct BitmapIndexHandler<P> {
    processor: P,
}

impl<P> BitmapIndexHandler<P>
where
    P: BitmapIndexProcessor + Send + Sync + 'static,
{
    pub(crate) fn new(processor: P, _config: &ConcurrentLayer) -> Self {
        Self { processor }
    }
}

#[async_trait]
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

#[async_trait]
impl<P> Handler for BitmapIndexHandler<P>
where
    P: BitmapIndexProcessor + Send + Sync + 'static,
{
    type Store = BigTableStore;
    type Batch = BitmapIndexBatch;

    fn batch(
        &self,
        batch: &mut Self::Batch,
        values: &mut std::vec::IntoIter<Self::Value>,
    ) -> BatchStatus {
        batch.extend(values);
        // The collector's size/timer bounds decide when to flush; a single
        // batch may span many checkpoints.
        BatchStatus::Pending
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        conn: &mut <Self::Store as Store>::Connection<'a>,
    ) -> anyhow::Result<usize> {
        let msg = batch.take();
        let n: usize = msg.rows.values().map(|r| r.bitmap.len() as usize).sum();
        conn.send_bitmap_batch::<P>(msg);
        Ok(n)
    }
}
