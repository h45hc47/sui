// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generic Roaring-bitmap inverted-index handler.
//!
//! Wires a [`BitmapIndexProcessor`] onto the framework's `sequential::Handler`
//! trait. `batch()` accumulates incoming values into a per-commit
//! [`BitmapIndexBatch`]; `commit()` merges the batch into the handler's
//! accumulated state and flushes dirty rows to BigTable. The framework wraps
//! the commit in a `SequentialStore::transaction` that defers the watermark
//! write until after `commit()` returns successfully (see
//! [`crate::bigtable::store`] for the deferred-watermark implementation).

use std::sync::Arc;

use async_trait::async_trait;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::sequential::Handler;
use sui_indexer_alt_framework_store_traits::Store;
use sui_types::full_checkpoint_content::Checkpoint;
use tokio::sync::Mutex;

use crate::bigtable::store::BigTableStore;
use crate::config::ConcurrentLayer;
use crate::handlers::bitmap::BitmapIndexProcessor;
use crate::handlers::bitmap::BitmapIndexValue;
use crate::handlers::bitmap::accumulated::AccumulatedState;
use crate::handlers::bitmap::batch::BitmapIndexBatch;
use crate::rate_limiter::CompositeRateLimiter;

/// Generic wrapper that implements `sequential::Handler` for any
/// [`BitmapIndexProcessor`].
///
/// The accumulated state is built lazily on the first `commit()` — at that
/// point the store has seen `init_watermark` for this pipeline and cached
/// `startup_tx_hi`, which the connection exposes.
pub struct BitmapIndexHandler<P> {
    processor: P,
    rate_limiter: Arc<CompositeRateLimiter>,
    accumulated: Arc<Mutex<Option<AccumulatedState>>>,
}

impl<P> BitmapIndexHandler<P>
where
    P: BitmapIndexProcessor + Send + Sync + 'static,
{
    pub(crate) fn new(
        processor: P,
        _config: &ConcurrentLayer,
        rate_limiter: Arc<CompositeRateLimiter>,
    ) -> Self {
        Self {
            processor,
            rate_limiter,
            accumulated: Arc::new(Mutex::new(None)),
        }
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

    // Bitmap writes are amortized over many checkpoints (one dirty row is
    // written per commit regardless of how many checkpoints contributed),
    // so bigger batches are cheaper per checkpoint.
    const MAX_BATCH_CHECKPOINTS: usize = 5_000;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        batch.extend(values);
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        conn: &mut <Self::Store as Store>::Connection<'a>,
    ) -> anyhow::Result<usize> {
        if batch.is_empty() {
            return Ok(0);
        }

        // The framework's `SequentialStore::transaction` has already called
        // `conn.set_committer_watermark(...)`, which buffers the watermark
        // on the connection. The actual BigTable write is deferred until
        // after this method returns successfully.
        let watermark = conn
            .pending_watermark()
            .expect("set_committer_watermark must be called before handler.commit");

        let mut state = self.accumulated.lock().await;
        let state = state.get_or_insert_with(|| {
            AccumulatedState::for_processor::<P>(conn.startup_tx_hi(P::NAME))
        });
        state.merge_batch(batch);
        state
            .flush(conn.client(), &watermark, &self.rate_limiter)
            .await
    }
}
