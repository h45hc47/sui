// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! BigTable Store implementation for sui-indexer-alt-framework.
//!
//! This implements the `Store` and `Connection` traits to allow the new framework
//! to use BigTable for watermark storage. Per-pipeline watermarks are stored in
//! the `watermark_alt` table.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use sui_indexer_alt_framework_store_traits::CommitterWatermark;
use sui_indexer_alt_framework_store_traits::ConcurrentConnection;
use sui_indexer_alt_framework_store_traits::Connection;
use sui_indexer_alt_framework_store_traits::InitWatermark;
use sui_indexer_alt_framework_store_traits::PrunerWatermark;
use sui_indexer_alt_framework_store_traits::ReaderWatermark;
use sui_indexer_alt_framework_store_traits::Store;

use crate::Watermark;
use crate::bigtable::client::BigTableClient;

mod bitmap_buffer;

pub use bitmap_buffer::BatchMessage;
pub use bitmap_buffer::BitmapBuffer;
pub use bitmap_buffer::BitmapIndexBatch;
pub use bitmap_buffer::BitmapIndexProcessor;
pub use bitmap_buffer::BitmapIndexValue;

/// A Store implementation backed by BigTable.
#[derive(Clone)]
pub struct BigTableStore {
    client: BigTableClient,
    /// Per-pipeline bitmap-index buffers. Populated lazily on the first
    /// `commit()` for each pipeline via
    /// [`BigTableConnection::bitmap_buffer`]. `set_committer_watermark`
    /// looks up the pipeline's buffer (if any) and flushes it to BigTable
    /// before persisting the new watermark.
    bitmap_buffers: Arc<RwLock<HashMap<&'static str, Arc<BitmapBuffer>>>>,
}

/// A connection to BigTable for watermark operations and data writes.
pub struct BigTableConnection<'a> {
    client: BigTableClient,
    bitmap_buffers: Arc<RwLock<HashMap<&'static str, Arc<BitmapBuffer>>>>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl BigTableStore {
    pub fn new(client: BigTableClient) -> Self {
        Self {
            client,
            bitmap_buffers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register pipeline `P`'s bitmap buffer. Called once per bitmap-index
    /// pipeline during indexer wiring (see `crate::lib`). Reads the
    /// persisted committer watermark to seed the buffer's `startup_tx_hi`
    /// so the buffer knows which bucket (if any) straddles the watermark
    /// and needs pre-restart state loaded on its first flush.
    ///
    /// Panics if called twice for the same pipeline — each pipeline gets
    /// exactly one buffer for the lifetime of the process.
    pub async fn register_bitmap_pipeline<P: BitmapIndexProcessor>(&self) -> Result<()> {
        let mut client = self.client.clone();
        let startup_tx_hi = client
            .get_pipeline_watermark(P::NAME)
            .await?
            .map(|w| w.tx_hi)
            .unwrap_or(0);
        let mut buffers = self.bitmap_buffers.write().unwrap();
        assert!(
            !buffers.contains_key(P::NAME),
            "bitmap pipeline {} already registered",
            P::NAME,
        );
        buffers.insert(
            P::NAME,
            Arc::new(BitmapBuffer::for_processor::<P>(startup_tx_hi)),
        );
        Ok(())
    }
}

impl BigTableConnection<'_> {
    /// Returns a mutable reference to the underlying BigTable client.
    pub fn client(&mut self) -> &mut BigTableClient {
        &mut self.client
    }

    /// Hand `msg` off to pipeline `P`'s bitmap buffer via its mpsc
    /// channel. The flush thread drains the channel and merges into
    /// canonical state inside
    /// [`Connection::set_committer_watermark`]. Panics if `P` wasn't
    /// registered via [`BigTableStore::register_bitmap_pipeline`].
    pub fn send_bitmap_batch<P: BitmapIndexProcessor>(&self, msg: BatchMessage) {
        let buffer = self
            .bitmap_buffers
            .read()
            .unwrap()
            .get(P::NAME)
            .cloned()
            .unwrap_or_else(|| panic!("bitmap pipeline {} not registered", P::NAME));
        buffer.send(msg);
    }
}

#[async_trait]
impl sui_indexer_alt_framework_store_traits::ConcurrentStore for BigTableStore {
    type ConcurrentConnection<'c> = BigTableConnection<'c>;
}

#[async_trait]
impl Store for BigTableStore {
    type Connection<'c> = BigTableConnection<'c>;

    async fn connect<'c>(&'c self) -> Result<Self::Connection<'c>> {
        Ok(BigTableConnection {
            client: self.client.clone(),
            bitmap_buffers: self.bitmap_buffers.clone(),
            _marker: std::marker::PhantomData,
        })
    }
}

#[async_trait]
impl Connection for BigTableConnection<'_> {
    async fn init_watermark(
        &mut self,
        pipeline_task: &str,
        _checkpoint_hi_inclusive: Option<u64>,
    ) -> Result<Option<InitWatermark>> {
        let watermark = self.committer_watermark(pipeline_task).await?;
        Ok(watermark.map(|w| InitWatermark {
            checkpoint_hi_inclusive: Some(w.checkpoint_hi_inclusive),
            reader_lo: None,
        }))
    }

    async fn accepts_chain_id(
        &mut self,
        _pipeline_task: &str,
        _chain_id: [u8; 32],
    ) -> Result<bool> {
        // TODO: Implement storing chain_id
        Ok(true)
    }

    async fn committer_watermark(
        &mut self,
        pipeline_task: &str,
    ) -> Result<Option<CommitterWatermark>> {
        Ok(self
            .client
            .get_pipeline_watermark(pipeline_task)
            .await?
            .map(Into::into))
    }

    async fn set_committer_watermark(
        &mut self,
        pipeline_task: &str,
        watermark: CommitterWatermark,
    ) -> Result<bool> {
        // If a bitmap-index buffer is registered for this pipeline, flush it
        // before the watermark advances. The framework retries
        // `set_committer_watermark` on `Err`, so a flush failure is recoverable
        // — buffer state is unchanged on retry.
        let buffer = self
            .bitmap_buffers
            .read()
            .unwrap()
            .get(pipeline_task)
            .cloned();
        if let Some(buffer) = buffer {
            buffer.flush_through(&mut self.client, &watermark).await?;
        }

        let pipeline_watermark: Watermark = watermark.into();
        self.client
            .set_pipeline_watermark(pipeline_task, &pipeline_watermark)
            .await?;
        Ok(true)
    }
}

#[async_trait]
impl ConcurrentConnection for BigTableConnection<'_> {
    async fn reader_watermark(&mut self, _pipeline: &str) -> Result<Option<ReaderWatermark>> {
        Ok(None)
    }

    async fn pruner_watermark(
        &mut self,
        _pipeline: &'static str,
        _delay: Duration,
    ) -> Result<Option<PrunerWatermark>> {
        Ok(None)
    }

    async fn set_reader_watermark(
        &mut self,
        _pipeline: &'static str,
        _reader_lo: u64,
    ) -> Result<bool> {
        Ok(false)
    }

    async fn set_pruner_watermark(
        &mut self,
        _pipeline: &'static str,
        _pruner_hi: u64,
    ) -> Result<bool> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::BigTableEmulator;
    use crate::testing::INSTANCE_ID;
    use crate::testing::create_tables;
    use crate::testing::require_bigtable_emulator;

    const PIPELINE: &str = "pipeline";
    const EPOCH_HI: u64 = 7;
    const CHECKPOINT_HI: u64 = 200;
    const TX_HI: u64 = 42;
    const TIMESTAMP_MS_HI: u64 = 99;

    /// Spawn a BigTable emulator and return a connected store.
    async fn store_conn() -> (BigTableEmulator, BigTableStore) {
        require_bigtable_emulator();
        let emulator = tokio::task::spawn_blocking(BigTableEmulator::start)
            .await
            .unwrap()
            .unwrap();
        create_tables(emulator.host(), INSTANCE_ID).await.unwrap();
        let client = BigTableClient::new_local(emulator.host().to_string(), INSTANCE_ID.into())
            .await
            .unwrap();
        (emulator, BigTableStore::new(client))
    }

    #[tokio::test]
    async fn test_init_watermark_returns_existing_on_conflict() {
        let (_emulator, store) = store_conn().await;
        let mut conn = store.connect().await.unwrap();

        let watermark = CommitterWatermark {
            epoch_hi_inclusive: EPOCH_HI,
            checkpoint_hi_inclusive: CHECKPOINT_HI,
            tx_hi: TX_HI,
            timestamp_ms_hi_inclusive: TIMESTAMP_MS_HI,
        };
        conn.set_committer_watermark(PIPELINE, watermark)
            .await
            .unwrap();

        // init must surface the existing committer watermark regardless of the input.
        let init = conn
            .init_watermark(PIPELINE, Some(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(init.checkpoint_hi_inclusive, Some(CHECKPOINT_HI));
        // BigTable has no trailing-edge / reader watermark concept.
        assert_eq!(init.reader_lo, None);
    }
}
