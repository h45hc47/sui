// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! BigTable Store implementation for sui-indexer-alt-framework.
//!
//! Implements the `Store`, `ConcurrentStore`, and `SequentialStore` traits.
//! Per-pipeline watermarks are stored in the `watermark_alt` table.
//!
//! ## Sequential transactions
//!
//! BigTable has no multi-row transaction, so [`SequentialStore::transaction`]
//! runs the closure inline and defers the watermark write until the closure
//! returns successfully. `set_committer_watermark` buffers its write on the
//! connection; the transaction impl flushes it to BigTable after the handler's
//! commit.
//!
//! This relies on the handler's writes being idempotent on replay — for the
//! bitmap pipelines that use this, OR-based bitmap writes with monotonic
//! cumulative state are idempotent under retry and under partial pre-crash
//! state (straddler-bucket load reconciles it).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::Result;
use anyhow::bail;
use async_trait::async_trait;
use scoped_futures::ScopedBoxFuture;
use sui_indexer_alt_framework_store_traits::CommitterWatermark;
use sui_indexer_alt_framework_store_traits::ConcurrentConnection;
use sui_indexer_alt_framework_store_traits::ConcurrentStore;
use sui_indexer_alt_framework_store_traits::Connection;
use sui_indexer_alt_framework_store_traits::InitWatermark;
use sui_indexer_alt_framework_store_traits::PrunerWatermark;
use sui_indexer_alt_framework_store_traits::ReaderWatermark;
use sui_indexer_alt_framework_store_traits::SequentialConnection;
use sui_indexer_alt_framework_store_traits::SequentialStore;
use sui_indexer_alt_framework_store_traits::Store;

use crate::Watermark;
use crate::bigtable::client::BigTableClient;

/// A Store implementation backed by BigTable.
#[derive(Clone)]
pub struct BigTableStore {
    client: BigTableClient,
    /// `tx_hi` observed per pipeline at startup (when the framework calls
    /// `init_watermark`). Handlers that need to reconcile pre-restart state
    /// (e.g. the bitmap pipelines, to lazy-load straddler-bucket rows) read
    /// this via [`BigTableConnection::startup_tx_hi`] on first commit.
    startup_tx_his: Arc<RwLock<HashMap<String, u64>>>,
}

/// A connection to BigTable for watermark operations and data writes.
///
/// While a [`SequentialStore::transaction`] is in flight,
/// `set_committer_watermark` buffers its write in `pending_watermark` instead
/// of hitting BigTable; the transaction impl flushes it after the handler's
/// commit returns successfully.
pub struct BigTableConnection<'a> {
    client: BigTableClient,
    startup_tx_his: Arc<RwLock<HashMap<String, u64>>>,
    pending_watermark: Option<(String, CommitterWatermark)>,
    /// `true` while running under `SequentialStore::transaction`.
    in_sequential_transaction: bool,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl BigTableStore {
    pub fn new(client: BigTableClient) -> Self {
        Self {
            client,
            startup_tx_his: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl BigTableConnection<'_> {
    /// Returns a mutable reference to the underlying BigTable client.
    pub fn client(&mut self) -> &mut BigTableClient {
        &mut self.client
    }

    /// Returns the watermark most recently staged by `set_committer_watermark`
    /// inside the current `SequentialStore::transaction`. Used by sequential
    /// handlers that need the about-to-be-persisted watermark in `commit()`.
    pub fn pending_watermark(&self) -> Option<CommitterWatermark> {
        self.pending_watermark.as_ref().map(|(_, w)| *w)
    }

    /// `tx_hi` observed for `pipeline` at startup (when the framework
    /// called `init_watermark`). Returns `0` for a fresh pipeline with no
    /// prior watermark.
    pub fn startup_tx_hi(&self, pipeline: &str) -> u64 {
        self.startup_tx_his
            .read()
            .unwrap()
            .get(pipeline)
            .copied()
            .unwrap_or(0)
    }
}

#[async_trait]
impl ConcurrentStore for BigTableStore {
    type ConcurrentConnection<'c> = BigTableConnection<'c>;
}

#[async_trait]
impl SequentialStore for BigTableStore {
    type SequentialConnection<'c> = BigTableConnection<'c>;

    async fn transaction<'a, R, F>(&self, f: F) -> Result<R>
    where
        R: Send + 'a,
        F: Send + 'a,
        F: for<'r> FnOnce(&'r mut Self::Connection<'_>) -> ScopedBoxFuture<'a, 'r, Result<R>>,
    {
        let mut conn = self.connect().await?;
        conn.in_sequential_transaction = true;
        let result = f(&mut conn).await?;
        // Closure returned `Ok` — now persist the staged watermark. If this
        // fails, the framework retries the whole closure; the handler's
        // write-and-merge is idempotent so retries converge.
        if let Some((pipeline, watermark)) = conn.pending_watermark.take() {
            let pw: Watermark = watermark.into();
            conn.client.set_pipeline_watermark(&pipeline, &pw).await?;
        }
        Ok(result)
    }
}

#[async_trait]
impl Store for BigTableStore {
    type Connection<'c> = BigTableConnection<'c>;

    async fn connect<'c>(&'c self) -> Result<Self::Connection<'c>> {
        Ok(BigTableConnection {
            client: self.client.clone(),
            startup_tx_his: self.startup_tx_his.clone(),
            pending_watermark: None,
            in_sequential_transaction: false,
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
        // Snapshot tx_hi so handlers can read it later via
        // `BigTableConnection::startup_tx_hi` without re-hitting BigTable.
        self.startup_tx_his
            .write()
            .unwrap()
            .insert(pipeline_task.to_string(), watermark.map_or(0, |w| w.tx_hi));
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
        if self.in_sequential_transaction {
            if let Some((prev, _)) = self.pending_watermark.as_ref()
                && prev != pipeline_task
            {
                bail!(
                    "set_committer_watermark called for '{pipeline_task}' \
                    inside a transaction that already staged '{prev}'"
                );
            }
            self.pending_watermark = Some((pipeline_task.to_string(), watermark));
            return Ok(true);
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

#[async_trait]
impl SequentialConnection for BigTableConnection<'_> {}

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
