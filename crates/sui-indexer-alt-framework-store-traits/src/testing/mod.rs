// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared test suites for implementations of [`Connection`], [`ConcurrentConnection`], and
//! [`SequentialConnection`].
//!
//! A caller invokes [`store_tests!`] in their `#[cfg(test)] mod tests` and the macro generates a
//! union of trait-level tests collected across existing impls. Extension-trait tests (concurrent,
//! sequential) are opt-in via flags — `macro_rules!` cannot introspect trait impls on stable Rust,
//! so the caller must tell the macro which extensions its store supports.
//!
//! The macro bodies are thin `#[tokio::test]` wrappers that delegate to generic `pub async fn`
//! helpers in this module. Keeping the assertions in normal functions means `assert!` failures
//! report the helper's source line here, not the caller's `store_tests!{…}` invocation line.
//!
//! [`Connection`]: crate::Connection
//! [`ConcurrentConnection`]: crate::ConcurrentConnection
//! [`SequentialConnection`]: crate::SequentialConnection

use std::time::Duration;

use scoped_futures::ScopedFutureExt;

use crate::CommitterWatermark;
use crate::ConcurrentConnection;
use crate::ConcurrentStore;
use crate::Connection;
use crate::SequentialStore;
use crate::Store;

pub mod mock_store;

const PIPELINE: &str = "pipeline";
const EPOCH_HI: u64 = 7;
const CHECKPOINT_HI: u64 = 200;
const TX_HI: u64 = 42;
const TIMESTAMP_MS_HI: u64 = 99;
const READER_LO: u64 = 123;
const PRUNER_HI: u64 = 77;
const ONE_HOUR_MS: u64 = 3_600_000;

fn watermark() -> CommitterWatermark {
    CommitterWatermark {
        epoch_hi_inclusive: EPOCH_HI,
        checkpoint_hi_inclusive: CHECKPOINT_HI,
        tx_hi: TX_HI,
        timestamp_ms_hi_inclusive: TIMESTAMP_MS_HI,
    }
}

async fn concurrent_bootstrap<C: ConcurrentConnection>(conn: &mut C, checkpoint_hi_inclusive: u64) {
    conn.init_watermark(PIPELINE, None).await.unwrap();
    conn.set_committer_watermark(
        PIPELINE,
        CommitterWatermark {
            epoch_hi_inclusive: 0,
            checkpoint_hi_inclusive,
            tx_hi: 0,
            timestamp_ms_hi_inclusive: 0,
        },
    )
    .await
    .unwrap();
}

// =============================================================================
// Connection-trait test helpers
// =============================================================================

pub async fn init_watermark_fresh_without_checkpoint<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();
    let init = conn.init_watermark(PIPELINE, None).await.unwrap().unwrap();
    assert_eq!(init.checkpoint_hi_inclusive, None);
}

pub async fn init_watermark_fresh_with_checkpoint<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();
    let init = conn
        .init_watermark(PIPELINE, Some(CHECKPOINT_HI))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(init.checkpoint_hi_inclusive, Some(CHECKPOINT_HI));
}

pub async fn init_watermark_returns_existing_on_conflict<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();

    conn.init_watermark(PIPELINE, Some(CHECKPOINT_HI))
        .await
        .unwrap();
    conn.set_committer_watermark(PIPELINE, watermark())
        .await
        .unwrap();

    // Second init must surface the existing checkpoint, not the input.
    let second = conn
        .init_watermark(PIPELINE, Some(0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.checkpoint_hi_inclusive, Some(CHECKPOINT_HI));
}

pub async fn committer_watermark_initial_is_none<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();

    conn.init_watermark(PIPELINE, None).await.unwrap();
    assert!(conn.committer_watermark(PIPELINE).await.unwrap().is_none());
}

pub async fn committer_watermark_roundtrip<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();

    conn.init_watermark(PIPELINE, None).await.unwrap();
    assert!(
        conn.set_committer_watermark(PIPELINE, watermark())
            .await
            .unwrap()
    );

    let stored = conn.committer_watermark(PIPELINE).await.unwrap().unwrap();
    assert_eq!(stored.epoch_hi_inclusive, EPOCH_HI);
    assert_eq!(stored.checkpoint_hi_inclusive, CHECKPOINT_HI);
    assert_eq!(stored.tx_hi, TX_HI);
    assert_eq!(stored.timestamp_ms_hi_inclusive, TIMESTAMP_MS_HI);
}

pub async fn set_committer_watermark_advances<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();

    conn.init_watermark(PIPELINE, None).await.unwrap();

    let lower = CommitterWatermark {
        checkpoint_hi_inclusive: CHECKPOINT_HI / 2,
        ..watermark()
    };
    assert!(conn.set_committer_watermark(PIPELINE, lower).await.unwrap());
    assert!(
        conn.set_committer_watermark(PIPELINE, watermark())
            .await
            .unwrap()
    );

    let stored = conn.committer_watermark(PIPELINE).await.unwrap().unwrap();
    assert_eq!(stored.checkpoint_hi_inclusive, CHECKPOINT_HI);
}

pub async fn set_committer_watermark_rejects_regression<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();

    conn.init_watermark(PIPELINE, None).await.unwrap();
    assert!(
        conn.set_committer_watermark(PIPELINE, watermark())
            .await
            .unwrap()
    );

    let regressed = CommitterWatermark {
        checkpoint_hi_inclusive: CHECKPOINT_HI / 2,
        ..watermark()
    };
    assert!(
        !conn
            .set_committer_watermark(PIPELINE, regressed)
            .await
            .unwrap()
    );

    // Stored watermark is unchanged.
    let stored = conn.committer_watermark(PIPELINE).await.unwrap().unwrap();
    assert_eq!(stored.checkpoint_hi_inclusive, CHECKPOINT_HI);
}

pub async fn accepts_chain_id_first_call_writes_and_accepts<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();
    conn.init_watermark(PIPELINE, None).await.unwrap();
    assert!(conn.accepts_chain_id(PIPELINE, [1u8; 32]).await.unwrap());
}

pub async fn accepts_chain_id_matching_accepts<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();
    conn.init_watermark(PIPELINE, None).await.unwrap();
    let chain_id = [1u8; 32];
    assert!(conn.accepts_chain_id(PIPELINE, chain_id).await.unwrap());
    assert!(conn.accepts_chain_id(PIPELINE, chain_id).await.unwrap());
}

pub async fn accepts_chain_id_mismatching_rejects<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();
    conn.init_watermark(PIPELINE, None).await.unwrap();
    let chain_id_a = [1u8; 32];
    let chain_id_b = [2u8; 32];
    assert!(conn.accepts_chain_id(PIPELINE, chain_id_a).await.unwrap());
    assert!(!conn.accepts_chain_id(PIPELINE, chain_id_b).await.unwrap());
    // Originally stored chain_id is still accepted.
    assert!(conn.accepts_chain_id(PIPELINE, chain_id_a).await.unwrap());
}

pub async fn accepts_chain_id_distinct_pipelines<S: Store>(store: S) {
    let mut conn = store.connect().await.unwrap();
    conn.init_watermark("a", None).await.unwrap();
    conn.init_watermark("b", None).await.unwrap();
    let chain_id_a = [1u8; 32];
    let chain_id_b = [2u8; 32];
    assert!(conn.accepts_chain_id("a", chain_id_a).await.unwrap());
    assert!(conn.accepts_chain_id("b", chain_id_b).await.unwrap());
    assert!(!conn.accepts_chain_id("a", chain_id_b).await.unwrap());
}

// =============================================================================
// ConcurrentConnection-trait test helpers
// =============================================================================

pub async fn reader_watermark_roundtrip<S: ConcurrentStore>(store: S) {
    let mut conn = store.connect().await.unwrap();
    concurrent_bootstrap(&mut conn, CHECKPOINT_HI).await;

    let watermark = conn.reader_watermark(PIPELINE).await.unwrap().unwrap();
    assert_eq!(watermark.checkpoint_hi_inclusive, CHECKPOINT_HI);

    assert!(
        conn.set_reader_watermark(PIPELINE, READER_LO)
            .await
            .unwrap()
    );
    let watermark = conn.reader_watermark(PIPELINE).await.unwrap().unwrap();
    assert_eq!(watermark.reader_lo, READER_LO);
}

pub async fn pruner_watermark_wait_for_ms<S: ConcurrentStore>(store: S) {
    // Bootstrap anchors `pruner_timestamp` at wall-clock now, so querying with a
    // `delay` of ONE_HOUR_MS should return a wait close to one hour.
    let mut conn = store.connect().await.unwrap();
    concurrent_bootstrap(&mut conn, CHECKPOINT_HI).await;

    let watermark = conn
        .pruner_watermark(PIPELINE, Duration::from_millis(ONE_HOUR_MS))
        .await
        .unwrap()
        .unwrap();
    // Generous slack for slow CI.
    assert!(
        watermark.wait_for_ms > (ONE_HOUR_MS as i64 - 100_000)
            && watermark.wait_for_ms <= ONE_HOUR_MS as i64,
        "wait_for_ms = {}",
        watermark.wait_for_ms,
    );
}

pub async fn pruner_watermark_saturates_when_ready<S: ConcurrentStore>(store: S) {
    let mut conn = store.connect().await.unwrap();
    concurrent_bootstrap(&mut conn, CHECKPOINT_HI).await;

    let watermark = conn
        .pruner_watermark(PIPELINE, Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert!(
        watermark.wait_for_ms <= 0,
        "wait_for_ms = {}",
        watermark.wait_for_ms,
    );
}

pub async fn set_pruner_watermark_roundtrip<S: ConcurrentStore>(store: S) {
    let mut conn = store.connect().await.unwrap();
    concurrent_bootstrap(&mut conn, CHECKPOINT_HI).await;

    assert!(
        conn.set_pruner_watermark(PIPELINE, PRUNER_HI)
            .await
            .unwrap()
    );
    let watermark = conn
        .pruner_watermark(PIPELINE, Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(watermark.pruner_hi, PRUNER_HI);
}

pub async fn set_reader_watermark_rejects_stale<S: ConcurrentStore>(store: S) {
    let mut conn = store.connect().await.unwrap();
    concurrent_bootstrap(&mut conn, CHECKPOINT_HI).await;

    assert!(
        conn.set_reader_watermark(PIPELINE, READER_LO)
            .await
            .unwrap()
    );
    assert!(
        !conn
            .set_reader_watermark(PIPELINE, READER_LO)
            .await
            .unwrap(),
        "equal reader_lo must be rejected"
    );
    assert!(
        !conn
            .set_reader_watermark(PIPELINE, READER_LO - 1)
            .await
            .unwrap(),
        "lower reader_lo must be rejected"
    );
}

pub async fn set_pruner_watermark_rejects_stale<S: ConcurrentStore>(store: S) {
    let mut conn = store.connect().await.unwrap();
    concurrent_bootstrap(&mut conn, CHECKPOINT_HI).await;

    assert!(
        conn.set_pruner_watermark(PIPELINE, PRUNER_HI)
            .await
            .unwrap()
    );
    assert!(
        !conn
            .set_pruner_watermark(PIPELINE, PRUNER_HI)
            .await
            .unwrap(),
        "equal pruner_hi must be rejected"
    );
    assert!(
        !conn
            .set_pruner_watermark(PIPELINE, PRUNER_HI - 1)
            .await
            .unwrap(),
        "lower pruner_hi must be rejected"
    );
}

// =============================================================================
// SequentialConnection-trait test helper
// =============================================================================

pub async fn transaction_rolls_back_on_error<S: SequentialStore>(store: S) {
    // A transaction whose closure returns `Err` must roll back — no write from inside
    // the closure should be visible to a subsequent connection. This is the core
    // atomicity invariant of `SequentialStore::transaction`.
    {
        let mut conn = store.connect().await.unwrap();
        conn.init_watermark(PIPELINE, None).await.unwrap();
        assert!(
            conn.set_committer_watermark(PIPELINE, watermark())
                .await
                .unwrap()
        );
    }

    // Run a transaction that advances the watermark, then errors.
    let result: anyhow::Result<()> = store
        .transaction(|conn| {
            async move {
                let advanced = CommitterWatermark {
                    checkpoint_hi_inclusive: CHECKPOINT_HI + 1,
                    ..watermark()
                };
                let _ = conn.set_committer_watermark(PIPELINE, advanced).await?;
                Err(anyhow::anyhow!("rollback"))
            }
            .scope_boxed()
        })
        .await;
    assert!(result.is_err());

    // The inflight write must not have persisted.
    let mut conn = store.connect().await.unwrap();
    let stored = conn.committer_watermark(PIPELINE).await.unwrap().unwrap();
    assert_eq!(stored.checkpoint_hi_inclusive, CHECKPOINT_HI);
}

// =============================================================================
// `store_tests!` macro — emits thin `#[tokio::test]` wrappers that delegate to
// the helpers above.
// =============================================================================

/// Generate the union of `Connection`/`ConcurrentConnection`/`SequentialConnection` trait tests
/// against a caller-provided `setup` function.
///
/// `setup` is the path to an `async fn` that returns `(Guard, Store)`. The `Guard` is bound for
/// the test body's lifetime — callers that don't need a real guard return `()` as the first
/// element of the tuple.
///
/// # Flags
///
/// - `concurrent` — emit `ConcurrentConnection` tests (opt in when the store implements it).
/// - `sequential` — emit `SequentialConnection` tests (opt in when the store implements it).
///
/// # Example
///
/// ```ignore
/// async fn make_store() -> ((), MyStore) { ((), MyStore::new()) }
///
/// sui_indexer_alt_framework_store_traits::store_tests! {
///     setup: make_store,
///     concurrent,
///     sequential,
/// }
/// ```
#[macro_export]
macro_rules! store_tests {
    (setup: $setup:path $(,)?) => {
        mod store_tests {
            use super::*;
            $crate::__store_tests_emit_connection!(setup: $setup);
        }
    };
    (setup: $setup:path, concurrent $(,)?) => {
        mod store_tests {
            use super::*;
            $crate::__store_tests_emit_connection!(setup: $setup);
            $crate::__store_tests_emit_concurrent!(setup: $setup);
        }
    };
    (setup: $setup:path, sequential $(,)?) => {
        mod store_tests {
            use super::*;
            $crate::__store_tests_emit_connection!(setup: $setup);
            $crate::__store_tests_emit_sequential!(setup: $setup);
        }
    };
    (setup: $setup:path, concurrent, sequential $(,)?) => {
        mod store_tests {
            use super::*;
            $crate::__store_tests_emit_connection!(setup: $setup);
            $crate::__store_tests_emit_concurrent!(setup: $setup);
            $crate::__store_tests_emit_sequential!(setup: $setup);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __store_tests_emit_connection {
    (setup: $setup:path) => {
        mod connection_tests {
            use super::*;

            #[::tokio::test]
            async fn init_watermark_fresh_without_checkpoint() {
                let (_guard, store) = $setup().await;
                $crate::testing::init_watermark_fresh_without_checkpoint(store).await;
            }

            #[::tokio::test]
            async fn init_watermark_fresh_with_checkpoint() {
                let (_guard, store) = $setup().await;
                $crate::testing::init_watermark_fresh_with_checkpoint(store).await;
            }

            #[::tokio::test]
            async fn init_watermark_returns_existing_on_conflict() {
                let (_guard, store) = $setup().await;
                $crate::testing::init_watermark_returns_existing_on_conflict(store).await;
            }

            #[::tokio::test]
            async fn committer_watermark_initial_is_none() {
                let (_guard, store) = $setup().await;
                $crate::testing::committer_watermark_initial_is_none(store).await;
            }

            #[::tokio::test]
            async fn committer_watermark_roundtrip() {
                let (_guard, store) = $setup().await;
                $crate::testing::committer_watermark_roundtrip(store).await;
            }

            #[::tokio::test]
            async fn set_committer_watermark_advances() {
                let (_guard, store) = $setup().await;
                $crate::testing::set_committer_watermark_advances(store).await;
            }

            #[::tokio::test]
            async fn set_committer_watermark_rejects_regression() {
                let (_guard, store) = $setup().await;
                $crate::testing::set_committer_watermark_rejects_regression(store).await;
            }

            #[::tokio::test]
            async fn accepts_chain_id_first_call_writes_and_accepts() {
                let (_guard, store) = $setup().await;
                $crate::testing::accepts_chain_id_first_call_writes_and_accepts(store).await;
            }

            #[::tokio::test]
            async fn accepts_chain_id_matching_accepts() {
                let (_guard, store) = $setup().await;
                $crate::testing::accepts_chain_id_matching_accepts(store).await;
            }

            #[::tokio::test]
            async fn accepts_chain_id_mismatching_rejects() {
                let (_guard, store) = $setup().await;
                $crate::testing::accepts_chain_id_mismatching_rejects(store).await;
            }

            #[::tokio::test]
            async fn accepts_chain_id_distinct_pipelines() {
                let (_guard, store) = $setup().await;
                $crate::testing::accepts_chain_id_distinct_pipelines(store).await;
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __store_tests_emit_concurrent {
    (setup: $setup:path) => {
        mod concurrent_connection_tests {
            use super::*;

            #[::tokio::test]
            async fn reader_watermark_roundtrip() {
                let (_guard, store) = $setup().await;
                $crate::testing::reader_watermark_roundtrip(store).await;
            }

            #[::tokio::test]
            async fn pruner_watermark_wait_for_ms() {
                let (_guard, store) = $setup().await;
                $crate::testing::pruner_watermark_wait_for_ms(store).await;
            }

            #[::tokio::test]
            async fn pruner_watermark_saturates_when_ready() {
                let (_guard, store) = $setup().await;
                $crate::testing::pruner_watermark_saturates_when_ready(store).await;
            }

            #[::tokio::test]
            async fn set_pruner_watermark_roundtrip() {
                let (_guard, store) = $setup().await;
                $crate::testing::set_pruner_watermark_roundtrip(store).await;
            }

            #[::tokio::test]
            async fn set_reader_watermark_rejects_stale() {
                let (_guard, store) = $setup().await;
                $crate::testing::set_reader_watermark_rejects_stale(store).await;
            }

            #[::tokio::test]
            async fn set_pruner_watermark_rejects_stale() {
                let (_guard, store) = $setup().await;
                $crate::testing::set_pruner_watermark_rejects_stale(store).await;
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __store_tests_emit_sequential {
    (setup: $setup:path) => {
        mod sequential_connection_tests {
            use super::*;

            #[::tokio::test]
            async fn transaction_rolls_back_on_error() {
                let (_guard, store) = $setup().await;
                $crate::testing::transaction_rolls_back_on_error(store).await;
            }
        }
    };
}

#[cfg(test)]
mod sanity_tests {
    //! Exercise every macro arm against `MockStore` to prove the macros compile and the test
    //! bodies behave correctly — without requiring the framework crate.

    use super::mock_store::MockStore;

    async fn make_store() -> ((), MockStore) {
        ((), MockStore::default())
    }

    mod all_traits {
        use super::make_store;
        crate::store_tests! {
            setup: make_store,
            concurrent,
            sequential,
        }
    }

    mod connection_only {
        use super::make_store;
        crate::store_tests! {
            setup: make_store,
        }
    }
}
