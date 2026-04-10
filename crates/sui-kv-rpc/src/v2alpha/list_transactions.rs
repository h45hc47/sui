// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use bytes::Bytes;
use futures::StreamExt;
use futures::TryStreamExt;
use sui_kvstore::BigTableClient;
use sui_kvstore::KeyValueStoreReader;
use sui_rpc_api::ErrorReason;
use sui_rpc_api::RpcError;
use sui_rpc_api::proto::google::rpc::bad_request::FieldViolation;

use super::filter::transaction_filter_to_query;
use crate::PackageResolver;
use crate::proto::sui::rpc::kv::v2alpha::ListTransactionsRequest;
use crate::proto::sui::rpc::kv::v2alpha::ListTransactionsResponse;
use crate::proto::sui::rpc::kv::v2alpha::TransactionResult;
use crate::v2::get_transaction::fetch_object_map;
use crate::v2::get_transaction::needs_object_types;
use crate::v2::get_transaction::transaction_columns;
use crate::v2::get_transaction::transaction_to_response;
use crate::v2::get_transaction::validate_read_mask;

const DEFAULT_PAGE_SIZE: u32 = 50;
const MAX_PAGE_SIZE: u32 = 1000;

pub(crate) async fn list_transactions(
    mut client: BigTableClient,
    request: ListTransactionsRequest,
    resolver: &PackageResolver,
) -> Result<ListTransactionsResponse, RpcError> {
    let read_mask = validate_read_mask(request.read_mask)?;
    let page_size = request
        .page_size
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE) as usize;

    let cursor = request
        .page_token
        .as_ref()
        .map(decode_tx_page_token)
        .transpose()?;

    let tx_range = resolve_tx_range(
        &client,
        cursor,
        request.start_checkpoint,
        request.end_checkpoint,
    )
    .await?;

    if tx_range.is_empty() {
        return Ok(ListTransactionsResponse::default());
    }

    // Collect up to page_size + 1 candidate tx_sequence_numbers. If a filter is
    // set, evaluate the bitmap query; otherwise walk the tx range directly.
    // Bounding to page_size + 1 BEFORE chunking lets the bitmap scan stop
    // reading buckets as soon as we have enough candidates. List_transactions
    // is 1-to-1 (each tx_seq → one result), so this is an exact ceiling.
    let seqs: Vec<u64> = if let Some(filter) = &request.filter {
        let query = transaction_filter_to_query(filter)?;
        client
            .eval_bitmap_query_stream(query, tx_range.clone())
            .take(page_size + 1)
            .try_collect()
            .await?
    } else {
        tx_range.take(page_size + 1).collect()
    };

    // Two multi_gets: resolve tx_seqs → digests, then fetch the tx rows.
    // Output arrives in an unspecified order; sort by tx_seq for stable paging.
    let columns = transaction_columns(&read_mask);
    let mut page = client
        .get_transactions_for_seqs(seqs, Some(&columns))
        .await?;
    page.sort_by_key(|(seq, _, _)| *seq);

    let has_next = page.len() > page_size;
    page.truncate(page_size);

    if page.is_empty() {
        return Ok(ListTransactionsResponse::default());
    }

    // Fetch objects for type resolution if needed.
    let objects = if needs_object_types(&read_mask) {
        fetch_object_map(&mut client, page.iter().map(|(_, _, tx)| tx)).await?
    } else {
        HashMap::new()
    };

    let last_tx_seq = page.last().map(|(seq, _, _)| *seq);
    let mut transactions = Vec::with_capacity(page.len());
    for (tx_seq, checkpoint_seq, tx_data) in page {
        let executed = transaction_to_response(tx_data, &read_mask, &objects, resolver).await?;
        transactions.push(TransactionResult {
            cursor: Some(encode_tx_page_token(tx_seq)),
            checkpoint: Some(checkpoint_seq),
            transaction: Some(executed),
            ..Default::default()
        });
    }

    let next_page_token = if has_next {
        last_tx_seq.map(encode_tx_page_token)
    } else {
        None
    };

    Ok(ListTransactionsResponse {
        transactions,
        next_page_token,
        ..Default::default()
    })
}

/// Determine the tx_sequence_number range from request parameters.
///
/// Clamps `start_checkpoint` / `end_checkpoint` against the indexed watermark
/// before resolving so that out-of-range bounds produce an empty result rather
/// than an error on a missing checkpoint summary.
async fn resolve_tx_range(
    client: &BigTableClient,
    cursor: Option<u64>,
    start_checkpoint: Option<u64>,
    end_checkpoint: Option<u64>,
) -> Result<std::ops::Range<u64>, RpcError> {
    let mut wm_client = client.clone();
    let wm = wm_client
        .get_watermark()
        .await?
        .ok_or_else(|| RpcError::new(tonic::Code::Unavailable, "no watermark available"))?;
    let wm_hi_exclusive = wm.checkpoint_hi_inclusive + 1;

    let start_cp = start_checkpoint.unwrap_or(0).min(wm_hi_exclusive);
    let end_cp = end_checkpoint
        .unwrap_or(wm_hi_exclusive)
        .min(wm_hi_exclusive);
    if start_cp >= end_cp {
        return Ok(0..0);
    }

    let start_fut = {
        let mut client = client.clone();
        async move {
            if let Some(seq) = cursor {
                return Ok::<u64, RpcError>(seq + 1);
            }
            if start_cp == 0 {
                return Ok(0);
            }
            Ok(client
                .checkpoint_to_tx_range(start_cp..start_cp + 1)
                .await?
                .start)
        }
    };

    let end_fut = {
        let mut client = client.clone();
        async move { Ok::<u64, RpcError>(client.checkpoint_to_tx_range(0..end_cp).await?.end) }
    };

    let (start_tx, end_tx) = tokio::try_join!(start_fut, end_fut)?;
    if start_tx >= end_tx {
        return Ok(0..0);
    }
    Ok(start_tx..end_tx)
}

fn encode_tx_page_token(tx_seq: u64) -> Bytes {
    Bytes::from(tx_seq.to_be_bytes().to_vec())
}

fn decode_tx_page_token(token: &Bytes) -> Result<u64, RpcError> {
    let bytes: [u8; 8] = token.as_ref().try_into().map_err(|_| {
        FieldViolation::new("page_token")
            .with_description("invalid page_token: expected 8 bytes")
            .with_reason(ErrorReason::FieldInvalid)
    })?;
    Ok(u64::from_be_bytes(bytes))
}
