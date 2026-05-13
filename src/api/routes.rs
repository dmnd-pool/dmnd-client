use super::{utils::get_cpu_and_memory_usage, AppState};
use crate::config::Configuration;
use crate::proxy_state::ProxyState;
use crate::valid_job_tracker::FoundValidJob;
use axum::{
    extract::{Path, Query, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
#[cfg(test)]
use bitcoin::hashes::Hash;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

pub struct Api {}

impl Api {
    // Retrieves connected donwnstreams stats
    pub async fn get_downstream_stats(State(state): State<AppState>) -> impl IntoResponse {
        match state.stats_sender.collect_stats().await {
            Ok(stats) => (StatusCode::OK, Json(APIResponse::success(Some(stats)))),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(APIResponse::error(Some(format!(
                    "Failed to collect stats: {e}"
                )))),
            ),
        }
    }

    // Retrieves system stats (CPU and memory usage)
    pub async fn system_stats() -> impl IntoResponse {
        let (cpu, memory) = get_cpu_and_memory_usage().await;
        let cpu_usgae = format!("{cpu:.3}");
        let data = serde_json::json!({"cpu_usage_%": cpu_usgae, "memory_usage_bytes": memory});
        Json(APIResponse::success(Some(data)))
    }

    // Returns aggregate stats of all downstream devices
    pub async fn get_aggregate_stats(State(state): State<AppState>) -> impl IntoResponse {
        let stats = match state.stats_sender.collect_stats().await {
            Ok(stats) => stats,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(APIResponse::error(Some(format!(
                        "Failed to collect stats: {e}"
                    )))),
                );
            }
        };
        let mut total_connected_device = 0;
        let mut total_accepted_shares = 0;
        let mut total_rejected_shares = 0;
        let mut total_hashrate = 0.0;
        let mut total_diff = 0.0;
        for (_, downstream) in stats {
            total_connected_device += 1;
            total_accepted_shares += downstream.accepted_shares;
            total_rejected_shares += downstream.rejected_shares;
            total_hashrate += downstream.hashrate as f64;
            total_diff += downstream.current_difficulty as f64
        }
        let result = AggregateStates {
            total_connected_device,
            aggregate_hashrate: total_hashrate,
            aggregate_accepted_shares: total_accepted_shares,
            aggregate_rejected_shares: total_rejected_shares,
            aggregate_diff: total_diff,
        };
        (StatusCode::OK, Json(APIResponse::success(Some(result))))
    }

    pub async fn get_session_timing() -> impl IntoResponse {
        (
            StatusCode::OK,
            Json(APIResponse::success(Some(crate::debug_timing::snapshot()))),
        )
    }

    // Retrieves the current pool information
    pub async fn get_pool_info(State(state): State<AppState>) -> impl IntoResponse {
        let current_pool_address = state.router.current_pool;
        let latency = *state.router.latency_rx.borrow();

        match (current_pool_address, latency) {
            (Some(address), Some(latency)) => {
                let response_data = serde_json::json!({
                    "address": address.to_string(),
                    "latency": latency.as_millis().to_string()
                });
                (
                    StatusCode::OK,
                    Json(APIResponse::success(Some(response_data))),
                )
            }
            (_, _) => (
                StatusCode::NOT_FOUND,
                Json(APIResponse::error(Some(
                    "Pool information unavailable".to_string(),
                ))),
            ),
        }
    }

    // Returns the status of the Proxy
    pub async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
        if state.downstream_handoff.is_closed() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::error(Some(
                    "Overloaded: translator handoff channel is closed".to_string(),
                ))),
            );
        }

        if state.downstream_handoff.capacity() == 0 {
            let max_capacity = state.downstream_handoff.max_capacity();
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::error(Some(format!(
                    "Overloaded: translator handoff channel queue is full (0/{max_capacity} slots available)"
                )))),
            );
        }

        if let Some(max_active_downstreams) = Configuration::max_active_downstreams() {
            if let Ok(stats) = state.stats_sender.collect_stats().await {
                let active_downstreams = stats.len();
                if active_downstreams >= max_active_downstreams {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(APIResponse::error(Some(format!(
                            "Overloaded: active downstreams {active_downstreams}/{max_active_downstreams}"
                        )))),
                    );
                }
            }
        }

        match ProxyState::is_proxy_down() {
            (false, None) => (
                StatusCode::OK,
                Json(APIResponse::success(Some("Proxy OK".to_string()))),
            ),
            (true, Some(states)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::error(Some(states))),
            ),
            _ => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::error(Some("Unknown proxy state".to_string()))),
            ),
        }
    }

    pub async fn send_tx_to_bitcoind(
        State(state): State<AppState>,
        headers: HeaderMap,
        Path(tx): Path<String>,
    ) -> impl IntoResponse {
        let Some(prioritizing_txs) = state.prioritizing_txs.as_ref() else {
            warn!("PRIORITIZING TXS NOT ENABLED");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::<String>::error(Some(
                    "PRIORITIZING TXS NOT ENABLED".to_string(),
                ))),
            );
        };

        if !is_authorized_for_tx_prioritization(&headers, &prioritizing_txs.api_tx_token) {
            warn!("unauthorized tx prioritization request");
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::<String>::error(Some(
                    "Unauthorized".to_string(),
                ))),
            );
        }

        match prioritizing_txs.rpc.submit_transaction(&tx).await {
            Ok(txid) => {
                info!("transaction sent to bitcoind: {txid}");
                (StatusCode::OK, Json(APIResponse::success(Some(txid))))
            }
            Err(e) => {
                error!("Failed to send transaction to bitcoind: {e}");
                (
                    e.status_code(),
                    Json(APIResponse::error(Some(e.to_string()))),
                )
            }
        }
    }

    pub async fn get_prioritized_transactions(
        State(state): State<AppState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        let Some(prioritizing_txs) = state.prioritizing_txs.as_ref() else {
            warn!("PRIORITIZING TXS NOT ENABLED");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::<PrioritizedTransactions>::error(Some(
                    "PRIORITIZING TXS NOT ENABLED".to_string(),
                ))),
            );
        };

        if !is_authorized_for_tx_prioritization(&headers, &prioritizing_txs.api_tx_token) {
            warn!("unauthorized prioritized txs request");
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::<PrioritizedTransactions>::error(Some(
                    "Unauthorized".to_string(),
                ))),
            );
        }

        let mut txids = crate::prioritized_transactions::snapshot();
        txids.sort();
        let response = PrioritizedTransactions {
            count: txids.len(),
            txids,
        };

        (StatusCode::OK, Json(APIResponse::success(Some(response))))
    }

    pub async fn queue_coinbase_op_return(
        State(state): State<AppState>,
        Json(request): Json<QueueCoinbaseOpReturnRequest>,
    ) -> impl IntoResponse {
        let Some(secret) = state.api_secret.as_deref() else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::<serde_json::Value>::error(Some(
                    "OP_RETURN API is disabled: API_SECRET is not configured".to_string(),
                ))),
            )
                .into_response();
        };

        if request.secret != secret {
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::<serde_json::Value>::error(Some(
                    "Invalid API secret".to_string(),
                ))),
            )
                .into_response();
        }

        match state
            .op_return_injector
            .queue_from_hex_with_rsk_target(&request.data_hex, request.rsk_target_hex.as_deref())
            .await
        {
            Ok(queued) => {
                info!(
                    "Set desired OP_RETURN for JD templates (payload={} bytes, txout={} bytes, replaced_pending={})",
                    queued.payload_len_bytes,
                    queued.tx_out_len_bytes,
                    queued.replaced_pending
                );
                (
                    StatusCode::ACCEPTED,
                    Json(APIResponse::success(Some(QueueCoinbaseOpReturnResponse {
                        payload_len_bytes: queued.payload_len_bytes,
                        tx_out_len_bytes: queued.tx_out_len_bytes,
                        replaced_pending: queued.replaced_pending,
                    }))),
                )
                    .into_response()
            }
            Err(error) => (
                StatusCode::BAD_REQUEST,
                Json(APIResponse::<serde_json::Value>::error(Some(
                    error.to_string(),
                ))),
            )
                .into_response(),
        }
    }

    pub async fn poll_found_valid_job(
        State(state): State<AppState>,
        Query(query): Query<PollFoundValidJobQuery>,
    ) -> impl IntoResponse {
        let Some(secret) = state.api_secret.as_deref() else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::<serde_json::Value>::error(Some(
                    "Merge-mining polling API is disabled: API_SECRET is not configured"
                        .to_string(),
                ))),
            )
                .into_response();
        };

        if query.secret != secret {
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::<serde_json::Value>::error(Some(
                    "Invalid API secret".to_string(),
                ))),
            )
                .into_response();
        }

        match state.valid_job_tracker.take_found_job() {
            Ok(Some(found_job)) => {
                (StatusCode::OK, Json(APIResponse::success(Some(found_job)))).into_response()
            }
            Ok(None) => (
                StatusCode::OK,
                Json(APIResponse::<FoundValidJob>::success(None)),
            )
                .into_response(),
            Err(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(APIResponse::<serde_json::Value>::error(Some(
                    error.to_string(),
                ))),
            )
                .into_response(),
        }
    }
}

fn is_authorized_for_tx_prioritization(headers: &HeaderMap, expected_token: &str) -> bool {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == expected_token)
}

#[derive(Serialize)]
struct AggregateStates {
    total_connected_device: u32,
    aggregate_hashrate: f64, // f64 is used here to avoid overflow
    aggregate_accepted_shares: u64,
    aggregate_rejected_shares: u64,
    aggregate_diff: f64,
}

#[derive(Serialize)]
struct PrioritizedTransactions {
    count: usize,
    txids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct APIResponse<T> {
    success: bool,
    message: Option<String>,
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
pub(super) struct QueueCoinbaseOpReturnRequest {
    secret: String,
    data_hex: String,
    #[serde(default)]
    rsk_target_hex: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct PollFoundValidJobQuery {
    secret: String,
}

#[derive(Debug, Serialize)]
struct QueueCoinbaseOpReturnResponse {
    payload_len_bytes: usize,
    tx_out_len_bytes: usize,
    replaced_pending: bool,
}

impl<T: Serialize> APIResponse<T> {
    fn success(data: Option<T>) -> Self {
        APIResponse {
            success: true,
            message: None,
            data,
        }
    }

    fn error(message: Option<String>) -> Self {
        APIResponse {
            success: false,
            message,
            data: None,
        }
    }
}

#[tokio::test]
async fn health_check_reports_full_translator_handoff() {
    use axum::extract::State;
    use axum::response::IntoResponse;
    use std::{net::IpAddr, time::Instant};
    use tokio::sync::mpsc;

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let (send_to_upstream, recv_from_downstream) = mpsc::channel(1);

    handoff_tx
        .try_send(crate::DownstreamConnection {
            send_to_downstream: send_to_upstream,
            recv_from_downstream,
            address: IpAddr::from([127, 0, 0, 1]),
            accepted_at: Instant::now(),
        })
        .expect("test handoff queue should accept first item");

    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                "http://127.0.0.1:8332".to_string(),
                "user".to_string(),
                "password".to_string(),
                100_000_000,
            )),
            api_tx_token: "api-token".to_string(),
        }),
        op_return_injector: crate::op_return_injector::OpReturnInjector::default(),
        valid_job_tracker: crate::valid_job_tracker::ValidJobTracker::default(),
        api_secret: None,
    };

    let response = Api::health_check(State(state)).await.into_response();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn send_tx_reports_unavailable_when_rpc_is_disabled() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use tokio::sync::mpsc;

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: None,
        op_return_injector: crate::op_return_injector::OpReturnInjector::default(),
        valid_job_tracker: crate::valid_job_tracker::ValidJobTracker::default(),
        api_secret: None,
    };

    let response = Api::send_tx_to_bitcoind(
        State(state),
        axum::http::HeaderMap::new(),
        Path("00".to_string()),
    )
    .await
    .into_response();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn send_tx_rejects_missing_api_tx_token_header() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use tokio::sync::mpsc;

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                "http://127.0.0.1:8332".to_string(),
                "user".to_string(),
                "password".to_string(),
                100_000_000,
            )),
            api_tx_token: "api-token".to_string(),
        }),
        op_return_injector: crate::op_return_injector::OpReturnInjector::default(),
        valid_job_tracker: crate::valid_job_tracker::ValidJobTracker::default(),
        api_secret: None,
    };

    let response = Api::send_tx_to_bitcoind(
        State(state),
        axum::http::HeaderMap::new(),
        Path("00".to_string()),
    )
    .await
    .into_response();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn get_prioritized_transactions_returns_snapshot() {
    use axum::body::to_bytes;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use tokio::sync::mpsc;

    crate::prioritized_transactions::record("routes-test-prioritized-a");
    crate::prioritized_transactions::record("routes-test-prioritized-b");

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                "http://127.0.0.1:8332".to_string(),
                "user".to_string(),
                "password".to_string(),
                100_000_000,
            )),
            api_tx_token: "api-token".to_string(),
        }),
        op_return_injector: crate::op_return_injector::OpReturnInjector::default(),
        valid_job_tracker: crate::valid_job_tracker::ValidJobTracker::default(),
        api_secret: None,
    };

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    let response = Api::get_prioritized_transactions(State(state), headers)
        .await
        .into_response();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let txids = body["data"]["txids"].as_array().unwrap();

    assert_eq!(body["success"], true);
    assert!(txids
        .iter()
        .any(|txid| txid.as_str() == Some("routes-test-prioritized-a")));
    assert!(txids
        .iter()
        .any(|txid| txid.as_str() == Some("routes-test-prioritized-b")));
}

#[test]
fn tx_prioritization_auth_accepts_matching_bearer_token() {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    assert!(is_authorized_for_tx_prioritization(&headers, "api-token"));
}

#[tokio::test]
async fn queue_coinbase_op_return_rejects_invalid_secret() {
    use axum::extract::State;
    use axum::response::IntoResponse;
    use tokio::sync::mpsc;

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let injector = crate::op_return_injector::OpReturnInjector::default();
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: None,
        op_return_injector: injector.clone(),
        valid_job_tracker: crate::valid_job_tracker::ValidJobTracker::default(),
        api_secret: Some("topsecret".to_string()),
    };

    let response = Api::queue_coinbase_op_return(
        State(state),
        Json(QueueCoinbaseOpReturnRequest {
            secret: "wrong".to_string(),
            data_hex: "aa".to_string(),
            rsk_target_hex: None,
        }),
    )
    .await
    .into_response();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let mut template = make_empty_template_for_tests();
    let applied = injector
        .apply_pending_to_template(&mut template)
        .await
        .expect("apply should succeed");
    assert!(applied.is_none(), "invalid secret must not queue payload");
}

#[tokio::test]
async fn queue_coinbase_op_return_accepts_valid_secret() {
    use axum::extract::State;
    use axum::response::IntoResponse;
    use tokio::sync::mpsc;

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let injector = crate::op_return_injector::OpReturnInjector::default();
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: None,
        op_return_injector: injector.clone(),
        valid_job_tracker: crate::valid_job_tracker::ValidJobTracker::default(),
        api_secret: Some("topsecret".to_string()),
    };

    let response = Api::queue_coinbase_op_return(
        State(state),
        Json(QueueCoinbaseOpReturnRequest {
            secret: "topsecret".to_string(),
            data_hex: "deadbeef".to_string(),
            rsk_target_hex: None,
        }),
    )
    .await
    .into_response();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let mut template = make_empty_template_for_tests();
    let applied = injector
        .apply_pending_to_template(&mut template)
        .await
        .expect("apply should succeed");
    assert!(applied.is_some(), "valid secret should queue payload");
    assert_eq!(template.coinbase_tx_outputs_count, 1);
}

#[tokio::test]
async fn poll_found_valid_job_returns_recorded_event() {
    use axum::response::IntoResponse;
    use axum::{
        body::to_bytes,
        extract::{Query, State},
    };
    use bitcoin::{consensus::serialize, hex::FromHex};
    use tokio::sync::mpsc;

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let op_return_injector = crate::op_return_injector::OpReturnInjector::default();
    let valid_job_tracker =
        crate::valid_job_tracker::ValidJobTracker::new(op_return_injector.clone());
    let payload_hex = "52534b424c4f434b3adeadbeef";
    let payload_bytes = Vec::from_hex(payload_hex).expect("payload should decode");
    let mut template = make_empty_template_for_tests();
    template.template_id = 7;
    op_return_injector
        .queue_from_hex(payload_hex)
        .await
        .expect("payload should queue");
    op_return_injector
        .apply_pending_to_template(&mut template)
        .await
        .expect("payload should apply")
        .expect("template should receive payload");
    let coinbase_tx = serialize(&make_coinbase_tx_for_found_job_tests(&payload_bytes));
    let n_bits = 0x207f_ffff;
    let header_nonce = find_pow_valid_nonce_for_found_job_tests(
        [0x11; 32],
        n_bits,
        8,
        9,
        &bitcoin::consensus::deserialize::<bitcoin::Transaction>(&coinbase_tx)
            .expect("coinbase bytes should decode"),
    );
    valid_job_tracker
        .cache_template_context(7, [0x11; 32], n_bits, vec![], 1)
        .expect("tracker should cache template context");
    let found_job = valid_job_tracker
        .record_found_job(7, 8, 9, header_nonce, &coinbase_tx)
        .expect("tracker should record event")
        .expect("template context should exist");
    crate::valid_job_tracker::validate_found_job_against_rskj_contract_for_tests(&found_job, None)
        .expect("single-tx found job should satisfy the bridge/RskJ contract");

    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: None,
        op_return_injector,
        valid_job_tracker,
        api_secret: Some("topsecret".to_string()),
    };

    let response = Api::poll_found_valid_job(
        State(state),
        Query(PollFoundValidJobQuery {
            secret: "topsecret".to_string(),
        }),
    )
    .await
    .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should be readable");
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("response body should be valid json");

    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["template_id"], 7);
    assert_eq!(json["data"]["version"], 8);
    assert_eq!(json["data"]["header_timestamp"], 9);
    assert_eq!(json["data"]["header_nonce"], header_nonce);
    assert_eq!(
        json["data"]["bitcoin_block_hash_hex"],
        found_job.bitcoin_block_hash_hex
    );
    assert_eq!(json["data"]["block_header_hex"], found_job.block_header_hex);
    assert_eq!(json["data"]["coinbase_tx_hex"], found_job.coinbase_tx_hex);
    assert_eq!(json["data"]["merkle_hashes_hex"], serde_json::json!([]));
    assert_eq!(json["data"]["block_tx_count"], 1);
    assert_eq!(json["data"]["op_return_payload_hex"], payload_hex);
    let returned_coinbase =
        Vec::<u8>::from_hex(json["data"]["coinbase_tx_hex"].as_str().unwrap()).unwrap();
    let returned_coinbase: bitcoin::Transaction =
        bitcoin::consensus::deserialize(&returned_coinbase).unwrap();
    assert!(
        returned_coinbase.input[0].witness.is_empty(),
        "API must return witness-stripped coinbase bytes for RskJ compatibility"
    );
    assert!(
        crate::valid_job_tracker::coinbase_contains_expected_op_return_payload(
            &returned_coinbase,
            &payload_bytes
        ),
        "returned coinbase must contain the same payload reported by op_return_payload_hex"
    );
}

#[cfg(test)]
fn make_empty_template_for_tests(
) -> roles_logic_sv2::template_distribution_sv2::NewTemplate<'static> {
    use binary_sv2::{Seq0255, B0255, B064K, U256};
    use std::convert::TryInto;

    let coinbase_prefix: B0255<'static> = Vec::new()
        .try_into()
        .expect("coinbase prefix should fit in B0255");
    let coinbase_tx_outputs: B064K<'static> = Vec::new()
        .try_into()
        .expect("coinbase outputs should fit in B064K");
    let merkle_path: Seq0255<'static, U256<'static>> = Vec::new().into();

    roles_logic_sv2::template_distribution_sv2::NewTemplate {
        template_id: 1,
        future_template: false,
        version: 0,
        coinbase_tx_version: 1,
        coinbase_prefix,
        coinbase_tx_input_sequence: 0,
        coinbase_tx_value_remaining: 0,
        coinbase_tx_outputs_count: 0,
        coinbase_tx_outputs,
        coinbase_tx_locktime: 0,
        merkle_path,
    }
}

#[cfg(test)]
fn make_coinbase_tx_for_found_job_tests(payload: &[u8]) -> bitcoin::Transaction {
    use bitcoin::{
        absolute::LockTime, script::PushBytesBuf, Amount, OutPoint, ScriptBuf, Sequence, TxIn,
        TxOut, Witness,
    };

    bitcoin::Transaction {
        version: bitcoin::transaction::Version(2),
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: vec![0x03, 0x01, 0x51].into(),
            sequence: Sequence::MAX,
            witness: Witness::from_slice(&[vec![0x42; 32]]),
        }],
        output: vec![
            TxOut {
                value: Amount::from_sat(5_000_000_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            },
            TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::new_op_return(
                    PushBytesBuf::try_from(payload.to_vec()).unwrap(),
                ),
            },
        ],
    }
}

#[cfg(test)]
fn find_pow_valid_nonce_for_found_job_tests(
    prev_hash: [u8; 32],
    n_bits: u32,
    version: u32,
    header_timestamp: u32,
    coinbase_tx: &bitcoin::Transaction,
) -> u32 {
    let coinbase_txid = coinbase_tx.compute_txid();
    for nonce in 0..=u32::MAX {
        let header = bitcoin::block::Header {
            version: bitcoin::block::Version::from_consensus(version as i32),
            prev_blockhash: bitcoin::BlockHash::from_byte_array(prev_hash),
            merkle_root: bitcoin::TxMerkleNode::from_byte_array(
                coinbase_txid.to_raw_hash().to_byte_array(),
            ),
            time: header_timestamp,
            bits: bitcoin::CompactTarget::from_consensus(n_bits),
            nonce,
        };
        if header.target().is_met_by(header.block_hash()) {
            return nonce;
        }
    }

    panic!("failed to find a PoW-valid nonce for found-job API test");
}
