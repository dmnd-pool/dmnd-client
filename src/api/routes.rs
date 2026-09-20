use super::{
    bitcoin_rpc::{BitcoindRpc, BitcoindRpcError, PrioTx},
    utils::get_cpu_and_memory_usage,
    AppState, PRIORITIZED_TRANSACTIONS_POLL_LOCK,
};
use crate::config::Configuration;
use crate::proxy_state::ProxyState;
use axum::{
    extract::{Path, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use bitcoin::Txid;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use tracing::{error, info, warn};

#[cfg(test)]
use serde_json::json;

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
        match current_pool_address {
            Some(address) => {
                let latency = match state.router.get_latency(address).await {
                    Ok(latency) => Some(latency),
                    Err(_) => None,
                };
                let response_data = serde_json::json!({
                    "address": address.to_string(),
                    "latency": latency.map(|d| d.as_millis().to_string()).unwrap_or_else(|| "N/A".to_string())
                });
                (
                    StatusCode::OK,
                    Json(APIResponse::success(Some(response_data))),
                )
            }
            None => (
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

    pub async fn prioritize_transaction(
        State(state): State<AppState>,
        headers: HeaderMap,
        Path((txid, fee_delta)): Path<(Txid, i64)>,
    ) -> impl IntoResponse {
        if fee_delta == 0 {
            return (StatusCode::OK, Json(APIResponse::success(None)));
        }
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

        let _prioritized_transactions_guard = PRIORITIZED_TRANSACTIONS_POLL_LOCK.lock().await;
        if crate::prioritized_transactions::BAD_TRANSACTIONS
            .get(&txid)
            .is_some()
        {
            warn!(%txid, "transaction acceleration rejected because transaction is cached as bad");
            return (
                StatusCode::BAD_REQUEST,
                Json(APIResponse::<String>::error(Some(format!(
                    "Transaction acceleration was unsuccessful: {txid} was previously rejected by bitcoind"
                )))),
            );
        }

        match prioritizing_txs
            .rpc
            .prioritise_transaction(&txid, fee_delta)
            .await
        {
            Ok(()) => {
                let current_fee_delta = crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS
                    .get(&txid)
                    .unwrap_or(0);
                let fee_delta = current_fee_delta + fee_delta;
                crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.record(txid, fee_delta);
                info!("transaction prioritized in bitcoind: {txid}");
                (
                    StatusCode::OK,
                    Json(APIResponse::success(Some(txid.to_string()))),
                )
            }
            Err(e) => {
                error!("Failed to prioritize transaction in bitcoind: {e}");
                (
                    e.status_code(),
                    Json(APIResponse::error(Some(e.to_string()))),
                )
            }
        }
    }

    pub async fn restore_tx_priority(
        State(state): State<AppState>,
        headers: HeaderMap,
        Path(txid): Path<Txid>,
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
            warn!("unauthorized tx priority restoration request");
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::<String>::error(Some(
                    "Unauthorized".to_string(),
                ))),
            );
        }

        let _prioritized_transactions_guard = PRIORITIZED_TRANSACTIONS_POLL_LOCK.lock().await;
        match Self::restore_tx_priority_from_stores(&prioritizing_txs.rpc, &txid).await {
            Ok(()) => {
                info!(%txid, "transaction priority restored in bitcoind");
                (
                    StatusCode::OK,
                    Json(APIResponse::success(Some(txid.to_string()))),
                )
            }
            Err(e) => {
                error!(%txid, error = %e, "failed to restore transaction priority in bitcoind");
                (
                    e.status_code(),
                    Json(APIResponse::error(Some(e.to_string()))),
                )
            }
        }
    }

    pub(super) async fn restore_tx_priority_from_stores(
        rpc: &BitcoindRpc,
        txid: &Txid,
    ) -> Result<(), BitcoindRpcError> {
        let stores = [
            &crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS,
            &crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED,
        ];

        for store in stores {
            let Some(fee_delta) = store.get(txid) else {
                continue;
            };
            let fee_delta_adjustment = fee_delta.checked_neg().ok_or_else(|| {
                BitcoindRpcError::Prioritize("fee delta adjustment overflow".to_string())
            })?;

            rpc.prioritise_transaction(txid, fee_delta_adjustment)
                .await?;
        }

        for store in stores {
            store.remove(txid);
        }

        Ok(())
    }

    pub async fn get_prioritized_transactions(
        State(state): State<AppState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        let Some(prioritizing_txs) = state.prioritizing_txs.as_ref() else {
            warn!("PRIORITIZING TXS NOT ENABLED");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::<CategorizedPrioritizedTransactions>::error(
                    Some("PRIORITIZING TXS NOT ENABLED".to_string()),
                )),
            );
        };

        if !is_authorized_for_tx_prioritization(&headers, &prioritizing_txs.api_tx_token) {
            warn!("unauthorized prioritized txs request");
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::<CategorizedPrioritizedTransactions>::error(
                    Some("Unauthorized".to_string()),
                )),
            );
        }

        let _prioritized_transactions_guard = PRIORITIZED_TRANSACTIONS_POLL_LOCK.lock().await;
        let transactions = match prioritizing_txs.rpc.get_prioritised_transactions().await {
            Ok(transactions) => transactions,
            Err(e) => {
                error!(error = %e, "failed to fetch prioritized transactions from bitcoind");
                return (
                    e.status_code(),
                    Json(APIResponse::<CategorizedPrioritizedTransactions>::error(
                        Some(e.to_string()),
                    )),
                );
            }
        };
        let response = categorize_prioritized_transactions(
            transactions,
            &crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.snapshot_txids(),
            &crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.snapshot_txids(),
        );

        (StatusCode::OK, Json(APIResponse::success(Some(response))))
    }
}

#[derive(Serialize)]
struct CategorizedPrioritizedTransactions {
    mempool_space: BTreeMap<String, PrioTx>,
    manually_prioritized: BTreeMap<String, PrioTx>,
    unknown: BTreeMap<String, PrioTx>,
}

fn categorize_prioritized_transactions(
    transactions: HashMap<Txid, PrioTx>,
    mempool_space_txids: &HashSet<Txid>,
    manually_prioritized_txids: &HashSet<Txid>,
) -> CategorizedPrioritizedTransactions {
    let mut categorized = CategorizedPrioritizedTransactions {
        mempool_space: BTreeMap::new(),
        manually_prioritized: BTreeMap::new(),
        unknown: BTreeMap::new(),
    };

    for (txid, transaction) in transactions {
        let category = if mempool_space_txids.contains(&txid) {
            &mut categorized.mempool_space
        } else if manually_prioritized_txids.contains(&txid) {
            &mut categorized.manually_prioritized
        } else {
            &mut categorized.unknown
        };
        category.insert(txid.to_string(), transaction);
    }

    categorized
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

#[derive(Debug, Serialize)]
struct APIResponse<T> {
    success: bool,
    message: Option<String>,
    data: Option<T>,
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
            )),
            api_tx_token: "api-token".to_string(),
        }),
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
    };

    let response = Api::prioritize_transaction(
        State(state),
        axum::http::HeaderMap::new(),
        Path(("00".repeat(32).parse().expect("valid txid"), 42)),
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
            )),
            api_tx_token: "api-token".to_string(),
        }),
    };

    let response = Api::prioritize_transaction(
        State(state),
        axum::http::HeaderMap::new(),
        Path(("00".repeat(32).parse().expect("valid txid"), 42)),
    )
    .await
    .into_response();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn prioritize_transaction_rejects_a_cached_bad_transaction() {
    use axum::body::to_bytes;
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use tokio::sync::mpsc;

    let txid = "00000000000000000000000000000000000000000000000000000000000000b1"
        .parse()
        .expect("valid test txid");
    crate::prioritized_transactions::BAD_TRANSACTIONS.remove(&txid);
    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&txid);
    crate::prioritized_transactions::BAD_TRANSACTIONS.record(txid, 75_000);

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);
    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                "http://127.0.0.1:1".to_string(),
                "user".to_string(),
                "password".to_string(),
            )),
            api_tx_token: "api-token".to_string(),
        }),
    };
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    let response = Api::prioritize_transaction(State(state), headers, Path((txid, 42)))
        .await
        .into_response();

    crate::prioritized_transactions::BAD_TRANSACTIONS.remove(&txid);
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.get(&txid),
        None
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["success"], false);
    assert_eq!(
        body["message"],
        format!(
            "Transaction acceleration was unsuccessful: {txid} was previously rejected by bitcoind"
        )
    );
    assert_eq!(body["data"], serde_json::Value::Null);
}

#[tokio::test]
async fn successful_prioritization_records_the_applied_fee_delta() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};
    use tokio::sync::mpsc;

    async fn mock_bitcoind(Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
        assert_eq!(body["method"], "prioritisetransaction");
        (
            StatusCode::OK,
            Json(json!({
                "result": true,
                "error": null,
                "id": "dmnd-client"
            })),
        )
    }

    let txid = "0000000000000000000000000000000000000000000000000000000000000003"
        .parse()
        .expect("valid test txid");
    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&txid);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server local addr");
    let app = Router::new().route("/bitcoin", post(mock_bitcoind));
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);
    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                format!("http://{addr}/bitcoin"),
                "user".to_string(),
                "password".to_string(),
            )),
            api_tx_token: "api-token".to_string(),
        }),
    };
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    let response = Api::prioritize_transaction(State(state), headers, Path((txid, 42)))
        .await
        .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS
            .snapshot()
            .get(&txid),
        Some(&42)
    );

    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&txid);
    server.abort();
}

#[tokio::test]
async fn successful_restore_reverses_and_removes_all_cached_fee_deltas() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct MockBitcoindState {
        requests: mpsc::UnboundedSender<Value>,
    }

    async fn mock_bitcoind(
        State(state): State<MockBitcoindState>,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state
            .requests
            .send(body)
            .expect("test should receive bitcoind request");
        (
            StatusCode::OK,
            Json(json!({
                "result": true,
                "error": null,
                "id": "dmnd-client"
            })),
        )
    }

    let txid = "0000000000000000000000000000000000000000000000000000000000000004"
        .parse()
        .expect("valid test txid");
    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&txid);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.remove(&txid);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server local addr");
    let (requests, mut received_requests) = mpsc::unbounded_channel();
    let app = Router::new()
        .route("/bitcoin", post(mock_bitcoind))
        .with_state(MockBitcoindState { requests });
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);
    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                format!("http://{addr}/bitcoin"),
                "user".to_string(),
                "password".to_string(),
            )),
            api_tx_token: "api-token".to_string(),
        }),
    };
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.record(txid, 100_000_000);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.record(txid, 25_000_000);

    let response = Api::restore_tx_priority(State(state), headers, Path(txid))
        .await
        .into_response();

    assert_eq!(response.status(), StatusCode::OK);

    let manual_restore_request = received_requests
        .recv()
        .await
        .expect("manual prioritisetransaction restore request");
    assert_eq!(manual_restore_request["method"], "prioritisetransaction");
    assert_eq!(
        manual_restore_request["params"],
        json!([txid, 0, -100_000_000])
    );

    let accelerator_restore_request = received_requests
        .recv()
        .await
        .expect("accelerator prioritisetransaction restore request");
    assert_eq!(
        accelerator_restore_request["method"],
        "prioritisetransaction"
    );
    assert_eq!(
        accelerator_restore_request["params"],
        json!([txid, 0, -25_000_000])
    );
    assert_eq!(
        crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.get(&txid),
        None
    );
    assert_eq!(
        crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.get(&txid),
        None
    );
    assert!(received_requests.try_recv().is_err());

    server.abort();
}

#[tokio::test]
async fn get_prioritized_transactions_returns_categorized_bitcoind_snapshot() {
    use axum::body::to_bytes;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::{routing::post, Json, Router};
    use bitcoin::Txid;
    use serde_json::{json, Value};
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct MockBitcoindState {
        requests: mpsc::UnboundedSender<Value>,
    }

    async fn mock_bitcoind(
        State(state): State<MockBitcoindState>,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state
            .requests
            .send(body)
            .expect("test should receive bitcoind request");
        (
            StatusCode::OK,
            Json(json!({
                "result": {
                    "00000000000000000000000000000000000000000000000000000000000000a1": {
                        "fee_delta": 100_000,
                        "in_mempool": true,
                        "modified_fee": 110_000
                    },
                    "00000000000000000000000000000000000000000000000000000000000000a2": {
                        "fee_delta": 200_000,
                        "in_mempool": false
                    },
                    "00000000000000000000000000000000000000000000000000000000000000a3": {
                        "fee_delta": -300_000,
                        "in_mempool": true,
                        "modified_fee": 30_000
                    }
                },
                "error": null,
                "id": "dmnd-client"
            })),
        )
    }

    let manual_txid: Txid = "00000000000000000000000000000000000000000000000000000000000000a1"
        .parse()
        .expect("valid test txid");
    let mempool_space_txid: Txid =
        "00000000000000000000000000000000000000000000000000000000000000a2"
            .parse()
            .expect("valid test txid");
    let unknown_txid: Txid = "00000000000000000000000000000000000000000000000000000000000000a3"
        .parse()
        .expect("valid test txid");

    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&manual_txid);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.remove(&mempool_space_txid);
    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.record(manual_txid, 100_000);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED
        .record(mempool_space_txid, 200_000);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server local addr");
    let (requests, mut received_requests) = mpsc::unbounded_channel();
    let app = Router::new()
        .route("/", post(mock_bitcoind))
        .with_state(MockBitcoindState { requests });
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                format!("http://{addr}"),
                "user".to_string(),
                "password".to_string(),
            )),
            api_tx_token: "api-token".to_string(),
        }),
    };

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    let response = Api::get_prioritized_transactions(State(state), headers)
        .await
        .into_response();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["success"], true);
    assert_eq!(
        body["data"],
        json!({
            "mempool_space": {
                mempool_space_txid.to_string(): {
                    "fee_delta": 200_000,
                    "in_mempool": false
                }
            },
            "manually_prioritized": {
                manual_txid.to_string(): {
                    "fee_delta": 100_000,
                    "in_mempool": true,
                    "modified_fee": 110_000
                }
            },
            "unknown": {
                unknown_txid.to_string(): {
                    "fee_delta": -300_000,
                    "in_mempool": true,
                    "modified_fee": 30_000
                }
            }
        })
    );
    let request = received_requests
        .recv()
        .await
        .expect("getprioritisedtransactions request");
    assert_eq!(request["method"], "getprioritisedtransactions");
    assert_eq!(request["params"], json!([]));
    assert!(received_requests.try_recv().is_err());

    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&manual_txid);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.remove(&mempool_space_txid);
    server.abort();
}

#[test]
fn tx_prioritization_auth_accepts_matching_bearer_token() {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    assert!(is_authorized_for_tx_prioritization(&headers, "api-token"));
}
