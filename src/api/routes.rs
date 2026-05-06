use super::{utils::get_cpu_and_memory_usage, AppState};
use crate::config::Configuration;
use crate::proxy_state::ProxyState;
use axum::{
    extract::{Path, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Serialize;
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
                Json(APIResponse::error(Some(
                    "PRIORITIZING TXS NOT ENABLED".to_string(),
                ))),
            );
        };

        if !is_authorized_for_tx_prioritization(&headers, &prioritizing_txs.api_tx_token) {
            warn!("unauthorized tx prioritization request");
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::error(Some("Unauthorized".to_string()))),
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
                100_000_000,
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

#[test]
fn tx_prioritization_auth_accepts_matching_bearer_token() {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    assert!(is_authorized_for_tx_prioritization(&headers, "api-token"));
}
