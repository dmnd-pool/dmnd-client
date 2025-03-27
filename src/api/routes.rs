use super::{stats::DownstreamStatsRegistry, utils::get_cpu_and_memory_usage, AppState};
use crate::proxy_state::ProxyState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use std::{collections::HashMap, sync::atomic::Ordering};

pub struct Api {}

impl Api {
    // Retrieves connected donwnstreams stats
    pub async fn get_downstream_stats() -> impl IntoResponse {
        match DownstreamStatsRegistry.collect_stats() {
            Ok(stats) => {
                // converts to serializable format
                let serializable_stats = stats
                    .iter()
                    .map(|(id, arc_stats)| {
                        let stats = arc_stats;
                        (
                            *id,
                            SerializableDownstreamStats {
                                device_name: DownstreamStatsRegistry.get_device_name(id),
                                hashrate: stats.hashrate.load(Ordering::Acquire),
                                accepted_shares: stats.accepted_shares.load(Ordering::Acquire),
                                rejected_shares: stats.rejected_shares.load(Ordering::Acquire),
                            },
                        )
                    })
                    .collect::<HashMap<u32, SerializableDownstreamStats>>();
                (
                    StatusCode::OK,
                    Json(APIResponse::success(Some(serializable_stats))),
                )
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(APIResponse::error(Some(e))),
            ),
        }
    }

    // Retrieves system stats (CPU and memory usage)
    pub async fn system_stats() -> impl IntoResponse {
        let (cpu, memory) = get_cpu_and_memory_usage().await;
        let cpu_usgae = format!("{:.3}", cpu);
        let data = serde_json::json!({"cpu_usage_%": cpu_usgae, "memory_usage_bytes": memory});
        Json(APIResponse::success(Some(data)))
    }

    // Returns aggregate stats of all downstream devices
    pub async fn get_aggregate_stats() -> impl IntoResponse {
        let mut total_connected_device = 0;
        let mut total_accepted_shares = 0;
        let mut total_rejected_shares = 0;
        let mut total_hashrate = 0.0;
        match DownstreamStatsRegistry.collect_stats() {
            Ok(stats) => {
                for (_, downstream) in stats {
                    total_connected_device += 1;
                    total_accepted_shares += downstream.accepted_shares.load(Ordering::Acquire);
                    total_rejected_shares += downstream.rejected_shares.load(Ordering::Acquire);
                    total_hashrate += downstream.hashrate.load(Ordering::Acquire) as f64;
                }
                let result = AggregateStates {
                    total_connected_device,
                    aggregate_hashrate: total_hashrate,
                    aggregate_accepted_shares: total_accepted_shares,
                    aggregate_rejected_shares: total_rejected_shares,
                };
                (StatusCode::OK, Json(APIResponse::success(Some(result))))
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(APIResponse::error(Some(e))),
            ),
        }
    }

    // Retrieves the current pool information
    pub async fn get_pool_info(State(state): State<AppState>) -> impl IntoResponse {
        let current_pool_address = state.router.current_pool;
        let latency = state.router.latency;

        let response_data = match (current_pool_address, latency) {
            (Some(address), Some(latency)) => {
                serde_json::json!({
                    "address": address.to_string(),
                    "latency": latency.as_millis().to_string()
                })
            }
            (_, _) => {
                serde_json::json!({
                    "address": "Pool address not available".to_string(),
                    "latency": "Pool letency not available".to_string()
                })
            }
        };

        Json(APIResponse::success(Some(response_data)))
    }

    // Returns the status of the Proxy
    pub async fn health_check() -> impl IntoResponse {
        let response = match ProxyState::is_proxy_down() {
            (false, None) => "Proxy OK".to_string(),
            (true, Some(states)) => states,
            _ => "Unknown proxy state".to_string(),
        };
        Json(APIResponse::success(Some(response)))
    }
}

#[derive(Serialize)]
struct SerializableDownstreamStats {
    device_name: Option<String>,
    hashrate: f32,
    accepted_shares: u64,
    rejected_shares: u64,
}

#[derive(Serialize)]
struct AggregateStates {
    total_connected_device: u32,
    aggregate_hashrate: f64, // f64 is used here to avoid overflow
    aggregate_accepted_shares: u64,
    aggregate_rejected_shares: u64,
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
