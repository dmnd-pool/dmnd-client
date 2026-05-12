pub(crate) mod bitcoin_rpc;
mod routes;
pub mod stats;
mod utils;
use std::sync::Arc;

use crate::{api::bitcoin_rpc::BitcoindRpc, router::Router, Configuration};
use axum::{
    routing::{get, post},
    Router as AxumRouter,
};
use routes::Api;
use stats::StatsSender;

// Holds shared state (like the router) that so that it can be accessed in all routes.
#[derive(Clone)]
pub struct AppState {
    router: Router,
    stats_sender: StatsSender,
    downstream_handoff: crate::DownstreamHandoffSender,
    prioritizing_txs: Option<PrioritizingTxs>,
}

#[derive(Clone)]
struct PrioritizingTxs {
    rpc: Arc<BitcoindRpc>,
    api_tx_token: String,
}

pub(crate) async fn start(
    router: Router,
    stats_sender: StatsSender,
    downstream_handoff: crate::DownstreamHandoffSender,
) {
    let prioritizing_txs = Configuration::bitcoind_rpc_config().map(|config| {
        let rpc = Arc::new(BitcoindRpc::new(
            config.url,
            config.user,
            config.pwd,
            config.fee_delta,
        ));
        PrioritizingTxs {
            rpc,
            api_tx_token: config.api_tx_token,
        }
    });

    let state = AppState {
        router,
        stats_sender,
        downstream_handoff,
        prioritizing_txs,
    };
    let app = AxumRouter::new()
        .route("/api/health", get(Api::health_check))
        .route("/api/tx/submit/{tx}", post(Api::send_tx_to_bitcoind))
        .route(
            "/api/tx/prioritized",
            get(Api::get_prioritized_transactions),
        )
        .route("/api/pool/info", get(Api::get_pool_info))
        .route("/api/stats/miners", get(Api::get_downstream_stats))
        .route("/api/stats/aggregate", get(Api::get_aggregate_stats))
        .route("/api/stats/session-timing", get(Api::get_session_timing))
        .route("/api/stats/system", get(Api::system_stats))
        .with_state(state);

    let api_server_port = crate::config::Configuration::api_server_port();
    let api_server_addr = format!("0.0.0.0:{api_server_port}");
    let listener = tokio::net::TcpListener::bind(api_server_addr)
        .await
        .expect("Invalid server address");
    println!("API Server listening on port {api_server_port}");
    axum::serve(listener, app).await.unwrap();
}
