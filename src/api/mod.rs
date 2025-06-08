mod routes;
pub mod stats;
mod utils;
use crate::{router::Router, API_SERVER_PORT};
use axum::{routing::get, Router as AxumRouter};
use bitcoincore_rpc::{Auth, Client};
pub mod mempool;
use routes::Api;
use stats::StatsSender;
use std::sync::Arc;
use tokio::sync::broadcast;
// Holds shared state (like the router) that so that it can be accessed in all routes.
#[derive(Clone)]
pub struct AppState {
    router: Router,
    stats_sender: StatsSender,
    rpc: Arc<Client>,
}

pub(crate) async fn start(router: Router, stats_sender: StatsSender) {
    let rpc = Client::new(
        "http://127.0.0.1:8332",
        Auth::UserPass("username".to_string(), "password".to_string()),
    )
    .expect("Failed to connect to Bitcoin RPC");

    let rpc = Arc::new(rpc);

    let state = AppState {
        router,
        stats_sender,
        rpc: rpc.clone(),
    };

    let (tx_sender, _) = broadcast::channel::<mempool::Tx>(300);
    mempool::spawn_zmq_task(rpc.clone(), tx_sender.clone(), "tcp://127.0.0.1:28332");

    let app = AxumRouter::new()
        .route("/api/health", get(Api::health_check))
        .route("/api/pool/info", get(Api::get_pool_info))
        .route("/api/stats/miners", get(Api::get_downstream_stats))
        .route("/api/stats/aggregate", get(Api::get_aggregate_stats))
        .route("/api/stats/system", get(Api::system_stats))
        .route("/mempool/old", get(mempool::get_mempool_txs))
        .route("/ws/mempool/new", get(mempool::ws_upgrade))
        .with_state(state)
        .layer(axum::Extension(tx_sender));

    let api_server_addr = format!("0.0.0.0:{}", *API_SERVER_PORT);
    let listener = tokio::net::TcpListener::bind(api_server_addr)
        .await
        .expect("Invalid server address");
    println!("API Server listening on port {}", *API_SERVER_PORT);
    axum::serve(listener, app).await.unwrap();
}
