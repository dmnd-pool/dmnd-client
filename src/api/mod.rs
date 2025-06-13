mod routes;
pub mod stats;
mod utils;
use crate::{
    api::mempool::{start_zmq_block_stream, start_zmq_tx_stream, BlockBroadcaster, TxBroadcaster},
    router::Router,
    API_SERVER_PORT,
};
use axum::{routing::get, Router as AxumRouter};
use bitcoincore_rpc::{Auth, Client};
pub mod mempool;
use routes::Api;
use stats::StatsSender;
use std::sync::Arc;
use tokio::sync::broadcast;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
// Holds shared state (like the router) that so that it can be accessed in all routes.
#[derive(Clone)]
pub struct AppState {
    router: Router,
    stats_sender: StatsSender,
    rpc: Arc<Client>,
    tx_broadcaster: TxBroadcaster,
    block_broadcaster: BlockBroadcaster,
}

pub(crate) async fn start(router: Router, stats_sender: StatsSender) {
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::exact("http://localhost:3000".parse().unwrap()))
        .allow_methods(Any)
        .allow_headers(Any);

    let rpc = Client::new(
        "http://127.0.0.1:8332",
        Auth::UserPass("username".to_string(), "password".to_string()),
    )
    .expect("Failed to connect to Bitcoin RPC");

    let rpc = Arc::new(rpc);

    let (tx_broadcaster, _) = broadcast::channel(300);
    let (blk_broadcaster, _) = broadcast::channel(300);

    let state = AppState {
        router,
        stats_sender,
        rpc: rpc.clone(),
        tx_broadcaster: tx_broadcaster.clone(),
        block_broadcaster: blk_broadcaster.clone(),
    };

    start_zmq_tx_stream(rpc.clone(), tx_broadcaster, "tcp://127.0.0.1:28332");
    start_zmq_block_stream(blk_broadcaster, "tcp://127.0.0.1:28333");

    let app = AxumRouter::new()
        .route("/api/health", get(Api::health_check))
        .route("/api/pool/info", get(Api::get_pool_info))
        .route("/api/stats/miners", get(Api::get_downstream_stats))
        .route("/api/stats/aggregate", get(Api::get_aggregate_stats))
        .route("/api/stats/system", get(Api::system_stats))
        .route("/api/mempool", get(mempool::fetch_mempool))
        .route("/ws/txs/new", get(mempool::ws_new_transactions))
        .route(
            "/ws/blocks/confirmed/txids",
            get(mempool::ws_block_transactions),
        )
        .with_state(state)
        .layer(cors);

    let api_server_addr = format!("0.0.0.0:{}", *API_SERVER_PORT);
    let listener = tokio::net::TcpListener::bind(api_server_addr)
        .await
        .expect("Invalid server address");
    println!("API Server listening on port {}", *API_SERVER_PORT);
    axum::serve(listener, app).await.unwrap();
}
