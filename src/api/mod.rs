mod routes;
pub mod stats;
mod utils;
use crate::{
    api::mempool::{spawn_zmq_events, submit_tx_list, ws_events_handler, EventBroadcaster},
    bitcoin_rpc, config,
    router::Router,
    API_SERVER_PORT,
};
use axum::{
    routing::{get, post},
    Router as AxumRouter,
};
use binary_sv2::{Seq064K, B016M};
use bitcoincore_rpc::Client;
pub mod mempool;
use routes::Api;
use stats::StatsSender;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender as TSender;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{info, warn};

// Holds shared state (like the router) that so that it can be accessed in all routes.
#[derive(Clone)]
pub struct AppState {
    router: Router,
    stats_sender: StatsSender,
    rpc: Option<Arc<Client>>,
    event_broadcaster: EventBroadcaster,
    tx_list_sender: TSender<Seq064K<'static, B016M<'static>>>,
}

pub(crate) async fn start(
    router: Router,
    stats_sender: StatsSender,
    tx_list_sender: TSender<Seq064K<'static, B016M<'static>>>,
) {
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::exact("http://localhost:3000".parse().unwrap()))
        .allow_methods(Any)
        .allow_headers(Any);

    let rpc = match bitcoin_rpc::create_rpc_client() {
        Ok(client) => {
            info!("Successfully connected to Bitcoin RPC");
            Some(client)
        }
        Err(e) => {
            let datadir = config::Configuration::bitcoin_datadir();
            warn!("{e}");
            println!("Possible reasons:");
            println!("1. Bitcoin Core is not running");
            println!("2. Incorrect RPC credentials in bitcoin.conf");
            println!(
                "3. RPC port {} is not accessible",
                config::Configuration::rpc_port()
            );
            println!("4. Incorrect Bitcoin data directory path in config.toml");
            println!("   Data directory: {}", datadir.display());
            None
        }
    };

    let (event_broadcaster, _) = broadcast::channel(300);

    let state = AppState {
        router,
        stats_sender,
        rpc: rpc.clone(),
        event_broadcaster: event_broadcaster.clone(),
        tx_list_sender,
    };

    let zmq_pub_sequence = config::Configuration::zmq_pub_sequence();

    if let Some(rpc_client) = rpc {
        spawn_zmq_events(rpc_client, event_broadcaster, zmq_pub_sequence);
    } else {
        eprintln!("Skipping ZMQ events setup due to missing Bitcoin RPC connection");
    }

    let app = AxumRouter::new()
        .route("/api/health", get(Api::health_check))
        .route("/api/pool/info", get(Api::get_pool_info))
        .route("/api/stats/miners", get(Api::get_downstream_stats))
        .route("/api/stats/aggregate", get(Api::get_aggregate_stats))
        .route("/api/stats/system", get(Api::system_stats))
        .route("/api/mempool", get(mempool::fetch_mempool))
        .route("/ws/bitcoin/stream", get(ws_events_handler))
        .route("/api/job-declaration", post(submit_tx_list))
        .with_state(state)
        .layer(cors);

    let api_server_addr = format!("0.0.0.0:{}", *API_SERVER_PORT);
    let listener = tokio::net::TcpListener::bind(api_server_addr)
        .await
        .expect("Invalid server address");
    println!("API Server listening on port {}", *API_SERVER_PORT);
    axum::serve(listener, app).await.unwrap();
}
