mod routes;
pub mod stats;
mod utils;
use crate::{
    api::mempool::{
        spawn_zmq_events, submit_tx_list, ws_mempool_events_handler, MempoolEventBroadcaster,
    },
    config,
    dashboard::{
        dashboard::static_handler,
        jd_event_ws::{ws_event_handler, JobDeclarationData, TemplateNotificationBroadcaster},
    },
    db::connect_db,
    router::Router,
    API_SERVER_PORT,
};
use axum::{
    routing::{get, post},
    Router as AxumRouter,
};
use binary_sv2::{Seq064K, B016M};
mod bitcoin_rpc;
use bitcoincore_rpc::Client;
pub mod mempool;
use routes::Api;
use stats::StatsSender;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender as TSender;
use tokio::sync::oneshot;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{info, warn};

// Type for sending job declaration responses back to API endpoints
pub type JobResponseSender = oneshot::Sender<JobDeclarationData>;

// Type for transaction list with optional job response sender
pub type TxListWithResponse = (Seq064K<'static, B016M<'static>>, Option<JobResponseSender>);

// Holds shared state (like the router) that so that it can be accessed in all routes.
#[derive(Clone)]
pub struct AppState {
    router: Router,
    stats_sender: StatsSender,
    rpc: Option<Arc<Client>>,
    mempool_event_broadcaster: MempoolEventBroadcaster,
    tx_list_sender: TSender<TxListWithResponse>,
    pub jd_event_broadcaster: TemplateNotificationBroadcaster,
    db: Option<sqlx::SqlitePool>,
}

pub(crate) async fn start(
    router: Router,
    stats_sender: StatsSender,
    tx_list_sender: TSender<TxListWithResponse>,
    jd_event_broadcaster: TemplateNotificationBroadcaster,
) {
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::any())
        .allow_methods(Any)
        .allow_headers(Any);

    let rpc = match bitcoin_rpc::create_rpc_client() {
        Ok(client) => {
            info!("Successfully connected to Bitcoin RPC");
            Some(client)
        }
        Err(e) => {
            warn!("{e}");
            None
        }
    };

    // Connect to the database if rpc is available
    let db = if rpc.is_some() {
        info!("Connecting to the database");
        match connect_db().await {
            Ok(pool) => {
                info!("Database connection established");
                Some(pool)
            }
            Err(e) => {
                warn!("Failed to connect to the database: {e}");
                None
            }
        }
    } else {
        warn!("Skipping database connection due to missing Bitcoin RPC connection");
        None
    };

    let (mempool_event_broadcaster, _) = broadcast::channel(300);

    let state = AppState {
        router,
        stats_sender,
        rpc: rpc.clone(),
        mempool_event_broadcaster: mempool_event_broadcaster.clone(),
        tx_list_sender,
        jd_event_broadcaster: jd_event_broadcaster.clone(),
        db,
    };

    let zmq_pub_sequence = config::Configuration::zmq_pub_sequence();

    if let Some(rpc_client) = rpc {
        spawn_zmq_events(rpc_client, mempool_event_broadcaster, zmq_pub_sequence);
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
        .route("/ws/bitcoin/stream", get(ws_mempool_events_handler))
        .route("/ws/jd/stream", get(ws_event_handler))
        .route("/api/job-declaration", post(submit_tx_list))
        .route("/api/job-history", get(Api::get_job_history))
        .route("/api/job-txids/{template_id}", get(Api::get_job_txids))
        // Dashboard routes
        .route("/", get(static_handler))
        .route("/{*path}", get(static_handler))
        .with_state(state)
        .layer(cors);

    let api_server_addr = format!("0.0.0.0:{}", *API_SERVER_PORT);
    let listener = tokio::net::TcpListener::bind(api_server_addr)
        .await
        .expect("Invalid server address");
    println!("API Server listening on port {}", *API_SERVER_PORT);
    axum::serve(listener, app).await.unwrap();
}
