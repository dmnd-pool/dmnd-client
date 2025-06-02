mod routes;
pub mod stats;
mod utils;
use crate::router::Router;
use axum::{routing::get, Router as AxumRouter};
use routes::Api;
use stats::StatsSender;

// Holds shared state (like the router) that so that it can be accessed in all routes.
#[derive(Clone)]
pub struct AppState {
    router: Option<Router>,
    stats_sender: StatsSender,
}

pub(crate) async fn start(router: Option<Router>, stats_sender: StatsSender) {
    let state = AppState {
        router,
        stats_sender,
    };
    let app = AxumRouter::new()
        .route("/api/health", get(Api::health_check))
        .route("/api/pool/info", get(Api::get_pool_info))
        .route("/api/stats/miners", get(Api::get_downstream_stats))
        .route("/api/stats/aggregate", get(Api::get_aggregate_stats))
        .route("/api/stats/system", get(Api::system_stats))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001")
        .await
        .expect("Invalid server address");
    println!("API Server listening on port 3001 ");
    axum::serve(listener, app).await.unwrap();
}
