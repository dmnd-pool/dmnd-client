mod routes;
pub mod stats;
mod utils;
use crate::router::Router;
use axum::{routing::get, Router as AxumRouter};
use routes::Api;

// Holds shared state (like the router) that so that it can be accessed in all routes.
#[derive(Clone)]
pub struct AppState {
    router: Router,
}

pub(crate) async fn start(router: Router) {
    let state = AppState { router };
    let app = AxumRouter::new()
        .route("/health", get(Api::health_check))
        .route("/pool/info", get(Api::get_pool_info))
        .route("/stats/miners", get(Api::get_downstream_stats))
        .route("/stats/aggregate", get(Api::get_aggregate_stats))
        .route("/stats/system", get(Api::system_stats))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9099")
        .await
        .expect("Invalid server address");
    println!("API Server listening on port 3001 ");
    axum::serve(listener, app).await.unwrap();
}
