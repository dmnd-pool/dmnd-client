use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use mime_guess;

use crate::dashboard::assets::Asset;

/// Handles requests for static dashboard assets and routes.
/// The dashboard assets is created from https://github.com/demand-open-source/jd-tx-selection-dashboard.
///
/// This function serves static files for the dashboard, handling special cases for dashboard routes.
/// If the requested asset is not found, it falls back to serving `index.html` if available, or returns a 404.
///
/// # Arguments
/// * `path` - Optional path parameter specifying the requested asset or route.
///
/// # Returns
/// An HTTP response containing the asset data, a fallback `index.html`, or a 404 error.
pub async fn static_handler(path: Option<Path<String>>) -> impl IntoResponse {
    let path = match path {
        Some(Path(p)) => {
            if p.is_empty() {
                "index.html".to_string()
            } else {
                p
            }
        }
        None => "index.html".to_string(),
    };

    // Handle dashboard routes specially
    let asset_path = if path == "overview" {
        "dashboard/overview.html".to_string()
    } else if path.starts_with("dashboard/") {
        // For paths like "dashboard/something", try "dashboard/something.html"
        if path.ends_with(".html") {
            path.clone()
        } else {
            format!("{}.html", path)
        }
    } else {
        path.clone()
    };

    match Asset::get(&asset_path) {
        Some(content) => {
            let body = content.data.into_owned();
            let mime = mime_guess::from_path(&asset_path).first_or_octet_stream();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime.as_ref())
                .body(body.into())
                .unwrap()
        }
        None => match Asset::get("index.html") {
            Some(content) => {
                let body = content.data.into_owned();
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/html")
                    .body(body.into())
                    .unwrap()
            }
            None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
        },
    }
}
