use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use mime_guess;

use crate::dashboard::assets::Asset;

/// Handles requests for static dashboard assets and routes.
///
/// This function serves static files for the dashboard, handling special cases for dashboard routes and paths.
/// - Requests for `/`, `/dashboard/`, or an empty path will serve `index.html`.
/// - Requests for `/overview` or `/history` will serve `dashboard/overview.html` and `dashboard/history.html` respectively.
/// - Requests for any path starting with `dashboard/` and not ending with `.html` will have `.html` appended.
/// - If the requested asset is not found, it will serve `404.html` if available, otherwise a plain 404 response.
///
/// # Arguments
/// * `path` - Optional path parameter specifying the requested asset or route.
///
/// # Returns
/// An HTTP response containing the asset data, a fallback `404.html`, or a plain 404 error.
pub async fn static_handler(path: Option<Path<String>>) -> impl IntoResponse {
    let path = match path {
        Some(Path(p)) => {
            let p = p.trim_matches('/');
            if p.is_empty() || p == "dashboard" {
                "index.html".to_string()
            } else {
                p.to_string()
            }
        }
        None => "index.html".to_string(),
    };

    // Handle dashboard routes specially
    let asset_path = match path.as_str() {
        "overview" => "dashboard/overview.html".to_string(),
        "history" => "dashboard/history.html".to_string(),
        p if p.starts_with("dashboard/") && !p.ends_with(".html") => format!("{}.html", p),
        _ => path.clone(),
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
        None => match Asset::get("404.html") {
            Some(content) => {
                let body = content.data.into_owned();
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(header::CONTENT_TYPE, "text/html")
                    .body(body.into())
                    .unwrap()
            }
            None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
        },
    }
}
