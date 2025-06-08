use crate::assets::Asset;
use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use mime_guess;
use tracing::debug;
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

    debug!("Requested path: '{}'", path);

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

    debug!("Looking for asset: '{}'", asset_path);

    match Asset::get(&asset_path) {
        Some(content) => {
            debug!("Found asset: {}", asset_path);
            let body = content.data.into_owned();
            let mime = mime_guess::from_path(&asset_path).first_or_octet_stream();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime.as_ref())
                .body(body.into())
                .unwrap()
        }
        None => {
            debug!("Asset not found: {}", asset_path);
            match Asset::get("index.html") {
                Some(content) => {
                    debug!("Serving fallback index.html");
                    let body = content.data.into_owned();
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "text/html")
                        .body(body.into())
                        .unwrap()
                }
                None => {
                    debug!("Even index.html not found!");
                    (StatusCode::NOT_FOUND, "404 Not Found").into_response()
                }
            }
        }
    }
}
