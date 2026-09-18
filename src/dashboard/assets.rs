use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "frontend"]
pub struct Asset;

const SPA_INDEX: &str = "index.html";

fn resolve_asset_path(request_path: Option<&str>) -> String {
    let path = request_path.unwrap_or_default().trim_matches('/');

    match path {
        "app.css" | "app.js" => path.to_string(),
        "index.html" | "overview.html" | "dashboard/overview.html" => SPA_INDEX.to_string(),
        path if path.contains('.') => path.to_string(),
        _ => SPA_INDEX.to_string(),
    }
}

/// Handles requests for static dashboard assets and routes.
pub async fn static_handler(path: Option<Path<String>>) -> impl IntoResponse {
    let request_path = path.as_ref().map(|Path(path)| path.as_str());
    let normalized_path = request_path.unwrap_or_default().trim_matches('/');
    if normalized_path == "api" || normalized_path.starts_with("api/") {
        return (StatusCode::NOT_FOUND, "404 Not Found").into_response();
    }
    let asset_path = resolve_asset_path(request_path);

    match Asset::get(&asset_path) {
        Some(content) => {
            let body = content.data.into_owned();
            let mime = mime_guess::from_path(&asset_path).first_or_octet_stream();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime.as_ref())
                .header(header::CACHE_CONTROL, "no-store")
                .body(body.into())
                .unwrap()
        }
        None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_routes_resolve_to_the_spa() {
        let overview = resolve_asset_path(Some("dashboard/overview"));

        assert_eq!(overview, SPA_INDEX);
        assert_eq!(
            resolve_asset_path(Some("dashboard/overview.html")),
            SPA_INDEX
        );
        assert!(Asset::get(&overview).is_some());
    }

    #[test]
    fn resolves_the_only_frontend_assets() {
        let javascript = resolve_asset_path(Some("app.js"));
        let stylesheet = resolve_asset_path(Some("app.css"));

        assert_eq!(javascript, "app.js");
        assert_eq!(stylesheet, "app.css");
        assert!(Asset::get(&javascript).is_some());
        assert!(Asset::get(&stylesheet).is_some());
    }

    #[test]
    fn missing_files_remain_missing() {
        let missing = resolve_asset_path(Some("missing.png"));

        assert!(Asset::get(&missing).is_none());
    }
}
