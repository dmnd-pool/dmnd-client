pub mod assets;

use std::sync::atomic::{AtomicBool, Ordering};
use tracing::info;

/// A flag to ensure the dashboard is opened only once.
static OPENED: AtomicBool = AtomicBool::new(false);

/// Open the dashboard in a browser once, when `--headful`and TP address are set.
pub(crate) fn open_dashboard(port: &str) {
    if crate::Configuration::tp_address().is_none()
        || !crate::Configuration::headful()
        || OPENED.swap(true, Ordering::Relaxed)
    {
        return;
    }
    let url = format!("http://127.0.0.1:{port}");
    let (program, args): (&str, &[&str]) = if cfg!(target_os = "macos") {
        ("open", &[])
    } else if cfg!(target_os = "windows") {
        ("cmd", &["/C", "start", ""])
    } else {
        ("xdg-open", &[])
    };
    match std::process::Command::new(program)
        .args(args)
        .arg(&url)
        .spawn()
    {
        Ok(_) => info!(%url, "opened the dashboard"),
        Err(error) => {
            info!(%error, %url, "no browser to open, please visit {url} to see the dashboard")
        }
    }
}
