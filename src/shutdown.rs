use tokio::net::TcpStream;
use tokio::signal;
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{debug, error, info};

use crate::monitor::worker_activity::WorkerActivity;
use crate::monitor::{self, node_register_endpoint, node_unregister_endpoint};

/// Spawns a background task that listens for SIGINT (Ctrl+C) and SIGTERM,
/// unregisters the node, and then notifies all listeners via a watch channel.
pub fn handle_shutdown() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);

    tokio::spawn(async move {
        // Set up SIGTERM stream (Unix only)
        let mut sigterm = match unix_signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to listen for SIGTERM: {:?}", e);
                return;
            }
        };

        tokio::select! {
            res = signal::ctrl_c() => {
                match res {
                    Ok(()) => info!("Received SIGINT (Ctrl+C), shutting down..."),
                    Err(e) => {
                        error!("Failed to listen for Ctrl+C: {:?}", e);
                        return;
                    }
                }
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down...");
            }
        }

        update_node_status("unregister").await;
        WorkerActivity::disconnect_all_active_workers()
            .await
            .unwrap_or_else(|e| {
                error!("Failed to disconnect all active workers: {}", e);
            });

        // Notify all listeners; ignore error if there are no receivers left.
        let _ = tx.send(true);
    });

    rx
}

/// Updates the node status by either registering or unregistering it only if TP_ADDRESS is set and reachable.
pub async fn update_node_status(action: &str) {
    let tp_address = match crate::config::Configuration::tp_address() {
        Some(addr) => addr,
        None => {
            debug!("TP_ADDRESS not set, skipping node {}.", action);
            return;
        }
    };

    // Test if TP_ADDRESS is reachable
    if !is_reachable(&tp_address).await {
        debug!(
            "TP_ADDRESS {} is unreachable, skipping node {}.",
            tp_address, action
        );
        return;
    }
    match action {
        "unregister" => {
            let monitor_api = monitor::MonitorAPI::new(node_unregister_endpoint());
            monitor_api.unregister_bitcoin_node().await
        }
        "register" => {
            let monitor_api = monitor::MonitorAPI::new(node_register_endpoint());
            monitor_api.register_bitcoin_node().await
        }
        _ => {
            error!("Unknown action: {}", action);
            return;
        }
    }
    .unwrap_or_else(|e| {
        error!("Failed to {} bitcoin node: {}", action, e);
    });
}

async fn is_reachable(address: &str) -> bool {
    // Try to establish a TCP connection
    debug!("Checking reachability of {}", address);
    match timeout(
        std::time::Duration::from_secs(5),
        TcpStream::connect(address),
    )
    .await
    {
        Ok(Ok(_)) => true,
        Ok(Err(e)) => {
            debug!("Failed to connect to {}: {}", address, e);
            false
        }
        Err(_) => {
            debug!("Connection to {} timed out", address);
            false
        }
    }
}
