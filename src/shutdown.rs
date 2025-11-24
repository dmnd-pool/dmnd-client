use tokio::signal;
use tokio::sync::watch;
use tracing::{error, info};

/// Spawns a background task that listens for SIGINT (Ctrl+C) and SIGTERM,
/// unregisters the node, and then notifies all listeners via a watch channel.
pub fn handle_shutdown() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);

    tokio::spawn(async move {
        #[cfg(unix)]
        use tokio::signal::unix::{signal as unix_signal, SignalKind};
        #[cfg(unix)]
        let mut sigterm = match unix_signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to listen for SIGTERM: {:?}", e);
                return;
            }
        };
        #[cfg(unix)]
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
        #[cfg(not(unix))]
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
        }

        // Notify all listeners; ignore error if there are no receivers left.
        let _ = tx.send(true);
    });

    rx
}
