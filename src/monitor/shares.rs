use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::{
    monitor::{shares_server_endpoint, MonitorAPI},
    proxy_state::{DownstreamType, ProxyState},
};

#[derive(serde::Serialize, Clone, Debug)]
pub struct ShareInfo {
    worker_name: String,
    difficulty: Option<f32>,
    job_id: i64,
    // if None, the share was accepted
    rejection_reason: Option<RejectionReason>,
    timestamp: u64,
}

impl ShareInfo {
    pub fn new(
        worker_name: String,
        difficulty: Option<f32>,
        job_id: i64,
        rejection_reason: Option<RejectionReason>,
    ) -> Self {
        ShareInfo {
            worker_name,
            difficulty,
            job_id,
            rejection_reason,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SharesMonitor {
    shares: Arc<Mutex<Vec<ShareInfo>>>,
}

impl SharesMonitor {
    pub fn new() -> Self {
        SharesMonitor {
            shares: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Inserts a new share into the pending shares list.
    pub fn insert_share(&self, share: ShareInfo) {
        self.shares
            .safe_lock(|event| {
                event.push(share);
            })
            .unwrap_or_else(|e| {
                error!("Failed to lock pending shares: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
            });
    }

    /// Retrieves the list of pending shares.
    fn get_next_shares(&self) -> Vec<ShareInfo> {
        self.shares
            .safe_lock(|event| event.clone())
            .unwrap_or_else(|e| {
                error!("Failed to lock pending shares: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                Vec::new()
            })
    }

    /// Clears the list of pending shares.
    fn clear_next_shares(&self) {
        self.shares
            .safe_lock(|event| {
                event.clear();
            })
            .unwrap_or_else(|e| {
                error!("Failed to lock pending shares: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
            });
    }

    pub async fn monitor(&self) -> Result<(), ()> {
        let api = match MonitorAPI::new(shares_server_endpoint()).await {
            Ok(api) => api,
            Err(_) => {
                error!("Shares monitor server is unreachable");
                return Err(());
            }
        };
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        // First tick completes immediately
        interval.tick().await;
        loop {
            interval.tick().await;
            let shares_to_send = self.get_next_shares();
            if !shares_to_send.is_empty() {
                match api.send_shares(shares_to_send.clone()).await {
                    Ok(_) => {
                        info!(
                            "Saved {} shares to the monitoring server",
                            shares_to_send.len()
                        );
                        self.clear_next_shares();
                    }
                    Err(err) => {
                        warn!("Failed to send shares, this does not affect mining but may cause issues with monitoring: {:?}", err);
                    }
                }
            } else {
                warn!("No pending shares to send. If this happens frequently, check your miner.");
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum RejectionReason {
    JobIdNotFound,
    InvalidShare,
    InvalidJobIdFormat,
    DifficultyMismatch,
}

impl std::fmt::Display for RejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectionReason::JobIdNotFound => write!(f, "Job ID not found"),
            RejectionReason::InvalidShare => write!(f, "Invalid share"),
            RejectionReason::InvalidJobIdFormat => write!(f, "Invalid job ID format"),
            RejectionReason::DifficultyMismatch => write!(f, "Difficulty mismatch"),
        }
    }
}
