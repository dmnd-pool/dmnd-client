use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::{
    monitor::{shares_server_endpoint, MonitorAPI},
    proxy_state::{DownstreamType, ProxyState},
};

const SHARE_BATCH_INTERVAL_SECS: u64 = 180;

#[derive(serde::Serialize, Clone, Debug)]
pub struct ShareInfo {
    worker_name: String,
    difficulty: Option<f32>,
    job_id: i64,
    nonce: i64,
    // if None, the share was accepted
    rejection_reason: Option<RejectionReason>,
    timestamp: u64,
}

impl ShareInfo {
    pub fn new(
        worker_name: String,
        difficulty: Option<f32>,
        job_id: i64,
        nonce: i64,
        rejection_reason: Option<RejectionReason>,
    ) -> Self {
        ShareInfo {
            worker_name,
            difficulty,
            job_id,
            nonce,
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
    token: Arc<Mutex<String>>,
}

impl SharesMonitor {
    pub fn new(token: Arc<Mutex<String>>) -> Self {
        SharesMonitor {
            shares: Arc::new(Mutex::new(Vec::new())),
            token,
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

    /// Atomically takes all pending shares, leaving the internal list empty for new
    /// shares. This avoids the race condition where a clone + clear would drop shares
    /// inserted between the two operations.
    fn take_shares(&self) -> Vec<ShareInfo> {
        self.shares
            .safe_lock(|shares| std::mem::take(shares))
            .unwrap_or_else(|e| {
                error!("Failed to lock pending shares: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                Vec::new()
            })
    }

    /// Re-inserts shares back into the pending list. Used when a send fails so the
    /// shares are retried on the next cycle.
    fn requeue_shares(&self, mut failed_shares: Vec<ShareInfo>) {
        self.shares
            .safe_lock(|shares| {
                failed_shares.append(shares);
                *shares = failed_shares;
            })
            .unwrap_or_else(|e| {
                error!("Failed to lock pending shares: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
            });
    }

    pub async fn monitor(&self) {
        let api = MonitorAPI::new(shares_server_endpoint());
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(SHARE_BATCH_INTERVAL_SECS));

        // First tick completes immediately
        interval.tick().await;
        loop {
            interval.tick().await;
            let shares_to_send = self.take_shares();
            if !shares_to_send.is_empty() {
                let token = self.token.safe_lock(|t| t.clone()).unwrap();
                match api.send_shares(shares_to_send.clone(), &token).await {
                    Ok(_) => {
                        info!(
                            "Saved {} shares to the monitoring server",
                            shares_to_send.len()
                        );
                    }
                    Err(err) => {
                        warn!("Failed to send shares, this does not affect mining but may cause issues with monitoring: {:?}", err);
                        self.requeue_shares(shares_to_send);
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
