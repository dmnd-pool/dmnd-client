use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::{
    monitor::MonitorAPI,
    proxy_state::{DownstreamType, ProxyState},
};

const SHARE_BATCH_INTERVAL_SECS: u64 = 180;
const SHARES_PER_REQUEST: usize = 200;

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

    /// Atomically takes ownership of the pending shares, leaving the buffer empty.
    fn drain_pending_shares(&self) -> Vec<ShareInfo> {
        self.shares.safe_lock(std::mem::take).unwrap_or_else(|e| {
            error!("Failed to lock pending shares: {:?}", e);
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
            Vec::new()
        })
    }

    /// Re-inserts shares at the front of the buffer so they are retried on the next tick.
    fn requeue_shares(&self, mut shares: Vec<ShareInfo>) {
        if shares.is_empty() {
            return;
        }
        self.shares
            .safe_lock(|event| {
                shares.append(event);
                *event = shares;
            })
            .unwrap_or_else(|e| {
                error!("Failed to lock pending shares: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
            });
    }

    pub async fn monitor(&self) {
        let api = MonitorAPI::shares();
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(SHARE_BATCH_INTERVAL_SECS));

        // First tick completes immediately
        interval.tick().await;
        loop {
            interval.tick().await;
            let pending = self.drain_pending_shares();
            if pending.is_empty() {
                warn!("No pending shares to send. If this happens frequently, check your miner.");
                continue;
            }

            let token = self.token.safe_lock(|t| t.clone()).unwrap();
            let total = pending.len();
            let mut sent = 0usize;
            let mut failed: Vec<ShareInfo> = Vec::new();

            for chunk in pending.chunks(SHARES_PER_REQUEST) {
                match api.send_shares(chunk, &token).await {
                    Ok(_) => sent += chunk.len(),
                    Err(err) => {
                        warn!(
                            "Failed to send shares chunk of {}, will retry next tick: {:?}",
                            chunk.len(),
                            err
                        );
                        failed.extend_from_slice(chunk);
                    }
                }
            }

            if sent > 0 {
                info!("Saved {}/{} shares to the monitoring server", sent, total);
            }
            self.requeue_shares(failed);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum RejectionReason {
    JobIdNotFound,
    InvalidShare,
    InvalidJobIdFormat,
    DifficultyMismatch,
    UpstreamRejected,
    RateLimited,
}

impl std::fmt::Display for RejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectionReason::JobIdNotFound => write!(f, "Job ID not found"),
            RejectionReason::InvalidShare => write!(f, "Invalid share"),
            RejectionReason::InvalidJobIdFormat => write!(f, "Invalid job ID format"),
            RejectionReason::DifficultyMismatch => write!(f, "Difficulty mismatch"),
            RejectionReason::UpstreamRejected => write!(f, "Upstream rejected share"),
            RejectionReason::RateLimited => write!(f, "Share rate limited"),
        }
    }
}
