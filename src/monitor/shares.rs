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
        self.shares.safe_lock(std::mem::take).unwrap_or_default()
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
                let batch_len = shares_to_send.len();
                match api.send_shares(shares_to_send, &token).await {
                    Ok(_) => {
                        info!("Saved {} shares to the monitoring server", batch_len);
                    }
                    Err(err) => {
                        warn!("Failed to send {} shares, this does not affect mining but may cause issues with monitoring: {:?}", batch_len, err);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn token() -> Arc<Mutex<String>> {
        Arc::new(Mutex::new(String::from("test-token")))
    }

    fn make_share(name: &str, job_id: i64) -> ShareInfo {
        ShareInfo::new(name.to_string(), Some(1.0), job_id, 0, None)
    }

    #[test]
    fn insert_then_take_returns_all_inserted() {
        let m = SharesMonitor::new(token());
        m.insert_share(make_share("w1", 1));
        m.insert_share(make_share("w2", 2));

        let taken = m.take_shares();
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[0].worker_name, "w1");
        assert_eq!(taken[1].worker_name, "w2");
    }

    #[test]
    fn take_drains_internal_state() {
        let m = SharesMonitor::new(token());
        m.insert_share(make_share("w1", 1));
        let _ = m.take_shares();
        assert!(m.take_shares().is_empty());
    }

    #[test]
    fn take_on_empty_returns_empty() {
        let m = SharesMonitor::new(token());
        assert!(m.take_shares().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_insert_and_take_loses_nothing() {
        const WORKERS: usize = 8;
        const PER_WORKER: i64 = 200;

        let m = SharesMonitor::new(token());
        let total_taken = Arc::new(AtomicUsize::new(0));

        let inserters: Vec<_> = (0..WORKERS)
            .map(|w| {
                let m = m.clone();
                tokio::spawn(async move {
                    for j in 0..PER_WORKER {
                        m.insert_share(make_share(&format!("w{w}"), j));
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();

        let taker = {
            let m = m.clone();
            let total = total_taken.clone();
            tokio::spawn(async move {
                for _ in 0..50 {
                    let batch = m.take_shares();
                    total.fetch_add(batch.len(), Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            })
        };

        for h in inserters {
            h.await.unwrap();
        }
        taker.await.unwrap();

        let tail = m.take_shares().len();
        let taken = total_taken.load(Ordering::Relaxed) + tail;
        assert_eq!(taken, WORKERS * PER_WORKER as usize);
    }
}
