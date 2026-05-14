use roles_logic_sv2::utils::Mutex;
use std::sync::Arc;
use tracing::error;

use crate::{
    monitor::MonitorAPI,
    proxy_state::{DownstreamType, ProxyState},
};

#[derive(Clone, Debug)]
pub struct ShareInfo {
    worker_name: String,
    difficulty: Option<f32>,
    #[allow(dead_code)]
    job_id: i64,
    #[allow(dead_code)]
    nonce: i64,
    #[allow(dead_code)]
    rejection_reason: Option<RejectionReason>,
    #[allow(dead_code)]
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
    connection_id: u32,
    token: Arc<Mutex<String>>,
}

impl SharesMonitor {
    pub fn new(connection_id: u32, token: Arc<Mutex<String>>) -> Self {
        SharesMonitor {
            connection_id,
            token,
        }
    }

    pub fn insert_share(&self, share: ShareInfo) {
        let token = self.token.safe_lock(|t| t.clone()).unwrap_or_else(|e| {
            error!("Failed to read monitor token: {:?}", e);
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
            String::new()
        });

        if token.is_empty() {
            return;
        }

        MonitorAPI::record_share(
            self.connection_id,
            share.worker_name,
            token,
            share.difficulty,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
