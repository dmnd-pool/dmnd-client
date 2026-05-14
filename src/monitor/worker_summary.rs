#[derive(serde::Serialize, Debug, Clone)]
pub struct WorkerSummary {
    worker_name: String,
    hashrate: f64,
    valid_shares: u64,
    invalid_shares: u64,
}

impl WorkerSummary {
    pub fn new(worker_name: String, hashrate: f64, valid_shares: u64, invalid_shares: u64) -> Self {
        Self {
            worker_name,
            hashrate,
            valid_shares,
            invalid_shares,
        }
    }

    pub(crate) fn worker_name(&self) -> &str {
        &self.worker_name
    }

    #[cfg(test)]
    pub(crate) fn hashrate(&self) -> f64 {
        self.hashrate
    }

    #[cfg(test)]
    pub(crate) fn total_valid_shares(&self) -> u64 {
        self.valid_shares
    }

    #[cfg(test)]
    pub(crate) fn total_invalid_shares(&self) -> u64 {
        self.invalid_shares
    }
}
