use atomic_float::AtomicF32;
use lazy_static::lazy_static;
use roles_logic_sv2::utils::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{error, warn};

use crate::proxy_state::ProxyState;

lazy_static! {
    pub static ref DownstreamStatsRegistry: Arc<Mutex<HashMap<u32, Arc<DownstreamConnectionStats>>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

// Stats for each connected downstream device
#[derive(Debug)]
pub struct DownstreamConnectionStats {
    pub device_name: Arc<Mutex<Option<String>>>,
    pub hashrate: Arc<AtomicF32>,
    pub accepted_shares: Arc<AtomicU64>,
    pub rejected_shares: Arc<AtomicU64>,
}

impl DownstreamStatsRegistry {
    // Starts tracking stats for a new downstream connection
    pub async fn setup_stats(connection_id: u32) {
        let stats = Arc::new(DownstreamConnectionStats {
            device_name: Arc::new(Mutex::new(None)),
            hashrate: Arc::new(AtomicF32::new(0.0)),
            accepted_shares: Arc::new(AtomicU64::new(0)),
            rejected_shares: Arc::new(AtomicU64::new(0)),
        });
        if let Err(e) = DownstreamStatsRegistry.safe_lock(|dm| dm.insert(connection_id, stats)) {
            error!("Failed to acquire DownstreamStatsRegistry lock: {e}")
        }
    }

    // Retrieves stats for all connected downstream devices
    pub fn collect_stats(&self) -> Result<HashMap<u32, Arc<DownstreamConnectionStats>>, String> {
        match self.safe_lock(|dsr| dsr.clone()) {
            Ok(dsr) => Ok(dsr),
            Err(e) => Err(format!(
                "Failed to acquire DownstreamStatsRegistry lock: {e}"
            )),
        }
    }

    // Updates the hashrate for a specific connected downstream device
    pub fn update_hashrate(&self, new_hashrate: f32, connection_id: &u32) {
        if let Err(e) = self.safe_lock(|dm| {
            if let Some(stats) = dm.get(connection_id) {
                stats.hashrate.store(new_hashrate, Ordering::Relaxed);
            }
        }) {
            error!("Failed to acquire DownstreamStatsRegistry lock: {e}");
            ProxyState::update_inconsistency(Some(1))
        }
    }

    // Updates the accepted share count for a specific connected downstream device
    pub fn update_accepted_shares(&self, connection_id: &u32) {
        if let Err(e) = self.safe_lock(|dm| {
            if let Some(stats) = dm.get(connection_id) {
                stats.accepted_shares.fetch_add(1, Ordering::Acquire);
            }
        }) {
            error!("Failed to acquire DownstreamStatsRegistry lock: {e}");
            ProxyState::update_inconsistency(Some(1))
        }
    }

    // Updates the rejected share count for a specific connected downstream device
    pub fn update_rejected_shares(&self, connection_id: &u32) {
        if let Err(e) = self.safe_lock(|dm| {
            if let Some(stats) = dm.get(connection_id) {
                stats.rejected_shares.fetch_add(1, Ordering::Acquire);
            }
        }) {
            error!("Failed to acquire DownstreamStatsRegistry lock: {e}");
            ProxyState::update_inconsistency(Some(1))
        }
    }

    // Updates the device name for a specific downstream
    pub fn update_device_name(&self, name: &str, connection_id: &u32) {
        if let Err(e) = self.safe_lock(|dm| {
            if let Some(downstream) = dm.get(connection_id) {
                if let Err(e) = downstream.device_name.safe_lock(|device_name| {
                    *device_name = Some(name.to_string());
                }) {
                    error!("Failed to acquire device_name lock: {e}");
                    ProxyState::update_inconsistency(Some(1))
                }
            }
        }) {
            error!("Failed to acquire DownstreamStatsRegistry lock: {e}");
            ProxyState::update_inconsistency(Some(1))
        }
    }

    // Retrieves the device_name for a specific connected downstream
    pub fn get_device_name(&self, connection_id: &u32) -> Option<String> {
        match self.safe_lock(|dm| dm.get(connection_id).cloned()) {
            Ok(Some(downstream)) => {
                match downstream
                    .device_name
                    .safe_lock(|device_name| device_name.clone())
                {
                    Ok(device_name) => device_name,
                    Err(e) => {
                        error!("Failed to acquire device_name lock: {}", e);
                        ProxyState::update_inconsistency(Some(1));
                        None
                    }
                }
            }
            Ok(None) => None,
            Err(e) => {
                error!("Failed to acquire DownstreamStatsRegistry lock: {}", e);
                ProxyState::update_inconsistency(Some(1));
                None
            }
        }
    }

    //// Removes stats for a disconnected downstream device
    pub fn remove_downstream_stats(&self, connection_id: &u32) {
        if let Err(e) = self.safe_lock(|dm| {
            if dm.remove(connection_id).is_some() {
                warn!(
                    "Stats for Downstream with connection {} removed",
                    connection_id
                )
            }
        }) {
            error!("Failed to acquire DownstreamStatsRegistry lock: {e}");
            ProxyState::update_inconsistency(Some(1))
        }
    }
}
