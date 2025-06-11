use lazy_static::lazy_static;
use roles_logic_sv2::utils::Mutex;
use std::collections::HashMap;
use tracing::{error, info};

lazy_static! {
    static ref DOWNSTREAM_HASHRATE: Mutex<f32> = Mutex::new(0.0);
    static ref UPSTREAM_CONNECTIONS: Mutex<HashMap<String, ConnectionInfo>> =
        Mutex::new(HashMap::new());
}

#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub connected: bool,
    pub address: std::net::SocketAddr,
    #[allow(dead_code)]
    pub allocated_percentage: f32,
    #[allow(dead_code)]
    pub last_seen: std::time::Instant,
}

pub struct ConnectionManager;

impl ConnectionManager {
    pub fn set_downstream_hashrate(hashrate: f32) {
        info!("Setting downstream hashrate to: {} h/s", hashrate);
        if DOWNSTREAM_HASHRATE.safe_lock(|h| *h = hashrate).is_err() {
            error!("Failed to set downstream hashrate");
        }
    }

    pub fn get_downstream_hashrate() -> f32 {
        let mut hashrate = 0.0;
        if DOWNSTREAM_HASHRATE.safe_lock(|h| hashrate = *h).is_err() {
            error!("Failed to get downstream hashrate");
        }
        hashrate
    }

    pub fn set_upstream_connection(id: &str, info: ConnectionInfo) {
        if UPSTREAM_CONNECTIONS
            .safe_lock(|conns| {
                conns.insert(id.to_string(), info.clone());
            })
            .is_err()
        {
            error!("Failed to update upstream connection");
        }

        if info.connected {
            info!("Upstream {} connected to {}", id, info.address);
        } else {
            info!("Upstream {} disconnected", id);
        }
    }

    #[allow(dead_code)]
    pub fn get_upstream_connection(id: &str) -> Option<ConnectionInfo> {
        let mut result = None;
        if UPSTREAM_CONNECTIONS
            .safe_lock(|conns| {
                result = conns.get(id).cloned();
            })
            .is_err()
        {
            error!("Failed to get upstream connection");
        }
        result
    }

    #[allow(dead_code)]
    pub fn get_all_upstream_connections() -> HashMap<String, ConnectionInfo> {
        let mut result = HashMap::new();
        if UPSTREAM_CONNECTIONS
            .safe_lock(|conns| {
                result = conns.clone();
            })
            .is_err()
        {
            error!("Failed to get all upstream connections");
        }
        result
    }

    #[allow(dead_code)]
    pub fn update_upstream_percentage(id: &str, percentage: f32) {
        if UPSTREAM_CONNECTIONS
            .safe_lock(|conns| {
                if let Some(info) = conns.get_mut(id) {
                    info.allocated_percentage = percentage;
                }
            })
            .is_err()
        {
            error!("Failed to update upstream percentage");
        }
    }
}
