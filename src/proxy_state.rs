use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use lazy_static::lazy_static;
use roles_logic_sv2::utils::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

lazy_static! {
    static ref PROXY_STATE: Arc<Mutex<ProxyState>> = Arc::new(Mutex::new(ProxyState::new()));
}

/// Main enum representing the overall state of the proxy
#[derive(Debug, Clone, PartialEq)]
pub enum ProxyStates {
    Pool(PoolState),
    Tp(TpState),
    Jd(JdState),
    ShareAccounter(ShareAccounterState),
    InternalInconsistency(u32),
    Downstream(DownstreamState),
    Upstream(UpstreamState),
    Translator(TranslatorState),
}

/// Represents the state of the pool
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PoolState {
    Up,
    Down,
}

/// Represents the state of the Tp
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TpState {
    Up,
    Down,
}

/// Represents the state of the Translator
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TranslatorState {
    Up,
    Down,
}

/// Represents the state of the JD
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum JdState {
    Up,
    Down,
}

/// Represents the state of the Share Accounter
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ShareAccounterState {
    Up,
    Down,
}

/// Represents the state of the Downstream
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DownstreamState {
    Up,
    Down(Vec<DownstreamType>), // A specific downstream is down
}

/// Represents the state of the Upstream
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UpstreamState {
    Up,
    Down(Vec<UpstreamType>), // A specific upstream is down
}

/// Represents different downstreams
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DownstreamType {
    JdClientMiningDownstream,
    TranslatorDownstream,
}

/// Represents different upstreams
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UpstreamType {
    JDCMiningUpstream,
    TranslatorUpstream,
}

/// Create an UpstreamConnection struct to store connection info
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct UpstreamConnection {
    pub url: String,
    pub address: std::net::SocketAddr,
    pub auth_key: key_utils::Secp256k1PublicKey,
    pub connection_type: UpstreamType,
    pub is_connected: bool,
    pub shares_submitted: u64,
    pub shares_accepted: u64,
    pub last_used: Instant,
}

/// Represents global proxy state
#[derive(Debug)]
pub struct ProxyState {
    pub pool: PoolState,
    pub tp: TpState,
    pub jd: JdState,
    pub share_accounter: ShareAccounterState,
    pub translator: TranslatorState,
    pub inconsistency: Option<u32>,
    pub downstream: DownstreamState,
    pub upstream: UpstreamState,

    // New fields for multiple upstream support
    pub upstream_connections: HashMap<String, UpstreamConnection>,
    pub total_hashrate: f32,
    pub current_upstream_index: usize,
}

impl ProxyState {
    pub fn new() -> Self {
        Self {
            pool: PoolState::Up,
            tp: TpState::Up,
            jd: JdState::Up,
            share_accounter: ShareAccounterState::Up,
            translator: TranslatorState::Up,
            inconsistency: None,
            downstream: DownstreamState::Up,
            upstream: UpstreamState::Up,

            // Initialize new fields
            upstream_connections: HashMap::new(),
            total_hashrate: 0.0,
            current_upstream_index: 0,
        }
    }

    pub fn update_pool_state(pool_state: PoolState) {
        info!("Updating PoolState state to {:?}", pool_state);
        if PROXY_STATE
            .safe_lock(|state| {
                state.pool = pool_state;
                // // state.update_proxy_state();
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_tp_state(tp_state: TpState) {
        info!("Updating TpState state to {:?}", tp_state);
        if PROXY_STATE
            .safe_lock(|state| {
                state.tp = tp_state;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_jd_state(jd_state: JdState) {
        info!("Updating JdState state to {:?}", jd_state);
        if PROXY_STATE
            .safe_lock(|state| {
                state.jd = jd_state;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_translator_state(translator_state: TranslatorState) {
        info!("Updating Translator state to {:?}", translator_state);
        if PROXY_STATE
            .safe_lock(|state| {
                state.translator = translator_state;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_share_accounter_state(share_accounter_state: ShareAccounterState) {
        info!(
            "Updating ShareAccounterState state to {:?}",
            share_accounter_state
        );
        if PROXY_STATE
            .safe_lock(|state| {
                state.share_accounter = share_accounter_state;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_inconsistency(code: Option<u32>) {
        info!("Updating Internal Inconsistency state to {:?}", code);
        if PROXY_STATE
            .safe_lock(|state| {
                state.inconsistency = code;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_downstream_state(downstream_type: DownstreamType) {
        info!("Updating Downstream state to {:?}", downstream_type);
        if PROXY_STATE
            .safe_lock(|state| {
                state.downstream = DownstreamState::Down(vec![downstream_type]);
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_upstream_state(upstream_type: UpstreamType) {
        info!("Updating Upstream state to {:?}", upstream_type);
        if PROXY_STATE
            .safe_lock(|state| {
                state.upstream = UpstreamState::Down(vec![upstream_type]);
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    pub fn update_proxy_state_up() {
        if PROXY_STATE
            .safe_lock(|state| {
                state.pool = PoolState::Up;
                state.jd = JdState::Up;
                state.translator = TranslatorState::Up;
                state.tp = TpState::Up;
                state.share_accounter = ShareAccounterState::Up;
                state.upstream = UpstreamState::Up;
                state.downstream = DownstreamState::Up;
                state.inconsistency = None;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    /// Add a new upstream connection
    pub fn add_upstream_connection(
        id: String,
        url: String,
        address: std::net::SocketAddr,
        auth_key: key_utils::Secp256k1PublicKey,
        connection_type: UpstreamType,
    ) {
        info!("Adding upstream connection: {} at {}", id, url);

        if PROXY_STATE
            .safe_lock(|state| {
                let connection = UpstreamConnection {
                    url,
                    address,
                    auth_key,
                    connection_type,
                    is_connected: false,
                    shares_submitted: 0,
                    shares_accepted: 0,
                    last_used: Instant::now(),
                };

                state.upstream_connections.insert(id, connection);
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    /// Set the total hashrate to be distributed among upstreams
    pub fn set_total_hashrate(hashrate: f32) {
        info!("Setting total hashrate to: {} h/s", hashrate);
        if PROXY_STATE
            .safe_lock(|state| {
                state.total_hashrate = hashrate;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    /// Get the total hashrate
    pub fn get_total_hashrate() -> f32 {
        let mut hashrate = 0.0;
        if PROXY_STATE
            .safe_lock(|state| {
                hashrate = state.total_hashrate;
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
        hashrate
    }

    /// Get the next upstream in round-robin fashion
    pub fn get_next_upstream(
    ) -> Option<(String, std::net::SocketAddr, key_utils::Secp256k1PublicKey)> {
        let mut result = None;

        if PROXY_STATE
            .safe_lock(|state| {
                // Get IDs of all connected upstreams
                let active_upstreams: Vec<&String> = state
                    .upstream_connections
                    .iter()
                    .filter(|(_, conn)| conn.is_connected)
                    .map(|(id, _)| id)
                    .collect();

                if active_upstreams.is_empty() {
                    return;
                }

                // Use round-robin to select the next upstream
                if state.current_upstream_index >= active_upstreams.len() {
                    state.current_upstream_index = 0;
                }

                let id = active_upstreams[state.current_upstream_index].clone();
                if let Some(conn) = state.upstream_connections.get(&id) {
                    result = Some((id.clone(), conn.address, conn.auth_key));
                }

                // Update index for next call
                state.current_upstream_index =
                    (state.current_upstream_index + 1) % active_upstreams.len();
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }

        result
    }

    /// Get the hashrate for a specific upstream (equal distribution)
    #[allow(dead_code)]
    pub fn get_hashrate_for_upstream(id: Option<&str>) -> f32 {
        let mut hashrate = 0.0;

        if PROXY_STATE
            .safe_lock(|state| {
                // Count active connections
                let active_count = state
                    .upstream_connections
                    .values()
                    .filter(|conn| conn.is_connected)
                    .count();

                if active_count > 0 {
                    // Equal distribution - each upstream gets the same portion
                    hashrate = state.total_hashrate / active_count as f32;

                    // If a specific ID was provided, check if it's active
                    if let Some(id) = id {
                        if let Some(conn) = state.upstream_connections.get(id) {
                            if !conn.is_connected {
                                // If this specific upstream isn't connected, return 0
                                hashrate = 0.0;
                            }
                        } else {
                            // If this ID doesn't exist, return 0
                            hashrate = 0.0;
                        }
                    }
                }
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }

        hashrate
    }

    /// Record a share submission to an upstream
    #[allow(dead_code)]
    pub fn record_share_submission(upstream_id: &str) {
        if PROXY_STATE
            .safe_lock(|state| {
                if let Some(conn) = state.upstream_connections.get_mut(upstream_id) {
                    conn.shares_submitted += 1;
                    conn.last_used = Instant::now();
                }
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    /// Record a share acceptance from an upstream
    #[allow(dead_code)]
    pub fn record_share_acceptance(upstream_id: &str) {
        if PROXY_STATE
            .safe_lock(|state| {
                if let Some(conn) = state.upstream_connections.get_mut(upstream_id) {
                    conn.shares_accepted += 1;
                }
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    /// Update connection status for an upstream with timestamp
    pub fn set_upstream_connection_status(id: &str, connected: bool) {
        if PROXY_STATE
            .safe_lock(|state| {
                if let Some(conn) = state.upstream_connections.get_mut(id) {
                    conn.is_connected = connected;
                    if connected {
                        conn.last_used = Instant::now();
                        info!("Upstream {} is now connected", id);
                    } else {
                        info!("Upstream {} is now disconnected", id);
                    }
                }
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    /// Get connection count
    pub fn get_upstream_connection_count() -> usize {
        let mut count = 0;

        if PROXY_STATE
            .safe_lock(|state| {
                count = state.upstream_connections.len();
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }

        count
    }

    /// Get active connection count
    pub fn get_active_upstream_count() -> usize {
        let mut count = 0;

        if PROXY_STATE
            .safe_lock(|state| {
                count = state
                    .upstream_connections
                    .values()
                    .filter(|conn| conn.is_connected)
                    .count();
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }

        count
    }

    /// Update upstream shares
    pub fn update_upstream_shares(upstream_id: &str, submitted: u64, accepted: u64) {
        if PROXY_STATE
            .safe_lock(|state| {
                if let Some(conn) = state.upstream_connections.get_mut(upstream_id) {
                    conn.shares_submitted += submitted;
                    conn.shares_accepted += accepted;
                    conn.last_used = Instant::now();
                }
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    /// Remove an upstream connection
    pub fn remove_upstream_connection(upstream_id: &str) {
        info!("Removing upstream connection: {}", upstream_id);

        if PROXY_STATE
            .safe_lock(|state| {
                state.upstream_connections.remove(upstream_id);
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }
    }

    /// Check if proxy is down
    pub fn is_proxy_down() -> (bool, Option<String>) {
        let errors = Self::get_errors();
        if errors.is_ok() && errors.as_ref().unwrap().is_empty() {
            (false, None)
        } else {
            let error_descriptions: Vec<String> =
                errors.iter().map(|e| format!("{:?}", e)).collect();
            (true, Some(error_descriptions.join(", ")))
        }
    }

    pub fn get_errors() -> Result<Vec<ProxyStates>, ()> {
        let mut errors = Vec::new();
        if PROXY_STATE
            .safe_lock(|state| {
                if state.pool == PoolState::Down {
                    errors.push(ProxyStates::Pool(state.pool));
                }
                if state.tp == TpState::Down {
                    errors.push(ProxyStates::Tp(state.tp));
                }
                if state.jd == JdState::Down {
                    errors.push(ProxyStates::Jd(state.jd));
                }
                if state.share_accounter == ShareAccounterState::Down {
                    errors.push(ProxyStates::ShareAccounter(state.share_accounter));
                }
                if state.translator == TranslatorState::Down {
                    errors.push(ProxyStates::Translator(state.translator));
                }
                if let Some(inconsistency) = state.inconsistency {
                    errors.push(ProxyStates::InternalInconsistency(inconsistency));
                }
                if matches!(state.downstream, DownstreamState::Down(_)) {
                    errors.push(ProxyStates::Downstream(state.downstream.clone()));
                }
                if matches!(state.upstream, UpstreamState::Down(_)) {
                    errors.push(ProxyStates::Upstream(state.upstream.clone()));
                }
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        } else {
            Ok(errors)
        }
    }

    /// Get upstream statistics
    pub fn get_upstream_stats() -> Vec<(String, bool, u64, u64)> {
        let mut stats = Vec::new();

        if PROXY_STATE
            .safe_lock(|state| {
                stats = state
                    .upstream_connections
                    .iter()
                    .map(|(id, conn)| {
                        (
                            id.clone(),
                            conn.is_connected,
                            conn.shares_submitted,
                            conn.shares_accepted,
                        )
                    })
                    .collect();
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }

        stats
    }

    /// Get all upstream connections (including inactive ones)
    pub fn get_all_upstream_connections() -> Vec<(
        String,
        std::net::SocketAddr,
        key_utils::Secp256k1PublicKey,
        bool,
    )> {
        let mut connections = Vec::new();

        if PROXY_STATE
            .safe_lock(|state| {
                connections = state
                    .upstream_connections
                    .iter()
                    .map(|(id, conn)| (id.clone(), conn.address, conn.auth_key, conn.is_connected))
                    .collect();
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }

        connections
    }

    /// Get only active upstream connections (existing method is fine)
    pub fn get_upstream_connections(
    ) -> Vec<(String, std::net::SocketAddr, key_utils::Secp256k1PublicKey)> {
        let mut connections = Vec::new();

        if PROXY_STATE
            .safe_lock(|state| {
                connections = state
                    .upstream_connections
                    .iter()
                    .filter(|(_, conn)| conn.is_connected)
                    .map(|(id, conn)| (id.clone(), conn.address, conn.auth_key))
                    .collect();
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }

        connections
    }

    /// Mark an upstream as inactive (disconnect it)
    pub fn mark_upstream_inactive(upstream_id: &str) {
        Self::set_upstream_connection_status(upstream_id, false);
    }

    /// Get best upstream based on latency or other criteria
    /// For now, just returns the first active upstream
    pub fn get_best_upstream(
    ) -> Option<(String, std::net::SocketAddr, key_utils::Secp256k1PublicKey)> {
        let mut result = None;

        if PROXY_STATE
            .safe_lock(|state| {
                // Find the first active upstream
                for (id, conn) in &state.upstream_connections {
                    if conn.is_connected {
                        result = Some((id.clone(), conn.address, conn.auth_key));
                        break;
                    }
                }
            })
            .is_err()
        {
            error!("Global Proxy Mutex Corrupted");
            std::process::exit(1);
        }

        result
    }
}
