use crate::{minin_pool_connection, proxy_state::ProxyState, HashUnit};
use key_utils::Secp256k1PublicKey;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct MultiUpstreamManager {
    upstreams: Arc<Mutex<HashMap<String, UpstreamConnection>>>,
    auth_pub_k: Secp256k1PublicKey,
    setup_connection_msg: Option<roles_logic_sv2::common_messages_sv2::SetupConnection<'static>>,
    timer: Option<Duration>,
}

#[derive(Clone)]
pub struct UpstreamConnection {
    pub address: SocketAddr,
    #[allow(dead_code)]
    pub allocated_hashrate: f64,
    pub allocated_percentage: f32,
    pub is_active: bool,
}

impl UpstreamConnection {
    fn new(address: SocketAddr) -> Self {
        Self {
            address,
            allocated_hashrate: 0.0,
            allocated_percentage: 0.0,
            is_active: false,
        }
    }
    #[allow(dead_code)] // This method is kept for future use
    fn update_allocation(&mut self, percentage: f32, total_hashrate: f64) {
        self.allocated_percentage = percentage;
        self.allocated_hashrate = total_hashrate * percentage as f64 / 100.0;
    }
}

impl MultiUpstreamManager {
    pub fn new(
        upstreams: Vec<SocketAddr>,
        auth_pub_k: Secp256k1PublicKey,
        setup_connection_msg: Option<
            roles_logic_sv2::common_messages_sv2::SetupConnection<'static>,
        >,
        timer: Option<Duration>,
    ) -> Self {
        let upstream_map = upstreams
            .into_iter()
            .enumerate()
            .map(|(i, addr)| (format!("upstream-{}", i), UpstreamConnection::new(addr)))
            .collect();

        Self {
            upstreams: Arc::new(Mutex::new(upstream_map)),
            auth_pub_k,
            setup_connection_msg,
            timer,
        }
    }

    /// Start and maintain all upstream connections according to hashrate distribution
    pub async fn maintain_connections(&self) {
        let upstreams = self.upstreams.clone();
        let auth_pub_k = self.auth_pub_k;
        let setup_connection_msg = self.setup_connection_msg.clone();
        let timer = self.timer;

        let upstreams_guard = upstreams.lock().await;
        for (id, conn) in upstreams_guard.iter() {
            let id = id.clone();
            let address = conn.address;
            let upstreams = upstreams.clone();
            let setup_connection_msg = setup_connection_msg.clone();

            tokio::spawn(async move {
                loop {
                    match minin_pool_connection::connect_pool(
                        address,
                        auth_pub_k,
                        setup_connection_msg.clone(),
                        timer,
                    )
                    .await
                    {
                        Ok((_send, _recv, _abortable)) => {
                            Self::update_upstream_status(&upstreams, &id, true).await;
                            // Keep the connection alive (simulate mining)
                            tokio::time::sleep(Duration::from_secs(30)).await;
                        }
                        Err(e) => {
                            error!("Failed to connect to upstream {}: {:?}", id, e);
                            Self::update_upstream_status(&upstreams, &id, false).await;
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            });
        }
    }

    /// Helper method to update upstream connection status
    async fn update_upstream_status(
        upstreams: &Arc<Mutex<HashMap<String, UpstreamConnection>>>,
        id: &str,
        is_active: bool,
    ) {
        let mut upstreams = upstreams.lock().await;
        if let Some(upstream) = upstreams.get_mut(id) {
            upstream.is_active = is_active;
        }
    }

    pub async fn set_hashrate_distribution(
        &mut self,
        mut distribution: Vec<f32>,
    ) -> Result<(), String> {
        let upstream_count = {
            let upstreams = self.upstreams.lock().await;
            upstreams.len()
        };

        // Pad with zeros if needed
        if distribution.len() < upstream_count {
            let zeros_needed = upstream_count - distribution.len();
            distribution.extend(vec![0.0; zeros_needed]);
            debug!("Padded distribution with {} zeros", zeros_needed);
        } else if distribution.len() > upstream_count {
            return Err(format!(
                "Distribution array has {} values but only {} pools available",
                distribution.len(),
                upstream_count
            ));
        }

        // Sort distribution highest to lowest
        distribution.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));

        // Get pools ranked by latency (best first)
        let ranked_pools = self.rank_by_latency().await;

        // Normalize percentages
        let total: f32 = distribution.iter().sum();
        if (total - 100.0).abs() > 0.1 {
            warn!("Distribution sum: {:.1}%, normalizing to 100%", total);
            if total > 0.0 {
                for percentage in &mut distribution {
                    *percentage = (*percentage / total) * 100.0;
                }
            } else {
                return Err("All distribution percentages are zero or negative".to_string());
            }
        }

        // Apply distribution to ranked pools
        let mut upstreams = self.upstreams.lock().await;

        // Reset all to 0%
        for upstream in upstreams.values_mut() {
            upstream.allocated_percentage = 0.0;
        }

        // Assign percentages to best latency pools first
        let downstream_hashrate = ProxyState::get_downstream_hashrate();

        for (i, (pool_id, _address, latency)) in ranked_pools.iter().enumerate() {
            if let Some(&percentage) = distribution.get(i) {
                if let Some(upstream) = upstreams.get_mut(pool_id) {
                    upstream.allocated_percentage = percentage;

                    if percentage > 0.0 {
                        let allocated_hashrate = (percentage / 100.0) * downstream_hashrate;
                        info!(
                            "{}: {} ({:.1}%) - {}ms",
                            pool_id,
                            HashUnit::format_value(allocated_hashrate),
                            percentage,
                            latency.as_millis()
                        );
                    }
                }
            }
        }

        Ok(())
    }

    // Add this method to test latency
    async fn test_pool_latency(address: &SocketAddr) -> Result<Duration, String> {
        let start = std::time::Instant::now();

        match tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(address),
        )
        .await
        {
            Ok(Ok(_)) => Ok(start.elapsed()),
            Ok(Err(e)) => Err(format!("Connection failed: {}", e)),
            Err(_) => Err("Timeout".to_string()),
        }
    }

    // Add this method to rank pools by latency
    pub async fn rank_by_latency(&self) -> Vec<(String, SocketAddr, Duration)> {
        let upstreams = self.upstreams.lock().await;
        let mut pool_latencies = Vec::new();

        info!("Testing latency for {} pools", upstreams.len());

        for (id, conn) in upstreams.iter() {
            match Self::test_pool_latency(&conn.address).await {
                Ok(latency) => {
                    debug!("{}: {}ms", id, latency.as_millis());
                    pool_latencies.push((id.clone(), conn.address, latency));
                }
                Err(e) => {
                    warn!("{}: latency test failed - {}", id, e);
                    pool_latencies.push((id.clone(), conn.address, Duration::from_secs(999)));
                }
            }
        }

        // Sort by latency (lowest first)
        pool_latencies.sort_by_key(|(_, _, latency)| *latency);

        pool_latencies
    }
    #[allow(dead_code)] // This method is kept for future use
    pub async fn get_upstreams(&self) -> HashMap<String, UpstreamConnection> {
        self.upstreams.lock().await.clone()
    }
    pub async fn get_detailed_connection_stats(&self) -> Vec<(String, bool, f32)> {
        let upstreams = self.upstreams.lock().await;

        upstreams
            .iter()
            .map(|(id, conn)| (id.clone(), conn.is_active, conn.allocated_percentage))
            .collect()
    }

    // Add upstream (called from Router)
    pub async fn add_upstream(
        &self,
        id: String,
        address: SocketAddr,
        _key: Secp256k1PublicKey,
        _setup_connection_msg: Option<
            roles_logic_sv2::common_messages_sv2::SetupConnection<'static>,
        >,
        _timer: Option<Duration>,
    ) -> Result<(), String> {
        let mut upstreams = self.upstreams.lock().await;

        // Check if upstream already exists - if so, don't overwrite it
        if upstreams.contains_key(&id) {
            return Ok(());
        }

        // Only insert if it doesn't exist
        upstreams.insert(id.clone(), UpstreamConnection::new(address));
        Ok(())
    }

    // Maintain connections (called from Router)
    pub async fn initialize_connections(&self) {
        self.maintain_connections().await;
    }
    #[allow(dead_code)] // This method is kept for future use
    pub async fn handle_upstream_failure(&mut self, upstream_id: &str) {
        {
            let mut upstreams = self.upstreams.lock().await;

            if let Some(upstream) = upstreams.get_mut(upstream_id) {
                upstream.is_active = false;
                warn!(
                    "Upstream {} disconnected, attempting reconnection",
                    upstream_id
                );
            }
        } // Drop the guard here

        // Redistribute hashrate among remaining active upstreams
        self.rebalance_on_failure().await;
    }
    #[allow(dead_code)] // This method is kept for future use
    async fn rebalance_on_failure(&mut self) {
        let upstreams = self.upstreams.lock().await;
        let active_count = upstreams.values().filter(|u| u.is_active).count();

        if active_count == 0 {
            error!("All upstreams disconnected - proxy will retry connections");
            return;
        }

        info!(
            "Rebalancing hashrate across {} active upstreams",
            active_count
        );
        // Implement rebalancing logic here
    }
}
