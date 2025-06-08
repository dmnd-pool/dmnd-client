use crate::{minin_pool_connection, proxy_state::ProxyState, HashUnit};
use key_utils::Secp256k1PublicKey;
use rand;
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

#[derive(Debug)]
pub struct UpstreamConnection {
    pub address: SocketAddr,
    pub is_active: bool,
    pub allocated_percentage: f32,
    // Add connection channels if needed
    pub sender: Option<
        tokio::sync::mpsc::Sender<demand_share_accounting_ext::parser::PoolExtMessages<'static>>,
    >,
    pub receiver: Option<
        tokio::sync::mpsc::Receiver<demand_share_accounting_ext::parser::PoolExtMessages<'static>>,
    >,
}

impl UpstreamConnection {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            is_active: false,
            allocated_percentage: 0.0,
            sender: None,
            receiver: None,
        }
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
                    info!("Attempting to connect to upstream: {} ({})", id, address);
                    match minin_pool_connection::connect_pool(
                        address,
                        auth_pub_k,
                        setup_connection_msg.clone(),
                        timer,
                    )
                    .await
                    {
                        Ok((send, recv, _abortable)) => {
                            info!("Successfully connected to upstream: {} ({})", id, address);

                            // Store both sender and receiver
                            {
                                let mut upstreams_guard = upstreams.lock().await;
                                if let Some(upstream) = upstreams_guard.get_mut(&id) {
                                    upstream.sender = Some(send);
                                    upstream.receiver = Some(recv); // Store receiver too!
                                    upstream.is_active = true;
                                    info!("Stored sender and receiver for upstream: {}", id);
                                }
                            }

                            // Keep connection alive
                            loop {
                                tokio::time::sleep(Duration::from_secs(30)).await;
                            }
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
        drop(upstreams_guard);
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
        let upstreams = self.upstreams.lock().await;
        upstreams
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    UpstreamConnection {
                        address: v.address,
                        is_active: v.is_active,
                        allocated_percentage: v.allocated_percentage,
                        sender: v.sender.clone(),
                        receiver: None, // receiver can't be cloned, set to None
                    },
                )
            })
            .collect()
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

    pub async fn start_multi_upstream_share_accounting(
        &self,
        mut from_translator_recv: tokio::sync::mpsc::Receiver<
            roles_logic_sv2::parsers::Mining<'static>,
        >,
        to_translator_send: tokio::sync::mpsc::Sender<roles_logic_sv2::parsers::Mining<'static>>, // Remove underscore
    ) -> Result<crate::shared::utils::AbortOnDrop, Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting multi-upstream share accounting");

        // Clone the manager for use in the async task
        let manager = self.clone();

        let share_accounting_task = tokio::spawn({
            async move {
                info!("Multi-upstream share accounting task started");

                let mut message_count = 0;
                let response_count = 0;

                // Start response handlers for each upstream
                manager
                    .start_upstream_response_handlers(to_translator_send.clone())
                    .await;

                loop {
                    tokio::select! {
                        // Handle outbound messages FROM translator TO upstreams
                        Some(message) = from_translator_recv.recv() => {
                            message_count += 1;
                            info!("Received message #{} from translator", message_count);

                            // Route message to appropriate upstream based on hashrate distribution
                            manager.route_message_to_upstream(message).await;
                        }

                        // Keep task alive
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                            debug!("Multi-upstream share accounting keepalive (out: {}, in: {} messages)", message_count, response_count);
                        }

                        else => {
                            warn!("All channels closed, stopping share accounting");
                            break;
                        }
                    }
                }

                info!(
                    "Multi-upstream share accounting task finished (out: {}, in: {} messages)",
                    message_count, response_count
                );
            }
        });

        // Create AbortOnDrop to manage the task
        Ok(crate::shared::utils::AbortOnDrop::new(
            share_accounting_task,
        ))
    }

    pub async fn route_message_to_upstream(
        &self,
        message: roles_logic_sv2::parsers::Mining<'static>,
    ) {
        let upstreams_guard = self.upstreams.lock().await;
        let active_upstreams: Vec<(String, f32)> = upstreams_guard
            .iter()
            .filter_map(|(id, conn)| {
                if conn.is_active {
                    Some((id.clone(), conn.allocated_percentage))
                } else {
                    None
                }
            })
            .collect();

        if active_upstreams.is_empty() {
            warn!("No active upstreams available for message routing");
            return;
        }

        let total_percentage: f32 = active_upstreams.iter().map(|(_, pct)| pct).sum();
        if total_percentage <= 0.0 {
            warn!("Total upstream percentage is 0, cannot distribute");
            return;
        }

        // Weighted random
        let random_value = rand::random::<f32>() * total_percentage;
        let mut cumulative = 0.0;
        let mut selected_upstream: Option<String> = None;

        for (upstream_id, percentage) in &active_upstreams {
            cumulative += percentage;
            if random_value <= cumulative {
                selected_upstream = Some(upstream_id.clone());
                break;
            }
        }

        if let Some(upstream_id) = selected_upstream {
            let percentage = active_upstreams
                .iter()
                .find(|(id, _)| id == &upstream_id)
                .map(|(_, pct)| *pct)
                .unwrap_or(0.0);

            let message_type = match &message {
                roles_logic_sv2::parsers::Mining::OpenStandardMiningChannel(_) => {
                    "OpenStandardMiningChannel"
                }
                roles_logic_sv2::parsers::Mining::OpenExtendedMiningChannel(_) => {
                    "OpenExtendedMiningChannel"
                }
                roles_logic_sv2::parsers::Mining::SubmitSharesStandard(_) => "SubmitSharesStandard",
                roles_logic_sv2::parsers::Mining::SubmitSharesExtended(_) => "SubmitSharesExtended",
                _ => "Other",
            };

            debug!(
                "Routing {} message to upstream: {} ({:.1}%)",
                message_type, upstream_id, percentage
            );

            if let Some(upstream) = upstreams_guard.get(&upstream_id) {
                if let Some(ref sender) = upstream.sender {
                    let pool_message =
                        demand_share_accounting_ext::parser::PoolExtMessages::Mining(message);
                    if let Err(e) = sender.send(pool_message).await {
                        warn!("Failed to send message to upstream {}: {}", upstream_id, e);
                    } else {
                        info!(
                            "Successfully routed {} to {} ({:.1}%)",
                            message_type, upstream_id, percentage
                        );
                    }
                } else {
                    warn!("No sender available for upstream {}", upstream_id);
                }
            }
        }

        drop(upstreams_guard);
    }
    /// Start response handlers for all upstream connections
    async fn start_upstream_response_handlers(
        &self,
        to_translator_send: tokio::sync::mpsc::Sender<roles_logic_sv2::parsers::Mining<'static>>,
    ) {
        let upstreams = self.upstreams.lock().await;

        for (upstream_id, _conn) in upstreams.iter() {
            let upstream_id = upstream_id.clone();
            let upstreams = self.upstreams.clone();
            let to_translator_send = to_translator_send.clone();

            tokio::spawn(async move {
                loop {
                    // Try to get the receiver for this upstream
                    let mut receiver_opt = None;
                    {
                        let mut upstreams_guard = upstreams.lock().await;
                        if let Some(upstream) = upstreams_guard.get_mut(&upstream_id) {
                            if upstream.is_active && upstream.receiver.is_some() {
                                receiver_opt = upstream.receiver.take(); // Take ownership
                            }
                        }
                    }

                    if let Some(mut receiver) = receiver_opt {
                        info!("Starting response handler for upstream: {}", upstream_id);

                        // Listen for responses from this upstream
                        loop {
                            tokio::select! {
                                Some(response) = receiver.recv() => {
                                    debug!("Received response from upstream: {}", upstream_id);

                                    // Convert PoolExtMessages back to Mining message
                                    if let demand_share_accounting_ext::parser::PoolExtMessages::Mining(mining_response) = response {
                                        // Forward response back to translator (and then to cpuminer)
                                        if let Err(e) = to_translator_send.send(mining_response).await {
                                            warn!("Failed to send response to translator from {}: {}", upstream_id, e);
                                            break;
                                        } else {
                                            debug!("Successfully forwarded response from {} to translator", upstream_id);
                                        }
                                    }
                                }

                                _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                                    // Check if upstream is still active
                                    let upstreams_guard = upstreams.lock().await;
                                    if let Some(upstream) = upstreams_guard.get(&upstream_id) {
                                        if !upstream.is_active {
                                            warn!("Upstream {} became inactive, stopping response handler", upstream_id);
                                            break;
                                        }
                                    }
                                }

                                else => {
                                    warn!("Response channel closed for upstream: {}", upstream_id);
                                    break;
                                }
                            }
                        }

                        // Put receiver back if upstream is still active
                        {
                            let mut upstreams_guard = upstreams.lock().await;
                            if let Some(upstream) = upstreams_guard.get_mut(&upstream_id) {
                                if upstream.is_active {
                                    upstream.receiver = Some(receiver);
                                }
                            }
                        }
                    } else {
                        // No receiver available, wait and retry
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            });
        }
    }
}
