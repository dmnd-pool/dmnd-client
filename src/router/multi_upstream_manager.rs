use crate::{proxy_state::ProxyState, shared::utils::AbortOnDrop};
use demand_share_accounting_ext::parser::PoolExtMessages;
use key_utils::Secp256k1PublicKey;
use roles_logic_sv2::common_messages_sv2::SetupConnection;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct UpstreamConnection {
    pub id: String,
    pub address: SocketAddr,
    pub auth_key: Secp256k1PublicKey,
    pub sender: mpsc::Sender<PoolExtMessages<'static>>,
    pub is_active: bool,
    pub connection_handle: Option<AbortOnDrop>,
}

#[derive(Clone)]
pub struct MultiUpstreamManager {
    upstreams: Arc<Mutex<HashMap<String, UpstreamConnection>>>,
    aggregated_sender: tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
    aggregated_receiver: Arc<Mutex<Option<tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>>>>,
    // New field for custom hashrate distribution
    hashrate_distribution: Arc<Mutex<Vec<f32>>>,
}

impl MultiUpstreamManager {
    pub fn new(use_parallel: bool) -> (Self, tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(1000);
        let manager = Self {
            upstreams: Arc::new(Mutex::new(HashMap::new())),
            aggregated_sender: sender,
            aggregated_receiver: Arc::new(Mutex::new(None)), // Start with None
            hashrate_distribution: Arc::new(Mutex::new(Vec::new())), // Initialize empty
        };
        
        (manager, receiver)
    }

    /// Get the aggregated receiver (for use in the main loop)
    pub async fn get_aggregated_receiver(&self) -> Option<tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>> {
        let mut rx = self.aggregated_receiver.lock().await;
        rx.take()
    }
    
    /// Set the aggregated receiver (if needed to put it back)
    pub async fn set_aggregated_receiver(&self, receiver: tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>) {
        let mut rx = self.aggregated_receiver.lock().await;
        *rx = Some(receiver);
    }

    /// Add a new upstream connection
    pub async fn add_upstream(
        &self,
        id: String,
        address: SocketAddr,
        auth_key: Secp256k1PublicKey,
        setup_connection_msg: Option<SetupConnection<'static>>,
        timer: Option<Duration>,
    ) -> Result<(), String> {
        info!("🔍 ADD_UPSTREAM CALLED: {} -> {}", id, address);

        let mut upstreams = self.upstreams.lock().await;

        // Check if upstream already exists
        if upstreams.contains_key(&id) {
            warn!("Upstream {} already exists, skipping", id);
            return Ok(());
        }

        // Create upstream connection
        let upstream_connection = UpstreamConnection {
            id: id.clone(),
            address,
            auth_key,
            is_active: false, // Start as inactive, will be set to true when connected
            sender: self.aggregated_sender.clone(), // Use a proper sender here
            connection_handle: None, // Set to None initially
        };

        upstreams.insert(id.clone(), upstream_connection);
        info!("✅ Added upstream {} to manager", id);

        // Start connection task
        let upstreams_clone = self.upstreams.clone();
        let sender_clone = self.aggregated_sender.clone();

        tokio::spawn(async move {
            Self::connect_upstream(
                upstreams_clone,
                id,
                address,
                auth_key,
                setup_connection_msg,
                timer,
                sender_clone,
            )
            .await
        });

        Ok(())
    }

    /// Connect to a specific upstream
    async fn connect_upstream(
        upstreams: Arc<Mutex<HashMap<String, UpstreamConnection>>>,
        id: String,
        address: SocketAddr,
        auth_key: Secp256k1PublicKey,
        setup_connection_msg: Option<SetupConnection<'static>>,
        timer: Option<Duration>,
        sender: tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
    ) {
        info!("Connecting to upstream {}: {}", id, address);

        match crate::minin_pool_connection::connect_pool(
            address,
            auth_key,
            setup_connection_msg,
            timer,
        )
        .await
        {
            Ok((send_to_pool, recv_from_pool, _abort_handle)) => {
                info!("Successfully connected to upstream {}: {}", id, address);

                // Update upstream status in both MultiUpstreamManager and ProxyState
                {
                    let mut upstreams_lock = upstreams.lock().await;
                    if let Some(upstream) = upstreams_lock.get_mut(&id) {
                        upstream.is_active = true;
                    }
                }

                // Also update ProxyState
                crate::proxy_state::ProxyState::set_upstream_connection_status(&id, true);

                // Handle messages from this upstream
                Self::handle_upstream_messages(id.clone(), recv_from_pool, sender).await;

                // If we reach here, connection was lost
                warn!("Connection to upstream {} lost", id);

                // Mark as inactive
                {
                    let mut upstreams_lock = upstreams.lock().await;
                    if let Some(upstream) = upstreams_lock.get_mut(&id) {
                        upstream.is_active = false;
                    }
                }
                crate::proxy_state::ProxyState::set_upstream_connection_status(&id, false);
            }
            Err(e) => {
                error!("Failed to connect to upstream {}: {}", id, e);

                // Mark as inactive
                let mut upstreams_lock = upstreams.lock().await;
                if let Some(upstream) = upstreams_lock.get_mut(&id) {
                    upstream.is_active = false;
                }
                crate::proxy_state::ProxyState::set_upstream_connection_status(&id, false);
            }
        }
    }

    /// Handle messages from an upstream
    async fn handle_upstream_messages(
        upstream_id: String,
        mut receiver: tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>,
        sender: tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
    ) {
        info!("Starting message handler for upstream {}", upstream_id);

        while let Some(message) = receiver.recv().await {
            if let Err(e) = sender.send(message).await {
                error!(
                    "Failed to forward message from upstream {}: {}",
                    upstream_id, e
                );
                break;
            }
        }

        warn!("Message handler for upstream {} stopped", upstream_id);
    }

    /// Send a message to a specific upstream
    pub async fn send_to_upstream(
        &self,
        upstream_id: &str,
        message: PoolExtMessages<'static>,
    ) -> Result<(), String> {
        let connections = self.upstreams.lock().await;
        if let Some(connection) = connections.get(upstream_id) {
            if connection.is_active {
                connection
                    .sender
                    .send(message)
                    .await
                    .map_err(|e| format!("Failed to send to upstream {}: {}", upstream_id, e))?;
                Ok(())
            } else {
                Err(format!("Upstream {} is not active", upstream_id))
            }
        } else {
            Err(format!("Upstream {} not found", upstream_id))
        }
    }

    /// Send a message to the next upstream using the configured strategy
    pub async fn send_to_next_upstream(
        &self,
        message: PoolExtMessages<'static>,
    ) -> Result<(), String> {
        info!("Broadcasting to all upstreams (parallel mode)");
        let results = self.broadcast(message).await;

        // Check if any sends were successful
        let success_count = results
            .iter()
            .filter(|result| result.contains("Success"))
            .count();

        if success_count > 0 {
            info!("Successfully sent to {} upstreams", success_count);
            Ok(())
        } else {
            Err(format!("Failed to send to any upstreams: {:?}", results))
        }
    }

    /// Connect to upstream with retry logic
    async fn connect_upstream_with_retry(
        upstreams: Arc<Mutex<HashMap<String, UpstreamConnection>>>,
        id: String,
        address: SocketAddr,
        auth_key: Secp256k1PublicKey,
        setup_connection_msg: Option<SetupConnection<'static>>,
        timer: Option<Duration>,
        sender: tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
    ) {
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY: Duration = Duration::from_secs(5);

        while retry_count < MAX_RETRIES {
            info!(
                "Connecting to upstream {} (attempt {}/{}): {}",
                id,
                retry_count + 1,
                MAX_RETRIES,
                address
            );

            match crate::minin_pool_connection::connect_pool(
                address,
                auth_key,
                setup_connection_msg.clone(),
                timer,
            )
            .await
            {
                Ok((send_to_pool, recv_from_pool, _abort_handle)) => {
                    info!("Successfully connected to upstream {}: {}", id, address);

                    // Update upstream status
                    {
                        let mut upstreams_lock = upstreams.lock().await;
                        if let Some(upstream) = upstreams_lock.get_mut(&id) {
                            upstream.is_active = true;
                        }
                    }

                    // Handle messages from this upstream
                    Self::handle_upstream_messages(id.clone(), recv_from_pool, sender).await;
                    return; // Success, exit retry loop
                }
                Err(e) => {
                    error!(
                        "Failed to connect to upstream {} (attempt {}): {}",
                        id,
                        retry_count + 1,
                        e
                    );
                    retry_count += 1;

                    if retry_count < MAX_RETRIES {
                        info!("Retrying connection to {} in {:?}", id, RETRY_DELAY);
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            }
        }

        error!(
            "Failed to connect to upstream {} after {} attempts",
            id, MAX_RETRIES
        );

        // Mark as inactive after all retries failed
        let mut upstreams_lock = upstreams.lock().await;
        if let Some(upstream) = upstreams_lock.get_mut(&id) {
            upstream.is_active = false;
        }
    }
    /// Broadcast to all active upstreams (parallel execution)
    pub async fn broadcast(&self, message: PoolExtMessages<'static>) -> Vec<String> {
        let upstreams = self.upstreams.lock().await;
        let mut results = Vec::new();

        if upstreams.is_empty() {
            results.push("No upstreams configured".to_string());
            return results;
        }

        let active_upstreams: Vec<_> = upstreams
            .values()
            .filter(|upstream| upstream.is_active)
            .collect();

        if active_upstreams.is_empty() {
            results.push("No active upstreams available".to_string());
            return results;
        }

        for upstream in active_upstreams {
            match upstream.sender.send(message.clone()).await {
                Ok(_) => {
                    results.push(format!("Sent to {}: Success", upstream.id));
                }
                Err(e) => {
                    results.push(format!("Sent to {}: Failed - {}", upstream.id, e));
                    // Mark upstream as inactive if send fails
                    warn!(
                        "Upstream {} send failed, may need reconnection: {}",
                        upstream.id, e
                    );
                }
            }
        }

        results
    }

    /// Get all upstreams (for stats)
    pub async fn get_upstreams(&self) -> std::collections::HashMap<String, UpstreamConnection> {
        let upstreams = self.upstreams.lock().await;
        upstreams.clone()
    }

    /// Get list of active upstream IDs
    pub async fn get_active_upstream_ids(&self) -> Vec<String> {
        let upstreams = self.upstreams.lock().await;
        upstreams
            .values()
            .filter(|upstream| upstream.is_active)
            .map(|upstream| upstream.id.clone())
            .collect()
    }

    /// Mark an upstream as inactive
    pub async fn mark_upstream_inactive(&self, upstream_id: &str) {
        let mut connections = self.upstreams.lock().await;
        if let Some(connection) = connections.get_mut(upstream_id) {
            connection.is_active = false;
            ProxyState::set_upstream_connection_status(upstream_id, false);
            info!("Marked upstream {} as inactive", upstream_id);
        }
    }

    /// Remove an upstream connection
    pub async fn remove_upstream(&self, upstream_id: &str) {
        let mut connections = self.upstreams.lock().await;
        if let Some(connection) = connections.remove(upstream_id) {
            if let Some(handle) = connection.connection_handle {
                drop(handle); // This will abort the connection
            }
            ProxyState::set_upstream_connection_status(upstream_id, false);
            info!("Removed upstream {}", upstream_id);
        }
    }

    /// Get connection count
    pub async fn connection_count(&self) -> usize {
        let connections = self.upstreams.lock().await;
        connections.len()
    }

    /// Get active connection count
    pub async fn active_connection_count(&self) -> usize {
        let connections = self.upstreams.lock().await;
        connections.values().filter(|conn| conn.is_active).count()
    }

    /// Set custom hashrate distribution percentages
    pub async fn set_hashrate_distribution(&self, distribution: Vec<f32>) {
        let mut dist = self.hashrate_distribution.lock().await;
        *dist = distribution;
        info!("Set custom hashrate distribution: {:?}", *dist);
    }
    
    /// Get current hashrate distribution
    pub async fn get_hashrate_distribution(&self) -> Vec<f32> {
        self.hashrate_distribution.lock().await.clone()
    }
    
    /// Validate that distribution percentages add up to 100%
    pub fn validate_distribution(distribution: &[f32]) -> Result<(), String> {
        if distribution.is_empty() {
            return Err("Distribution cannot be empty".to_string());
        }
        
        let total: f32 = distribution.iter().sum();
        if (total - 100.0).abs() > 0.1 { // Allow small floating point errors
            return Err(format!(
                "Distribution percentages must add up to 100%, got {:.1}%", 
                total
            ));
        }
        
        for (idx, &percentage) in distribution.iter().enumerate() {
            if percentage < 0.0 || percentage > 100.0 {
                return Err(format!(
                    "Invalid percentage at index {}: {:.1}%. Must be between 0% and 100%", 
                    idx, percentage
                ));
            }
        }
        
        Ok(())
    }
}
