use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::{error, info};
use crate::{
    minin_pool_connection,
    shared::utils::AbortOnDrop,
    proxy_state::ProxyState,
    HashUnit,
};
use demand_share_accounting_ext::parser::PoolExtMessages;
use key_utils::Secp256k1PublicKey;

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

    fn update_allocation(&mut self, percentage: f32, total_hashrate: f64) {
        self.allocated_percentage = percentage;
        self.allocated_hashrate = total_hashrate * percentage as f64 / 100.0;
    }
}

impl MultiUpstreamManager {
    pub fn new(
        upstreams: Vec<SocketAddr>,
        auth_pub_k: Secp256k1PublicKey,
        setup_connection_msg: Option<roles_logic_sv2::common_messages_sv2::SetupConnection<'static>>,
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
                    info!("Connecting to upstream {}: {}", id, address);
                    match minin_pool_connection::connect_pool(
                        address,
                        auth_pub_k,
                        setup_connection_msg.clone(),
                        timer,
                    )
                    .await
                    {
                        Ok((_send, _recv, _abortable)) => {
                            info!("✅ Connected to upstream {} - maintaining connection", id);
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

    /// Set hashrate distribution for each upstream (percentages)
    pub async fn set_hashrate_distribution(&self, distribution: Vec<f32>) {
        println!("calling set_hashrate_distribution in multi manager with: {:?}", distribution);
        
        let mut upstreams = self.upstreams.lock().await;
        let total_hashrate = ProxyState::get_total_hashrate() as f64;
        
        println!("Total hashrate: {}", HashUnit::format_value(total_hashrate as f32));
        
        for (i, (_id, conn)) in upstreams.iter_mut().enumerate() {
            if let Some(&percentage) = distribution.get(i) {
                println!("setting upstream {} to {}%", i, percentage);
                conn.update_allocation(percentage, total_hashrate);
                info!(
                    "Upstream {}: allocated {} ({:.1}%)",
                    conn.address,
                    HashUnit::format_value(conn.allocated_hashrate as f32),
                    percentage
                );
            }
        }
    }

    pub async fn get_upstreams(&self) -> HashMap<String, UpstreamConnection> {
        self.upstreams.lock().await.clone()
    }
pub async fn get_detailed_connection_stats(&self) -> Vec<(String, bool, f32)> {
    let upstreams = self.upstreams.lock().await;
    
    upstreams
        .iter()
        .map(|(id, conn)| {
            println!(
                "Upstream {}: {}% allocation", 
                id, 
                conn.allocated_percentage
            );
            // Return the percentage, not the hashrate
            (id.clone(), conn.is_active, conn.allocated_percentage)
        })
        .collect()
}

    pub async fn get_hashrate_distribution(&self) -> Vec<f32> {
        let upstreams = self.upstreams.lock().await;
        upstreams
            .values()
            .map(|conn| conn.allocated_percentage)
            .collect()
    }

    pub async fn get_active_upstream_ids(&self) -> Vec<String> {
        let upstreams = self.upstreams.lock().await;
        upstreams
            .iter()
            .filter(|(_, conn)| conn.is_active)
            .map(|(id, _)| id.clone())
            .collect()
    }

    // Dummy implementations for sender/receiver if needed
    pub async fn get_aggregated_receiver(&self) -> Option<tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>> {
        None
    }

    pub async fn get_aggregated_sender(&self) -> tokio::sync::mpsc::Sender<PoolExtMessages<'static>> {
        let (sender, _) = tokio::sync::mpsc::channel(1);
        sender
    }

    // Add upstream (called from Router)
 pub async fn add_upstream(
    &self,
    id: String,
    address: SocketAddr,
    _key: Secp256k1PublicKey,
    _setup_connection_msg: Option<roles_logic_sv2::common_messages_sv2::SetupConnection<'static>>,
    _timer: Option<Duration>,
) -> Result<(), String> {
    let mut upstreams = self.upstreams.lock().await;
    
    // Check if upstream already exists - if so, don't overwrite it
    if upstreams.contains_key(&id) {
        println!("⚠️ Upstream {} already exists, not overwriting", id);
        return Ok(());
    }
    
    // Only insert if it doesn't exist
    upstreams.insert(id.clone(), UpstreamConnection::new(address));
    println!("✅ Added new upstream {}", id);
    Ok(())
}

    // Maintain connections (called from Router)
    pub async fn initialize_connections(&self) {
        self.maintain_connections().await;
    }
}