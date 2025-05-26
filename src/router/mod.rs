use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::jd_client::job_declarator::{setup_connection::SetupConnectionHandler, JobDeclarator};
use codec_sv2::{buffer_sv2::Slice, HandshakeRole};
use demand_share_accounting_ext::parser::PoolExtMessages;
use demand_sv2_connection::noise_connection_tokio::Connection;
use key_utils::Secp256k1PublicKey;
use noise_sv2::Initiator;
use roles_logic_sv2::{common_messages_sv2::SetupConnection, parsers::Mining};
use tokio::{
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
};
use tracing::{error, info};

use crate::{
    minin_pool_connection::{self, get_mining_setup_connection_msg, mining_setup_connection},
    proxy_state::ProxyState, // Add ProxyState import
    shared::utils::AbortOnDrop,
};

/// Router handles connection to Multiple upstreams.
pub struct Router {
    pool_addresses: Vec<SocketAddr>,
    auth_keys: Vec<Secp256k1PublicKey>, // Store auth keys separately to map to addresses
    current_pool: Option<SocketAddr>,
    auth_pub_k: Secp256k1PublicKey,
    setup_connection_msg: Option<SetupConnection<'static>>,
    timer: Option<Duration>,
    use_round_robin: bool, // Add flag to control selection strategy
}

impl Router {
    /// Creates a new `Router` instance with the specified upstream addresses.
    pub fn new(
        pool_addresses: Vec<SocketAddr>,
        auth_pub_k: Secp256k1PublicKey,
        // Configuration msg used to setup connection between client and pool
        // If not, present `get_mining_setup_connection_msg()` is called to generated default values
        setup_connection_msg: Option<SetupConnection<'static>>,
        // Max duration for pool setup after which it times out.
        // If None, default time of 5s is used.
        timer: Option<Duration>,
    ) -> Self {
        // Create auth_keys vector with the same key for all addresses
        let auth_keys = vec![auth_pub_k.clone(); pool_addresses.len()];
        
        Self {
            pool_addresses,
            auth_keys,
            current_pool: None,
            auth_pub_k,
            setup_connection_msg,
            timer,
            use_round_robin: false, // Default to latency-based selection
        }
    }
    
    /// Creates a new Router with multiple upstream addresses and auth keys
    pub fn new_multi(
        pool_address_keys: Vec<(SocketAddr, Secp256k1PublicKey)>,
        setup_connection_msg: Option<SetupConnection<'static>>,
        timer: Option<Duration>,
        use_round_robin: bool,
    ) -> Result<Self, &'static str> {
        if pool_address_keys.is_empty() {
            return Err("Cannot create router with empty pool_address_keys");
        }
        
        let mut addresses = Vec::new();
        let mut keys = Vec::new();
        
        for (addr, key) in &pool_address_keys {
            addresses.push(*addr);
            keys.push(key.clone());
        }
        
        // Use the first key as the default auth_pub_k
        let default_key = pool_address_keys[0].1.clone();
        
        // Register all upstreams in the ProxyState
        for (idx, (addr, key)) in pool_address_keys.iter().enumerate() {
            let id = format!("upstream-{}", idx);
            ProxyState::add_upstream_connection(
                id.clone(),
                format!("{:?}", addr),
                *addr,
                key.clone(),
                crate::proxy_state::UpstreamType::JDCMiningUpstream,
            );
        }
        
        Ok(Self {
            pool_addresses: addresses,
            auth_keys: keys,
            current_pool: None,
            auth_pub_k: default_key,
            setup_connection_msg,
            timer,
            use_round_robin,
        })
    }
    
    /// Enable or disable round-robin upstream selection
    pub fn set_round_robin(&mut self, enabled: bool) {
        self.use_round_robin = enabled;
    }

    /// Internal function to select pool with the least latency.
    async fn select_pool(&self) -> Option<(SocketAddr, Duration)> {
        // If round-robin is enabled, use ProxyState to get the next upstream
        if self.use_round_robin {
            if let Some((id, addr, _)) = ProxyState::get_next_upstream() {
                info!("Round-robin selected upstream {}: {:?}", id, addr);
                // Return with a dummy zero latency
                return Some((addr, Duration::from_millis(0)));
            }
        }
        
        // Fall back to latency-based selection
        let mut best_pool = None;
        let mut least_latency = Duration::MAX;

        for &pool_addr in &self.pool_addresses {
            if let Ok(latency) = self.get_latency(pool_addr).await {
                if latency < least_latency {
                    least_latency = latency;
                    best_pool = Some(pool_addr)
                }
            }
        }

        best_pool.map(|pool| (pool, least_latency))
    }

    /// Select the best pool for connection
    pub async fn select_pool_connect(&mut self) -> Option<SocketAddr> {
        info!("Selecting the best upstream");
        
        if self.use_round_robin {
            // Check if we have any registered upstreams in ProxyState
            if let Some((id, addr, _)) = ProxyState::get_next_upstream() {
                info!("Round-robin selected upstream {}: {:?}", id, addr);
                return Some(addr);
            }
            
            // If no upstreams registered in ProxyState yet, register them now
            if !self.pool_addresses.is_empty() {
                for (idx, (addr, key)) in self.pool_addresses.iter().zip(self.auth_keys.iter()).enumerate() {
                    let id = format!("upstream-{}", idx);
                    ProxyState::add_upstream_connection(
                        id,
                        format!("{:?}", addr),
                        *addr,
                        key.clone(),
                        crate::proxy_state::UpstreamType::JDCMiningUpstream,
                    );
                    // Mark all as initially connected
                    ProxyState::set_upstream_connection_status(&format!("upstream-{}", idx), true);
                }
                
                // Now try again to get a round-robin selection
                if let Some((_, addr, _)) = ProxyState::get_next_upstream() {
                    return Some(addr);
                }
            }
        }
        
        // Fall back to latency-based selection
        if let Some((pool, latency)) = self.select_pool().await {
            info!("Latency for upstream {:?} is {:?}", pool, latency);
            Some(pool)
        } else {
            None
        }
    }

    /// Select the best pool for monitoring
    async fn select_pool_monitor(&self, epsilon: Duration) -> Option<SocketAddr> {
        // If using round-robin, just get the next upstream
        if self.use_round_robin {
            if let Some((_, addr, _)) = ProxyState::get_next_upstream() {
                // Only switch if it's different from the current one
                if Some(addr) != self.current_pool {
                    return Some(addr);
                }
                return None;
            }
        }
        
        // Otherwise use latency-based selection
        if let Some((best_pool, best_pool_latency)) = self.select_pool().await {
            if let Some(current_pool) = self.current_pool {
                if best_pool == current_pool {
                    return None;
                }
                let current_latency = match self.get_latency(current_pool).await {
                    Ok(latency) => latency,
                    Err(e) => {
                        error!("Failed to get latency: {:?}", e);
                        return None;
                    }
                };
                if best_pool_latency < (current_latency - epsilon) {
                    info!(
                        "Found faster pool: {:?} with latency {:?}",
                        best_pool, best_pool_latency
                    );
                    return Some(best_pool);
                } else {
                    return None;
                }
            } else {
                return Some(best_pool);
            }
        }
        None
    }

    /// Selects the best upstream and connects to.
    /// Uses minin_pool_connection::connect_pool
    pub async fn connect_pool(
        &mut self,
        pool_addr: Option<SocketAddr>,
    ) -> Result<
        (
            tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
            tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>,
            AbortOnDrop,
        ),
        minin_pool_connection::errors::Error,
    > {
        let pool = match pool_addr {
            Some(addr) => addr,
            None => match self.select_pool_connect().await {
                Some(addr) => addr,
                // Called when we initialize the proxy, without a valid pool we can not start mine and we
                // return Err
                None => {
                    return Err(minin_pool_connection::errors::Error::Unrecoverable);
                }
            },
        };
        self.current_pool = Some(pool);

        info!("Upstream {:?} selected", pool);
        
        // Find the matching auth key for this address
        let auth_key = if let Some(index) = self.pool_addresses.iter().position(|&a| a == pool) {
            self.auth_keys[index].clone()
        } else {
            self.auth_pub_k.clone()
        };

        match minin_pool_connection::connect_pool(
            pool,
            auth_key,
            self.setup_connection_msg.clone(),
            self.timer,
        )
        .await
        {
            Ok((send_to_pool, recv_from_pool, pool_connection_abortable)) => {
                // Update ProxyState with successful connection
                let upstream_id = format!("upstream-{}", 
                    self.pool_addresses.iter().position(|&a| a == pool).unwrap_or(0));
                ProxyState::set_upstream_connection_status(&upstream_id, true);
                
                Ok((send_to_pool, recv_from_pool, pool_connection_abortable))
            }

            Err(e) => {
                // Update ProxyState with failed connection
                let upstream_id = format!("upstream-{}", 
                    self.pool_addresses.iter().position(|&a| a == pool).unwrap_or(0));
                ProxyState::set_upstream_connection_status(&upstream_id, false);
                
                Err(e)
            }
        }
    }

    /// Returns the sum all the latencies for a given upstream
    async fn get_latency(&self, pool_address: SocketAddr) -> Result<Duration, ()> {
        // Find the auth key for this address
        let auth_pub_key = if let Some(index) = self.pool_addresses.iter().position(|&a| a == pool_address) {
            self.auth_keys[index].clone()
        } else {
            self.auth_pub_k.clone()
        };
        
        let mut pool = PoolLatency::new(pool_address);
        let setup_connection_msg = self.setup_connection_msg.as_ref();
        let timer = self.timer.as_ref();

        // Rest of the function remains the same
        tokio::time::timeout(
            Duration::from_secs(15),
            PoolLatency::get_mining_setup_latencies(
                &mut pool,
                setup_connection_msg.cloned(),
                timer.cloned(),
                auth_pub_key,
            ),
        )
        .await
        .map_err(|_| {
            error!(
                "Failed to get mining setup latencies for {:?}: Timeout",
                pool_address
            );
        })??;

        // Rest of the function remains unchanged
        if (PoolLatency::get_mining_setup_latencies(
            &mut pool,
            setup_connection_msg.cloned(),
            timer.cloned(),
            auth_pub_key,
        )
        .await)
            .is_err()
        {
            error!(
                "Failed to get mining setup latencies for: {:?}",
                pool_address
            );
            return Err(());
        }
        if (PoolLatency::get_jd_latencies(&mut pool, auth_pub_key).await).is_err() {
            error!("Failed to get jd setup latencies for: {:?}", pool_address);
            return Err(());
        }

        let latencies = [
            pool.open_sv2_mining_connection,
            pool.setup_a_channel,
            pool.receive_first_job,
            pool.receive_first_set_new_prev_hash,
            pool.open_sv2_jd_connection,
            pool.get_a_mining_token,
        ];
        // Get sum of all latencies for pool
        let sum_of_latencies: Duration = latencies.iter().flatten().sum();
        Ok(sum_of_latencies)
    }

    /// Checks for faster upstream switch to it if found
    pub async fn monitor_upstream(&mut self, epsilon: Duration) -> Option<SocketAddr> {
        // If using round-robin, just get the next upstream
        if self.use_round_robin {
            if let Some((_, addr, _)) = ProxyState::get_next_upstream() {
                if Some(addr) != self.current_pool {
                    info!("Switching to next round-robin upstream {:?}", addr);
                    return Some(addr);
                }
                return None;
            }
        }
        
        // Otherwise use latency-based selection
        if let Some(best_pool) = self.select_pool_monitor(epsilon).await {
            if Some(best_pool) != self.current_pool {
                info!("Switching to faster upstream {:?}", best_pool);
                return Some(best_pool);
            } else {
                return None;
            }
        }
        None
    }

    pub fn get_current_upstream(&self) -> Option<std::net::SocketAddr> {
        self.current_pool
    }
    
    // For compatibility with existing code
    pub fn is_current_upstream(&self, addr: &std::net::SocketAddr) -> bool {
        if let Some(current) = self.current_pool {
            return current == *addr;
        }
        false
    }
}

/// Track latencies for various stages of pool connection setup.
#[derive(Clone, Copy, Debug)]
struct PoolLatency {
    pool: SocketAddr,
    open_sv2_mining_connection: Option<Duration>,
    setup_a_channel: Option<Duration>,
    receive_first_job: Option<Duration>,
    receive_first_set_new_prev_hash: Option<Duration>,
    open_sv2_jd_connection: Option<Duration>,
    get_a_mining_token: Option<Duration>,
}

impl PoolLatency {
    // Create new `PoolLatency` given an upstream address
    fn new(pool: SocketAddr) -> PoolLatency {
        Self {
            pool,
            open_sv2_mining_connection: None,
            setup_a_channel: None,
            receive_first_job: None,
            receive_first_set_new_prev_hash: None,
            open_sv2_jd_connection: None,
            get_a_mining_token: None,
        }
    }

    /// Sets the `PoolLatency`'s `open_sv2_mining_connection`, `setup_channel_timer`, `receive_first_job`,
    /// and `receive_first_set_new_prev_hash`
    async fn get_mining_setup_latencies(
        &mut self,
        setup_connection_msg: Option<SetupConnection<'static>>,
        timer: Option<Duration>,
        authority_public_key: Secp256k1PublicKey,
    ) -> Result<(), ()> {
        // Set open_sv2_mining_connection latency
        let open_sv2_mining_connection_timer = Instant::now();
        match TcpStream::connect(self.pool).await {
            Ok(stream) => {
                self.open_sv2_mining_connection = Some(open_sv2_mining_connection_timer.elapsed());

                let (mut receiver, mut sender, setup_connection_msg) =
                    initialize_mining_connections(
                        setup_connection_msg,
                        stream,
                        authority_public_key,
                    )
                    .await?;

                // Set setup_channel latency
                let setup_channel_timer = Instant::now();
                let result = mining_setup_connection(
                    &mut receiver,
                    &mut sender,
                    setup_connection_msg,
                    timer.unwrap_or(Duration::from_secs(2)),
                )
                .await;
                match result {
                    Ok(_) => {
                        self.setup_a_channel = Some(setup_channel_timer.elapsed());
                        let (send_to_down, mut recv_from_down) = tokio::sync::mpsc::channel(10);
                        let (send_from_down, recv_to_up) = tokio::sync::mpsc::channel(10);
                        let channel = open_channel();
                        if send_from_down
                            .send(PoolExtMessages::Mining(channel))
                            .await
                            .is_err()
                        {
                            error!("Failed to send channel to pool");
                            return Err(());
                        }

                        let relay_up_task = minin_pool_connection::relay_up(recv_to_up, sender);
                        let relay_down_task =
                            minin_pool_connection::relay_down(receiver, send_to_down);

                        let timer = Instant::now();
                        let mut received_new_job = false;
                        let mut received_prev_hash = false;

                        while let Some(message) = recv_from_down.recv().await {
                            if let PoolExtMessages::Mining(Mining::NewExtendedMiningJob(
                                _new_ext_job,
                            )) = message.clone()
                            {
                                // Set receive_first_job latency
                                self.receive_first_job = Some(timer.elapsed());
                                received_new_job = true;
                            }
                            if let PoolExtMessages::Mining(Mining::SetNewPrevHash(_new_prev_hash)) =
                                message.clone()
                            {
                                // Set receive_first_set_new_prev_hash latency
                                self.receive_first_set_new_prev_hash = Some(timer.elapsed());
                                received_prev_hash = true;
                            }
                            // Both latencies have been set so we break the loop
                            if received_new_job && received_prev_hash {
                                break;
                            }
                        }
                        drop(relay_up_task);
                        drop(relay_down_task);

                        Ok(())
                    }
                    Err(e) => {
                        error!(
                            "Failed to get mining setup latency for pool {}: {:?}",
                            self.pool, e
                        );
                        Err(())
                    }
                }
            }
            _ => {
                error!("Failed to get mining setup latencies for: {:?}", self.pool);
                Err(())
            }
        }
    }

    /// Sets the `PoolLatency`'s `open_sv2_jd_connection` and `get_a_mining_token`
    async fn get_jd_latencies(
        &mut self,
        authority_public_key: Secp256k1PublicKey,
    ) -> Result<(), ()> {
        let address = self.pool;

        // Set open_sv2_jd_connection latency
        let open_sv2_jd_connection_timer = Instant::now();

        match tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                let tp = crate::TP_ADDRESS
                    .safe_lock(|tp| tp.clone())
                    .map_err(|_| error!(" TP_ADDRESS Mutex Corrupted"))?;
                if let Some(_tp_addr) = tp {
                    let initiator = Initiator::from_raw_k(authority_public_key.into_bytes())
                        // Safe expect Key is a constant and must be right
                        .expect("Unable to create initialtor");
                    let (mut receiver, mut sender, _, _) =
                        match Connection::new(stream, HandshakeRole::Initiator(initiator)).await {
                            Ok(connection) => connection,
                            Err(e) => {
                                error!("Failed to create jd connection: {:?}", e);
                                return Err(());
                            }
                        };
                    if let Err(e) =
                        SetupConnectionHandler::setup(&mut receiver, &mut sender, address).await
                    {
                        error!("Failed to setup connection: {:?}", e);
                        return Err(());
                    }

                    self.open_sv2_jd_connection = Some(open_sv2_jd_connection_timer.elapsed());

                    let (sender, mut _receiver) = tokio::sync::mpsc::channel(10);
                    let upstream =
                        match crate::jd_client::mining_upstream::Upstream::new(0, sender).await {
                            Ok(upstream) => upstream,
                            Err(e) => {
                                error!("Failed to create upstream: {:?}", e);
                                return Err(());
                            }
                        };

                    let (job_declarator, _aborter) = match JobDeclarator::new(
                        address,
                        authority_public_key.into_bytes(),
                        upstream,
                        false,
                    )
                    .await
                    {
                        Ok(new) => new,
                        Err(e) => {
                            error!("Failed to create job declarator: {:?}", e);
                            return Err(());
                        }
                    };

                    // Set get_a_mining_token latency
                    let get_a_mining_token_timer = Instant::now();
                    let _token = JobDeclarator::get_last_token(&job_declarator).await;
                    self.get_a_mining_token = Some(get_a_mining_token_timer.elapsed());
                } else {
                    self.open_sv2_jd_connection = Some(Duration::from_millis(0));
                    self.get_a_mining_token = Some(Duration::from_millis(0));
                }
                Ok(())
            }
            _ => Err(()),
        }
    }
}

// Helper functions
fn open_channel() -> Mining<'static> {
    roles_logic_sv2::parsers::Mining::OpenExtendedMiningChannel(
        roles_logic_sv2::mining_sv2::OpenExtendedMiningChannel {
            request_id: 0,
            max_target: binary_sv2::u256_from_int(u64::MAX),
            min_extranonce_size: 8,
            user_identity: "ABC"
                .to_string()
                .try_into()
                // This can never fail
                .expect("Failed to convert user identity to string"),
            nominal_hash_rate: 0.0,
        },
    )
}

async fn initialize_mining_connections(
    setup_connection_msg: Option<SetupConnection<'static>>,
    stream: TcpStream,
    authority_public_key: Secp256k1PublicKey,
) -> Result<
    (
        Receiver<codec_sv2::Frame<PoolExtMessages<'static>, Slice>>,
        Sender<codec_sv2::Frame<PoolExtMessages<'static>, Slice>>,
        SetupConnection<'static>,
    ),
    (),
> {
    let initiator =
        // Safe expect Key is a constant and must be right
        Initiator::from_raw_k(authority_public_key.into_bytes()).expect("Invalid authority key");
    let (receiver, sender, _, _) =
        match Connection::new(stream, HandshakeRole::Initiator(initiator)).await {
            Ok(connection) => connection,
            Err(e) => {
                error!("Failed to create mining connection: {:?}", e);
                return Err(());
            }
        };
    let setup_connection_msg =
        setup_connection_msg.unwrap_or(get_mining_setup_connection_msg(true));
    Ok((receiver, sender, setup_connection_msg))
}