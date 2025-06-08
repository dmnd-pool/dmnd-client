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
    sync::{
        mpsc::{Receiver, Sender},
        watch,
    },
};
use tracing::{debug, error, info};

use crate::{
    minin_pool_connection::{self, get_mining_setup_connection_msg, mining_setup_connection},
    proxy_state::ProxyState,
    shared::utils::AbortOnDrop,
};

// Add the new module
pub mod multi_upstream_manager;
pub use multi_upstream_manager::MultiUpstreamManager;

/// Router handles connection to Multiple upstreams.
pub struct Router {
    pool_addresses: Vec<SocketAddr>,
    keys: Vec<Secp256k1PublicKey>,
    pub current_pool: Option<SocketAddr>,
    upstream_manager: Option<MultiUpstreamManager>,

    // Keep these fields for backward compatibility with single upstream mode
    auth_pub_k: Secp256k1PublicKey,
    setup_connection_msg: Option<SetupConnection<'static>>,
    timer: Option<Duration>,
    latency_tx: tokio::sync::watch::Sender<Option<Duration>>,
    pub latency_rx: tokio::sync::watch::Receiver<Option<Duration>>,
}
impl Clone for Router {
    fn clone(&self) -> Self {
        Self {
            pool_addresses: self.pool_addresses.clone(),
            keys: self.keys.clone(),
            current_pool: self.current_pool,
            upstream_manager: self.upstream_manager.clone(),
            auth_pub_k: self.auth_pub_k,
            setup_connection_msg: self.setup_connection_msg.clone(),
            timer: self.timer,
            latency_tx: self.latency_tx.clone(),
            latency_rx: self.latency_rx.clone(),
        }
    }
}

impl Router {
    /// Creates a new `Router` instance with the specified upstream addresses.
    /// Now supports multiple pools with different auth keys for latency-based selection
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
        let (latency_tx, latency_rx) = watch::channel(None);
        let auth_pub_keys = vec![auth_pub_k; pool_addresses.len()];

        Self {
            pool_addresses,
            keys: auth_pub_keys,
            current_pool: None,
            upstream_manager: None,
            auth_pub_k,
            setup_connection_msg,
            timer,
            latency_tx,
            latency_rx,
        }
    }

    /// Creates a new Router with multiple upstream addresses and auth keys
    /// Support both latency-based selection and custom distribution
    pub fn new_with_keys(
        pool_address_keys: Vec<(SocketAddr, Secp256k1PublicKey)>,
        setup_connection_msg: Option<SetupConnection<'static>>,
        timer: Option<Duration>,
    ) -> Self {
        let pool_addresses: Vec<SocketAddr> =
            pool_address_keys.iter().map(|(addr, _)| *addr).collect();
        let keys: Vec<Secp256k1PublicKey> = pool_address_keys.iter().map(|(_, key)| *key).collect();
        let auth_pub_k = keys[0]; // Use first key as primary

        let (latency_tx, latency_rx) = watch::channel(None);

        Self {
            pool_addresses,
            keys,
            current_pool: None,
            upstream_manager: None,
            auth_pub_k,
            setup_connection_msg,
            timer,
            latency_tx,
            latency_rx,
        }
    }

    /// Creates a new Router with multiple upstream addresses and auth keys
    /// This now supports both latency-based selection and custom distribution
    pub async fn new_multi(
        pool_address_keys: Vec<(SocketAddr, Secp256k1PublicKey)>,
        setup_connection_msg: Option<SetupConnection<'static>>,
        timer: Option<Duration>,
        use_distribution: bool, // Changed from use_parallel to use_distribution
    ) -> Result<Self, &'static str> {
        let pool_addresses: Vec<SocketAddr> =
            pool_address_keys.iter().map(|(addr, _)| *addr).collect();
        let keys: Vec<Secp256k1PublicKey> = pool_address_keys.iter().map(|(_, key)| *key).collect();

        // Create upstream manager only if we want custom distribution
        let upstream_manager = if use_distribution {
            Some(MultiUpstreamManager::new(
                pool_addresses.clone(),
                keys[0],
                setup_connection_msg.clone(),
                timer,
            ))
        } else {
            None
        };

        // Use
        let auth_pub_k = keys
            .first()
            .copied()
            .ok_or("No authentication keys provided")?;

        let (latency_tx, latency_rx) = watch::channel(None);

        Ok(Self {
            pool_addresses,
            keys,
            current_pool: None,
            upstream_manager,
            auth_pub_k,
            setup_connection_msg,
            timer,
            latency_tx,
            latency_rx,
        })
    }

    /// Get detailed connection statistics
    pub async fn get_detailed_connection_stats(&self) -> Vec<(String, bool, f32)> {
        if let Some(ref manager) = self.upstream_manager {
            manager.get_detailed_connection_stats().await
        } else {
            vec![]
        }
    }

    /// Check if multi-upstream is enabled
    pub fn is_multi_upstream_enabled(&self) -> bool {
        self.upstream_manager.is_some()
    }

    /// Checks for faster upstream and switches to it if found
    pub async fn monitor_upstream(&mut self, epsilon: Duration) -> Option<SocketAddr> {
        // For multi-upstream mode, we don't switch since we use all simultaneously
        if self.is_multi_upstream_enabled() {
            // In distribution mode, we don't need to switch upstreams
            // All upstreams are used simultaneously
            return None;
        }

        // For single upstream mode, check for better latency
        if let Some(best_pool) = self.select_pool_monitor(epsilon).await {
            if Some(best_pool) != self.current_pool {
                info!("Switching to faster upstream {:?}", best_pool);
                return Some(best_pool);
            }
        }
        None
    }

    /// Internal function to select pool with the least latency.
    async fn select_pool(&self) -> Option<(SocketAddr, Duration)> {
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
    /// Select the best pool for monitoring
    async fn select_pool_monitor(&self, epsilon: Duration) -> Option<SocketAddr> {
        if let Some((best_pool, best_pool_latency)) = self.select_pool().await {
            if let Some(current_pool) = self.current_pool {
                if best_pool == current_pool {
                    return None;
                }
                let current_latency = match self.get_latency(current_pool).await {
                    Ok(latency) => latency,
                    Err(e) => {
                        error!("Failed to get latency: {:?}", e);
                        return Some(best_pool);
                    }
                };
                // saturating_sub is used to avoid panic on negative duration result
                if best_pool_latency < current_latency.saturating_sub(epsilon) {
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

    /// Select the best pool for connection
    pub async fn select_pool_connect(&mut self) -> Option<SocketAddr> {
        info!("Selecting the best upstream");

        // Remove round-robin logic - only use latency-based selection
        if let Some((pool, latency)) = self.select_pool().await {
            info!("Latency for upstream {:?} is {:?}", pool, latency);
            self.latency_tx.send_replace(Some(latency));
            Some(pool)
        } else {
            None
        }
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

        // Find the matching auth key for this address - fix field name
        let auth_pub_key = if let Some(index) = self.pool_addresses.iter().position(|&a| a == pool)
        {
            self.keys[index]
        } else {
            self.auth_pub_k
        };

        match minin_pool_connection::connect_pool(
            pool,
            auth_pub_key,
            self.setup_connection_msg.clone(),
            self.timer,
        )
        .await
        {
            Ok((send_to_pool, recv_from_pool, pool_connection_abortable)) => {
                // Update ProxyState with successful connection
                let upstream_id = format!(
                    "upstream-{}",
                    self.pool_addresses
                        .iter()
                        .position(|&a| a == pool)
                        .unwrap_or(0)
                );
                ProxyState::set_upstream_connection_status(&upstream_id, true);

                // Update current pool address
                crate::POOL_ADDRESS
                    .safe_lock(|pool_address| {
                        *pool_address = Some(pool);
                    })
                    .unwrap_or_else(|_| {
                        error!("Pool address Mutex corrupt");
                        crate::proxy_state::ProxyState::update_inconsistency(Some(1));
                    });

                Ok((send_to_pool, recv_from_pool, pool_connection_abortable))
            }

            Err(e) => {
                // Update ProxyState with failed connection
                let upstream_id = format!(
                    "upstream-{}",
                    self.pool_addresses
                        .iter()
                        .position(|&a| a == pool)
                        .unwrap_or(0)
                );
                ProxyState::set_upstream_connection_status(&upstream_id, false);

                Err(e)
            }
        }
    }

    /// Start multi-upstream share accounting
    pub async fn start_multi_upstream_share_accounting(
        &self,
        from_translator_recv: tokio::sync::mpsc::Receiver<
            roles_logic_sv2::parsers::Mining<'static>,
        >,
        to_translator_send: tokio::sync::mpsc::Sender<roles_logic_sv2::parsers::Mining<'static>>,
    ) -> Result<crate::shared::utils::AbortOnDrop, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(ref manager) = self.upstream_manager {
            manager
                .start_multi_upstream_share_accounting(from_translator_recv, to_translator_send)
                .await
        } else {
            Err("Multi-upstream manager not initialized".into())
        }
    }

    /// Returns the sum all the latencies for a given upstream
    async fn get_latency(&self, pool_address: SocketAddr) -> Result<Duration, ()> {
        // Find the auth key for this address - fix field names
        let auth_pub_key =
            if let Some(index) = self.pool_addresses.iter().position(|&a| a == pool_address) {
                self.keys[index]
            } else {
                self.auth_pub_k
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

    /// Initialize upstream connections for the manager
    pub async fn initialize_upstream_connections(&mut self) -> Result<(), String> {
        if let Some(ref manager) = self.upstream_manager {
            debug!(
                "Initializing {} upstream connections",
                self.pool_addresses.len()
            );

            // Add each unique upstream only once - fix field names
            for (idx, (addr, key)) in self.pool_addresses.iter().zip(self.keys.iter()).enumerate() {
                let id = format!("upstream-{}", idx);

                debug!("Adding upstream {}: {} ({})", id, addr, key);

                if let Err(e) = manager
                    .add_upstream(
                        id.clone(),
                        *addr,
                        *key,
                        self.setup_connection_msg.clone(),
                        self.timer,
                    )
                    .await
                {
                    error!("Failed to add upstream {}: {:?}", id, e);
                }
            }

            // IMPORTANT: Initialize connections BEFORE any hashrate distribution is set
            // This ensures add_upstream() calls are complete and won't overwrite distribution
            manager.initialize_connections().await;

            Ok(())
        } else {
            Err("No upstream manager available".to_string())
        }
    }

    /// Sets the hashrate distribution for the upstream manager.
    pub async fn set_hashrate_distribution(
        &mut self,
        distribution: Vec<f32>,
    ) -> Result<(), &'static str> {
        if let Some(ref mut manager) = self.upstream_manager {
            let _ = manager.set_hashrate_distribution(distribution).await;
            Ok(())
        } else {
            Err("No upstream manager available")
        }
    }
    pub async fn start_multi_upstream_share_accounting_with_jdc(
        &self,
        from_jdc_recv: Receiver<roles_logic_sv2::parsers::Mining<'static>>,
        to_jdc_send: Sender<roles_logic_sv2::parsers::Mining<'static>>,
    ) -> Result<AbortOnDrop, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(ref manager) = self.upstream_manager {
            // Direct pass-through since types already match
            let manager_handle = manager
                .start_multi_upstream_share_accounting(from_jdc_recv, to_jdc_send)
                .await?;

            // No type conversion needed - just return the handle
            Ok(manager_handle)
        } else {
            Err("Multi-upstream manager not initialized".into())
        }
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
