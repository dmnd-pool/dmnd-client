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
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{Receiver, Sender},
        watch,
    },
};
use tracing::{error, info};

use crate::{
    config::{Configuration, PoolConfig},
    minin_pool_connection::{self, get_mining_setup_connection_msg, mining_setup_connection},
    shared::utils::AbortOnDrop,
};

/// Router handles connection to Multiple upstreams.
use std::sync::Arc;

#[derive(Clone)]
pub struct Router {
    pool_addresses: Vec<SocketAddr>,
    pub current_pool: Option<SocketAddr>,
    auth_pub_k: Secp256k1PublicKey,
    setup_connection_msg: Option<SetupConnection<'static>>,
    timer: Option<Duration>,
    latency_tx: watch::Sender<Option<Duration>>,
    pub latency_rx: watch::Receiver<Option<Duration>>,
    multi_pool_configs: Option<Vec<PoolConfig>>,
    pub weighted_dist: Option<Arc<Vec<AtomicU32>>>,
}

impl Router {
    /// Creates a new `Router` instance with the specified upstream addresses.
    pub async fn new(
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

        let multi_pool_configs = Configuration::pool_configs().await;
        let weighted_dist = multi_pool_configs
            .as_ref()
            .map(|configs| Arc::new((0..configs.len()).map(|_| AtomicU32::new(0)).collect()));

        Self {
            pool_addresses,
            current_pool: None,
            auth_pub_k,
            setup_connection_msg,
            timer,
            latency_tx,
            latency_rx,
            multi_pool_configs,
            weighted_dist,
        }
    }

    pub fn is_multi_upstream(&self) -> bool {
        self.multi_pool_configs
            .as_ref()
            .is_some_and(|configs| configs.len() > 1)
    }

    pub async fn assign_miner_to_pool(&self) -> Option<SocketAddr> {
        if self.is_multi_upstream() {
            if let Some(configs) = &self.multi_pool_configs {
                let assignments = self.weighted_dist.as_ref().unwrap();
                let total_miners: u32 = assignments.iter().map(|a| a.load(Ordering::Relaxed)).sum();

                let mut best_pool_id = 0;
                let mut best_difference = f32::NEG_INFINITY;

                for (pool_id, config) in configs.iter().enumerate() {
                    let current_count = assignments[pool_id].load(Ordering::Relaxed);
                    let current_ratio = if total_miners == 0 {
                        0.0
                    } else {
                        current_count as f32 / total_miners as f32
                    };
                    let target_ratio = config.weight;
                    let difference = target_ratio - current_ratio;

                    if difference > best_difference {
                        best_difference = difference;
                        best_pool_id = pool_id;
                    }
                }

                // Assign to most under-represented pool
                let new_count = assignments[best_pool_id].fetch_add(1, Ordering::Relaxed) + 1;
                let total_after = total_miners + 1;
                let selected_pool = configs[best_pool_id].address;

                info!(
                    "✅ Miner {} → Pool {} ({}) [Pool:{}, Total:{}]",
                    total_after, best_pool_id, selected_pool, new_count, total_after
                );

                Some(selected_pool)
            } else {
                // Fallback to existing logic
                self.current_pool
                    .or_else(|| self.pool_addresses.first().copied())
            }
        } else {
            // Single pool fallback
            self.current_pool
                .or_else(|| self.pool_addresses.first().copied())
        }
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

    /// Select the best pool for connection
    pub async fn select_pool_connect(&self) -> Option<SocketAddr> {
        info!("Selecting best Pool for connection");
        if self.pool_addresses.is_empty() {
            error!("No pool addresses provided");
            return None;
        }
        if self.pool_addresses.len() == 1 {
            info!(
                "Only one pool address available, using: {:?}",
                self.pool_addresses[0]
            );
            return Some(self.pool_addresses[0]);
        }
        if let Some((pool, latency)) = self.select_pool().await {
            info!("Latency for Pool {:?} is {:?}", pool, latency);
            self.latency_tx.send_replace(Some(latency)); // update latency
            Some(pool)
        } else {
            None
        }
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
                        return None;
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

        info!("Trying to connect to Pool {:?}", pool);

        match minin_pool_connection::connect_pool(
            pool,
            self.auth_pub_k,
            self.setup_connection_msg.clone(),
            self.timer,
        )
        .await
        {
            Ok((send_to_pool, recv_from_pool, pool_connection_abortable)) => {
                crate::POOL_ADDRESS
                    .safe_lock(|pool_address| {
                        *pool_address = Some(pool);
                    })
                    .unwrap_or_else(|_| {
                        error!("Pool address Mutex corrupt");
                        crate::proxy_state::ProxyState::update_inconsistency(Some(1));
                    });
                info!(
                    "Completed Handshake And SetupConnection with Pool at {:?}",
                    pool
                );

                Ok((send_to_pool, recv_from_pool, pool_connection_abortable))
            }

            Err(e) => Err(e),
        }
    }

    /// Returns the sum all the latencies for a given upstream
    async fn get_latency(&self, pool_address: SocketAddr) -> Result<Duration, ()> {
        let mut pool = PoolLatency::new(pool_address);
        let setup_connection_msg = self.setup_connection_msg.as_ref();
        let timer = self.timer.as_ref();
        let auth_pub_key = self.auth_pub_k;

        tokio::time::timeout(
            Duration::from_secs(8),
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
        if self.is_multi_upstream() {
            info!("Multi-upstream mode: No monitoring required");
            return None;
        }
        if let Some(best_pool) = self.select_pool_monitor(epsilon).await {
            if Some(best_pool) != self.current_pool {
                info!("Switching to faster upstreamn {:?}", best_pool);
                return Some(best_pool);
            } else {
                return None;
            }
        }
        None
    }

    /// Connect to all configured pools
    pub async fn connect_all_pools(
        &self,
    ) -> Vec<(
        usize,
        SocketAddr,
        Result<
            (
                tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
                tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>,
                crate::shared::utils::AbortOnDrop,
            ),
            crate::minin_pool_connection::errors::Error,
        >,
    )> {
        if let Some(configs) = &self.multi_pool_configs {
            let mut connections = Vec::new();
            for (id, config) in configs.iter().enumerate() {
                info!(
                    "Connecting to pool {} with weight {}",
                    config.address, config.weight
                );
                let result = self.clone().connect_pool(Some(config.address)).await;
                connections.push((id, config.address, result));
            }
            connections
        } else {
            Vec::new()
        }
    }

    /// Removes a miner from the assigned pool, allowing for rebalancing
    pub async fn remove_miner_from_pool(&self, pool_address: SocketAddr) {
        if let Some(configs) = &self.multi_pool_configs {
            if let Some(pool_id) = configs.iter().position(|c| c.address == pool_address) {
                let assignments = self.weighted_dist.as_ref().unwrap();
                let old = assignments[pool_id].load(Ordering::Relaxed);
                if old > 0 {
                    let new = assignments[pool_id].fetch_sub(1, Ordering::Relaxed) - 1;
                    let total: u32 = assignments.iter().map(|a| a.load(Ordering::Relaxed)).sum();
                    info!(
                        "Miner disconnected from Pool {}. New: {}/{} total",
                        pool_id, new, total
                    );
                }
            }
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

                        let relay_up_task =
                            minin_pool_connection::relay_up(recv_to_up, sender, self.pool);
                        let relay_down_task =
                            minin_pool_connection::relay_down(receiver, send_to_down, self.pool);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PoolConfig;
    use key_utils::Secp256k1PublicKey;
    use std::net::SocketAddr;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    const TEST_AUTH_PUB_KEY: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";

    async fn setup_multi_upstream_router() -> Router {
        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        let pool_addresses = vec![
            "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:12346".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:12347".parse::<SocketAddr>().unwrap(),
        ];
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None).await;

        // Mock multi-upstream configuration
        router.multi_pool_configs = Some(vec![
            PoolConfig {
                address: "127.0.0.1:12345".parse().unwrap(),
                weight: 0.5,
            },
            PoolConfig {
                address: "127.0.0.1:12346".parse().unwrap(),
                weight: 0.3,
            },
            PoolConfig {
                address: "127.0.0.1:12347".parse().unwrap(),
                weight: 0.2,
            },
        ]);
        router.weighted_dist = Some(Arc::new(vec![
            AtomicU32::new(0),
            AtomicU32::new(0),
            AtomicU32::new(0),
        ]));

        router
    }

    #[tokio::test]
    async fn test_is_multi_upstream_detection() {
        let router = setup_multi_upstream_router().await;
        assert!(router.is_multi_upstream());

        // Test single upstream
        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        let single_pool = vec!["127.0.0.1:12345".parse::<SocketAddr>().unwrap()];
        let mut single_router = Router::new(single_pool, auth_pub_k, None, None).await;
        single_router.multi_pool_configs = Some(vec![PoolConfig {
            address: "127.0.0.1:12345".parse().unwrap(),
            weight: 1.0,
        }]);
        assert!(!single_router.is_multi_upstream());
    }

    #[tokio::test]
    async fn test_assign_miner_weighted_distribution() {
        let router = setup_multi_upstream_router().await;

        // First miner should go to pool with highest weight (0.5)
        let pool1 = router.assign_miner_to_pool().await;
        assert_eq!(pool1, Some("127.0.0.1:12345".parse().unwrap()));

        // Check that counter is updated
        let assignments = router.weighted_dist.as_ref().unwrap();
        assert_eq!(assignments[0].load(Ordering::Relaxed), 1);
        assert_eq!(assignments[1].load(Ordering::Relaxed), 0);
        assert_eq!(assignments[2].load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_assign_miner_load_balancing() {
        let router = setup_multi_upstream_router().await;
        let assignments = router.weighted_dist.as_ref().unwrap();

        // Pre-assign miners to create imbalance
        assignments[0].store(10, Ordering::Relaxed); // Pool 0: 10 miners (weight 0.5)
        assignments[1].store(0, Ordering::Relaxed); // Pool 1: 0 miners (weight 0.3)
        assignments[2].store(0, Ordering::Relaxed); // Pool 2: 0 miners (weight 0.2)

        // Next assignment should go to pool 1 (most under-represented)
        let pool = router.assign_miner_to_pool().await;
        assert_eq!(pool, Some("127.0.0.1:12346".parse().unwrap()));
        assert_eq!(assignments[1].load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_multi_upstream_no_monitoring() {
        let mut router = setup_multi_upstream_router().await;

        let result = router.monitor_upstream(Duration::from_millis(100)).await;

        // Should return None for multi-upstream mode (no monitoring needed)
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_remove_miner_from_pool_multi_upstream() {
        let router = setup_multi_upstream_router().await;
        let assignments = router.weighted_dist.as_ref().unwrap();
        let pool_addr = "127.0.0.1:12345".parse().unwrap();

        // Pre-assign miners
        assignments[0].store(5, Ordering::Relaxed);
        assignments[1].store(3, Ordering::Relaxed);
        assignments[2].store(2, Ordering::Relaxed);

        // Remove miner from pool 0
        router.remove_miner_from_pool(pool_addr).await;

        // Pool 0 should have one less miner
        assert_eq!(assignments[0].load(Ordering::Relaxed), 4);
        assert_eq!(assignments[1].load(Ordering::Relaxed), 3);
        assert_eq!(assignments[2].load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_weighted_distribution_large_scale() {
        let router = setup_multi_upstream_router().await;
        let assignments = router.weighted_dist.as_ref().unwrap();

        // Assign 100 miners and check distribution
        for _ in 0..100 {
            router.assign_miner_to_pool().await;
        }

        let total: u32 = assignments.iter().map(|a| a.load(Ordering::Relaxed)).sum();
        assert_eq!(total, 100);

        // Check that distribution roughly matches weights
        let pool0_ratio = assignments[0].load(Ordering::Relaxed) as f32 / 100.0;
        let pool1_ratio = assignments[1].load(Ordering::Relaxed) as f32 / 100.0;
        let pool2_ratio = assignments[2].load(Ordering::Relaxed) as f32 / 100.0;

        // Should be approximately correct (within reasonable tolerance)
        assert!(pool0_ratio >= 0.4 && pool0_ratio <= 0.6); // Target: 0.5
        assert!(pool1_ratio >= 0.2 && pool1_ratio <= 0.4); // Target: 0.3
        assert!(pool2_ratio >= 0.1 && pool2_ratio <= 0.3); // Target: 0.2
    }
}
