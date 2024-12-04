use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

pub mod task_manager;

use crate::jd_client::job_declarator::{setup_connection::SetupConnectionHandler, JobDeclarator};
use codec_sv2::{buffer_sv2::Slice, HandshakeRole};
use demand_share_accounting_ext::parser::PoolExtMessages;
use demand_sv2_connection::noise_connection_tokio::Connection;
use key_utils::Secp256k1PublicKey;
use noise_sv2::Initiator;
use roles_logic_sv2::utils::Mutex;
use roles_logic_sv2::{common_messages_sv2::SetupConnection, parsers::Mining};
use task_manager::{PoolState, TaskManager};
use tokio::{
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
};
use tracing::{debug, error, info};

use crate::{
    minin_pool_connection::{self, get_mining_setup_connection_msg, mining_setup_connection},
    shared::utils::AbortOnDrop,
};

/// Router handles connection to Multiple upstreams.
pub struct Router {
    pool_addresses: Vec<SocketAddr>,
    current_pool: Option<SocketAddr>,
    auth_pub_k: Secp256k1PublicKey,
    setup_connection_msg: Option<SetupConnection<'static>>,
    timer: Option<Duration>,
    pub task_manager: Arc<Mutex<TaskManager>>,
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
        let task_manager = TaskManager::initialize();
        Self {
            pool_addresses,
            current_pool: None,
            auth_pub_k,
            setup_connection_msg,
            timer,
            task_manager,
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
        info!("Selecting the best upstream ");
        if let Some((pool, latency)) = self.select_pool().await {
            info!("Latency for upstream {:?} is {:?}", pool, latency);
            Some(pool)
        } else {
            info!("No available pool");
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
                let current_latency = self
                    .get_latency(current_pool)
                    .await
                    .expect("Error getting latency");
                if best_pool_latency < (current_latency - epsilon) {
                    debug!(
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
            AbortOnDrop,
        ),
        (),
    > {
        let pool = match pool_addr {
            Some(addr) => addr,
            None => self
                .select_pool_connect()
                .await
                .ok_or(())
                .expect("Failed to select pool"),
        };
        self.current_pool = Some(pool);

        info!("Upstream {:?} selected", pool);

        match minin_pool_connection::connect_pool(
            pool,
            self.auth_pub_k,
            self.setup_connection_msg.clone(),
            self.timer,
        )
        .await
        {
            Ok((send_to_pool, recv_from_pool, pool_connection_abortable)) => {
                // Successfully connected to the pool
                // Start watch_pool_state task to monitor pool's state
                TaskManager::watch_pool_state(self.task_manager.clone()).await;
                // Update the pool state to "Up" since the connection was successful
                if let Err(err) = self
                    .task_manager
                    .safe_lock(|t| t.update_pool_state(PoolState::Up))
                {
                    error!("Error updating pool state to Up: {:?}", err);
                }

                // Retrieve the abort handle for the pool state tasks
                let pool_state_aborter = match self.task_manager.safe_lock(|t| t.get_aborter()) {
                    Ok(Some(aborter)) => aborter,
                    Ok(None) => {
                        error!("Router abort handle is missing");
                        return Err(()); // Return an error if abort handle is not found
                    }
                    Err(err) => {
                        error!("Failed to retrieve router abort handle: {:?}", err);
                        return Err(());
                    }
                };

                Ok((
                    send_to_pool,
                    recv_from_pool,
                    pool_connection_abortable,
                    pool_state_aborter,
                ))
            }

            Err(_) => {
                // Connection to the pool failed
                // Update the pool state to "Down" since connection was not successful
                if let Err(err) = self
                    .task_manager
                    .safe_lock(|t| t.update_pool_state(PoolState::Down))
                {
                    error!("Error updating pool state to Down: {:?}", err);
                }
                Err(())
            }
        }
    }

    /// Returns the sum all the latencies for a given upstream
    async fn get_latency(&self, pool_address: SocketAddr) -> Result<Duration, ()> {
        let mut pool = PoolLatency::new(pool_address);
        let setup_connection_msg = self.setup_connection_msg.as_ref();
        let timer = self.timer.as_ref();
        let auth_pub_key = self.auth_pub_k;
        if (PoolLatency::get_mining_setup_latencies(
            &mut pool,
            setup_connection_msg.cloned(),
            timer.cloned(),
            auth_pub_key,
        )
        .await)
            .is_err()
        {
            return Err(());
        }
        if (PoolLatency::get_jd_latencies(&mut pool, auth_pub_key).await).is_err() {
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
        if let Some(best_pool) = self.select_pool_monitor(epsilon).await {
            if Some(best_pool) != self.current_pool {
                info!("Switching to faster upstreamn {:?}", best_pool);
                // Temporary set the PoolState to Down beacuse a better upstream was found
                if let Err(err) = self
                    .task_manager
                    .safe_lock(|t| t.update_pool_state(PoolState::Down))
                {
                    error!("Error updating pool state to Down: {:?}", err);
                }
                return Some(best_pool);
            } else {
                return None;
            }
        }
        None
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
        match tokio::time::timeout(
            Duration::from_secs(5),
            TcpStream::connect(crate::POOL_ADDRESS),
        )
        .await
        {
            Ok(Ok(stream)) => {
                self.open_sv2_mining_connection = Some(open_sv2_mining_connection_timer.elapsed());

                let (mut receiver, mut sender, setup_connection_msg) =
                    initialize_mining_connections(
                        setup_connection_msg,
                        stream,
                        authority_public_key,
                    )
                    .await;

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
                        send_from_down
                            .send(PoolExtMessages::Mining(channel))
                            .await
                            .expect("Failed to send msg to pool");

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
                    Err(_) => Err(()),
                }
            }
            _ => Err(()),
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
                if let Some(_tp_addr) = crate::TP_ADDRESS.as_ref() {
                    let initiator = Initiator::from_raw_k(authority_public_key.into_bytes())
                        .expect("Unable to create initialtor");
                    let (mut receiver, mut sender, _, _) =
                        Connection::new(stream, HandshakeRole::Initiator(initiator))
                            .await
                            .expect("impossible to connect");
                    SetupConnectionHandler::setup(&mut receiver, &mut sender, address)
                        .await
                        .expect("Failed to set up connection");

                    self.open_sv2_jd_connection = Some(open_sv2_jd_connection_timer.elapsed());

                    let (sender, mut _receiver) = tokio::sync::mpsc::channel(10);
                    let upstream = crate::jd_client::mining_upstream::Upstream::new(0, sender)
                        .await
                        .expect("Failed to create upstream");

                    let (job_declarator, _aborter) =
                        JobDeclarator::new(address, authority_public_key.into_bytes(), upstream)
                            .await
                            .expect("Failed to instantiate job declarator");

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
                .expect("Failed to convert user identity to string"),
            nominal_hash_rate: 0.0,
        },
    )
}

async fn initialize_mining_connections(
    setup_connection_msg: Option<SetupConnection<'static>>,
    stream: TcpStream,
    authority_public_key: Secp256k1PublicKey,
) -> (
    Receiver<codec_sv2::Frame<PoolExtMessages<'static>, Slice>>,
    Sender<codec_sv2::Frame<PoolExtMessages<'static>, Slice>>,
    SetupConnection<'static>,
) {
    let initiator =
        Initiator::from_raw_k(authority_public_key.into_bytes()).expect("Invalid authority key");
    let (receiver, sender, _, _) = Connection::new(stream, HandshakeRole::Initiator(initiator))
        .await
        .expect("Failed to create connection");
    let setup_connection_msg =
        setup_connection_msg.unwrap_or(get_mining_setup_connection_msg(true));
    (receiver, sender, setup_connection_msg)
}
