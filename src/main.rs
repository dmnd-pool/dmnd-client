#[cfg(not(target_os = "windows"))]
use jemallocator::Jemalloc;
use router::Router;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
#[cfg(not(target_os = "windows"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use crate::shared::utils::AbortOnDrop;
use config::Configuration;
use key_utils::Secp256k1PublicKey;
use lazy_static::lazy_static;
use proxy_state::{PoolState, ProxyState, TpState, TranslatorState}; // Add ProxyStates
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc::channel;
use tracing::{error, info, warn};
mod api;

mod config;
mod ingress;
pub mod jd_client;
mod minin_pool_connection;
mod proxy_state;
mod router;
mod share_accounter;
mod shared;
mod translator;

const TRANSLATOR_BUFFER_SIZE: usize = 32;
const MIN_EXTRANONCE_SIZE: u16 = 6;
const MIN_EXTRANONCE2_SIZE: u16 = 5;
const UPSTREAM_EXTRANONCE1_SIZE: usize = 15;
const DEFAULT_SV1_HASHPOWER: f32 = 100_000_000_000_000.0;
const SHARE_PER_MIN: f32 = 10.0;
const CHANNEL_DIFF_UPDTATE_INTERVAL: u32 = 10;
const MAX_LEN_DOWN_MSG: u32 = 10000;
const MAIN_AUTH_PUB_KEY: &str = "9bQHWXsQ2J9TRFTaxRh3KjoxdyLRfWVEy25YHtKF8y8gotLoCZZ";
const TEST_AUTH_PUB_KEY: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
const DEFAULT_LISTEN_ADDRESS: &str = "0.0.0.0:32767";

lazy_static! {
    static ref SV1_DOWN_LISTEN_ADDR: String =
        Configuration::downstream_listening_addr().unwrap_or(DEFAULT_LISTEN_ADDRESS.to_string());
    static ref TP_ADDRESS: roles_logic_sv2::utils::Mutex<Option<String>> =
        roles_logic_sv2::utils::Mutex::new(Configuration::tp_address());
    static ref POOL_ADDRESS: roles_logic_sv2::utils::Mutex<Option<SocketAddr>> =
        roles_logic_sv2::utils::Mutex::new(None); // Connected pool address
    static ref EXPECTED_SV1_HASHPOWER: f32 = Configuration::downstream_hashrate();
    static ref API_SERVER_PORT: String = Configuration::api_server_port();
}

lazy_static! {
    pub static ref AUTH_PUB_KEY: &'static str = if Configuration::test() {
        TEST_AUTH_PUB_KEY
    } else {
        MAIN_AUTH_PUB_KEY
    };
}

#[tokio::main]
async fn main() {
    let log_level = Configuration::loglevel();

    let noise_connection_log_level = Configuration::nc_loglevel();

    //Disable noise_connection error (for now) because:
    // 1. It produce logs that are not very user friendly and also bloat the logs
    // 2. The errors resulting from noise_connection are handled. E.g if unrecoverable error from noise connection occurs during Pool connection: We either retry connecting immediatley or we update Proxy state to Pool Down
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::new(format!(
            "{},demand_sv2_connection::noise_connection_tokio={}",
            log_level, noise_connection_log_level
        )))
        .init();
    Configuration::token().expect("TOKEN is not set");

    if Configuration::test() {
        info!("Connecting to test endpoint...");
    }

    let auth_pub_k: Secp256k1PublicKey = AUTH_PUB_KEY.parse().expect("Invalid public key");

    let pool_addresses = Configuration::pool_address()
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| panic!("Pool address is missing"));

    // Set downstream hashrate using Configuration pattern
    ProxyState::set_downstream_hashrate(Configuration::downstream_hashrate());

    // Get pool addresses with auth keys using Configuration pattern
    let pool_address_keys: Vec<(SocketAddr, Secp256k1PublicKey)> = pool_addresses
        .iter()
        .map(|&addr| (addr, auth_pub_k))
        .collect();

    // Determine hashrate distribution using Configuration pattern
    let wants_distribution = Configuration::wants_hashrate_distribution();

    // Create router based on configuration
    let mut router = if wants_distribution {
        // Multi-upstream with distribution
        match Router::new_multi(pool_address_keys.clone(), None, None, true).await {
            Ok(router) => router,
            Err(e) => {
                error!("Failed to create multi-upstream router: {}", e);
                std::process::exit(1);
            }
        }
    } else if pool_address_keys.len() > 1 {
        // Multiple pools, latency-based selection
        Router::new_with_keys(pool_address_keys.clone(), None, None)
    } else {
        // Single upstream (backward compatible)
        Router::new(pool_addresses, auth_pub_k, None, None)
    };

    let epsilon = Duration::from_millis(30_000);

    // Handle the three different scenarios

    if wants_distribution {
        // Get the distribution from config
        let distribution = Configuration::hashrate_distribution().unwrap_or_else(|| {
            let count = pool_address_keys.len();
            let equal_percentage = 100.0 / count as f32;
            vec![equal_percentage; count]
        });

        info!("Using hashrate distribution: {:?}", distribution);

        // Initialize upstream connections
        if let Err(e) = router.initialize_upstream_connections().await {
            error!("Failed to initialize upstream connections: {}", e);
            std::process::exit(1);
        }

        // Set the distribution
        if let Err(e) = router.set_hashrate_distribution(distribution.clone()).await {
            error!("Failed to set hashrate distribution: {}", e);
            std::process::exit(1);
        }

        // Wait for connections to establish
        tokio::time::sleep(Duration::from_secs(3)).await;
        initialize_proxy(&mut router, None, epsilon).await;
    } else if pool_address_keys.len() > 1 {
        // Test latency and select best pool
        let best_upstream = router.select_pool_connect().await;

        if let Some(ref _upstream) = best_upstream {
        } else {
            error!("Failed to connect to any upstream pool");
            std::process::exit(1);
        }

        initialize_proxy(&mut router, best_upstream, epsilon).await;
    } else {
        let best_upstream = router.select_pool_connect().await;
        if best_upstream.is_none() {
            error!("Failed to connect to upstream pool");
            std::process::exit(1);
        }
        initialize_proxy(&mut router, best_upstream, epsilon).await;
    }
}

async fn initialize_proxy(
    router: &mut Router,
    mut pool_addr: Option<std::net::SocketAddr>,
    epsilon: Duration,
) {
    // Check if we're in multi-upstream mode
    if router.is_multi_upstream_enabled() {
        // Initialize all the same components as single upstream mode
        let stats_sender = api::stats::StatsSender::new();

        // Start SV1 ingress to handle downstream (mining device) connections
        let (downs_sv1_tx, downs_sv1_rx) = channel(10);
        let sv1_ingress_abortable = ingress::sv1_ingress::start_listen_for_downstream(downs_sv1_tx);

        // Start translator to handle SV1 <-> SV2 translation
        let (translator_up_tx, mut translator_up_rx) = channel(10);
        let translator_abortable =
            match translator::start(downs_sv1_rx, translator_up_tx, stats_sender.clone()).await {
                Ok(abortable) => abortable,
                Err(e) => {
                    error!("Impossible to initialize translator: {e}");
                    ProxyState::update_translator_state(TranslatorState::Down);
                    ProxyState::update_tp_state(TpState::Down);
                    return;
                }
            };

        // Get translator channels
        let (jdc_to_translator_sender, jdc_from_translator_receiver, _) = translator_up_rx
            .recv()
            .await
            .expect("Translator failed before initialization");

        // Setup JDC channels with correct types
        let (from_jdc_to_share_accounter_send, from_jdc_to_share_accounter_recv) =
            channel::<roles_logic_sv2::parsers::Mining<'static>>(10);
        let (from_share_accounter_to_jdc_send, from_share_accounter_to_jdc_recv) =
            channel::<roles_logic_sv2::parsers::Mining<'static>>(10);

        // Check if TP is available first
        let tp_available = TP_ADDRESS.safe_lock(|tp| tp.clone()).ok().flatten();

        let (share_accounter_abortable, jdc_abortable) = if let Some(_tp_addr) = tp_available {
            // WITH TP: Start JDC first, then share accounting with JDC channels
            let jdc_handle = jd_client::start(
                jdc_from_translator_receiver,
                jdc_to_translator_sender,
                from_share_accounter_to_jdc_recv,
                from_jdc_to_share_accounter_send,
            )
            .await;

            if jdc_handle.is_some() {
                // JDC started successfully, use JDC channels
                match router
                    .start_multi_upstream_share_accounting_with_jdc(
                        from_jdc_to_share_accounter_recv,
                        from_share_accounter_to_jdc_send,
                    )
                    .await
                {
                    Ok(handle) => (handle, jdc_handle),
                    Err(e) => {
                        error!(
                            "Failed to start multi-upstream share accounting with JDC: {}",
                            e
                        );
                        return;
                    }
                }
            } else {
                error!("Failed to start JDC with TP");
                return;
            }
        } else {
            // WITHOUT TP: Use translator channels directly
            match router
                .start_multi_upstream_share_accounting(
                    jdc_from_translator_receiver,
                    jdc_to_translator_sender,
                )
                .await
            {
                Ok(handle) => (handle, None),
                Err(e) => {
                    error!("Failed to start multi-upstream share accounting: {}", e);
                    return;
                }
            }
        };

        // Start API server
        let server_handle = tokio::spawn(api::start(router.clone(), stats_sender));

        // Collect abort handles for monitoring
        let mut abort_handles = vec![
            (sv1_ingress_abortable, "sv1_ingress".to_string()),
            (translator_abortable, "translator".to_string()),
            (share_accounter_abortable, "share_accounter".to_string()),
        ];
        if let Some(jdc_handle) = jdc_abortable {
            abort_handles.push((jdc_handle, "jdc".to_string()));
        }

        // Use combined monitoring function
        monitor_multi_upstream(router.clone(), abort_handles, server_handle, epsilon).await;
        return;
    }

    // Single upstream mode only
    loop {
        // Initial setup for the proxy
        let stats_sender = api::stats::StatsSender::new();

        let (send_to_pool, recv_from_pool, pool_connection_abortable) =
            match router.connect_pool(pool_addr).await {
                Ok(connection) => connection,
                Err(_) => {
                    error!("No upstream available. Retrying...");
                    warn!("Are you using the correct TOKEN??");
                    let mut secs = 10;
                    while secs > 0 {
                        tracing::warn!("Retrying in {} seconds...", secs);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        secs -= 1;
                    }
                    // Restart loop, esentially restarting proxy
                    continue;
                }
            };

        let (downs_sv1_tx, downs_sv1_rx) = channel(10);
        let sv1_ingress_abortable = ingress::sv1_ingress::start_listen_for_downstream(downs_sv1_tx);

        let (translator_up_tx, mut translator_up_rx) = channel(10);
        let translator_abortable =
            match translator::start(downs_sv1_rx, translator_up_tx, stats_sender.clone()).await {
                Ok(abortable) => abortable,
                Err(e) => {
                    error!("Impossible to initialize translator: {e}");
                    // Impossible to start the proxy so we restart proxy
                    ProxyState::update_translator_state(TranslatorState::Down);
                    ProxyState::update_tp_state(TpState::Down);
                    return;
                }
            };

        // This creates the channels but translator has no upstream to connect to
        let (jdc_to_translator_sender, jdc_from_translator_receiver, _) = translator_up_rx
            .recv()
            .await
            .expect("Translator failed before initialization");

        let (from_jdc_to_share_accounter_send, from_jdc_to_share_accounter_recv) = channel(10);
        let (from_share_accounter_to_jdc_send, from_share_accounter_to_jdc_recv) = channel(10);
        let jdc_abortable: Option<AbortOnDrop>;
        let share_accounter_abortable;
        let tp = match TP_ADDRESS.safe_lock(|tp| tp.clone()) {
            Ok(tp) => tp,
            Err(e) => {
                error!("TP_ADDRESS Mutex Corrupted: {e}");
                return;
            }
        };

        if let Some(_tp_addr) = tp {
            jdc_abortable = jd_client::start(
                jdc_from_translator_receiver,
                jdc_to_translator_sender,
                from_share_accounter_to_jdc_recv,
                from_jdc_to_share_accounter_send,
            )
            .await;
            if jdc_abortable.is_none() {
                ProxyState::update_tp_state(TpState::Down);
            };
            share_accounter_abortable = match share_accounter::start(
                from_jdc_to_share_accounter_recv,
                from_share_accounter_to_jdc_send,
                recv_from_pool,
                send_to_pool,
            )
            .await
            {
                Ok(abortable) => abortable,
                Err(_) => {
                    error!("Failed to start share_accounter");
                    return;
                }
            }
        } else {
            jdc_abortable = None;

            share_accounter_abortable = match share_accounter::start(
                jdc_from_translator_receiver,
                jdc_to_translator_sender,
                recv_from_pool,
                send_to_pool,
            )
            .await
            {
                Ok(abortable) => abortable,
                Err(_) => {
                    error!("Failed to start share_accounter");
                    return;
                }
            };
        };

        // Collecting all abort handles
        let mut abort_handles = vec![
            (pool_connection_abortable, "pool_connection".to_string()),
            (sv1_ingress_abortable, "sv1_ingress".to_string()),
            (translator_abortable, "translator".to_string()),
            (share_accounter_abortable, "share_accounter".to_string()),
        ];
        if let Some(jdc_handle) = jdc_abortable {
            abort_handles.push((jdc_handle, "jdc".to_string()));
        }
        let server_handle = tokio::spawn(api::start(router.clone(), stats_sender));
        match monitor(router, abort_handles, epsilon, server_handle).await {
            Reconnect::NewUpstream(new_pool_addr) => {
                ProxyState::update_proxy_state_up();
                pool_addr = Some(new_pool_addr);
                continue;
            }
            Reconnect::NoUpstream => {
                ProxyState::update_proxy_state_up();
                pool_addr = None;
                continue;
            }
        }
    }
}

async fn monitor(
    router: &mut Router,
    abort_handles: Vec<(AbortOnDrop, std::string::String)>,
    epsilon: Duration,
    server_handle: tokio::task::JoinHandle<()>,
) -> Reconnect {
    let mut should_check_upstreams_latency = 0;
    loop {
        // Check if a better upstream exist every 100 seconds
        if should_check_upstreams_latency == 10 * 100 {
            should_check_upstreams_latency = 0;
            if let Some(new_upstream) = router.monitor_upstream(epsilon).await {
                info!("Faster upstream detected. Reinitializing proxy...");
                drop(abort_handles);
                server_handle.abort(); // abort server

                // Needs a little to time to drop
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                return Reconnect::NewUpstream(new_upstream);
            }
        }

        // Monitor finished tasks
        if let Some((_handle, name)) = abort_handles
            .iter()
            .find(|(handle, _name)| handle.is_finished())
        {
            error!("Task {:?} finished, Closing connection", name);
            for (handle, _name) in abort_handles {
                drop(handle);
            }
            server_handle.abort(); // abort server

            // Check if the proxy state is down, and if so, reinitialize the proxy.
            let is_proxy_down = ProxyState::is_proxy_down();
            if is_proxy_down.0 {
                error!(
                    "Status: {:?}. Reinitializing proxy...",
                    is_proxy_down.1.unwrap_or("Proxy".to_string())
                );
                return Reconnect::NoUpstream;
            } else {
                return Reconnect::NoUpstream;
            }
        }

        // Check if the proxy state is down, and if so, reinitialize the proxy.
        let is_proxy_down = ProxyState::is_proxy_down();
        if is_proxy_down.0 {
            error!(
                "{:?} is DOWN. Reinitializing proxy...",
                is_proxy_down.1.unwrap_or("Proxy".to_string())
            );
            drop(abort_handles); // Drop all abort handles
            server_handle.abort(); // abort server
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // Needs a little to time to drop
            return Reconnect::NoUpstream;
        }

        should_check_upstreams_latency += 1;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

// Combined monitoring function for multi-upstream mode
async fn monitor_multi_upstream(
    router: Router,
    abort_handles: Vec<(AbortOnDrop, String)>,
    server_handle: tokio::task::JoinHandle<()>,
    _epsilon: Duration,
) {
    let mut report_counter = 0;

    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Monitor finished tasks (critical components)
        if let Some((_handle, name)) = abort_handles
            .iter()
            .find(|(handle, _name)| handle.is_finished())
        {
            if name != "jdc" {
                error!("Critical task {:?} failed, restarting", name);
                for (handle, _name) in abort_handles {
                    drop(handle);
                }
                server_handle.abort();
                return;
            }
        }

        // Check proxy state - but be more lenient for multi-upstream mode
        let is_proxy_down = ProxyState::is_proxy_down();
        if is_proxy_down.0 {
            if let Some(ref status) = is_proxy_down.1 {
                let status_str = format!("{:?}", status);
                if !status_str.contains("Tp(Down)") && !status_str.contains("InternalInconsistency")
                {
                    error!("{:?} is DOWN, restarting", status);
                    drop(abort_handles);
                    server_handle.abort();
                    return;
                }
            }
        }

        if report_counter >= 10 {
            let detailed_stats = router.get_detailed_connection_stats().await;
            let active_count = detailed_stats
                .iter()
                .filter(|(_, active, _)| *active)
                .count();

            if active_count > 0 {
                info!("Active pools: {}", active_count);
                for (upstream_id, is_active, percentage) in &detailed_stats {
                    if *is_active && *percentage > 0.0 {
                        info!("  {} {:.1}%", upstream_id, percentage);
                    }
                }
            }
            report_counter = 0;
        } else {
            report_counter += 1;
        }
    }
}
pub enum Reconnect {
    NewUpstream(std::net::SocketAddr), // Reconnecting with a new upstream
    NoUpstream,                        // Reconnecting without upstream
}

enum HashUnit {
    Tera,
    Peta,
    Exa,
}

impl HashUnit {
    /// Returns the multiplier for each unit in h/s
    fn multiplier(&self) -> f32 {
        match self {
            HashUnit::Tera => 1e12,
            HashUnit::Peta => 1e15,
            HashUnit::Exa => 1e18,
        }
    }

    // Converts a unit string (e.g., "T") to a HashUnit variant
    fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "T" => Some(HashUnit::Tera),
            "P" => Some(HashUnit::Peta),
            "E" => Some(HashUnit::Exa),
            _ => None,
        }
    }

    /// Formats a hashrate value (f32) into a string with the appropriate unit
    fn format_value(hashrate: f32) -> String {
        if hashrate >= 1e18 {
            format!("{:.2}E", hashrate / 1e18)
        } else if hashrate >= 1e15 {
            format!("{:.2}P", hashrate / 1e15)
        } else if hashrate >= 1e12 {
            format!("{:.2}T", hashrate / 1e12)
        } else {
            format!("{:.2}", hashrate)
        }
    }
}
