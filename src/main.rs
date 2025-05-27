use clap::{Parser, ArgAction};
#[cfg(not(target_os = "windows"))]
use jemallocator::Jemalloc;
use router::Router;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
#[cfg(not(target_os = "windows"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use crate::shared::utils::AbortOnDrop;
use key_utils::Secp256k1PublicKey;
use lazy_static::lazy_static;
use proxy_state::{PoolState, ProxyState, TpState, TranslatorState};
use std::{net::ToSocketAddrs, time::Duration};
use tokio::sync::mpsc::channel;
use tracing::{error, info, warn};
mod api;
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
const MAIN_POOL_ADDRESS: &str = "mining.dmnd.work:2000";
//const TEST_POOL_ADDRESS: &str = "127.0.0.1:20000";
const TEST_POOL_ADDRESS: &str = "18.193.252.132:2000";
const MAIN_AUTH_PUB_KEY: &str = "9bQHWXsQ2J9TRFTaxRh3KjoxdyLRfWVEy25YHtKF8y8gotLoCZZ";
const TEST_AUTH_PUB_KEY: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
//const TP_ADDRESS: &str = "127.0.0.1:8442";
const DEFAULT_LISTEN_ADDRESS: &str = "0.0.0.0:32767";

lazy_static! {
    static ref ARGS: Args = Args::parse();
    // Remove or comment out the old TOKEN line:
    // static ref TOKEN: String = std::env::var("TOKEN").expect("Missing TOKEN environment variable");
    
    // Other existing lazy_static variables...
    static ref SV1_DOWN_LISTEN_ADDR: String =
        std::env::var("SV1_DOWN_LISTEN_ADDR").unwrap_or(DEFAULT_LISTEN_ADDRESS.to_string());
    static ref TP_ADDRESS: roles_logic_sv2::utils::Mutex<Option<String>> =
        roles_logic_sv2::utils::Mutex::new(None); // We'll set this from config
    static ref EXPECTED_SV1_HASHPOWER: f32 = {
        if let Some(value) = ARGS.downstream_hashrate {
            value
        } else {
            let env_var = std::env::var("EXPECTED_SV1_HASHPOWER").ok();
            env_var.and_then(|s| parse_hashrate(&s).ok())
                .unwrap_or(DEFAULT_SV1_HASHPOWER)
        }
    };
}

lazy_static! {
    pub static ref POOL_ADDRESS: &'static str = if ARGS.test {
        TEST_POOL_ADDRESS
    } else {
        MAIN_POOL_ADDRESS
    };
    pub static ref AUTH_PUB_KEY: &'static str = if ARGS.test {
        TEST_AUTH_PUB_KEY
    } else {
        MAIN_AUTH_PUB_KEY
    };
}
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    // Use test enpoint if test flag is provided
    #[clap(long)]
    test: bool,
    #[clap(long ="d", short ='d', value_parser = parse_hashrate)]
    downstream_hashrate: Option<f32>,
    #[clap(long = "loglevel", short = 'l', default_value = "info")]
    loglevel: String,
    #[clap(long = "nc", short = 'n', default_value = "off")]
    noise_connection_log: String,
    #[clap(long = "delay", default_value = "0")]
    delay: u64,
    #[clap(long = "interval", short = 'i', default_value = "120000")]
    adjustment_interval: u64,
    // New argument for multiple upstream servers
    #[clap(long = "upstream", short = 'u', action = ArgAction::Append)]
    upstream_servers: Option<Vec<String>>,
    // Option to enable round-robin distribution
    #[clap(long = "round-robin", short = 'r')]
    round_robin: bool,
    // In Args struct
#[clap(long = "monitor-hashrate", short = 'm')]
monitor_hashrate: bool,
    #[clap(long = "config", short = 'c')]
    pub config_file: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let log_level = match args.loglevel.to_lowercase().as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => args.loglevel,
        _ => {
            error!(
                "Invalid log level '{}'. Defaulting to 'info'.",
                args.loglevel
            );
            "info".to_string()
        }
    };

    let noise_connection_log_level = match args.noise_connection_log.as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => args.noise_connection_log,
        _ => {
            error!(
                "Invalid log level for noise_connection '{}' Defaulting to 'off'.",
                args.noise_connection_log
            );
            "off".to_string()
        }
    };

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
    // std::env::var("TOKEN").expect("Missing TOKEN environment variable");

    // Load configuration first
    let config = if let Some(config_path) = &ARGS.config_file {
        match Config::from_file(config_path) {
            Ok(config) => Some(config),
            Err(e) => {
                error!("Failed to load config file: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    // Get TOKEN from config or environment
    let token = if let Some(ref config) = config {
        config.token.clone()
    } else {
        std::env::var("TOKEN").unwrap_or_else(|_| {
            error!("TOKEN must be provided either in config file or as environment variable");
            std::process::exit(1);
        })
    };

  
    // Use configured hashrate if available
    let hashpower = if let Some(ref config) = config {
        if let Some(ref hashrate_str) = config.downstream_hashrate {
            parse_hashrate(hashrate_str).unwrap_or(*EXPECTED_SV1_HASHPOWER)
        } else {
            *EXPECTED_SV1_HASHPOWER
        }
    } else {
        *EXPECTED_SV1_HASHPOWER
    };
    
    ProxyState::set_total_hashrate(hashpower);

    if args.downstream_hashrate.is_some() {
        info!(
            "Using downstream hashrate: {}h/s",
            HashUnit::format_value(hashpower)
        );
    } else {
        warn!(
            "No downstream hashrate provided, using default value: {}h/s",
            HashUnit::format_value(hashpower)
        );
    }

    // Handle multiple upstream servers from config or hard-coded fallback
    let mut pool_addresses = Vec::new();
    
    if let Some(ref config) = config {
        // Use configuration file pool addresses
        info!("Loading pool addresses from configuration file");
        
        for (idx, pool_addr_str) in config.pool_addresses.iter().enumerate() {
            // Parse address
            let addr = if let Ok(mut addrs) = pool_addr_str.to_socket_addrs() {
                addrs.next().unwrap_or_else(|| {
                    error!("Failed to resolve address: {}", pool_addr_str);
                    std::process::exit(1);
                })
            } else {
                error!("Invalid pool address: {}", pool_addr_str);
                std::process::exit(1);
            };
            
            // Use test public key for now (you might want to add this to config)
            let pubkey: Secp256k1PublicKey = TEST_AUTH_PUB_KEY
                .parse()
                .expect("Invalid test public key");
            
            info!("Adding upstream server {}: {}", idx + 1, addr);
            pool_addresses.push((addr, pubkey.clone()));
            
            ProxyState::add_upstream_connection(
                format!("upstream-{}", idx),
                format!("config-pool-{}", idx + 1),
                addr,
                pubkey,
                crate::proxy_state::UpstreamType::JDCMiningUpstream,
            );
        }
    } else {
        // Fallback to hard-coded addresses (your existing code)
        info!("Using hard-coded pool addresses");
        
        let test_pubkey: Secp256k1PublicKey = TEST_AUTH_PUB_KEY
            .parse()
            .expect("Invalid test public key");
            
        let test_addr = if let Ok(addr) = "3.74.36.119:2000".to_socket_addrs() {
            addr.collect::<Vec<_>>()[0]
        } else {
            "3.74.36.119:2000".parse().expect("Invalid IP address")
        };
        
        // Add hard-coded pools
        for i in 0..2 {
            info!("Adding upstream server {}: {}", i + 1, test_addr);
            pool_addresses.push((test_addr, test_pubkey.clone()));
            ProxyState::add_upstream_connection(
                format!("upstream-{}", i),
                format!("test-pool-{}", i + 1),
                test_addr,
                test_pubkey.clone(),
                crate::proxy_state::UpstreamType::JDCMiningUpstream,
            );
        }
    }

    // Direct verification
    info!("DIRECT VERIFICATION: Checking hashrate distribution");
    let upstream_connections = ProxyState::get_upstream_connections();
    let active_count = upstream_connections.len();
    info!("Active upstreams: {}", active_count);

    if active_count > 0 && ARGS.round_robin {
        let hashrate_per_upstream = hashpower / active_count as f32;
        info!("Round-robin hashrate per upstream: {}", HashUnit::format_value(hashrate_per_upstream));
        
        for (upstream_id, addr, _) in upstream_connections {
            info!("Upstream {}: {} - allocated {}", 
                upstream_id, 
                addr,
                HashUnit::format_value(hashrate_per_upstream));
        }
    }

    // Set all upstreams as initially connected
    for (idx, (addr, _)) in pool_addresses.iter().enumerate() {
        let id = if idx == 0 && pool_addresses.len() == 1 {
            "upstream-default".to_string()
        } else {
            format!("upstream-{}", idx) 
        };
        ProxyState::set_upstream_connection_status(&id, true);
    }

    let pool_socket_addresses: Vec<std::net::SocketAddr> = pool_addresses.iter().map(|(addr, _)| *addr).collect();
    let auth_pub_keys: Vec<Secp256k1PublicKey> = pool_addresses.iter().map(|(_, pubkey)| pubkey.clone()).collect();
    let auth_pub_k = auth_pub_keys.first().expect("No public keys available").clone();
    let mut router = router::Router::new(pool_socket_addresses, auth_pub_k, None, None);
    let epsilon = Duration::from_millis(10);
    let best_upstream = router.select_pool_connect().await;
    initialize_proxy(&mut router, best_upstream, epsilon).await;
    info!("exiting");
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
}

async fn initialize_proxy(
    router: &mut Router,
    mut pool_addr: Option<std::net::SocketAddr>,
    epsilon: Duration,
) {
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

        let (from_jdc_to_share_accounter_send, from_jdc_to_share_accounter_recv) = channel(10);
        let (from_share_accounter_to_jdc_send, from_share_accounter_to_jdc_recv) = channel(10);
        let (jdc_to_translator_sender, jdc_from_translator_receiver, _) = translator_up_rx
            .recv()
            .await
            .expect("Translator failed before initialization");

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
        };
    }
}

async fn monitor(
    router: &mut Router,
    abort_handles: Vec<(AbortOnDrop, std::string::String)>,
    epsilon: Duration,
    server_handle: tokio::task::JoinHandle<()>,
) -> Reconnect {
    let mut should_check_upstreams_latency = 0;
    let mut distribution_check_counter = 0;
    
    loop {
        if distribution_check_counter >= 100 {
            distribution_check_counter = 0;
            
            // Add debug output to confirm this code runs
            info!("Generating hashrate distribution report...");
            
            info!("Hashrate Distribution Report:");
            info!("Total hashrate: {}", HashUnit::format_value(*EXPECTED_SV1_HASHPOWER));
            
            // Get upstream info directly from ProxyState
            if ARGS.round_robin {
                // In round-robin mode, check how many active upstreams we have
                let upstream_connections = ProxyState::get_upstream_connections();
                let active_count = upstream_connections.len();
                
                if active_count > 0 {
                    let hashrate_per_upstream = *EXPECTED_SV1_HASHPOWER / active_count as f32;
                    info!("Round-robin mode: {} active upstreams", active_count);
                    info!("Each upstream allocated: {}", HashUnit::format_value(hashrate_per_upstream));
                    
                    // List each upstream and its allocated hashrate
                    for (upstream_id, addr, _) in upstream_connections {
                        info!("Upstream {}: {} - allocated {}", 
                            upstream_id, 
                            addr,
                            HashUnit::format_value(hashrate_per_upstream));
                    }
                } else {
                    info!("No active upstreams found");
                }
            } else {
                // In latency-based mode
                info!("Latency-based mode: Using best upstream");
                if let Some(current_addr) = router.get_current_upstream() {
                    info!("Current upstream: {} - allocated {}", 
                        current_addr, 
                        HashUnit::format_value(*EXPECTED_SV1_HASHPOWER));
                } else {
                    info!("No upstream currently selected");
                }
            }
        }

        // Check if a better upstream exist every 100 seconds
        if should_check_upstreams_latency == 10 * 100 {
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

        // Increment the counters
        should_check_upstreams_latency += 1;
        distribution_check_counter += 1;
        
        // Make sure this line exists to give time for the loop
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

/// Parses a hashrate string (e.g., "10T", "2.5P", "500E") into an f32 value in h/s.
fn parse_hashrate(hashrate_str: &str) -> Result<f32, String> {
    let hashrate_str = hashrate_str.trim();
    if hashrate_str.is_empty() {
        return Err("Hashrate cannot be empty. Expected format: '<number><unit>' (e.g., '10T', '2.5P', '5E'".to_string());
    }

    let unit = hashrate_str.chars().last().unwrap_or(' ').to_string();
    let num = &hashrate_str[..hashrate_str.len().saturating_sub(1)];

    let num: f32 = num.parse().map_err(|_| {
        format!(
            "Invalid number '{}'. Expected format: '<number><unit>' (e.g., '10T', '2.5P', '5E')",
            num
        )
    })?;

    let multiplier = HashUnit::from_str(&unit)
        .map(|unit| unit.multiplier())
        .ok_or_else(|| format!(
            "Invalid unit '{}'. Expected 'T' (Terahash), 'P' (Petahash), or 'E' (Exahash). Example: '10T', '2.5P', '5E'",
            unit
        ))?;

    let hashrate = num * multiplier;

    if hashrate.is_infinite() || hashrate.is_nan() {
        return Err("Hashrate too large or invalid".to_string());
    }

    Ok(hashrate)
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
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub token: String,
    pub pool_addresses: Vec<String>,
    pub tp_address: Option<String>,
    pub interval: Option<u64>,
    pub delay: Option<u64>,
    pub downstream_hashrate: Option<String>,
    pub loglevel: Option<String>,
    pub nc_loglevel: Option<String>,
    pub test: Option<bool>,
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}
