use clap::{ArgAction, Parser};
#[cfg(not(target_os = "windows"))]
use jemallocator::Jemalloc;
use router::Router;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
// Add these missing imports
use serde::{Deserialize, Serialize};
use std::path::Path;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use crate::shared::utils::AbortOnDrop;
use key_utils::Secp256k1PublicKey;
use lazy_static::lazy_static;
use proxy_state::{PoolState, ProxyState};
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
   
    // In Args struct
    // #[clap(long = "monitor-hashrate", short = 'm')]
    // monitor_hashrate: bool,
    #[clap(long = "config", short = 'c')]
    pub config_file: Option<String>,
 

    /// Enable parallel upstream usage (sends to all upstreams simultaneously)
    #[clap(long = "parallel", short = 'p')]
    pub parallel: bool,
}

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
        info!("No config file provided, using default settings");
        None
    };
    // Get TOKEN from config or environment
    let _token = if let Some(ref config) = config {
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

            let pubkey: Secp256k1PublicKey =
                TEST_AUTH_PUB_KEY.parse().expect("Invalid test public key");

            info!("Adding upstream server {}: {}", idx + 1, addr);
            pool_addresses.push((addr, pubkey));

            // ONLY add to ProxyState, don't create connections yet
            ProxyState::add_upstream_connection(
                format!("upstream-{}", idx),
                format!("config-pool-{}", idx + 1),
                addr,
                pubkey,
                crate::proxy_state::UpstreamType::JDCMiningUpstream,
            );
        }
    }     
     else {
        // Fallback to hard-coded address
        info!("Using hard-coded fallback pool address");

        let test_addr = match "3.74.36.119:2000".to_socket_addrs() {
            Ok(mut addrs) => addrs.next().unwrap_or_else(|| {
                error!("Failed to resolve fallback pool address");
                std::process::exit(1);
            }),
            Err(_) => {
                error!("Invalid fallback pool address format");
                std::process::exit(1);
            }
        };

        let test_pubkey: Secp256k1PublicKey = TEST_AUTH_PUB_KEY
            .parse()
            .expect("Invalid fallback public key");

        info!("Adding fallback upstream server: {}", test_addr);
        pool_addresses.push((test_addr, test_pubkey));

        ProxyState::add_upstream_connection(
            "upstream-0".to_string(),
            "fallback-pool-1".to_string(),
            test_addr,
            test_pubkey,
            crate::proxy_state::UpstreamType::JDCMiningUpstream
        );
    }


    // Direct verification
    info!("DIRECT VERIFICATION: Checking hashrate distribution");
    let upstream_connections = ProxyState::get_upstream_connections();
    let active_count = upstream_connections.len();
    info!("Active upstreams: {}", active_count);

    // Remove round-robin hashrate calculation
    // if active_count > 0 && ARGS.round_robin {
    //     let hashrate_per_upstream = hashpower / active_count as f32;
    //     info!(
    //         "Round-robin hashrate per upstream: {}",
    //         HashUnit::format_value(hashrate_per_upstream)
    //     );
    //
    //     let upstream_connections: Vec<(
    //         String,
    //         std::net::SocketAddr,
    //         key_utils::Secp256k1PublicKey,
    //     )> = ProxyState::get_upstream_connections();
    //
    //     // And update the usage in the loop below:
    //     for (upstream_id, addr, _auth_key) in upstream_connections {
    //         info!(
    //             "Upstream {}: {} - allocated {}",
    //             upstream_id,
    //             addr,
    //             HashUnit::format_value(hashrate_per_upstream)
    //         );
    //     }
    // }
    
    // Create the router - always use multi upstream with parallel mode
    let mut router = if !pool_addresses.is_empty() {
        // Always use multi-upstream mode (even for single upstream)
        match router::Router::new_multi(
            pool_addresses.clone(),
            None, // setup_connection_msg
            None, // timer
            true, // Always enable parallel mode
        ).await {
            Ok(mut router) => {
                if let Err(e) = router.initialize_upstream_connections().await {
                    error!("Failed to initialize upstream connections: {}", e);
                    std::process::exit(1);
                }
                router
            },
            Err(e) => {
                error!("Failed to create multi-upstream router: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        error!("No pool addresses configured. Please provide pool addresses via config file.");
        std::process::exit(1);
    };

    let epsilon = Duration::from_millis(100);

    // Always use multi-upstream mode
    initialize_proxy(&mut router, None, epsilon).await;
    
    // Wait a moment for connections to establish
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Show immediate hashrate distribution with network verification
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    info!("🔍 VERIFYING NETWORK CONNECTIONS:");
    
    // Check network connections
    let output = std::process::Command::new("ss")
        .args(&["-tn"])
        .output();
        
    if let Ok(output) = output {
        let connections = String::from_utf8_lossy(&output.stdout);
        let pool_connections: Vec<&str> = connections
            .lines()
            .filter(|line| line.contains("18.193.252.132:2000") || line.contains("3.74.36.119:2000"))
            .collect();
            
        info!("📡 Active pool connections: {}", pool_connections.len());
        for conn in pool_connections {
            info!("  🔗 {}", conn.trim());
        }
    }
    
    // Show hashrate distribution
    let total_hashrate = ProxyState::get_total_hashrate();
    let detailed_stats = router.get_detailed_connection_stats().await;
    
    info!("🚀 === INITIAL HASHRATE DISTRIBUTION ===");
    info!("🔋 Total configured hashrate: {}", HashUnit::format_value(total_hashrate));
    info!("🌐 Mode: Parallel (each upstream gets full hashrate)");
    
    for (upstream_id, is_active, hashrate) in detailed_stats {
        if is_active {
            info!("  ✅ {}: receiving {} (100% of total)", upstream_id, HashUnit::format_value(hashrate));
        } else {
            info!("  ❌ {}: {} (INACTIVE)", upstream_id, HashUnit::format_value(hashrate));
        }
    }
    info!("========================================");
    
    // Start monitoring task for multi-upstream
    let router_clone = router.clone();
    tokio::spawn(async move {
        monitor_multi_upstream(router_clone, epsilon).await;
    });
    
    // Keep main thread alive
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        info!("Multi-upstream proxy running...");
    }
}

async fn initialize_proxy(
    router: &mut Router,
    mut _pool_addr: Option<std::net::SocketAddr>, // Add underscore to indicate unused
    _epsilon: Duration, // Add underscore to indicate unused
) {
    // Add a static flag to prevent multiple downstream listeners
    static DOWNSTREAM_STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    
    // For multi-upstream mode, don't loop - just set up and wait
    if router.is_multi_upstream_enabled() {
        info!("Multi-upstream mode: using aggregated message handling");
        
        // Only start downstream listener once
        if !DOWNSTREAM_STARTED.swap(true, std::sync::atomic::Ordering::SeqCst) {
            info!("Starting downstream listener (multi-upstream mode)");
            
            // Create channel for downstream connections
            let (downstreams_tx, mut downstreams_rx) = tokio::sync::mpsc::channel(10);
            
            // Start the downstream listener
            let _abort_handle = ingress::sv1_ingress::start_listen_for_downstream(downstreams_tx);
            
            // Handle incoming downstream connections
            tokio::spawn(async move {
                while let Some((send_to_downstream, recv_from_downstream, client_addr)) = downstreams_rx.recv().await {
                    info!("New downstream client connected: {}", client_addr);
                    
                    // Here you would typically start the translator for this downstream connection
                    // For now, we'll just log the connection
                    tokio::spawn(async move {
                        // Handle this specific downstream connection
                        // You'll need to integrate this with your translator logic
                        let mut recv = recv_from_downstream;
                        while let Some(message) = recv.recv().await {
                            info!("Received from downstream {}: {}", client_addr, message);
                            // Process the message and potentially send to upstreams
                        }
                        info!("Downstream client {} disconnected", client_addr);
                    });
                }
            });
        }
        
        // Get aggregated receiver if available
        if let Some(aggregated_receiver) = router.get_aggregated_receiver() {
            tokio::spawn(async move {
                let mut receiver = aggregated_receiver;
                while let Some(message) = receiver.recv().await {
                    info!("Received aggregated message from upstream: {:?}", message);
                }
            });
        }
        
        // Instead of looping, just wait indefinitely
        info!("Multi-upstream proxy initialized successfully");
        return; // Exit the function, don't loop
    }
    
    // For single upstream mode - just log and exit since we're not supporting it
    error!("Single upstream mode is not supported in this version. Please use multi-upstream mode with --config option.");
    std::process::exit(1);
}

async fn monitor_multi_upstream(router: Router, _epsilon: Duration) {
    let mut distribution_check_counter = 0;
    
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        distribution_check_counter += 1;
        
        // Show report every 30 seconds
        if distribution_check_counter >= 3 {
            distribution_check_counter = 0;
            
            let total_hashrate = ProxyState::get_total_hashrate();
            let detailed_stats = router.get_detailed_connection_stats().await;
            
            info!("📊 === HASHRATE DISTRIBUTION REPORT ===");
            info!("🔋 Total configured hashrate: {}", HashUnit::format_value(total_hashrate));
            info!("🌐 Parallel mode: Each upstream receives FULL hashrate");
            info!("📡 Upstream details:");
            
            let mut active_count = 0;
            for (upstream_id, is_active, hashrate) in detailed_stats {
                if is_active {
                    active_count += 1;
                    info!("  ✅ {}: {} (ACTIVE)", upstream_id, HashUnit::format_value(hashrate));
                } else {
                    info!("  ❌ {}: {} (INACTIVE)", upstream_id, HashUnit::format_value(hashrate));
                }
            }
            
            if active_count > 0 {
                let total_distributed = total_hashrate * active_count as f32;
                info!("🚀 Total hashrate being sent: {} across {} upstreams", 
                      HashUnit::format_value(total_distributed), active_count);
                info!("💡 Note: In parallel mode, each upstream receives the full hashrate simultaneously");
            } else {
                warn!("⚠️  WARNING: No active upstream connections!");
            }
            info!("==========================================");
        }
    }
}
async fn monitor(
    router: &mut Router,
    abort_handles: Vec<(AbortOnDrop, std::string::String)>,
    epsilon: Duration,
    server_handle: tokio::task::JoinHandle<()>,
) -> Reconnect {
    // Since we only support multi-upstream mode now, this monitor function
    // is simplified to just handle multi-upstream monitoring
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Generate periodic reports for multi-upstream
        let (total_connections, active_connections) = router.get_connection_stats().await;
        let total_hashrate = ProxyState::get_total_hashrate();
        
        info!("Multi-upstream mode: {} total, {} active connections", total_connections, active_connections);
        
        if active_connections > 0 {
            info!("Parallel mode: Using ALL upstreams simultaneously");
            info!(
                "Total hashrate distributed across {} upstreams: {}",
                active_connections,
                HashUnit::format_value(total_hashrate)
            );
            
            let active_upstreams = router.get_active_upstreams().await;
            for upstream_id in active_upstreams {
                info!(
                    "Upstream {}: receiving full hashrate {}",
                    upstream_id,
                    HashUnit::format_value(total_hashrate)
                );
            }
        } else {
            warn!("No active upstream connections!");
            // In a real implementation, you might want to try reconnecting here
        }
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


