#[cfg(not(target_os = "windows"))]
use jemallocator::Jemalloc;
use router::Router;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use clap::{ArgAction, Parser};
use serde::{Deserialize, Serialize};
use std::path::Path;
use crate::shared::utils::AbortOnDrop; 
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;


use key_utils::Secp256k1PublicKey;
use lazy_static::lazy_static;
use proxy_state::{PoolState, ProxyState, TpState, TranslatorState};
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
const MAIN_POOL_ADDRESS: &str = "mining.dmnd.work:2000";
//const TEST_POOL_ADDRESS: &str = "127.0.0.1:20000";
const TEST_POOL_ADDRESS: &str = "18.193.252.132:2000";
const MAIN_AUTH_PUB_KEY: &str = "9bQHWsQ2J9TRFTaxRh3KjoxdyLRfWVEy25YHtKF8y8gotLoCZZ";
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
    // Use test
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
    // Add the missing upstream field
    #[clap(long = "upstream", short = 'u', action = ArgAction::Append)]
    upstream: Option<Vec<String>>,
    #[clap(long = "config", short = 'c')]
    pub config_file: Option<String>,
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
    pub hashrate_distribution: Option<Vec<f32>>, // New field for custom distribution
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

    // Load configuration and get pool addresses
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
    println!("Config: {:?}", config);
    
    // Set the total hashrate from config BEFORE creating router
    if let Some(ref config) = config {
        if let Some(ref hashrate_str) = config.downstream_hashrate {
            match parse_hashrate(hashrate_str) {
                Ok(hashrate) => {
                    ProxyState::set_total_hashrate(hashrate);
                    info!("Set total hashrate to: {}", HashUnit::format_value(hashrate));
                }
                Err(e) => {
                    error!("Failed to parse downstream_hashrate: {}", e);
                }
            }
        }
    } else if let Some(hashrate) = ARGS.downstream_hashrate {
        ProxyState::set_total_hashrate(hashrate);
        info!("Set total hashrate from args to: {}", HashUnit::format_value(hashrate));
    }

    // Get pool addresses with auth keys
    let pool_address_keys = if let Some(ref config) = config {
        config.pool_addresses.iter().map(|addr_str| {
            let addr = addr_str.parse::<SocketAddr>()
                .unwrap_or_else(|_| {
                    error!("Invalid pool address: {}", addr_str);
                    std::process::exit(1);
                });
            
            let auth_key: Secp256k1PublicKey = if config.test.unwrap_or(false) {
                TEST_AUTH_PUB_KEY.parse().expect("Invalid test public key")
            } else {
                MAIN_AUTH_PUB_KEY.parse().expect("Invalid main public key")
            };
            
            (addr, auth_key)
        }).collect()
    } else {
        // Fallback to single upstream
        let addr = if ARGS.test {
            TEST_POOL_ADDRESS.parse::<SocketAddr>().expect("Invalid test address")
        } else {
            MAIN_POOL_ADDRESS.parse::<SocketAddr>().expect("Invalid main address")
        };
        
        let auth_key = if ARGS.test {
            TEST_AUTH_PUB_KEY.parse().expect("Invalid test public key")
        } else {
            MAIN_AUTH_PUB_KEY.parse().expect("Invalid main public key")
        };
        
        vec![(addr, auth_key)]
    };

    // Determine if we want hashrate distribution
    let wants_distribution = config.as_ref()
        .and_then(|c| c.hashrate_distribution.as_ref())
        .is_some() || ARGS.parallel;

    // Create router based on configuration
    let mut router = if wants_distribution {
        // User wants hashrate distribution - use multi-upstream mode
        let pool_address_keys_clone = pool_address_keys.clone();
        match Router::new_multi(
            pool_address_keys_clone,
            None, // setup_connection_msg
            None, // timer
            true, // use_distribution = true
        ).await {
            Ok(router) => {
                info!("Created multi-upstream router with {} upstreams for hashrate distribution", 
                      pool_address_keys.len());
                router
            }
            Err(e) => {
                error!("Failed to create multi-upstream router: {}", e);
                std::process::exit(1);
            }
        }
    } else if pool_address_keys.len() > 1 {
        // Multiple pools but no distribution specified - use latency-based selection (single upstream mode)
        info!("Created latency-based router with {} pools (will select best latency)", pool_address_keys.len());
        Router::new_with_keys(
            pool_address_keys.clone(),
            None, // setup_connection_msg
            None, // timer
        )
    } else {
        // Single pool - use single upstream mode
        let (addr, auth_key) = pool_address_keys[0];
        info!("Created single upstream router");
        Router::new(
            vec![addr],
            auth_key,
            None, // setup_connection_msg
            None, // timer
        )
    };

    // Add this right after router creation, before the if statements:
    let epsilon = Duration::from_millis(10);

    // Handle the three different scenarios
    if wants_distribution {
        info!("=== HASHRATE DISTRIBUTION MODE ===");
        
        // Get the desired distribution
        let distribution = config.as_ref()
            .and_then(|c| c.hashrate_distribution.clone())
            .unwrap_or_else(|| {
                // Default equal distribution if parallel flag is set but no custom distribution
                let count = pool_address_keys.len();
                let equal_percentage = 100.0 / count as f32;
                vec![equal_percentage; count]
            });
            println!("Using hashrate distribution: {:?}", distribution);

        let desired_pool_count = distribution.len();
        info!("Desired pool count for distribution: {}", desired_pool_count);

        // Select best pools based on latency (implements condition 2 and 3)
        let selected_pools = if pool_address_keys.len() <= desired_pool_count {
            // Condition 3: Use all available pools (skip latency testing)
            info!("Using all {} available pools (condition 3)", pool_address_keys.len());
            pool_address_keys.clone()
        } else {
            // Condition 2: Select best pools based on latency
            info!("Selecting {} best pools from {} available (condition 2)", desired_pool_count, pool_address_keys.len());
            
            // Create temporary router for latency testing
            let mut temp_router = Router::new_with_keys(
                pool_address_keys.clone(),
                None,
                None,
            );
            
            temp_router.select_best_pools_for_distribution(desired_pool_count).await
        };

        // Now create the final multi-upstream router with selected pools
        router = match Router::new_multi(
            selected_pools,
            None,
            None,
            true,
        ).await {
            Ok(router) => {
                info!("Created distribution router with {} selected pools", desired_pool_count);
                router
            }
            Err(e) => {
                error!("Failed to create distribution router: {}", e);
                std::process::exit(1);
            }
        };

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
        info!("Set hashrate distribution: {:?}", distribution);

        
        info!("🔧 About to call initialize_proxy");
        initialize_proxy(&mut router, None, epsilon).await;
    } else if pool_address_keys.len() > 1 {
        info!("=== LATENCY-BASED SELECTION MODE (Condition 1) ===");
        info!("Multiple pools available, will select best latency");
        
        // Actually select the best latency pool
        let best_upstream = router.select_pool_connect().await;
        if best_upstream.is_some() {
            info!("Selected best upstream: {:?}", best_upstream);
        }
        initialize_proxy(&mut router, best_upstream, epsilon).await;
    } else {
        info!("=== SINGLE UPSTREAM MODE ===");
        
        // Single pool mode
        let best_upstream = router.select_pool_connect().await;
        initialize_proxy(&mut router, best_upstream, epsilon).await;
    }

    info!("Proxy initialization complete");

    // Wait a moment for connections to establish
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Show immediate hashrate distribution with network verification
    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("🔍 VERIFYING NETWORK CONNECTIONS:");

    // Check network connections
    let output = std::process::Command::new("ss").args(&["-tn"]).output();

    if let Ok(output) = output {
        let connections = String::from_utf8_lossy(&output.stdout);
        let pool_connections: Vec<&str> = connections
            .lines()
            .filter(|line| {
                line.contains("18.193.252.132:2000") || line.contains("3.74.36.119:2000")
            })
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
    info!(
        "🔋 Total configured hashrate: {}",
        HashUnit::format_value(total_hashrate)
    );
    
    let active_count = detailed_stats.iter().filter(|(_, is_active, _)| *is_active).count();
    
    if active_count > 0 {
        // Check if using custom distribution
        let mut is_custom_distribution = false;
        for (_, is_active, hashrate) in &detailed_stats {
            if *is_active {
                let percentage = (*hashrate / total_hashrate) * 100.0;
                if (percentage - (100.0 / active_count as f32)).abs() > 1.0 {
                    is_custom_distribution = true;
                    break;
                }
            }
        }
        
        if is_custom_distribution {
            info!("🎯 Mode: Custom Distribution (from config)");
        } else {
            let percentage_per_upstream = 100.0 / active_count as f32;
            info!("🌐 Mode: Equal Distribution ({:.1}% per upstream)", percentage_per_upstream);
        }

        for (upstream_id, is_active, hashrate) in detailed_stats {
            if is_active {
                info!("{} = hashrate", hashrate);
                let percentage = (hashrate / total_hashrate) * 100.0;
                info!(
                    "  ✅ {}: receiving {} ({:.1}% of total)",
                    upstream_id,
                    HashUnit::format_value(hashrate),
                    percentage
                );
            } else {
                info!(
                    "  ❌ {}: {} (INACTIVE)",
                    upstream_id,
                    HashUnit::format_value(hashrate)
                );
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
}

async fn initialize_proxy(
    router: &mut Router,
    mut pool_addr: Option<std::net::SocketAddr>,
    epsilon: Duration,
) {
    // Check if we're in multi-upstream mode
    if router.is_multi_upstream_enabled() {
        info!("Initializing proxy in HASHRATE DISTRIBUTION mode");
        info!("🎯 Focus: Distribute hashrate allocation across upstreams");
        
        // Start API server for monitoring
        let stats_sender = api::stats::StatsSender::new();
        let _server_handle = tokio::spawn(api::start(router.clone(), stats_sender));

        // Start the monitor task in the background
        let router_clone = router.clone();
        tokio::spawn(async move {
            monitor_multi_upstream(router_clone, epsilon).await;
        });

        info!("✅ Hashrate distribution monitor started");
        return;
    }
    
    // Single upstream mode only
    loop {
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

// Consolidated monitoring function for multi-upstream mode
async fn monitor_multi_upstream(router: Router, epsilon: Duration) {
    let mut stats_report_counter = 0;
    
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        stats_report_counter += 1;

        // Generate periodic reports every 30 seconds (3 * 10 seconds)
        if stats_report_counter >= 3 {
            stats_report_counter = 0;
            
            let (total_count, active_count) = router.get_upstream_counts().await;
            let total_hashrate = ProxyState::get_total_hashrate();
            
            info!("🚀 === MULTI-UPSTREAM HASHRATE REPORT ===");
            info!("📊 Total upstreams: {}, Active: {}", total_count, active_count);
            info!("🔋 Total hashrate: {}", HashUnit::format_value(total_hashrate));
            
            if active_count > 0 {
                let detailed_stats = router.get_detailed_connection_stats().await;
                let distribution = router.get_hashrate_distribution().await;
                
                // Check if using custom distribution
                let is_custom = !distribution.is_empty() && distribution.len() == detailed_stats.len();
                
                if is_custom {
                    info!("🎯 Distribution mode: Custom");
                    info!("📈 Distribution percentages: {:?}", distribution);
                } else {
                    info!("⚖️  Distribution mode: Equal (automatic)");
                }

                for (upstream_id, is_active, hashrate) in detailed_stats {
                    if is_active {
                        let percentage = (hashrate / total_hashrate) * 100.0;
                        info!(
                            "  ✅ {}: receiving {} ({:.1}% of total)",
                            upstream_id,
                            HashUnit::format_value(hashrate),
                            percentage
                        );
                    } else {
                        info!(
                            "  ❌ {}: {} (INACTIVE)",
                            upstream_id,
                            HashUnit::format_value(hashrate)
                        );
                    }
                }
            } else {
                warn!("❌ No active upstream connections!");
            }
            info!("=========================================");
        }

        // Check proxy state
        let (is_down, error_msg) = ProxyState::is_proxy_down();
        if is_down {
            error!("⚠️  Proxy state is DOWN: {:?}", error_msg);
            break;
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
