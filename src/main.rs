use clap::Parser;
#[cfg(not(target_os = "windows"))]
use jemallocator::Jemalloc;
use router::Router;
use shared::utils::HashUnit;
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
    static ref SV1_DOWN_LISTEN_ADDR: String =
        std::env::var("SV1_DOWN_LISTEN_ADDR").unwrap_or(DEFAULT_LISTEN_ADDRESS.to_string());
    static ref TP_ADDRESS: roles_logic_sv2::utils::Mutex<Option<String>> =
        roles_logic_sv2::utils::Mutex::new(std::env::var("TP_ADDRESS").ok());
    static ref EXPECTED_SV1_HASHPOWER: f32 =
        ARGS.downstream_hashrate.unwrap_or(DEFAULT_SV1_HASHPOWER);
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
#[derive(Parser)]
struct Args {
    // Use test enpoint if test flag is provided
    #[clap(long)]
    test: bool,
    #[clap(long ="d", short ='d', value_parser = shared::utils::parse_hashrate)]
    downstream_hashrate: Option<f32>,
    #[clap(long = "loglevel", short = 'l', default_value = "info")]
    loglevel: String,
    #[clap(long = "nc", short = 'n', default_value = "off")]
    noise_connection_log: String,
    #[clap(long = "delay", default_value = "0")]
    delay: u64,
    #[clap(long = "interval", short = 'i', default_value = "120000")]
    adjustment_interval: u64,
    #[clap(long = "solo", short = 's')]
    solo: bool,
    #[arg(short = 'c', long, default_value = "config.toml")]
    config: std::path::PathBuf,
    #[clap(long)]
    payout_address: Option<String>,
    #[clap(long)]
    withhold: Option<bool>,
    #[clap(long, help = "Bitcoin network (bitcoin, testnet, regtest, signet)")]
    network: Option<String>,
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

    if args.solo {
        initialize_proxy_solo(args.config).await;
    } else {
        std::env::var("TOKEN").expect("Missing TOKEN environment variable");

        let hashpower = *EXPECTED_SV1_HASHPOWER;

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
        if args.test {
            info!("Connecting to test endpoint...");
        }

        let auth_pub_k: Secp256k1PublicKey = AUTH_PUB_KEY.parse().expect("Invalid public key");
        let address = POOL_ADDRESS
            .to_socket_addrs()
            .expect("Invalid pool address")
            .next()
            .expect("Invalid pool address");

        // We will add upstream addresses here
        let pool_addresses = vec![address];

        let mut router = router::Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(10);
        let best_upstream = router.select_pool_connect().await;
        initialize_proxy(&mut router, best_upstream, epsilon).await;
        info!("exiting");
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
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
        let server_handle = tokio::spawn(api::start(Some(router.clone()), stats_sender));
        match monitor(Some(router), abort_handles, Some(epsilon), server_handle).await {
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

async fn initialize_proxy_solo(config: std::path::PathBuf) {
    loop {
        let (downs_sv1_tx, downs_sv1_rx) = channel(10);
        let sv1_ingress_abortable = ingress::sv1_ingress::start_listen_for_downstream(downs_sv1_tx);
        let stats_sender = api::stats::StatsSender::new();
        let (translator_up_tx, mut translator_up_rx) = channel(10);
        let translator_abortable =
            match translator::start(downs_sv1_rx, translator_up_tx, stats_sender.clone()).await {
                Ok(abortable) => abortable,
                Err(e) => {
                    error!("Impossible to initialize translator: {e}");
                    ProxyState::update_translator_state(TranslatorState::Down);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            };

        let (jdc_to_translator_sender, jdc_from_translator_receiver, _) = translator_up_rx
            .recv()
            .await
            .expect("Translator failed before initialization");

        let jdc_abortable = match jd_client::initialize_jd_as_solo_miner(
            jdc_from_translator_receiver,
            jdc_to_translator_sender,
            config.clone(),
        )
        .await
        {
            Some(abortable) => abortable,
            None => {
                error!("Impossible to initialize JDC");
                ProxyState::update_tp_state(TpState::Down);
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        let abort_handles = vec![
            (sv1_ingress_abortable, "sv1_ingress".to_string()),
            (translator_abortable, "translator".to_string()),
            (jdc_abortable, "jdc".to_string()),
        ];

        let server_handle = tokio::spawn(api::start(None, stats_sender));
        if monitor(None, abort_handles, None, server_handle).await == Reconnect::NoUpstream {
            ProxyState::update_proxy_state_up();
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }
    }
}

async fn monitor(
    mut router: Option<&mut Router>,
    abort_handles: Vec<(AbortOnDrop, std::string::String)>,
    epsilon: Option<Duration>,
    server_handle: tokio::task::JoinHandle<()>,
) -> Reconnect {
    let mut should_check_upstreams_latency = 0;
    loop {
        // Check if a better upstream exist every 100 seconds
        if should_check_upstreams_latency == 10 * 100 {
            should_check_upstreams_latency = 0;
            if let (Some(router), Some(epsilon)) = (router.as_mut(), epsilon) {
                if let Some(new_upstream) = router.monitor_upstream(epsilon).await {
                    info!("Faster upstream detected. Reinitializing proxy...");
                    drop(abort_handles);
                    server_handle.abort(); // abort server

                    // Needs a little to time to drop
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    return Reconnect::NewUpstream(new_upstream);
                }
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

#[derive(PartialEq)]
pub enum Reconnect {
    NewUpstream(std::net::SocketAddr), // Reconnecting with a new upstream
    NoUpstream,                        // Reconnecting without upstream
}
