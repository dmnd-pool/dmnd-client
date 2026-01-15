#[cfg(not(target_os = "windows"))]
use jemallocator::Jemalloc;
use router::Router;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
#[cfg(not(target_os = "windows"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use crate::auto_update::check_update_proxy;
use crate::shared::utils::AbortOnDrop;
use config::Configuration;
use key_utils::Secp256k1PublicKey;
use lazy_static::lazy_static;
use proxy_state::{PoolState, ProxyState, TpState, TranslatorState};
use std::sync::OnceLock;
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc::channel;
use tracing::{error, info, warn};

mod api;
mod auto_update;
mod config;
mod ingress;
pub mod jd_client;
mod minin_pool_connection;
mod monitor;
mod proxy_state;
mod router;
mod share_accounter;
mod shared;
mod translator;

const TRANSLATOR_BUFFER_SIZE: usize = 32;
const MIN_EXTRANONCE_SIZE: u16 = 6;
const MIN_EXTRANONCE2_SIZE: u16 = 5;
const UPSTREAM_EXTRANONCE1_SIZE: usize = 20;
const DEFAULT_SV1_HASHPOWER: f32 = 100_000_000_000_000.0;
const CHANNEL_DIFF_UPDTATE_INTERVAL: u32 = 10;
const MAX_LEN_DOWN_MSG: u32 = 10000;
const MAIN_AUTH_PUB_KEY: &str = "9c44K6QVizyPWb9xfeqhckFRosxWwB3EfytGa4CfTdD526qb2QV";
const TEST_AUTH_PUB_KEY: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
pub const DEFAULT_LISTEN_ADDRESS: &str = "0.0.0.0:32767";
const STAGING_URL: &str = "https://staging-user-dashboard-server.dmnd.work";
const LOCAL_URL: &str = "http://localhost:8787";
const TESTNET3_URL: &str = "https://testnet3-user-dashboard-server.dmnd.work";
const PRODUCTION_URL: &str = "https://production-user-dashboard-server.dmnd.work";

lazy_static! {
    static ref TP_ADDRESS: roles_logic_sv2::utils::Mutex<Option<String>> =
        roles_logic_sv2::utils::Mutex::new(Configuration::tp_address());
    static ref ACTIVE_POOL_ADDRESS: roles_logic_sv2::utils::Mutex<Option<SocketAddr>> =
        roles_logic_sv2::utils::Mutex::new(None); // Connected pool address
}

lazy_static! {

    // for staging and local environments, use the test auth public key
    // for production, use the main auth public key
    pub static ref AUTH_PUB_KEY: &'static str = if Configuration::staging() || Configuration::local() || Configuration::testnet3() {
        TEST_AUTH_PUB_KEY
    } else {
        MAIN_AUTH_PUB_KEY
    };
}
lazy_static! {
    static ref SHARE_PER_MIN: f32 = std::env::var("SHARE_PER_MIN")
        .unwrap_or("10.0".to_string())
        .parse::<f32>()
        .expect("SHARE_PER_MIN is not a valid number");
}

static LOG_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

pub async fn start() {
    let log_level = Configuration::loglevel();
    let noise_connection_log_level = Configuration::nc_loglevel();

    let enable_file_logging = Configuration::enable_file_logging();

    // let remote_layer = SendLogLayer::new();

    //Disable noise_connection error (for now) because:
    // 1. It produce logs that are not very user friendly and also bloat the logs
    // 2. The errors resulting from noise_connection are handled. E.g if unrecoverable error from
    //    noise connection occurs during Pool connection: We either retry connecting immediatley or
    //    we update Proxy state to Pool Down

    let console_layer =
        tracing_subscriber::fmt::layer().with_filter(tracing_subscriber::EnvFilter::new(format!(
            "{},demand_sv2_connection::noise_connection_tokio={}",
            log_level, noise_connection_log_level
        )));

    if enable_file_logging {
        let file_appender = tracing_appender::rolling::daily("logs", "dmnd-client.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        LOG_GUARD.set(guard).unwrap_or(());
        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(non_blocking)
            .with_ansi(false) // file logs should not contain color codes
            .with_filter(tracing_subscriber::EnvFilter::new(format!(
                "{},demand_sv2_connection::noise_connection_tokio={}",
                log_level, noise_connection_log_level
            )));
        tracing_subscriber::registry()
            .with(console_layer)
            .with(file_layer)
            // .with(remote_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(console_layer)
            // .with(remote_layer)
            .init();
    }

    Configuration::token().expect("TOKEN is not set");

    //`self_update` performs synchronous I/O so spawn_blocking is needed
    if Configuration::auto_update() {
        if let Err(e) = tokio::task::spawn_blocking(check_update_proxy).await {
            error!("An error occured while trying to update Proxy; {:?}", e);
            ProxyState::update_inconsistency(Some(1));
        };
    }

    if Configuration::staging() {
        info!("Package is running in staging mode");
    }
    if Configuration::local() {
        info!("Package is running in local mode");
    }
    if Configuration::testnet3() {
        info!("Package is running in testnet3 mode");
    }

    let auth_pub_k: Secp256k1PublicKey = AUTH_PUB_KEY.parse().expect("Invalid public key");

    let pool_addresses = Configuration::pool_address()
        .await
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| match Configuration::environment().as_str() {
            "staging" => panic!("Staging pool address is missing"),
            "testnet3" => panic!("Testnet3 pool address is missing"),
            "local" => panic!("Local pool address is missing"),
            "production" => panic!("Pool address is missing"),
            _ => unreachable!(),
        });

    let mut router = router::Router::new(pool_addresses, auth_pub_k, None, None);
    let epsilon = Duration::from_millis(30_000);
    let best_upstream = router.select_pool_connect().await;
    initialize_proxy(
        &mut router,
        best_upstream,
        epsilon,
        Configuration::signature(),
    )
    .await;
    info!("exiting");
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
}

async fn initialize_proxy(
    router: &mut Router,
    mut pool_addr: Option<std::net::SocketAddr>,
    epsilon: Duration,
    signature: String,
) {
    loop {
        let stats_sender = api::stats::StatsSender::new();
        let (send_to_pool, recv_from_pool, pool_connection_abortable) =
            match router.connect_pool(pool_addr).await {
                Ok(connection) => connection,
                Err(_) => {
                    error!("No upstream available. Retrying in 5 seconds...");
                    warn!(
                        "Please make sure the your token {} is correct",
                        Configuration::token().expect("Token is not set")
                    );
                    let secs = 5;
                    tokio::time::sleep(Duration::from_secs(secs)).await;
                    continue;
                }
            };

        let (downs_sv1_tx, downs_sv1_rx) = channel(10);
        let sv1_ingress_abortable = ingress::sv1_ingress::start_listen_for_downstream(downs_sv1_tx);

        let (translator_up_tx, mut translator_up_rx) = channel(10);
        let translator_abortable = match translator::start(
            downs_sv1_rx,
            translator_up_tx,
            stats_sender.clone(),
            signature.clone(),
        )
        .await
        {
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
        abort_handles.push((server_handle.into(), "api_server".to_string()));
        match monitor(router, abort_handles, epsilon).await {
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
) -> Reconnect {
    let mut should_check_upstreams_latency = 0;
    loop {
        if Configuration::monitor() {
            // Check if a better upstream exist every 100 seconds
            if should_check_upstreams_latency == 10 * 100 {
                should_check_upstreams_latency = 0;
                if let Some(new_upstream) = router.monitor_upstream(epsilon).await {
                    info!("Faster upstream detected. Reinitializing proxy...");
                    drop(abort_handles);

                    // Needs a little to time to drop
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    return Reconnect::NewUpstream(new_upstream);
                }
            }
            should_check_upstreams_latency += 1;
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
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // Needs a little to time to drop
            return Reconnect::NoUpstream;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

pub enum Reconnect {
    NewUpstream(std::net::SocketAddr), // Reconnecting with a new upstream
    NoUpstream,                        // Reconnecting without upstream
}
