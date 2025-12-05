#[cfg(not(target_os = "windows"))]
use jemallocator::Jemalloc;
use router::Router;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
#[cfg(not(target_os = "windows"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use crate::{
    monitor::logs::SendLogLayer, shared::utils::AbortOnDrop, shutdown::update_node_status,
};
use config::Configuration;
use key_utils::Secp256k1PublicKey;
use lazy_static::lazy_static;
use proxy_state::{PoolState, ProxyState, TpState, TranslatorState};
use self_update::{backends, cargo_crate_version, update::UpdateStatus, TempDir};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::{mpsc::channel, watch};
use tracing::{debug, error, info, warn};
mod api;
mod shutdown;

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
const DEFAULT_LISTEN_ADDRESS: &str = "0.0.0.0:32767";
const REPO_OWNER: &str = "demand-open-source";
const REPO_NAME: &str = "demand-cli";
const BIN_NAME: &str = "demand-cli";
const STAGING_URL: &str = "https://staging-user-dashboard-server.dmnd.work";
const LOCAL_URL: &str = "http://localhost:8787";
const TESTNET3_URL: &str = "https://testnet3-user-dashboard-server.dmnd.work";
const PRODUCTION_URL: &str = "https://production-user-dashboard-server.dmnd.work";

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
        .unwrap_or("6.0".to_string())
        .parse::<f32>()
        .expect("SHARE_PER_MIN is not a valid number");
}

#[tokio::main]
async fn main() {
    let log_level = Configuration::loglevel();
    let noise_connection_log_level = Configuration::nc_loglevel();

    let remote_layer = SendLogLayer::new();
    let console_layer =
        tracing_subscriber::fmt::layer().with_filter(tracing_subscriber::EnvFilter::new(format!(
            "{},demand_sv2_connection::noise_connection_tokio={}",
            log_level, noise_connection_log_level
        )));
    //Disable noise_connection error (for now) because:
    // 1. It produce logs that are not very user friendly and also bloat the logs
    // 2. The errors resulting from noise_connection are handled. E.g if unrecoverable error from noise connection occurs during Pool connection: We either retry connecting immediatley or we update Proxy state to Pool Down
    tracing_subscriber::registry()
        .with(console_layer)
        .with(remote_layer)
        .init();

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

    let shutdown_signal = shutdown::handle_shutdown();
    let mut router = router::Router::new(pool_addresses, auth_pub_k, None, None);
    let epsilon = Duration::from_millis(30_000);
    let best_upstream = router.select_pool_connect().await;
    // Register node on startup
    update_node_status("register").await;

    initialize_proxy(
        &mut router,
        best_upstream,
        epsilon,
        Configuration::signature(),
        shutdown_signal,
    )
    .await;
    info!("exiting");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
}

async fn initialize_proxy(
    router: &mut Router,
    mut pool_addr: Option<std::net::SocketAddr>,
    epsilon: Duration,
    signature: String,
    shutdown_signal: watch::Receiver<bool>,
) {
    loop {
        if *shutdown_signal.borrow() {
            break;
        }
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
        match monitor(router, abort_handles, epsilon, shutdown_signal.clone()).await {
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
    shutdown_signal: watch::Receiver<bool>,
) -> Reconnect {
    let mut should_check_upstreams_latency = 0;
    loop {
        if *shutdown_signal.borrow() {
            info!("Dropping all abort handles");
            drop(abort_handles);
            return Reconnect::NoUpstream;
        }
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

fn check_update_proxy() {
    info!("Checking for latest released version...");
    // Determine the OS and map to the asset name
    let os = std::env::consts::OS;
    let target_bin = match os {
        "linux" => "demand-cli-linux",
        "macos" => "demand-cli-macos",
        "windows" => "demand-cli-windows.exe",
        _ => {
            error!("Warning: Unsupported OS '{}', skipping update", os);
            unreachable!()
        }
    };

    debug!("OS: {}", target_bin);
    debug!("DMND-PROXY version: {}", cargo_crate_version!());
    let original_path = std::env::current_exe().expect("Failed to get current executable path");
    let tmp_dir = TempDir::new_in(::std::env::current_dir().expect("Failed to get current dir"))
        .expect("Failed to create tmp dir");

    let updater = match backends::github::Update::configure()
        .repo_owner(REPO_OWNER)
        .repo_name(REPO_NAME)
        .bin_name(BIN_NAME)
        .current_version(cargo_crate_version!())
        .target(target_bin)
        .show_output(false)
        .no_confirm(true)
        .bin_install_path(tmp_dir.path())
        .build()
    {
        Ok(updater) => updater,
        Err(e) => {
            error!("Failed to configure update: {}", e);
            return;
        }
    };

    match updater.update_extended() {
        Ok(status) => match status {
            UpdateStatus::UpToDate => {
                info!("Package is up to date");
            }
            UpdateStatus::Updated(release) => {
                info!(
                    "Proxy updated to version {}. Restarting Proxy",
                    release.version
                );
                for asset in release.assets {
                    if asset.name == target_bin {
                        let bin_name = std::path::PathBuf::from(target_bin);
                        let new_exe = tmp_dir.path().join(&bin_name);
                        let mut file =
                            std::fs::File::create(&new_exe).expect("Failed to create file");
                        let mut download = self_update::Download::from_url(&asset.download_url);
                        download.set_header(
                            reqwest::header::ACCEPT,
                            reqwest::header::HeaderValue::from_static("application/octet-stream"), // to triggers a redirect to the actual binary.
                        );
                        download
                            .download_to(&mut file)
                            .expect("Failed to download file");
                    }
                }
                let bin_name = std::path::PathBuf::from(target_bin);
                let new_exe = tmp_dir.path().join(&bin_name);
                if let Err(e) = std::fs::rename(&new_exe, &original_path) {
                    error!(
                        "Failed to move new binary to {}: {}",
                        original_path.display(),
                        e
                    );
                    return;
                }

                let _ = std::fs::remove_dir_all(tmp_dir); // clean up tmp dir
                                                          // Get original cli rgs
                let args = std::env::args().skip(1).collect::<Vec<_>>();

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    use std::os::unix::process::CommandExt;
                    // On Unix-like systems, replace the current process with the new binary
                    if let Err(e) = std::fs::set_permissions(
                        &original_path,
                        std::fs::Permissions::from_mode(0o755),
                    ) {
                        error!(
                            "Failed to set executable permissions on {}: {}",
                            original_path.display(),
                            e
                        );
                        return;
                    }

                    let err = std::process::Command::new(&original_path)
                        .args(&args)
                        .exec();
                    // If exec fails, log the error and exit
                    error!("Failed to exec new binary: {:?}", err);
                    std::process::exit(1);
                }
                #[cfg(not(unix))]
                {
                    // On Windows, spawn the new process and exit the current one
                    std::process::Command::new(&original_path)
                        .args(&args)
                        .spawn()
                        .expect("Failed to start proxy");
                    std::process::exit(0);
                }
            }
        },
        Err(e) => {
            error!("Failed to update proxy: {}", e);
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
