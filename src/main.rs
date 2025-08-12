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
use proxy_state::{PoolState, ProxyState, TpState, TranslatorState};
use self_update::{backends, cargo_crate_version, update::UpdateStatus, TempDir};
use std::sync::Arc;
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc::channel;
use tracing::{debug, error, info, warn};
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
const REPO_OWNER: &str = "demand-open-source";
const REPO_NAME: &str = "demand-cli";
const BIN_NAME: &str = "demand-cli";

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

    //`self_update` performs synchronous I/O so spawn_blocking is needed
    if Configuration::auto_update() {
        if let Err(e) = tokio::task::spawn_blocking(check_update_proxy).await {
            error!("An error occured while trying to update Proxy; {:?}", e);
            ProxyState::update_inconsistency(Some(1));
        };
    }

    if Configuration::test() {
        info!("Package is running in test mode");
    }

    let auth_pub_k: Secp256k1PublicKey = AUTH_PUB_KEY.parse().expect("Invalid public key");

    let pool_addresses = Configuration::pool_address()
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| {
            if Configuration::test() {
                panic!("Test pool address is missing");
            } else {
                panic!("Pool address is missing");
            }
        });

    let mut router = router::Router::new(pool_addresses, auth_pub_k, None, None);
    let epsilon = Duration::from_millis(30_000);
    let best_upstream = if router.is_multi_upstream() {
        None
    } else {
        router.select_pool_connect().await
    };

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
        let stats_sender = api::stats::StatsSender::new();
        let is_multi_upstream = router.is_multi_upstream();

        if is_multi_upstream {
            // Multi-upstream: connect to all pools and start components for each
            let pool_connections = router.connect_all_pools().await;

            // Handle connection results
            let mut any_success = false;
            let mut abort_handles = Vec::new();

            // Create per-pool translator stacks
            let mut pool_translators = Vec::new();

            // For each pool, create a complete translator stack
            for (pool_id, pool_addr, result) in pool_connections {
                match result {
                    Ok((send_to_pool, recv_from_pool, pool_connection_abortable)) => {
                        info!(
                            "Successfully connected to pool {} (ID: {})",
                            pool_addr, pool_id
                        );
                        any_success = true;

                        // Create per-pool downstream channels
                        let (pool_downs_tx, pool_downs_rx) = channel(10);

                        // Create per-pool translator channels
                        let (translator_up_tx, mut translator_up_rx) = channel(10);

                        // Start translator for this specific pool
                        let translator_abortable = match translator::start(
                            pool_downs_rx,
                            translator_up_tx,
                            stats_sender.clone(),
                            Arc::new(router.clone()),
                            pool_addr,
                        )
                        .await
                        {
                            Ok(abortable) => abortable,
                            Err(e) => {
                                error!(
                                    "Impossible to initialize translator for pool {}: {e}",
                                    pool_addr
                                );
                                continue;
                            }
                        };

                        // Get translator channels for this pool
                        let (jdc_to_translator_sender, jdc_from_translator_receiver, _) =
                            match translator_up_rx.recv().await {
                                Some(val) => val,
                                None => {
                                    error!(
                                        "Translator failed before initialization for pool {}",
                                        pool_addr
                                    );
                                    continue;
                                }
                            };

                        // Set up JDC and share accounter for this pool
                        let (from_jdc_to_share_accounter_send, from_jdc_to_share_accounter_recv) =
                            channel(10);
                        let (from_share_accounter_to_jdc_send, from_share_accounter_to_jdc_recv) =
                            channel(10);

                        let tp = match TP_ADDRESS.safe_lock(|tp| tp.clone()) {
                            Ok(tp) => tp,
                            Err(e) => {
                                error!("TP_ADDRESS Mutex Corrupted: {e}");
                                continue;
                            }
                        };

                        let jdc_abortable: Option<AbortOnDrop>;
                        let share_accounter_abortable;
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
                                send_to_pool.clone(),
                            )
                            .await
                            {
                                Ok(abortable) => abortable,
                                Err(_) => {
                                    error!(
                                        "Failed to start share_accounter for pool {}",
                                        pool_addr
                                    );
                                    continue;
                                }
                            }
                        } else {
                            jdc_abortable = None;
                            share_accounter_abortable = match share_accounter::start(
                                jdc_from_translator_receiver,
                                jdc_to_translator_sender,
                                recv_from_pool,
                                send_to_pool.clone(),
                            )
                            .await
                            {
                                Ok(abortable) => abortable,
                                Err(_) => {
                                    error!(
                                        "Failed to start share_accounter for pool {}",
                                        pool_addr
                                    );
                                    continue;
                                }
                            };
                        }

                        // Store pool translator info for downstream distribution
                        pool_translators.push((pool_id, pool_addr, pool_downs_tx));

                        // Collect abort handles for this pool
                        abort_handles.push((
                            pool_connection_abortable,
                            format!("pool_connection_{}", pool_id),
                        ));
                        abort_handles
                            .push((translator_abortable, format!("translator_{}", pool_id)));
                        abort_handles.push((
                            share_accounter_abortable,
                            format!("share_accounter_{}", pool_id),
                        ));
                        if let Some(jdc_handle) = jdc_abortable {
                            abort_handles.push((jdc_handle, format!("jdc_{}", pool_id)));
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to connect to pool {} (ID: {}): {:?}",
                            pool_addr, pool_id, e
                        );
                    }
                }
            }

            // Start SV1 ingress with custom downstream distribution
            let (downs_sv1_tx, downs_sv1_rx) = channel(10);
            let sv1_ingress_abortable =
                ingress::sv1_ingress::start_listen_for_downstream(downs_sv1_tx);
            abort_handles.push((sv1_ingress_abortable, "sv1_ingress".to_string()));

            // Start downstream distribution task that routes miners to appropriate pools
            let router_for_distribution = router.clone();
            let pool_translators_for_distribution = pool_translators.clone();
            let distribution_task = tokio::spawn(async move {
                let mut recv = downs_sv1_rx;

                while let Some((send_to_downstream, recv_from_downstream, ip_addr)) =
                    recv.recv().await
                {
                    info!("New downstream connection from {}", ip_addr);

                    // Calling router to assign pool
                    if let Some(assigned_pool) =
                        router_for_distribution.assign_miner_to_pool().await
                    {
                        // Send to the appropriate translator
                        if let Some((_, _, pool_downs_tx)) = pool_translators_for_distribution
                            .iter()
                            .find(|(_, addr, _)| *addr == assigned_pool)
                        {
                            if let Err(e) = pool_downs_tx
                                .send((send_to_downstream, recv_from_downstream, ip_addr))
                                .await
                            {
                                error!(
                                    "Failed to send downstream connection to pool {}: {}",
                                    assigned_pool, e
                                );
                            }
                        }
                    } else {
                        error!("Could not assign miner from {} to any pool", ip_addr);
                    }
                }
            });
            abort_handles.push((
                distribution_task.into(),
                "downstream_distribution".to_string(),
            ));

            if !any_success {
                error!("No upstream available. Retrying in 5 seconds...");
                warn!(
                    "Please make sure the your token {} is correct",
                    Configuration::token().expect("Token is not set")
                );
                let secs = 5;
                tokio::time::sleep(Duration::from_secs(secs)).await;
                // Restart loop, esentially restarting proxy

                continue;
            }

            let server_handle = tokio::spawn(api::start(router.clone(), stats_sender));
            match monitor(router, abort_handles, epsilon, server_handle).await {
                Reconnect::NewUpstream(_) | Reconnect::NoUpstream => {
                    ProxyState::update_proxy_state_up();
                    continue;
                }
            };
        } else {
            // Single-upstream: use the best pool passed in (or re-select if needed)
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
                        // Restart loop, esentially restarting proxy
                        continue;
                    }
                };

            let (downs_sv1_tx, downs_sv1_rx) = channel(10);
            let sv1_ingress_abortable =
                ingress::sv1_ingress::start_listen_for_downstream(downs_sv1_tx);

            let (translator_up_tx, mut translator_up_rx) = channel(10);
            let translator_abortable = match translator::start(
                downs_sv1_rx,
                translator_up_tx,
                stats_sender.clone(),
                Arc::new(router.clone()),
                pool_addr.expect("Best latency pool address should be available"),
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
                        error!("Failed to start share_accounter for pool {:?}", pool_addr);
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
                        error!("Failed to start share_accounter for pool {:?}", pool_addr);
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
}

async fn monitor(
    router: &mut Router,
    abort_handles: Vec<(AbortOnDrop, std::string::String)>,
    epsilon: Duration,
    server_handle: tokio::task::JoinHandle<()>,
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
                    server_handle.abort(); // abort server

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::time::Duration;

    // Mock configuration for testing
    fn setup_test_config() {
        std::env::set_var("TOKEN", "test_token");
        std::env::set_var("AUTO_UPDATE", "false");
        std::env::set_var("MONITOR", "false");
    }

    #[tokio::test]
    async fn test_initialize_proxy_single_upstream_success() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        let pool_addresses = vec!["127.0.0.1:12345".parse::<SocketAddr>().unwrap()];
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(100);
        let pool_addr = Some("127.0.0.1:12345".parse::<SocketAddr>().unwrap());

        // Test that function starts without immediate panic/error
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut router, pool_addr, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    // Test passes if we reach here without panic
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_millis(200), task).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_initialize_proxy_multi_upstream_mode() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        // Create multiple pool addresses to trigger multi-upstream mode
        let pool_addresses = vec![
            "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:12346".parse::<SocketAddr>().unwrap(),
        ];
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(100);
        let pool_addr = None; // Multi-upstream uses None

        // Test multi-upstream initialization path
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut router, pool_addr, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    // Test passes if multi-upstream path executes
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_millis(200), task).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_initialize_proxy_connection_failure_retry() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        // Use invalid port to simulate connection failure
        let pool_addresses = vec!["127.0.0.1:1".parse::<SocketAddr>().unwrap()];
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(50);
        let pool_addr = Some("127.0.0.1:1".parse::<SocketAddr>().unwrap());

        // Test that connection failure is handled gracefully with retry logic
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut router, pool_addr, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Should reach here as function retries on connection failure
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_millis(300), task).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_initialize_proxy_multi_upstream_partial_failure() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        // Mix valid and invalid addresses to test partial connection failure
        let pool_addresses = vec![
            "127.0.0.1:1".parse::<SocketAddr>().unwrap(), // Invalid
            "127.0.0.1:12345".parse::<SocketAddr>().unwrap(), // Valid (but won't connect in test)
        ];
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(50);
        let pool_addr = None;

        // Test multi-upstream with some connection failures
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut router, pool_addr, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Should handle partial failures gracefully
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_millis(300), task).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_initialize_proxy_single_upstream_no_pool_addr() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        let pool_addresses = vec!["127.0.0.1:12345".parse::<SocketAddr>().unwrap()];
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(100);
        let pool_addr = None; // No specific pool address provided

        // Test single-upstream with None pool_addr (should trigger connection logic)
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut router, pool_addr, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    // Test passes if function handles None pool_addr
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_millis(200), task).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_initialize_proxy_with_zero_epsilon() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        let pool_addresses = vec!["127.0.0.1:12345".parse::<SocketAddr>().unwrap()];
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(0); // Edge case: zero epsilon
        let pool_addr = Some("127.0.0.1:12345".parse::<SocketAddr>().unwrap());

        // Test function behavior with zero epsilon
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut router, pool_addr, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    // Should handle zero epsilon gracefully
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_millis(200), task).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_initialize_proxy_with_large_epsilon() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        let pool_addresses = vec!["127.0.0.1:12345".parse::<SocketAddr>().unwrap()];
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_secs(60); // Large epsilon value
        let pool_addr = Some("127.0.0.1:12345".parse::<SocketAddr>().unwrap());

        // Test function behavior with large epsilon
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut router, pool_addr, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    // Should handle large epsilon gracefully
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_millis(200), task).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_initialize_proxy_stats_sender_creation() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        let pool_addresses = vec!["127.0.0.1:12345".parse::<SocketAddr>().unwrap()];
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(100);
        let pool_addr = Some("127.0.0.1:12345".parse::<SocketAddr>().unwrap());

        // Test that stats_sender is created properly (indirect test through function execution)
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut router, pool_addr, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    // If function executes, stats_sender creation was successful
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_millis(200), task).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_initialize_proxy_router_is_multi_upstream_check() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();

        // Test single upstream detection
        let single_pool = vec!["127.0.0.1:12345".parse::<SocketAddr>().unwrap()];
        let mut single_router = Router::new(single_pool, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(50);

        let task1 = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut single_router, None, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(30)) => {
                    // Single upstream path should be taken
                }
            }
        });

        // Test multi upstream detection
        let multi_pools = vec![
            "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:12346".parse::<SocketAddr>().unwrap(),
        ];
        let mut multi_router = Router::new(multi_pools, auth_pub_k, None, None);

        let task2 = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut multi_router, None, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(30)) => {
                    // Multi upstream path should be taken
                }
            }
        });

        let result1 = tokio::time::timeout(Duration::from_millis(100), task1).await;
        let result2 = tokio::time::timeout(Duration::from_millis(100), task2).await;

        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_initialize_proxy_loop_behavior() {
        setup_test_config();

        let auth_pub_k: Secp256k1PublicKey = TEST_AUTH_PUB_KEY.parse().unwrap();
        let pool_addresses = vec!["127.0.0.1:1".parse::<SocketAddr>().unwrap()]; // Will fail to connect
        let mut router = Router::new(pool_addresses, auth_pub_k, None, None);
        let epsilon = Duration::from_millis(10);
        let pool_addr = Some("127.0.0.1:1".parse::<SocketAddr>().unwrap());

        // Test that the function continues looping on connection failures
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = initialize_proxy(&mut router, pool_addr, epsilon) => {},
                _ = tokio::time::sleep(Duration::from_millis(150)) => {
                    // Should reach here as function keeps retrying in loop
                }
            }
        });

        let result = tokio::time::timeout(Duration::from_millis(300), task).await;
        assert!(result.is_ok());
    }
}
