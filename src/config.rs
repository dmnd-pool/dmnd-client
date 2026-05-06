use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::OnceLock,
    time::Duration,
};
use tracing::{debug, error, info, warn};

use crate::{
    shared::{
        error::Error,
        miner_tag::{format_miner_tag, validate_miner_name},
    },
    DEFAULT_SV1_HASHPOWER, PRODUCTION_URL, STAGING_URL, TESTNET3_URL,
};

static CONFIG: OnceLock<Configuration> = OnceLock::new();

#[derive(Parser, Default)]
struct Args {
    #[clap(long)]
    staging: bool,
    #[clap(long)]
    testnet3: bool,
    #[clap(long)]
    local: bool,
    #[clap(long = "d", short = 'd', value_parser = parse_hashrate)]
    downstream_hashrate: Option<f32>,
    #[clap(long = "loglevel", short = 'l')]
    loglevel: Option<String>,
    #[clap(long = "nc", short = 'n')]
    noise_connection_log: Option<String>,
    #[clap(long = "sv1_loglevel")]
    sv1_loglevel: bool,
    #[clap(long = "share-log")]
    share_log: bool,
    #[clap(long)]
    file_logging: bool,
    #[clap(long = "delay")]
    delay: Option<u64>,
    #[clap(long = "interval", short = 'i')]
    adjustment_interval: Option<u64>,
    #[clap(long)]
    token: Option<String>,
    #[clap(long)]
    tp_address: Option<String>,
    #[clap(long)]
    listening_addr: Option<String>,
    #[clap(long)]
    max_active_downstreams: Option<usize>,
    #[clap(long)]
    accept_backoff_ms: Option<u64>,
    #[clap(long)]
    accept_backlog: Option<u32>,
    #[clap(long)]
    max_accepts_per_window: Option<usize>,
    #[clap(long)]
    accept_window_ms: Option<u64>,
    #[clap(long = "config", short = 'c')]
    config_file: Option<PathBuf>,
    #[clap(long = "api-server-port", short = 's')]
    api_server_port: Option<String>,
    #[clap(long, short = 'm')]
    monitor: bool,
    #[clap(long, short = 'u')]
    auto_update: bool,
    #[clap(long)]
    signature: Option<String>,
    #[clap(long)]
    miner_name: Option<String>,
    #[clap(long)]
    rpc_url: Option<String>,
    #[clap(long)]
    rpc_user: Option<String>,
    #[clap(long)]
    rpc_pwd: Option<String>,
    #[clap(long)]
    rpc_fee_delta: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct ConfigFile {
    token: Option<String>,
    tp_address: Option<String>,
    interval: Option<u64>,
    delay: Option<u64>,
    downstream_hashrate: Option<String>,
    loglevel: Option<String>,
    nc_loglevel: Option<String>,
    sv1_log: Option<bool>,
    share_log: Option<bool>,
    staging: Option<bool>,
    local: Option<bool>,
    testnet3: Option<bool>,
    listening_addr: Option<String>,
    max_active_downstreams: Option<usize>,
    accept_backoff_ms: Option<u64>,
    accept_backlog: Option<u32>,
    max_accepts_per_window: Option<usize>,
    accept_window_ms: Option<u64>,
    api_server_port: Option<String>,
    monitor: Option<bool>,
    auto_update: Option<bool>,
    miner_name: Option<String>,
    rpc_url: Option<String>,
    rpc_user: Option<String>,
    rpc_pwd: Option<String>,
    rpc_fee_delta: Option<i64>,
}

impl ConfigFile {
    pub fn default() -> Self {
        ConfigFile {
            token: None,
            tp_address: None,
            interval: None,
            delay: None,
            downstream_hashrate: None,
            loglevel: None,
            nc_loglevel: None,
            sv1_log: None,
            share_log: None,
            staging: None,
            testnet3: None,
            local: None,
            listening_addr: None,
            max_active_downstreams: None,
            accept_backoff_ms: None,
            accept_backlog: None,
            max_accepts_per_window: None,
            accept_window_ms: None,
            api_server_port: None,
            monitor: None,
            auto_update: None,
            miner_name: None,
            rpc_url: None,
            rpc_user: None,
            rpc_pwd: None,
            rpc_fee_delta: None,
        }
    }
}

#[derive(Debug)]
pub struct Configuration {
    token: Option<String>,
    tp_address: Option<String>,
    interval: u64,
    delay: u64,
    downstream_hashrate: f32,
    loglevel: String,
    nc_loglevel: String,
    sv1_log: bool,
    share_log: bool,
    file_logging: bool,
    staging: bool,
    testnet3: bool,
    local: bool,
    listening_addr: Option<String>,
    max_active_downstreams: Option<usize>,
    accept_backoff_ms: u64,
    accept_backlog: u32,
    max_accepts_per_window: Option<usize>,
    accept_window_ms: u64,
    api_server_port: String,
    monitor: bool,
    auto_update: bool,
    signature: String,
    miner_name: Option<String>,
    prioritizing_txs_config: Option<BitcoindRpcConfig>,
    missing_prioritizing_txs_variables: Vec<&'static str>,
}

#[derive(Clone, Debug)]
pub(crate) struct BitcoindRpcConfig {
    pub url: String,
    pub user: String,
    pub pwd: String,
    pub fee_delta: i64,
}

impl Configuration {
    fn validate_supported_delay(delay: u64) -> Result<(), String> {
        if delay == 0 {
            return Ok(());
        }

        Err(format!(
            "Non-zero `delay` is currently unsupported.\n\n\
Known issue:\n\
When `delay > 0`, the translator keeps a scheduled bootstrap `mining.set_difficulty` replay alive during the delay window. \
If a downstream retarget happens before that replay fires, the stale bootstrap difficulty can be resent after the newer retarget. \
That leaves the miner's downstream difficulty out of sync with the bridge/upstream target that was already advanced.\n\n\
Configured `delay`: {delay}\n\n\
Do not use `--delay`, `DELAY`, or config `delay` until this is fixed.\n\
Before enabling non-zero delay again, remove `#[ignore]` from \
`translator::downstream::diff_management::test::positive_delay_does_not_replay_stale_bootstrap_difficulty_after_retarget` \
and make that test pass."
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        token: Option<String>,
        tp_address: Option<String>,
        interval: u64,
        delay: u64,
        downstream_hashrate: f32,
        loglevel: String,
        nc_loglevel: String,
        sv1_log: bool,
        share_log: bool,
        file_logging: bool,
        staging: bool,
        testnet3: bool,
        local: bool,
        listening_addr: Option<String>,
        max_active_downstreams: Option<usize>,
        accept_backoff_ms: u64,
        accept_backlog: u32,
        max_accepts_per_window: Option<usize>,
        accept_window_ms: u64,
        api_server_port: String,
        monitor: bool,
        auto_update: bool,
        signature: String,
        miner_name: Option<String>,
        rpc_url: String,
        rpc_user: String,
        rpc_pwd: String,
        rpc_fee_delta: String,
    ) -> Self {
        if let Err(error) = Self::validate_supported_delay(delay) {
            panic!("{error}");
        }

        let (prioritizing_txs_config, missing_prioritizing_txs_variables) =
            Self::build_prioritizing_txs_config(rpc_url, rpc_user, rpc_pwd, rpc_fee_delta);

        Configuration {
            token,
            tp_address,
            interval,
            delay,
            downstream_hashrate,
            loglevel,
            nc_loglevel,
            sv1_log,
            share_log,
            file_logging,
            staging,
            testnet3,
            local,
            listening_addr,
            max_active_downstreams,
            accept_backoff_ms,
            accept_backlog,
            max_accepts_per_window,
            accept_window_ms,
            api_server_port,
            monitor,
            auto_update,
            signature,
            miner_name,
            prioritizing_txs_config,
            missing_prioritizing_txs_variables,
        }
    }

    fn build_prioritizing_txs_config(
        url: String,
        user: String,
        pwd: String,
        fee_delta: String,
    ) -> (Option<BitcoindRpcConfig>, Vec<&'static str>) {
        let mut missing = Vec::new();
        if url.trim().is_empty() {
            missing.push("RPC_URL");
        }
        if user.trim().is_empty() {
            missing.push("RPC_USER");
        }
        if pwd.trim().is_empty() {
            missing.push("RPC_PWD");
        }
        if fee_delta.trim().is_empty() {
            missing.push("RPC_FEE_DELTA");
        }

        if !missing.is_empty() {
            return (None, missing);
        }

        let fee_delta = match fee_delta.trim().parse() {
            Ok(fee_delta) => fee_delta,
            Err(_) => return (None, vec!["RPC_FEE_DELTA"]),
        };

        (
            Some(BitcoindRpcConfig {
                url,
                user,
                pwd,
                fee_delta,
            }),
            missing,
        )
    }

    pub(crate) fn init(config: Configuration) {
        CONFIG
            .set(config)
            .expect("Configuration already initialized");
    }

    #[cfg(test)]
    fn default_for_tests() -> Self {
        Self::new(
            Some("test_token".to_string()),
            None,
            120_000,
            0,
            DEFAULT_SV1_HASHPOWER,
            "info".to_string(),
            "off".to_string(),
            false,
            false,
            false,
            false,
            false,
            true,
            None,
            Some(8001),
            250,
            16_384,
            Some(512),
            250,
            "3001".to_string(),
            false,
            false,
            "DDxDD".to_string(),
            None,
            "http://127.0.0.1:8332".to_string(),
            "user".to_string(),
            "password".to_string(),
            "100000000".to_string(),
        )
    }

    fn cfg() -> &'static Configuration {
        #[cfg(test)]
        {
            CONFIG.get_or_init(Self::default_for_tests)
        }

        #[cfg(not(test))]
        {
            CONFIG
                .get()
                .expect("Configuration not initialized; call start() first")
        }
    }

    pub fn token() -> Option<String> {
        Self::cfg().token.clone()
    }

    pub fn tp_address() -> Option<String> {
        Self::cfg().tp_address.clone()
    }

    pub async fn pool_address() -> Option<Vec<SocketAddr>> {
        match fetch_pool_urls().await {
            Ok(addresses) => Some(addresses),
            Err(e) => {
                error!("Failed to fetch pool addresses: {}", e);
                None
            }
        }
    }

    pub fn adjustment_interval() -> u64 {
        Self::cfg().interval
    }

    pub fn delay() -> u64 {
        Self::cfg().delay
    }

    pub fn difficulty_updates_disabled() -> bool {
        Self::delay() > 100_000
    }

    pub fn downstream_hashrate() -> f32 {
        Self::cfg().downstream_hashrate
    }

    pub fn downstream_listening_addr() -> Option<String> {
        Self::cfg().listening_addr.clone()
    }

    pub fn max_active_downstreams() -> Option<usize> {
        Self::cfg()
            .max_active_downstreams
            .filter(|value| *value > 0)
    }

    pub fn accept_backoff_ms() -> u64 {
        Self::cfg().accept_backoff_ms
    }

    pub fn accept_backlog() -> u32 {
        Self::cfg().accept_backlog
    }

    pub fn max_accepts_per_window() -> Option<usize> {
        Self::cfg()
            .max_accepts_per_window
            .filter(|value| *value > 0)
    }

    pub fn accept_window_ms() -> u64 {
        Self::cfg().accept_window_ms
    }

    pub fn api_server_port() -> String {
        Self::cfg().api_server_port.clone()
    }

    pub fn loglevel() -> &'static str {
        match Self::cfg().loglevel.to_lowercase().as_str() {
            "trace" | "debug" | "info" | "warn" | "error" | "off" => &Self::cfg().loglevel,
            _ => {
                eprintln!(
                    "Invalid log level '{}'. Defaulting to 'info'.",
                    Self::cfg().loglevel
                );
                "info"
            }
        }
    }

    pub fn nc_loglevel() -> &'static str {
        match Self::cfg().nc_loglevel.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" | "off" => &Self::cfg().nc_loglevel,
            _ => {
                eprintln!(
                    "Invalid log level for noise_connection '{}' Defaulting to 'off'.",
                    &Self::cfg().nc_loglevel
                );
                "off"
            }
        }
    }

    pub fn enable_file_logging() -> bool {
        Self::cfg().file_logging
    }
    pub fn sv1_ingress_log() -> bool {
        Self::cfg().sv1_log
    }

    pub fn share_log() -> bool {
        Self::cfg().share_log
    }

    pub fn staging() -> bool {
        Self::cfg().staging
    }

    pub fn local() -> bool {
        Self::cfg().local
    }

    pub fn testnet3() -> bool {
        Self::cfg().testnet3
    }

    /// Returns the environment based on the configuration.
    /// Possible values: "staging", "local", "production".
    /// If no environment is set, it defaults to "production".
    pub fn environment() -> String {
        if Self::cfg().staging {
            "staging".to_string()
        } else if Self::cfg().local {
            "local".to_string()
        } else if Self::cfg().testnet3 {
            "testnet3".to_string()
        } else {
            "production".to_string()
        }
    }

    pub fn monitor() -> bool {
        Self::cfg().monitor
    }

    pub fn auto_update() -> bool {
        Self::cfg().auto_update
    }

    pub fn signature() -> String {
        Self::cfg().signature.clone()
    }

    pub fn miner_name() -> Option<String> {
        Self::cfg().miner_name.clone()
    }

    pub(crate) fn prioritizing_txs_enabled() -> bool {
        Self::cfg().prioritizing_txs_config.is_some()
    }

    pub(crate) fn bitcoind_rpc_config() -> Option<BitcoindRpcConfig> {
        if !Self::prioritizing_txs_enabled() {
            return None;
        }

        Self::cfg().prioritizing_txs_config.clone()
    }

    pub(crate) fn log_prioritizing_txs_status() {
        let missing = &Self::cfg().missing_prioritizing_txs_variables;
        if missing.is_empty() {
            return;
        }

        if missing.len() == 4 {
            warn!("PRIORITIZING TXS NOT ENABLED");
        } else {
            error!(
                missing_env_variables = %missing.join(", "),
                "PRIORITIZING TXS NOT ENABLED, missing env variable"
            );
        }
    }

    // Loads config from CLI args, config file, and env vars with precedence: CLI > file > env.
    pub fn from_cli() -> Self {
        let args = Args::parse();
        let config_path: PathBuf = args
            .config_file
            .or_else(|| {
                std::env::var("DMND_CLIENT_CONFIG_FILE")
                    .ok()
                    .map(PathBuf::from)
            })
            .unwrap_or("config.toml".into());
        let config: ConfigFile = std::fs::read_to_string(&config_path)
            .ok()
            .and_then(|content| toml::from_str(&content).ok())
            .unwrap_or(ConfigFile::default());

        let token = args
            .token
            .or(config.token)
            .or_else(|| std::env::var("TOKEN").ok());
        println!("User Token: {token:?}");

        let signature = match args.signature {
            Some(s) => {
                if s.len() == 2 {
                    println!("Signature provided: DDx{s}");
                    format!("DDx{s}")
                } else {
                    println!("Invalid signature provided, using DDxDD");
                    "DDxDD".to_string()
                }
            }
            None => {
                println!("Signature not provided, using DDxDD");
                "DDxDD".to_string()
            }
        };

        let tp_address = args
            .tp_address
            .or(config.tp_address)
            .or_else(|| std::env::var("TP_ADDRESS").ok());

        let miner_name = args
            .miner_name
            .or(config.miner_name)
            .or_else(|| std::env::var("MINER_NAME").ok());

        let rpc_url = args
            .rpc_url
            .or(config.rpc_url)
            .or_else(|| std::env::var("RPC_URL").ok())
            .unwrap_or_default();
        let rpc_user = args
            .rpc_user
            .or(config.rpc_user)
            .or_else(|| std::env::var("RPC_USER").ok())
            .unwrap_or_default();
        let rpc_pwd = args
            .rpc_pwd
            .or(config.rpc_pwd)
            .or_else(|| std::env::var("RPC_PWD").ok())
            .unwrap_or_default();
        let rpc_fee_delta = args
            .rpc_fee_delta
            .map(|value| value.to_string())
            .or_else(|| config.rpc_fee_delta.map(|value| value.to_string()))
            .or_else(|| std::env::var("RPC_FEE_DELTA").ok())
            .unwrap_or_default();
        if let Some(ref miner_name) = miner_name {
            validate_miner_name(miner_name).unwrap_or_else(|e| panic!("{e}"));
        }
        println!(
            "Using miner tag: {}",
            format_miner_tag(miner_name.as_deref())
        );

        let interval = args
            .adjustment_interval
            .or(config.interval)
            .or_else(|| std::env::var("INTERVAL").ok().and_then(|s| s.parse().ok()))
            .unwrap_or(120_000);

        let delay = args
            .delay
            .or(config.delay)
            .or_else(|| std::env::var("DELAY").ok().and_then(|s| s.parse().ok()))
            .unwrap_or(0);

        let expected_hashrate = args
            .downstream_hashrate
            .or_else(|| {
                config
                    .downstream_hashrate
                    .as_deref()
                    .and_then(|d| parse_hashrate(d).ok())
            })
            .or_else(|| {
                std::env::var("DOWNSTREAM_HASHRATE")
                    .ok()
                    .and_then(|s| s.parse().ok())
            });
        let downstream_hashrate;
        if let Some(hashpower) = expected_hashrate {
            downstream_hashrate = hashpower;
            println!(
                "Using downstream hashrate: {}h/s",
                HashUnit::format_value(hashpower)
            );
        } else {
            downstream_hashrate = DEFAULT_SV1_HASHPOWER;
            println!(
                "No downstream hashrate provided, using default value: {}h/s",
                HashUnit::format_value(DEFAULT_SV1_HASHPOWER)
            );
        }

        let listening_addr = args.listening_addr.or(config.listening_addr).or_else(|| {
            std::env::var("LISTENING_ADDR")
                .ok()
                .and_then(|s| s.parse().ok())
        });
        let max_active_downstreams = args
            .max_active_downstreams
            .or(config.max_active_downstreams)
            .or_else(|| {
                std::env::var("MAX_ACTIVE_DOWNSTREAMS")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .or(Some(8001))
            .filter(|value| *value > 0);
        let accept_backoff_ms = args
            .accept_backoff_ms
            .or(config.accept_backoff_ms)
            .or_else(|| {
                std::env::var("ACCEPT_BACKOFF_MS")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(250);
        let accept_backlog = args
            .accept_backlog
            .or(config.accept_backlog)
            .or_else(|| {
                std::env::var("ACCEPT_BACKLOG")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(16_384);
        let max_accepts_per_window = args
            .max_accepts_per_window
            .or(config.max_accepts_per_window)
            .or_else(|| {
                std::env::var("MAX_ACCEPTS_PER_WINDOW")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .or(Some(512))
            .filter(|value| *value > 0);
        let accept_window_ms = args
            .accept_window_ms
            .or(config.accept_window_ms)
            .or_else(|| {
                std::env::var("ACCEPT_WINDOW_MS")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(250);
        let api_server_port = args
            .api_server_port
            .or(config.api_server_port)
            .or_else(|| {
                std::env::var("API_SERVER_PORT")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or("3001".to_string());

        let loglevel = args
            .loglevel
            .or(config.loglevel)
            .or_else(|| std::env::var("LOGLEVEL").ok())
            .unwrap_or("info".to_string());

        let nc_loglevel = args
            .noise_connection_log
            .or(config.nc_loglevel)
            .or_else(|| std::env::var("NC_LOGLEVEL").ok())
            .unwrap_or("off".to_string());

        let sv1_log = args.sv1_loglevel
            || config.sv1_log.unwrap_or(false)
            || std::env::var("SV1_LOGLEVEL").is_ok();

        let share_log = args.share_log
            || config.share_log.unwrap_or(false)
            || std::env::var("SHARE_LOG").is_ok();

        let file_logging = args.file_logging || std::env::var("FILE_LOGGING").is_ok();

        let staging =
            args.staging || config.staging.unwrap_or(false) || std::env::var("STAGING").is_ok();
        let testnet3 =
            args.testnet3 || config.testnet3.unwrap_or(false) || std::env::var("TESTNET3").is_ok();
        let local = args.local || config.local.unwrap_or(false) || std::env::var("LOCAL").is_ok();
        let monitor =
            args.monitor || config.monitor.unwrap_or(false) || std::env::var("MONITOR").is_ok();

        let auto_update = args.auto_update
            || config.auto_update.unwrap_or(true)
            || std::env::var("AUTO_UPDATE").is_ok();

        Self::new(
            token,
            tp_address,
            interval,
            delay,
            downstream_hashrate,
            loglevel,
            nc_loglevel,
            sv1_log,
            share_log,
            file_logging,
            staging,
            testnet3,
            local,
            listening_addr,
            max_active_downstreams,
            accept_backoff_ms,
            accept_backlog,
            max_accepts_per_window,
            accept_window_ms,
            api_server_port,
            monitor,
            auto_update,
            signature,
            miner_name,
            rpc_url,
            rpc_user,
            rpc_pwd,
            rpc_fee_delta,
        )
    }
}

/// Parses a hashrate string (e.g., "10T", "2.5P", "500E") into an f32 value in h/s.
fn parse_hashrate(hashrate_str: &str) -> Result<f32, String> {
    info!("Received hashrate: '{}'", hashrate_str);
    let hashrate_str = hashrate_str.trim();
    if hashrate_str.is_empty() {
        return Err("Hashrate cannot be empty. Expected format: '<number><unit>' (e.g., '10T', '2.5P', '5E'".to_string());
    }

    let unit = hashrate_str.chars().last().unwrap_or(' ').to_string();
    let num = &hashrate_str[..hashrate_str.len().saturating_sub(1)];

    let num: f32 = num.parse().map_err(|_| {
        format!(
            "Invalid number '{num}'. Expected format: '<number><unit>' (e.g., '10T', '2.5P', '5E')"
        )
    })?;

    let multiplier = HashUnit::from_str(&unit)
        .map(|unit| unit.multiplier())
        .ok_or_else(|| format!(
            "Invalid unit '{unit}'. Expected 'T' (Terahash), 'P' (Petahash), or 'E' (Exahash). Example: '10T', '2.5P', '5E'"
        ))?;

    let hashrate = num * multiplier;

    if hashrate.is_infinite() || hashrate.is_nan() {
        return Err("Hashrate too large or invalid".to_string());
    }
    info!("Parsed hashrate: {} h/s", hashrate);
    Ok(hashrate)
}

fn parse_address(addr: String) -> Option<SocketAddr> {
    match addr.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(socket_addr) => Some(socket_addr),
            None => {
                error!("Failed to parse address: {}", addr);
                None
            }
        },
        Err(e) => {
            error!("Failed to parse address '{}': {}", addr, e);
            None
        }
    }
}

/// Fetches pool URLs from the server based on the environment.
async fn fetch_pool_urls() -> Result<Vec<SocketAddr>, Error> {
    if Configuration::cfg().local {
        info!("Running in local mode, using hardcoded address 127.0.0.1:20000");
        return Ok(vec![
            parse_address("127.0.0.1:20000".to_string()).expect("Invalid local address")
        ]);
    };
    let url = if Configuration::cfg().staging {
        STAGING_URL
    } else if Configuration::cfg().testnet3 {
        TESTNET3_URL
    } else {
        PRODUCTION_URL
    };
    let endpoint = format!("{url}/api/pool/urls");
    info!("Fetching pool URLs from: {}", endpoint);
    let token = Configuration::token().expect("TOKEN is not set");
    let mut retries = 8;
    let client = reqwest::Client::new();

    let response = loop {
        let request = client
            .post(endpoint.clone())
            .json(&json!({"token": token}))
            .timeout(Duration::from_secs(15));

        match request.send().await {
            Ok(resp) => break resp,
            Err(e) => {
                error!("Failed to fetch pool urls: {}", e);
                if retries == 0 {
                    return Err(Error::from(e));
                }
                retries -= 1;
                info!("Retrying in 3 seconds...");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    };

    debug!("Response status: {}", response.status());
    let addresses: Vec<PoolAddress> = match response.json().await {
        Ok(addrs) => addrs,
        Err(e) => {
            error!("Failed to parse pool urls: {}", e);
            return Err(Error::from(e));
        }
    };

    // Parse the addresses into SocketAddr
    let socket_addrs: Vec<SocketAddr> = addresses
        .into_iter()
        .filter_map(|addr| {
            let address = format!("{}:{}", addr.host, addr.port);
            parse_address(address)
        }) // Filter out any None values, i.e., invalid addresses
        .collect();
    info!("Found {} pool addresses", socket_addrs.len());
    info!("Pool addresses: {:?}", &socket_addrs);
    Ok(socket_addrs)
}

#[derive(Debug, Deserialize)]
struct PoolAddress {
    host: String,
    port: u16,
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
            format!("{hashrate:.2}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Configuration;

    #[test]
    fn zero_delay_remains_supported() {
        assert!(Configuration::validate_supported_delay(0).is_ok());
    }

    #[test]
    fn non_zero_delay_message_describes_bug_and_fix_path() {
        let error = Configuration::validate_supported_delay(1).unwrap_err();

        assert!(error.contains("Non-zero `delay` is currently unsupported"));
        assert!(error.contains("bootstrap `mining.set_difficulty`"));
        assert!(error
            .contains("positive_delay_does_not_replay_stale_bootstrap_difficulty_after_retarget"));
        assert!(error.contains("Do not use `--delay`, `DELAY`, or config `delay`"));
    }

    #[test]
    fn prioritizing_txs_requires_all_rpc_values() {
        let (config, missing) = Configuration::build_prioritizing_txs_config(
            "http://127.0.0.1:8332".to_string(),
            "user".to_string(),
            "password".to_string(),
            "42".to_string(),
        );

        assert!(missing.is_empty());
        assert_eq!(config.expect("config should be enabled").fee_delta, 42);

        let (config, missing) = Configuration::build_prioritizing_txs_config(
            "http://127.0.0.1:8332".to_string(),
            "".to_string(),
            "password".to_string(),
            "42".to_string(),
        );

        assert!(config.is_none());
        assert_eq!(missing, vec!["RPC_USER"]);
    }

    #[test]
    fn prioritizing_txs_reports_all_missing_rpc_values() {
        let (config, missing) = Configuration::build_prioritizing_txs_config(
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
        );

        assert!(config.is_none());
        assert_eq!(
            missing,
            vec!["RPC_URL", "RPC_USER", "RPC_PWD", "RPC_FEE_DELTA"]
        );
    }
}
