use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::OnceLock,
    time::Duration,
};
use tracing::{debug, error, info};

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
    staging: Option<bool>,
    local: Option<bool>,
    testnet3: Option<bool>,
    listening_addr: Option<String>,
    api_server_port: Option<String>,
    monitor: Option<bool>,
    auto_update: Option<bool>,
    miner_name: Option<String>,
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
            staging: None,
            testnet3: None,
            local: None,
            listening_addr: None,
            api_server_port: None,
            monitor: None,
            auto_update: None,
            miner_name: None,
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
    file_logging: bool,
    staging: bool,
    testnet3: bool,
    local: bool,
    listening_addr: Option<String>,
    api_server_port: String,
    monitor: bool,
    auto_update: bool,
    signature: String,
    miner_name: Option<String>,
}
impl Configuration {
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
        file_logging: bool,
        staging: bool,
        testnet3: bool,
        local: bool,
        listening_addr: Option<String>,
        api_server_port: String,
        monitor: bool,
        auto_update: bool,
        signature: String,
        miner_name: Option<String>,
    ) -> Self {
        Configuration {
            token,
            tp_address,
            interval,
            delay,
            downstream_hashrate,
            loglevel,
            nc_loglevel,
            sv1_log,
            file_logging,
            staging,
            testnet3,
            local,
            listening_addr,
            api_server_port,
            monitor,
            auto_update,
            signature,
            miner_name,
        }
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
            true,
            None,
            "3001".to_string(),
            false,
            false,
            "DDxDD".to_string(),
            None,
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

        Configuration {
            token,
            tp_address,
            interval,
            delay,
            downstream_hashrate,
            loglevel,
            nc_loglevel,
            sv1_log,
            file_logging,
            staging,
            testnet3,
            local,
            listening_addr,
            api_server_port,
            monitor,
            auto_update,
            signature,
            miner_name,
        }
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
