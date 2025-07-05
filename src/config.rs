use clap::Parser;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    time::Duration,
};
use tracing::{debug, error, info, warn};

use crate::{
    shared::error::Error, HashUnit, DEFAULT_SV1_HASHPOWER, PRODUCTION_URL, STAGING_URL,
    TESTNET3_URL,
};
lazy_static! {
    pub static ref CONFIG: Configuration = Configuration::load_config();
}
#[derive(Parser)]
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
    #[clap(long = "custom-job-timeout")]
    custom_job_timeout: Option<u64>,
    #[clap(long = "zmq-pub-sequence")]
    zmq_pub_sequence: Option<String>,
    #[clap(long = "rpc-allow-ip")]
    rpc_allow_ip: Option<String>,
    #[clap(long = "rpc-port")]
    rpc_port: Option<u16>,
    #[clap(long = "rpc-username")]
    rpcusername: Option<String>,
    #[clap(long = "rpc-password")]
    rpcpassword: Option<String>,
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
    custom_job_timeout: Option<u64>,
    zmq_pub_sequence: Option<String>,
    rpc_allow_ip: Option<String>,
    rpc_port: Option<u16>,
    rpcusername: Option<String>,
    rpcpassword: Option<String>,
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
            custom_job_timeout: None,
            zmq_pub_sequence: None,
            rpc_allow_ip: None,
            rpc_port: None,
            rpcusername: None,
            rpcpassword: None,
        }
    }
}
pub struct Configuration {
    token: Option<String>,
    tp_address: Option<String>,
    interval: u64,
    delay: u64,
    downstream_hashrate: f32,
    loglevel: String,
    nc_loglevel: String,
    sv1_log: bool,
    staging: bool,
    testnet3: bool,
    local: bool,
    listening_addr: Option<String>,
    api_server_port: String,
    monitor: bool,
    auto_update: bool,
    custom_job_timeout: u64,
    zmq_pub_sequence: String,
    rpc_allow_ip: String,
    rpc_port: u16,
    rpcusername: String,
    rpcpassword: String,
}
impl Configuration {
    pub fn token() -> Option<String> {
        CONFIG.token.clone()
    }

    pub fn tp_address() -> Option<String> {
        CONFIG.tp_address.clone()
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
        CONFIG.interval
    }

    pub fn delay() -> u64 {
        CONFIG.delay
    }

    pub fn downstream_hashrate() -> f32 {
        CONFIG.downstream_hashrate
    }

    pub fn downstream_listening_addr() -> Option<String> {
        CONFIG.listening_addr.clone()
    }

    pub fn api_server_port() -> String {
        CONFIG.api_server_port.clone()
    }

    pub fn loglevel() -> &'static str {
        match CONFIG.loglevel.to_lowercase().as_str() {
            "trace" | "debug" | "info" | "warn" | "error" | "off" => &CONFIG.loglevel,
            _ => {
                eprintln!(
                    "Invalid log level '{}'. Defaulting to 'info'.",
                    CONFIG.loglevel
                );
                "info"
            }
        }
    }

    pub fn nc_loglevel() -> &'static str {
        match CONFIG.nc_loglevel.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" | "off" => &CONFIG.nc_loglevel,
            _ => {
                eprintln!(
                    "Invalid log level for noise_connection '{}' Defaulting to 'off'.",
                    &CONFIG.nc_loglevel
                );
                "off"
            }
        }
    }
    pub fn sv1_ingress_log() -> bool {
        CONFIG.sv1_log
    }

    pub fn staging() -> bool {
        CONFIG.staging
    }

    pub fn local() -> bool {
        CONFIG.local
    }

    pub fn testnet3() -> bool {
        CONFIG.testnet3
    }

    /// Returns the environment based on the configuration.
    /// Possible values: "staging", "local", "production".
    /// If no environment is set, it defaults to "production".
    pub fn environment() -> String {
        if CONFIG.staging {
            "staging".to_string()
        } else if CONFIG.local {
            "local".to_string()
        } else if CONFIG.testnet3 {
            "testnet3".to_string()
        } else {
            "production".to_string()
        }
    }

    pub fn monitor() -> bool {
        CONFIG.monitor
    }

    pub fn auto_update() -> bool {
        CONFIG.auto_update
    }

    pub fn custom_job_timeout() -> u64 {
        CONFIG.custom_job_timeout
    }

    pub fn zmq_pub_sequence() -> &'static str {
        &CONFIG.zmq_pub_sequence
    }

    pub fn rpc_allow_ip() -> &'static str {
        &CONFIG.rpc_allow_ip
    }

    pub fn rpc_port() -> u16 {
        CONFIG.rpc_port
    }

    pub fn rpcusername() -> &'static String {
        &CONFIG.rpcusername
    }

    pub fn rpcpassword() -> &'static String {
        &CONFIG.rpcpassword
    }

    // Loads config from CLI, file, or env vars with precedence: CLI > file > env.
    fn load_config() -> Self {
        let args = Args::parse();
        let config_path: PathBuf = args.config_file.unwrap_or("config.toml".into());
        let config: ConfigFile = std::fs::read_to_string(&config_path)
            .ok()
            .and_then(|content| toml::from_str(&content).ok())
            .unwrap_or(ConfigFile::default());

        let token = args
            .token
            .or(config.token)
            .or_else(|| std::env::var("TOKEN").ok());
        debug!("User Token: {:?}", token);

        let tp_address = args
            .tp_address
            .or(config.tp_address)
            .or_else(|| std::env::var("TP_ADDRESS").ok());

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
            info!(
                "Using downstream hashrate: {}h/s",
                HashUnit::format_value(hashpower)
            );
        } else {
            downstream_hashrate = DEFAULT_SV1_HASHPOWER;
            warn!(
                "No downstream hashrate provided, using default value: {}h/s",
                HashUnit::format_value(DEFAULT_SV1_HASHPOWER)
            );
        }

        let listening_addr = args.listening_addr.or(config.listening_addr).or_else(|| {
            std::env::var("DOWNSTREAM_HASHRATE")
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

        let custom_job_timeout = args
            .custom_job_timeout
            .or(config.custom_job_timeout)
            .or_else(|| {
                std::env::var("CUSTOM_JOB_TIMEOUT")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(30);

        let zmq_pub_sequence = args
            .zmq_pub_sequence
            .or(config.zmq_pub_sequence)
            .or_else(|| std::env::var("ZMQ_PUB_SEQUENCE").ok())
            .unwrap_or_else(|| "tcp://127.0.0.1:28334".to_string());

        let rpc_allow_ip = args
            .rpc_allow_ip
            .or(config.rpc_allow_ip)
            .or_else(|| std::env::var("RPC_ALLOW_IP").ok())
            .unwrap_or_else(|| "127.0.0.1".to_string());

        let rpc_port = args
            .rpc_port
            .or(config.rpc_port)
            .or_else(|| std::env::var("RPC_PORT").ok().and_then(|s| s.parse().ok()))
            .unwrap_or(8332);

        let rpcusername = args
            .rpcusername
            .or(config.rpcusername)
            .or_else(|| std::env::var("RPC_USERNAME").ok())
            .map(|username| username.to_string())
            .unwrap_or_default();

        let rpcpassword = args
            .rpcpassword
            .or(config.rpcpassword)
            .or_else(|| std::env::var("RPC_PASSWORD").ok())
            .map(|password| password.to_string())
            .unwrap_or_default();

        Configuration {
            token,
            tp_address,
            interval,
            delay,
            downstream_hashrate,
            loglevel,
            nc_loglevel,
            sv1_log,
            staging,
            testnet3,
            local,
            listening_addr,
            api_server_port,
            monitor,
            auto_update,
            custom_job_timeout,
            zmq_pub_sequence,
            rpc_allow_ip,
            rpc_port,
            rpcusername,
            rpcpassword,
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
    if CONFIG.local {
        info!("Running in local mode, using hardcoded address 127.0.0.1:20000");
        return Ok(vec![
            parse_address("127.0.0.1:20000".to_string()).expect("Invalid local address")
        ]);
    };
    let url = if CONFIG.staging {
        STAGING_URL
    } else if CONFIG.testnet3 {
        TESTNET3_URL
    } else {
        PRODUCTION_URL
    };
    let endpoint = format!("{}/api/pool/urls", url);
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
