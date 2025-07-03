use crate::config;
use bitcoincore_rpc::{Auth, Client, RpcApi};
use ini::Ini;
use std::{fs, path::Path, sync::Arc};

/// Attempts to read cookie-based authentication credentials from the Bitcoin data directory.
///
/// Looks for a `.cookie` file in the given `datadir`. If found, parses the file to extract the username and password.
/// Returns `Some(Auth::UserPass)` if successful, or `None` if the file is missing or invalid.
///
/// # Arguments
/// * `datadir` - Path to the Bitcoin data directory.
///
/// Returns
/// * `Some(Auth)` if credentials are found, otherwise `None`.
pub fn get_cookie_auth(datadir: &Path) -> Option<Auth> {
    let cookie_path = datadir.join(".cookie");
    println!("Looking for cookie file at: {}", cookie_path.display());
    if cookie_path.exists() {
        println!("Found cookie file at: {}", cookie_path.display());
    } else {
        println!("Cookie file not found at: {}", cookie_path.display());
        println!("Trying to use rpcuser/rpcpassword from bitcoin.conf instead.");
        return None;
    }
    let contents = fs::read_to_string(cookie_path).ok()?;
    let mut parts = contents.trim().split(':');
    let user = parts.next()?.to_string();
    let pass = parts.next()?.to_string();
    Some(Auth::UserPass(user, pass))
}

/// Attempts to read RPC credentials from `bitcoin.conf` in the given data directory.
///
/// Searches for `rpcuser` and `rpcpassword` in the general section or `[main]` section of the config file.
/// Returns `Some(Auth::UserPass)` if both are found, otherwise `None`.
///
/// # Arguments
/// * `datadir` - Path to the Bitcoin data directory.
///
/// # Returns
/// * `Some(Auth)` if credentials are found, otherwise `None`.
pub fn get_basic_auth_from_conf(datadir: &Path) -> Option<Auth> {
    println!("Looking for bitcoin.conf in: {}", datadir.display());
    if !datadir.exists() {
        println!(
            "Bitcoin data directory does not exist: {}",
            datadir.display()
        );
        return None;
    }
    let conf_path = datadir.join("bitcoin.conf");
    let conf = Ini::load_from_file(conf_path).ok()?;

    // First try the general section (no section header)
    let section = conf.general_section();
    let mut rpcuser = section.get("rpcuser").map(|s| s.to_string());
    let mut rpcpassword = section.get("rpcpassword").map(|s| s.to_string());

    // If not found in general section, try [main] sections
    if rpcuser.is_none() || rpcpassword.is_none() {
        for (section_name, section) in conf.iter() {
            if section_name == Some("main") {
                println!("Checking section: {:?}", section_name);
                if rpcuser.is_none() {
                    if let Some(user) = section.get("rpcuser") {
                        rpcuser = Some(user.to_string());
                    }
                }
                if rpcpassword.is_none() {
                    if let Some(pass) = section.get("rpcpassword") {
                        rpcpassword = Some(pass.to_string());
                    }
                }
                if rpcuser.is_some() && rpcpassword.is_some() {
                    break;
                }
            }
        }
    }

    match (rpcuser, rpcpassword) {
        (Some(user), Some(pass)) => {
            println!("Found RPC credentials in bitcoin.conf");
            println!("Using rpcuser: {}", user);
            println!("Using rpcpassword: {}", pass);
            Some(Auth::UserPass(user, pass))
        }
        _ => None,
    }
}

/// Attempts to connect to the Bitcoin RPC using available authentication methods.
///
/// Tries cookie-based authentication first, then falls back to credentials from `bitcoin.conf`.
/// Returns a `Client` if authentication and connection succeed, otherwise `None`.
///
/// # Arguments
/// * `datadir` - Path to the Bitcoin data directory.
///
/// # Returns
/// * `Some(Client)` if connection is successful, otherwise `None`.
pub fn connect_to_rpc(datadir: &Path) -> Option<Client> {
    let rpc_url = format!(
        "http://{}:{}",
        config::Configuration::rpc_allow_ip(),
        config::Configuration::rpc_port()
    );

    if let Some(auth) = get_cookie_auth(datadir) {
        println!("Using cookie-based authentication");
        if let Ok(client) = Client::new(&rpc_url, auth.clone()) {
            if client.get_blockchain_info().is_ok() {
                return Some(client);
            }
        }
    }

    if let Some(auth) = get_basic_auth_from_conf(datadir) {
        println!("Trying rpcuser/rpcpassword from bitcoin.conf");
        if let Ok(client) = Client::new(&rpc_url, auth.clone()) {
            if client.get_blockchain_info().is_ok() {
                return Some(client);
            }
        }
    }

    println!("Failed to authenticate with Bitcoin RPC");
    None
}

/// Creates a shared (Arc) Bitcoin RPC client using the configured data directory.
///
/// Returns an error string if connection fails.
///
/// # Returns
/// * `Ok(Arc<Client>)` if connection is successful, otherwise `Err(String)`.
pub fn create_rpc_client() -> Result<Arc<Client>, String> {
    let datadir = config::Configuration::bitcoin_datadir();
    connect_to_rpc(&datadir)
        .map(Arc::new)
        .ok_or_else(|| "Failed to connect to Bitcoin RPC".to_string())
}
