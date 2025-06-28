use crate::config;
use bitcoincore_rpc::{Auth, Client, RpcApi};
use ini::Ini;
use std::{fs, path::Path, sync::Arc};

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

pub fn create_rpc_client() -> Result<Arc<Client>, String> {
    let datadir = config::Configuration::bitcoin_datadir();
    connect_to_rpc(&datadir)
        .map(Arc::new)
        .ok_or_else(|| "Failed to connect to Bitcoin RPC".to_string())
}
