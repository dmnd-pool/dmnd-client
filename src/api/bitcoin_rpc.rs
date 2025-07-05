use crate::config;
use bitcoincore_rpc::{Auth, Client};
use std::sync::Arc;

/// Creates a shared (Arc) Bitcoin RPC client using the configured data directory.
///
/// Returns an error string if connection fails.
///
/// # Returns
/// * `Ok(Arc<Client>)` if connection is successful, otherwise `Err(String)`.
pub fn create_rpc_client() -> Result<Arc<Client>, String> {
    let rpcusername = config::Configuration::rpcusername();
    let rpcpassword = config::Configuration::rpcpassword();

    if rpcusername.is_empty() || rpcpassword.is_empty() {
        return Err("RPC credentials not found".to_string());
    }

    let auth = Auth::UserPass(rpcusername.into(), rpcpassword.into());
    let rpc_url = format!(
        "http://{}:{}",
        config::Configuration::rpc_allow_ip(),
        config::Configuration::rpc_port()
    );

    match Client::new(&rpc_url, auth) {
        Ok(client) => Ok(Arc::new(client)),
        Err(e) => Err(format!("Failed to create RPC client: {}", e)),
    }
}
