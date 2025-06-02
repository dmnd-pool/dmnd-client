use std::str::FromStr;

use super::error::Error;
use bitcoin::{Address, Amount, Network, ScriptBuf, TxOut};
use serde::Deserialize;
use tracing::{error, info};

use crate::proxy_state::{ProxyState, TpState};

// Used when tp is down or connection was unsuccessful to retry connection.
pub async fn retry_connection(address: String) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
    loop {
        info!("TP Retrying connection....");
        interval.tick().await;
        if tokio::net::TcpStream::connect(address.clone())
            .await
            .is_ok()
        {
            info!("Successfully reconnected to TP: Restarting Proxy...");
            if crate::TP_ADDRESS
                .safe_lock(|tp| *tp = Some(address))
                .is_err()
            {
                error!("TP_ADDRESS Mutex failed");
                std::process::exit(1);
            };
            // This force the proxy to restart. If we use Up the proxy just ignore it.
            // So updating it to Down and setting the TP_ADDRESS to Some(address) will make the
            // proxy restart with TP, the the TpState will be set to Up.
            ProxyState::update_tp_state(TpState::Down);
            break;
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    payout_address: String,
    withhold: Option<bool>,
    network: String,
}
impl Config {
    pub fn withhold(self) -> Option<bool> {
        crate::ARGS.withhold.or(self.withhold)
    }
}

pub fn get_coinbase_output(config: &Config) -> Result<Vec<TxOut>, Error> {
    let network = match crate::ARGS
        .network
        .as_ref()
        .unwrap_or(&config.network)
        .to_lowercase()
        .as_str()
    {
        "bitcoin" => Network::Bitcoin,
        "testnet" => Network::Testnet,
        "regtest" => Network::Regtest,
        "signet" => Network::Signet,
        _ => {
            return Err(Error::InvalidNetwork(format!(
                "Unknown network: {}",
                config.network
            )))
        }
    };

    let payout_address = crate::ARGS
        .payout_address
        .as_ref()
        .unwrap_or(&config.payout_address);

    let addr =
        Address::from_str(payout_address).map_err(|e| Error::InvalidAddress(e.to_string()))?;

    let address = addr
        .require_network(network)
        .map_err(|e| Error::NetworkMismatch(e.to_string()))?;

    // Get the script pubkey from the address
    let script_pubkey: ScriptBuf = address.script_pubkey();

    info!("Detected script type: {:?}", address.address_type());
    println!("Script type: {:?}", script_pubkey);
    let tx_out = TxOut {
        value: Amount::from_sat(0),
        script_pubkey,
    };

    Ok(vec![tx_out])
}

pub fn parse_tp_address() -> Option<(String, u16, String)> {
    let tp_address = match crate::TP_ADDRESS.safe_lock(|tp| tp.clone()) {
        Ok(tp_address) => tp_address
            .expect("Unreachable code, jdc is not instantiated when TP_ADDRESS not present"),
        Err(e) => {
            error!("TP_ADDRESS mutex corrupted: {e}");
            return None;
        }
    };

    let mut parts = tp_address.split(':');
    let ip_tp = parts.next().expect("The passed value for TP address is not valid. Terminating.... TP_ADDRESS should be in this format `127.0.0.1:8442`").to_string();
    let port_tp = parts.next().expect("The passed value for TP address is not valid. Terminating.... TP_ADDRESS should be in this format `127.0.0.1:8442`").parse::<u16>().expect("This operation should not fail because a valid port_tp should always be converted to U16");

    Some((ip_tp, port_tp, tp_address))
}
