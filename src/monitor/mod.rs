use reqwest::Url;
use serde_json::json;
use tracing::{debug, error};

use crate::{
    config::Configuration,
    monitor::{logs::ProxyLog, shares::ShareInfo},
    shared::error::Error,
    LOCAL_URL, PRODUCTION_URL, STAGING_URL, TESTNET3_URL,
};

pub mod logs;
pub mod shares;

pub struct MonitorAPI {
    pub url: Url,
    pub client: reqwest::Client,
}
fn proxy_log_server_endpoint() -> String {
    match Configuration::environment().as_str() {
        "staging" => format!("{}/api/proxy/logs", STAGING_URL),
        "testnet3" => format!("{}/api/proxy/logs", TESTNET3_URL),
        "local" => format!("{}/api/proxy/logs", LOCAL_URL),
        "production" => format!("{}/api/proxy/logs", PRODUCTION_URL),
        _ => unreachable!(),
    }
}
fn shares_server_endpoint() -> String {
    // Determine the monitoring server URL based on the environment
    match Configuration::environment().as_str() {
        "staging" => format!("{}/api/share/save", STAGING_URL),
        "testnet3" => format!("{}/api/share/save", TESTNET3_URL),
        "local" => format!("{}/api/share/save", LOCAL_URL),
        "production" => format!("{}/api/share/save", PRODUCTION_URL),
        _ => unreachable!(),
    }
}

impl MonitorAPI {
    pub fn new(url: String) -> Self {
        let client = reqwest::Client::new();
        MonitorAPI {
            url: url.parse().expect("Invalid URL"),
            client,
        }
    }

    /// Sends a batch of shares to the monitoring server.
    async fn send_shares(&self, shares: Vec<ShareInfo>) -> Result<(), Error> {
        let token = crate::config::Configuration::token().expect("Token is not set");

        debug!("Sending batch of {} shares to API", shares.len());
        let response = self
            .client
            .post(self.url.clone())
            .json(&json!({ "shares": shares, "token": token }))
            .send()
            .await?;

        match response.error_for_status() {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("Failed to send shares: {}", err);
                Err(err.into())
            }
        }
    }

    /// Sends a log to the monitoring server.
    pub async fn send_log(&self, log: ProxyLog) -> Result<(), Error> {
        let token = crate::config::Configuration::token().expect("Token is not set");

        debug!("Sending log to API: {:?}", log);
        let response = self
            .client
            .post(self.url.clone())
            .json(&json!({ "log": log, "token": token }))
            .send()
            .await?;

        match response.error_for_status() {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("Failed to send log: {}", err);
                Err(err.into())
            }
        }
    }
}
