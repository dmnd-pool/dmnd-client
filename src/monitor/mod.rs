use reqwest::Url;
use serde_json::json;
use tracing::{debug, error};

use crate::{
    config::Configuration,
    monitor::{shares::ShareInfo, worker_activity::WorkerActivity},
    shared::error::Error,
    LOCAL_URL, PRODUCTION_URL, STAGING_URL, TESTNET3_URL,
};

pub mod shares;
pub mod worker_activity;
pub struct MonitorAPI {
    pub url: Url,
    pub client: reqwest::Client,
}

fn shares_server_endpoint() -> String {
    // Determine the monitoring server URL based on the environment
    match Configuration::environment().as_str() {
        "staging" => format!("{STAGING_URL}/api/share/save"),
        "testnet3" => format!("{TESTNET3_URL}/api/share/save"),
        "local" => format!("{LOCAL_URL}/api/share/save"),
        "production" => format!("{PRODUCTION_URL}/api/share/save"),
        _ => unreachable!(),
    }
}

fn worker_activity_server_endpoint() -> String {
    // Determine the monitoring server URL based on the environment
    match Configuration::environment().as_str() {
        "staging" => format!("{STAGING_URL}/api/worker/activity"),
        "testnet3" => format!("{TESTNET3_URL}/api/worker/activity"),
        "local" => format!("{LOCAL_URL}/api/worker/activity"),
        "production" => format!("{PRODUCTION_URL}/api/worker/activity"),
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
    async fn send_shares(&self, shares: Vec<ShareInfo>, token: &str) -> Result<(), Error> {
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

    /// Sends a worker activity log to the monitoring server.
    pub async fn send_worker_activity(
        &self,
        activity: WorkerActivity,
        token: &str,
    ) -> Result<(), Error> {
        debug!("Sending worker activity to API: {:?}", activity);
        let response = self
            .client
            .post(worker_activity_server_endpoint())
            .json(&json!({ "data": activity, "token": token }))
            .send()
            .await?;

        match response.error_for_status() {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("Failed to send worker activity: {}", err);
                Err(err.into())
            }
        }
    }
}
