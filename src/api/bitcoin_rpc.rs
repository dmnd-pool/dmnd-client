use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{fmt, time::Duration};
use tracing::warn;

const DEFAULT_RPC_URL: &str = "http://127.0.0.1:8332";
const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_RPC_FEE_DELTA: i64 = 100_000_000;

pub(crate) struct BitcoindRpc {
    url: Option<String>,
    user: Option<String>,
    pwd: Option<String>,
    client: reqwest::Client,
    fee_delta: i64,
}

impl BitcoindRpc {
    pub(crate) fn new(
        url: Option<String>,
        user: Option<String>,
        pwd: Option<String>,
        fee_delta: Option<i64>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(RPC_CONNECT_TIMEOUT)
            .timeout(RPC_REQUEST_TIMEOUT)
            .build()
            .expect("Failed to build client");
        let fee_delta = fee_delta.unwrap_or(DEFAULT_RPC_FEE_DELTA);

        Self {
            url,
            user,
            pwd,
            client,
            fee_delta,
        }
    }

    pub(crate) async fn submit_transaction(&self, tx: &str) -> Result<String, BitcoindRpcError> {
        let tx = validate_transaction_hex(tx)?;
        let (status, text) = self.send_request("sendrawtransaction", json!([tx])).await?;
        let txid = RpcResponse::from_response(status, &text)?
            .and_then(|v| v.as_str().map(str::to_owned))
            .ok_or_else(|| {
                BitcoindRpcError::InvalidResponse("empty response from bitcoind".to_string())
            })?;
        self.prioritise_transaction(&txid).await?;
        Ok(txid)
    }

    async fn prioritise_transaction(&self, txid: &str) -> Result<(), BitcoindRpcError> {
        let (status, text) = self
            .send_request("prioritisetransaction", json!([txid, 0, self.fee_delta]))
            .await?;
        RpcResponse::from_response(status, &text)
            .map_err(|e| BitcoindRpcError::Prioritize(e.to_string()))?;
        Ok(())
    }

    async fn send_request(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(StatusCode, String), BitcoindRpcError> {
        let (user, pwd) = match (&self.user, &self.pwd) {
            (Some(u), Some(p)) => (u.as_str(), p.as_str()),
            _ => {
                let msg = match (&self.user, &self.pwd) {
                    (None, None) => "RPC_USER and RPC_PWD are not set.".to_string(),
                    (None, _) => "RPC_USER is not set.".to_string(),
                    _ => "RPC_PWD is not set.".to_string(),
                };
                return Err(BitcoindRpcError::MissingConfig(msg));
            }
        };
        let url = self.url.as_deref().unwrap_or_else(|| {
            warn!("RPC_URL not configured, using default: {DEFAULT_RPC_URL}");
            DEFAULT_RPC_URL
        });

        let body = json!({
            "jsonrpc": "1.0",
            "id": "dmnd-client",
            "method": method,
            "params": params
        });

        let response = self
            .client
            .post(url)
            .basic_auth(user, Some(pwd))
            .json(&body)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;
        Ok((status, text))
    }
}

#[derive(Deserialize, Debug)]
struct RpcResponse {
    result: Option<Value>,
    error: Option<Value>,
}

impl RpcResponse {
    fn from_response(status: StatusCode, text: &str) -> Result<Option<Value>, BitcoindRpcError> {
        let resp: Self = match serde_json::from_str(text) {
            Ok(r) => r,
            Err(e) if status.is_success() => {
                return Err(BitcoindRpcError::InvalidResponse(format!(
                    "failed to decode bitcoind response ({e}): {text}"
                )));
            }
            Err(_) => {
                return Err(BitcoindRpcError::Other(format!(
                    "bitcoind HTTP {status}: {text}"
                )));
            }
        };
        if !status.is_success() {
            return Err(BitcoindRpcError::Other(format!(
                "bitcoind HTTP {status}: {text}"
            )));
        }
        if let Some(err) = resp.error {
            return Err(BitcoindRpcError::Rejected(format!(
                "bitcoind RPC error: {err}"
            )));
        }
        Ok(resp.result)
    }
}

fn validate_transaction_hex(tx: &str) -> Result<&str, BitcoindRpcError> {
    let tx = tx.trim();
    if tx.is_empty() {
        return Err(BitcoindRpcError::InvalidTransaction(
            "transaction hex cannot be empty".to_string(),
        ));
    }
    // Hex encoding represents each byte with two characters,
    // so valid transaction hex must have an even length.
    if tx.len() % 2 == 1 {
        return Err(BitcoindRpcError::InvalidTransaction(
            "Invalid transaction hash".to_string(),
        ));
    }
    if !tx.as_bytes().iter().all(|b| b.is_ascii_hexdigit()) {
        return Err(BitcoindRpcError::InvalidTransaction(
            "transaction must be hex encoded".to_string(),
        ));
    }
    Ok(tx)
}

#[derive(Debug)]
pub(crate) enum BitcoindRpcError {
    InvalidTransaction(String),
    MissingConfig(String),
    Rejected(String),
    Timeout(String),
    Other(String),
    InvalidResponse(String),
    Prioritize(String),
}

impl BitcoindRpcError {
    pub(crate) fn status_code(&self) -> StatusCode {
        match self {
            BitcoindRpcError::InvalidTransaction(_) | BitcoindRpcError::Rejected(_) => {
                StatusCode::BAD_REQUEST
            }
            BitcoindRpcError::MissingConfig(_) => StatusCode::SERVICE_UNAVAILABLE,
            BitcoindRpcError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            BitcoindRpcError::Other(_)
            | BitcoindRpcError::InvalidResponse(_)
            | BitcoindRpcError::Prioritize(_) => StatusCode::BAD_GATEWAY,
        }
    }
}

impl From<reqwest::Error> for BitcoindRpcError {
    fn from(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            BitcoindRpcError::Timeout(format!("timed out while connecting to bitcoind: {error}"))
        } else {
            BitcoindRpcError::Other(format!("failed to connect to bitcoind: {error}"))
        }
    }
}

impl fmt::Display for BitcoindRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BitcoindRpcError::InvalidTransaction(msg)
            | BitcoindRpcError::MissingConfig(msg)
            | BitcoindRpcError::Rejected(msg)
            | BitcoindRpcError::Timeout(msg)
            | BitcoindRpcError::Other(msg)
            | BitcoindRpcError::InvalidResponse(msg)
            | BitcoindRpcError::Prioritize(msg) => f.write_str(msg),
        }
    }
}
