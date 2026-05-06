use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{error::Error as StdError, fmt, time::Duration};
use tracing::{debug, info};

const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) struct BitcoindRpc {
    url: String,
    user: String,
    pwd: String,
    fee_delta: i64,
}

impl BitcoindRpc {
    pub(crate) fn new(url: String, user: String, pwd: String, fee_delta: i64) -> Self {
        Self {
            url,
            user,
            pwd,
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
        crate::prioritized_transactions::record(&txid);
        Ok(txid)
    }

    async fn prioritise_transaction(&self, txid: &str) -> Result<(), BitcoindRpcError> {
        let (status, text) = self
            .send_request("prioritisetransaction", json!([txid, 0, self.fee_delta]))
            .await?;
        info!(
            txid,
            fee_delta = self.fee_delta,
            %status,
            response = %text,
            "bitcoind prioritisetransaction response"
        );

        let result = RpcResponse::from_response(status, &text)
            .map_err(|e| BitcoindRpcError::Prioritize(e.to_string()))?;

        match result.and_then(|value| value.as_bool()) {
            Some(true) => Ok(()),
            Some(false) => Err(BitcoindRpcError::Prioritize(format!(
                "bitcoind returned false for prioritisetransaction: {text}"
            ))),
            None => Err(BitcoindRpcError::Prioritize(format!(
                "invalid prioritisetransaction response from bitcoind: {text}"
            ))),
        }
    }

    async fn send_request(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(StatusCode, String), BitcoindRpcError> {
        let body = json!({
            "jsonrpc": "1.0",
            "id": "dmnd-client",
            "method": method,
            "params": params
        });

        debug!(
            method,
            url = self.url.as_str(),
            rpc_user = self.user.as_str(),
            fee_delta = self.fee_delta,
            "sending bitcoind RPC request"
        );

        let client = reqwest::Client::builder()
            .connect_timeout(RPC_CONNECT_TIMEOUT)
            .timeout(RPC_REQUEST_TIMEOUT)
            .build()
            .expect("Failed to build client");

        let response = client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.pwd))
            .json(&body)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;
        debug!(method, %status, response = %text, "received bitcoind RPC response");
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

#[derive(Clone, Debug)]
pub(crate) enum BitcoindRpcError {
    InvalidTransaction(String),
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
            BitcoindRpcError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            BitcoindRpcError::Other(_)
            | BitcoindRpcError::InvalidResponse(_)
            | BitcoindRpcError::Prioritize(_) => StatusCode::BAD_GATEWAY,
        }
    }
}

impl From<reqwest::Error> for BitcoindRpcError {
    fn from(error: reqwest::Error) -> Self {
        let source = error
            .source()
            .map(|source| format!("; source: {source}"))
            .unwrap_or_default();
        if error.is_timeout() {
            BitcoindRpcError::Timeout(format!(
                "timed out while connecting to bitcoind: {error}{source}"
            ))
        } else if error.is_builder() {
            BitcoindRpcError::Other(format!(
                "failed to build bitcoind RPC request: {error}{source}"
            ))
        } else {
            BitcoindRpcError::Other(format!("failed to connect to bitcoind: {error}{source}"))
        }
    }
}

impl fmt::Display for BitcoindRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BitcoindRpcError::InvalidTransaction(msg)
            | BitcoindRpcError::Rejected(msg)
            | BitcoindRpcError::Timeout(msg)
            | BitcoindRpcError::Other(msg)
            | BitcoindRpcError::InvalidResponse(msg)
            | BitcoindRpcError::Prioritize(msg) => f.write_str(msg),
        }
    }
}
