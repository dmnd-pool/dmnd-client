use axum::http::StatusCode;
use bitcoin::{blockdata::transaction::Transaction, consensus::encode::deserialize_hex};
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
        let txid = txid_from_sendrawtransaction_response(tx, status, &text)?;
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

    pub(crate) async fn transaction_in_mempool(
        &self,
        txid: &str,
    ) -> Result<bool, BitcoindRpcError> {
        let txid = validate_transaction_hex(txid)?;
        let (status, text) = self.send_request("getmempoolentry", json!([txid])).await?;
        let resp = RpcResponse::decode(status, &text)?;

        if let Some(error) = resp.error {
            if is_not_in_mempool_error(&error) {
                return Ok(false);
            }

            return Err(BitcoindRpcError::Other(format!(
                "bitcoind RPC error while checking mempool entry: {error}"
            )));
        }

        if !status.is_success() {
            return Err(BitcoindRpcError::Other(format!(
                "bitcoind HTTP {status}: {text}"
            )));
        }

        match resp.result {
            Some(Value::Object(_)) => Ok(true),
            _ => Err(BitcoindRpcError::InvalidResponse(format!(
                "invalid getmempoolentry response from bitcoind: {text}"
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

fn txid_from_sendrawtransaction_response(
    tx: &str,
    status: StatusCode,
    text: &str,
) -> Result<String, BitcoindRpcError> {
    let resp = RpcResponse::decode(status, text)?;

    if let Some(error) = resp.error.as_ref() {
        if is_already_in_mempool_error(error) {
            let txid = txid_from_transaction_hex(tx)?;
            info!(
                txid,
                response = %text,
                "transaction already in bitcoind mempool; prioritizing existing transaction"
            );
            return Ok(txid);
        }

        return Err(BitcoindRpcError::Rejected(format!(
            "bitcoind RPC error: {error}"
        )));
    }

    if !status.is_success() {
        return Err(BitcoindRpcError::Other(format!(
            "bitcoind HTTP {status}: {text}"
        )));
    }

    resp.result
        .and_then(|v| v.as_str().map(str::to_owned))
        .ok_or_else(|| {
            BitcoindRpcError::InvalidResponse("empty response from bitcoind".to_string())
        })
}

fn txid_from_transaction_hex(tx: &str) -> Result<String, BitcoindRpcError> {
    let transaction: Transaction = deserialize_hex(tx).map_err(|e| {
        BitcoindRpcError::InvalidTransaction(format!("failed to decode transaction hex: {e}"))
    })?;
    Ok(transaction.compute_txid().to_string())
}

#[derive(Deserialize, Debug)]
struct RpcResponse {
    result: Option<Value>,
    error: Option<Value>,
}

impl RpcResponse {
    fn from_response(status: StatusCode, text: &str) -> Result<Option<Value>, BitcoindRpcError> {
        let resp = Self::decode(status, text)?;
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

    fn decode(status: StatusCode, text: &str) -> Result<Self, BitcoindRpcError> {
        match serde_json::from_str(text) {
            Ok(r) => Ok(r),
            Err(e) if status.is_success() => Err(BitcoindRpcError::InvalidResponse(format!(
                "failed to decode bitcoind response ({e}): {text}"
            ))),
            Err(_) => Err(BitcoindRpcError::Other(format!(
                "bitcoind HTTP {status}: {text}"
            ))),
        }
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

fn is_already_in_mempool_error(error: &Value) -> bool {
    error
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|message| {
            let message = message.to_ascii_lowercase();
            message.contains("already in mempool") || message.contains("txn-already-in-mempool")
        })
}

fn is_not_in_mempool_error(error: &Value) -> bool {
    let code_is_not_found = error.get("code").and_then(Value::as_i64) == Some(-5);
    let message_says_not_in_mempool = error
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|message| message.to_ascii_lowercase().contains("not in mempool"));

    code_is_not_found || message_says_not_in_mempool
}

#[cfg(test)]
mod tests {
    use super::{
        is_already_in_mempool_error, is_not_in_mempool_error, txid_from_transaction_hex,
        BitcoindRpc,
    };
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use serde_json::json;
    use serde_json::Value;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

    const RAW_TX: &str = concat!(
        "01000000",
        "01",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "ffffffff",
        "00",
        "ffffffff",
        "01",
        "0000000000000000",
        "00",
        "00000000",
    );

    #[test]
    fn detects_bitcoind_not_in_mempool_error() {
        let error = json!({
            "code": -5,
            "message": "Transaction not in mempool"
        });

        assert!(is_not_in_mempool_error(&error));
    }

    #[test]
    fn ignores_unrelated_bitcoind_errors() {
        let error = json!({
            "code": -26,
            "message": "mandatory-script-verify-flag-failed"
        });

        assert!(!is_not_in_mempool_error(&error));
    }

    #[test]
    fn detects_bitcoind_already_in_mempool_error() {
        let error = json!({
            "code": -27,
            "message": "txn-already-in-mempool"
        });

        assert!(is_already_in_mempool_error(&error));
    }

    #[test]
    fn ignores_already_in_chain_error_for_mempool_detection() {
        let error = json!({
            "code": -27,
            "message": "Transaction already in block chain"
        });

        assert!(!is_already_in_mempool_error(&error));
    }

    #[tokio::test]
    async fn submit_transaction_prioritizes_when_tx_is_already_in_mempool() {
        #[derive(Clone)]
        struct MockBitcoindState {
            requests: UnboundedSender<Value>,
        }

        async fn mock_bitcoind(
            State(state): State<MockBitcoindState>,
            Json(body): Json<Value>,
        ) -> (StatusCode, Json<Value>) {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .map(str::to_owned);
            state
                .requests
                .send(body)
                .expect("test should receive bitcoind request");

            match method.as_deref() {
                Some("sendrawtransaction") => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "result": null,
                        "error": {
                            "code": -27,
                            "message": "txn-already-in-mempool"
                        },
                        "id": "dmnd-client"
                    })),
                ),
                Some("prioritisetransaction") => (
                    StatusCode::OK,
                    Json(json!({
                        "result": true,
                        "error": null,
                        "id": "dmnd-client"
                    })),
                ),
                _ => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "result": null,
                        "error": {
                            "code": -32601,
                            "message": "unknown method"
                        },
                        "id": "dmnd-client"
                    })),
                ),
            }
        }

        let expected_txid = txid_from_transaction_hex(RAW_TX).expect("valid test transaction");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let addr = listener.local_addr().expect("test server local addr");
        let (requests, mut received_requests) = unbounded_channel();
        let app = Router::new()
            .route("/", post(mock_bitcoind))
            .with_state(MockBitcoindState { requests });
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });

        let rpc = BitcoindRpc::new(
            format!("http://{addr}"),
            "user".to_string(),
            "password".to_string(),
            100_000_000,
        );

        let txid = rpc
            .submit_transaction(RAW_TX)
            .await
            .expect("already-in-mempool tx should still be prioritized");

        assert_eq!(txid, expected_txid);

        let submit_request = received_requests
            .recv()
            .await
            .expect("sendrawtransaction request");
        assert_eq!(submit_request["method"], "sendrawtransaction");

        let prioritize_request = received_requests
            .recv()
            .await
            .expect("prioritisetransaction request");
        assert_eq!(prioritize_request["method"], "prioritisetransaction");
        assert_eq!(
            prioritize_request["params"],
            json!([expected_txid, 0, 100_000_000])
        );

        server.abort();
    }
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
