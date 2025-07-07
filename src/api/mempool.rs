use crate::api::routes::APIResponse;
use crate::dashboard::jd_event_ws::JobDeclarationData;
use crate::db::handlers::JobDeclarationHandler;
use crate::db::model::JobDeclarationInsert;

use super::AppState;
use axum::http::StatusCode;
use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    Json,
};
use binary_sv2::{Seq064K, B016M};
use bitcoin::consensus::encode;
use bitcoin::hashes::Hash;
use bitcoin::Txid;
use bitcoincore_rpc::json::GetMempoolEntryResultFees;
use bitcoincore_rpc::{Client, RpcApi};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, info, warn};

#[derive(Debug, Serialize, Clone)]
pub struct MempoolTransaction {
    pub txid: String,
    pub vsize: u64,
    pub weight: Option<u64>,
    pub time: u64,
    pub height: u64,
    pub descendant_count: u64,
    pub descendant_size: u64,
    pub ancestor_count: u64,
    pub ancestor_size: u64,
    pub wtxid: Txid,
    pub fees: GetMempoolEntryResultFees,
    pub depends: Vec<Txid>,
    pub spent_by: Vec<Txid>,
    pub bip125_replaceable: bool,
    pub unbroadcast: Option<bool>,
}

impl From<(Txid, bitcoincore_rpc::json::GetMempoolEntryResult)> for MempoolTransaction {
    fn from((txid, entry): (Txid, bitcoincore_rpc::json::GetMempoolEntryResult)) -> Self {
        Self {
            txid: txid.to_string(),
            vsize: entry.vsize,
            weight: entry.weight,
            time: entry.time,
            height: entry.height,
            descendant_count: entry.descendant_count,
            descendant_size: entry.descendant_size,
            ancestor_count: entry.ancestor_count,
            ancestor_size: entry.ancestor_size,
            wtxid: entry.wtxid,
            fees: entry.fees,
            depends: entry.depends,
            spent_by: entry.spent_by,
            bip125_replaceable: entry.bip125_replaceable,
            unbroadcast: entry.unbroadcast,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct BlockTransactionList {
    pub height: usize,
    pub block_hash: String,
    pub txids: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "event")]
pub enum SequenceEvent {
    #[serde(rename = "A")]
    MempoolAdd {
        sequence: u64,
        transaction: MempoolTransaction,
    },
    #[serde(rename = "R")]
    MempoolRemove { sequence: u64, txid: String },
    #[serde(rename = "C")]
    BlockConnect { block: BlockTransactionList },
    #[serde(rename = "D")]
    BlockDisconnect {
        block_hash: String,
        transactions: Vec<MempoolTransaction>,
    },
}

pub type MempoolEventBroadcaster = broadcast::Sender<SequenceEvent>;

/// Streams broadcast events to a WebSocket client as JSON messages.
///
/// This function listens for events from a broadcast channel and sends them to the connected WebSocket client.
/// It serializes each event to JSON and sends it as a text message. If sending fails, the loop breaks and the connection is closed.
///
/// # Arguments
/// * `socket` - The WebSocket connection to send messages to.
/// * `subscriber` - The broadcast receiver for events to stream.
/// * `label` - A label for logging purposes.
async fn stream_to_ws<T: Serialize + Clone + Send + 'static>(
    mut socket: WebSocket,
    subscriber: broadcast::Receiver<T>,
    label: &str,
) {
    let mut stream = BroadcastStream::new(subscriber);
    while let Some(Ok(item)) = stream.next().await {
        if let Ok(json) = serde_json::to_string(&item) {
            if socket
                .send(axum::extract::ws::Message::Text(json.into()))
                .await
                .is_err()
            {
                break;
            }
        }
    }
    warn!("WebSocket closed: {}", label);
}

/// WebSocket handler for mempool events.
///
/// Upgrades the HTTP connection to a WebSocket and streams mempool events to the client.
///
/// # Arguments
/// * `ws` - The WebSocket upgrade request.
/// * `State(state)` - The application state containing the event broadcaster.
pub async fn ws_mempool_events_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        stream_to_ws(
            socket,
            state.mempool_event_broadcaster.subscribe(),
            "sequence_txs",
        )
    })
}

/// Fetches the current mempool transactions from the Bitcoin node.
///
/// Returns a JSON array of `MempoolTransaction` objects. If the Bitcoin RPC is unavailable or an error occurs,
/// returns an empty array and logs the error.
///
/// # Arguments
/// * `State(state)` - The application state containing the Bitcoin RPC client.
pub async fn fetch_mempool(State(state): State<AppState>) -> Json<Vec<MempoolTransaction>> {
    match &state.rpc {
        Some(rpc) => match rpc.get_raw_mempool_verbose() {
            Ok(entries) => Json(entries.into_iter().map(MempoolTransaction::from).collect()),
            Err(e) => {
                error!("Failed to fetch mempool: {e}");
                Json(Vec::new())
            }
        },
        None => {
            warn!("Bitcoin RPC not available, returning empty mempool");
            Json(Vec::new())
        }
    }
}

/// Spawns a background thread to listen for ZMQ sequence events from the Bitcoin node and broadcast them.
///
/// This function connects to the ZMQ socket, subscribes to sequence events, parses them, and sends them to the event broadcaster.
/// Handles mempool additions/removals and block connect/disconnect events. Maintains a backlog if broadcasting fails.
///
/// # Arguments
/// * `rpc` - Shared Bitcoin RPC client.
/// * `event_broadcaster` - Broadcast channel for sequence events.
/// * `zmq_url` - The ZMQ endpoint to connect to.
pub fn spawn_zmq_events(
    rpc: Arc<Client>,
    event_broadcaster: MempoolEventBroadcaster,
    zmq_url: &'static str,
) {
    std::thread::spawn(move || {
        let context = zmq::Context::new();
        let mut backlog: VecDeque<SequenceEvent> = VecDeque::new();

        loop {
            let subscriber = context
                .socket(zmq::SUB)
                .expect("Failed to create ZMQ socket");
            subscriber
                .set_reconnect_ivl(0)
                .expect("Failed to set reconnect interval");
            subscriber
                .connect(zmq_url)
                .expect("Failed to connect to ZMQ address");
            subscriber
                .set_subscribe("sequence".as_bytes())
                .expect("Failed to subscribe");

            loop {
                // Process backlog first
                while let Some(seq_event) = backlog.pop_front() {
                    if event_broadcaster.send(seq_event.clone()).is_err() {
                        backlog.push_front(seq_event);
                        break;
                    }
                }

                let topic_msg = subscriber.recv_msg(0).expect("Failed to receive topic");
                if topic_msg.as_str().unwrap_or("") == "sequence" {
                    let raw = subscriber.recv_bytes(0).expect("Failed to receive data");

                    // Parse sequence event
                    if raw.len() < 33 {
                        continue;
                    }

                    let mut hash_bytes = raw[0..32].to_vec();
                    hash_bytes.reverse();

                    let event = raw[32] as char;
                    let sequence = if raw.len() >= 41 {
                        let seq_bytes: [u8; 8] = raw[33..41].try_into().unwrap_or([0; 8]);
                        u64::from_le_bytes(seq_bytes)
                    } else {
                        0
                    };

                    let seq_event = match event {
                        'A' => {
                            // Transaction added to mempool
                            if let Ok(txid) = bitcoin::Txid::from_slice(&hash_bytes) {
                                if let Ok(entry) = rpc.get_mempool_entry(&txid) {
                                    let mempool_tx = MempoolTransaction::from((txid, entry));
                                    Some(SequenceEvent::MempoolAdd {
                                        sequence,
                                        transaction: mempool_tx,
                                    })
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        'R' => {
                            // Transaction removed from mempool
                            if let Ok(txid) = bitcoin::Txid::from_slice(&hash_bytes) {
                                Some(SequenceEvent::MempoolRemove {
                                    sequence,
                                    txid: txid.to_string(),
                                })
                            } else {
                                None
                            }
                        }
                        'C' => {
                            // Block connected
                            if let Ok(block_hash) = bitcoin::BlockHash::from_slice(&hash_bytes) {
                                match rpc.get_block_info(&block_hash) {
                                    Ok(block_info) => {
                                        let block_tx_list = BlockTransactionList {
                                            block_hash: block_hash.to_string(),
                                            height: block_info.height as usize,
                                            txids: block_info
                                                .tx
                                                .iter()
                                                .map(|t| t.to_string())
                                                .collect(),
                                        };
                                        Some(SequenceEvent::BlockConnect {
                                            block: block_tx_list,
                                        })
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to fetch block info for {}: {}",
                                            block_hash, e
                                        );
                                        None
                                    }
                                }
                            } else {
                                None
                            }
                        }
                        'D' => {
                            // Block disconnected
                            if let Ok(block_hash) = bitcoin::BlockHash::from_slice(&hash_bytes) {
                                if let Ok(block) = rpc.get_block(&block_hash) {
                                    let transactions: Vec<MempoolTransaction> = block
                                        .txdata
                                        .into_iter()
                                        .filter_map(|tx| {
                                            let txid = tx.compute_txid();
                                            rpc.get_mempool_entry(&txid).ok().map(|entry| {
                                                MempoolTransaction::from((txid, entry))
                                            })
                                        })
                                        .collect();

                                    Some(SequenceEvent::BlockDisconnect {
                                        block_hash: block_hash.to_string(),
                                        transactions,
                                    })
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        _ => {
                            warn!("Unknown sequence event: {}", event);
                            None
                        }
                    };

                    if let Some(event) = seq_event {
                        if event_broadcaster.send(event.clone()).is_err() {
                            backlog.push_back(event);
                        }
                    }
                }
            }
        }
    });
}

#[derive(Deserialize)]
pub struct ApiRequest {
    pub template_id: u64,
    pub txids: Vec<String>,
}

/// Handles submission of a list of transaction IDs for a given template.
///
/// Validates and serializes each transaction, sends them to the job declaration process, and waits for a response.
/// Returns a JSON response with the result, including any invalid transactions and the job declaration data.
///
/// # Arguments
/// * `State(state)` - The application state containing the Bitcoin RPC client and job declaration sender.
/// * `Json(api_request)` - The request payload containing the template ID and transaction IDs.
pub async fn submit_tx_list(
    State(state): State<AppState>,
    Json(api_request): Json<ApiRequest>,
) -> impl IntoResponse {
    let template_id = api_request.template_id;
    let txids = api_request.txids;
    let mut txs: Vec<B016M<'static>> = Vec::new();
    let mut invalid_tx: Vec<(String, String)> = Vec::new();
    let mut valid_txids: Vec<Txid> = Vec::new(); // Store valid txids for database
    let mut tx_count = 0;
    for txid_str in txids {
        let txid = match txid_str.parse::<Txid>() {
            Ok(t) => t,
            Err(_) => {
                invalid_tx.push((txid_str.clone(), "Invalid txid".to_string()));
                continue;
            }
        };
        tx_count += 1;
        match &state.rpc {
            Some(rpc) => match rpc.get_raw_transaction(&txid, None) {
                Ok(tx) => {
                    let bytes = encode::serialize(&tx);
                    let serialized: B016M<'static> = match bytes.try_into() {
                        Ok(s) => s,
                        Err(_) => {
                            invalid_tx
                                .push((txid.to_string(), "Serialization invalid_tx".to_string()));
                            continue;
                        }
                    };
                    txs.push(serialized);
                    valid_txids.push(txid); // Store valid txid
                }
                Err(e) => {
                    invalid_tx.push((txid.to_string(), format!("Failed to fetch tx: {}", e)));
                    continue;
                }
            },
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(APIResponse::error(Some(
                        "Bitcoin RPC not available".to_string(),
                    ))),
                );
            }
        }
    }

    if tx_count == 0 {
        // All transactions are invalid
        return (
            StatusCode::BAD_REQUEST,
            Json(APIResponse::error(Some(format!(
                "All transactions are invalid: {:?}",
                invalid_tx
            )))),
        );
    }

    let seq: Seq064K<'static, B016M<'static>> = txs.into();

    // Create a oneshot channel to wait for the job declaration response
    let (response_sender, response_receiver) = oneshot::channel();

    if let Err(e) = state
        .tx_list_sender
        .send((seq, Some(response_sender)))
        .await
    {
        error!("Failed to send tx list: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(APIResponse::error(Some(
                "Failed to send tx list".to_string(),
            ))),
        );
    }

    info!(
        "Successfully sent tx list for template_id: {}, waiting for job declaration response...",
        template_id
    );

    // Wait for the job declaration response with a timeout
    match tokio::time::timeout(std::time::Duration::from_secs(30), response_receiver).await {
        Ok(Ok(job_data)) => {
            info!("Received job declaration response: {:?}", job_data);
            store_jd_in_db(&state, &job_data, &valid_txids).await;

            (
                StatusCode::OK,
                Json(APIResponse::success(Some(serde_json::json!({
                    "template_id": template_id,
                    "submitted_tx_count": tx_count,
                    "invalid_txids": invalid_tx,
                    "job_declaration": job_data
                })))),
            )
        }
        Ok(Err(_)) => {
            error!("Job declaration response channel was closed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(APIResponse::error(Some(
                    "Job declaration failed - response channel closed".to_string(),
                ))),
            )
        }
        Err(_) => {
            error!("Timeout waiting for job declaration response");
            (
                StatusCode::REQUEST_TIMEOUT,
                Json(APIResponse::error(Some(
                    "Timeout waiting for job declaration response".to_string(),
                ))),
            )
        }
    }
}

async fn store_jd_in_db(state: &AppState, job_data: &JobDeclarationData, valid_txids: &[Txid]) {
    // Store job declaration data in database if available
    if let Some(db) = &state.db {
        let handler = JobDeclarationHandler::new(db.clone());

        // Extract data from job_data
        if let (
            Some(template_id),
            Some(channel_id),
            Some(request_id),
            Some(job_id),
            Some(mining_job_token),
        ) = (
            job_data.template_id,
            job_data.channel_id,
            job_data.req_id,
            job_data.job_id,
            job_data.mining_job_token.as_ref(),
        ) {
            let txid_strings: Vec<String> = valid_txids.iter().map(|t| t.to_string()).collect();
            let job_declaration = JobDeclarationInsert {
                template_id: template_id as i64,
                channel_id: channel_id as i64,
                request_id: request_id as i64,
                job_id: job_id as i64,
                mining_job_token: mining_job_token.clone(),
                txids: txid_strings,
            };

            if handler
                .insert_job_declaration(&job_declaration)
                .await
                .is_err()
            {
                error!("Failed to store job declaration");
            }
        }
    }
}
