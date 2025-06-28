use crate::api::routes::APIResponse;

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
use serde::Serialize;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, warn};

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

pub type EventBroadcaster = broadcast::Sender<SequenceEvent>;

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

pub async fn ws_events_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        stream_to_ws(socket, state.event_broadcaster.subscribe(), "sequence_txs")
    })
}

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

// ZMQ sequence stream handles all events
pub fn spawn_zmq_events(
    rpc: Arc<Client>,
    event_broadcaster: EventBroadcaster,
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
                                if let Ok(block) = rpc.get_block(&block_hash) {
                                    let block_tx_list = BlockTransactionList {
                                        block_hash: block_hash.to_string(),
                                        txids: block
                                            .txdata
                                            .into_iter()
                                            .map(|t| t.compute_txid().to_string())
                                            .collect(),
                                    };
                                    Some(SequenceEvent::BlockConnect {
                                        block: block_tx_list,
                                    })
                                } else {
                                    None
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

pub async fn submit_tx_list(
    State(state): State<AppState>,
    Json(txids): Json<Vec<String>>,
) -> impl IntoResponse {
    let mut txs: Vec<B016M<'static>> = Vec::new();
    for txid_str in txids {
        let txid = match txid_str.parse::<Txid>() {
            Ok(t) => t,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(APIResponse::error(Some(format!(
                        "Invalid txid: {}",
                        txid_str
                    )))),
                );
            }
        };
        match &state.rpc {
            Some(rpc) => match rpc.get_raw_transaction(&txid, None) {
                Ok(tx) => {
                    let bytes = encode::serialize(&tx);
                    let serialized: B016M<'static> = bytes.try_into().unwrap();
                    txs.push(serialized);
                }
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(APIResponse::error(Some(format!(
                            "Failed to fetch tx {}: {}",
                            txid, e
                        )))),
                    );
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
    let seq: Seq064K<'static, B016M<'static>> = txs.into();

    if state.tx_list_sender.send(seq).await.is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(APIResponse::error(Some(
                "Failed to send tx list".to_string(),
            ))),
        );
    }
    (StatusCode::OK, Json(APIResponse::success(None::<()>)))
}
