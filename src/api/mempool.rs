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
use bitcoin::consensus::{deserialize, encode};
use bitcoin::{Block, Transaction, Txid};
use bitcoincore_rpc::json::GetMempoolEntryResultFees;
use bitcoincore_rpc::{Client, RpcApi};
use futures::StreamExt;
use serde::Serialize;
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, info};

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

pub type TxBroadcaster = broadcast::Sender<MempoolTransaction>;
pub type BlockBroadcaster = broadcast::Sender<BlockTransactionList>;

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
    info!("WebSocket closed: {}", label);
}

pub async fn ws_new_transactions(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| stream_to_ws(socket, state.tx_broadcaster.subscribe(), "new_txs"))
}

pub async fn ws_block_transactions(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        stream_to_ws(socket, state.block_broadcaster.subscribe(), "block_txs")
    })
}

pub async fn fetch_mempool(State(state): State<AppState>) -> Json<Vec<MempoolTransaction>> {
    match state.rpc.get_raw_mempool_verbose() {
        Ok(entries) => Json(entries.into_iter().map(MempoolTransaction::from).collect()),
        Err(e) => {
            error!("Failed to fetch mempool: {e}");
            Json(Vec::new())
        }
    }
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
        match state.rpc.get_raw_transaction(&txid, None) {
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

/// Generic ZMQ stream spawner for broadcasting messages of type T
fn start_zmq_stream<T, F>(
    broadcaster: broadcast::Sender<T>,
    zmq_url: &'static str,
    subscribe_topic: &'static str,
    process_msg: F,
) where
    T: Clone + Send + 'static,
    F: Fn(&[u8]) -> Option<T> + Send + 'static,
{
    std::thread::spawn(move || {
        let context = zmq::Context::new();
        let subscriber = context
            .socket(zmq::SUB)
            .expect("Failed to create ZMQ socket");
        subscriber
            .connect(zmq_url)
            .expect("Failed to connect to ZMQ address");
        subscriber
            .set_subscribe(subscribe_topic.as_bytes())
            .expect("Failed to subscribe");

        let mut backlog: Vec<T> = Vec::new();

        loop {
            backlog.retain(|item| match broadcaster.send(item.clone()) {
                Ok(_) => false,
                Err(_) => true,
            });

            let topic_msg = subscriber.recv_msg(0).expect("Failed to receive topic");
            if topic_msg.as_str().unwrap_or("") == subscribe_topic {
                let raw = subscriber.recv_bytes(0).expect("Failed to receive data");
                if let Some(item) = process_msg(&raw) {
                    if broadcaster.send(item.clone()).is_err() {
                        backlog.push(item);
                    }
                }
            }
        }
    });
}

pub fn start_zmq_tx_stream(rpc: Arc<Client>, broadcaster: TxBroadcaster, zmq_url: &'static str) {
    start_zmq_stream(broadcaster, zmq_url, "rawtx", move |raw| {
        encode::deserialize::<Transaction>(raw).ok().and_then(|tx| {
            let txid = tx.compute_txid();
            rpc.get_mempool_entry(&txid)
                .ok()
                .map(|entry| MempoolTransaction::from((txid, entry)))
        })
    });
}

pub fn start_zmq_block_stream(broadcaster: BlockBroadcaster, zmq_url: &'static str) {
    start_zmq_stream(broadcaster, zmq_url, "rawblock", move |raw| {
        deserialize::<Block>(raw)
            .ok()
            .map(|block| BlockTransactionList {
                block_hash: block.block_hash().to_string(),
                txids: block
                    .txdata
                    .into_iter()
                    .map(|t| t.compute_txid().to_string())
                    .collect(),
            })
    });
}
