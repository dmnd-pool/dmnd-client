use super::AppState;
use axum::Extension;
use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    Json,
};
use bitcoin::consensus::encode;
use bitcoin::{Transaction, Txid};
use bitcoincore_rpc::json::GetMempoolEntryResultFees;
use bitcoincore_rpc::{Client, RpcApi};
use futures::StreamExt;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

#[derive(Debug, Serialize, Clone)]
pub struct Tx {
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

impl From<(Txid, bitcoincore_rpc::json::GetMempoolEntryResult)> for Tx {
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

type TxSender = broadcast::Sender<Tx>;

pub async fn get_mempool_txs(State(state): State<AppState>) -> Json<Vec<Tx>> {
    match state.rpc.get_raw_mempool_verbose() {
        Ok(entries) => {
            let txs = entries.into_iter().map(Tx::from).collect();
            Json(txs)
        }
        Err(err) => {
            println!("rpc.get_raw_mempool_verbose failed: {err}");
            Json(Vec::new())
        }
    }
}

pub fn spawn_zmq_task(rpc: Arc<Client>, tx_sender: TxSender, zmq_address: &'static str) {
    let _handle = std::thread::spawn(move || {
        let context = zmq::Context::new();
        let subscriber = context
            .socket(zmq::SUB)
            .expect("Failed to create ZMQ socket");
        subscriber
            .connect(zmq_address)
            .expect("Failed to connect to ZMQ address");
        subscriber
            .set_subscribe(b"rawtx")
            .expect("Failed to subscribe");

        let mut backlog = Vec::new();

        loop {
            backlog.retain(|tx: &Tx| match tx_sender.send(tx.clone()) {
                Ok(_) => false,
                Err(_) => true,
            });

            let topic = subscriber.recv_msg(0).expect("Failed to receive topic");
            if topic.as_str().unwrap_or("") != "rawtx" {
                continue;
            }

            let raw_tx = subscriber
                .recv_bytes(0)
                .expect("Failed to receive rawtx data");

            if let Ok(tx) = encode::deserialize::<Transaction>(&raw_tx) {
                let txid = tx.compute_txid();
                match rpc.get_mempool_entry(&txid) {
                    Ok(entry) => {
                        let tx_meta = Tx::from((txid, entry));
                        if tx_sender.send(tx_meta.clone()).is_err() {
                            backlog.push(tx_meta);
                        }
                    }
                    Err(err) => println!("mempool entry not found for {txid}: {err}"),
                }
            }
        }
    });
}

pub async fn ws_upgrade(
    (ws, Extension(tx_sender)): (WebSocketUpgrade, Extension<TxSender>),
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, tx_sender.subscribe()))
}

async fn handle_ws(mut socket: WebSocket, rx: broadcast::Receiver<Tx>) {
    let mut stream = BroadcastStream::new(rx);
    loop {
        tokio::select! {
            biased;
            Some(Ok(tx)) = stream.next() => {
                if let Ok(json) = serde_json::to_string(&tx) {
                    if socket.send(axum::extract::ws::Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
            }
            _ = socket.recv() => break,
        }
    }
    println!("WebSocket connection closed");
}
