pub(crate) mod bitcoin_rpc;
mod routes;
pub mod stats;
mod utils;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::HashMap,
    error::Error as StdError,
    fmt,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, Instant},
};

use crate::{
    api::bitcoin_rpc::{BitcoindRpc, BitcoindRpcError, PrioTx},
    dashboard::{assets::static_handler, open_dashboard},
    db::prioritized,
    router::Router,
    Configuration,
};
use axum::{
    routing::{delete, get, post},
    Router as AxumRouter,
};
use bitcoin::{
    consensus::encode::{deserialize_hex, FromHexError},
    Transaction, Txid,
};
use routes::Api;
use stats::StatsSender;
use tracing::{error, info, warn};

const MEMPOOL_SPACE_API_BASE_URL: &str = "https://mempool.space/api";
const PRIORITIZED_TRANSACTIONS_POLL_INTERVAL: Duration = Duration::from_secs(60);
const BAD_TRANSACTIONS_CLEAR_INTERVAL: Duration = Duration::from_secs(3 * 60 * 60);
pub(crate) static START_TX_PRIO: AtomicBool = AtomicBool::new(true);
static PRIORITIZED_TRANSACTIONS_POLL_LOCK: LazyLock<Arc<tokio::sync::Mutex<()>>> =
    LazyLock::new(|| Arc::new(tokio::sync::Mutex::new(())));

const NODE_PRIORITIZED_VIEW_MAX_AGE: Duration = Duration::from_secs(30);

/// The last reading of the node's prioritized transactions, and when it was taken. This is used to avoid repeated calls to `getprioritisetransaction` for every API request.
static NODE_PRIORITIZED_VIEW: Mutex<Option<(HashMap<Txid, PrioTx>, Instant)>> = Mutex::new(None);

pub(crate) fn set_node_prioritized_view(transactions: Option<&HashMap<Txid, PrioTx>>) {
    if let Ok(mut view) = NODE_PRIORITIZED_VIEW.lock() {
        *view = transactions.map(|transactions| (transactions.clone(), Instant::now()));
    }
}

pub(crate) fn node_prioritized_view() -> Option<HashMap<Txid, PrioTx>> {
    let view = NODE_PRIORITIZED_VIEW.lock().ok()?;
    let (transactions, taken_at) = view.as_ref()?;
    (taken_at.elapsed() <= NODE_PRIORITIZED_VIEW_MAX_AGE).then(|| transactions.clone())
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MempoolSpaceAcceleration {
    txid: bitcoin::Txid,
    fee_delta: i64,
}

// Holds shared state (like the router) that so that it can be accessed in all routes.
#[derive(Clone)]
pub struct AppState {
    router: Router,
    stats_sender: StatsSender,
    downstream_handoff: crate::DownstreamHandoffSender,
    api_tx_token: Option<String>,
    prioritizing_txs: Option<PrioritizingTxs>,
}

#[derive(Clone)]
struct PrioritizingTxs {
    rpc: Arc<BitcoindRpc>,
}

pub(crate) async fn reset_node_fee_deltas_at_startup() -> bool {
    let Some(config) = Configuration::bitcoind_rpc_config() else {
        return true;
    };
    let rpc = BitcoindRpc::new(config.url, config.user, config.pwd);

    match reset_node_fee_deltas(&rpc).await {
        Ok(reset_count) => {
            info!(reset_count, "reset bitcoind fee deltas at startup");
            true
        }
        Err(error) => {
            error!(
                %error,
                "failed to reset bitcoind fee deltas at startup; transaction prioritization is disabled"
            );
            false
        }
    }
}

async fn reset_node_fee_deltas(rpc: &BitcoindRpc) -> Result<usize, BitcoindRpcError> {
    let transactions = rpc.get_prioritised_transactions().await?;
    let mut reset_count = 0;

    for transaction in transactions.into_values() {
        if transaction.fee_delta == 0 {
            continue;
        }

        // `i64::MIN` has no positive `i64` counterpart, so reverse it in two
        // adjustments rather than overflowing while negating it.
        let adjustments = transaction
            .fee_delta
            .checked_neg()
            .map_or_else(|| vec![i64::MAX, 1], |adjustment| vec![adjustment]);
        let mut reset_succeeded = true;
        for adjustment in adjustments {
            if let Err(e) = rpc
                .prioritise_transaction(&transaction.txid, adjustment)
                .await
            {
                error!(%transaction.txid, %e, "failed to reset bitcoind fee delta");
                reset_succeeded = false;
                break;
            }
        }
        if !reset_succeeded {
            continue;
        }
        reset_count += 1;
    }

    Ok(reset_count)
}

pub(crate) async fn start(
    router: Router,
    stats_sender: StatsSender,
    downstream_handoff: crate::DownstreamHandoffSender,
) {
    if let Err(error) = crate::db::init().await {
        warn!(
            "Could not open history database `jd_history.db`: {}. Mining will continue, but \
             history will not be stored. Check disk space and directory permissions. If the \
             database is disposable or corrupted, stop the client, move `jd_history.db`, \
             `jd_history.db-wal`, and `jd_history.db-shm` to a backup location, then restart.",
            error
        );
    }
    let api_tx_token = Configuration::api_tx_token();
    let prioritizing_txs = Configuration::bitcoind_rpc_config().map(|config| PrioritizingTxs {
        rpc: Arc::new(BitcoindRpc::new(config.url, config.user, config.pwd)),
    });
    let mut _tx_prio_tasks = None;
    if START_TX_PRIO.load(Ordering::Relaxed) {
        if let Some(config) = prioritizing_txs.as_ref() {
            let mut tasks = crate::shared::utils::AbortOnDrop::from(tokio::spawn(
                poll_mempool_space_accelerations(
                    Arc::clone(&config.rpc),
                    Arc::clone(&PRIORITIZED_TRANSACTIONS_POLL_LOCK),
                ),
            ));
            tokio::time::sleep(Duration::from_secs(30)).await;

            // This periodically aligns prioritized transactions in the cache with the node. If a
            // positively prioritized transaction is missing from the mempool, it fetches the
            // transaction from mempool.space and submits it to the node.
            tasks.add_task(tokio::spawn(poll_node_prioritized_transactions(
                Arc::clone(&config.rpc),
                Arc::clone(&PRIORITIZED_TRANSACTIONS_POLL_LOCK),
            )));

            tasks.add_task(tokio::spawn(clear_bad_transactions_periodically(
                Arc::clone(&PRIORITIZED_TRANSACTIONS_POLL_LOCK),
            )));

            _tx_prio_tasks = Some(tasks);
        }
    }

    let state = AppState {
        router,
        stats_sender,
        downstream_handoff,
        api_tx_token,
        prioritizing_txs,
    };
    let app = AxumRouter::new()
        .route("/api/health", get(Api::health_check))
        .route("/api/capabilities", get(Api::get_capabilities))
        .route(
            "/api/merge-mining/status",
            get(Api::get_merge_mining_status),
        )
        .route(
            "/api/coinbase/op-return",
            post(crate::merge_mining::set_pair_api),
        )
        .route(
            "/api/merge-mining/found-job",
            get(crate::merge_mining::poll_found_job_api),
        )
        .route(
            "/api/tx/prioritize/restore/{txid}",
            post(Api::restore_tx_priority),
        )
        .route(
            "/api/tx/prioritize/{txid}/{feedelta}",
            post(Api::prioritize_transaction),
        )
        .route(
            "/api/tx/prioritized",
            get(Api::get_prioritized_transactions),
        )
        .route("/api/pool/info", get(Api::get_pool_info))
        .route("/api/stats/miners", get(Api::get_downstream_stats))
        .route("/api/stats/aggregate", get(Api::get_aggregate_stats))
        .route("/api/stats/session-timing", get(Api::get_session_timing))
        .route("/api/stats/system", get(Api::system_stats))
        .route("/api/templates/recent", get(Api::get_recent_templates))
        .route("/api/declaration-policy", post(Api::set_declaration_policy))
        .route("/api/templates/{template_id}", get(Api::get_template_by_id))
        .route("/api/job-history", get(Api::get_job_history))
        .route("/api/job-history", delete(Api::clear_job_history))
        .route(
            "/api/history/retention",
            get(Api::get_history_retention).post(Api::set_history_retention),
        )
        .route("/api/job-txids/{template_id}", get(Api::get_job_txids))
        .route(
            "/api/prioritized-history",
            get(Api::get_prioritized_history),
        )
        // Dashboard routes
        .route("/", get(static_handler))
        .route("/{*path}", get(static_handler))
        .with_state(state);

    let api_server_port = crate::config::Configuration::api_server_port();
    let api_bind_address =
        std::env::var("API_BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string());
    let api_server_addr = format!("{api_bind_address}:{api_server_port}");
    loop {
        let listener = match tokio::net::TcpListener::bind(&api_server_addr).await {
            Ok(listener) => listener,
            Err(error) => {
                error!(%error, %api_server_addr, "API server could not bind; mining remains active");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        info!(%api_server_addr, "API server listening");
        open_dashboard(&api_server_port);
        if let Err(error) = axum::serve(listener, app.clone()).await {
            error!(%error, "API server stopped; mining remains active");
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn clear_bad_transactions_periodically(poll_lock: Arc<tokio::sync::Mutex<()>>) {
    loop {
        tokio::time::sleep(BAD_TRANSACTIONS_CLEAR_INTERVAL).await;
        let _poll_guard = poll_lock.lock().await;
        crate::prioritized_transactions::BAD_TRANSACTIONS.clear();
        info!("cleared bad transactions cache");
    }
}

async fn poll_node_prioritized_transactions(
    rpc: Arc<BitcoindRpc>,
    poll_lock: Arc<tokio::sync::Mutex<()>>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("valid mempool.space HTTP client");
    let mut interval = tokio::time::interval(PRIORITIZED_TRANSACTIONS_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let _poll_guard = poll_lock.lock().await;
        reconcile_node_prioritized_transactions(
            &rpc,
            &client,
            MEMPOOL_SPACE_API_BASE_URL,
            &crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS,
            &crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED,
        )
        .await;
    }
}

async fn reconcile_node_prioritized_transactions(
    rpc: &BitcoindRpc,
    client: &reqwest::Client,
    mempool_space_api_base_url: &str,
    manually_prioritized: &crate::prioritized_transactions::TransactionStore<i64>,
    mempool_space_accelerated: &crate::prioritized_transactions::TransactionStore<i64>,
) {
    let transactions = match rpc.get_prioritised_transactions().await {
        Ok(transactions) => transactions,
        Err(error) => {
            error!(%error, "failed to fetch prioritized transactions from bitcoind");
            return;
        }
    };

    // a node will remove any prioritizations on unconfirmed transactions. So, if a tx gets mined,
    // it will removed from prioritization and therefore from internal caches thank to these lines
    for store in [manually_prioritized, mempool_space_accelerated] {
        for txid in store.snapshot_txids() {
            if !transactions.contains_key(&txid) {
                store.remove(&txid);
            }
        }
    }

    for transaction in transactions.into_values() {
        if crate::prioritized_transactions::BAD_TRANSACTIONS
            .get(&transaction.txid)
            .is_some()
        {
            continue;
        }

        for (source, store) in [
            (prioritized::MANUAL, manually_prioritized),
            (prioritized::MEMPOOL_SPACE, mempool_space_accelerated),
        ] {
            if store.update_existing(&transaction.txid, transaction.fee_delta) {
                prioritized::record(source, &transaction.txid, transaction.fee_delta).await;
            }
        }

        if transaction.fee_delta <= 0 || transaction.in_mempool {
            continue;
        }

        match fetch_mempool_space_transaction_from(
            client,
            mempool_space_api_base_url,
            transaction.txid,
        )
        .await
        {
            Ok(fetched_transaction) => match rpc.submit_transaction(&fetched_transaction).await {
                Ok(_) => {
                    info!(
                        txid = %transaction.txid,
                        fee_delta = transaction.fee_delta,
                        "submitted missing prioritized transaction to bitcoind"
                    );
                }
                Err(error) => {
                    error!(
                        txid = %transaction.txid,
                        fee_delta = transaction.fee_delta,
                        %error,
                        "failed to submit missing prioritized transaction to bitcoind"
                    );

                    crate::prioritized_transactions::BAD_TRANSACTIONS
                        .record(transaction.txid, transaction.fee_delta);
                    if let Err(error) = rpc
                        .prioritise_transaction(&transaction.txid, -transaction.fee_delta)
                        .await
                    {
                        error!(
                            txid = %transaction.txid,
                            fee_delta = transaction.fee_delta,
                            %error,
                            "failed to restore rejected transaction fee delta to zero"
                        );
                    } else {
                        manually_prioritized.update_existing(&transaction.txid, 0);
                        mempool_space_accelerated.update_existing(&transaction.txid, 0);
                    }
                }
            },
            Err(error) => {
                error!(
                    txid = %transaction.txid,
                    fee_delta = transaction.fee_delta,
                    %error,
                    "failed to fetch missing prioritized transaction from mempool.space"
                );
            }
        }
    }
}

// NOTE what the inner loop does
// 1. Fetch accelerations and build the desired set
// 2. get the list of prio transactions with getptioritisedtransactions and make the list of
//    those txs whose prioritization need to be changed, with the fee_delta adjustment to apply.
// 3. call prioritise_transaction for all these transactions with the fee_delta adjustment to apply
// 4. for all the tx that are in the accelerations from mempool.space but not in the local cache,
//    fetch them from mempool.space api, validate, write in the list to_be_recorded
// 5. for all the tx that are in the local cache but not in the accelerations from mempool.space,
//    restore transaction true priority and evict them from local cache
// 6. for all txs in to_be_recorded, record it in the local cache
//
// NOTE this cancels manually submitted transactions. This is a feature that needs to be added
// again in the future. It may be sufficient to add a marker on local cache for these txs
async fn poll_mempool_space_accelerations(
    rpc: Arc<BitcoindRpc>,
    poll_lock: Arc<tokio::sync::Mutex<()>>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("valid mempool.space HTTP client");
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let _poll_guard = poll_lock.lock().await;
        match fetch_mempool_space_accelerations(&client).await {
            Ok(accelerations) => {
                let cached_txids =
                    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.snapshot();
                for (txid, fee_delta) in &accelerations {
                    let fee_delta_adjustment = *fee_delta - cached_txids.get(txid).unwrap_or(&0);
                    if fee_delta_adjustment != 0 {
                        if let Err(error) =
                            rpc.prioritise_transaction(txid, fee_delta_adjustment).await
                        {
                            error!(%txid, %error, "failed to prioritize mempool.space transaction");
                        } else {
                            crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED
                                .record(*txid, *fee_delta);
                        }
                    }
                    // Record the acceleration in the database if it is not already there. This ensures that the database reflects the current state of the node's prioritizations, even if the node was restarted or the application was restarted.
                    if crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED
                        .get(txid)
                        .is_some()
                    {
                        prioritized::record(prioritized::MEMPOOL_SPACE, txid, *fee_delta).await;
                    }
                    if !cached_txids.contains_key(txid) {
                        match fetch_mempool_space_transaction(&client, *txid).await {
                            Ok(transaction) => {
                                if let Err(e) = rpc.submit_transaction(&transaction).await {
                                    error!(%txid, %e, "failed to restore mempool.space transaction priority after submit error");
                                }
                            }
                            Err(error) => {
                                error!(%txid, %error, "failed to fetch or parse mempool.space transaction");
                            }
                        }
                    }
                }
            }
            Err(error) => {
                error!(%error, "failed to fetch or parse mempool.space accelerations");
            }
        }
    }
}

// the other fields of this call are ignored. This is the shape of a typical api response
// curl -s https://mempool.space/api/v1/services/accelerator/accelerations | jq
//[
//  {
//    "txid": "83bfbcebd65f98e15fbfabe9294d6831a426ec47904cd6b9b69875eba124e75d",
//    "added": 1785519334,
//    "feeDelta": 6438,
//    "effectiveVsize": 652,
//    "effectiveFee": 652,
//    "pools": [
//      112,
//      102,
//      94,
//      143,
//      110,
//      105,
//      4,
//      44,
//      43,
//      2,
//      116,
//      6,
//      115,
//      111,
//      36
//    ]
//  }
//]
async fn fetch_mempool_space_accelerations(
    client: &reqwest::Client,
) -> reqwest::Result<HashMap<bitcoin::Txid, i64>> {
    let accelerations = client
        .get("https://mempool.space/api/v1/services/accelerator/accelerations")
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<MempoolSpaceAcceleration>>()
        .await?;

    Ok(accelerations
        .into_iter()
        .map(|acceleration| (acceleration.txid, acceleration.fee_delta))
        .collect())
}

#[derive(Debug)]
enum FetchMempoolSpaceTransactionError {
    Request(reqwest::Error),
    Decode(FromHexError),
    TxidMismatch { requested: Txid, received: Txid },
}

impl fmt::Display for FetchMempoolSpaceTransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(error) => write!(formatter, "mempool.space request failed: {error}"),
            Self::Decode(error) => write!(
                formatter,
                "failed to decode mempool.space transaction hex: {error}"
            ),
            Self::TxidMismatch {
                requested,
                received,
            } => write!(
                formatter,
                "mempool.space returned transaction {received} for requested transaction {requested}"
            ),
        }
    }
}

impl StdError for FetchMempoolSpaceTransactionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Request(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::TxidMismatch { .. } => None,
        }
    }
}

impl From<reqwest::Error> for FetchMempoolSpaceTransactionError {
    fn from(error: reqwest::Error) -> Self {
        Self::Request(error)
    }
}

impl From<FromHexError> for FetchMempoolSpaceTransactionError {
    fn from(error: FromHexError) -> Self {
        Self::Decode(error)
    }
}

async fn fetch_mempool_space_transaction(
    client: &reqwest::Client,
    txid: Txid,
) -> Result<Transaction, FetchMempoolSpaceTransactionError> {
    fetch_mempool_space_transaction_from(client, MEMPOOL_SPACE_API_BASE_URL, txid).await
}

async fn fetch_mempool_space_transaction_from(
    client: &reqwest::Client,
    api_base_url: &str,
    txid: Txid,
) -> Result<Transaction, FetchMempoolSpaceTransactionError> {
    let transaction_hex = client
        .get(format!("{api_base_url}/tx/{txid}/hex"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let transaction: Transaction = deserialize_hex(transaction_hex.trim())?;
    let received_txid = transaction.compute_txid();

    if received_txid != txid {
        return Err(FetchMempoolSpaceTransactionError::TxidMismatch {
            requested: txid,
            received: received_txid,
        });
    }

    Ok(transaction)
}

#[cfg(test)]
mod tests {
    use super::{
        fetch_mempool_space_transaction_from, reconcile_node_prioritized_transactions,
        reset_node_fee_deltas, BitcoindRpc, FetchMempoolSpaceTransactionError,
    };
    use axum::{
        extract::{Path, State},
        http::StatusCode,
        routing::{get, post},
        Json, Router,
    };
    use bitcoin::{
        consensus::encode::{deserialize_hex, serialize_hex},
        Transaction, Txid,
    };
    use serde_json::{json, Value};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
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

    async fn raw_transaction(Path(_txid): Path<Txid>) -> &'static str {
        RAW_TX
    }

    async fn spawn_mempool_space_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let address = listener.local_addr().expect("test server local address");
        let app = Router::new().route("/api/tx/{txid}/hex", get(raw_transaction));
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });

        (format!("http://{address}/api"), server)
    }

    #[tokio::test]
    async fn fetches_and_decodes_mempool_space_transaction_hex() {
        let expected: Transaction = deserialize_hex(RAW_TX).expect("valid test transaction");
        let expected_txid = expected.compute_txid();
        let (api_base_url, server) = spawn_mempool_space_server().await;

        let transaction = fetch_mempool_space_transaction_from(
            &reqwest::Client::new(),
            &api_base_url,
            expected_txid,
        )
        .await
        .expect("transaction should be fetched and decoded");

        assert_eq!(transaction, expected);
        server.abort();
    }

    #[tokio::test]
    async fn rejects_a_transaction_that_does_not_match_the_requested_txid() {
        let transaction: Transaction = deserialize_hex(RAW_TX).expect("valid test transaction");
        let received = transaction.compute_txid();
        let requested = "0000000000000000000000000000000000000000000000000000000000000001"
            .parse()
            .expect("valid test txid");
        let (api_base_url, server) = spawn_mempool_space_server().await;

        let error =
            fetch_mempool_space_transaction_from(&reqwest::Client::new(), &api_base_url, requested)
                .await
                .expect_err("mismatched transaction should be rejected");

        assert!(matches!(
            error,
            FetchMempoolSpaceTransactionError::TxidMismatch {
                requested: actual_requested,
                received: actual_received,
            } if actual_requested == requested && actual_received == received
        ));
        server.abort();
    }

    #[derive(Clone)]
    struct ResetFeeDeltasServerState {
        fee_deltas: Arc<Mutex<HashMap<Txid, i64>>>,
        requests: UnboundedSender<Value>,
    }

    async fn reset_fee_deltas_bitcoind(
        State(state): State<ResetFeeDeltasServerState>,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state
            .requests
            .send(body.clone())
            .expect("test should receive bitcoind request");

        match body.get("method").and_then(Value::as_str) {
            Some("getprioritisedtransactions") => {
                let transactions = state
                    .fee_deltas
                    .lock()
                    .expect("fee delta mutex should not be poisoned")
                    .iter()
                    .map(|(txid, fee_delta)| {
                        (
                            txid.to_string(),
                            json!({
                                "fee_delta": fee_delta,
                                "in_mempool": true
                            }),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>();
                (
                    StatusCode::OK,
                    Json(json!({
                        "result": transactions,
                        "error": null,
                        "id": "dmnd-client"
                    })),
                )
            }
            Some("prioritisetransaction") => {
                let txid = body["params"][0]
                    .as_str()
                    .expect("txid should be a string")
                    .parse::<Txid>()
                    .expect("txid should be valid");
                let adjustment = body["params"][2]
                    .as_i64()
                    .expect("fee delta adjustment should be an integer");
                let mut fee_deltas = state
                    .fee_deltas
                    .lock()
                    .expect("fee delta mutex should not be poisoned");
                let updated_fee_delta = fee_deltas
                    .get(&txid)
                    .copied()
                    .unwrap_or_default()
                    .checked_add(adjustment)
                    .expect("fee delta should not overflow");
                if updated_fee_delta == 0 {
                    fee_deltas.remove(&txid);
                } else {
                    fee_deltas.insert(txid, updated_fee_delta);
                }
                (
                    StatusCode::OK,
                    Json(json!({
                        "result": true,
                        "error": null,
                        "id": "dmnd-client"
                    })),
                )
            }
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

    #[tokio::test]
    async fn startup_reset_reverses_every_node_fee_delta_and_is_idempotent() {
        let positive_txid = "0000000000000000000000000000000000000000000000000000000000000001"
            .parse::<Txid>()
            .expect("valid txid");
        let negative_txid = "0000000000000000000000000000000000000000000000000000000000000002"
            .parse::<Txid>()
            .expect("valid txid");
        let minimum_txid = "0000000000000000000000000000000000000000000000000000000000000003"
            .parse::<Txid>()
            .expect("valid txid");
        let fee_deltas = Arc::new(Mutex::new(HashMap::from([
            (positive_txid, 75_000),
            (negative_txid, -25_000),
            (minimum_txid, i64::MIN),
        ])));
        let (requests, mut received_requests) = unbounded_channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let address = listener.local_addr().expect("test server local address");
        let app = Router::new()
            .route("/", post(reset_fee_deltas_bitcoind))
            .with_state(ResetFeeDeltasServerState {
                fee_deltas: Arc::clone(&fee_deltas),
                requests,
            });
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });
        let rpc = BitcoindRpc::new(
            format!("http://{address}"),
            "user".to_string(),
            "password".to_string(),
        );

        assert_eq!(
            reset_node_fee_deltas(&rpc)
                .await
                .expect("fee deltas should be reset"),
            3
        );
        assert!(fee_deltas
            .lock()
            .expect("fee delta mutex should not be poisoned")
            .is_empty());

        assert_eq!(
            received_requests
                .recv()
                .await
                .expect("getprioritisedtransactions request")["method"],
            "getprioritisedtransactions"
        );
        let mut adjustments: HashMap<Txid, Vec<i64>> = HashMap::new();
        for _ in 0..4 {
            let request = received_requests
                .recv()
                .await
                .expect("prioritisetransaction request");
            assert_eq!(request["method"], "prioritisetransaction");
            assert_eq!(request["params"][1], 0);
            adjustments
                .entry(
                    request["params"][0]
                        .as_str()
                        .expect("txid should be a string")
                        .parse::<Txid>()
                        .expect("txid should be valid"),
                )
                .or_default()
                .push(
                    request["params"][2]
                        .as_i64()
                        .expect("fee delta adjustment should be an integer"),
                );
        }
        assert_eq!(
            adjustments,
            HashMap::from([
                (positive_txid, vec![-75_000]),
                (negative_txid, vec![25_000]),
                (minimum_txid, vec![i64::MAX, 1]),
            ])
        );

        assert_eq!(
            reset_node_fee_deltas(&rpc)
                .await
                .expect("a repeated reset should succeed"),
            0
        );
        assert_eq!(
            received_requests
                .recv()
                .await
                .expect("second getprioritisedtransactions request")["method"],
            "getprioritisedtransactions"
        );
        assert!(received_requests.try_recv().is_err());

        server.abort();
    }

    #[derive(Clone)]
    struct ReconciliationServerState {
        txid: Txid,
        in_mempool_txid: Txid,
        non_positive_txid: Txid,
        transaction_hex: String,
        reject_submission: bool,
        requests: UnboundedSender<Value>,
    }

    async fn reconciliation_bitcoind(
        State(state): State<ReconciliationServerState>,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state
            .requests
            .send(body.clone())
            .expect("test should receive bitcoind request");

        match body.get("method").and_then(Value::as_str) {
            Some("getprioritisedtransactions") => {
                let mut transactions = serde_json::Map::new();
                transactions.insert(
                    state.txid.to_string(),
                    json!({
                        "fee_delta": 75_000,
                        "in_mempool": false
                    }),
                );
                transactions.insert(
                    state.in_mempool_txid.to_string(),
                    json!({
                        "fee_delta": 50_000,
                        "in_mempool": true
                    }),
                );
                transactions.insert(
                    state.non_positive_txid.to_string(),
                    json!({
                        "fee_delta": -25_000,
                        "in_mempool": false
                    }),
                );
                (
                    StatusCode::OK,
                    Json(json!({
                        "result": transactions,
                        "error": null,
                        "id": "dmnd-client"
                    })),
                )
            }
            Some("sendrawtransaction") => {
                assert_eq!(body["params"], json!([state.transaction_hex]));
                if state.reject_submission {
                    (
                        StatusCode::OK,
                        Json(json!({
                            "result": null,
                            "error": {
                                "code": -26,
                                "message": "bad transaction"
                            },
                            "id": "dmnd-client"
                        })),
                    )
                } else {
                    (
                        StatusCode::OK,
                        Json(json!({
                            "result": state.txid,
                            "error": null,
                            "id": "dmnd-client"
                        })),
                    )
                }
            }
            Some("prioritisetransaction") => {
                assert!(state.reject_submission);
                (
                    StatusCode::OK,
                    Json(json!({
                        "result": true,
                        "error": null,
                        "id": "dmnd-client"
                    })),
                )
            }
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

    async fn reconciliation_raw_transaction(
        State(state): State<ReconciliationServerState>,
        Path(txid): Path<Txid>,
    ) -> (StatusCode, String) {
        state
            .requests
            .send(json!({ "mempool_space_txid": txid }))
            .expect("test should receive mempool.space request");

        if txid == state.txid {
            (StatusCode::OK, state.transaction_hex)
        } else {
            (StatusCode::NOT_FOUND, String::new())
        }
    }

    #[tokio::test]
    async fn reconciliation_removes_stale_cache_entries_and_submits_a_positive_missing_transaction()
    {
        let transaction: Transaction = deserialize_hex(RAW_TX).expect("valid test transaction");
        let txid = transaction.compute_txid();
        let in_mempool_txid = "0000000000000000000000000000000000000000000000000000000000000001"
            .parse()
            .expect("valid test txid");
        let non_positive_txid = "0000000000000000000000000000000000000000000000000000000000000002"
            .parse()
            .expect("valid test txid");
        let stale_txid = "0000000000000000000000000000000000000000000000000000000000000003"
            .parse()
            .expect("valid test txid");
        let transaction_hex = serialize_hex(&transaction);
        let manually_prioritized = crate::prioritized_transactions::TransactionStore::new();
        let mempool_space_accelerated = crate::prioritized_transactions::TransactionStore::new();
        manually_prioritized.record(txid, 1);
        manually_prioritized.record(in_mempool_txid, 3);
        manually_prioritized.record(stale_txid, 5);
        mempool_space_accelerated.record(txid, 2);
        mempool_space_accelerated.record(non_positive_txid, 4);
        mempool_space_accelerated.record(stale_txid, 6);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let address = listener.local_addr().expect("test server local address");
        let (requests, mut received_requests) = unbounded_channel();
        let state = ReconciliationServerState {
            txid,
            in_mempool_txid,
            non_positive_txid,
            transaction_hex,
            reject_submission: false,
            requests,
        };
        let app = Router::new()
            .route("/bitcoin", post(reconciliation_bitcoind))
            .route("/api/tx/{txid}/hex", get(reconciliation_raw_transaction))
            .with_state(state);
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should run");
        });
        let rpc = BitcoindRpc::new(
            format!("http://{address}/bitcoin"),
            "user".to_string(),
            "password".to_string(),
        );

        reconcile_node_prioritized_transactions(
            &rpc,
            &reqwest::Client::new(),
            &format!("http://{address}/api"),
            &manually_prioritized,
            &mempool_space_accelerated,
        )
        .await;

        let manually_prioritized = manually_prioritized.snapshot();
        let mempool_space_accelerated = mempool_space_accelerated.snapshot();
        assert_eq!(manually_prioritized.get(&txid), Some(&75_000));
        assert_eq!(manually_prioritized.get(&in_mempool_txid), Some(&50_000));
        assert!(!manually_prioritized.contains_key(&non_positive_txid));
        assert!(!manually_prioritized.contains_key(&stale_txid));
        assert_eq!(mempool_space_accelerated.get(&txid), Some(&75_000));
        assert_eq!(
            mempool_space_accelerated.get(&non_positive_txid),
            Some(&-25_000)
        );
        assert!(!mempool_space_accelerated.contains_key(&in_mempool_txid));
        assert!(!mempool_space_accelerated.contains_key(&stale_txid));
        assert_eq!(
            received_requests
                .try_recv()
                .expect("getprioritisedtransactions request")["method"],
            "getprioritisedtransactions"
        );
        assert_eq!(
            received_requests
                .try_recv()
                .expect("mempool.space transaction request")["mempool_space_txid"],
            txid.to_string()
        );
        assert_eq!(
            received_requests
                .try_recv()
                .expect("sendrawtransaction request")["method"],
            "sendrawtransaction"
        );
        assert!(received_requests.try_recv().is_err());

        server.abort();
    }
}
