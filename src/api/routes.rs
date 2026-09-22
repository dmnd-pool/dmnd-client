use super::{
    bitcoin_rpc::{BitcoindRpc, BitcoindRpcError, PrioTx},
    utils::get_cpu_and_memory_usage,
    AppState, PRIORITIZED_TRANSACTIONS_POLL_LOCK,
};
use crate::db::prioritized;
use crate::{config::Configuration, db::history, proxy_state::ProxyState};
use axum::{
    extract::{Path, Query, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use bitcoin::Txid;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use tracing::{error, info, warn};

/// Render one received template for the dashboard.
fn template_payload(
    snapshot: &crate::block_templates::TemplateSnapshot,
    with_transactions: bool,
) -> serde_json::Value {
    let accelerated =
        crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.snapshot_txids();
    let mut prioritized =
        crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.snapshot_txids();
    prioritized.extend(accelerated.iter().copied());
    let prioritized_included: Vec<String> = snapshot
        .transactions
        .iter()
        .filter(|tx| prioritized.contains(&tx.txid))
        .map(|tx| tx.txid.to_string())
        .collect();

    let mut priced_tx_count: usize = 0;
    let mut transactions = Vec::with_capacity(if with_transactions {
        snapshot.transactions.len()
    } else {
        0
    });

    for tx in &snapshot.transactions {
        if tx.fee_sat.is_some() {
            priced_tx_count += 1;
        }
        if with_transactions {
            transactions.push(json!({
                "txid": tx.txid.to_string(),
                "weight": tx.weight,
                "vsize": tx.vsize,
                "fee_sat": tx.fee_sat,
                "fee_rate_sat_per_vb": tx.fee_sat.map(|fee| fee as f64 / tx.vsize.max(1) as f64),
            }));
        }
    }

    let mut payload = json!({
        "available": true,
        "template_id": snapshot.template_id,
        "future_template": snapshot.future_template,
        "version": snapshot.version,
        "height": snapshot.height,
        "coinbase_value_sat": snapshot.coinbase_tx_value_remaining,
        "subsidy_sat": snapshot.subsidy_sat,
        "total_fees_sat": snapshot.total_fees_sat,
        "tx_count": snapshot.transactions.len(),
        "priced_tx_count": priced_tx_count,
        "total_weight": snapshot.total_weight,
        "received_at": snapshot.received_at,
        "prioritized_included": prioritized_included,
        "mempool_space_included": snapshot.transactions.iter()
            .filter(|tx| accelerated.contains(&tx.txid))
            .map(|tx| tx.txid.to_string()).collect::<Vec<_>>(),
    });
    if with_transactions {
        payload["transactions"] = json!(transactions);
    }
    payload
}

/// `None` keeps everything.
#[derive(Debug, Deserialize)]
pub struct HistoryRetentionRequest {
    pub keep_blocks: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct DeclarationPolicyRequest {
    /// One of `highest_fees`, `block_weight`.
    pub policy: String,
}

pub struct Api {}

impl Api {
    // Retrieves connected donwnstreams stats
    pub async fn get_downstream_stats(State(state): State<AppState>) -> impl IntoResponse {
        match state.stats_sender.collect_stats().await {
            Ok(stats) => (StatusCode::OK, Json(APIResponse::success(Some(stats)))),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(APIResponse::error(Some(format!(
                    "Failed to collect stats: {e}"
                )))),
            ),
        }
    }

    // Retrieves system stats (CPU and memory usage)
    pub async fn system_stats() -> impl IntoResponse {
        let (cpu, memory) = get_cpu_and_memory_usage().await;
        let cpu_usgae = format!("{cpu:.3}");
        let data = serde_json::json!({"cpu_usage_%": cpu_usgae, "memory_usage_bytes": memory});
        Json(APIResponse::success(Some(data)))
    }

    // Returns aggregate stats of all downstream devices
    pub async fn get_aggregate_stats(State(state): State<AppState>) -> impl IntoResponse {
        let stats = match state.stats_sender.collect_stats().await {
            Ok(stats) => stats,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(APIResponse::error(Some(format!(
                        "Failed to collect stats: {e}"
                    )))),
                );
            }
        };
        let mut total_connected_device = 0;
        let mut total_accepted_shares = 0;
        let mut total_rejected_shares = 0;
        let mut total_hashrate = 0.0;
        let mut total_diff = 0.0;
        for (_, downstream) in stats {
            total_connected_device += 1;
            total_accepted_shares += downstream.accepted_shares;
            total_rejected_shares += downstream.rejected_shares;
            total_hashrate += downstream.hashrate as f64;
            total_diff += downstream.current_difficulty as f64
        }
        let result = AggregateStates {
            total_connected_device,
            aggregate_hashrate: total_hashrate,
            aggregate_accepted_shares: total_accepted_shares,
            aggregate_rejected_shares: total_rejected_shares,
            aggregate_diff: total_diff,
        };
        (StatusCode::OK, Json(APIResponse::success(Some(result))))
    }

    pub async fn get_session_timing() -> impl IntoResponse {
        (
            StatusCode::OK,
            Json(APIResponse::success(Some(crate::debug_timing::snapshot()))),
        )
    }

    pub async fn get_pool_info(State(state): State<AppState>) -> impl IntoResponse {
        let address = crate::ACTIVE_POOL_ADDRESS
            .safe_lock(|address| *address)
            .unwrap_or(None);
        let setup_latency = *state.router.latency_rx.borrow();

        match address {
            Some(address) => (
                StatusCode::OK,
                Json(APIResponse::success(Some(serde_json::json!({
                    "address": address.to_string(),
                    "latency": setup_latency.map(|latency| latency.as_millis().to_string()),
                    "declaration_latency_ms": super::stats::declaration_latency_ms(),
                    "bandwidth_bytes_per_sec": super::stats::bandwidth_bytes_per_sec(),
                })))),
            ),
            None => (
                StatusCode::NOT_FOUND,
                Json(APIResponse::error(Some(
                    "not connected to a pool yet".to_string(),
                ))),
            ),
        }
    }

    // Returns the status of the Proxy
    pub async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
        if state.downstream_handoff.is_closed() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::error(Some(
                    "Overloaded: translator handoff channel is closed".to_string(),
                ))),
            );
        }

        if state.downstream_handoff.capacity() == 0 {
            let max_capacity = state.downstream_handoff.max_capacity();
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::error(Some(format!(
                    "Overloaded: translator handoff channel queue is full (0/{max_capacity} slots available)"
                )))),
            );
        }

        if let Some(max_active_downstreams) = Configuration::max_active_downstreams() {
            if let Ok(stats) = state.stats_sender.collect_stats().await {
                let active_downstreams = stats.len();
                if active_downstreams >= max_active_downstreams {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(APIResponse::error(Some(format!(
                            "Overloaded: active downstreams {active_downstreams}/{max_active_downstreams}"
                        )))),
                    );
                }
            }
        }

        match ProxyState::is_proxy_down() {
            (false, None) => (
                StatusCode::OK,
                Json(APIResponse::success(Some("Proxy OK".to_string()))),
            ),
            (true, Some(states)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::error(Some(states))),
            ),
            _ => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::error(Some("Unknown proxy state".to_string()))),
            ),
        }
    }

    pub async fn get_capabilities(State(state): State<AppState>) -> impl IntoResponse {
        Json(APIResponse::success(Some(json!({
            "templates": state.api_tx_token.is_some(),
            "transaction_prioritization": state.api_tx_token.is_some()
                && state.prioritizing_txs.is_some()
        }))))
    }

    pub async fn get_merge_mining_status(
        State(state): State<AppState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }
        match crate::merge_mining::dashboard_snapshot() {
            Ok(snapshot) => (
                StatusCode::OK,
                Json(APIResponse::success(Some(json!({
                    "configured": crate::merge_mining::configured(),
                    "job_declaration": Configuration::tp_address().is_some(),
                    "activity": snapshot,
                })))),
            ),
            Err(error) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::error(Some(error.to_string()))),
            ),
        }
    }

    /// Candidates for the current tip, newest first.
    pub async fn get_recent_templates(
        State(state): State<AppState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }

        let policy = crate::block_templates::declaration_policy();
        let templates: Vec<serde_json::Value> = crate::block_templates::with_candidates(|held| {
            held.iter()
                .map(|snapshot| template_payload(snapshot, false))
                .collect()
        });

        (
            StatusCode::OK,
            Json(APIResponse::success(Some(json!({
                "templates": templates,
                "candidate_limit": crate::block_templates::CANDIDATE_LIMIT,
                // The declaration the pool has accepted
                "active_declaration": crate::block_templates::active_declaration(),
                "policy": policy.as_str(),
                // What the policy resolves to
                "policy_pick": crate::block_templates::policy_pick(policy),
            })))),
        )
    }

    /// Set which candidate gets auto-declared.
    pub async fn set_declaration_policy(
        State(state): State<AppState>,
        headers: HeaderMap,
        Json(request): Json<DeclarationPolicyRequest>,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }

        let Some(policy) = crate::block_templates::DeclarationPolicy::parse(&request.policy) else {
            return (
                StatusCode::BAD_REQUEST,
                Json(APIResponse::error(Some(format!(
                    "unknown declaration policy {:?}",
                    request.policy
                )))),
            );
        };
        crate::block_templates::set_declaration_policy(policy);
        info!(policy = policy.as_str(), "declaration policy changed");
        (
            StatusCode::OK,
            Json(APIResponse::success(Some(json!({
            "policy": policy.as_str(),
            })))),
        )
    }

    /// One received template with its full transaction list.
    pub async fn get_template_by_id(
        State(state): State<AppState>,
        headers: HeaderMap,
        Path(template_id): Path<u64>,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }

        let Some(snapshot) = crate::block_templates::by_id(template_id) else {
            return (
                StatusCode::NOT_FOUND,
                Json(APIResponse::error(Some(format!(
                    "template {template_id} is no longer held; only the last {} received are kept",
                    crate::block_templates::CANDIDATE_LIMIT
                )))),
            );
        };

        (
            StatusCode::OK,
            Json(APIResponse::success(Some(template_payload(
                &snapshot, true,
            )))),
        )
    }

    pub async fn prioritize_transaction(
        State(state): State<AppState>,
        headers: HeaderMap,
        Path((txid, fee_delta)): Path<(Txid, i64)>,
    ) -> impl IntoResponse {
        if fee_delta == 0 {
            return (StatusCode::OK, Json(APIResponse::success(None)));
        }
        let Some(prioritizing_txs) = state.prioritizing_txs.as_ref() else {
            warn!("PRIORITIZING TXS NOT ENABLED");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::<String>::error(Some(
                    "PRIORITIZING TXS NOT ENABLED".to_string(),
                ))),
            );
        };

        if !is_authorized_for_tx_prioritization(
            &headers,
            state.api_tx_token.as_deref().unwrap_or(""),
        ) {
            warn!("unauthorized tx prioritization request");
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::<String>::error(Some(
                    "Unauthorized".to_string(),
                ))),
            );
        }

        let _prioritized_transactions_guard = PRIORITIZED_TRANSACTIONS_POLL_LOCK.lock().await;
        if crate::prioritized_transactions::BAD_TRANSACTIONS
            .get(&txid)
            .is_some()
        {
            warn!(%txid, "transaction acceleration rejected because transaction is cached as bad");
            return (
                StatusCode::BAD_REQUEST,
                Json(APIResponse::<String>::error(Some(format!(
                    "Transaction acceleration was unsuccessful: {txid} was previously rejected by bitcoind"
                )))),
            );
        }

        match prioritizing_txs
            .rpc
            .prioritise_transaction(&txid, fee_delta)
            .await
        {
            Ok(()) => {
                let current_fee_delta = crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS
                    .get(&txid)
                    .unwrap_or(0);
                let fee_delta = current_fee_delta + fee_delta;
                crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.record(txid, fee_delta);
                prioritized::record(prioritized::MANUAL, &txid, fee_delta).await;
                crate::api::set_node_prioritized_view(None);
                info!("transaction prioritized in bitcoind: {txid}");
                (
                    StatusCode::OK,
                    Json(APIResponse::success(Some(txid.to_string()))),
                )
            }
            Err(e) => {
                error!("Failed to prioritize transaction in bitcoind: {e}");
                (
                    e.status_code(),
                    Json(APIResponse::error(Some(e.to_string()))),
                )
            }
        }
    }

    pub async fn restore_tx_priority(
        State(state): State<AppState>,
        headers: HeaderMap,
        Path(txid): Path<Txid>,
    ) -> impl IntoResponse {
        let Some(prioritizing_txs) = state.prioritizing_txs.as_ref() else {
            warn!("PRIORITIZING TXS NOT ENABLED");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::<String>::error(Some(
                    "PRIORITIZING TXS NOT ENABLED".to_string(),
                ))),
            );
        };
        if !is_authorized_for_tx_prioritization(
            &headers,
            state.api_tx_token.as_deref().unwrap_or(""),
        ) {
            warn!("unauthorized tx priority restoration request");
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::<String>::error(Some(
                    "Unauthorized".to_string(),
                ))),
            );
        }

        let _prioritized_transactions_guard = PRIORITIZED_TRANSACTIONS_POLL_LOCK.lock().await;
        match Self::restore_tx_priority_from_stores(&prioritizing_txs.rpc, &txid).await {
            Ok(()) => {
                info!(%txid, "transaction priority restored in bitcoind");
                (
                    StatusCode::OK,
                    Json(APIResponse::success(Some(txid.to_string()))),
                )
            }
            Err(e) => {
                error!(%txid, error = %e, "failed to restore transaction priority in bitcoind");
                (
                    e.status_code(),
                    Json(APIResponse::error(Some(e.to_string()))),
                )
            }
        }
    }

    pub(super) async fn restore_tx_priority_from_stores(
        rpc: &BitcoindRpc,
        txid: &Txid,
    ) -> Result<(), BitcoindRpcError> {
        let stores = [
            &crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS,
            &crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED,
        ];

        for store in stores {
            let Some(fee_delta) = store.get(txid) else {
                continue;
            };
            let fee_delta_adjustment = fee_delta.checked_neg().ok_or_else(|| {
                BitcoindRpcError::Prioritize("fee delta adjustment overflow".to_string())
            })?;

            rpc.prioritise_transaction(txid, fee_delta_adjustment)
                .await?;
        }

        for store in stores {
            store.remove(txid);
        }
        crate::api::set_node_prioritized_view(None);

        Ok(())
    }

    pub async fn get_prioritized_transactions(
        State(state): State<AppState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        let Some(prioritizing_txs) = state.prioritizing_txs.as_ref() else {
            warn!("PRIORITIZING TXS NOT ENABLED");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(APIResponse::<CategorizedPrioritizedTransactions>::error(
                    Some("PRIORITIZING TXS NOT ENABLED".to_string()),
                )),
            );
        };

        if !is_authorized_for_tx_prioritization(
            &headers,
            state.api_tx_token.as_deref().unwrap_or(""),
        ) {
            warn!("unauthorized prioritized txs request");
            return (
                StatusCode::UNAUTHORIZED,
                Json(APIResponse::<CategorizedPrioritizedTransactions>::error(
                    Some("Unauthorized".to_string()),
                )),
            );
        }

        let transactions = match crate::api::node_prioritized_view() {
            Some(transactions) => transactions,
            None => {
                let _prioritized_transactions_guard =
                    PRIORITIZED_TRANSACTIONS_POLL_LOCK.lock().await;
                match prioritizing_txs.rpc.get_prioritised_transactions().await {
                    Ok(transactions) => {
                        crate::api::set_node_prioritized_view(Some(&transactions));
                        transactions
                    }
                    Err(e) => {
                        error!(error = %e, "failed to fetch prioritized transactions from bitcoind");
                        return (
                            e.status_code(),
                            Json(APIResponse::<CategorizedPrioritizedTransactions>::error(
                                Some(e.to_string()),
                            )),
                        );
                    }
                }
            }
        };
        let response = categorize_prioritized_transactions(
            transactions,
            &crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.snapshot_txids(),
            &crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.snapshot_txids(),
        );

        (StatusCode::OK, Json(APIResponse::success(Some(response))))
    }

    /// One page of declarations, newest first.
    pub async fn get_job_history(
        State(state): State<AppState>,
        headers: HeaderMap,
        Query(params): Query<std::collections::HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }
        let Some(db) = crate::db::pool() else {
            return no_database();
        };
        let page = params
            .get("page")
            .and_then(|p| p.parse::<i64>().ok())
            .unwrap_or(1);
        let per_page = params
            .get("per_page")
            .and_then(|p| p.parse::<i64>().ok())
            .unwrap_or(10);

        match history::page(db, page, per_page).await {
            Ok(response) => (StatusCode::OK, Json(APIResponse::success(Some(response)))),
            Err(e) => internal(format!("Failed to get job history: {e}")),
        }
    }

    /// One page of stored prioritizations, newest first.
    pub async fn get_prioritized_history(
        State(state): State<AppState>,
        headers: HeaderMap,
        Query(params): Query<std::collections::HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }
        let Some(db) = crate::db::pool() else {
            return no_database();
        };
        let page = params
            .get("page")
            .and_then(|p| p.parse::<i64>().ok())
            .unwrap_or(1);
        let per_page = params
            .get("per_page")
            .and_then(|p| p.parse::<i64>().ok())
            .unwrap_or(10);

        match prioritized::page(db, page, per_page).await {
            Ok(response) => (StatusCode::OK, Json(APIResponse::success(Some(response)))),
            Err(e) => internal(format!("Failed to get the prioritization history: {e}")),
        }
    }

    /// The transactions in the latest declaration of a template.
    pub async fn get_job_txids(
        State(state): State<AppState>,
        headers: HeaderMap,
        Path(template_id): Path<i64>,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }
        let Some(db) = crate::db::pool() else {
            return no_database();
        };
        let txids = match history::txids(db, template_id).await {
            Ok(txids) => txids,
            Err(e) => return internal(format!("Failed to get job txids: {e}")),
        };

        (
            StatusCode::OK,
            Json(APIResponse::success(Some(json!({
                "template_id": template_id,
                "total": txids.len(),
                "txids": txids,
            })))),
        )
    }

    /// How many blocks of history the proxy is keeping.
    pub async fn get_history_retention(
        State(state): State<AppState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }
        let Some(db) = crate::db::pool() else {
            return no_database();
        };
        (
            StatusCode::OK,
            Json(APIResponse::success(Some(json!({
                "keep_blocks": history::keep_blocks(db).await,
                "default_keep_blocks": history::default_keep_blocks(),
            })))),
        )
    }

    /// Set retention; null keeps everything.
    pub async fn set_history_retention(
        State(state): State<AppState>,
        headers: HeaderMap,
        Json(request): Json<HistoryRetentionRequest>,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }
        let Some(db) = crate::db::pool() else {
            return no_database();
        };
        if let Some(keep) = request.keep_blocks {
            if keep < 1 {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(APIResponse::error(Some(
                        "keep_blocks must be at least 1, or null to keep everything".to_string(),
                    ))),
                );
            }
        }
        match history::set_keep_blocks(db, request.keep_blocks).await {
            Ok(()) => {
                info!(keep_blocks = ?request.keep_blocks, "history retention set");
                (
                    StatusCode::OK,
                    Json(APIResponse::success(Some(
                        json!({ "keep_blocks": request.keep_blocks }),
                    ))),
                )
            }
            Err(e) => internal(format!("Failed to set the history retention: {e}")),
        }
    }

    /// Delete the whole history.
    pub async fn clear_job_history(
        State(state): State<AppState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        if let Some(refusal) = check_authorization(&state, &headers) {
            return refusal;
        }
        let Some(db) = crate::db::pool() else {
            return no_database();
        };
        match history::clear(db).await {
            Ok(removed) => (
                StatusCode::OK,
                Json(APIResponse::success(Some(json!({ "removed": removed })))),
            ),
            Err(e) => internal(format!("Failed to clear the job history: {e}")),
        }
    }
}

#[derive(Serialize)]
struct CategorizedPrioritizedTransactions {
    mempool_space: BTreeMap<String, PrioTx>,
    manually_prioritized: BTreeMap<String, PrioTx>,
    unknown: BTreeMap<String, PrioTx>,
}

fn categorize_prioritized_transactions(
    transactions: HashMap<Txid, PrioTx>,
    mempool_space_txids: &HashSet<Txid>,
    manually_prioritized_txids: &HashSet<Txid>,
) -> CategorizedPrioritizedTransactions {
    let mut categorized = CategorizedPrioritizedTransactions {
        mempool_space: BTreeMap::new(),
        manually_prioritized: BTreeMap::new(),
        unknown: BTreeMap::new(),
    };

    for (txid, transaction) in transactions {
        let category = if mempool_space_txids.contains(&txid) {
            &mut categorized.mempool_space
        } else if manually_prioritized_txids.contains(&txid) {
            &mut categorized.manually_prioritized
        } else {
            &mut categorized.unknown
        };
        category.insert(txid.to_string(), transaction);
    }

    categorized
}

fn is_authorized_for_tx_prioritization(headers: &HeaderMap, expected_token: &str) -> bool {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == expected_token)
}

/// Checks if the request is authorized based on the provided headers and the expected API token.
fn check_authorization<T: Serialize>(
    state: &AppState,
    headers: &HeaderMap,
) -> Option<(StatusCode, Json<APIResponse<T>>)> {
    let Some(expected_token) = state.api_tx_token.as_deref() else {
        warn!("API TX TOKEN NOT SET");
        return Some((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(APIResponse::error(Some("API TX TOKEN NOT SET".to_string()))),
        ));
    };

    if !is_authorized_for_tx_prioritization(headers, expected_token) {
        warn!("unauthorized API request");
        return Some((
            StatusCode::UNAUTHORIZED,
            Json(APIResponse::error(Some("Unauthorized".to_string()))),
        ));
    }

    None
}

#[derive(Serialize)]
struct AggregateStates {
    total_connected_device: u32,
    aggregate_hashrate: f64, // f64 is used here to avoid overflow
    aggregate_accepted_shares: u64,
    aggregate_rejected_shares: u64,
    aggregate_diff: f64,
}

#[derive(Debug, Serialize)]
pub struct APIResponse<T> {
    success: bool,
    message: Option<String>,
    data: Option<T>,
}

/// Shared 503 for database-backed endpoints.
fn no_database<T: Serialize>() -> (StatusCode, Json<APIResponse<T>>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(APIResponse::error(Some(
            "Database not available".to_string(),
        ))),
    )
}

fn internal<T: Serialize>(message: String) -> (StatusCode, Json<APIResponse<T>>) {
    error!(%message);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(APIResponse::error(Some(message))),
    )
}

impl<T: Serialize> APIResponse<T> {
    pub fn success(data: Option<T>) -> Self {
        APIResponse {
            success: true,
            message: None,
            data,
        }
    }

    pub fn error(message: Option<String>) -> Self {
        APIResponse {
            success: false,
            message,
            data: None,
        }
    }
}

#[tokio::test]
async fn health_check_reports_full_translator_handoff() {
    use axum::extract::State;
    use axum::response::IntoResponse;
    use std::{net::IpAddr, time::Instant};
    use tokio::sync::mpsc;

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let (send_to_upstream, recv_from_downstream) = mpsc::channel(1);

    handoff_tx
        .try_send(crate::DownstreamConnection {
            send_to_downstream: send_to_upstream,
            recv_from_downstream,
            address: IpAddr::from([127, 0, 0, 1]),
            accepted_at: Instant::now(),
        })
        .expect("test handoff queue should accept first item");

    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        api_tx_token: Some("api-token".to_string()),
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                "http://127.0.0.1:8332".to_string(),
                "user".to_string(),
                "password".to_string(),
            )),
        }),
    };

    let response = Api::health_check(State(state)).await.into_response();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn send_tx_reports_unavailable_when_rpc_is_disabled() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use tokio::sync::mpsc;

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);

    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        api_tx_token: Some("api-token".to_string()),
        prioritizing_txs: None,
    };

    let response = Api::prioritize_transaction(
        State(state),
        axum::http::HeaderMap::new(),
        Path(("00".repeat(32).parse().expect("valid txid"), 42)),
    )
    .await
    .into_response();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn send_tx_rejects_missing_api_tx_token_header() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use tokio::sync::mpsc;

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        api_tx_token: Some("api-token".to_string()),
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                "http://127.0.0.1:8332".to_string(),
                "user".to_string(),
                "password".to_string(),
            )),
        }),
    };

    let response = Api::prioritize_transaction(
        State(state),
        axum::http::HeaderMap::new(),
        Path(("00".repeat(32).parse().expect("valid txid"), 42)),
    )
    .await
    .into_response();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn prioritize_transaction_rejects_a_cached_bad_transaction() {
    use axum::body::to_bytes;
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use tokio::sync::mpsc;

    let txid = "00000000000000000000000000000000000000000000000000000000000000b1"
        .parse()
        .expect("valid test txid");
    crate::prioritized_transactions::BAD_TRANSACTIONS.remove(&txid);
    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&txid);
    crate::prioritized_transactions::BAD_TRANSACTIONS.record(txid, 75_000);

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);
    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        api_tx_token: Some("api-token".to_string()),
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                "http://127.0.0.1:1".to_string(),
                "user".to_string(),
                "password".to_string(),
            )),
        }),
    };
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    let response = Api::prioritize_transaction(State(state), headers, Path((txid, 42)))
        .await
        .into_response();

    crate::prioritized_transactions::BAD_TRANSACTIONS.remove(&txid);
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.get(&txid),
        None
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["success"], false);
    assert_eq!(
        body["message"],
        format!(
            "Transaction acceleration was unsuccessful: {txid} was previously rejected by bitcoind"
        )
    );
    assert_eq!(body["data"], serde_json::Value::Null);
}

#[tokio::test]
async fn successful_prioritization_records_the_applied_fee_delta() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};
    use tokio::sync::mpsc;

    async fn mock_bitcoind(Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
        assert_eq!(body["method"], "prioritisetransaction");
        (
            StatusCode::OK,
            Json(json!({
                "result": true,
                "error": null,
                "id": "dmnd-client"
            })),
        )
    }

    let txid = "0000000000000000000000000000000000000000000000000000000000000003"
        .parse()
        .expect("valid test txid");
    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&txid);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server local addr");
    let app = Router::new().route("/bitcoin", post(mock_bitcoind));
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);
    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        api_tx_token: Some("api-token".to_string()),
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                format!("http://{addr}/bitcoin"),
                "user".to_string(),
                "password".to_string(),
            )),
        }),
    };
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    let response = Api::prioritize_transaction(State(state), headers, Path((txid, 42)))
        .await
        .into_response();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS
            .snapshot()
            .get(&txid),
        Some(&42)
    );

    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&txid);
    server.abort();
}

#[tokio::test]
async fn successful_restore_reverses_and_removes_all_cached_fee_deltas() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;
    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct MockBitcoindState {
        requests: mpsc::UnboundedSender<Value>,
    }

    async fn mock_bitcoind(
        State(state): State<MockBitcoindState>,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state
            .requests
            .send(body)
            .expect("test should receive bitcoind request");
        (
            StatusCode::OK,
            Json(json!({
                "result": true,
                "error": null,
                "id": "dmnd-client"
            })),
        )
    }

    let txid = "0000000000000000000000000000000000000000000000000000000000000004"
        .parse()
        .expect("valid test txid");
    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&txid);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.remove(&txid);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server local addr");
    let (requests, mut received_requests) = mpsc::unbounded_channel();
    let app = Router::new()
        .route("/bitcoin", post(mock_bitcoind))
        .with_state(MockBitcoindState { requests });
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);
    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        api_tx_token: Some("api-token".to_string()),
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                format!("http://{addr}/bitcoin"),
                "user".to_string(),
                "password".to_string(),
            )),
        }),
    };
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.record(txid, 100_000_000);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.record(txid, 25_000_000);

    let response = Api::restore_tx_priority(State(state), headers, Path(txid))
        .await
        .into_response();

    assert_eq!(response.status(), StatusCode::OK);

    let manual_restore_request = received_requests
        .recv()
        .await
        .expect("manual prioritisetransaction restore request");
    assert_eq!(manual_restore_request["method"], "prioritisetransaction");
    assert_eq!(
        manual_restore_request["params"],
        json!([txid, 0, -100_000_000])
    );

    let accelerator_restore_request = received_requests
        .recv()
        .await
        .expect("accelerator prioritisetransaction restore request");
    assert_eq!(
        accelerator_restore_request["method"],
        "prioritisetransaction"
    );
    assert_eq!(
        accelerator_restore_request["params"],
        json!([txid, 0, -25_000_000])
    );
    assert_eq!(
        crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.get(&txid),
        None
    );
    assert_eq!(
        crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.get(&txid),
        None
    );
    assert!(received_requests.try_recv().is_err());

    server.abort();
}

#[tokio::test]
async fn get_prioritized_transactions_returns_categorized_bitcoind_snapshot() {
    use axum::body::to_bytes;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::{routing::post, Json, Router};
    use bitcoin::Txid;
    use serde_json::{json, Value};
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct MockBitcoindState {
        requests: mpsc::UnboundedSender<Value>,
    }

    async fn mock_bitcoind(
        State(state): State<MockBitcoindState>,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state
            .requests
            .send(body)
            .expect("test should receive bitcoind request");
        (
            StatusCode::OK,
            Json(json!({
                "result": {
                    "00000000000000000000000000000000000000000000000000000000000000a1": {
                        "fee_delta": 100_000,
                        "in_mempool": true,
                        "modified_fee": 110_000
                    },
                    "00000000000000000000000000000000000000000000000000000000000000a2": {
                        "fee_delta": 200_000,
                        "in_mempool": false
                    },
                    "00000000000000000000000000000000000000000000000000000000000000a3": {
                        "fee_delta": -300_000,
                        "in_mempool": true,
                        "modified_fee": 30_000
                    }
                },
                "error": null,
                "id": "dmnd-client"
            })),
        )
    }

    let manual_txid: Txid = "00000000000000000000000000000000000000000000000000000000000000a1"
        .parse()
        .expect("valid test txid");
    let mempool_space_txid: Txid =
        "00000000000000000000000000000000000000000000000000000000000000a2"
            .parse()
            .expect("valid test txid");
    let unknown_txid: Txid = "00000000000000000000000000000000000000000000000000000000000000a3"
        .parse()
        .expect("valid test txid");

    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&manual_txid);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.remove(&mempool_space_txid);
    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.record(manual_txid, 100_000);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED
        .record(mempool_space_txid, 200_000);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server local addr");
    let (requests, mut received_requests) = mpsc::unbounded_channel();
    let app = Router::new()
        .route("/", post(mock_bitcoind))
        .with_state(MockBitcoindState { requests });
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    let router = crate::router::Router::new(vec![], auth_pub_k, None, None);

    let (handoff_tx, _handoff_rx) = mpsc::channel(1);
    let state = AppState {
        router,
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        api_tx_token: Some("api-token".to_string()),
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                format!("http://{addr}"),
                "user".to_string(),
                "password".to_string(),
            )),
        }),
    };

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    let response = Api::get_prioritized_transactions(State(state), headers)
        .await
        .into_response();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["success"], true);
    assert_eq!(
        body["data"],
        json!({
            "mempool_space": {
                mempool_space_txid.to_string(): {
                    "fee_delta": 200_000,
                    "in_mempool": false
                }
            },
            "manually_prioritized": {
                manual_txid.to_string(): {
                    "fee_delta": 100_000,
                    "in_mempool": true,
                    "modified_fee": 110_000
                }
            },
            "unknown": {
                unknown_txid.to_string(): {
                    "fee_delta": -300_000,
                    "in_mempool": true,
                    "modified_fee": 30_000
                }
            }
        })
    );
    let request = received_requests
        .recv()
        .await
        .expect("getprioritisedtransactions request");
    assert_eq!(request["method"], "getprioritisedtransactions");
    assert_eq!(request["params"], json!([]));
    assert!(received_requests.try_recv().is_err());

    crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&manual_txid);
    crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.remove(&mempool_space_txid);
    server.abort();
}

#[test]
fn tx_prioritization_auth_accepts_matching_bearer_token() {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, "Bearer api-token".parse().unwrap());

    assert!(is_authorized_for_tx_prioritization(&headers, "api-token"));
}

#[cfg(test)]
fn declaration_test_state(api_tx_token: Option<&str>) -> AppState {
    let (handoff_tx, _handoff_rx) = tokio::sync::mpsc::channel(1);
    let auth_pub_k = crate::AUTH_PUB_KEY.parse().expect("Invalid public key");
    AppState {
        router: crate::router::Router::new(vec![], auth_pub_k, None, None),
        stats_sender: crate::api::stats::StatsSender::new(),
        downstream_handoff: handoff_tx,
        api_tx_token: api_tx_token.map(str::to_string),
        prioritizing_txs: Some(super::PrioritizingTxs {
            rpc: std::sync::Arc::new(crate::api::bitcoin_rpc::BitcoindRpc::new(
                "http://127.0.0.1:8332".to_string(),
                "user".to_string(),
                "password".to_string(),
            )),
        }),
    }
}

#[cfg(test)]
fn bearer(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
    headers
}

#[tokio::test]
async fn merge_mining_status_requires_dashboard_authorization() {
    for (token, headers, expected, message) in [
        (
            None,
            bearer("api-token"),
            StatusCode::SERVICE_UNAVAILABLE,
            Some("API TX TOKEN NOT SET"),
        ),
        (
            Some("api-token"),
            HeaderMap::new(),
            StatusCode::UNAUTHORIZED,
            Some("Unauthorized"),
        ),
        (
            Some("api-token"),
            bearer("wrong"),
            StatusCode::UNAUTHORIZED,
            Some("Unauthorized"),
        ),
        (Some("api-token"), bearer("api-token"), StatusCode::OK, None),
    ] {
        let response = Api::get_merge_mining_status(State(declaration_test_state(token)), headers)
            .await
            .into_response();
        assert_eq!(response.status(), expected);
        // The route answers 503 both for an unset token and for unreadable
        // merge-mining state, so pin down which refusal this is.
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(body["message"].as_str(), message);
    }
}

// The token gates the templates a caller can read and the criterion it can
// change, so all three routes refuse the same way.
#[tokio::test]
async fn job_declaration_routes_require_the_bearer_token() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;

    let wrong_token = bearer("not-the-token");
    for headers in [HeaderMap::new(), wrong_token] {
        let state = declaration_test_state(Some("api-token"));
        assert_eq!(
            Api::get_recent_templates(State(state.clone()), headers.clone())
                .await
                .into_response()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            Api::get_template_by_id(State(state.clone()), headers.clone(), Path(1))
                .await
                .into_response()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            Api::set_declaration_policy(
                State(state),
                headers,
                Json(DeclarationPolicyRequest {
                    policy: "block_weight".to_string(),
                }),
            )
            .await
            .into_response()
            .status(),
            StatusCode::UNAUTHORIZED
        );
    }
}

// A proxy started without a token does not serve these routes at all, rather
// than serving them to anyone.
#[tokio::test]
async fn job_declaration_routes_are_disabled_without_the_api_tx_token() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;

    let state = declaration_test_state(None);
    let headers = bearer("api-token");

    assert_eq!(
        Api::get_recent_templates(State(state.clone()), headers.clone())
            .await
            .into_response()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        Api::get_template_by_id(State(state.clone()), headers.clone(), Path(1))
            .await
            .into_response()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        Api::set_declaration_policy(
            State(state),
            headers,
            Json(DeclarationPolicyRequest {
                policy: "block_weight".to_string(),
            }),
        )
        .await
        .into_response()
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

// The matching token gets through with no bitcoind RPC configured, and a
// missing template is then a 404, not a 401 or a 503.
#[tokio::test]
async fn the_configured_token_reaches_the_job_declaration_routes() {
    use axum::extract::{Path, State};
    use axum::response::IntoResponse;

    let state = declaration_test_state(Some("api-token"));
    let headers = bearer("api-token");

    assert_eq!(
        Api::get_recent_templates(State(state.clone()), headers.clone())
            .await
            .into_response()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        Api::set_declaration_policy(
            State(state.clone()),
            headers.clone(),
            Json(DeclarationPolicyRequest {
                policy: "block_weight".to_string(),
            }),
        )
        .await
        .into_response()
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        Api::get_template_by_id(State(state), headers, Path(u64::MAX))
            .await
            .into_response()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn history_routes_require_dashboard_authorization() {
    for headers in [HeaderMap::new(), bearer("wrong")] {
        let state = declaration_test_state(Some("api-token"));
        let responses = [
            Api::get_job_history(State(state.clone()), headers.clone(), Query(HashMap::new()))
                .await
                .into_response(),
            Api::get_job_txids(State(state.clone()), headers.clone(), Path(1))
                .await
                .into_response(),
            Api::get_history_retention(State(state.clone()), headers.clone())
                .await
                .into_response(),
            Api::set_history_retention(
                State(state.clone()),
                headers.clone(),
                Json(HistoryRetentionRequest {
                    keep_blocks: Some(5),
                }),
            )
            .await
            .into_response(),
            Api::clear_job_history(State(state), headers)
                .await
                .into_response(),
        ];
        for response in responses {
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }
}
