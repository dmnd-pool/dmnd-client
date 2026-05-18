use reqwest::Url;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Mutex as StdMutex, OnceLock},
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};

use crate::{
    config::Configuration, monitor::worker_summary::WorkerSummary, shared::error::Error, LOCAL_URL,
    PRODUCTION_URL, STAGING_URL, TESTNET3_URL,
};

pub mod shares;
pub mod worker_summary;

#[derive(Clone)]
pub struct MonitorAPI {
    pub url: Url,
    pub client: reqwest::Client,
}

#[derive(Default)]
struct WorkerSummaryDispatcherState {
    started: bool,
    session_keys: HashMap<u32, WorkerSummaryKey>,
    workers: HashMap<WorkerSummaryKey, WorkerSummaryAccumulator>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WorkerSummaryKey {
    token: String,
    worker_name: String,
}

#[derive(Debug)]
struct ValidShareSample {
    difficulty: f64,
    recorded_at: Instant,
}

#[derive(Debug, Default)]
struct WorkerSummaryAccumulator {
    active_session_count: usize,
    total_valid_shares: u64,
    total_invalid_shares: u64,
    disconnected_at: Option<Instant>,
    zero_hashrate_since: Option<Instant>,
    valid_shares_window: VecDeque<ValidShareSample>,
}

#[derive(serde::Serialize)]
struct WorkerSummaryPayload<'a> {
    entries: &'a [WorkerSummary],
    token: &'a str,
}

static MONITOR_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
static WORKER_SUMMARY_ENDPOINT: OnceLock<Url> = OnceLock::new();
static WORKER_SUMMARY_DISPATCHER: OnceLock<StdMutex<WorkerSummaryDispatcherState>> =
    OnceLock::new();

const MONITOR_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const MONITOR_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const WORKER_SUMMARY_INTERVAL: Duration = Duration::from_secs(300);
const WORKER_HASHRATE_WINDOW: Duration = Duration::from_secs(300);
const ZERO_HASHRATE_CONNECTED_TIMEOUT: Duration = Duration::from_secs(600);
const WORKER_SUMMARY_RETENTION: Duration = Duration::from_secs(300);
const WORKER_SUMMARIES_PER_REQUEST: usize = 256;

fn shared_client() -> reqwest::Client {
    MONITOR_CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .connect_timeout(MONITOR_CONNECT_TIMEOUT)
                .build()
                .expect("failed to build monitor client")
        })
        .clone()
}

fn worker_summary_server_endpoint() -> String {
    // Send live worker telemetry through the dashboard's primary worker-entry
    // ingestion path.
    match Configuration::environment().as_str() {
        "staging" => format!("{STAGING_URL}/api/worker/entry"),
        "testnet3" => format!("{TESTNET3_URL}/api/worker/entry"),
        "local" => format!("{LOCAL_URL}/api/worker/entry"),
        "production" => format!("{PRODUCTION_URL}/api/worker/entry"),
        _ => unreachable!(),
    }
}

impl MonitorAPI {
    pub fn worker_summary() -> Self {
        MonitorAPI {
            url: WORKER_SUMMARY_ENDPOINT
                .get_or_init(|| {
                    worker_summary_server_endpoint()
                        .parse()
                        .expect("Invalid worker summary URL")
                })
                .clone(),
            client: shared_client(),
        }
    }

    fn worker_summary_dispatcher() -> &'static StdMutex<WorkerSummaryDispatcherState> {
        WORKER_SUMMARY_DISPATCHER
            .get_or_init(|| StdMutex::new(WorkerSummaryDispatcherState::default()))
    }

    fn ensure_worker_summary_dispatcher() -> bool {
        if tokio::runtime::Handle::try_current().is_err() {
            warn!("Skipping worker summary dispatch because no Tokio runtime is active");
            return false;
        }

        let dispatcher = Self::worker_summary_dispatcher();
        let mut state = match dispatcher.lock() {
            Ok(state) => state,
            Err(e) => {
                error!("Failed to lock worker summary dispatcher: {e}");
                return false;
            }
        };

        if state.started {
            return true;
        }
        state.started = true;
        drop(state);

        tokio::spawn(async move {
            let api = MonitorAPI::worker_summary();
            let mut interval = tokio::time::interval(WORKER_SUMMARY_INTERVAL);
            interval.tick().await;

            loop {
                interval.tick().await;
                let summaries_by_token = {
                    let dispatcher = MonitorAPI::worker_summary_dispatcher();
                    match dispatcher.lock() {
                        Ok(mut state) => state.snapshot_connected_worker_summaries(Instant::now()),
                        Err(e) => {
                            error!("Failed to lock worker summary dispatcher: {e}");
                            return;
                        }
                    }
                };

                if summaries_by_token.is_empty() {
                    debug!("No connected workers to summarize");
                    continue;
                }

                for (token, summaries) in summaries_by_token {
                    let total = summaries.len();
                    let mut sent = 0usize;

                    for chunk in summaries.chunks(WORKER_SUMMARIES_PER_REQUEST) {
                        match api.send_worker_summaries(chunk, &token).await {
                            Ok(_) => sent += chunk.len(),
                            Err(err) => warn!(
                                "Failed to send worker summary chunk of {}, will retry on the next interval: {:?}",
                                chunk.len(),
                                err
                            ),
                        }
                    }

                    if sent > 0 {
                        info!(
                            "Saved {}/{} worker summaries to the monitoring server",
                            sent, total
                        );
                    }
                }
            }
        });

        true
    }

    pub fn worker_connected(connection_id: u32, worker_name: String, token: String) {
        if !Self::ensure_worker_summary_dispatcher() {
            return;
        }

        let dispatcher = Self::worker_summary_dispatcher();
        match dispatcher.lock() {
            Ok(mut state) => state.worker_connected(
                connection_id,
                WorkerSummaryKey { token, worker_name },
                Instant::now(),
            ),
            Err(e) => error!("Failed to lock worker summary dispatcher: {e}"),
        }
    }

    pub fn worker_disconnected(connection_id: u32) {
        if !Self::ensure_worker_summary_dispatcher() {
            return;
        }

        let dispatcher = Self::worker_summary_dispatcher();
        match dispatcher.lock() {
            Ok(mut state) => state.worker_disconnected(connection_id, Instant::now()),
            Err(e) => error!("Failed to lock worker summary dispatcher: {e}"),
        }
    }

    pub fn record_share(
        connection_id: u32,
        worker_name: String,
        token: String,
        difficulty: Option<f32>,
    ) {
        if !Self::ensure_worker_summary_dispatcher() {
            return;
        }

        let dispatcher = Self::worker_summary_dispatcher();
        match dispatcher.lock() {
            Ok(mut state) => state.record_share(
                connection_id,
                worker_name,
                token,
                difficulty,
                Instant::now(),
            ),
            Err(e) => error!("Failed to lock worker summary dispatcher: {e}"),
        }
    }

    #[cfg(test)]
    pub(crate) fn worker_summary_totals(token: &str, worker_name: &str) -> Option<(u64, u64)> {
        let dispatcher = Self::worker_summary_dispatcher();
        let state = dispatcher.lock().ok()?;
        let key = WorkerSummaryKey {
            token: token.to_string(),
            worker_name: worker_name.to_string(),
        };
        state
            .workers
            .get(&key)
            .map(|worker| (worker.total_valid_shares, worker.total_invalid_shares))
    }

    async fn send_worker_summaries(
        &self,
        summaries: &[WorkerSummary],
        token: &str,
    ) -> Result<(), Error> {
        debug!("Sending {} worker summaries to API", summaries.len());
        let response = self
            .client
            .post(self.url.clone())
            .timeout(MONITOR_REQUEST_TIMEOUT)
            .json(&WorkerSummaryPayload {
                entries: summaries,
                token,
            })
            .send()
            .await?;

        match response.error_for_status() {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("Failed to send worker summaries: {}", err);
                Err(err.into())
            }
        }
    }
}

impl WorkerSummaryDispatcherState {
    fn worker_connected(&mut self, connection_id: u32, key: WorkerSummaryKey, now: Instant) {
        if let Some(existing_key) = self.session_keys.insert(connection_id, key.clone()) {
            if existing_key == key {
                return;
            }

            warn!(
                "Worker summary session {connection_id} changed worker key from `{}` to `{}` without disconnect; resetting local state",
                existing_key.worker_name,
                key.worker_name,
            );
            self.mark_worker_disconnected(&existing_key, now);
        }

        self.workers.entry(key).or_default().mark_connected(now);
    }

    fn worker_disconnected(&mut self, connection_id: u32, now: Instant) {
        let Some(key) = self.session_keys.remove(&connection_id) else {
            return;
        };

        self.mark_worker_disconnected(&key, now);
    }

    fn record_share(
        &mut self,
        connection_id: u32,
        worker_name: String,
        token: String,
        difficulty: Option<f32>,
        now: Instant,
    ) {
        let key = self.connected_key_for_share(connection_id, worker_name, token, now);

        let worker = self.workers.entry(key).or_default();
        match difficulty {
            Some(difficulty) => worker.record_valid_share(now, difficulty),
            None => worker.record_invalid_share(),
        }
    }

    fn connected_key_for_share(
        &mut self,
        connection_id: u32,
        worker_name: String,
        token: String,
        now: Instant,
    ) -> WorkerSummaryKey {
        let key = WorkerSummaryKey { token, worker_name };

        match self.session_keys.get(&connection_id).cloned() {
            Some(existing_key) if existing_key.token == key.token => existing_key,
            Some(existing_key) => {
                warn!(
                    "Worker summary session {connection_id} recorded a share for token `{}` after being registered to token `{}`; resetting local state",
                    key.token,
                    existing_key.token,
                );
                self.mark_worker_disconnected(&existing_key, now);
                self.session_keys.insert(connection_id, key.clone());
                self.workers
                    .entry(key.clone())
                    .or_default()
                    .mark_connected(now);
                key
            }
            None => {
                self.session_keys.insert(connection_id, key.clone());
                self.workers
                    .entry(key.clone())
                    .or_default()
                    .mark_connected(now);
                key
            }
        }
    }

    fn snapshot_connected_worker_summaries(
        &mut self,
        now: Instant,
    ) -> HashMap<String, Vec<WorkerSummary>> {
        self.workers.retain(|_, worker| {
            worker.prune_valid_shares(now);
            !worker.should_evict(now)
        });
        self.expire_stale_zero_hashrate_sessions(now);

        let mut summaries_by_token: HashMap<String, Vec<WorkerSummary>> = HashMap::new();
        for (key, worker) in &self.workers {
            if !worker.is_connected() {
                continue;
            }

            summaries_by_token
                .entry(key.token.clone())
                .or_default()
                .push(WorkerSummary::new(
                    key.worker_name.clone(),
                    worker.hashrate(),
                    worker.total_valid_shares,
                    worker.total_invalid_shares,
                ));
        }

        for summaries in summaries_by_token.values_mut() {
            summaries.sort_by(|left, right| left.worker_name().cmp(right.worker_name()));
        }

        summaries_by_token
    }

    fn mark_worker_disconnected(&mut self, key: &WorkerSummaryKey, now: Instant) {
        if let Some(worker) = self.workers.get_mut(key) {
            worker.mark_disconnected(now);
        }
    }

    fn expire_stale_zero_hashrate_sessions(&mut self, now: Instant) {
        let expired_keys: Vec<WorkerSummaryKey> = self
            .workers
            .iter_mut()
            .filter_map(|(key, worker)| {
                if worker.should_force_disconnect_zero_hashrate(now) {
                    warn!(
                        "Expiring connected worker `{}` from monitoring after zero hashrate for {} seconds",
                        key.worker_name,
                        ZERO_HASHRATE_CONNECTED_TIMEOUT.as_secs(),
                    );
                    worker.force_disconnected(now);
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        if expired_keys.is_empty() {
            return;
        }

        self.session_keys
            .retain(|_, key| !expired_keys.iter().any(|expired| expired == key));
    }
}

impl WorkerSummaryAccumulator {
    fn mark_connected(&mut self, now: Instant) {
        self.active_session_count = self.active_session_count.saturating_add(1);
        self.disconnected_at = None;
        if self.valid_shares_window.is_empty() && self.zero_hashrate_since.is_none() {
            self.zero_hashrate_since = Some(now);
        }
    }

    fn mark_disconnected(&mut self, now: Instant) {
        self.active_session_count = self.active_session_count.saturating_sub(1);
        if self.active_session_count == 0 {
            self.disconnected_at = Some(now);
            self.zero_hashrate_since = None;
        }
    }

    fn record_valid_share(&mut self, now: Instant, difficulty: f32) {
        self.total_valid_shares = self.total_valid_shares.saturating_add(1);
        self.prune_valid_shares(now);

        if difficulty.is_finite() && difficulty > 0.0 {
            self.valid_shares_window.push_back(ValidShareSample {
                difficulty: difficulty as f64,
                recorded_at: now,
            });
            self.zero_hashrate_since = None;
        } else {
            warn!(
                "Skipping worker hashrate contribution for invalid share difficulty: {}",
                difficulty
            );
        }
    }

    fn record_invalid_share(&mut self) {
        self.total_invalid_shares = self.total_invalid_shares.saturating_add(1);
    }

    fn prune_valid_shares(&mut self, now: Instant) {
        let mut zero_hashrate_started_at = None;
        while let Some(front) = self.valid_shares_window.front() {
            if now.duration_since(front.recorded_at) <= WORKER_HASHRATE_WINDOW {
                break;
            }
            zero_hashrate_started_at = Some(front.recorded_at + WORKER_HASHRATE_WINDOW);
            self.valid_shares_window.pop_front();
        }
        if self.valid_shares_window.is_empty() {
            if let Some(started_at) = zero_hashrate_started_at {
                self.zero_hashrate_since = Some(started_at);
            }
        } else {
            self.zero_hashrate_since = None;
        }
    }

    fn is_connected(&self) -> bool {
        self.active_session_count > 0
    }

    fn hashrate(&self) -> f64 {
        let total_difficulty: f64 = self
            .valid_shares_window
            .iter()
            .map(|share| share.difficulty)
            .sum();
        total_difficulty * 2.0_f64.powi(32) / WORKER_HASHRATE_WINDOW.as_secs_f64()
    }

    fn should_evict(&self, now: Instant) -> bool {
        !self.is_connected()
            && self
                .disconnected_at
                .is_some_and(|at| now.duration_since(at) >= WORKER_SUMMARY_RETENTION)
    }

    fn should_force_disconnect_zero_hashrate(&self, now: Instant) -> bool {
        self.is_connected()
            && self.valid_shares_window.is_empty()
            && self
                .zero_hashrate_since
                .is_some_and(|at| now.duration_since(at) >= ZERO_HASHRATE_CONNECTED_TIMEOUT)
    }

    fn force_disconnected(&mut self, now: Instant) {
        self.active_session_count = 0;
        self.disconnected_at = Some(now);
        self.zero_hashrate_since = None;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        shared_client, MonitorAPI, WorkerSummaryDispatcherState, WorkerSummaryKey,
        WorkerSummaryPayload, MONITOR_REQUEST_TIMEOUT, WORKER_HASHRATE_WINDOW,
        WORKER_SUMMARY_RETENTION, ZERO_HASHRATE_CONNECTED_TIMEOUT,
    };
    use crate::{monitor::worker_summary::WorkerSummary, shared::error::Error};
    use std::time::{Duration, Instant};
    use tokio::net::TcpListener;

    fn key(token: &str, worker_name: &str) -> WorkerSummaryKey {
        WorkerSummaryKey {
            token: token.to_string(),
            worker_name: worker_name.to_string(),
        }
    }

    #[test]
    fn worker_summary_dispatcher_aggregates_sessions_per_worker() {
        let mut state = WorkerSummaryDispatcherState::default();
        let now = Instant::now();
        state.worker_connected(1, key("token", "worker-1"), now);
        state.worker_connected(2, key("token", "worker-1"), now);

        state.record_share(
            1,
            "worker-1".to_string(),
            "token".to_string(),
            Some(16.0),
            now,
        );
        state.record_share(2, "worker-1".to_string(), "token".to_string(), None, now);

        let summaries = state.snapshot_connected_worker_summaries(now);
        let token_summaries = summaries.get("token").expect("token summary should exist");
        assert_eq!(token_summaries.len(), 1);
        assert_eq!(token_summaries[0].worker_name(), "worker-1");
        assert_eq!(token_summaries[0].total_valid_shares(), 1);
        assert_eq!(token_summaries[0].total_invalid_shares(), 1);
        assert!(token_summaries[0].hashrate() > 0.0);

        state.worker_disconnected(1, now);
        let summaries = state.snapshot_connected_worker_summaries(now);
        assert_eq!(
            summaries
                .get("token")
                .expect("worker should still be connected through session 2")
                .len(),
            1
        );

        state.worker_disconnected(2, now);
        let summaries = state.snapshot_connected_worker_summaries(now);
        assert!(!summaries.contains_key("token"));
    }

    #[test]
    fn worker_summary_dispatcher_hashrate_uses_only_last_five_minutes() {
        let mut state = WorkerSummaryDispatcherState::default();
        let now = Instant::now();
        let worker_key = key("token", "worker-1");
        state.worker_connected(1, worker_key.clone(), now);

        let worker = state
            .workers
            .get_mut(&worker_key)
            .expect("worker should exist");
        worker.record_valid_share(now - WORKER_HASHRATE_WINDOW - Duration::from_secs(1), 100.0);
        worker.record_valid_share(now - Duration::from_secs(20), 2.0);
        worker.record_valid_share(now - Duration::from_secs(10), 3.0);
        worker.record_invalid_share();

        let summaries = state.snapshot_connected_worker_summaries(now);
        let summary = &summaries.get("token").expect("token summary should exist")[0];

        let expected_hashrate =
            (2.0_f64 + 3.0_f64) * 2.0_f64.powi(32) / WORKER_HASHRATE_WINDOW.as_secs_f64();
        assert!((summary.hashrate() - expected_hashrate).abs() < 0.01);
        assert_eq!(summary.total_valid_shares(), 3);
        assert_eq!(summary.total_invalid_shares(), 1);
    }

    #[test]
    fn worker_summary_dispatcher_evicts_disconnected_workers_after_retention() {
        let mut state = WorkerSummaryDispatcherState::default();
        let now = Instant::now();
        state.worker_connected(1, key("token", "worker-1"), now);
        state.worker_disconnected(1, now);

        let summaries = state.snapshot_connected_worker_summaries(now);
        assert!(summaries.is_empty());
        assert_eq!(state.workers.len(), 1);

        let summaries = state.snapshot_connected_worker_summaries(
            now + WORKER_SUMMARY_RETENTION + Duration::from_secs(1),
        );
        assert!(summaries.is_empty());
        assert!(state.workers.is_empty());
    }

    #[test]
    fn worker_summary_dispatcher_expires_connected_zero_hashrate_workers() {
        let mut state = WorkerSummaryDispatcherState::default();
        let now = Instant::now();
        let worker_key = key("token", "worker-1");
        state.worker_connected(1, worker_key.clone(), now);
        state.record_share(
            1,
            "worker-1".to_string(),
            "token".to_string(),
            Some(8.0),
            now,
        );

        let stale_at =
            now + WORKER_HASHRATE_WINDOW + ZERO_HASHRATE_CONNECTED_TIMEOUT + Duration::from_secs(1);
        let summaries = state.snapshot_connected_worker_summaries(stale_at);

        assert!(summaries.is_empty());
        assert!(state.session_keys.is_empty());
        assert!(!state
            .workers
            .get(&worker_key)
            .expect("worker should remain until retention")
            .is_connected());
    }

    #[test]
    fn worker_summary_dispatcher_reconnects_expired_worker_on_later_share() {
        let mut state = WorkerSummaryDispatcherState::default();
        let now = Instant::now();
        let worker_key = key("token", "worker-1");
        state.worker_connected(1, worker_key, now);
        state.record_share(
            1,
            "worker-1".to_string(),
            "token".to_string(),
            Some(8.0),
            now,
        );

        let stale_at =
            now + WORKER_HASHRATE_WINDOW + ZERO_HASHRATE_CONNECTED_TIMEOUT + Duration::from_secs(1);
        assert!(state
            .snapshot_connected_worker_summaries(stale_at)
            .is_empty());

        let resumed_at = stale_at + Duration::from_secs(1);
        state.record_share(
            1,
            "worker-1".to_string(),
            "token".to_string(),
            Some(4.0),
            resumed_at,
        );
        let summaries = state.snapshot_connected_worker_summaries(resumed_at);

        let token_summaries = summaries.get("token").expect("token summary should exist");
        assert_eq!(token_summaries.len(), 1);
        assert_eq!(token_summaries[0].worker_name(), "worker-1");
        assert!(token_summaries[0].hashrate() > 0.0);
    }

    #[test]
    fn worker_summary_payload_matches_dashboard_worker_entry_contract() {
        let payload = WorkerSummaryPayload {
            entries: &[WorkerSummary::new("worker-1".to_string(), 42.5, 7, 2)],
            token: "token",
        };

        let json = serde_json::to_value(&payload).expect("payload should serialize");
        assert_eq!(
            json,
            serde_json::json!({
                "entries": [{
                    "worker_name": "worker-1",
                    "hashrate": 42.5,
                    "valid_shares": 7,
                    "invalid_shares": 2
                }],
                "token": "token"
            })
        );
    }

    #[tokio::test]
    async fn send_worker_summaries_times_out_for_hung_monitor_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have address");
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("listener should accept");
            tokio::time::sleep(MONITOR_REQUEST_TIMEOUT + Duration::from_secs(1)).await;
        });

        let api = MonitorAPI {
            url: format!("http://{addr}/api/worker/entry")
                .parse()
                .expect("url should parse"),
            client: shared_client(),
        };

        let err = api
            .send_worker_summaries(
                &[WorkerSummary::new("worker-1".to_string(), 1.0, 1, 0)],
                "token",
            )
            .await
            .expect_err("hung monitor endpoint should time out");

        match err {
            Error::ReqwestError(err) => {
                assert!(err.is_timeout(), "expected timeout error, got {err}")
            }
        }

        server.abort();
        let _ = server.await;
    }
}
