use reqwest::Url;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Mutex as StdMutex, OnceLock},
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use tracing::{debug, error, warn};

use crate::{
    config::Configuration,
    monitor::{shares::ShareInfo, worker_activity::WorkerActivity},
    shared::error::Error,
    LOCAL_URL, PRODUCTION_URL, STAGING_URL, TESTNET3_URL,
};

pub mod shares;
pub mod worker_activity;
#[derive(Clone)]
pub struct MonitorAPI {
    pub url: Url,
    pub client: reqwest::Client,
}

#[derive(Debug)]
struct WorkerActivityRequest {
    key: WorkerActivityKey,
    activity: WorkerActivity,
}

#[derive(Default)]
struct WorkerActivityDispatcherState {
    started: bool,
    session_keys: HashMap<u32, WorkerActivityKey>,
    active_session_counts: HashMap<WorkerActivityKey, usize>,
    pending_by_key: HashMap<WorkerActivityKey, WorkerActivityRequest>,
    ready_queue: VecDeque<WorkerActivityKey>,
    ready_keys: HashSet<WorkerActivityKey>,
    in_flight_keys: HashSet<WorkerActivityKey>,
    dropped_events_since_log: usize,
    last_drop_log_at: Option<Instant>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WorkerActivityKey {
    token: String,
    worker_name: String,
}

#[derive(serde::Serialize)]
struct SharesRequest<'a> {
    shares: &'a [ShareInfo],
    token: &'a str,
}

#[derive(serde::Serialize)]
struct WorkerActivityPayload<'a> {
    data: &'a WorkerActivity,
    token: &'a str,
}

static MONITOR_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
static SHARES_ENDPOINT: OnceLock<Url> = OnceLock::new();
static WORKER_ACTIVITY_ENDPOINT: OnceLock<Url> = OnceLock::new();
static WORKER_ACTIVITY_DISPATCHER: OnceLock<StdMutex<WorkerActivityDispatcherState>> =
    OnceLock::new();
static WORKER_ACTIVITY_NOTIFY: OnceLock<Notify> = OnceLock::new();

const MONITOR_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const MONITOR_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const WORKER_ACTIVITY_MAX_IN_FLIGHT: usize = 16;
const WORKER_ACTIVITY_MAX_PENDING_WORKERS: usize = 4096;
const WORKER_ACTIVITY_DROP_LOG_INTERVAL: Duration = Duration::from_secs(5);

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

fn shares_server_endpoint() -> String {
    // Determine the monitoring server URL based on the environment
    match Configuration::environment().as_str() {
        "staging" => format!("{STAGING_URL}/api/share/save"),
        "testnet3" => format!("{TESTNET3_URL}/api/share/save"),
        "local" => format!("{LOCAL_URL}/api/share/save"),
        "production" => format!("{PRODUCTION_URL}/api/share/save"),
        _ => unreachable!(),
    }
}

fn worker_activity_server_endpoint() -> String {
    // Determine the monitoring server URL based on the environment
    match Configuration::environment().as_str() {
        "staging" => format!("{STAGING_URL}/api/worker/activity"),
        "testnet3" => format!("{TESTNET3_URL}/api/worker/activity"),
        "local" => format!("{LOCAL_URL}/api/worker/activity"),
        "production" => format!("{PRODUCTION_URL}/api/worker/activity"),
        _ => unreachable!(),
    }
}

impl MonitorAPI {
    fn worker_activity_notify() -> &'static Notify {
        WORKER_ACTIVITY_NOTIFY.get_or_init(Notify::new)
    }

    pub fn shares() -> Self {
        MonitorAPI {
            url: SHARES_ENDPOINT
                .get_or_init(|| {
                    shares_server_endpoint()
                        .parse()
                        .expect("Invalid shares URL")
                })
                .clone(),
            client: shared_client(),
        }
    }

    pub fn worker_activity() -> Self {
        MonitorAPI {
            url: WORKER_ACTIVITY_ENDPOINT
                .get_or_init(|| {
                    worker_activity_server_endpoint()
                        .parse()
                        .expect("Invalid worker activity URL")
                })
                .clone(),
            client: shared_client(),
        }
    }

    fn worker_activity_dispatcher() -> &'static StdMutex<WorkerActivityDispatcherState> {
        WORKER_ACTIVITY_DISPATCHER
            .get_or_init(|| StdMutex::new(WorkerActivityDispatcherState::default()))
    }

    fn ensure_worker_activity_dispatcher() -> bool {
        if tokio::runtime::Handle::try_current().is_err() {
            warn!("Skipping worker activity dispatch because no Tokio runtime is active");
            return false;
        }

        let dispatcher = Self::worker_activity_dispatcher();
        let mut state = match dispatcher.lock() {
            Ok(state) => state,
            Err(e) => {
                error!("Failed to lock worker activity dispatcher: {e}");
                return false;
            }
        };

        if state.started {
            return true;
        }
        state.started = true;
        drop(state);

        tokio::spawn(async move {
            let api = MonitorAPI::worker_activity();
            loop {
                let notified = MonitorAPI::worker_activity_notify().notified();
                let mut launched_work = false;

                while let Some(request) = {
                    let dispatcher = MonitorAPI::worker_activity_dispatcher();
                    match dispatcher.lock() {
                        Ok(mut state) => state.pop_ready_request(),
                        Err(e) => {
                            error!("Failed to lock worker activity dispatcher: {e}");
                            return;
                        }
                    }
                } {
                    launched_work = true;
                    let api = api.clone();
                    tokio::spawn(async move {
                        let key = request.key.clone();
                        if let Err(e) = api.send_worker_activity(request.activity, &key.token).await
                        {
                            error!("Failed to send worker activity: {e}");
                        }

                        let should_notify = {
                            let dispatcher = MonitorAPI::worker_activity_dispatcher();
                            match dispatcher.lock() {
                                Ok(mut state) => state.finish_request(&key),
                                Err(e) => {
                                    error!(
                                        "Failed to lock worker activity dispatcher after send: {e}"
                                    );
                                    return;
                                }
                            }
                        };

                        if should_notify {
                            MonitorAPI::worker_activity_notify().notify_one();
                        }
                    });
                }

                if !launched_work {
                    notified.await;
                }
            }
        });

        true
    }

    pub fn worker_connected(
        connection_id: u32,
        user_agent: String,
        worker_name: String,
        token: String,
    ) {
        Self::queue_worker_activity_for_connection(
            connection_id,
            WorkerActivity::new(
                user_agent,
                worker_name,
                worker_activity::WorkerActivityType::Connected,
            ),
            token,
        );
    }

    pub fn worker_disconnected(connection_id: u32, user_agent: String) {
        if !Self::ensure_worker_activity_dispatcher() {
            return;
        }

        let should_notify = {
            let dispatcher = Self::worker_activity_dispatcher();
            match dispatcher.lock() {
                Ok(mut state) => state.worker_disconnected(connection_id, user_agent),
                Err(e) => {
                    error!("Failed to lock worker activity dispatcher: {e}");
                    return;
                }
            }
        };

        if should_notify {
            Self::worker_activity_notify().notify_one();
        }
    }

    fn queue_worker_activity_for_connection(
        connection_id: u32,
        activity: WorkerActivity,
        token: String,
    ) {
        if !Self::ensure_worker_activity_dispatcher() {
            return;
        }

        let should_notify = {
            let dispatcher = Self::worker_activity_dispatcher();
            match dispatcher.lock() {
                Ok(mut state) => state
                    .worker_connected(connection_id, WorkerActivityRequest::new(activity, token)),
                Err(e) => {
                    error!("Failed to lock worker activity dispatcher: {e}");
                    return;
                }
            }
        };

        if should_notify {
            Self::worker_activity_notify().notify_one();
        }
    }

    /// Sends a batch of shares to the monitoring server.
    async fn send_shares(&self, shares: &[ShareInfo], token: &str) -> Result<(), Error> {
        debug!("Sending batch of {} shares to API", shares.len());
        let response = self
            .client
            .post(self.url.clone())
            .timeout(MONITOR_REQUEST_TIMEOUT)
            .json(&SharesRequest { shares, token })
            .send()
            .await?;

        match response.error_for_status() {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("Failed to send shares: {}", err);
                Err(err.into())
            }
        }
    }

    /// Sends a worker activity log to the monitoring server.
    pub async fn send_worker_activity(
        &self,
        activity: WorkerActivity,
        token: &str,
    ) -> Result<(), Error> {
        debug!("Sending worker activity to API: {:?}", activity);
        let response = self
            .client
            .post(self.url.clone())
            .timeout(MONITOR_REQUEST_TIMEOUT)
            .json(&WorkerActivityPayload {
                data: &activity,
                token,
            })
            .send()
            .await?;

        match response.error_for_status() {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("Failed to send worker activity: {}", err);
                Err(err.into())
            }
        }
    }
}

impl WorkerActivityRequest {
    fn new(activity: WorkerActivity, token: String) -> Self {
        let key = WorkerActivityKey {
            token,
            worker_name: activity.worker_name().to_string(),
        };
        Self { key, activity }
    }
}

impl WorkerActivityDispatcherState {
    fn worker_connected(&mut self, connection_id: u32, request: WorkerActivityRequest) -> bool {
        let key = request.key.clone();

        if let Some(existing_key) = self.session_keys.get(&connection_id).cloned() {
            if existing_key == key {
                return false;
            }

            warn!(
                "Worker activity session {connection_id} changed worker key from `{}` to `{}` without disconnect; resetting local state",
                existing_key.worker_name,
                key.worker_name,
            );
            self.remove_session(connection_id);
        }

        self.session_keys.insert(connection_id, key.clone());
        let active_count = self.active_session_counts.entry(key).or_insert(0);
        *active_count = active_count.saturating_add(1);

        if *active_count == 1 {
            self.enqueue(request)
        } else {
            false
        }
    }

    fn worker_disconnected(&mut self, connection_id: u32, user_agent: String) -> bool {
        let Some(key) = self.session_keys.get(&connection_id).cloned() else {
            return false;
        };

        if self.remove_session(connection_id) == 0 {
            return self.enqueue(WorkerActivityRequest {
                key: key.clone(),
                activity: WorkerActivity::new(
                    user_agent,
                    key.worker_name,
                    worker_activity::WorkerActivityType::Disconnected,
                ),
            });
        }

        false
    }

    fn enqueue(&mut self, request: WorkerActivityRequest) -> bool {
        let key = request.key.clone();

        if let Some(existing) = self.pending_by_key.get_mut(&key) {
            *existing = request;
            return false;
        }

        if !self.in_flight_keys.contains(&key)
            && self.pending_by_key.len() >= WORKER_ACTIVITY_MAX_PENDING_WORKERS
        {
            self.record_drop(&request);
            return false;
        }

        self.pending_by_key.insert(key.clone(), request);

        if self.in_flight_keys.contains(&key) {
            return false;
        }

        self.mark_ready(key)
    }

    fn pop_ready_request(&mut self) -> Option<WorkerActivityRequest> {
        if self.in_flight_keys.len() >= WORKER_ACTIVITY_MAX_IN_FLIGHT {
            return None;
        }

        while let Some(key) = self.ready_queue.pop_front() {
            self.ready_keys.remove(&key);
            if let Some(request) = self.pending_by_key.remove(&key) {
                self.in_flight_keys.insert(key);
                return Some(request);
            }
        }

        None
    }

    fn finish_request(&mut self, key: &WorkerActivityKey) -> bool {
        self.in_flight_keys.remove(key);

        if self.pending_by_key.contains_key(key) {
            let _ = self.mark_ready(key.clone());
        }

        !self.ready_queue.is_empty()
    }

    fn mark_ready(&mut self, key: WorkerActivityKey) -> bool {
        if self.ready_keys.insert(key.clone()) {
            self.ready_queue.push_back(key);
            true
        } else {
            false
        }
    }

    fn remove_session(&mut self, connection_id: u32) -> usize {
        let Some(key) = self.session_keys.remove(&connection_id) else {
            return 0;
        };

        let Some(count) = self.active_session_counts.get_mut(&key) else {
            return 0;
        };

        *count = count.saturating_sub(1);
        if *count == 0 {
            self.active_session_counts.remove(&key);
            0
        } else {
            *count
        }
    }

    fn record_drop(&mut self, request: &WorkerActivityRequest) {
        self.dropped_events_since_log = self.dropped_events_since_log.saturating_add(1);
        let now = Instant::now();
        let should_log = self
            .last_drop_log_at
            .is_none_or(|last| now.duration_since(last) >= WORKER_ACTIVITY_DROP_LOG_INTERVAL);

        if should_log {
            warn!(
                "Worker activity backlog full (max_pending_workers={WORKER_ACTIVITY_MAX_PENDING_WORKERS}, in_flight={}); dropped {} newest events; latest dropped worker=`{}` activity={:?}",
                self.in_flight_keys.len(),
                self.dropped_events_since_log,
                request.key.worker_name,
                request.activity,
            );
            self.dropped_events_since_log = 0;
            self.last_drop_log_at = Some(now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        shared_client, MonitorAPI, WorkerActivityDispatcherState, WorkerActivityRequest,
        MONITOR_REQUEST_TIMEOUT, WORKER_ACTIVITY_MAX_PENDING_WORKERS,
    };
    use crate::monitor::worker_activity::{WorkerActivity, WorkerActivityType};
    use crate::shared::error::Error;
    use std::time::Duration;
    use tokio::net::TcpListener;

    fn request(worker_name: &str, activity: WorkerActivityType) -> WorkerActivityRequest {
        WorkerActivityRequest::new(
            WorkerActivity::new("ua".to_string(), worker_name.to_string(), activity),
            "token".to_string(),
        )
    }

    #[test]
    fn worker_activity_dispatcher_coalesces_pending_state_per_worker() {
        let mut state = WorkerActivityDispatcherState::default();

        assert!(state.enqueue(request("worker-1", WorkerActivityType::Connected)));
        assert!(!state.enqueue(request("worker-1", WorkerActivityType::Disconnected)));

        let ready = state.pop_ready_request().expect("request should be ready");
        assert_eq!(ready.activity.activity(), WorkerActivityType::Disconnected);
        assert_eq!(ready.key.worker_name, "worker-1");
    }

    #[test]
    fn worker_activity_dispatcher_preserves_order_across_in_flight_and_follow_up_state() {
        let mut state = WorkerActivityDispatcherState::default();

        assert!(state.enqueue(request("worker-1", WorkerActivityType::Connected)));
        let first = state
            .pop_ready_request()
            .expect("first request should be ready");
        assert_eq!(first.activity.activity(), WorkerActivityType::Connected);

        assert!(!state.enqueue(request("worker-1", WorkerActivityType::Disconnected)));
        assert!(state.pop_ready_request().is_none());

        assert!(state.finish_request(&first.key));
        let second = state
            .pop_ready_request()
            .expect("follow-up request should be ready after completion");
        assert_eq!(second.activity.activity(), WorkerActivityType::Disconnected);
    }

    #[test]
    fn worker_activity_dispatcher_bounds_pending_workers() {
        let mut state = WorkerActivityDispatcherState::default();

        for index in 0..WORKER_ACTIVITY_MAX_PENDING_WORKERS {
            assert!(state.enqueue(request(
                &format!("worker-{index}"),
                WorkerActivityType::Connected,
            )));
        }

        assert!(!state.enqueue(request("worker-overflow", WorkerActivityType::Connected,)));
        assert_eq!(
            state.pending_by_key.len(),
            WORKER_ACTIVITY_MAX_PENDING_WORKERS
        );
    }

    #[test]
    fn worker_activity_dispatcher_keeps_worker_connected_until_last_session_disconnects() {
        let mut state = WorkerActivityDispatcherState::default();

        assert!(state.worker_connected(1, request("worker-1", WorkerActivityType::Connected),));
        assert!(!state.worker_connected(2, request("worker-1", WorkerActivityType::Connected),));

        let first = state
            .pop_ready_request()
            .expect("first connected state should be ready");
        assert_eq!(first.activity.activity(), WorkerActivityType::Connected);

        assert!(!state.worker_disconnected(1, "ua".to_string()));
        assert!(!state.finish_request(&first.key));

        assert!(state.worker_disconnected(2, "ua".to_string()));
        let second = state
            .pop_ready_request()
            .expect("last disconnect should be ready");
        assert_eq!(second.activity.activity(), WorkerActivityType::Disconnected);
    }

    #[tokio::test]
    async fn send_worker_activity_times_out_for_hung_monitor_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have address");
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("listener should accept");
            tokio::time::sleep(MONITOR_REQUEST_TIMEOUT + Duration::from_secs(1)).await;
        });

        let api = MonitorAPI {
            url: format!("http://{addr}/api/worker/activity")
                .parse()
                .expect("url should parse"),
            client: shared_client(),
        };

        let err = api
            .send_worker_activity(
                WorkerActivity::new(
                    "ua".to_string(),
                    "worker-1".to_string(),
                    WorkerActivityType::Connected,
                ),
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
