use crate::{
    api::stats::StatsSender,
    config::Configuration,
    monitor::{
        shares::{RejectionReason, ShareInfo, SharesMonitor},
        worker_activity::{WorkerActivity, WorkerActivityType},
    },
    proxy_state::{DownstreamType, ProxyState, UpstreamType},
    shared::utils::AbortOnDrop,
    translator::{error::Error, utils::validate_share},
};

use super::{
    super::upstream::diff_management::UpstreamDifficultyConfig, task_manager::TaskManager,
};
use pid::Pid;
use tokio::sync::{
    broadcast,
    mpsc::{channel, Receiver, Sender},
};

use super::{
    accept_connection::start_accept_connection, notify::start_notify,
    receive_from_downstream::start_receive_downstream,
    send_to_downstream::start_send_to_downstream, DownstreamMessages, SubmitShareWithChannelId,
};

use roles_logic_sv2::{
    common_properties::{IsDownstream, IsMiningDownstream},
    utils::Mutex,
};

use rand::Rng;
use server_to_client::Notify;
use std::{
    cell::Cell,
    collections::{hash_map::Entry, HashMap, VecDeque},
    net::IpAddr,
    sync::Arc,
};
use sv1_api::{
    client_to_server, json_rpc, server_to_client,
    utils::{Extranonce, HexU32Be},
    IsServer,
};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct DownstreamDifficultyConfig {
    pub estimated_downstream_hash_rate: f32,
    pub submits: VecDeque<std::time::Instant>,
    pub pid_controller: Pid<f32>,
    pub current_difficulties: VecDeque<f32>,
    pub initial_difficulty: f32,
}

impl DownstreamDifficultyConfig {
    pub fn share_count(&mut self) -> Option<f32> {
        let now = std::time::Instant::now();
        if self.submits.is_empty() {
            return Some(0.0);
        }
        let oldest = self.submits[0];
        if now - oldest < std::time::Duration::from_secs(20) {
            return None;
        }
        if now - oldest < std::time::Duration::from_secs(60) {
            let elapsed = now - oldest;
            Some(self.submits.len() as f32 / (elapsed.as_millis() as f32 / (60.0 * 1000.0)))
        } else {
            self.submits.pop_front();
            self.share_count()
        }
    }
    pub fn on_new_valid_share(&mut self) {
        self.submits.push_back(std::time::Instant::now());
    }

    pub fn reset(&mut self) {
        self.submits.clear();
    }

    pub fn add_difficulty(&mut self, new_diff: f32) {
        if self.current_difficulties.len() >= 3 {
            self.current_difficulties.pop_front();
        }
        self.current_difficulties.push_back(new_diff);
    }
}

impl PartialEq for DownstreamDifficultyConfig {
    fn eq(&self, other: &Self) -> bool {
        other.estimated_downstream_hash_rate.round() as u32
            == self.estimated_downstream_hash_rate.round() as u32
    }
}

/// Handles the sending and receiving of messages to and from an SV2 Upstream role (most typically
/// a SV2 Pool server).
#[derive(Debug)]
pub struct Downstream {
    /// List of authorized Downstream Mining Devices.
    pub(super) connection_id: u32,
    pub(super) authorized_names: Vec<String>,
    extranonce1: Vec<u8>,
    /// `extranonce1` to be sent to the Downstream in the SV1 `mining.subscribe` message response.
    //extranonce1: Vec<u8>,
    //extranonce2_size: usize,
    /// Version rolling mask bits
    pub(super) version_rolling_mask: Option<HexU32Be>,
    /// Minimum version rolling mask bits size
    version_rolling_min_bit: Option<HexU32Be>,
    /// Sends a SV1 `mining.submit` message received from the Downstream role to the `Bridge` for
    /// translation into a SV2 `SubmitSharesExtended`.
    tx_sv1_bridge: Sender<DownstreamMessages>,
    tx_outgoing: Sender<json_rpc::Message>,
    extranonce2_len: usize,
    pub(super) difficulty_mgmt: DownstreamDifficultyConfig,
    pub(super) upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
    pub last_call_to_update_hr: u128,
    pub(super) stats_sender: StatsSender,
    pub recent_jobs: RecentJobs,
    pub first_job: Notify<'static>,
    pub share_monitor: SharesMonitor,
    pub user_agent: std::cell::RefCell<String>, // RefCell is used here because `handle_subscribe` and `handle_authorize` take &self not &mut self and we need to mutate user_agent
    pub token: Arc<Mutex<String>>,
    tx_update_token: Sender<String>,
    disconnect_requested: Cell<bool>,
    shared_token_state: Arc<Mutex<TokenChangeTracker>>,
}

#[derive(Debug)]
pub(super) struct TokenChangeTracker {
    current_token: String,
    change_count: u32,
    max_token_changes_per_connection: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum AuthorizeTokenAction {
    KeepCurrent(String),
    Update(String),
    Disconnect,
}

impl Downstream {
    /// Instantiate a new `Downstream`.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn new_downstream(
        connection_id: u32,
        tx_sv1_bridge: Sender<DownstreamMessages>,
        rx_sv1_notify: broadcast::Receiver<server_to_client::Notify<'static>>,
        extranonce1: Vec<u8>,
        last_notify: Option<server_to_client::Notify<'static>>,
        extranonce2_len: usize,
        host: String,
        upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
        send_to_down: Sender<String>,
        recv_from_down: Receiver<String>,
        task_manager: Arc<Mutex<TaskManager>>,
        initial_difficulty: f32,
        stats_sender: StatsSender,
        shared_token_state: Arc<Mutex<TokenChangeTracker>>,
        tx_update_token: Sender<String>,
    ) {
        assert!(last_notify.is_some());

        let (tx_outgoing, receiver_outgoing) = channel(crate::TRANSLATOR_BUFFER_SIZE);

        // The PID controller uses negative proportional (P) and integral (I) gains to reduce difficulty
        // when the actual share rate falls below the target rate (SHARE_PER_MIN). Negative gains are chosen
        // because a lower share rate indicates the difficulty is too high for the miner, requiring a downward
        // adjustment to make mining easier.
        //
        // // Example:
        // - Target share rate (SHARE_PER_MIN) = 10 shares/min.
        // - Case 1: Actual share rate = 5 shares/min (less than target):
        //   - Error = 10 - 5 = 5 (positive).
        //   - P output = -3.0 * 5 = -15 (reduces difficulty by 15).
        //   - I output (assuming error persists 5 intervals) = -0.5 * (-15 * 5) = -37.5 (further reduction).
        //   - Difficulty decreases, making mining easier to increase share rate.
        //
        // - Case 2: Actual share rate = 12 shares/min (greater than target):
        //   - Error = 10 - 12 = -2 (negative).
        //   - P output = -3.0 * -2 = 6 (increases difficulty by 6).
        //   - I output (5 intervals) = -0.5 * (-2 * 5) = 5 (further increase).
        //   - Difficulty increases, slows share rate.
        //
        // The positive D gain (0.05) dampens rapid changes, e.g., if share rate jumps from 8 to 12, D might
        // add a small positive adjustment to prevent overshooting.

        let mut pid: Pid<f32> = Pid::new(*crate::SHARE_PER_MIN, initial_difficulty * 10.0);
        let pk = -initial_difficulty * 0.01;
        //let pi = initial_difficulty * 0.1;
        //let pd = initial_difficulty * 0.01;
        pid.p(pk, f32::MAX).i(0.0, f32::MAX).d(0.0, f32::MAX);

        let estimated_downstream_hash_rate = Configuration::downstream_hashrate();
        let mut current_difficulties = VecDeque::with_capacity(3);
        current_difficulties.push_back(initial_difficulty);

        let difficulty_mgmt = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate,
            submits: vec![].into(),
            pid_controller: pid,
            current_difficulties,
            initial_difficulty,
        };

        let token = Arc::new(Mutex::new(
            shared_token_state
                .safe_lock(|state| state.current_token.clone())
                .unwrap_or_else(|_| Configuration::token().expect("Token is not set")),
        ));

        let downstream = Arc::new(Mutex::new(Downstream {
            connection_id,
            authorized_names: vec![],
            extranonce1,
            version_rolling_mask: None,
            version_rolling_min_bit: None,
            tx_sv1_bridge,
            tx_outgoing,
            extranonce2_len,
            difficulty_mgmt,
            upstream_difficulty_config,
            last_call_to_update_hr: 0,
            stats_sender,
            recent_jobs: RecentJobs::new(),
            first_job: last_notify.expect("we have an assertion at the beginning of this function"),
            share_monitor: SharesMonitor::new(token.clone()),
            user_agent: std::cell::RefCell::new(String::new()),
            token,
            tx_update_token,
            disconnect_requested: Cell::new(false),
            shared_token_state,
        }));

        if let Err(e) = start_receive_downstream(
            task_manager.clone(),
            downstream.clone(),
            recv_from_down,
            connection_id,
        )
        .await
        {
            error!("Failed to start receive downstream task: {e}");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
        };

        if let Err(e) = start_send_to_downstream(
            task_manager.clone(),
            receiver_outgoing,
            send_to_down,
            connection_id,
            host.clone(),
        )
        .await
        {
            error!("Failed to start send_to_downstream task {e}");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
        };

        if let Err(e) = start_notify(
            task_manager.clone(),
            downstream.clone(),
            rx_sv1_notify,
            host.clone(),
            connection_id,
        )
        .await
        {
            error!("Failed to start notify task: {e}");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
        };

        if let Err(e) = Self::start_share_monitor(task_manager.clone(), downstream.clone()).await {
            error!("Failed to start share monitor task: {e}");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
        }
    }

    /// Starts the shares monitor task.
    async fn start_share_monitor(
        task_manager: Arc<Mutex<TaskManager>>,
        downstream: Arc<Mutex<Self>>,
    ) -> Result<(), Error<'static>> {
        let (share_monitor, connection_id) =
            downstream.safe_lock(|s| (s.share_monitor.clone(), s.connection_id))?;

        // Create an abortable task for the shares monitor
        let abortable = tokio::spawn(async move {
            info!("Starting shares monitor for downstream: {}", connection_id);
            share_monitor.clone().monitor().await;
        });

        // Register the task with the task manager so it can be aborted when needed
        TaskManager::add_shares_monitor(connection_id, task_manager, abortable.into())
            .await
            .map_err(|_| Error::TranslatorTaskManagerFailed)
    }

    /// Accept connections from one or more SV1 Downstream roles (SV1 Mining Devices) and create a
    /// new `Downstream` for each connection.
    pub async fn accept_connections(
        tx_sv1_submit: Sender<DownstreamMessages>,
        tx_mining_notify: broadcast::Sender<server_to_client::Notify<'static>>,
        bridge: Arc<Mutex<super::super::proxy::Bridge>>,
        upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
        downstreams: Receiver<(Sender<String>, Receiver<String>, IpAddr)>,
        stats_sender: StatsSender,
        tx_update_token: Sender<String>,
    ) -> Result<AbortOnDrop, Error<'static>> {
        let shared_token_state = Arc::new(Mutex::new(TokenChangeTracker::new(
            Configuration::token().expect("Token is not set"),
            Configuration::max_token_changes_per_connection(),
        )));
        let task_manager = TaskManager::initialize();
        let abortable = task_manager
            .safe_lock(|t| t.get_aborter())
            .map_err(|_| Error::TranslatorTaskManagerMutexPoisoned)?
            .ok_or(Error::TranslatorTaskManagerFailed)?;
        if let Err(e) = start_accept_connection(
            task_manager.clone(),
            tx_sv1_submit,
            tx_mining_notify,
            bridge,
            upstream_difficulty_config,
            downstreams,
            stats_sender,
            shared_token_state,
            tx_update_token,
        )
        .await
        {
            error!("Translator downstream failed to accept: {e}");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
            return Err(e);
        };
        Ok(abortable)
    }

    /// As SV1 messages come in, determines if the message response needs to be translated to SV2
    /// and sent to the `Upstream`, or if a direct response can be sent back by the `Translator`
    /// (SV1 and SV2 protocol messages are NOT 1-to-1).
    pub(super) async fn handle_incoming_sv1(
        self_: Arc<Mutex<Self>>,
        message_sv1: json_rpc::Message,
    ) -> Result<(), super::super::error::Error<'static>> {
        // `handle_message` in `IsServer` trait + calls `handle_request`
        // TODO: Map err from V1Error to Error::V1Error

        let (response, disconnect_requested) = self_.safe_lock(|s| {
            let response = s.handle_message(message_sv1.clone());
            let disconnect_requested = s.take_disconnect_request();
            (response, disconnect_requested)
        })?;
        if disconnect_requested {
            return Err(Error::DisconnectDownstream);
        }
        match response {
            Ok(res) => {
                if let Some(r) = res {
                    // If some response is received, indicates no messages translation is needed
                    // and response should be sent directly to the SV1 Downstream. Otherwise,
                    // message will be sent to the upstream Translator to be translated to SV2 and
                    // forwarded to the `Upstream`
                    // let sender = self_.safe_lock(|s| s.connection.sender_upstream)
                    Self::send_message_downstream(self_, r.into()).await;
                    Ok(())
                } else {
                    // If None response is received, indicates this SV1 message received from the
                    // Downstream MD is passed to the `Translator` for translation into SV2
                    Ok(())
                }
            }
            Err(e) => {
                error!("{e}");
                Err(Error::V1Protocol(Box::new(e)))
            }
        }
    }

    /// Send SV1 response message that is generated by `Downstream` (as opposed to being received
    /// by `Bridge`) to be written to the SV1 Downstream role.
    pub(super) async fn send_message_downstream(
        self_: Arc<Mutex<Self>>,
        response: json_rpc::Message,
    ) {
        let sender = match self_.safe_lock(|s| s.tx_outgoing.clone()) {
            Ok(sender) => sender,
            Err(e) => {
                // Poisoned mutex
                error!("{e}");
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                return;
            }
        };
        let _ = sender.send(response).await;
    }

    /// Send SV1 response message that is generated by `Downstream` (as opposed to being received
    /// by `Bridge`) to be written to the SV1 Downstream role.
    pub(super) async fn send_message_upstream(self_: &Arc<Mutex<Self>>, msg: DownstreamMessages) {
        let sender = match self_.safe_lock(|s| s.tx_sv1_bridge.clone()) {
            Ok(sender) => sender,
            Err(e) => {
                error!("{e}");
                // Poisoned mutex
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                return;
            }
        };
        if sender.send(msg).await.is_err() {
            error!("Translator downstream failed to send message");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
        }
    }

    fn handle_authorize_token(&self, new_token: &str) -> AuthorizeTokenAction {
        let action = self
            .shared_token_state
            .safe_lock(|state| state.handle_authorize_token(new_token))
            // Here we can not update the token so downstream risk to mine for someone else, we
            // could do unwrap_or_else and return Discconnect that will discconnect the downstream
            // but if the mutex is poisoned this will keep happening for all the new downstream do
            // we are keeping up a non fully functional proxy. Better to fail.
            .unwrap();
        if let AuthorizeTokenAction::Update(_) = action {
            self.token
                .safe_lock(|token| *token = new_token.to_string())
                .unwrap();
        }
        action
    }

    fn take_disconnect_request(&self) -> bool {
        self.disconnect_requested.replace(false)
    }
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connection_id: u32,
        authorized_names: Vec<String>,
        extranonce1: Vec<u8>,
        version_rolling_mask: Option<HexU32Be>,
        version_rolling_min_bit: Option<HexU32Be>,
        tx_sv1_bridge: Sender<DownstreamMessages>,
        tx_outgoing: Sender<json_rpc::Message>,
        extranonce2_len: usize,
        difficulty_mgmt: DownstreamDifficultyConfig,
        upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
        stats_sender: StatsSender,
        first_job: Notify<'static>,
        initial_token: String,
        max_token_changes_per_connection: u32,
        tx_update_token: Sender<String>,
    ) -> Self {
        let shared_token_state = Arc::new(Mutex::new(TokenChangeTracker::new(
            initial_token.clone(),
            max_token_changes_per_connection,
        )));
        let token = Arc::new(Mutex::new(initial_token));
        Self::new_with_shared_token_state(
            connection_id,
            authorized_names,
            extranonce1,
            version_rolling_mask,
            version_rolling_min_bit,
            tx_sv1_bridge,
            tx_outgoing,
            extranonce2_len,
            difficulty_mgmt,
            upstream_difficulty_config,
            stats_sender,
            first_job,
            token,
            shared_token_state,
            tx_update_token,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn new_with_shared_token_state(
        connection_id: u32,
        authorized_names: Vec<String>,
        extranonce1: Vec<u8>,
        version_rolling_mask: Option<HexU32Be>,
        version_rolling_min_bit: Option<HexU32Be>,
        tx_sv1_bridge: Sender<DownstreamMessages>,
        tx_outgoing: Sender<json_rpc::Message>,
        extranonce2_len: usize,
        difficulty_mgmt: DownstreamDifficultyConfig,
        upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
        stats_sender: StatsSender,
        first_job: Notify<'static>,
        token: Arc<Mutex<String>>,
        shared_token_state: Arc<Mutex<TokenChangeTracker>>,
        tx_update_token: Sender<String>,
    ) -> Self {
        use crate::monitor::shares::SharesMonitor;

        Downstream {
            connection_id,
            authorized_names,
            extranonce1,
            version_rolling_mask,
            version_rolling_min_bit,
            tx_sv1_bridge,
            tx_outgoing,
            extranonce2_len,
            difficulty_mgmt,
            upstream_difficulty_config,
            last_call_to_update_hr: 0,
            first_job,
            stats_sender,
            recent_jobs: RecentJobs::new(),
            share_monitor: SharesMonitor::new(token.clone()),
            user_agent: std::cell::RefCell::new(String::new()),
            token,
            tx_update_token,
            disconnect_requested: Cell::new(false),
            shared_token_state,
        }
    }
}

impl TokenChangeTracker {
    fn new(current_token: String, max_token_changes_per_connection: u32) -> Self {
        Self {
            current_token,
            change_count: 0,
            max_token_changes_per_connection,
        }
    }

    fn handle_authorize_token(&mut self, new_token: &str) -> AuthorizeTokenAction {
        if self.max_token_changes_per_connection == 0 {
            return AuthorizeTokenAction::KeepCurrent(self.current_token.clone());
        }
        if new_token == self.current_token {
            return AuthorizeTokenAction::KeepCurrent(self.current_token.clone());
        }

        self.change_count = self.change_count.saturating_add(1);
        if self.change_count > self.max_token_changes_per_connection || new_token.is_empty() {
            return AuthorizeTokenAction::Disconnect;
        }

        self.current_token = new_token.to_string();
        AuthorizeTokenAction::Update(self.current_token.clone())
    }
}

/// Implements `IsServer` for `Downstream` to handle the SV1 messages.
impl IsServer<'static> for Downstream {
    /// Handle the incoming `mining.configure` message which is received after a Downstream role is
    /// subscribed and authorized. Contains the version rolling mask parameters.
    fn handle_configure(
        &mut self,
        request: &client_to_server::Configure,
    ) -> (Option<server_to_client::VersionRollingParams>, Option<bool>) {
        info!("Down: Handling mining.configure: {:?}", &request);
        let (version_rolling_mask, version_rolling_min_bit_count) =
            crate::shared::utils::sv1_rolling(request);

        self.version_rolling_mask = Some(version_rolling_mask.clone());
        self.version_rolling_min_bit = Some(version_rolling_min_bit_count.clone());
        let mut first_job = self.first_job.clone();
        self.recent_jobs
            .add_job(&mut first_job, self.version_rolling_mask.clone());
        self.first_job = first_job;

        (
            Some(server_to_client::VersionRollingParams::new(
                    version_rolling_mask,version_rolling_min_bit_count
            ).expect("Version mask invalid, automatic version mask selection not supported, please change it in carte::downstream_sv1::mod.rs")),
            Some(false),
        )
    }

    fn handle_suggest_difficulty(&mut self, _: &sv1_api::client_to_server::SuggestDifficulty) {
        println!("Received Suggest Diff");
    }

    /// Handle the response to a `mining.subscribe` message received from the client.
    /// The subscription messages are erroneous and just used to conform the SV1 protocol spec.
    /// Because no one unsubscribed in practice, they just unplug their machine.
    fn handle_subscribe(&self, request: &client_to_server::Subscribe) -> Vec<(String, String)> {
        info!("Down: Handling mining.subscribe: {:?}", &request);
        self.stats_sender
            .update_device_name(self.connection_id, request.agent_signature.clone());

        let set_difficulty_sub = (
            "mining.set_difficulty".to_string(),
            super::new_subscription_id(),
        );
        let notify_sub = (
            "mining.notify".to_string(),
            "ae6812eb4cd7735a302a8a9dd95cf71f".to_string(),
        );
        self.user_agent.replace(request.agent_signature.clone());
        vec![set_difficulty_sub, notify_sub]
    }

    fn handle_authorize(&self, request: &client_to_server::Authorize) -> bool {
        if self.authorized_names.is_empty() {
            let new_token = request.password.clone();
            if !new_token.is_empty() {
                let token = match self.handle_authorize_token(&request.password) {
                    AuthorizeTokenAction::KeepCurrent(token) => token,
                    AuthorizeTokenAction::Update(token) => {
                        let tx_update_token = self.tx_update_token.clone();
                        let token_to_send = token.clone();
                        tokio::spawn(async move {
                            if tx_update_token.send(token_to_send).await.is_err() {
                                error!("Failed to send token update to upstream");
                                ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                            }
                        });
                        token
                    }
                    AuthorizeTokenAction::Disconnect => {
                        self.disconnect_requested.set(true);
                        return false;
                    }
                };
                if let Err(e) = self.token.safe_lock(|t| *t = token.clone()) {
                    error!("Failed to update token: {:?}", e);
                    ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                }
                let tx_update_token = self.tx_update_token.clone();
                // TODO not sure if we should await this task or not
                tokio::spawn(async move {
                    if tx_update_token.send(new_token).await.is_err() {
                        error!("Failed to send token update to upstream");
                        ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                    }
                });
            } else if self
                .shared_token_state
                .safe_lock(|s| s.max_token_changes_per_connection == 0)
                .unwrap()
            {
                info!("Skip token check, max_token_changes_per_connection set to 0");
            } else {
                error!("Downstream is expected to carry a token");
                return false;
            }

            let token = self.token.safe_lock(|t| t.clone()).unwrap_or_else(|e| {
                error!("Failed to read token: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                String::new()
            });
            let user_agent = self.user_agent.borrow().clone();
            let worker_activity = WorkerActivity::new(
                user_agent,
                request.name.clone(),
                WorkerActivityType::Connected,
            );

            tokio::spawn(async move {
                if let Err(e) = worker_activity
                    .monitor_api()
                    .send_worker_activity(worker_activity, &token)
                    .await
                {
                    error!("Failed to send worker activity: {}", e);
                }
            });

            true
        } else {
            // when downstream is already authorized we do not want return an ok response otherwise
            // the sv1 proxy could thing that we are saying that downstream produced a valid share.
            warn!("Downstream is trying to authorize again, this should not happen");
            false
        }
    }

    /// When miner find the job which meets requested difficulty, it can submit share to the server.
    /// Only [Submit](client_to_server::Submit) requests for authorized user names can be submitted.
    fn handle_submit(&self, request: &client_to_server::Submit<'static>) -> bool {
        info!(
            "Handling mining.submit request {} from {} with job_id {}, nonce: {:?}",
            request.id, request.user_name, request.job_id, request.nonce
        );

        let mut request = request.clone();
        let job_id_as_number = request.job_id.parse::<u32>();
        let nonce = request.nonce.0 as i64;
        if job_id_as_number.is_err() {
            error!(
                "Share rejected: can not convert v1 job id to number. v1 id: {}",
                request.job_id
            );

            let share = ShareInfo::new(
                request.user_name.clone(),
                None,
                0,
                nonce,
                // We can use this here beacuse we are inside the error branch
                //
                // TODO: Think about a better way to handle job id when it can not be parsed to
                // number
                // job_id_as_number.expect("checked above") as i64,
                Some(RejectionReason::InvalidJobIdFormat),
            );
            self.share_monitor.insert_share(share);

            self.stats_sender.update_rejected_shares(self.connection_id);
            return false;
        }
        let job_id = job_id_as_number.clone().expect("checked above") as i64;
        crate::translator::utils::update_share_count(self.connection_id); // update share count
        if let Some(job) = self
            .recent_jobs
            .get_matching_job(job_id_as_number.expect("checked above"))
        {
            request.job_id = job.job_id.clone();
            //check share is valid
            if let Some(met_difficulty) = validate_share(
                &request,
                &job,
                &self.difficulty_mgmt.current_difficulties,
                self.extranonce1.clone(),
                self.version_rolling_mask.clone(),
            ) {
                // Only forward upstream if the share meets the latest difficulty
                if let Some(latest_difficulty) = self.difficulty_mgmt.current_difficulties.back() {
                    if met_difficulty == *latest_difficulty {
                        let to_send = SubmitShareWithChannelId {
                            channel_id: self.connection_id,
                            share: request.clone(),
                            extranonce: self.extranonce1.clone(),
                            extranonce2_len: self.extranonce2_len,
                            version_rolling_mask: self.version_rolling_mask.clone(),
                        };
                        if let Err(e) = self
                            .tx_sv1_bridge
                            .try_send(DownstreamMessages::SubmitShares(to_send))
                        {
                            error!("Failed to start receive downstream task: {e:?}");
                            self.stats_sender.update_rejected_shares(self.connection_id);
                            // Return false because submit was not properly handled
                            return false;
                        }
                        // Share is accepted here
                        let share = ShareInfo::new(
                            request.user_name.clone(),
                            Some(met_difficulty),
                            job_id,
                            nonce,
                            None,
                        );
                        self.share_monitor.insert_share(share);
                    } else {
                        // met_difficulty is not latest difficulty, so we mark it as rejected
                        let share = ShareInfo::new(
                            request.user_name.clone(),
                            None,
                            job_id, // rejected because it was not sent upstream
                            nonce,
                            Some(RejectionReason::DifficultyMismatch),
                        );
                        self.share_monitor.insert_share(share);
                    }
                }
                self.stats_sender.update_accepted_shares(self.connection_id);
                info!(
                    "Share for Job {} and difficulty {} is accepted",
                    request.job_id, met_difficulty
                );
                true
            } else {
                let share = ShareInfo::new(
                    request.user_name.clone(),
                    None,
                    job_id,
                    nonce,
                    Some(RejectionReason::InvalidShare),
                );
                self.share_monitor.insert_share(share);
                error!("Share rejected: Invalid share");
                self.stats_sender.update_rejected_shares(self.connection_id);
                false
            }
        } else {
            let event = ShareInfo::new(
                request.user_name.clone(),
                None,
                job_id,
                nonce,
                Some(RejectionReason::JobIdNotFound),
            );
            self.share_monitor.insert_share(event);
            error!(
                "Share rejected: can not find job with id {}",
                request.job_id
            );
            self.stats_sender.update_rejected_shares(self.connection_id);
            false
        }
    }

    /// Indicates to the server that the client supports the mining.set_extranonce method.
    fn handle_extranonce_subscribe(&self) {}

    /// Checks if a Downstream role is authorized.
    fn is_authorized(&self, name: &str) -> bool {
        self.authorized_names.contains(&name.to_string())
    }

    /// Authorizes a Downstream role.
    fn authorize(&mut self, name: &str) {
        self.authorized_names.push(name.to_string());
    }

    /// Sets the `extranonce1` field sent in the SV1 `mining.notify` message to the value specified
    /// by the SV2 `OpenExtendedMiningChannelSuccess` message sent from the Upstream role.
    fn set_extranonce1(
        &mut self,
        _extranonce1: Option<Extranonce<'static>>,
    ) -> Extranonce<'static> {
        self.extranonce1.clone().try_into().expect("Internal error: this opration can not fail because the Vec<U8> can always be converted into Extranonce")
    }

    /// Returns the `Downstream`'s `extranonce1` value.
    fn extranonce1(&self) -> Extranonce<'static> {
        self.extranonce1.clone().try_into().expect("Internal error: this opration can not fail because the Vec<U8> can always be converted into Extranonce")
    }

    /// Sets the `extranonce2_size` field sent in the SV1 `mining.notify` message to the value
    /// specified by the SV2 `OpenExtendedMiningChannelSuccess` message sent from the Upstream role.
    fn set_extranonce2_size(&mut self, _extra_nonce2_size: Option<usize>) -> usize {
        self.extranonce2_len
    }

    /// Returns the `Downstream`'s `extranonce2_size` value.
    fn extranonce2_size(&self) -> usize {
        self.extranonce2_len
    }

    /// Returns the version rolling mask.
    fn version_rolling_mask(&self) -> Option<HexU32Be> {
        self.version_rolling_mask.clone()
    }

    /// Sets the version rolling mask.
    fn set_version_rolling_mask(&mut self, mask: Option<HexU32Be>) {
        self.version_rolling_mask = mask;
    }

    /// Sets the minimum version rolling bit.
    fn set_version_rolling_min_bit(&mut self, mask: Option<HexU32Be>) {
        self.version_rolling_min_bit = mask
    }

    fn notify(&'_ mut self) -> Result<json_rpc::Message, sv1_api::error::Error<'_>> {
        unreachable!()
    }
}

impl IsMiningDownstream for Downstream {}

impl IsDownstream for Downstream {
    fn get_downstream_mining_data(
        &self,
    ) -> roles_logic_sv2::common_properties::CommonDownstreamData {
        todo!()
    }
}

#[derive(Debug)]
pub struct RecentJobs {
    v1_to_v2: HashMap<u32, u32>,
    v2_to_v1: HashMap<u32, Vec<u32>>,
    jobs: VecDeque<Notify<'static>>,
    last_v2s: CircularBuffer<u32, 3>,
    tracked_jobs: usize,
}
fn apply_mask(mask: Option<HexU32Be>, message: &mut server_to_client::Notify<'static>) {
    if let Some(mask) = mask {
        message.version = HexU32Be(message.version.0 & !mask.0);
    }
}
impl RecentJobs {
    pub fn add_job(&mut self, notify: &mut Notify<'static>, mask: Option<HexU32Be>) {
        apply_mask(mask, notify);
        // save it with the v2 id
        self.jobs.push_back(notify.clone());
        let new_id = self.new_v1(notify.job_id.parse::<u32>().unwrap());
        // send it with the v1 id
        notify.job_id = new_id.to_string();
        if self.jobs.len() > self.tracked_jobs {
            self.jobs.pop_front();
        };
    }

    pub fn clone_last(&mut self) -> Option<Notify<'static>> {
        if let Some(job) = self.jobs.back() {
            let mut job = job.clone();
            let new_id = self.new_v1(job.job_id.parse::<u32>().unwrap());
            job.job_id = new_id.to_string();
            Some(job.clone())
        } else {
            None
        }
    }

    pub fn current_jobs(&self) -> VecDeque<Notify<'static>> {
        self.jobs.clone()
    }

    pub fn get_matching_job(&self, v1_id: u32) -> Option<Notify<'static>> {
        let v2_id = self.get_v2(v1_id)?;
        self.current_jobs()
            .iter()
            .find(|notify| notify.job_id == v2_id)
            .cloned()
    }

    fn new_v1(&mut self, v2_id: u32) -> u32 {
        let mut v1_id = rand::thread_rng().gen();
        while self.v1_to_v2.contains_key(&v1_id) {
            v1_id = rand::thread_rng().gen();
        }
        match self.v2_to_v1.entry(v2_id) {
            Entry::Occupied(mut v) => {
                v.get_mut().push(v1_id);
            }
            Entry::Vacant(v) => {
                v.insert(vec![v1_id]);
                if let Some(first) = self.last_v2s.push_back(v2_id) {
                    self.remove_v2(first);
                }
            }
        }
        self.v1_to_v2.insert(v1_id, v2_id);
        v1_id
    }
    fn remove_v2(&mut self, v2_id: u32) {
        if let Some(v1_ids) = self.v2_to_v1.remove(&v2_id) {
            for v1_id in v1_ids {
                self.v1_to_v2.remove(&v1_id);
            }
        }
    }
    fn get_v2(&self, v1_id: u32) -> Option<String> {
        self.v1_to_v2.get(&v1_id).cloned().map(|v| v.to_string())
    }
    pub fn new() -> Self {
        Self {
            v1_to_v2: HashMap::new(),
            v2_to_v1: HashMap::new(),
            last_v2s: CircularBuffer::new(),
            jobs: VecDeque::new(),
            tracked_jobs: 3,
        }
    }
}

impl Default for RecentJobs {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct CircularBuffer<T, const N: usize> {
    buffer: [Option<T>; N],
    len: usize,
    start: usize,
}

impl<T, const N: usize> CircularBuffer<T, N> {
    pub fn new() -> Self {
        Self {
            buffer: std::array::from_fn(|_| None),
            len: 0,
            start: 0,
        }
    }

    pub fn push_back(&mut self, value: T) -> Option<T> {
        let end = (self.start + self.len) % N;

        if self.len < N {
            self.buffer[end] = Some(value);
            self.len += 1;
            None
        } else {
            let evicted = self.buffer[self.start].take();
            self.buffer[self.start] = Some(value);
            self.start = (self.start + 1) % N;
            evicted
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translator::upstream::diff_management::UpstreamDifficultyConfig;
    use rand::Rng;
    use sv1_api::utils::{MerkleNode, PrevHash};
    use tokio::sync::mpsc::{channel, Receiver};

    fn test_first_job() -> Notify<'static> {
        let random_str = rand::thread_rng().gen::<[u8; 32]>().to_vec();
        Notify {
            job_id: "1".to_string(),
            prev_hash: PrevHash::try_from("0".repeat(64).as_str()).unwrap(),
            coin_base1: "ffff".try_into().unwrap(),
            coin_base2: "ffff".try_into().unwrap(),
            merkle_branch: vec![MerkleNode::try_from(random_str).unwrap()],
            version: HexU32Be(1),
            bits: HexU32Be(1),
            time: HexU32Be(1),
            clean_jobs: true,
        }
    }

    fn build_test_downstream_with_shared_state(
        connection_id: u32,
        shared_token_state: Arc<Mutex<TokenChangeTracker>>,
        tx_update_token: Sender<String>,
    ) -> (Arc<Mutex<Downstream>>, Receiver<json_rpc::Message>) {
        let mut current_difficulties = VecDeque::new();
        current_difficulties.push_back(1.0);

        let downstream_conf = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate: 0.0,
            submits: VecDeque::new(),
            pid_controller: Pid::new(10.0, 1.0),
            current_difficulties,
            initial_difficulty: 1.0,
        };
        let upstream_config = UpstreamDifficultyConfig {
            channel_diff_update_interval: 60,
            channel_nominal_hashrate: 0.0,
        };
        let (tx_sv1_submit, _rx_sv1_submit) = channel(10);
        let (tx_outgoing, rx_outgoing) = channel(10);
        let token = Arc::new(Mutex::new(
            shared_token_state
                .safe_lock(|state| state.current_token.clone())
                .unwrap(),
        ));

        let downstream = Downstream::new_with_shared_token_state(
            connection_id,
            vec![],
            vec![],
            None,
            None,
            tx_sv1_submit,
            tx_outgoing,
            0,
            downstream_conf,
            Arc::new(Mutex::new(upstream_config)),
            crate::api::stats::StatsSender::new(),
            test_first_job(),
            token,
            shared_token_state,
            tx_update_token,
        );

        (Arc::new(Mutex::new(downstream)), rx_outgoing)
    }

    #[allow(clippy::type_complexity)]
    fn build_test_downstream(
        initial_token: &str,
        max_token_changes_per_connection: u32,
    ) -> (
        Arc<Mutex<Downstream>>,
        Receiver<json_rpc::Message>,
        Receiver<String>,
        Arc<Mutex<TokenChangeTracker>>,
    ) {
        let shared_token_state = Arc::new(Mutex::new(TokenChangeTracker::new(
            initial_token.to_string(),
            max_token_changes_per_connection,
        )));
        let (tx_update_token, rx_update_token) = channel(10);
        let (downstream, rx_outgoing) =
            build_test_downstream_with_shared_state(1, shared_token_state.clone(), tx_update_token);

        (downstream, rx_outgoing, rx_update_token, shared_token_state)
    }

    fn authorize_request(id: u64, name: &str, password: &str) -> json_rpc::Message {
        client_to_server::Authorize {
            id,
            name: name.to_string(),
            password: password.to_string(),
        }
        .into()
    }

    #[tokio::test]
    async fn changed_token_updates_shared_state_until_limit() {
        let (downstream, _rx_outgoing, _rx_update_token, shared_token_state) =
            build_test_downstream("token-a", 1);

        let action = downstream
            .safe_lock(|d| d.handle_authorize_token("token-b"))
            .unwrap();
        let (token, disconnect_requested) = downstream
            .safe_lock(|d| {
                (
                    d.token.safe_lock(|t| t.clone()).unwrap(),
                    d.disconnect_requested.get(),
                )
            })
            .unwrap();
        let (current_token, change_count) = shared_token_state
            .safe_lock(|state| (state.current_token.clone(), state.change_count))
            .unwrap();

        assert_eq!(action, AuthorizeTokenAction::Update("token-b".to_string()));
        assert_eq!(token, "token-b");
        assert_eq!(current_token, "token-b");
        assert_eq!(change_count, 1);
        assert!(!disconnect_requested);
    }

    #[tokio::test]
    async fn zero_token_change_limit_ignores_requested_token_changes() {
        let (downstream, _rx_outgoing, _rx_update_token, shared_token_state) =
            build_test_downstream("token-a", 0);
        let auth_m = client_to_server::Authorize {
            id: 1,
            name: "name".to_string(),
            password: "token-b".to_string(),
        };

        downstream
            .safe_lock(|d| d.handle_authorize(&auth_m))
            .unwrap();
        let (token, disconnect_requested) = downstream
            .safe_lock(|d| {
                (
                    d.token.safe_lock(|t| t.clone()).unwrap(),
                    d.disconnect_requested.get(),
                )
            })
            .unwrap();
        let (current_token, change_count) = shared_token_state
            .safe_lock(|state| (state.current_token.clone(), state.change_count))
            .unwrap();

        assert_eq!(token, "token-a");
        assert_eq!(current_token, "token-a");
        assert_eq!(change_count, 0);
        assert!(!disconnect_requested);
    }

    #[tokio::test]
    async fn unchanged_token_does_not_increment_shared_counter_and_authorize_is_deduplicated() {
        let (downstream, _rx_outgoing, _rx_update_token, shared_token_state) =
            build_test_downstream("token-a", 1);

        let first = downstream
            .safe_lock(|d| {
                let action = d.handle_authorize_token("token-a");
                action
            })
            .unwrap();
        let second = downstream
            .safe_lock(|d| {
                let action = d.handle_authorize_token("token-a");
                d.authorize("worker-b");
                action
            })
            .unwrap();
        let (_, token) = downstream
            .safe_lock(|d| {
                (
                    d.authorized_names.clone(),
                    d.token.safe_lock(|t| t.clone()).unwrap(),
                )
            })
            .unwrap();
        let (current_token, change_count) = shared_token_state
            .safe_lock(|state| (state.current_token.clone(), state.change_count))
            .unwrap();

        assert_eq!(
            first,
            AuthorizeTokenAction::KeepCurrent("token-a".to_string())
        );
        assert_eq!(
            second,
            AuthorizeTokenAction::KeepCurrent("token-a".to_string())
        );
        assert_eq!(change_count, 0);
        assert_eq!(current_token, "token-a");
        assert_eq!(token, "token-a");
    }

    #[tokio::test]
    async fn exceeding_token_change_limit_disconnects_across_downstreams_without_response_or_upstream_update(
    ) {
        let shared_token_state = Arc::new(Mutex::new(TokenChangeTracker::new(
            "token-a".to_string(),
            1,
        )));
        let (tx_update_token, mut rx_update_token) = channel(10);
        let (first_downstream, _first_rx_outgoing) = build_test_downstream_with_shared_state(
            1,
            shared_token_state.clone(),
            tx_update_token.clone(),
        );

        let first_action = first_downstream
            .safe_lock(|d| {
                let action = d.handle_authorize_token("token-b");
                d.authorize("worker-a");
                action
            })
            .unwrap();
        assert_eq!(
            first_action,
            AuthorizeTokenAction::Update("token-b".to_string())
        );

        let (second_downstream, mut second_rx_outgoing) =
            build_test_downstream_with_shared_state(2, shared_token_state.clone(), tx_update_token);
        let result = Downstream::handle_incoming_sv1(
            second_downstream.clone(),
            authorize_request(7, "worker-b", "token-c"),
        )
        .await;

        let (token, disconnect_requested) = second_downstream
            .safe_lock(|d| {
                (
                    d.token.safe_lock(|t| t.clone()).unwrap(),
                    d.disconnect_requested.get(),
                )
            })
            .unwrap();
        let (current_token, change_count) = shared_token_state
            .safe_lock(|state| (state.current_token.clone(), state.change_count))
            .unwrap();

        dbg!(shared_token_state);
        assert!(matches!(result, Err(Error::DisconnectDownstream)));
        assert_eq!(token, "token-b");
        assert_eq!(current_token, "token-b");
        assert_eq!(change_count, 2);
        assert!(!disconnect_requested);
        assert!(second_rx_outgoing.try_recv().is_err());
        assert!(rx_update_token.try_recv().is_err());
    }
}
