use crate::{
    api::stats::StatsSender,
    config::Configuration,
    debug_timing::DownstreamSessionTiming,
    monitor::{
        shares::{RejectionReason, ShareInfo, SharesMonitor},
        MonitorAPI,
    },
    proxy_state::{DownstreamType, ProxyState, UpstreamType},
    share_log_enabled,
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
    send_to_downstream::start_send_to_downstream, DownstreamMessages, SubmitShareResultReceiver,
    SubmitShareWithChannelId,
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
    sync::Arc,
};
use sv1_api::{
    client_to_server, json_rpc, server_to_client,
    utils::{Extranonce, HexU32Be},
    IsServer,
};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct DownstreamDifficultyConfig {
    pub estimated_downstream_hash_rate: f32,
    pub submits: VecDeque<std::time::Instant>,
    pub pid_controller: Pid<f32>,
    pub current_difficulties: VecDeque<f32>,
    pub initial_difficulty: f32,
    pub hard_minimum_difficulty: Option<f32>,
}

impl DownstreamDifficultyConfig {
    pub fn share_count(&mut self) -> Option<f32> {
        let now = std::time::Instant::now();
        while let Some(&oldest) = self.submits.front() {
            let elapsed = now.duration_since(oldest);
            if elapsed < std::time::Duration::from_secs(20) {
                return None;
            }
            if elapsed < std::time::Duration::from_secs(60) {
                return Some(
                    self.submits.len() as f32 / (elapsed.as_millis() as f32 / (60.0 * 1000.0)),
                );
            }
            self.submits.pop_front();
        }
        Some(0.0)
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
    pub session_timing: std::cell::RefCell<DownstreamSessionTiming>,
    submit_counts_for_diff: Cell<bool>,
    channel_hashrate_registered: Cell<bool>,
    closed: Cell<bool>,
}

impl Downstream {
    /// Instantiate a new `Downstream`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_downstream(
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
        hard_minimum_difficulty: Option<f32>,
        stats_sender: StatsSender,
        tx_update_token: Sender<String>,
        accepted_at: std::time::Instant,
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
            hard_minimum_difficulty,
        };

        let token = Arc::new(Mutex::new(
            Configuration::token().expect("Token is not set"),
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
            share_monitor: SharesMonitor::new(connection_id, token.clone()),
            user_agent: std::cell::RefCell::new(String::new()),
            token,
            tx_update_token,
            session_timing: std::cell::RefCell::new(DownstreamSessionTiming::new(accepted_at)),
            submit_counts_for_diff: Cell::new(false),
            channel_hashrate_registered: Cell::new(false),
            closed: Cell::new(false),
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

        // Notify/bootstrap startup is not required before the miner can receive
        // subscribe/authorize responses. Defer it off the connection-open critical path so
        // startup slots are held only for the bidirectional SV1 relay tasks.
        let bootstrap_registration_manager = task_manager.clone();
        let bootstrap_handle = tokio::spawn(async move {
            let should_skip_bootstrap =
                downstream.safe_lock(|d| d.is_closed()).unwrap_or_else(|e| {
                    error!("Failed to read downstream closed state for bootstrap: {e}");
                    ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                    true
                });
            if should_skip_bootstrap {
                return;
            }

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
        });
        if let Err(e) = TaskManager::add_bootstrap(
            bootstrap_registration_manager,
            bootstrap_handle.into(),
            connection_id,
        )
        .await
        {
            error!("Failed to register bootstrap task for downstream {connection_id}: {e:?}");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
        }
    }

    /// Accept connections from one or more SV1 Downstream roles (SV1 Mining Devices) and create a
    /// new `Downstream` for each connection.
    pub async fn accept_connections(
        tx_sv1_submit: Sender<DownstreamMessages>,
        tx_mining_notify: broadcast::Sender<server_to_client::Notify<'static>>,
        bridge: Arc<Mutex<super::super::proxy::Bridge>>,
        upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
        downstreams: Receiver<crate::DownstreamConnection>,
        stats_sender: StatsSender,
        tx_update_token: Sender<String>,
    ) -> Result<AbortOnDrop, Error<'static>> {
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
        if let json_rpc::Message::StandardRequest(request) = &message_sv1 {
            if request.method == "mining.submit" {
                let submit = match client_to_server::Submit::try_from(request.clone()) {
                    Ok(submit) => submit,
                    Err(_) => {
                        Self::reject_invalid_submit(self_, &message_sv1).await?;
                        return Ok(());
                    }
                };

                let is_valid_submission = self_.safe_lock(|s| {
                    let has_valid_version_bits = match &submit.version_bits {
                        Some(version_bits) => s
                            .version_rolling_mask()
                            .map(|mask| mask.check_mask(version_bits))
                            .unwrap_or(false),
                        None => s.version_rolling_mask().is_none(),
                    };

                    s.is_authorized(&submit.user_name)
                        && s.extranonce2_size() == submit.extra_nonce2.len()
                        && has_valid_version_bits
                })?;

                if !is_valid_submission {
                    Self::reject_invalid_submit(self_, &message_sv1).await?;
                    return Ok(());
                }

                return Self::handle_submit_request(self_, submit).await;
            }
        }

        // `handle_message` in `IsServer` trait + calls `handle_request`
        // TODO: Map err from V1Error to Error::V1Error

        let response = self_.safe_lock(|s| s.handle_message(message_sv1.clone()))?;
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
                if matches!(e, sv1_api::error::Error::InvalidSubmission) {
                    Self::reject_invalid_submit(self_, &message_sv1).await?;
                    return Ok(());
                }
                error!("{e}");
                Err(Error::V1Protocol(Box::new(e)))
            }
        }
    }

    fn invalid_submit_rejection(message_sv1: &json_rpc::Message) -> Option<json_rpc::Message> {
        match message_sv1 {
            json_rpc::Message::StandardRequest(request) if request.method == "mining.submit" => {
                Some(
                    json_rpc::Response {
                        id: request.id,
                        result: serde_json::to_value(false)
                            .expect("bool serialization is infallible"),
                        error: None,
                    }
                    .into(),
                )
            }
            _ => None,
        }
    }

    fn invalid_submit_reason(&self, message_sv1: &json_rpc::Message) -> String {
        let request = match message_sv1 {
            json_rpc::Message::StandardRequest(request) if request.method == "mining.submit" => {
                request
            }
            _ => return "message is not mining.submit".to_string(),
        };

        let submit = match client_to_server::Submit::try_from(request.clone()) {
            Ok(submit) => submit,
            Err(e) => {
                return format!("failed to parse mining.submit after InvalidSubmission: {e:?}",)
            }
        };

        let mut reasons = Vec::new();

        if !self.is_authorized(&submit.user_name) {
            reasons.push(format!("unauthorized worker `{}`", submit.user_name));
        }

        let expected_extranonce2_len = self.extranonce2_size();
        let received_extranonce2_len = submit.extra_nonce2.len();
        if expected_extranonce2_len != received_extranonce2_len {
            reasons.push(format!(
                "invalid extranonce2 length: expected {expected_extranonce2_len}, got {received_extranonce2_len}"
            ));
        }

        match (self.version_rolling_mask(), submit.version_bits.clone()) {
            (Some(mask), Some(version_bits)) if !mask.check_mask(&version_bits) => {
                reasons.push(format!(
                    "invalid version_bits `{:08x}` for version-rolling mask `{:08x}`",
                    version_bits.0, mask.0
                ))
            }
            (Some(mask), None) => reasons.push(format!(
                "missing version_bits while version-rolling mask `{:08x}` is configured",
                mask.0
            )),
            (None, Some(version_bits)) => reasons.push(format!(
                "unexpected version_bits `{:08x}` when version rolling is not enabled",
                version_bits.0
            )),
            _ => {}
        }

        if reasons.is_empty() {
            "validation failed but no local InvalidSubmission reason matched".to_string()
        } else {
            reasons.join("; ")
        }
    }

    fn invalid_submit_share(message_sv1: &json_rpc::Message) -> Option<ShareInfo> {
        let request = match message_sv1 {
            json_rpc::Message::StandardRequest(request) if request.method == "mining.submit" => {
                request
            }
            _ => return None,
        };

        if let Ok(submit) = client_to_server::Submit::try_from(request.clone()) {
            let worker_name = submit.user_name.trim();
            if worker_name.is_empty() {
                return None;
            }

            let job_id = submit.job_id.parse::<i64>().unwrap_or_default();
            return Some(ShareInfo::new(
                worker_name.to_string(),
                None,
                job_id,
                submit.nonce.0 as i64,
                Some(RejectionReason::InvalidShare),
            ));
        }

        let params = request.params.as_array()?;
        let worker_name = params.first()?.as_str()?.trim();
        if worker_name.is_empty() {
            return None;
        }

        let job_id = params
            .get(1)
            .and_then(|value| {
                value
                    .as_str()
                    .and_then(|job_id| job_id.parse::<i64>().ok())
                    .or_else(|| value.as_i64())
            })
            .unwrap_or_default();
        let nonce = params
            .get(4)
            .and_then(|value| {
                value
                    .as_u64()
                    .map(|nonce| nonce.min(i64::MAX as u64) as i64)
                    .or_else(|| {
                        value
                            .as_str()
                            .and_then(|nonce| {
                                u32::from_str_radix(nonce.trim_start_matches("0x"), 16).ok()
                            })
                            .map(i64::from)
                    })
            })
            .unwrap_or_default();

        Some(ShareInfo::new(
            worker_name.to_string(),
            None,
            job_id,
            nonce,
            Some(RejectionReason::InvalidShare),
        ))
    }

    pub(super) fn current_difficulty(&self) -> f32 {
        self.difficulty_mgmt
            .current_difficulties
            .back()
            .copied()
            .unwrap_or(self.difficulty_mgmt.initial_difficulty)
    }

    async fn reject_invalid_submit(
        self_: Arc<Mutex<Self>>,
        message_sv1: &json_rpc::Message,
    ) -> Result<(), Error<'static>> {
        if let Some(response) = Self::invalid_submit_rejection(message_sv1) {
            let invalid_share = Self::invalid_submit_share(message_sv1);
            let (connection_id, reason, share_monitor) = self_.safe_lock(|s| {
                s.stats_sender.update_rejected_shares(s.connection_id);
                (
                    s.connection_id,
                    s.invalid_submit_reason(message_sv1),
                    s.share_monitor.clone(),
                )
            })?;
            if let Some(share) = invalid_share {
                share_monitor.insert_share(share);
            }
            error!(
                "Downstream {}: rejected invalid mining.submit without disconnecting downstream: {}",
                connection_id, reason
            );
            Self::send_message_downstream(self_, response).await;
        }
        Ok(())
    }

    fn record_downstream_valid_share(
        &self,
        worker_name: String,
        difficulty: f32,
        job_id: i64,
        nonce: i64,
    ) {
        let share = ShareInfo::new(worker_name, Some(difficulty), job_id, nonce, None);
        self.share_monitor.insert_share(share);
        self.stats_sender.update_accepted_shares(self.connection_id);
    }

    fn spawn_submit_accounting(
        response_rx: SubmitShareResultReceiver,
        difficulty: f32,
        request_job_id: String,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let submit_result = match response_rx.await {
                Ok(result) => result,
                Err(_) => Err(RejectionReason::UpstreamRejected),
            };

            match submit_result {
                Ok(()) => {
                    if share_log_enabled() {
                        info!(
                            "Share for Job {} and difficulty {} is accepted upstream",
                            request_job_id, difficulty
                        );
                    }
                }
                Err(reason) => {
                    if share_log_enabled() {
                        info!(
                            "Share for Job {} and difficulty {} was rejected with {:?}",
                            request_job_id, difficulty, reason
                        );
                    }
                }
            }
        })
    }

    async fn handle_submit_request(
        self_: Arc<Mutex<Self>>,
        request: client_to_server::Submit<'static>,
    ) -> Result<(), Error<'static>> {
        if share_log_enabled() {
            info!(
                "Handling mining.submit request {} from {} with job_id {}, nonce: {:?}",
                request.id, request.user_name, request.job_id, request.nonce
            );
        }

        let job_id_as_number = match request.job_id.parse::<u32>() {
            Ok(job_id) => job_id,
            Err(_) => {
                error!(
                    "Share rejected: can not convert v1 job id to number. v1 id: {}",
                    request.job_id
                );
                let nonce = request.nonce.0 as i64;
                let share = ShareInfo::new(
                    request.user_name.clone(),
                    None,
                    0,
                    nonce,
                    Some(RejectionReason::InvalidJobIdFormat),
                );
                self_.safe_lock(|s| {
                    s.share_monitor.insert_share(share);
                    s.stats_sender.update_rejected_shares(s.connection_id);
                })?;
                Self::send_message_downstream(self_, request.respond(false).into()).await;
                return Ok(());
            }
        };

        let job_id = job_id_as_number as i64;
        let nonce = request.nonce.0 as i64;
        let connection_id = self_.safe_lock(|s| s.connection_id)?;
        crate::translator::utils::update_share_count(connection_id);

        let (job, extranonce1, version_rolling_mask) = self_.safe_lock(|s| {
            (
                s.recent_jobs.get_matching_job(job_id_as_number),
                s.extranonce1.clone(),
                s.version_rolling_mask.clone(),
            )
        })?;

        let job = match job {
            Some(job) => job,
            None => {
                let share = ShareInfo::new(
                    request.user_name.clone(),
                    None,
                    job_id,
                    nonce,
                    Some(RejectionReason::JobIdNotFound),
                );
                self_.safe_lock(|s| {
                    s.share_monitor.insert_share(share);
                    s.stats_sender.update_rejected_shares(s.connection_id);
                })?;
                error!(
                    "Share rejected: can not find job with id {}",
                    request.job_id
                );
                Self::send_message_downstream(self_, request.respond(false).into()).await;
                return Ok(());
            }
        };

        let mut upstream_request = request.clone();
        upstream_request.job_id = job.notify.job_id.clone();

        if !validate_share(
            &upstream_request,
            &job.notify,
            job.difficulty,
            extranonce1,
            version_rolling_mask,
        ) {
            let share = ShareInfo::new(
                request.user_name.clone(),
                None,
                job_id,
                nonce,
                Some(RejectionReason::InvalidShare),
            );
            self_.safe_lock(|s| {
                s.share_monitor.insert_share(share);
                s.stats_sender.update_rejected_shares(s.connection_id);
            })?;
            error!("Share rejected: Invalid share for stored job");
            Self::send_message_downstream(self_, request.respond(false).into()).await;
            return Ok(());
        }

        match Self::forward_submit_share(&self_, upstream_request).await {
            Ok(response_rx) => {
                self_.safe_lock(|s| {
                    s.record_downstream_valid_share(
                        request.user_name.clone(),
                        job.difficulty,
                        job_id,
                        nonce,
                    )
                })?;
                std::mem::drop(Self::spawn_submit_accounting(
                    response_rx,
                    job.difficulty,
                    request.job_id.clone(),
                ));
                self_.safe_lock(|s| s.submit_counts_for_diff.set(true))?;
                Self::send_message_downstream(self_, request.respond(true).into()).await;
            }
            Err(e) => {
                error!("Failed to forward downstream submit: {e}");
                let share = ShareInfo::new(
                    request.user_name.clone(),
                    None,
                    job_id,
                    nonce,
                    Some(RejectionReason::UpstreamRejected),
                );
                self_.safe_lock(|s| {
                    s.share_monitor.insert_share(share);
                    s.stats_sender.update_rejected_shares(s.connection_id);
                })?;
                Self::send_message_downstream(self_, request.respond(false).into()).await;
            }
        }

        Ok(())
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

    async fn forward_submit_share(
        self_: &Arc<Mutex<Self>>,
        request: client_to_server::Submit<'static>,
    ) -> Result<SubmitShareResultReceiver, Error<'static>> {
        let (channel_id, extranonce, extranonce2_len, version_rolling_mask, sender) = self_
            .safe_lock(|s| {
                (
                    s.connection_id,
                    s.extranonce1.clone(),
                    s.extranonce2_len,
                    s.version_rolling_mask.clone(),
                    s.tx_sv1_bridge.clone(),
                )
            })?;
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let to_send = SubmitShareWithChannelId {
            channel_id,
            share: request,
            extranonce,
            extranonce2_len,
            version_rolling_mask,
            result_tx,
        };

        sender
            .send(DownstreamMessages::SubmitShares(to_send))
            .await
            .map_err(|_| Error::AsyncChannelError)?;

        Ok(result_rx)
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
        tx_update_token: Sender<String>,
    ) -> Self {
        use crate::monitor::shares::SharesMonitor;

        let token = Arc::new(Mutex::new(String::new()));
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
            share_monitor: SharesMonitor::new(connection_id, token.clone()),
            user_agent: std::cell::RefCell::new(String::new()),
            token,
            tx_update_token,
            session_timing: std::cell::RefCell::new(DownstreamSessionTiming::new(
                std::time::Instant::now(),
            )),
            submit_counts_for_diff: Cell::new(false),
            channel_hashrate_registered: Cell::new(false),
            closed: Cell::new(false),
        }
    }

    pub(super) fn reset_submit_diff_count_flag(&self) {
        self.submit_counts_for_diff.set(false);
    }

    pub(super) fn take_submit_diff_count_flag(&self) -> bool {
        self.submit_counts_for_diff.replace(false)
    }

    pub(super) fn mark_channel_hashrate_registered(&self) {
        self.channel_hashrate_registered.set(true);
    }

    pub(super) fn take_channel_hashrate_registered(&self) -> bool {
        self.channel_hashrate_registered.replace(false)
    }

    pub(super) fn mark_closed(&self) {
        self.closed.set(true);
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed.get()
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
        debug!("Down: Handling mining.configure: {:?}", &request);
        let (version_rolling_mask, version_rolling_min_bit_count) =
            crate::shared::utils::sv1_rolling(request);

        self.version_rolling_mask = Some(version_rolling_mask.clone());
        self.version_rolling_min_bit = Some(version_rolling_min_bit_count.clone());
        let mut first_job = self.first_job.clone();
        self.recent_jobs.add_job(
            &mut first_job,
            self.version_rolling_mask.clone(),
            self.current_difficulty(),
        );
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
        debug!("Down: Handling mining.subscribe: {:?}", &request);
        self.session_timing
            .borrow_mut()
            .record_subscribe_if_needed();
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
                if let Err(e) = self.token.safe_lock(|t| *t = new_token.clone()) {
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
            } else {
                error!("Downstream is expected to carry a token");
                return false;
            }

            let token = self.token.safe_lock(|t| t.clone()).unwrap_or_else(|e| {
                error!("Failed to read token: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                String::new()
            });
            MonitorAPI::worker_connected(self.connection_id, request.name.clone(), token);

            self.session_timing
                .borrow_mut()
                .record_authorize_if_needed();

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
    fn handle_submit(&self, _request: &client_to_server::Submit<'static>) -> bool {
        error!("Downstream::handle_submit should not be called directly");
        false
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

const TRACKED_RECENT_JOBS: usize = 3;

#[derive(Debug, Clone)]
pub(crate) struct IssuedJob {
    pub notify: Notify<'static>,
    pub difficulty: f32,
}

#[derive(Debug)]
pub struct RecentJobs {
    v1_to_v2: HashMap<u32, u32>,
    v2_to_v1: HashMap<u32, Vec<u32>>,
    issued_jobs: HashMap<u32, IssuedJob>,
    jobs: VecDeque<Notify<'static>>,
    last_v2s: CircularBuffer<u32, TRACKED_RECENT_JOBS>,
    tracked_jobs: usize,
}
fn apply_mask(mask: Option<HexU32Be>, message: &mut server_to_client::Notify<'static>) {
    if let Some(mask) = mask {
        message.version = HexU32Be(message.version.0 & !mask.0);
    }
}
impl RecentJobs {
    pub(crate) fn add_job(
        &mut self,
        notify: &mut Notify<'static>,
        mask: Option<HexU32Be>,
        difficulty: f32,
    ) {
        apply_mask(mask, notify);
        // save it with the v2 id
        self.jobs.push_back(notify.clone());
        let new_id = self.new_v1(
            notify.job_id.parse::<u32>().unwrap(),
            notify.clone(),
            difficulty,
        );
        // send it with the v1 id
        notify.job_id = new_id.to_string();
        if self.jobs.len() > self.tracked_jobs {
            self.jobs.pop_front();
        };
    }

    pub(crate) fn clone_last(&mut self, difficulty: f32) -> Option<Notify<'static>> {
        if let Some(job) = self.jobs.back() {
            let mut job = job.clone();
            let new_id = self.new_v1(job.job_id.parse::<u32>().unwrap(), job.clone(), difficulty);
            job.job_id = new_id.to_string();
            Some(job.clone())
        } else {
            None
        }
    }

    pub(crate) fn current_jobs(&self) -> VecDeque<Notify<'static>> {
        self.jobs.clone()
    }

    pub(crate) fn get_matching_job(&self, v1_id: u32) -> Option<IssuedJob> {
        self.issued_jobs.get(&v1_id).cloned()
    }

    fn new_v1(&mut self, v2_id: u32, notify: Notify<'static>, difficulty: f32) -> u32 {
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
        self.issued_jobs
            .insert(v1_id, IssuedJob { notify, difficulty });
        v1_id
    }
    fn remove_v2(&mut self, v2_id: u32) {
        if let Some(v1_ids) = self.v2_to_v1.remove(&v2_id) {
            for v1_id in v1_ids {
                self.v1_to_v2.remove(&v1_id);
                self.issued_jobs.remove(&v1_id);
            }
        }
    }
    pub fn new() -> Self {
        Self {
            v1_to_v2: HashMap::new(),
            v2_to_v1: HashMap::new(),
            issued_jobs: HashMap::new(),
            last_v2s: CircularBuffer::new(),
            jobs: VecDeque::new(),
            tracked_jobs: TRACKED_RECENT_JOBS,
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
    use crate::{
        api::stats::StatsSender,
        translator::{
            downstream::{receive_from_downstream::process_incoming_message, DownstreamMessages},
            upstream::diff_management::UpstreamDifficultyConfig,
        },
    };
    use pid::Pid;
    use roles_logic_sv2::utils::Mutex;
    use std::{collections::VecDeque, sync::Arc, time::Duration};
    use sv1_api::{
        client_to_server,
        server_to_client::Notify,
        utils::{Extranonce, MerkleNode, PrevHash},
    };
    use tokio::{
        sync::mpsc::{channel, Receiver},
        time::timeout,
    };

    static TEST_TOKEN_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    fn first_job(job_id: &str) -> Notify<'static> {
        Notify {
            job_id: job_id.to_string(),
            prev_hash: PrevHash::try_from("0".repeat(64).as_str()).unwrap(),
            coin_base1: "ffff".try_into().unwrap(),
            coin_base2: "ffff".try_into().unwrap(),
            merkle_branch: vec![MerkleNode::try_from(vec![1_u8; 32]).unwrap()],
            version: HexU32Be(5667),
            bits: HexU32Be(5678),
            time: HexU32Be(5609),
            clean_jobs: true,
        }
    }

    async fn test_downstream(
        authorized_names: Vec<String>,
        extranonce2_len: usize,
        version_rolling_mask: Option<HexU32Be>,
    ) -> (
        Arc<Mutex<Downstream>>,
        Receiver<json_rpc::Message>,
        StatsSender,
    ) {
        let mut current_difficulties = VecDeque::new();
        current_difficulties.push_back(1.0);
        let difficulty_mgmt = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate: 1.0,
            submits: VecDeque::new(),
            pid_controller: Pid::new(*crate::SHARE_PER_MIN, 10.0),
            current_difficulties,
            initial_difficulty: 1.0,
            hard_minimum_difficulty: None,
        };
        let (upstream_config, _rx) =
            UpstreamDifficultyConfig::new(crate::CHANNEL_DIFF_UPDTATE_INTERVAL, 0.0);
        let (tx_sv1_submit, _rx_sv1_submit) = channel::<DownstreamMessages>(8);
        let (tx_outgoing, rx_outgoing) = channel(8);
        let (tx_update_token, _rx_update_token) = channel(8);
        let stats_sender = StatsSender::new();
        stats_sender.setup_stats_reliable(1).await.unwrap();

        let downstream = Arc::new(Mutex::new(Downstream::new(
            1,
            authorized_names,
            vec![],
            version_rolling_mask,
            None,
            tx_sv1_submit,
            tx_outgoing,
            extranonce2_len,
            difficulty_mgmt,
            Arc::new(Mutex::new(upstream_config)),
            stats_sender.clone(),
            first_job("42"),
            tx_update_token,
        )));
        let token = format!(
            "test-token-{}",
            TEST_TOKEN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        downstream
            .safe_lock(|d| d.token.safe_lock(|t| *t = token).unwrap())
            .unwrap();

        (downstream, rx_outgoing, stats_sender)
    }

    async fn assert_invalid_submit_is_rejected(
        downstream: Arc<Mutex<Downstream>>,
        rx_outgoing: &mut Receiver<json_rpc::Message>,
        stats_sender: &StatsSender,
        submit: client_to_server::Submit<'static>,
        expected_rejected_shares: u64,
    ) {
        let submit_id = submit.id;
        let worker_name = submit.user_name.clone();
        let token = downstream
            .safe_lock(|d| d.token.safe_lock(|t| t.clone()).unwrap())
            .unwrap();
        let previous_invalid_total = MonitorAPI::worker_summary_totals(&token, &worker_name)
            .map_or(0, |(_, invalid)| invalid);

        let result = Downstream::handle_incoming_sv1(downstream, submit.into()).await;
        assert!(result.is_ok());

        let response = timeout(Duration::from_secs(1), rx_outgoing.recv())
            .await
            .unwrap()
            .unwrap();
        let response = serde_json::to_value(&response).unwrap();
        assert_eq!(response["id"], serde_json::json!(submit_id));
        assert_eq!(response["result"], serde_json::json!(false));
        assert!(response["error"].is_null());

        let stats = stats_sender.collect_stats().await.unwrap();
        assert_eq!(
            stats.get(&1).unwrap().rejected_shares,
            expected_rejected_shares
        );
        assert_eq!(
            MonitorAPI::worker_summary_totals(&token, &worker_name)
                .map_or(0, |(_, invalid)| invalid),
            previous_invalid_total + 1
        );
    }

    #[tokio::test]
    async fn invalid_extranonce2_submit_returns_false_without_erroring() {
        let (downstream, mut rx_outgoing, stats_sender) =
            test_downstream(vec!["worker".to_string()], 4, None).await;

        assert_invalid_submit_is_rejected(
            downstream,
            &mut rx_outgoing,
            &stats_sender,
            client_to_server::Submit {
                user_name: "worker".to_string(),
                job_id: "42".to_string(),
                extra_nonce2: Extranonce::try_from(vec![1_u8]).unwrap(),
                time: HexU32Be(5609),
                nonce: HexU32Be(1),
                version_bits: None,
                id: 7,
            },
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_submit_does_not_count_for_difficulty_adjustment() {
        let (downstream, mut rx_outgoing, stats_sender) =
            test_downstream(vec!["worker".to_string()], 4, None).await;

        let submit: json_rpc::Message = client_to_server::Submit {
            user_name: "worker".to_string(),
            job_id: "42".to_string(),
            extra_nonce2: Extranonce::try_from(vec![1_u8]).unwrap(),
            time: HexU32Be(5609),
            nonce: HexU32Be(11),
            version_bits: None,
            id: 11,
        }
        .into();

        process_incoming_message(downstream.clone(), submit)
            .await
            .unwrap();

        let response = timeout(Duration::from_secs(1), rx_outgoing.recv())
            .await
            .unwrap()
            .unwrap();
        let response = serde_json::to_value(&response).unwrap();
        assert_eq!(response["id"], serde_json::json!(11));
        assert_eq!(response["result"], serde_json::json!(false));
        assert!(response["error"].is_null());

        let stats = stats_sender.collect_stats().await.unwrap();
        assert_eq!(stats.get(&1).unwrap().rejected_shares, 1);

        let counted_shares = downstream
            .safe_lock(|d| d.difficulty_mgmt.submits.len())
            .unwrap();
        assert_eq!(counted_shares, 0);
    }

    #[tokio::test]
    async fn upstream_rejection_after_downstream_accept_does_not_invalidate_worker_summary() {
        let (downstream, _rx_outgoing, stats_sender) =
            test_downstream(vec!["worker".to_string()], 4, None).await;
        let token = downstream
            .safe_lock(|d| d.token.safe_lock(|t| t.clone()).unwrap())
            .unwrap();
        let previous_totals = MonitorAPI::worker_summary_totals(&token, "worker").unwrap_or((0, 0));

        downstream
            .safe_lock(|d| {
                d.record_downstream_valid_share("worker".to_string(), 1.0, 42, 11);
            })
            .unwrap();

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let handle = Downstream::spawn_submit_accounting(result_rx, 1.0, "42".to_string());
        result_tx
            .send(Err(RejectionReason::UpstreamRejected))
            .unwrap();
        handle.await.unwrap();

        assert_eq!(
            MonitorAPI::worker_summary_totals(&token, "worker"),
            Some((previous_totals.0 + 1, previous_totals.1))
        );

        let stats = stats_sender.collect_stats().await.unwrap();
        assert_eq!(stats.get(&1).unwrap().accepted_shares, 1);
        assert_eq!(stats.get(&1).unwrap().rejected_shares, 0);
    }

    #[tokio::test]
    async fn unauthorized_submit_returns_false_without_erroring() {
        let (downstream, mut rx_outgoing, stats_sender) = test_downstream(vec![], 4, None).await;

        assert_invalid_submit_is_rejected(
            downstream,
            &mut rx_outgoing,
            &stats_sender,
            client_to_server::Submit {
                user_name: "intruder".to_string(),
                job_id: "42".to_string(),
                extra_nonce2: Extranonce::try_from(vec![1_u8; 4]).unwrap(),
                time: HexU32Be(5609),
                nonce: HexU32Be(2),
                version_bits: None,
                id: 8,
            },
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn invalid_version_bits_submit_returns_false_without_erroring() {
        let (downstream, mut rx_outgoing, stats_sender) =
            test_downstream(vec!["worker".to_string()], 4, Some(HexU32Be(0x0000_0001))).await;

        assert_invalid_submit_is_rejected(
            downstream,
            &mut rx_outgoing,
            &stats_sender,
            client_to_server::Submit {
                user_name: "worker".to_string(),
                job_id: "42".to_string(),
                extra_nonce2: Extranonce::try_from(vec![1_u8; 4]).unwrap(),
                time: HexU32Be(5609),
                nonce: HexU32Be(3),
                version_bits: Some(HexU32Be(0x0000_0002)),
                id: 9,
            },
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn malformed_submit_with_worker_name_counts_as_invalid_share() {
        let (downstream, mut rx_outgoing, stats_sender) =
            test_downstream(vec!["worker".to_string()], 4, None).await;
        let token = downstream
            .safe_lock(|d| d.token.safe_lock(|t| t.clone()).unwrap())
            .unwrap();
        let previous_invalid_total =
            MonitorAPI::worker_summary_totals(&token, "worker").map_or(0, |(_, invalid)| invalid);

        let message = json_rpc::Message::StandardRequest(json_rpc::StandardRequest {
            id: 12,
            method: "mining.submit".to_string(),
            params: serde_json::json!(["worker", "42", "not-hex", "000015e9", "0000000c"]),
        });

        let result = Downstream::handle_incoming_sv1(downstream, message).await;
        assert!(result.is_ok());

        let response = timeout(Duration::from_secs(1), rx_outgoing.recv())
            .await
            .unwrap()
            .unwrap();
        let response = serde_json::to_value(&response).unwrap();
        assert_eq!(response["id"], serde_json::json!(12));
        assert_eq!(response["result"], serde_json::json!(false));
        assert!(response["error"].is_null());

        let stats = stats_sender.collect_stats().await.unwrap();
        assert_eq!(stats.get(&1).unwrap().rejected_shares, 1);
        assert_eq!(
            MonitorAPI::worker_summary_totals(&token, "worker").map_or(0, |(_, invalid)| invalid),
            previous_invalid_total + 1
        );
    }

    #[tokio::test]
    async fn invalid_submit_reason_reports_extranonce_length() {
        let (downstream, _rx_outgoing, _stats_sender) =
            test_downstream(vec!["worker".to_string()], 4, None).await;
        let message: json_rpc::Message = client_to_server::Submit {
            user_name: "worker".to_string(),
            job_id: "42".to_string(),
            extra_nonce2: Extranonce::try_from(vec![1_u8]).unwrap(),
            time: HexU32Be(5609),
            nonce: HexU32Be(1),
            version_bits: None,
            id: 7,
        }
        .into();

        let reason = downstream
            .safe_lock(|d| d.invalid_submit_reason(&message))
            .unwrap();

        assert_eq!(reason, "invalid extranonce2 length: expected 4, got 1");
    }

    #[tokio::test]
    async fn invalid_submit_reason_reports_unauthorized_worker() {
        let (downstream, _rx_outgoing, _stats_sender) = test_downstream(vec![], 4, None).await;
        let message: json_rpc::Message = client_to_server::Submit {
            user_name: "intruder".to_string(),
            job_id: "42".to_string(),
            extra_nonce2: Extranonce::try_from(vec![1_u8; 4]).unwrap(),
            time: HexU32Be(5609),
            nonce: HexU32Be(2),
            version_bits: None,
            id: 8,
        }
        .into();

        let reason = downstream
            .safe_lock(|d| d.invalid_submit_reason(&message))
            .unwrap();

        assert_eq!(reason, "unauthorized worker `intruder`");
    }

    #[tokio::test]
    async fn invalid_submit_reason_reports_version_bits() {
        let (downstream, _rx_outgoing, _stats_sender) =
            test_downstream(vec!["worker".to_string()], 4, Some(HexU32Be(0x0000_0001))).await;
        let message: json_rpc::Message = client_to_server::Submit {
            user_name: "worker".to_string(),
            job_id: "42".to_string(),
            extra_nonce2: Extranonce::try_from(vec![1_u8; 4]).unwrap(),
            time: HexU32Be(5609),
            nonce: HexU32Be(3),
            version_bits: Some(HexU32Be(0x0000_0002)),
            id: 9,
        }
        .into();

        let reason = downstream
            .safe_lock(|d| d.invalid_submit_reason(&message))
            .unwrap();

        assert_eq!(
            reason,
            "invalid version_bits `00000002` for version-rolling mask `00000001`"
        );
    }

    #[test]
    fn recent_jobs_bind_difficulty_to_each_issued_v1_job() {
        let mut recent_jobs = RecentJobs::new();
        let mut job = first_job("42");

        recent_jobs.add_job(&mut job, None, 1.0);
        let first_v1_id = job.job_id.parse::<u32>().unwrap();
        let first_snapshot = recent_jobs.get_matching_job(first_v1_id).unwrap();
        assert_eq!(first_snapshot.notify.job_id, "42");
        assert_eq!(first_snapshot.difficulty, 1.0);

        let reissued_job = recent_jobs.clone_last(2.0).unwrap();
        let reissued_v1_id = reissued_job.job_id.parse::<u32>().unwrap();
        let reissued_snapshot = recent_jobs.get_matching_job(reissued_v1_id).unwrap();
        let original_snapshot = recent_jobs.get_matching_job(first_v1_id).unwrap();

        assert_eq!(reissued_snapshot.notify.job_id, "42");
        assert_eq!(reissued_snapshot.difficulty, 2.0);
        assert_eq!(original_snapshot.difficulty, 1.0);
    }

    #[test]
    fn recent_jobs_preserve_three_v2_job_retention() {
        let mut recent_jobs = RecentJobs::new();

        let mut job_1 = first_job("1");
        recent_jobs.add_job(&mut job_1, None, 1.0);
        let job_1_v1 = job_1.job_id.parse::<u32>().unwrap();
        let job_1_reissued = recent_jobs.clone_last(2.0).unwrap();
        let job_1_reissued_v1 = job_1_reissued.job_id.parse::<u32>().unwrap();

        let mut job_2 = first_job("2");
        recent_jobs.add_job(&mut job_2, None, 2.0);
        let job_2_v1 = job_2.job_id.parse::<u32>().unwrap();

        let mut job_3 = first_job("3");
        recent_jobs.add_job(&mut job_3, None, 3.0);
        let job_3_v1 = job_3.job_id.parse::<u32>().unwrap();

        let mut job_4 = first_job("4");
        recent_jobs.add_job(&mut job_4, None, 4.0);
        let job_4_v1 = job_4.job_id.parse::<u32>().unwrap();

        assert_eq!(recent_jobs.current_jobs().len(), 3);
        assert!(recent_jobs.get_matching_job(job_1_v1).is_none());
        assert!(recent_jobs.get_matching_job(job_1_reissued_v1).is_none());
        assert!(recent_jobs.get_matching_job(job_2_v1).is_some());
        assert!(recent_jobs.get_matching_job(job_3_v1).is_some());
        assert!(recent_jobs.get_matching_job(job_4_v1).is_some());
    }
}
