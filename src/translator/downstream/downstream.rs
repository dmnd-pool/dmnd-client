use crate::{
    api::stats::StatsSender,
    proxy_state::{DownstreamType, ProxyState},
    shared::utils::AbortOnDrop,
    translator::{
        downstream::diff_management::nearest_power_of_10,
        error::Error,
        utils::{allow_submit_share, validate_share},
    },
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

use std::{collections::VecDeque, net::IpAddr, sync::Arc};
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
    pub current_difficulty: f32,
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
    version_rolling_mask: Option<HexU32Be>,
    /// Minimum version rolling mask bits size
    version_rolling_min_bit: Option<HexU32Be>,
    /// Sends a SV1 `mining.submit` message received from the Downstream role to the `Bridge` for
    /// translation into a SV2 `SubmitSharesExtended`.
    tx_sv1_bridge: Sender<DownstreamMessages>,
    tx_outgoing: Sender<json_rpc::Message>,
    /// True if this is the first job received from `Upstream`.
    pub(super) first_job_received: bool,
    extranonce2_len: usize,
    pub(super) difficulty_mgmt: DownstreamDifficultyConfig,
    pub(super) upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
    pub last_call_to_update_hr: u128,
    pub(super) last_notify: Option<server_to_client::Notify<'static>>,
    pub(super) stats_sender: StatsSender,
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
        stats_sender: StatsSender,
    ) {
        assert!(last_notify.is_some());

        let (tx_outgoing, receiver_outgoing) = channel(crate::TRANSLATOR_BUFFER_SIZE);

        // The initial difficulty is derived from the formula: difficulty = hash_rate / (shares_per_second * 2^32),
        let initial_hash_rate = *crate::EXPECTED_SV1_HASHPOWER;
        let share_per_second = crate::SHARE_PER_MIN / 60.0;
        let initial_difficulty = dbg!(initial_hash_rate / (share_per_second * 2f32.powf(32.0)));
        let initial_difficulty = nearest_power_of_10(initial_difficulty);

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

        let mut pid: Pid<f32> = Pid::new(crate::SHARE_PER_MIN, initial_difficulty * 10.0);
        let pk = -initial_difficulty * 0.01;
        //let pi = initial_difficulty * 0.1;
        //let pd = initial_difficulty * 0.01;
        pid.p(pk, f32::MAX).i(0.0, f32::MAX).d(0.0, f32::MAX);

        let estimated_downstream_hash_rate = *crate::EXPECTED_SV1_HASHPOWER;
        let difficulty_mgmt = DownstreamDifficultyConfig {
            estimated_downstream_hash_rate,
            submits: vec![].into(),
            pid_controller: pid,
            current_difficulty: initial_difficulty,
            initial_difficulty,
        };

        let downstream = Arc::new(Mutex::new(Downstream {
            connection_id,
            authorized_names: vec![],
            extranonce1,
            version_rolling_mask: None,
            version_rolling_min_bit: None,
            tx_sv1_bridge,
            tx_outgoing,
            first_job_received: false,
            extranonce2_len,
            difficulty_mgmt,
            upstream_difficulty_config,
            last_call_to_update_hr: 0,
            last_notify: last_notify.clone(),
            stats_sender,
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
            last_notify,
            host.clone(),
            connection_id,
        )
        .await
        {
            error!("Failed to start notify task: {e}");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
        };
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
                error!("{e}");
                Err(Error::V1Protocol(e))
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
    #[cfg(test)]
    pub fn new(
        connection_id: u32,
        authorized_names: Vec<String>,
        extranonce1: Vec<u8>,
        version_rolling_mask: Option<HexU32Be>,
        version_rolling_min_bit: Option<HexU32Be>,
        tx_sv1_bridge: Sender<DownstreamMessages>,
        tx_outgoing: Sender<json_rpc::Message>,
        first_job_received: bool,
        extranonce2_len: usize,
        difficulty_mgmt: DownstreamDifficultyConfig,
        upstream_difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
        stats_sender: StatsSender,
    ) -> Self {
        Downstream {
            connection_id,
            authorized_names,
            extranonce1,
            version_rolling_mask,
            version_rolling_min_bit,
            tx_sv1_bridge,
            tx_outgoing,
            first_job_received,
            extranonce2_len,
            difficulty_mgmt,
            upstream_difficulty_config,
            last_call_to_update_hr: 0,
            last_notify: None,
            stats_sender,
        }
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

        (
            Some(server_to_client::VersionRollingParams::new(
                    version_rolling_mask,version_rolling_min_bit_count
            ).expect("Version mask invalid, automatic version mask selection not supported, please change it in carte::downstream_sv1::mod.rs")),
            Some(false),
        )
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

        vec![set_difficulty_sub, notify_sub]
    }

    fn handle_authorize(&self, _request: &client_to_server::Authorize) -> bool {
        if self.authorized_names.is_empty() {
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
        info!("Down: Handling mining.submit: {:?}", &request);

        // check first job received
        if !self.first_job_received {
            self.stats_sender.update_rejected_shares(self.connection_id);
            return false;
        }
        //check allowed to send shares
        match allow_submit_share() {
            Ok(true) => {
                let Some(job) = &self.last_notify else {
                    error!("Share rejected: No last job found");
                    self.stats_sender.update_rejected_shares(self.connection_id);
                    return false;
                };
                crate::translator::utils::update_share_count(self.connection_id); // update share count
                                                                                  //check share is valid
                if validate_share(
                    request,
                    job,
                    self.difficulty_mgmt.current_difficulty,
                    self.extranonce1.clone(),
                    self.version_rolling_mask.clone(),
                ) {
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
                    };
                    self.stats_sender.update_accepted_shares(self.connection_id);
                    true
                } else {
                    error!("Share rejected: Invalid share");
                    self.stats_sender.update_rejected_shares(self.connection_id);
                    false
                }
            }
            Ok(false) => {
                warn!("Share rejected: Exceeded 70 shares/min limit");
                self.stats_sender.update_rejected_shares(self.connection_id);
                false
            }
            Err(e) => {
                error!("Failed to record share: {e:?}");
                self.stats_sender.update_rejected_shares(self.connection_id);
                ProxyState::update_inconsistency(Some(1)); // restart proxy
                false
            }
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

    fn notify(&mut self) -> Result<json_rpc::Message, sv1_api::error::Error> {
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

//#[cfg(test)]
//mod tests {
//    use super::*;
//
//    #[test]
//    fn gets_difficulty_from_target() {
//        let target = vec![
//            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 255, 127,
//            0, 0, 0, 0, 0,
//        ];
//        let actual = Downstream::difficulty_from_target(target).unwrap();
//        let expect = 512.0;
//        assert_eq!(actual, expect);
//    }
//}
