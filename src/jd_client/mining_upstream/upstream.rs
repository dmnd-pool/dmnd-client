use crate::jd_client::IS_CUSTOM_JOB_SET;
use crate::proxy_state::{DownstreamType, ProxyState, TpState, UpstreamType};
use crate::{jd_client::error::Error, jd_client::error::ProxyResult, shared::utils::AbortOnDrop};

use crate::jd_client::mining_downstream::{DownstreamJob, DownstreamMiningNode as Downstream};

use binary_sv2::{B064KOwned, Seq0255Owned, U256Owned};
use bitcoin::{consensus::Decodable, Amount, ScriptBuf, TxOut};
use roles_logic_sv2::{
    channel_logic::channel_factory::PoolChannelFactory,
    handlers::mining::SendTo,
    job_declaration_sv2::DeclareMiningJobOwned,
    mining::ExtendedExtranonce,
    mining_sv2::SetCustomMiningJobOwned,
    parsers::MiningOwned,
    template_distribution_sv2::NewTemplateOwned,
    utils::{Id, Mutex},
    Error as RolesLogicError,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{
    mpsc::{Receiver as TReceiver, Sender as TSender},
    Mutex as TokioMutex,
};
use tokio::task;
use tracing::{error, info, warn};

use std::collections::VecDeque;

use super::task_manager::TaskManager;

const DIFFICULTY_COMMITMENT_TAG: &[u8] = b"DIFF32";
const OP_RETURN: u8 = 0x6a;

fn is_difficulty_commitment_script(script: &[u8]) -> bool {
    script.first() == Some(&OP_RETURN)
        && script.get(2..2 + DIFFICULTY_COMMITMENT_TAG.len()) == Some(DIFFICULTY_COMMITMENT_TAG)
}

#[derive(Debug)]
struct CircularBuffer {
    buffer: VecDeque<(u32, u32)>,
    capacity: usize,
}

impl CircularBuffer {
    fn new(capacity: usize) -> Self {
        CircularBuffer {
            buffer: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn insert(&mut self, key: u32, value: u32) {
        self.buffer.retain(|(existing, _)| *existing != key);
        if self.buffer.len() == self.capacity {
            self.buffer.pop_front();
        }
        self.buffer.push_back((key, value));
    }

    fn get(&self, id: u32) -> Option<u32> {
        self.buffer
            .iter()
            .find_map(|&(key, value)| if key == id { Some(value) } else { None })
    }
}

#[derive(Debug)]
struct PendingCustomJob {
    template_id: u64,
    downstream_job: DownstreamJob,
}

impl std::default::Default for CircularBuffer {
    fn default() -> Self {
        Self::new(1000)
    }
}

#[derive(Debug, Default)]
struct TemplateToJobId {
    downstream_to_pool_job_id: CircularBuffer,
    requests: HashMap<u32, PendingCustomJob>,
}

impl TemplateToJobId {
    fn register_request(&mut self, request_id: u32, request: PendingCustomJob) {
        self.requests.insert(request_id, request);
    }

    fn register_job_id(&mut self, downstream_job_id: u32, pool_job_id: u32) {
        self.downstream_to_pool_job_id
            .insert(downstream_job_id, pool_job_id);
    }

    fn get_job_id(&self, downstream_job_id: u32) -> Option<u32> {
        self.downstream_to_pool_job_id.get(downstream_job_id)
    }

    fn take_request(&mut self, request_id: u32) -> Option<PendingCustomJob> {
        self.requests.remove(&request_id)
    }

    fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
pub struct Upstream {
    /// Newly assigned identifier of the channel, stable for the whole lifetime of the connection,
    /// e.g. it is used for broadcasting new jobs by the `NewExtendedMiningJob` message.
    channel_id: Option<u32>,
    target: Option<[u8; 32]>,
    /// This allows the upstream threads to be able to communicate back to the main thread its
    /// current status.
    /// Minimum `extranonce2` size. Initially requested in the `jdc-config.toml`, and ultimately
    /// set by the SV2 Upstream via the SV2 `OpenExtendedMiningChannelSuccess` message.
    pub min_extranonce_size: u16,
    pub upstream_extranonce1_size: usize,
    /// Receives messages from the SV2 Upstream role
    //pub receiver: TReceiver<EitherFrame>,
    /// Sends messages to the SV2 Upstream role
    pub sender: TSender<MiningOwned>,
    pub downstream: Option<Arc<Mutex<Downstream>>>,
    channel_factory: Option<PoolChannelFactory>,
    template_to_job_id: TemplateToJobId,
    custom_job_send_lock: Arc<TokioMutex<()>>,
    req_ids: Id,
}

impl Upstream {
    pub async fn send(self_: &Arc<Mutex<Self>>, message: MiningOwned) -> ProxyResult<()> {
        let sender = self_
            .safe_lock(|s| s.sender.clone())
            .map_err(|_| Error::JdClientUpstreamMutexCorrupted)?;

        sender
            .send(message)
            .await
            .map_err(|_| Error::Unrecoverable)?;
        Ok(())
    }
    /// Instantiate a new `Upstream`.
    /// Connect to the SV2 Upstream role (most typically a SV2 Pool). Initializes the
    /// `UpstreamConnection` with a channel to send and receive messages from the SV2 Upstream
    /// role and uses channels provided in the function arguments to send and receive messages
    /// from the `Downstream`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        min_extranonce_size: u16,
        sender: TSender<MiningOwned>,
    ) -> ProxyResult<Arc<Mutex<Self>>> {
        Ok(Arc::new(Mutex::new(Self {
            channel_id: None,
            target: None,
            min_extranonce_size,
            upstream_extranonce1_size: 16, // 16 is the default since that is the only value the pool supports currently
            sender,
            downstream: None,
            channel_factory: None,
            template_to_job_id: TemplateToJobId::new(),
            custom_job_send_lock: Arc::new(TokioMutex::new(())),
            req_ids: Id::new(),
        })))
    }

    pub(crate) fn apply_difficulty_commitment(
        self_: &Arc<Mutex<Self>>,
        template: &mut NewTemplateOwned,
    ) -> ProxyResult<()> {
        let target = self_
            .safe_lock(|upstream| upstream.target)
            .map_err(|_| Error::JdClientUpstreamMutexCorrupted)?
            .ok_or(Error::Unrecoverable)?;
        let difficulty = bitcoin::Target::from_le_bytes(target).difficulty_float() as f32;
        let mut script = b"\x6a\x0aDIFF32".to_vec();
        script.extend_from_slice(&difficulty.to_le_bytes());
        let commitment = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(script),
        };

        let mut encoded_outputs = template.coinbase_tx_outputs.as_ref();
        let mut outputs = Vec::new();
        while !encoded_outputs.is_empty() {
            let output =
                TxOut::consensus_decode(&mut encoded_outputs).map_err(|_| Error::Unrecoverable)?;
            if !is_difficulty_commitment_script(output.script_pubkey.as_bytes()) {
                outputs.push(output);
            }
        }
        outputs.push(commitment);

        let output_count = u32::try_from(outputs.len()).map_err(|_| Error::Unrecoverable)?;
        let encoded_outputs = outputs
            .iter()
            .flat_map(bitcoin::consensus::serialize)
            .collect::<Vec<_>>()
            .try_into()?;
        template.coinbase_tx_outputs = encoded_outputs;
        template.coinbase_tx_outputs_count = output_count;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn set_custom_jobs(
        self_: &Arc<Mutex<Self>>,
        declare_mining_job: DeclareMiningJobOwned,
        set_new_prev_hash: roles_logic_sv2::template_distribution_sv2::SetNewPrevHashOwned,
        merkle_path: Seq0255Owned<U256Owned>,
        signed_token: binary_sv2::B0255Owned,
        coinbase_tx_version: u32,
        coinbase_prefix: binary_sv2::B0255Owned,
        coinbase_tx_input_n_sequence: u32,
        coinbase_tx_outs: B064KOwned,
        coinbase_tx_locktime: u32,
        template_id: u64,
        downstream_job: DownstreamJob,
    ) -> ProxyResult<()> {
        info!("Sending set custom mining job");
        let send_lock = self_
            .safe_lock(|state| state.custom_job_send_lock.clone())
            .map_err(|_| Error::JdClientUpstreamMutexCorrupted)?;
        let _send_guard = send_lock.lock().await;
        let request_id = self_
            .safe_lock(|s| s.req_ids.next())
            .map_err(|_| Error::JdClientUpstreamMutexCorrupted)?;
        let channel_id = loop {
            if let Some(id) = self_
                .safe_lock(|s| s.channel_id)
                .map_err(|_| Error::JdClientUpstreamMutexCorrupted)?
            {
                break id;
            };
            tokio::task::yield_now().await;
        };

        let to_send = SetCustomMiningJobOwned {
            channel_id,
            request_id,
            token: signed_token,
            version: declare_mining_job.version,
            prev_hash: set_new_prev_hash.prev_hash,
            min_ntime: set_new_prev_hash.header_timestamp,
            nbits: set_new_prev_hash.n_bits,
            coinbase_tx_version,
            coinbase_prefix,
            coinbase_tx_input_n_sequence,
            coinbase_tx_outputs: coinbase_tx_outs,
            coinbase_tx_locktime,
            merkle_path,
        };
        let message = MiningOwned::SetCustomMiningJob(to_send);
        self_
            .safe_lock(|s| {
                s.template_to_job_id.register_request(
                    request_id,
                    PendingCustomJob {
                        template_id,
                        downstream_job,
                    },
                )
            })
            .map_err(|_| Error::JdClientUpstreamMutexCorrupted)?;
        if let Err(error) = Self::send(self_, message).await {
            let _ = self_.safe_lock(|state| {
                state.template_to_job_id.take_request(request_id);
            });
            IS_CUSTOM_JOB_SET.store(true, std::sync::atomic::Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    /// Parses the incoming SV2 message from the Upstream role and routes the message to the
    /// appropriate handler.
    pub async fn parse_incoming(
        self_: Arc<Mutex<Self>>,
        mut recv: TReceiver<MiningOwned>,
    ) -> ProxyResult<AbortOnDrop> {
        let task_manager = TaskManager::initialize();
        let abortable = match task_manager
            .safe_lock(|t| t.get_aborter())
            .map_err(|_| Error::JdClientUpstreamTaskManagerFailed)?
        {
            Some(abortable) => abortable,
            None => {
                return Err(Error::Unrecoverable);
            }
        };
        let main_task = {
            let self_ = self_.clone();
            task::spawn(async move {
                loop {
                    // Waiting to receive a message from the SV2 Upstream role
                    let incoming = match recv.recv().await {
                        Some(msg) => msg,
                        None => {
                            error!("Upstream down");
                            // Update the proxy state to reflect the Tp is down
                            ProxyState::update_tp_state(TpState::Down);
                            break;
                        }
                    };

                    let next_message_to_send = self_
                        .safe_lock(|state| state.handle_owned_message(&incoming))
                        .map_err(|_| RolesLogicError::DownstreamDown)
                        .and_then(|result| result);

                    // Routes the incoming messages accordingly
                    match next_message_to_send {
                        // This is a transparent proxy it will only relay messages as received
                        Ok(SendTo::RelaySameMessageToRemote(downstream_mutex)) => {
                            if Downstream::send(&downstream_mutex, incoming).await.is_err() {
                                error!("Failed to send message downstream");
                                // Update global proxy downstream state
                                ProxyState::update_downstream_state(
                                    DownstreamType::JdClientMiningDownstream,
                                );
                                break;
                            };
                        }
                        Ok(SendTo::None(_)) => (),
                        Ok(_) => unreachable!(),
                        Err(e) => {
                            error!("{e:?}");
                            ProxyState::update_upstream_state(UpstreamType::JDCMiningUpstream);
                            break;
                        }
                    }
                }
            })
        };
        TaskManager::add_main_task(task_manager.clone(), main_task.into())
            .await
            .map_err(|_| Error::JdClientUpstreamTaskManagerFailed)?;
        Ok(abortable)
    }

    pub async fn take_channel_factory(
        self_: Arc<Mutex<Self>>,
    ) -> Result<PoolChannelFactory, Error> {
        while self_
            .safe_lock(|s| s.channel_factory.is_none())
            .map_err(|_| Error::JdClientUpstreamMutexCorrupted)?
        {
            tokio::task::yield_now().await;
        }

        self_
            .safe_lock(|s| {
                let mut factory = None;
                std::mem::swap(&mut s.channel_factory, &mut factory);
                factory.ok_or(Error::Unrecoverable)
            })
            .map_err(|_| Error::JdClientUpstreamMutexCorrupted)?
    }

    pub async fn get_job_id(
        self_: &Arc<Mutex<Self>>,
        downstream_job_id: u32,
    ) -> Result<u32, Error> {
        loop {
            if let Some(id) = self_
                .safe_lock(|s| s.template_to_job_id.get_job_id(downstream_job_id))
                .map_err(|_| Error::JdClientDownstreamMutexCorrupted)?
            {
                return Ok(id);
            }
            tokio::task::yield_now().await;
        }
    }
}

/// Connection-wide SV2 Upstream role messages parser implemented by a downstream ("downstream"
/// here is relative to the SV2 Upstream role and is represented by this `Upstream` struct).
impl Upstream {
    fn handle_owned_message(
        &mut self,
        message: &MiningOwned,
    ) -> Result<SendTo<Downstream>, RolesLogicError> {
        use roles_logic_sv2::parsers::IsSv2Message;
        match message {
            MiningOwned::OpenExtendedMiningChannelSuccess(m) => {
                self.handle_open_extended_mining_channel_success(m.clone())
            }
            MiningOwned::OpenMiningChannelError(m) => {
                self.handle_open_mining_channel_error(m.clone())
            }
            MiningOwned::UpdateChannelError(m) => self.handle_update_channel_error(m.clone()),
            MiningOwned::CloseChannel(m) => self.handle_close_channel(m.clone()),
            MiningOwned::SetExtranoncePrefix(m) => self.handle_set_extranonce_prefix(m.clone()),
            MiningOwned::SubmitSharesSuccess(m) => self.handle_submit_shares_success(m.clone()),
            MiningOwned::SubmitSharesError(m) => self.handle_submit_shares_error(m.clone()),
            MiningOwned::NewExtendedMiningJob(m) => self.handle_new_extended_mining_job(m.clone()),
            MiningOwned::SetNewPrevHash(m) => self.handle_set_new_prev_hash(m.clone()),
            MiningOwned::SetCustomMiningJobSuccess(m) => {
                self.handle_set_custom_mining_job_success(m.clone())
            }
            MiningOwned::SetCustomMiningJobError(m) => {
                self.handle_set_custom_mining_job_error(m.clone())
            }
            MiningOwned::SetTarget(m) => self.handle_set_target(m),
            MiningOwned::SetGroupChannel(_) => self
                .downstream
                .as_ref()
                .cloned()
                .map(SendTo::RelaySameMessageToRemote)
                .ok_or(RolesLogicError::DownstreamDown),
            _ => Err(RolesLogicError::UnexpectedMessage(message.message_type())),
        }
    }

    /// This is a transparent proxy so OpenExtendedMiningChannel is sent as it is downstream.
    /// This message is used also to create a PoolChannelFactory that mock the upstream pool.
    /// this PoolChannelFactory is used by the template provider client in order to check shares
    /// received by downstream using the right extranonce and seeing the same hash that the downstream
    /// saw. PoolChannelFactory coinbase pre and suf are setted by the JD client.
    pub(crate) fn handle_open_extended_mining_channel_success(
        &mut self,
        m: roles_logic_sv2::mining_sv2::OpenExtendedMiningChannelSuccessOwned,
    ) -> Result<SendTo<Downstream>, RolesLogicError> {
        info!("Receive open extended mining channel success");
        let ids = Arc::new(Mutex::new(roles_logic_sv2::utils::GroupId::new()));
        let prefix_len = m.extranonce_prefix.len();
        let self_len = 0;
        let total_len = prefix_len + m.extranonce_size as usize;
        let range_0 = 0..prefix_len;
        let range_1 = prefix_len..prefix_len + self_len;
        let range_2 = prefix_len + self_len..total_len;

        self.target = Some(m.target.to_array());

        let extranonces = ExtendedExtranonce::new(range_0, range_1, range_2);
        let creator = roles_logic_sv2::job_creator::JobsCreators::new(total_len as u8);
        let channel_kind =
            roles_logic_sv2::channel_logic::channel_factory::ExtendedChannelKind::ProxyJd {
                upstream_target: m.target.to_array().into(),
            };
        let mut channel_factory = PoolChannelFactory::new(
            ids,
            extranonces,
            creator,
            *crate::SHARE_PER_MIN,
            channel_kind,
            vec![],
            vec![],
        )
        .inspect_err(|_| {
            error!("Channel extranonce ranges exceed 32 bytes");
        })?;
        self.channel_id = Some(m.channel_id);
        channel_factory.replicate_upstream_extended_channel_only_jd(m)?;
        self.channel_factory = Some(channel_factory);

        let downstream = match self.downstream.as_ref() {
            Some(downstream) => downstream.clone(),
            None => {
                error!("Downstream not available");
                return Err(RolesLogicError::DownstreamDown);
            }
        };

        Ok(SendTo::RelaySameMessageToRemote(downstream))
    }

    /// Handles the SV2 `OpenExtendedMiningChannelError` message (TODO).
    fn handle_open_mining_channel_error(
        &mut self,
        _: roles_logic_sv2::mining_sv2::OpenMiningChannelErrorOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        let downstream = match self.downstream.as_ref() {
            Some(downstream) => downstream.clone(),
            None => {
                error!("Downstream not available");
                return Err(RolesLogicError::DownstreamDown);
            }
        };

        Ok(SendTo::RelaySameMessageToRemote(downstream))
    }

    /// Handles the SV2 `UpdateChannelError` message (TODO).
    fn handle_update_channel_error(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::UpdateChannelErrorOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        let downstream = match self.downstream.as_ref() {
            Some(downstream) => downstream.clone(),
            None => {
                error!("Downstream not available");
                return Err(RolesLogicError::DownstreamDown);
            }
        };

        Ok(SendTo::RelaySameMessageToRemote(downstream))
    }

    /// Handles the SV2 `CloseChannel` message (TODO).
    fn handle_close_channel(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::CloseChannelOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        let downstream = match self.downstream.as_ref() {
            Some(downstream) => downstream.clone(),
            None => {
                error!("Downstream not available");
                return Err(RolesLogicError::DownstreamDown);
            }
        };

        Ok(SendTo::RelaySameMessageToRemote(downstream))
    }

    /// Handles the SV2 `SetExtranoncePrefix` message (TODO).
    fn handle_set_extranonce_prefix(
        &mut self,
        _: roles_logic_sv2::mining_sv2::SetExtranoncePrefixOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        let downstream = match self.downstream.as_ref() {
            Some(downstream) => downstream.clone(),
            None => {
                error!("Downstream not available");
                return Err(RolesLogicError::DownstreamDown);
            }
        };

        Ok(SendTo::RelaySameMessageToRemote(downstream))
    }

    /// Handles the SV2 `SubmitSharesSuccess` message.
    fn handle_submit_shares_success(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::SubmitSharesSuccessOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        if let Some(downstream) = &self.downstream {
            Ok(SendTo::RelaySameMessageToRemote(downstream.clone()))
        } else {
            Err(RolesLogicError::DownstreamDown)
        }
    }

    /// Handles the SV2 `SubmitSharesError` message.
    fn handle_submit_shares_error(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::SubmitSharesErrorOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        // TODO remove the comments when share too low err get fixed
        //self.pool_chaneger_trigger
        //    .safe_lock(|t| t.start(self.tx_status.clone()))
        //    .unwrap();
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `NewExtendedMiningJob` message which is used (along with the SV2
    /// `SetNewPrevHash` message) to later create a SV1 `mining.notify` for the Downstream
    /// role.
    fn handle_new_extended_mining_job(
        &mut self,
        _: roles_logic_sv2::mining_sv2::NewExtendedMiningJobOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        warn!("Extended job received from upstream, proxy ignore it, and use the one declared by JOB DECLARATOR");
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `SetNewPrevHash` message which is used (along with the SV2
    /// `NewExtendedMiningJob` message) to later create a SV1 `mining.notify` for the Downstream
    /// role.
    fn handle_set_new_prev_hash(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::SetNewPrevHashOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        warn!("SNPH received from upstream, proxy ignores it, and uses the one declared by JOB DECLARATOR");
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `SetCustomMiningJobSuccess` message.
    fn handle_set_custom_mining_job_success(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetCustomMiningJobSuccessOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        if let Some(request) = self.template_to_job_id.take_request(m.request_id) {
            self.template_to_job_id
                .register_job_id(request.downstream_job.local_job_id, m.job_id);
            info!(
                "Set custom mining job success {}, for template {}",
                m.job_id, request.template_id
            );
            crate::block_templates::declaration_accepted(request.template_id);
            IS_CUSTOM_JOB_SET.store(true, std::sync::atomic::Ordering::Release);
            Ok(SendTo::None(None))
        } else {
            warn!(
                request_id = m.request_id,
                "ignoring unknown or late custom-job success"
            );
            Ok(SendTo::None(None))
        }
    }

    /// Handles the SV2 `SetCustomMiningJobError` message.
    fn handle_set_custom_mining_job_error(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetCustomMiningJobErrorOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        if let Some(request) = self.template_to_job_id.take_request(m.request_id) {
            crate::block_templates::declaration_rejected(request.template_id);
            warn!(
                template_id = request.template_id,
                request_id = m.request_id,
                "pool rejected custom job"
            );
            IS_CUSTOM_JOB_SET.store(true, std::sync::atomic::Ordering::Release);
        } else {
            warn!(
                request_id = m.request_id,
                "ignoring unknown or late custom-job rejection"
            );
        }
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `SetTarget` message which updates the Downstream role(s) target
    /// difficulty via the SV1 `mining.set_difficulty` message.
    fn handle_set_target(
        &mut self,
        m: &roles_logic_sv2::mining_sv2::SetTargetOwned,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        self.target = Some(m.maximum_target.to_array());
        if let Some(factory) = self.channel_factory.as_mut() {
            factory.update_target_for_channel(m.channel_id, m.maximum_target.to_array().into());
            factory.set_target(&mut m.maximum_target.to_array().into());
        }
        if let Some(downstream) = &self.downstream {
            downstream
                .safe_lock(|d| {
                    match d.status.get_channel() {
                        Ok(factory) => {
                            let mut target = m.maximum_target.to_array().into();
                            factory.set_target(&mut target);
                            factory.update_target_for_channel(m.channel_id, target);
                        }
                        Err(_) => return Err(RolesLogicError::NotFoundChannelId),
                    }
                    Ok(())
                })
                .map_err(|_| RolesLogicError::DownstreamDown)??;
        }

        if let Some(downstream) = &self.downstream {
            Ok(SendTo::RelaySameMessageToRemote(downstream.clone()))
        } else {
            Err(RolesLogicError::DownstreamDown)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn group_updates_reach_the_translator() {
        let (up_sender, _up_receiver) = tokio::sync::mpsc::channel(1);
        let upstream = Upstream::new(8, up_sender).await.unwrap();
        let (down_sender, mut down_receiver) = tokio::sync::mpsc::channel(1);
        let (solution_sender, _solutions) = tokio::sync::mpsc::channel(1);
        let downstream = Arc::new(Mutex::new(Downstream::new(
            down_sender,
            Some(upstream.clone()),
            solution_sender,
            false,
            Vec::new(),
            None,
        )));
        upstream
            .safe_lock(|state| state.downstream = Some(downstream))
            .unwrap();
        let (incoming_sender, incoming_receiver) = tokio::sync::mpsc::channel(1);
        let _task = Upstream::parse_incoming(upstream, incoming_receiver)
            .await
            .unwrap();
        incoming_sender
            .send(MiningOwned::SetGroupChannel(
                roles_logic_sv2::mining_sv2::SetGroupChannelOwned {
                    group_channel_id: 42,
                    channel_ids: vec![7, 9].try_into().unwrap(),
                },
            ))
            .await
            .unwrap();
        let message = tokio::time::timeout(std::time::Duration::from_secs(1), down_receiver.recv())
            .await
            .expect("group update was not forwarded")
            .unwrap();
        let MiningOwned::SetGroupChannel(update) = message else {
            panic!("expected group update")
        };
        assert_eq!(update.group_channel_id, 42);
        assert_eq!(update.channel_ids.as_slice(), &[7, 9]);
    }

    fn template_with_outputs(outputs: &[TxOut]) -> NewTemplateOwned {
        let encoded_outputs = outputs
            .iter()
            .flat_map(bitcoin::consensus::serialize)
            .collect::<Vec<_>>();
        NewTemplateOwned {
            template_id: 1,
            future_template: false,
            version: 0,
            coinbase_tx_version: 2,
            coinbase_prefix: Vec::new().try_into().expect("empty prefix"),
            coinbase_tx_input_sequence: 0,
            coinbase_tx_value_remaining: 0,
            coinbase_tx_outputs_count: outputs.len() as u32,
            coinbase_tx_outputs: encoded_outputs.try_into().expect("serialized outputs"),
            coinbase_tx_locktime: 0,
            merkle_path: Vec::new().try_into().expect("empty merkle path"),
        }
    }

    fn decode_template_outputs(template: &NewTemplateOwned) -> Vec<TxOut> {
        let mut encoded_outputs = template.coinbase_tx_outputs.as_ref();
        let mut outputs = Vec::new();
        while !encoded_outputs.is_empty() {
            outputs.push(
                TxOut::consensus_decode(&mut encoded_outputs).expect("valid serialized output"),
            );
        }
        outputs
    }

    fn difficulty_commitment(difficulty: f32) -> TxOut {
        let mut script = b"\x6a\x0aDIFF32".to_vec();
        script.extend_from_slice(&difficulty.to_le_bytes());
        TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(script),
        }
    }

    fn downstream_job(local_job_id: u32) -> DownstreamJob {
        DownstreamJob {
            local_job_id,
            coinbase_tx_prefix: Vec::new().try_into().expect("empty prefix"),
            coinbase_tx_suffix: Vec::new().try_into().expect("empty suffix"),
        }
    }

    #[test]
    fn pool_job_mappings_are_scoped_to_the_exact_miner_job() {
        let mut mappings = TemplateToJobId::new();
        mappings.register_job_id(11, 101);
        mappings.register_job_id(12, 102);

        assert_eq!(mappings.get_job_id(11), Some(101));
        assert_eq!(mappings.get_job_id(12), Some(102));
    }

    #[test]
    fn custom_job_responses_keep_request_correlation_when_reordered() {
        let mut mappings = TemplateToJobId::new();
        mappings.register_request(
            1,
            PendingCustomJob {
                template_id: 41,
                downstream_job: downstream_job(11),
            },
        );
        mappings.register_request(
            2,
            PendingCustomJob {
                template_id: 42,
                downstream_job: downstream_job(12),
            },
        );

        assert_eq!(
            mappings
                .take_request(2)
                .expect("second request")
                .downstream_job
                .local_job_id,
            12
        );
        assert_eq!(
            mappings
                .take_request(1)
                .expect("first request")
                .downstream_job
                .local_job_id,
            11
        );
    }

    #[tokio::test]
    async fn difficulty_commitment_replaces_all_existing_tagged_outputs() {
        let unrelated_output = TxOut {
            value: Amount::from_sat(42),
            script_pubkey: ScriptBuf::from_bytes(
                [b"\x6a\x0bX".as_slice(), b"DIFF32-data"].concat(),
            ),
        };
        let malformed_commitment = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(
                [b"\x6a\x0a".as_slice(), DIFFICULTY_COMMITMENT_TAG].concat(),
            ),
        };
        let mut template = template_with_outputs(&[
            difficulty_commitment(1.0),
            unrelated_output.clone(),
            malformed_commitment,
            difficulty_commitment(2.0),
        ]);
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let upstream = Upstream::new(0, sender).await.expect("upstream");

        let first_target =
            bitcoin::Target::from_compact(bitcoin::CompactTarget::from_consensus(0x1d00ffff));
        upstream
            .safe_lock(|state| state.target = Some(first_target.to_le_bytes()))
            .expect("upstream lock");
        Upstream::apply_difficulty_commitment(&upstream, &mut template).expect("first commitment");

        assert_eq!(template.coinbase_tx_outputs_count, 2);
        assert_eq!(
            decode_template_outputs(&template),
            vec![
                unrelated_output.clone(),
                difficulty_commitment(first_target.difficulty_float() as f32),
            ]
        );

        let second_target =
            bitcoin::Target::from_compact(bitcoin::CompactTarget::from_consensus(0x1c00ffff));
        upstream
            .safe_lock(|state| state.target = Some(second_target.to_le_bytes()))
            .expect("upstream lock");
        Upstream::apply_difficulty_commitment(&upstream, &mut template)
            .expect("replacement commitment");

        assert_eq!(template.coinbase_tx_outputs_count, 2);
        assert_eq!(
            decode_template_outputs(&template),
            vec![
                unrelated_output,
                difficulty_commitment(second_target.difficulty_float() as f32),
            ]
        );
    }
}
