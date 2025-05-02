use crate::jd_client::IS_CUSTOM_JOB_SET;
use crate::proxy_state::{DownstreamType, ProxyState, TpState, UpstreamType};
use crate::{jd_client::error::Error, jd_client::error::ProxyResult, shared::utils::AbortOnDrop};

use crate::jd_client::mining_downstream::DownstreamMiningNode as Downstream;

use binary_sv2::{Seq0255, U256};
use roles_logic_sv2::{
    channel_logic::channel_factory::PoolChannelFactory,
    common_messages_sv2::Protocol,
    common_properties::{IsMiningUpstream, IsUpstream},
    handlers::mining::{ParseUpstreamMiningMessages, SendTo},
    job_declaration_sv2::DeclareMiningJob,
    mining_sv2::{ExtendedExtranonce, Extranonce, SetCustomMiningJob},
    parsers::Mining,
    routing_logic::{MiningRoutingLogic, NoRouting},
    selectors::NullDownstreamMiningSelector,
    utils::{Id, Mutex},
    Error as RolesLogicError,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{Receiver as TReceiver, Sender as TSender};
use tokio::task;
use tracing::{error, info, warn};

use std::collections::VecDeque;

use super::task_manager::TaskManager;

#[derive(Debug)]
struct CircularBuffer {
    buffer: VecDeque<(u64, u32)>,
    capacity: usize,
}

impl CircularBuffer {
    fn new(capacity: usize) -> Self {
        CircularBuffer {
            buffer: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn insert(&mut self, key: u64, value: u32) {
        if self.buffer.len() == self.capacity {
            self.buffer.pop_front();
        }
        self.buffer.push_back((key, value));
    }

    fn get(&self, id: u64) -> Option<u32> {
        self.buffer
            .iter()
            .find_map(|&(key, value)| if key == id { Some(value) } else { None })
    }
}

impl std::default::Default for CircularBuffer {
    fn default() -> Self {
        Self::new(10)
    }
}

#[derive(Debug, Default)]
struct TemplateToJobId {
    template_id_to_job_id: CircularBuffer,
    request_id_to_template_id: HashMap<u32, u64>,
}

impl TemplateToJobId {
    fn register_template_id(&mut self, template_id: u64, request_id: u32) {
        self.request_id_to_template_id
            .insert(request_id, template_id);
    }

    fn register_job_id(&mut self, template_id: u64, job_id: u32) {
        self.template_id_to_job_id.insert(template_id, job_id);
    }

    fn get_job_id(&mut self, template_id: u64) -> Option<u32> {
        self.template_id_to_job_id.get(template_id)
    }

    fn take_template_id(&mut self, request_id: u32) -> Option<u64> {
        self.request_id_to_template_id.remove(&request_id)
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
    /// This allows the upstream threads to be able to communicate back to the main thread its
    /// current status.
    /// Minimum `extranonce2` size. Initially requested in the `jdc-config.toml`, and ultimately
    /// set by the SV2 Upstream via the SV2 `OpenExtendedMiningChannelSuccess` message.
    pub min_extranonce_size: u16,
    pub upstream_extranonce1_size: usize,
    /// Receives messages from the SV2 Upstream role
    //pub receiver: TReceiver<EitherFrame>,
    /// Sends messages to the SV2 Upstream role
    pub sender: TSender<Mining<'static>>,
    pub downstream: Option<Arc<Mutex<Downstream>>>,
    channel_factory: Option<PoolChannelFactory>,
    template_to_job_id: TemplateToJobId,
    req_ids: Id,
}

impl Upstream {
    pub async fn send(self_: &Arc<Mutex<Self>>, message: Mining<'static>) -> ProxyResult<()> {
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
        sender: TSender<Mining<'static>>,
    ) -> ProxyResult<Arc<Mutex<Self>>> {
        Ok(Arc::new(Mutex::new(Self {
            channel_id: None,
            min_extranonce_size,
            upstream_extranonce1_size: 16, // 16 is the default since that is the only value the pool supports currently
            sender,
            downstream: None,
            channel_factory: None,
            template_to_job_id: TemplateToJobId::new(),
            req_ids: Id::new(),
        })))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn set_custom_jobs(
        self_: &Arc<Mutex<Self>>,
        declare_mining_job: DeclareMiningJob<'static>,
        set_new_prev_hash: roles_logic_sv2::template_distribution_sv2::SetNewPrevHash<'static>,
        merkle_path: Seq0255<'static, U256<'static>>,
        signed_token: binary_sv2::B0255<'static>,
        coinbase_tx_version: u32,
        coinbase_prefix: binary_sv2::B0255<'static>,
        coinbase_tx_input_n_sequence: u32,
        coinbase_tx_value_remaining: u64,
        coinbase_tx_outs: Vec<u8>,
        coinbase_tx_locktime: u32,
        template_id: u64,
    ) -> ProxyResult<()> {
        info!("Sending set custom mining job");
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

        let updated_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Internal error: this operation can not fail because SystemTime should always be later than UNIX_EPOCH")
            .as_secs() as u32;

        let to_send = SetCustomMiningJob {
            channel_id,
            request_id,
            token: signed_token,
            version: declare_mining_job.version,
            prev_hash: set_new_prev_hash.prev_hash,
            min_ntime: updated_timestamp,
            nbits: set_new_prev_hash.n_bits,
            coinbase_tx_version,
            coinbase_prefix,
            coinbase_tx_input_n_sequence,
            coinbase_tx_value_remaining,
            coinbase_tx_outputs: coinbase_tx_outs.try_into().expect("Internal error: this operation can not fail because Vec<U8> can always be converted into Inner"),
            coinbase_tx_locktime,
            merkle_path,
            extranonce_size: 0,
        };
        let message = Mining::SetCustomMiningJob(to_send);
        self_
            .safe_lock(|s| {
                s.template_to_job_id
                    .register_template_id(template_id, request_id)
            })
            .map_err(|_| Error::JdClientUpstreamMutexCorrupted)?;
        Self::send(self_, message).await
    }

    /// Parses the incoming SV2 message from the Upstream role and routes the message to the
    /// appropriate handler.
    pub async fn parse_incoming(
        self_: Arc<Mutex<Self>>,
        mut recv: TReceiver<Mining<'static>>,
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

                    // Since this is not communicating with an SV2 proxy, but instead a custom SV1
                    // proxy where the routing logic is handled via the `Upstream`'s communication
                    // channels, we do not use the mining routing logic in the SV2 library and specify
                    // no mining routing logic here
                    let routing_logic = MiningRoutingLogic::None;

                    let next_message_to_send = Upstream::handle_message_mining_deserialized(
                        self_.clone(),
                        Ok(incoming.clone()),
                        routing_logic,
                    );

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

    pub async fn get_job_id(self_: &Arc<Mutex<Self>>, template_id: u64) -> Result<u32, Error> {
        loop {
            if let Some(id) = self_
                .safe_lock(|s| s.template_to_job_id.get_job_id(template_id))
                .map_err(|_| Error::JdClientDownstreamMutexCorrupted)?
            {
                return Ok(id);
            }
            tokio::task::yield_now().await;
        }
    }
}

impl IsUpstream<Downstream, NullDownstreamMiningSelector> for Upstream {
    fn get_version(&self) -> u16 {
        todo!()
    }

    fn get_flags(&self) -> u32 {
        todo!()
    }

    fn get_supported_protocols(&self) -> Vec<Protocol> {
        todo!()
    }

    fn get_id(&self) -> u32 {
        todo!()
    }

    fn get_mapper(&mut self) -> Option<&mut roles_logic_sv2::common_properties::RequestIdMapper> {
        todo!()
    }

    fn get_remote_selector(&mut self) -> &mut NullDownstreamMiningSelector {
        todo!()
    }
}

impl IsMiningUpstream<Downstream, NullDownstreamMiningSelector> for Upstream {
    fn total_hash_rate(&self) -> u64 {
        todo!()
    }

    fn add_hash_rate(&mut self, _to_add: u64) {
        todo!()
    }

    fn get_opened_channels(
        &mut self,
    ) -> &mut Vec<roles_logic_sv2::common_properties::UpstreamChannel> {
        todo!()
    }

    fn update_channels(&mut self, _c: roles_logic_sv2::common_properties::UpstreamChannel) {
        todo!()
    }
}

/// Connection-wide SV2 Upstream role messages parser implemented by a downstream ("downstream"
/// here is relative to the SV2 Upstream role and is represented by this `Upstream` struct).
impl ParseUpstreamMiningMessages<Downstream, NullDownstreamMiningSelector, NoRouting> for Upstream {
    /// Returns the channel type between the SV2 Upstream role and the `Upstream`, which will
    /// always be `Extended` for a SV1/SV2 Translator Proxy.
    fn get_channel_type(&self) -> roles_logic_sv2::handlers::mining::SupportedChannelTypes {
        roles_logic_sv2::handlers::mining::SupportedChannelTypes::Extended
    }

    /// Work selection is disabled for SV1/SV2 Translator Proxy and all work selection is performed
    /// by the SV2 Upstream role.
    fn is_work_selection_enabled(&self) -> bool {
        true
    }

    /// The SV2 `OpenStandardMiningChannelSuccess` message is NOT handled because it is NOT used
    /// for the Translator Proxy as only `Extended` channels are used between the SV1/SV2 Translator
    /// Proxy and the SV2 Upstream role.
    fn handle_open_standard_mining_channel_success(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::OpenStandardMiningChannelSuccess,
        _remote: Option<Arc<Mutex<Downstream>>>,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        panic!("Standard Mining Channels are not used in Translator Proxy")
    }

    /// This is a transparent proxy so OpenExtendedMiningChannel is sent as it is downstream.
    /// This message is used also to create a PoolChannelFactory that mock the upstream pool.
    /// this PoolChannelFactory is used by the template provider client in order to check shares
    /// received by downstream using the right extranonce and seeing the same hash that the downstream
    /// saw. PoolChannelFactory coinbase pre and suf are setted by the JD client.
    fn handle_open_extended_mining_channel_success(
        &mut self,
        m: roles_logic_sv2::mining_sv2::OpenExtendedMiningChannelSuccess,
    ) -> Result<SendTo<Downstream>, RolesLogicError> {
        info!("Receive open extended mining channel success");
        let ids = Arc::new(Mutex::new(roles_logic_sv2::utils::GroupId::new()));
        let prefix_len = m.extranonce_prefix.to_vec().len();
        let self_len = 0;
        let total_len = prefix_len + m.extranonce_size as usize;
        let range_0 = 0..prefix_len;
        let range_1 = prefix_len..prefix_len + self_len;
        let range_2 = prefix_len + self_len..total_len;

        let extranonces = ExtendedExtranonce::new(range_0, range_1, range_2);
        let creator = roles_logic_sv2::job_creator::JobsCreators::new(total_len as u8);
        let share_per_min = 1.0;
        let channel_kind =
            roles_logic_sv2::channel_logic::channel_factory::ExtendedChannelKind::ProxyJd {
                upstream_target: m.target.clone().into(),
            };
        let mut channel_factory = PoolChannelFactory::new(
            ids,
            extranonces,
            creator,
            share_per_min,
            channel_kind,
            vec![],
            vec![],
        )
        .inspect_err(|_| {
            error!("Signature + extranonce lens exceed 32 bytes");
        })?;
        let extranonce: Extranonce = m
            .extranonce_prefix
            .into_static()
            .to_vec()
            .try_into()
            .expect("Internal error: this operation can not fail because extranonce_pref can always be converted to Vec<u8>");
        self.channel_id = Some(m.channel_id);
        channel_factory
            .replicate_upstream_extended_channel_only_jd(
                m.target.into_static(),
                extranonce,
                m.channel_id,
                m.extranonce_size,
            )
            .expect("Impossible to open downstream channel");
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
        _: roles_logic_sv2::mining_sv2::OpenMiningChannelError,
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
        _m: roles_logic_sv2::mining_sv2::UpdateChannelError,
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
        _m: roles_logic_sv2::mining_sv2::CloseChannel,
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
        _: roles_logic_sv2::mining_sv2::SetExtranoncePrefix,
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
        _m: roles_logic_sv2::mining_sv2::SubmitSharesSuccess,
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
        _m: roles_logic_sv2::mining_sv2::SubmitSharesError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        // TODO remove the comments when share too low err get fixed
        //self.pool_chaneger_trigger
        //    .safe_lock(|t| t.start(self.tx_status.clone()))
        //    .unwrap();
        Ok(SendTo::None(None))
    }

    /// The SV2 `NewMiningJob` message is NOT handled because it is NOT used for the Translator
    /// Proxy as only `Extended` channels are used between the SV1/SV2 Translator Proxy and the SV2
    /// Upstream role.
    fn handle_new_mining_job(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::NewMiningJob,
    ) -> Result<SendTo<Downstream>, RolesLogicError> {
        panic!("Standard Mining Channels are not used in Translator Proxy")
    }

    /// Handles the SV2 `NewExtendedMiningJob` message which is used (along with the SV2
    /// `SetNewPrevHash` message) to later create a SV1 `mining.notify` for the Downstream
    /// role.
    fn handle_new_extended_mining_job(
        &mut self,
        _: roles_logic_sv2::mining_sv2::NewExtendedMiningJob,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        warn!("Extended job received from upstream, proxy ignore it, and use the one declared by JOB DECLARATOR");
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `SetNewPrevHash` message which is used (along with the SV2
    /// `NewExtendedMiningJob` message) to later create a SV1 `mining.notify` for the Downstream
    /// role.
    fn handle_set_new_prev_hash(
        &mut self,
        _: roles_logic_sv2::mining_sv2::SetNewPrevHash,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        warn!("SNPH received from upstream, proxy ignore it, and use the one declared by JOB DECLARATOR");
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `SetCustomMiningJobSuccess` message (TODO).
    fn handle_set_custom_mining_job_success(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetCustomMiningJobSuccess,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        // TODO
        info!("Set custom mining job success {}", m.job_id);
        if let Some(template_id) = self.template_to_job_id.take_template_id(m.request_id) {
            self.template_to_job_id
                .register_job_id(template_id, m.job_id);
            IS_CUSTOM_JOB_SET.store(true, std::sync::atomic::Ordering::Release);
            Ok(SendTo::None(None))
        } else {
            error!("Attention received a SetupConnectionSuccess with unknown request_id");
            Ok(SendTo::None(None))
        }
    }

    /// Handles the SV2 `SetCustomMiningJobError` message (TODO).
    fn handle_set_custom_mining_job_error(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::SetCustomMiningJobError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        todo!()
    }

    /// Handles the SV2 `SetTarget` message which updates the Downstream role(s) target
    /// difficulty via the SV1 `mining.set_difficulty` message.
    fn handle_set_target(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetTarget,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        if let Some(factory) = self.channel_factory.as_mut() {
            factory.update_target_for_channel(m.channel_id, m.maximum_target.clone().into());
            factory.set_target(&mut m.maximum_target.clone().into());
        }
        if let Some(downstream) = &self.downstream {
            downstream
                .safe_lock(|d| {
                    match d.status.get_channel() {
                        Ok(factory) => {
                            let mut target = m.maximum_target.clone().into();
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

    /// Handles the SV2 `Reconnect` message (TODO).
    fn handle_reconnect(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::Reconnect,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        if let Some(downstream) = &self.downstream {
            Ok(SendTo::RelaySameMessageToRemote(downstream.clone()))
        } else {
            Err(RolesLogicError::DownstreamDown)
        }
    }
}
