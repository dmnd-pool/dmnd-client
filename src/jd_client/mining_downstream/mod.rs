mod task_manager;
use crate::{
    proxy_state::{DownstreamType, JdState, ProxyState},
    shared::utils::AbortOnDrop,
};
use tokio::time::{timeout, Duration};

use super::{job_declarator::JobDeclarator, mining_upstream::Upstream as UpstreamMiningNode};
use crate::jd_client::error::Error as JdClientError;
use roles_logic_sv2::{
    channel_logic::channel_factory::{OnNewShare, PoolChannelFactory, Share},
    common_properties::{CommonDownstreamData, IsDownstream, IsMiningDownstream},
    errors::Error,
    handlers::mining::{ParseDownstreamMiningMessages, SendTo, SupportedChannelTypes},
    job_creator::JobsCreators,
    mining_sv2::*,
    parsers::{Mining, MiningDeviceMessages},
    template_distribution_sv2::{NewTemplate, SubmitSolution},
    utils::Mutex,
};
use task_manager::TaskManager;
use tokio::{
    sync::mpsc::{Receiver as TReceiver, Sender as TSender},
    task,
};
use tracing::{debug, error, warn};

use codec_sv2::{StandardEitherFrame, StandardSv2Frame};

use bitcoin::{consensus::Decodable, TxOut};

pub type Message = MiningDeviceMessages<'static>;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;

/// 1 to 1 connection with a downstream node that implement the mining (sub)protocol can be either
/// a mining device or a downstream proxy.
/// A downstream can only be linked with an upstream at a time. Support multi upstrems for
/// downstream do no make much sense.
#[derive(Debug)]
pub struct DownstreamMiningNode {
    sender: TSender<Mining<'static>>,
    pub status: DownstreamMiningNodeStatus,
    pub prev_job_id: Option<u32>,
    solution_sender: TSender<SubmitSolution<'static>>,
    withhold: bool,
    miner_coinbase_output: Vec<TxOut>,
    // used to retreive the job id of the share that we send upstream
    last_template_id: u64,
    jd: Option<Arc<Mutex<JobDeclarator>>>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum DownstreamMiningNodeStatus {
    Paired(Arc<Mutex<UpstreamMiningNode>>),
    ChannelOpened((PoolChannelFactory, Arc<Mutex<UpstreamMiningNode>>)),
    SoloMinerPaired(),
    SoloMinerChannelOpend(PoolChannelFactory),
}

impl DownstreamMiningNodeStatus {
    fn set_channel(&mut self, channel: PoolChannelFactory) -> bool {
        match self {
            DownstreamMiningNodeStatus::Paired(up) => {
                let self_ = Self::ChannelOpened((channel, up.clone()));
                let _ = std::mem::replace(self, self_);
                true
            }
            DownstreamMiningNodeStatus::ChannelOpened(_) => false,
            DownstreamMiningNodeStatus::SoloMinerPaired() => {
                let self_ = Self::SoloMinerChannelOpend(channel);
                let _ = std::mem::replace(self, self_);
                true
            }
            DownstreamMiningNodeStatus::SoloMinerChannelOpend(_) => false,
        }
    }

    pub fn get_channel(&mut self) -> Result<&mut PoolChannelFactory, Error> {
        match self {
            DownstreamMiningNodeStatus::Paired(_) => Err(Error::DownstreamDown),
            DownstreamMiningNodeStatus::ChannelOpened((channel, _)) => Ok(channel),
            DownstreamMiningNodeStatus::SoloMinerPaired() => Err(Error::DownstreamDown),
            DownstreamMiningNodeStatus::SoloMinerChannelOpend(channel) => Ok(channel),
        }
    }
    fn have_channel(&self) -> bool {
        match self {
            DownstreamMiningNodeStatus::Paired(_) => false,
            DownstreamMiningNodeStatus::ChannelOpened(_) => true,
            DownstreamMiningNodeStatus::SoloMinerPaired() => false,
            DownstreamMiningNodeStatus::SoloMinerChannelOpend(_) => true,
        }
    }
    fn get_upstream(&mut self) -> Option<Arc<Mutex<UpstreamMiningNode>>> {
        match self {
            DownstreamMiningNodeStatus::Paired(up) => Some(up.clone()),
            DownstreamMiningNodeStatus::ChannelOpened((_, up)) => Some(up.clone()),
            DownstreamMiningNodeStatus::SoloMinerPaired() => None,
            DownstreamMiningNodeStatus::SoloMinerChannelOpend(_) => None,
        }
    }
    fn is_solo_miner(&mut self) -> bool {
        matches!(
            self,
            DownstreamMiningNodeStatus::SoloMinerPaired()
                | DownstreamMiningNodeStatus::SoloMinerChannelOpend(_)
        )
    }
}

use core::convert::TryInto;
use std::sync::Arc;

impl DownstreamMiningNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sender: TSender<Mining<'static>>,
        upstream: Option<Arc<Mutex<UpstreamMiningNode>>>,
        solution_sender: TSender<SubmitSolution<'static>>,
        withhold: bool,
        miner_coinbase_output: Vec<TxOut>,
        jd: Option<Arc<Mutex<JobDeclarator>>>,
    ) -> Self {
        let status = match upstream {
            Some(up) => DownstreamMiningNodeStatus::Paired(up),
            None => DownstreamMiningNodeStatus::SoloMinerPaired(),
        };
        Self {
            sender,
            status,
            prev_job_id: None,
            solution_sender,
            withhold,
            miner_coinbase_output,
            // set it to an arbitrary value cause when we use it we always updated it.
            // Is used before sending the share to upstream in the main loop when we have a share.
            // Is upated in the message handler that si called earlier in the main loop.
            last_template_id: 0,
            jd,
        }
    }

    /// Strat listen for downstream mining node. Return as soon as one downstream connect.
    pub async fn start(
        self_mutex: Arc<Mutex<Self>>,
        mut receiver: TReceiver<Mining<'static>>,
    ) -> Result<AbortOnDrop, JdClientError> {
        let task_manager = TaskManager::initialize();
        let abortable = match task_manager
            .safe_lock(|t| t.get_aborter())
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?
        {
            Some(abortable) => abortable,
            // Aborter is None
            None => {
                error!("Failed to get Aborter: Not found.");
                return Err(JdClientError::JdClientDownstreamTaskManagerFailed);
            }
        };
        let factory_abortable = DownstreamMiningNode::set_channel_factory(self_mutex.clone());
        TaskManager::add_set_channel_factory(task_manager.clone(), factory_abortable?)
            .await
            .map_err(|_| JdClientError::JdClientDownstreamTaskManagerFailed)?;
        let main_task = task::spawn(async move {
            while let Some(message) = receiver.recv().await {
                if let Err(e) = DownstreamMiningNode::next(&self_mutex, message).await {
                    error!("Jd error can not receive message from downstream: {e:?}");
                    ProxyState::update_downstream_state(DownstreamType::JdClientMiningDownstream);
                };
            }
        });
        TaskManager::add_main_task(task_manager, main_task.into())
            .await
            .map_err(|_| JdClientError::JdClientDownstreamTaskManagerFailed)?;
        Ok(abortable)
    }

    // When we do pooled minig we create a channel factory when the pool send a open extended
    // mining channel success
    fn set_channel_factory(self_mutex: Arc<Mutex<Self>>) -> Result<AbortOnDrop, JdClientError> {
        let is_solo_miner = self_mutex
            .safe_lock(|s| s.status.is_solo_miner())
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?;
        let handle = tokio::task::spawn(async move {
            if !is_solo_miner {
                // Safe unwrap already checked if it contains an upstream withe `is_solo_miner`
                let upstream = match self_mutex.safe_lock(|s| s.status.get_upstream()) {
                    Ok(upstream) => match upstream {
                        Some(upstream) => upstream,
                        None => {
                            error!("Jd can not get upstream");
                            ProxyState::update_downstream_state(
                                DownstreamType::JdClientMiningDownstream,
                            );
                            return;
                        }
                    },
                    Err(e) => {
                        error!("Jd can not get upstream: {e}");
                        ProxyState::update_downstream_state(
                            DownstreamType::JdClientMiningDownstream,
                        );
                        return;
                    }
                };
                if let Ok(factory) = UpstreamMiningNode::take_channel_factory(upstream).await {
                    if self_mutex
                        .safe_lock(|s| {
                            s.status.set_channel(factory);
                        })
                        .is_err()
                    {
                        error!("Jd can not get channel factory");
                    }
                }
            }
        });
        Ok(handle.into())
    }

    /// Parse the received message and relay it to the right upstream
    pub async fn next(
        self_mutex: &Arc<Mutex<Self>>,
        incoming: Mining<'static>,
    ) -> Result<(), JdClientError> {
        let routing_logic = roles_logic_sv2::routing_logic::MiningRoutingLogic::None;

        let next_message_to_send =
            ParseDownstreamMiningMessages::handle_message_mining_deserialized(
                self_mutex.clone(),
                Ok(incoming.clone()),
                routing_logic,
            );
        Self::match_send_to(self_mutex.clone(), next_message_to_send, Some(incoming)).await
        //Propgate error, caller will restart proxy
    }

    #[async_recursion::async_recursion]
    async fn match_send_to(
        self_mutex: Arc<Mutex<Self>>,
        next_message_to_send: Result<SendTo<UpstreamMiningNode>, Error>,
        incoming: Option<Mining<'static>>,
    ) -> Result<(), JdClientError> {
        match next_message_to_send {
            Ok(SendTo::RelaySameMessageToRemote(upstream_mutex)) => {
                let incoming = match incoming {
                    Some(incoming) => incoming,
                    None => {
                        error!("JDC dowstream try to releay an inexistent message");
                        ProxyState::update_jd_state(JdState::Down);
                        return Err(JdClientError::Unrecoverable);
                    }
                };
                UpstreamMiningNode::send(&upstream_mutex, incoming).await?;
            }
            Ok(SendTo::RelayNewMessage(Mining::SubmitSharesExtended(mut share))) => {
                // If we have a realy new message it means that we are in a pooled mining mods.
                let upstream_mutex = self_mutex
                    .safe_lock(|s| s.status.get_upstream())
                    .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?
                    .ok_or_else(|| {
                        error!("Upstream is None Here");
                        JdClientError::Unrecoverable
                    })?;
                // When re receive SetupConnectionSuccess we link the last_template_id with the
                // pool's job_id. The below return as soon as we have a pairable job id for the
                // template_id associated with this share.
                let last_template_id = self_mutex
                    .safe_lock(|s| s.last_template_id)
                    .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?;
                let job_id_future =
                    UpstreamMiningNode::get_job_id(&upstream_mutex, last_template_id);
                //?check
                if let Ok(Ok(job_id)) = timeout(Duration::from_secs(20), job_id_future).await {
                    share.job_id = job_id;
                    debug!(
                        "Sending valid block solution upstream, with job_id {}",
                        job_id
                    );
                    let message = Mining::SubmitSharesExtended(share);
                    UpstreamMiningNode::send(&upstream_mutex, message).await?;
                } else {
                    error!("Timeout getting job_id for last_template_id: {last_template_id}, discard share");
                }
            }
            Ok(SendTo::RelayNewMessage(message)) => {
                let upstream_mutex = self_mutex
                    .safe_lock(|s| s.status.get_upstream())
                    .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?
                    .ok_or({
                        error!(
                        "We should return RelayNewMessage only if we are not in solo mining mode",
                    );
                        JdClientError::RolesSv2Logic(Error::NoUpstreamsConnected)
                        // Propagate error. Caller will restart proxy
                    })?;
                UpstreamMiningNode::send(&upstream_mutex, message).await?;
            }
            Ok(SendTo::Multiple(messages)) => {
                for message in messages {
                    if let Err(e) = Self::match_send_to(self_mutex.clone(), Ok(message), None).await
                    {
                        error!("Jd Unexpected message: {e:?}");
                        ProxyState::update_downstream_state(
                            DownstreamType::JdClientMiningDownstream,
                        );
                    }
                }
            }
            Ok(SendTo::Respond(message)) => Self::send(&self_mutex, message).await?,
            Ok(SendTo::None(None)) => (),
            Ok(m) => unreachable!("Unexpected message type: {:?}", m),
            Err(Error::ShareDoNotMatchAnyJob) => warn!("Error: ShareDoNotMatchAnyJob"),
            Err(e) => return Err(JdClientError::RolesSv2Logic(e)),
        }
        Ok(())
    }

    /// Send a message downstream
    pub async fn send(
        self_mutex: &Arc<Mutex<Self>>,
        message: Mining<'static>,
    ) -> Result<(), JdClientError> {
        let sender = self_mutex
            .safe_lock(|self_| self_.sender.clone())
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?;
        sender
            .send(message)
            .await
            .map_err(|_| JdClientError::Unrecoverable)
    }

    pub async fn on_new_template(
        self_mutex: &Arc<Mutex<Self>>,
        mut new_template: NewTemplate<'static>,
        pool_output: &[u8],
    ) -> Result<(), JdClientError> {
        if !self_mutex
            .safe_lock(|s| s.status.have_channel())
            .map_err(|e| Error::PoisonLock(e.to_string()))?
        {
            super::IS_NEW_TEMPLATE_HANDLED.store(true, std::sync::atomic::Ordering::Release);
            return Ok(());
        }
        let mut pool_out = &pool_output[0..];
        let pool_output =
            TxOut::consensus_decode(&mut pool_out).expect("Upstream sent an invalid coinbase");

        let to_send = {
            let pool_outputs = self_mutex
                .safe_lock(|s| {
                    let channel = s.status.get_channel().map_err(JdClientError::RolesSv2Logic);

                    match channel {
                        Ok(channel) => {
                            channel.update_pool_outputs(vec![pool_output]);
                            match channel.on_new_template(&mut new_template) {
                                Ok(pool_outputs) => Ok(pool_outputs),
                                Err(e) => Err(JdClientError::RolesSv2Logic(e)),
                            }
                        }
                        Err(e) => Err(e),
                    }
                })
                .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?;
            pool_outputs?
        };

        // to_send is HashMap<channel_id, messages_to_send> but here we have only one downstream so
        // only one channel opened downstream. That means that we can take all the messages in the
        // map and send them downstream.
        let to_send = to_send.into_values();
        for message in to_send {
            let message = if let Mining::NewExtendedMiningJob(job) = message {
                if let Some(jd) = self_mutex
                    .safe_lock(|s| s.jd.clone())
                    .map_err(|_| JdClientError::JobDeclaratorMutexCorrupted)?
                {
                    jd.safe_lock(|jd| jd.coinbase_tx_prefix = job.coinbase_tx_prefix.clone())
                        .map_err(|_| JdClientError::JobDeclaratorMutexCorrupted)?;
                    jd.safe_lock(|jd| jd.coinbase_tx_suffix = job.coinbase_tx_suffix.clone())
                        .map_err(|_| JdClientError::JobDeclaratorMutexCorrupted)?;
                }

                Mining::NewExtendedMiningJob(job)
            } else {
                message
            };
            Self::send(self_mutex, message)
                .await
                .map_err(|_| Error::DownstreamDown)?; // Caller will restart proxy
        }
        // See coment on the definition of the global for memory
        // ordering
        super::IS_NEW_TEMPLATE_HANDLED.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub async fn on_set_new_prev_hash(
        self_mutex: &Arc<Mutex<Self>>,
        new_prev_hash: roles_logic_sv2::template_distribution_sv2::SetNewPrevHash<'static>,
    ) -> Result<(), JdClientError> {
        if !self_mutex
            .safe_lock(|s| s.status.have_channel())
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?
        {
            return Ok(());
        }
        let job_id = self_mutex
            .safe_lock(|s| {
                let channel = s.status.get_channel()?;
                channel.on_new_prev_hash_from_tp(&new_prev_hash)
            })
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)??;

        let channel_ids = self_mutex
            .safe_lock(|s| {
                s.status
                    .get_channel()
                    .map_err(|_| Error::NotFoundChannelId)
                    .map(|channel| channel.get_extended_channels_ids())
            })
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?
            .map_err(|_| Error::NotFoundChannelId)?;

        let channel_id = match channel_ids.len() {
            1 => channel_ids[0],
            _ => unreachable!(),
        };
        let to_send = SetNewPrevHash {
            channel_id,
            job_id,
            prev_hash: new_prev_hash.prev_hash,
            min_ntime: new_prev_hash.header_timestamp,
            nbits: new_prev_hash.n_bits,
        };
        let message = Mining::SetNewPrevHash(to_send);
        Self::send(self_mutex, message)
            .await
            .map_err(|_| Error::DownstreamDown)?; // Caller will restart proxy
        Ok(())
    }
}

use roles_logic_sv2::selectors::NullDownstreamMiningSelector;
impl IsDownstream for DownstreamMiningNode {
    fn get_downstream_mining_data(&self) -> CommonDownstreamData {
        CommonDownstreamData {
            header_only: false,
            work_selection: true,
            version_rolling: true,
        }
    }
}
/// It impl UpstreamMining cause the proxy act as an upstream node for the DownstreamMiningNode
impl
    ParseDownstreamMiningMessages<
        UpstreamMiningNode,
        NullDownstreamMiningSelector,
        roles_logic_sv2::routing_logic::NoRouting,
    > for DownstreamMiningNode
{
    fn get_channel_type(&self) -> SupportedChannelTypes {
        SupportedChannelTypes::Extended
    }

    fn is_work_selection_enabled(&self) -> bool {
        false
    }

    fn handle_open_standard_mining_channel(
        &mut self,
        _: OpenStandardMiningChannel,
        _: Option<Arc<Mutex<UpstreamMiningNode>>>,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        warn!("Ignoring OpenStandardMiningChannel");
        Ok(SendTo::None(None))
    }

    fn handle_open_extended_mining_channel(
        &mut self,
        m: OpenExtendedMiningChannel,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        if !self.status.is_solo_miner() {
            // Safe unwrap alreay checked if it cointains upstream with is_solo_miner
            Ok(SendTo::RelaySameMessageToRemote(
                match self.status.get_upstream() {
                    Some(upstream) => upstream,
                    None => return Err(Error::NoUpstreamsConnected),
                },
            ))
        } else {
            // The channel factory is created here so that we are sure that if we have a channel
            // open we have a factory and if we have a factory we have a channel open. This allowto
            // not change the semantic of Status beween solo and pooled modes
            let extranonce_len = 28; // 28 + 4 (additional_coinbase_script_data) = 32 (max allowed)
            let range_0 = std::ops::Range { start: 0, end: 0 };
            let range_1 = std::ops::Range { start: 0, end: 16 };
            let range_2 = std::ops::Range {
                start: 16,
                end: extranonce_len,
            };
            let ids = Arc::new(Mutex::new(roles_logic_sv2::utils::GroupId::new()));
            let coinbase_outputs = self.miner_coinbase_output.clone();
            let extranonces = ExtendedExtranonce::new(range_0, range_1, range_2);
            let creator = JobsCreators::new(extranonce_len as u8);
            let share_per_min = 1.0;
            let kind = roles_logic_sv2::channel_logic::channel_factory::ExtendedChannelKind::Pool;
            let channel_factory = PoolChannelFactory::new(
                ids,
                extranonces,
                creator,
                share_per_min,
                kind,
                coinbase_outputs,
                "SOLO".as_bytes().to_vec(),
            )
            .inspect_err(|_| {
                error!("Signature + extranonce lens exceed 32 bytes");
            })?;

            self.status.set_channel(channel_factory);

            let request_id = m.request_id;
            let hash_rate = m.nominal_hash_rate;
            let min_extranonce_size = m.min_extranonce_size;
            let messages_res = self
                .status
                .get_channel()
                .map_err(|_| Error::NotFoundChannelId)?
                .new_extended_channel(request_id, hash_rate, min_extranonce_size);
            match messages_res {
                Ok(messages) => {
                    let messages = messages.into_iter().map(SendTo::Respond).collect();
                    Ok(SendTo::Multiple(messages))
                }
                Err(_) => Err(roles_logic_sv2::Error::ChannelIsNeitherExtendedNeitherInAPool),
            }
        }
    }

    fn handle_update_channel(
        &mut self,
        _: UpdateChannel,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        if !self.status.is_solo_miner() {
            // Safe unwrap alreay checked if it cointains upstream with is_solo_miner
            Ok(SendTo::RelaySameMessageToRemote(
                self.status
                    .get_upstream()
                    .ok_or(Error::NoUpstreamsConnected)?,
            ))
        } else {
            Ok(SendTo::None(None))
        }
    }

    fn handle_submit_shares_standard(
        &mut self,
        _: SubmitSharesStandard,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        panic!("Impossible message from downstream");
    }

    fn handle_submit_shares_extended(
        &mut self,
        m: SubmitSharesExtended,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        match self
            .status
            .get_channel()
            .map_err(|_| Error::NotFoundChannelId)?
            .on_submit_shares_extended(m.clone())?
        {
            OnNewShare::SendErrorDownstream(s) => {
                error!("Share do not meet downstream target");
                Ok(SendTo::Respond(Mining::SubmitSharesError(s)))
            }
            OnNewShare::SendSubmitShareUpstream((m, Some(template_id))) => {
                if !self.status.is_solo_miner() {
                    match m {
                        Share::Extended(share) => {
                            let for_upstream = Mining::SubmitSharesExtended(share);
                            self.last_template_id = template_id;
                            Ok(SendTo::RelayNewMessage(for_upstream))
                        }
                        // We are in an extended channel shares are extended
                        Share::Standard(_) => unreachable!(),
                    }
                } else {
                    Ok(SendTo::None(None))
                }
            }
            OnNewShare::RelaySubmitShareUpstream => unreachable!(),
            OnNewShare::ShareMeetBitcoinTarget((
                share,
                Some(template_id),
                coinbase,
                extranonce,
            )) => {
                match share {
                    Share::Extended(share) => {
                        let solution_sender = self.solution_sender.clone();
                        let solution = SubmitSolution {
                            template_id,
                            version: share.version,
                            header_timestamp: share.ntime,
                            header_nonce: share.nonce,
                            coinbase_tx: coinbase.try_into()?,
                        };
                        tokio::spawn(async move {
                            if solution_sender.send(solution).await.is_err() {
                                error!("Downstream channel closed, couldn't send solution");
                            }
                        });
                        if !self.status.is_solo_miner() {
                            {
                                let jd = self.jd.clone();
                                let mut share = share.clone();
                                share.extranonce = extranonce.try_into()?;
                                // This do not need to be put in a task manager it always return
                                // fastly
                                tokio::task::spawn(async move {
                                    if let Some(jd) = jd {
                                        if let Err(e) = JobDeclarator::on_solution(&jd, share).await
                                        {
                                            error!("Jd Error on solution: {e:?}");
                                            // Set the proxy state to internal inconsistency
                                            ProxyState::update_inconsistency(Some(1));
                                        }
                                    }
                                });
                            }
                        }

                        // Safe unwrap alreay checked if it cointains upstream with is_solo_miner
                        if !self.withhold && !self.status.is_solo_miner() {
                            self.last_template_id = template_id;
                            let for_upstream = Mining::SubmitSharesExtended(share);
                            Ok(SendTo::RelayNewMessage(for_upstream))
                        } else {
                            Ok(SendTo::None(None))
                        }
                    }
                    // We are in an extended channel shares are extended
                    Share::Standard(_) => unreachable!(),
                }
            }
            // When we have a ShareMeetBitcoinTarget it means that the proxy know the bitcoin
            // target that means that the proxy must have JD capabilities that means that the
            // second tuple elements can not be None but must be Some(template_id)
            OnNewShare::ShareMeetBitcoinTarget(_) => unreachable!(),
            OnNewShare::SendSubmitShareUpstream(_) => unreachable!(),
            OnNewShare::ShareMeetDownstreamTarget => Ok(SendTo::None(None)),
        }
    }

    fn handle_set_custom_mining_job(
        &mut self,
        _: SetCustomMiningJob,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        warn!("Ignoring SetCustomMiningJob");
        Ok(SendTo::None(None))
    }
}
impl IsMiningDownstream for DownstreamMiningNode {}
