mod task_manager;
use crate::{
    proxy_state::{DownstreamType, JdState, ProxyState, UpstreamType},
    shared::utils::AbortOnDrop,
};
use tokio::time::{timeout, Duration};

use super::{job_declarator::JobDeclarator, mining_upstream::Upstream as UpstreamMiningNode};
use crate::jd_client::error::Error as JdClientError;
use binary_sv2::B064KOwned;
use roles_logic_sv2::{
    channel_logic::channel_factory::{OnNewShare, PoolChannelFactory, Share},
    common_properties::{CommonDownstreamData, IsDownstream, IsMiningDownstream},
    errors::Error,
    handlers::mining::SendTo,
    job_creator::{decode_outputs, JobsCreators},
    mining::ExtendedExtranonce,
    mining_sv2::*,
    parsers::{MiningDeviceMessagesOwned, MiningOwned},
    template_distribution_sv2::{
        NewTemplateOwned, SetNewPrevHashOwned as TemplateSetNewPrevHashOwned, SubmitSolutionOwned,
    },
    utils::Mutex,
};
use task_manager::TaskManager;
use tokio::{
    sync::mpsc::{Receiver as TReceiver, Sender as TSender},
    task,
};
use tracing::{debug, error, warn};

use demand_sv2_connection::{StandardEitherFrame, StandardSv2Frame};

use bitcoin::{consensus::Decodable, TxOut};

pub type Message = MiningDeviceMessagesOwned;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;

const MAX_SUPPORTED_COINBASE_OUTPUTS: usize = 252;

#[derive(Clone, Debug)]
pub(crate) struct DownstreamJob {
    pub(crate) local_job_id: u32,
    pub(crate) coinbase_tx_prefix: B064KOwned,
    pub(crate) coinbase_tx_suffix: B064KOwned,
}

/// 1 to 1 connection with a downstream node that implement the mining (sub)protocol can be either
/// a mining device or a downstream proxy.
/// A downstream can only be linked with an upstream at a time. Support multi upstrems for
/// downstream do no make much sense.
#[derive(Debug)]
pub struct DownstreamMiningNode {
    sender: TSender<MiningOwned>,
    pub status: DownstreamMiningNodeStatus,
    pub prev_job_id: Option<u32>,
    solution_sender: TSender<SubmitSolutionOwned>,
    withhold: bool,
    miner_coinbase_output: Vec<TxOut>,
    pub jd: Option<Arc<Mutex<JobDeclarator>>>,
    pending_tasks: tokio::task::JoinSet<()>,
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
    pub(crate) fn decode_pool_coinbase_outputs(
        encoded_outputs: &[u8],
    ) -> Result<Vec<TxOut>, JdClientError> {
        decode_outputs(encoded_outputs).map_err(JdClientError::RolesSv2Logic)
    }

    pub(crate) fn preview_template_job(
        self_mutex: &Arc<Mutex<Self>>,
        template: NewTemplateOwned,
        pool_output: &[u8],
    ) -> Result<(B064KOwned, B064KOwned), JdClientError> {
        let extranonce_len = self_mutex
            .safe_lock(|state| {
                state
                    .status
                    .get_channel()
                    .map(|channel| channel.get_extranonce_len())
            })
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?
            .map_err(JdClientError::RolesSv2Logic)?;
        let pool_outputs = Self::decode_pool_coinbase_outputs(pool_output)?;
        Self::preview_template_job_with_extranonce(template, pool_outputs, extranonce_len)
    }

    fn preview_template_job_with_extranonce(
        mut template: NewTemplateOwned,
        pool_outputs: Vec<TxOut>,
        extranonce_len: usize,
    ) -> Result<(B064KOwned, B064KOwned), JdClientError> {
        if pool_outputs.is_empty() {
            return Err(JdClientError::Unrecoverable);
        }
        let extranonce_len =
            u8::try_from(extranonce_len).map_err(|_| JdClientError::Unrecoverable)?;
        let mut creator = JobsCreators::new(extranonce_len);
        // The throwaway creator only computes the exact coinbase fields. Its pinned
        // implementation increments template_id without checking overflow, so never pass it a
        // peer-controlled ID.
        template.template_id = 0;
        let job = creator
            .on_new_template(&mut template, true, pool_outputs, 0)
            .map_err(JdClientError::RolesSv2Logic)?;
        Ok((job.coinbase_tx_prefix, job.coinbase_tx_suffix))
    }

    fn decode_raw_template_outputs(mut bytes: &[u8]) -> Result<Vec<TxOut>, JdClientError> {
        let mut outputs = Vec::new();
        while !bytes.is_empty() {
            outputs.push(
                TxOut::consensus_decode(&mut bytes).map_err(|_| JdClientError::Unrecoverable)?,
            );
        }
        Ok(outputs)
    }

    fn validate_final_coinbase_output_count(
        template: &NewTemplateOwned,
        additional_output_count: usize,
    ) -> Result<(), JdClientError> {
        // JobsCreators uses the serialized outputs, rather than trusting the declared count.
        let template_output_count =
            Self::decode_raw_template_outputs(template.coinbase_tx_outputs.as_bytes())?.len();
        let final_output_count = template_output_count
            .checked_add(additional_output_count)
            .ok_or(JdClientError::UnsupportedCoinbaseOutputCount {
                count: usize::MAX,
                max: MAX_SUPPORTED_COINBASE_OUTPUTS,
            })?;
        if final_output_count > MAX_SUPPORTED_COINBASE_OUTPUTS {
            return Err(JdClientError::UnsupportedCoinbaseOutputCount {
                count: final_output_count,
                max: MAX_SUPPORTED_COINBASE_OUTPUTS,
            });
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sender: TSender<MiningOwned>,
        upstream: Option<Arc<Mutex<UpstreamMiningNode>>>,
        solution_sender: TSender<SubmitSolutionOwned>,
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
            jd,
            pending_tasks: tokio::task::JoinSet::new(),
        }
    }

    fn spawn_pending_task(&mut self, task: impl std::future::Future<Output = ()> + Send + 'static) {
        while self.pending_tasks.try_join_next().is_some() {}
        self.pending_tasks.spawn(task);
    }

    pub(super) fn abort_pending_tasks(&mut self) {
        self.pending_tasks.abort_all();
    }

    /// Strat listen for downstream mining node. Return as soon as one downstream connect.
    pub async fn start(
        self_mutex: Arc<Mutex<Self>>,
        mut receiver: TReceiver<MiningOwned>,
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
                    error!("Jd error: {e:?}");
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
        incoming: MiningOwned,
    ) -> Result<(), JdClientError> {
        let next_message_to_send = self_mutex
            .safe_lock(|state| state.handle_owned_message(&incoming))
            .map_err(|_| Error::DownstreamDown)
            .and_then(|result| result);
        Self::match_send_to(self_mutex.clone(), next_message_to_send, Some(incoming)).await
        //Propgate error, caller will restart proxy
    }

    #[async_recursion::async_recursion]
    async fn match_send_to(
        self_mutex: Arc<Mutex<Self>>,
        next_message_to_send: Result<SendTo<UpstreamMiningNode>, Error>,
        incoming: Option<MiningOwned>,
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
            Ok(SendTo::RelayNewMessage(MiningOwned::SubmitSharesExtended(mut share))) => {
                let task_state = self_mutex.clone();
                self_mutex.safe_lock(|state| state.spawn_pending_task(async move {
                    let local_job_id = share.job_id;
                    let upstream_mutex = match task_state
                        .safe_lock(|state| state.status.get_upstream())
                    {
                        Ok(Some(upstream)) => upstream,
                        Ok(None) => {
                            error!("Upstream is unavailable while relaying a share");
                            ProxyState::update_downstream_state(
                                DownstreamType::JdClientMiningDownstream,
                            );
                            return;
                        }
                        Err(error) => {
                            error!(%error, "Downstream state is unavailable while relaying a share");
                            ProxyState::update_downstream_state(
                                DownstreamType::JdClientMiningDownstream,
                            );
                            return;
                        }
                    };

                    let job_id_future =
                        UpstreamMiningNode::get_job_id(&upstream_mutex, local_job_id);
                    if let Ok(Ok(job_id)) = timeout(Duration::from_secs(20), job_id_future).await {
                        share.job_id = job_id;
                        debug!("Relaying share upstream with pool job_id {}", job_id);
                        let message = MiningOwned::SubmitSharesExtended(share);
                        if let Err(error) = UpstreamMiningNode::send(&upstream_mutex, message).await
                        {
                            error!(%error, "Failed to relay share upstream");
                            ProxyState::update_upstream_state(UpstreamType::JDCMiningUpstream);
                        }
                    } else {
                        error!(local_job_id, "Timeout getting pool job id; discard share");
                    }
                }))
                .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?;
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
        message: MiningOwned,
    ) -> Result<(), JdClientError> {
        let sender = self_mutex
            .safe_lock(|self_| self_.sender.clone())
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?;
        sender
            .send(message)
            .await
            .map_err(|_| JdClientError::Unrecoverable)
    }

    pub(crate) async fn wait_for_mining_channel(
        self_mutex: &Arc<Mutex<Self>>,
    ) -> Result<(), JdClientError> {
        // TP can arrive before mining setup. Wait before preparing the canonical template,
        // otherwise the first declaration and mining job can omit the required DIFF32 output.
        timeout(Duration::from_secs(10), async {
            loop {
                let ready = self_mutex
                    .safe_lock(|state| {
                        state
                            .status
                            .get_channel()
                            .is_ok_and(|channel| !channel.get_extended_channels_ids().is_empty())
                    })
                    .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?;
                if ready {
                    return Ok::<(), JdClientError>(());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| JdClientError::Unrecoverable)?
    }

    pub(crate) fn apply_difficulty_commitment(
        self_mutex: &Arc<Mutex<Self>>,
        template: &mut NewTemplateOwned,
    ) -> Result<(), JdClientError> {
        let upstream = self_mutex
            .safe_lock(|state| match &state.status {
                DownstreamMiningNodeStatus::ChannelOpened((_, upstream)) => Some(upstream.clone()),
                _ => None,
            })
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)?;
        if let Some(upstream) = upstream {
            UpstreamMiningNode::apply_difficulty_commitment(&upstream, template)?;
        }
        Ok(())
    }

    pub(crate) async fn on_new_template(
        self_mutex: &Arc<Mutex<Self>>,
        mut new_template: NewTemplateOwned,
        pool_output: &[u8],
        template_generation: Option<u64>,
    ) -> Result<Option<DownstreamJob>, JdClientError> {
        let pool_outputs = Self::decode_pool_coinbase_outputs(pool_output)?;
        Self::validate_final_coinbase_output_count(&new_template, pool_outputs.len())?;

        let to_send = self_mutex
            .safe_lock(|state| {
                let channel = state
                    .status
                    .get_channel()
                    .map_err(JdClientError::RolesSv2Logic)?;
                channel.update_pool_outputs(pool_outputs);
                channel
                    .on_new_template(&mut new_template)
                    .map_err(JdClientError::RolesSv2Logic)
            })
            .map_err(|_| JdClientError::JdClientDownstreamMutexCorrupted)??;

        // to_send is HashMap<channel_id, messages_to_send> but here we have only one downstream so
        // only one channel opened downstream. That means that we can take all the messages in the
        // map and send them downstream.
        let messages = to_send.into_values().collect::<Vec<_>>();
        let downstream_job = messages.iter().find_map(|message| {
            if let MiningOwned::NewExtendedMiningJob(job) = message {
                Some(DownstreamJob {
                    local_job_id: job.job_id,
                    coinbase_tx_prefix: job.coinbase_tx_prefix.clone(),
                    coinbase_tx_suffix: job.coinbase_tx_suffix.clone(),
                })
            } else {
                None
            }
        });

        if crate::merge_mining::enabled() {
            if let Some(job) = &downstream_job {
                let bound = template_generation.is_some_and(|generation| {
                    crate::merge_mining::global().bind_job_generation(
                        job.local_job_id,
                        generation,
                        job.coinbase_tx_prefix.as_bytes().to_vec(),
                        job.coinbase_tx_suffix.as_bytes().to_vec(),
                    )
                });
                if !bound {
                    crate::merge_mining::global().announce_unbound_job(job.local_job_id);
                }
            }
        }

        for message in messages {
            Self::send(self_mutex, message)
                .await
                .map_err(|_| Error::DownstreamDown)?; // Caller will restart proxy
        }
        // See coment on the definition of the global for memory
        // ordering
        super::IS_NEW_TEMPLATE_HANDLED.store(true, std::sync::atomic::Ordering::Release);
        Ok(downstream_job)
    }

    pub async fn on_set_new_prev_hash(
        self_mutex: &Arc<Mutex<Self>>,
        new_prev_hash: TemplateSetNewPrevHashOwned,
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
        let to_send = SetNewPrevHashOwned {
            channel_id,
            job_id,
            prev_hash: new_prev_hash.prev_hash,
            min_ntime: new_prev_hash.header_timestamp,
            nbits: new_prev_hash.n_bits,
        };
        let message = MiningOwned::SetNewPrevHash(to_send);
        Self::send(self_mutex, message)
            .await
            .map_err(|_| Error::DownstreamDown)?; // Caller will restart proxy
        Ok(())
    }
}

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
impl DownstreamMiningNode {
    fn handle_owned_message(
        &mut self,
        message: &MiningOwned,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        use roles_logic_sv2::parsers::IsSv2Message;
        match message {
            MiningOwned::OpenExtendedMiningChannel(m) => {
                self.handle_open_extended_mining_channel(m.clone())
            }
            MiningOwned::UpdateChannel(m) => self.handle_update_channel(m.clone()),
            MiningOwned::SubmitSharesExtended(m) => self.handle_submit_shares_extended(m.clone()),
            MiningOwned::SetCustomMiningJob(m) => self.handle_set_custom_mining_job(m.clone()),
            _ => Err(Error::UnexpectedMessage(message.message_type())),
        }
    }

    fn handle_open_extended_mining_channel(
        &mut self,
        m: roles_logic_sv2::mining_sv2::OpenExtendedMiningChannelOwned,
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
            let extranonce_len = 32;
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
                error!("Coinbase tag + extranonce lens exceed 32 bytes");
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
        _: roles_logic_sv2::mining_sv2::UpdateChannelOwned,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        if !self.status.is_solo_miner() {
            // Safe unwrap alreay checked if it cointains upstream with is_solo_miner
            Ok(SendTo::RelaySameMessageToRemote(
                self.status
                    .get_upstream()
                    .ok_or(Error::NoUpstreamsConnected)?,
            ))
        } else {
            error!("Solo Mining currently Unsupported");
            std::process::exit(1)
        }
    }

    fn handle_submit_shares_extended(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SubmitSharesExtendedOwned,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        match self
            .status
            .get_channel()
            .map_err(|_| Error::NotFoundChannelId)?
            .on_submit_shares_extended(m)?
        {
            OnNewShare::SendErrorDownstream(s) => {
                error!("Share do not meet downstream target");
                Ok(SendTo::Respond(MiningOwned::SubmitSharesError(s)))
            }
            OnNewShare::SendSubmitShareUpstream((m, Some(_template_id))) => {
                if !self.status.is_solo_miner() {
                    match m {
                        Share::Extended(share) => {
                            let for_upstream = MiningOwned::SubmitSharesExtended(share);
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
                        let solution = SubmitSolutionOwned {
                            template_id,
                            version: share.version,
                            header_timestamp: share.ntime,
                            header_nonce: share.nonce,
                            coinbase_tx: coinbase.try_into()?,
                        };
                        self.spawn_pending_task(async move {
                            if solution_sender.send(solution).await.is_err() {
                                error!("Downstream channel closed, couldn't send solution");
                            }
                        });
                        if !self.status.is_solo_miner() {
                            {
                                let jd = self.jd.clone();
                                let mut share = share.clone();
                                share.extranonce = extranonce.try_into()?;
                                self.spawn_pending_task(async move {
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
                            let for_upstream = MiningOwned::SubmitSharesExtended(share);
                            Ok(SendTo::RelayNewMessage(for_upstream))
                        } else {
                            Ok(SendTo::None(None))
                        }
                    }
                    // We are in an extended channel shares are extended
                    Share::Standard(_) => unreachable!(),
                }
            }
            // ShareMeetBitcoinTarget without template id is impossibvle
            OnNewShare::ShareMeetBitcoinTarget(_) => unreachable!(),
            OnNewShare::SendSubmitShareUpstream(_) => unreachable!(),
            OnNewShare::ShareMeetDownstreamTarget => Ok(SendTo::None(None)),
        }
    }

    fn handle_set_custom_mining_job(
        &mut self,
        _: roles_logic_sv2::mining_sv2::SetCustomMiningJobOwned,
    ) -> Result<SendTo<UpstreamMiningNode>, Error> {
        warn!("Ignoring SetCustomMiningJob");
        Ok(SendTo::None(None))
    }
}
impl IsMiningDownstream for DownstreamMiningNode {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pending_share_relay_stops_when_jd_session_is_aborted() {
        let manager = crate::jd_client::task_manager::TaskManager::initialize();
        let abortable = manager.safe_lock(|manager| manager.get_aborter()).unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let upstream = UpstreamMiningNode::new(8, sender).await.unwrap();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let (solutions, _receiver) = tokio::sync::mpsc::channel(1);
        let downstream = Arc::new(Mutex::new(DownstreamMiningNode::new(
            sender,
            Some(upstream.clone()),
            solutions,
            false,
            vec![],
            None,
        )));
        upstream
            .safe_lock(|upstream| upstream.downstream = Some(downstream.clone()))
            .unwrap();
        crate::jd_client::task_manager::TaskManager::add_mining_session(
            manager.clone(),
            upstream.clone(),
        )
        .await
        .unwrap();
        DownstreamMiningNode::match_send_to(
            downstream.clone(),
            Ok(SendTo::RelayNewMessage(MiningOwned::SubmitSharesExtended(
                SubmitSharesExtendedOwned {
                    channel_id: 1,
                    sequence_number: 1,
                    job_id: 999,
                    nonce: 0,
                    ntime: 0,
                    version: 0,
                    extranonce: vec![0; 8].try_into().unwrap(),
                },
            ))),
            None,
        )
        .await
        .unwrap();
        let old_downstream = Arc::downgrade(&downstream);
        drop(downstream);
        drop(upstream);
        drop(abortable);
        drop(manager);
        let released = tokio::time::timeout(Duration::from_secs(1), async {
            while old_downstream.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            released.is_ok(),
            "old share relay survives session shutdown"
        );
    }
    use bitcoin::{consensus::serialize, script::PushBytesBuf, Amount, ScriptBuf};

    #[test]
    fn solo_mining_commitment_does_not_wait_for_channel() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let (solution_sender, _solutions) = tokio::sync::mpsc::channel(1);
        let downstream = Arc::new(Mutex::new(DownstreamMiningNode::new(
            sender,
            None,
            solution_sender,
            false,
            vec![],
            None,
        )));
        let mut template = template_with_outputs(1, 1);
        let outputs = template.coinbase_tx_outputs.as_bytes().to_vec();

        DownstreamMiningNode::apply_difficulty_commitment(&downstream, &mut template)
            .expect("solo mining does not require a difficulty commitment");

        assert_eq!(template.coinbase_tx_outputs.as_ref(), outputs.as_slice());
        assert_eq!(template.coinbase_tx_outputs_count, 1);
    }

    #[tokio::test]
    async fn first_template_waits_before_adding_difficulty_commitment() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        let (solution_sender, _solutions) = tokio::sync::mpsc::channel(8);
        let (upstream_sender, _upstream_receiver) = tokio::sync::mpsc::channel(8);
        let upstream = UpstreamMiningNode::new(8, upstream_sender).await.unwrap();
        let target =
            bitcoin::Target::from_compact(bitcoin::CompactTarget::from_consensus(0x1d00ffff));
        let downstream = Arc::new(Mutex::new(DownstreamMiningNode {
            sender,
            status: DownstreamMiningNodeStatus::Paired(upstream.clone()),
            prev_job_id: None,
            solution_sender,
            withhold: false,
            miner_coinbase_output: vec![],
            jd: None,
            pending_tasks: tokio::task::JoinSet::new(),
        }));
        let mut template = template_with_outputs(0, 0);
        template.future_template = true;
        template.version = 0x20000000;
        template.coinbase_tx_version = 2;
        template.coinbase_prefix = vec![2, 1, 1, 0].try_into().unwrap();
        template.coinbase_tx_value_remaining = 5_000_000_000;
        let output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        };
        let encoded = serialize(&vec![output.clone()]);
        let task_downstream = downstream.clone();
        let task = tokio::spawn(async move {
            DownstreamMiningNode::wait_for_mining_channel(&task_downstream).await?;
            DownstreamMiningNode::apply_difficulty_commitment(&task_downstream, &mut template)?;
            DownstreamMiningNode::on_new_template(&task_downstream, template, &encoded, None).await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !task.is_finished(),
            "first template was discarded before channel setup"
        );
        upstream
            .safe_lock(|state| {
                state.downstream = Some(downstream.clone());
                state.handle_open_extended_mining_channel_success(
                    OpenExtendedMiningChannelSuccessOwned {
                        request_id: 1,
                        channel_id: 1,
                        group_channel_id: 7,
                        target: target.to_le_bytes().into(),
                        extranonce_size: 28,
                        extranonce_prefix: vec![0; 4].try_into().unwrap(),
                    },
                )
            })
            .unwrap()
            .unwrap();
        let mut factory = PoolChannelFactory::new(
            Arc::new(Mutex::new(roles_logic_sv2::utils::GroupId::new())),
            ExtendedExtranonce::new(0..0, 0..4, 4..32),
            JobsCreators::new(32),
            60.0,
            roles_logic_sv2::channel_logic::channel_factory::ExtendedChannelKind::Pool,
            vec![output],
            vec![],
        )
        .unwrap();
        factory.new_extended_channel(1, 100_000.0, 8).unwrap();
        downstream
            .safe_lock(|state| {
                state.status = DownstreamMiningNodeStatus::ChannelOpened((factory, upstream))
            })
            .unwrap();
        let job = timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            job.is_some(),
            "first template did not produce a declaration job"
        );
        let job = job.unwrap();
        let coinbase: bitcoin::Transaction = bitcoin::consensus::deserialize(
            &[
                job.coinbase_tx_prefix.as_ref(),
                &[0; 32],
                job.coinbase_tx_suffix.as_ref(),
            ]
            .concat(),
        )
        .unwrap();
        let mut commitment = b"\x6a\x0aDIFF32".to_vec();
        commitment.extend_from_slice(&(target.difficulty_float() as f32).to_le_bytes());
        assert!(coinbase
            .output
            .iter()
            .any(|output| output.value == Amount::ZERO
                && output.script_pubkey.as_bytes() == commitment));
        assert!(matches!(
            receiver.recv().await,
            Some(MiningOwned::NewExtendedMiningJob(_))
        ));
    }

    fn template_with_outputs(actual_count: usize, declared_count: u32) -> NewTemplateOwned {
        let output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new(),
        };
        let outputs = (0..actual_count)
            .flat_map(|_| serialize(&output))
            .collect::<Vec<_>>();
        NewTemplateOwned {
            template_id: 1,
            future_template: false,
            version: 0,
            coinbase_tx_version: 0,
            coinbase_prefix: Vec::new().try_into().expect("empty prefix"),
            coinbase_tx_input_sequence: 0,
            coinbase_tx_value_remaining: 0,
            coinbase_tx_outputs_count: declared_count,
            coinbase_tx_outputs: outputs.try_into().expect("serialized outputs"),
            coinbase_tx_locktime: 0,
            merkle_path: Vec::new().try_into().expect("empty merkle path"),
        }
    }

    #[test]
    fn normal_coinbase_accepts_252_final_outputs() {
        let template = template_with_outputs(250, 250);
        assert!(DownstreamMiningNode::validate_final_coinbase_output_count(&template, 2,).is_ok());
    }

    #[test]
    fn normal_coinbase_rejects_253_final_outputs() {
        let template = template_with_outputs(251, 251);
        assert!(matches!(
            DownstreamMiningNode::validate_final_coinbase_output_count(&template, 2,),
            Err(JdClientError::UnsupportedCoinbaseOutputCount {
                count: 253,
                max: MAX_SUPPORTED_COINBASE_OUTPUTS,
            })
        ));
    }

    #[test]
    fn normal_coinbase_uses_serialized_instead_of_declared_output_count() {
        let template = template_with_outputs(252, 251);
        assert!(matches!(
            DownstreamMiningNode::validate_final_coinbase_output_count(&template, 1,),
            Err(JdClientError::UnsupportedCoinbaseOutputCount { count: 253, .. })
        ));
    }

    #[test]
    fn decodes_every_pool_coinbase_output() {
        let outputs = vec![
            TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::new(),
            },
            TxOut {
                value: Amount::from_sat(2),
                script_pubkey: ScriptBuf::new(),
            },
        ];
        let encoded = serialize(&outputs);
        assert_eq!(
            DownstreamMiningNode::decode_pool_coinbase_outputs(&encoded)
                .expect("valid pool outputs"),
            outputs
        );
    }

    #[test]
    fn merge_preview_rejects_only_the_modified_b064k_boundary_job() {
        let mut template_outputs = (0..6)
            .map(|_| TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::from_bytes(vec![0x51; 9_360]),
            })
            .collect::<Vec<_>>();
        template_outputs.push(TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(vec![0x51; 9_246]),
        });
        let pristine_outputs = template_outputs
            .iter()
            .flat_map(serialize)
            .collect::<Vec<_>>();
        assert_eq!(pristine_outputs.len(), 65_483);

        let pristine = NewTemplateOwned {
            template_id: u64::MAX,
            future_template: true,
            version: 0,
            coinbase_tx_version: 2,
            coinbase_prefix: vec![1, 1, 0].try_into().expect("valid BIP34 test prefix"),
            coinbase_tx_input_sequence: 0,
            coinbase_tx_value_remaining: 1,
            coinbase_tx_outputs_count: 7,
            coinbase_tx_outputs: pristine_outputs
                .clone()
                .try_into()
                .expect("pristine outputs"),
            coinbase_tx_locktime: 0,
            merkle_path: Vec::new().try_into().expect("empty merkle path"),
        };
        let pool_outputs = vec![TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new(),
        }];

        let (_, pristine_suffix) = DownstreamMiningNode::preview_template_job_with_extranonce(
            pristine.clone(),
            pool_outputs.clone(),
            32,
        )
        .expect("pristine job must fit");
        assert_eq!(pristine_suffix.as_ref().len(), u16::MAX as usize);

        let payload = [b"RSKBLOCK:".as_slice(), &[0_u8; 32]].concat();
        let push = PushBytesBuf::try_from(payload).expect("valid OP_RETURN payload");
        let merge_output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(push),
        };
        let mut modified_outputs = pristine_outputs;
        modified_outputs.extend_from_slice(&serialize(&merge_output));
        assert_eq!(modified_outputs.len(), u16::MAX as usize);
        let mut modified = pristine;
        modified.coinbase_tx_outputs_count += 1;
        modified.coinbase_tx_outputs = modified_outputs.try_into().expect("modified outputs fit");

        assert!(DownstreamMiningNode::preview_template_job_with_extranonce(
            modified,
            pool_outputs,
            32,
        )
        .is_err());
    }
}
