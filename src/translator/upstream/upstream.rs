use super::super::{
    downstream::{Downstream, SubmitShareResult, SubmitShareResultSender, UpstreamSubmitShare},
    error::{Error, ProxyResult},
    upstream::diff_management::UpstreamDifficultyConfig,
};
use binary_sv2::U256;
use roles_logic_sv2::{
    common_messages_sv2::Protocol,
    common_properties::{IsMiningUpstream, IsUpstream},
    handlers::{
        common::{ParseUpstreamCommonMessages, SendTo as SendToCommon},
        mining::{ParseUpstreamMiningMessages, SendTo},
    },
    mining_sv2::{
        ExtendedExtranonce, Extranonce, NewExtendedMiningJob, OpenExtendedMiningChannel,
        SetCustomMiningJob, SetNewPrevHash, SubmitSharesExtended,
    },
    parsers::Mining,
    routing_logic::{MiningRoutingLogic, NoRouting},
    selectors::NullDownstreamMiningSelector,
    utils::Mutex,
    Error as RolesLogicError,
};
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::{
    sync::{
        mpsc::{Receiver as TReceiver, Sender as TSender},
        watch,
    },
    task,
};
use tracing::{error, info, warn};

use super::task_manager::TaskManager;
use crate::{
    proxy_state::{ProxyState, UpstreamType},
    shared::utils::AbortOnDrop,
    translator::utils::submit_error_to_rejection_reason,
};
use bitcoin::BlockHash;

pub static IS_NEW_JOB_HANDLED: AtomicBool = AtomicBool::new(true);
/// Represents the currently active `prevhash` of the mining job being worked on OR being submitted
/// from the Downstream role.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PrevHash {
    /// `prevhash` of mining job.
    prev_hash: BlockHash,
    /// `nBits` encoded difficulty target.
    nbits: u32,
}

#[derive(Debug)]
pub struct Upstream {
    /// Newly assigned identifier of the channel, stable for the whole lifetime of the connection,
    /// e.g. it is used for broadcasting new jobs by the `NewExtendedMiningJob` message.
    pub(super) channel_id: Option<u32>,
    /// Identifier of the job as provided by the `NewExtendedMiningJob` message.
    job_id: Option<u32>,
    /// Identifier of the job as provided by the ` SetCustomMiningJobSucces` message
    last_job_id: Option<u32>,
    /// Bytes used as implicit first part of `extranonce`.
    extranonce_prefix: Option<Vec<u8>>,
    /// Sends SV2 `SetNewPrevHash` messages to be translated (along with SV2 `NewExtendedMiningJob`
    /// messages) into SV1 `mining.notify` messages. Received and translated by the `Bridge`.
    tx_sv2_set_new_prev_hash: tokio::sync::mpsc::Sender<SetNewPrevHash<'static>>,
    /// Sends SV2 `NewExtendedMiningJob` messages to be translated (along with SV2 `SetNewPrevHash`
    /// messages) into SV1 `mining.notify` messages. Received and translated by the `Bridge`.
    tx_sv2_new_ext_mining_job: tokio::sync::mpsc::Sender<NewExtendedMiningJob<'static>>,
    /// Sends the extranonce1 and the channel id received in the SV2 `OpenExtendedMiningChannelSuccess` message to be
    /// used by the `Downstream` and sent to the Downstream role in a SV2 `mining.subscribe`
    /// response message. Passed to the `Downstream` on connection creation.
    tx_sv2_extranonce: tokio::sync::mpsc::Sender<(ExtendedExtranonce, u32)>,
    /// The first `target` is received by the Upstream role in the SV2
    /// `OpenExtendedMiningChannelSuccess` message, then updated periodically via SV2 `SetTarget`
    /// messages. Passed to the `Downstream` on connection creation and sent to the Downstream role
    /// via the SV1 `mining.set_difficulty` message.
    target: Arc<Mutex<Vec<u8>>>,
    /// Minimum `extranonce2` size. Initially requested in the `proxy-config.toml`, and ultimately
    /// set by the SV2 Upstream via the SV2 `OpenExtendedMiningChannelSuccess` message.
    pub min_extranonce_size: u16,
    pub upstream_extranonce1_size: usize,
    // values used to update the channel with the correct nominal hashrate.
    // each Downstream instance will add and subtract their hashrates as needed
    // and the upstream just needs to occasionally check if it has changed more than
    // than the configured percentage
    pub(super) difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
    pub sender: TSender<Mining<'static>>,
    signature: String,
    sent_up: u32,
    rejected: u32,
    toa: Vec<std::time::Instant>,
    next_submit_sequence: u32,
    pending_submits: BTreeMap<u32, SubmitShareResultSender>,
}

impl PartialEq for Upstream {
    fn eq(&self, other: &Self) -> bool {
        self.channel_id == other.channel_id
    }
}

impl Upstream {
    /// Instantiate a new `Upstream`.
    /// Connect to the SV2 Upstream role (most typically a SV2 Pool). Initializes the
    /// `UpstreamConnection` with a channel to send and receive messages from the SV2 Upstream
    /// role and uses channels provided in the function arguments to send and receive messages
    /// from the `Downstream`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        tx_sv2_set_new_prev_hash: tokio::sync::mpsc::Sender<SetNewPrevHash<'static>>,
        tx_sv2_new_ext_mining_job: tokio::sync::mpsc::Sender<NewExtendedMiningJob<'static>>,
        min_extranonce_size: u16,
        tx_sv2_extranonce: tokio::sync::mpsc::Sender<(ExtendedExtranonce, u32)>,
        target: Arc<Mutex<Vec<u8>>>,
        difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
        sender: TSender<Mining<'static>>,
        signature: String,
    ) -> ProxyResult<'static, Arc<Mutex<Self>>> {
        Ok(Arc::new(Mutex::new(Self {
            extranonce_prefix: None,
            tx_sv2_set_new_prev_hash,
            tx_sv2_new_ext_mining_job,
            channel_id: None,
            job_id: None,
            last_job_id: None,
            min_extranonce_size,
            upstream_extranonce1_size: crate::UPSTREAM_EXTRANONCE1_SIZE,
            tx_sv2_extranonce,
            target,
            difficulty_config,
            sender,
            signature,
            sent_up: 0,
            rejected: 0,
            toa: Vec::new(),
            next_submit_sequence: 0,
            pending_submits: BTreeMap::new(),
        })))
    }

    pub async fn start(
        self_: Arc<Mutex<Self>>,
        incoming_receiver: TReceiver<Mining<'static>>,
        rx_sv2_submit_shares_ext: TReceiver<UpstreamSubmitShare>,
        rx_update_token: TReceiver<String>,
        diff_update_rx: watch::Receiver<u64>,
    ) -> Result<AbortOnDrop, Error<'static>> {
        let task_manager = TaskManager::initialize();
        let abortable = task_manager
            .safe_lock(|t| t.get_aborter())
            .map_err(|_| Error::TranslatorTaskManagerMutexPoisoned)?
            .ok_or(Error::TranslatorTaskManagerFailed)?;

        Self::connect(self_.clone()).await?; // Propagate error, it will be handled in the caller

        let (diff_manager_abortable, main_loop_abortable) =
            Self::parse_incoming(self_.clone(), incoming_receiver, diff_update_rx)?;

        let handle_submit_abortable = Self::handle_submit(self_.clone(), rx_sv2_submit_shares_ext)?;
        let handle_token_update_abortable =
            Self::handle_token_updates(self_.clone(), rx_update_token)?;

        TaskManager::add_diff_managment(task_manager.clone(), diff_manager_abortable)
            .await
            .map_err(|_| Error::TranslatorTaskManagerFailed)?;
        TaskManager::add_main_loop(task_manager.clone(), main_loop_abortable)
            .await
            .map_err(|_| Error::TranslatorTaskManagerFailed)?;
        TaskManager::add_handle_submit(task_manager.clone(), handle_submit_abortable)
            .await
            .map_err(|_| Error::TranslatorTaskManagerFailed)?;
        TaskManager::add_handle_token_update(task_manager.clone(), handle_token_update_abortable)
            .await
            .map_err(|_| Error::TranslatorTaskManagerFailed)?;

        Ok(abortable)
    }

    /// Setups the connection with the SV2 Upstream role (most typically a SV2 Pool).
    async fn connect(self_: Arc<Mutex<Self>>) -> ProxyResult<'static, ()> {
        let sender = self_
            .safe_lock(|s| s.sender.clone())
            .map_err(|_e| Error::TranslatorUpstreamMutexPoisoned)?;

        // Send open channel request
        let nominal_hash_rate = self_
            .safe_lock(|u| {
                u.difficulty_config
                    .safe_lock(|c| c.channel_nominal_hashrate)
                    .map_err(|_e| Error::TranslatorDiffConfigMutexPoisoned)
            })
            .map_err(|_e| Error::TranslatorUpstreamMutexPoisoned)??;
        let user_identity = "ABC".to_string().try_into().expect("Internal error: this operation can not fail because the string ABC can always be converted into Inner");
        let open_channel = Mining::OpenExtendedMiningChannel(OpenExtendedMiningChannel {
            request_id: 0, // TODO
            user_identity, // TODO
            nominal_hash_rate,
            max_target: u256_max(),
            min_extranonce_size: crate::MIN_EXTRANONCE2_SIZE,
        });

        if sender.send(open_channel).await.is_err() {
            error!("Failed to send message");
            return Err(Error::AsyncChannelError);
        };
        Ok(())
    }

    fn handle_token_updates(
        self_: Arc<Mutex<Self>>,
        mut rx_update_token: TReceiver<String>,
    ) -> ProxyResult<'static, AbortOnDrop> {
        let handle = tokio::task::spawn(async move {
            while let Some(new_token) = rx_update_token.recv().await {
                let sender = match self_.safe_lock(|s| s.sender.clone()) {
                    Ok(sender) => sender,
                    Err(e) => {
                        error!("Failed to lock upstream sender: {e}");
                        ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                        break;
                    }
                };

                let channel_id = match self_.safe_lock(|s| s.channel_id.unwrap_or(0)) {
                    Ok(channel_id) => channel_id,
                    Err(e) => {
                        error!("Failed to lock upstream channel id: {e}");
                        ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                        break;
                    }
                };

                const UPDATE_USER_CODE: &[u8] = "UPDATE_USER_CODE_REPLACE_USER_ID".as_bytes();
                let special_message = Mining::SetCustomMiningJob(SetCustomMiningJob {
                    channel_id,
                    request_id: 0,
                    token: UPDATE_USER_CODE.to_vec().try_into().expect(
                        "Internal error: this operation can not fail because the string UPDATE_USER_CODE_REPLACE_USER_ID can always be converted into Inner",
                    ),
                    version: 0,
                    prev_hash: [0; 32].to_vec().try_into().expect(
                        "Internal error: this operation can not fail because the array [0; 32] can always be converted into Inner",
                    ),
                    min_ntime: 0,
                    nbits: 0,
                    coinbase_tx_version: 0,
                    coinbase_prefix: new_token.as_bytes().to_vec().try_into().expect(
                        "Internal error: this operation can not fail because the token can always be converted into Inner",
                    ),
                    coinbase_tx_input_n_sequence: 0,
                    coinbase_tx_value_remaining: 0,
                    coinbase_tx_outputs: [].to_vec().try_into().expect(
                        "Internal error: this operation can not fail because the array [] can always be converted into Inner",
                    ),
                    coinbase_tx_locktime: 0,
                    merkle_path: [].to_vec().into(),
                    extranonce_size: 0,
                });

                if sender.send(special_message).await.is_err() {
                    error!("Failed to update user");
                    ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                    break;
                } else {
                    info!("User updated successfully");
                }
            }
        });

        Ok(handle.into())
    }

    /// Parses the incoming SV2 message from the Upstream role and routes the message to the
    /// appropriate handler.
    #[allow(clippy::result_large_err)]
    pub fn parse_incoming(
        self_: Arc<Mutex<Self>>,
        mut receiver: TReceiver<Mining<'static>>,
        diff_update_rx: watch::Receiver<u64>,
    ) -> ProxyResult<'static, (AbortOnDrop, AbortOnDrop)> {
        let clone = self_.clone();
        let (tx_frame, tx_sv2_extranonce, tx_sv2_new_ext_mining_job, tx_sv2_set_new_prev_hash) =
            clone
                .safe_lock(|s| {
                    (
                        s.sender.clone(),
                        s.tx_sv2_extranonce.clone(),
                        s.tx_sv2_new_ext_mining_job.clone(),
                        s.tx_sv2_set_new_prev_hash.clone(),
                    )
                })
                .map_err(|_| Error::TranslatorUpstreamMutexPoisoned)?;
        let diff_manager_handle = {
            let self_ = self_.clone();
            task::spawn(async move { Self::run_diff_management(self_, diff_update_rx).await })
        };

        let main_loop_handle = {
            let self_ = self_.clone();
            task::spawn(async move {
                let mut future_j = None;
                while let Some(m) = receiver.recv().await {
                    let routing_logic = MiningRoutingLogic::None;

                    // Gets the response message for the received SV2 Upstream role message
                    // `handle_message_mining` takes care of the SetupConnection +
                    // SetupConnection.Success
                    let next_message_to_send = Upstream::handle_message_mining_deserialized(
                        self_.clone(),
                        Ok(m),
                        routing_logic,
                    );

                    // Routes the incoming messages accordingly
                    match next_message_to_send {
                        // No translation required, simply respond to SV2 pool w a SV2 message
                        Ok(SendTo::Respond(message_for_upstream)) => {
                            // Relay the response message to the Upstream role
                            if tx_frame.send(message_for_upstream).await.is_err() {
                                error!("Failed to send message to upstream role");
                                return;
                            };
                        }
                        // Does not send the messages anywhere, but instead handle them internally
                        Ok(SendTo::None(Some(m))) => {
                            match m {
                                Mining::OpenExtendedMiningChannelSuccess(m) => {
                                    let prefix_len = m.extranonce_prefix.len();
                                    // update upstream_extranonce1_size for tracking
                                    let miner_extranonce2_size = match self_.safe_lock(|u| {
                                        u.upstream_extranonce1_size = prefix_len;
                                        u.min_extranonce_size as usize
                                    }) {
                                        Ok(extranounce2_size) => extranounce2_size,
                                        Err(e) => {
                                            error!("Translator upstream mutex poisoned: {e}");
                                            return;
                                        }
                                    };
                                    let extranonce_prefix: Extranonce = m.extranonce_prefix.into();
                                    // Create the extended extranonce that will be saved in bridge and
                                    // it will be used to open downstream (sv1) channels
                                    // range 0 is the extranonce1 from upstream
                                    // range 1 is the extranonce1 added by the tproxy
                                    // range 2 is the extranonce2 used by the miner for rolling
                                    // range 0 + range 1 is the extranonce1 sent to the miner
                                    let tproxy_e1_len = proxy_extranonce1_len(
                                        m.extranonce_size as usize,
                                        miner_extranonce2_size,
                                    );
                                    let range_0 = 0..prefix_len; // upstream extranonce1
                                    let range_1 = prefix_len..prefix_len + tproxy_e1_len; // downstream extranonce1
                                    let range_2 = prefix_len + tproxy_e1_len
                                        ..prefix_len + m.extranonce_size as usize; // extranonce2
                                    let extended = match ExtendedExtranonce::from_upstream_extranonce(
                                        extranonce_prefix.clone(), range_0.clone(), range_1.clone(), range_2.clone(),
                                    ).ok_or( Error::InvalidExtranonce(format!("Impossible to create a valid extended extranonce from {extranonce_prefix:?} {range_0:?} {range_1:?} {range_2:?}"))) {
                                            Ok(extended_extranounce) => extended_extranounce,
                                            Err(e) => {
                                                error!(
                                                    "Failed to create a valid extended extranonce from {:?} {:?} {:?} {:?}: {:?}",
                                                    extranonce_prefix, range_0, range_1, range_2, e
                                                ); ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                                                break;
                                            }
                                        };

                                    if tx_sv2_extranonce
                                        .send((extended, m.channel_id))
                                        .await
                                        .is_err()
                                    {
                                        error!("Failed to send extended extranounce");
                                        return;
                                    };
                                }
                                Mining::NewExtendedMiningJob(m) => {
                                    info!("Parsing incoming NewExtendedMiningJob message from Pool for Channel Id: {}", m.channel_id);
                                    if m.is_future() {
                                        future_j = Some(m)
                                    } else {
                                        let job_id = m.job_id;
                                        if let Err(e) = self_.safe_lock(|s| {
                                            let _ = s.job_id.insert(job_id);
                                        }) {
                                            error!("Translator upstream mutex poisoned: {e}");
                                            return;
                                        };
                                        if tx_sv2_new_ext_mining_job.send(m).await.is_err() {
                                            error!("Failed to send NewExtendedMiningJob");
                                            return;
                                        };
                                    }
                                }
                                Mining::SetNewPrevHash(m) => {
                                    info!("Parsing incoming SetNewPrevHash message from Pool for Channel Id: {}", m.channel_id);
                                    if let Some(j) = future_j.clone() {
                                        future_j = None;
                                        let job_id = m.job_id;
                                        if let Err(e) = self_.safe_lock(|s| {
                                            let _ = s.job_id.insert(job_id);
                                        }) {
                                            error!("Translator upstream mutex poisoned: {e}");
                                            return;
                                        };
                                        if tx_sv2_new_ext_mining_job.send(j).await.is_err() {
                                            error!("Failed to send NewExtendedMiningJob");
                                            return;
                                        };
                                        if tx_sv2_set_new_prev_hash.send(m).await.is_err() {
                                            error!("Failed to send SetNewPrevHash");
                                            return;
                                        };
                                    } else if tx_sv2_set_new_prev_hash.send(m).await.is_err() {
                                        error!("Failed to send SetNewPrevHash");
                                        return;
                                    }
                                }
                                Mining::CloseChannel(_m) => {
                                    todo!()
                                }
                                Mining::OpenMiningChannelError(_)
                                | Mining::UpdateChannelError(_)
                                //| Mining::SubmitSharesError(_)
                                | Mining::SetCustomMiningJobError(_) => {
                                    todo!();
                                }
                                // impossible state: handle_message_mining only returns
                                // the above 3 messages in the Ok(SendTo::None(Some(m))) case to be sent
                                // to the bridge for translation.
                                _ => panic!(),
                            };
                        }
                        Ok(SendTo::None(None)) => (),
                        Ok(_) => panic!(),
                        Err(e) => {
                            error!("{}", Error::RolesSv2Logic(e));
                            return;
                        }
                    }
                }
                error!("Failed to receive message");
            })
        };
        Ok((diff_manager_handle.into(), main_loop_handle.into()))
    }

    fn register_pending_submit(&mut self, result_tx: SubmitShareResultSender) -> u32 {
        let sequence_number = self.next_submit_sequence;
        self.next_submit_sequence = self.next_submit_sequence.wrapping_add(1);
        self.pending_submits.insert(sequence_number, result_tx);
        sequence_number
    }

    fn resolve_submit_success(&mut self, last_sequence_number: u32, accepted_count: u32) {
        if accepted_count == 0 {
            return;
        }

        if accepted_count == 1 {
            if let Some(result_tx) = self.pending_submits.remove(&last_sequence_number) {
                let _ = result_tx.send(Ok(()));
            } else {
                warn!(
                    "SubmitSharesSuccess for unknown pending sequence {}",
                    last_sequence_number
                );
            }
            return;
        }

        let mut acknowledged_sequences: Vec<u32> = self
            .pending_submits
            .range(..=last_sequence_number)
            .map(|(sequence_number, _)| *sequence_number)
            .collect();

        acknowledged_sequences.reverse();
        acknowledged_sequences.truncate(accepted_count as usize);
        acknowledged_sequences.reverse();

        if acknowledged_sequences.len() < accepted_count as usize {
            warn!(
                "SubmitSharesSuccess acknowledged {} shares up to sequence {}, but only {} pending submits were available",
                accepted_count,
                last_sequence_number,
                acknowledged_sequences.len()
            );
        }

        for sequence_number in acknowledged_sequences {
            if let Some(result_tx) = self.pending_submits.remove(&sequence_number) {
                let _ = result_tx.send(Ok(()));
            }
        }
    }

    fn resolve_submit_error(&mut self, sequence_number: u32, result: SubmitShareResult) {
        if let Some(result_tx) = self.pending_submits.remove(&sequence_number) {
            let _ = result_tx.send(result);
        } else {
            warn!(
                "Received submit error for unknown pending sequence {}",
                sequence_number
            );
        }
    }

    fn handle_submit(
        self_: Arc<Mutex<Self>>,
        mut rx_submit: TReceiver<UpstreamSubmitShare>,
    ) -> ProxyResult<'static, AbortOnDrop> {
        let tx_frame = self_
            .safe_lock(|s| s.sender.clone())
            .map_err(|_| Error::TranslatorUpstreamMutexPoisoned)?;

        let handle = {
            let self_ = self_.clone();
            task::spawn(async move {
                loop {
                    let UpstreamSubmitShare {
                        mut share,
                        result_tx,
                    } = match rx_submit.recv().await {
                        Some(msg) => msg,
                        None => {
                            error!("Failed to receive SubmitShare message");
                            return;
                        }
                    };

                    let (channel_id, signature) = match self_.safe_lock(|s| {
                        s.channel_id
                            .ok_or(RolesLogicError::NotFoundChannelId)
                            .map(|channel_id| (channel_id, s.signature.clone()))
                    }) {
                        Ok(Ok(values)) => values,
                        Ok(Err(e)) => {
                            error!("{}", Error::RolesSv2Logic(e));
                            let _ = result_tx.send(Err(
                                crate::monitor::shares::RejectionReason::UpstreamRejected,
                            ));
                            return;
                        }
                        Err(e) => {
                            error!("Translator upstream mutex corrupted: {e}");
                            let _ = result_tx.send(Err(
                                crate::monitor::shares::RejectionReason::UpstreamRejected,
                            ));
                            return;
                        }
                    };

                    let mut pending_result_tx = Some(result_tx);
                    let sequence_number = match self_.safe_lock(|s| {
                        s.register_pending_submit(
                            pending_result_tx
                                .take()
                                .expect("pending submit result sender must be available"),
                        )
                    }) {
                        Ok(sequence_number) => sequence_number,
                        Err(e) => {
                            error!("Translator upstream mutex corrupted: {e}");
                            if let Some(result_tx) = pending_result_tx {
                                let _ = result_tx.send(Err(
                                    crate::monitor::shares::RejectionReason::UpstreamRejected,
                                ));
                            }
                            return;
                        }
                    };

                    share.channel_id = channel_id;
                    share.sequence_number = sequence_number;
                    let mut extranonce = signature.as_bytes().to_vec();
                    extranonce.extend_from_slice(&share.extranonce.to_vec());
                    share.extranonce = extranonce.try_into().unwrap();

                    let message = roles_logic_sv2::parsers::Mining::SubmitSharesExtended(share);

                    if tx_frame.send(message).await.is_err() {
                        if let Ok(Some(result_tx)) =
                            self_.safe_lock(|s| s.pending_submits.remove(&sequence_number))
                        {
                            let _ = result_tx.send(Err(
                                crate::monitor::shares::RejectionReason::UpstreamRejected,
                            ));
                        }
                        error!("Unable to send SubmitSharesExtended msg upstream");
                        return;
                    };
                    self_
                        .safe_lock(|s| {
                            s.sent_up += 1;
                            let avg = avg_seconds_between(&s.toa);
                            info!(
                                "accepted: {}/{} avg_toa {} blocks {}",
                                s.sent_up,
                                s.rejected,
                                avg,
                                s.toa.len()
                            );
                        })
                        .unwrap();
                }
            })
        };

        Ok(handle.into())
    }

    fn _is_contained_in_upstream_target(&self, _share: SubmitSharesExtended) -> bool {
        todo!()
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

impl ParseUpstreamCommonMessages<NoRouting> for Upstream {
    fn handle_setup_connection_success(
        &mut self,
        _: roles_logic_sv2::common_messages_sv2::SetupConnectionSuccess,
    ) -> Result<SendToCommon, RolesLogicError> {
        Ok(SendToCommon::None(None))
    }

    fn handle_setup_connection_error(
        &mut self,
        _: roles_logic_sv2::common_messages_sv2::SetupConnectionError,
    ) -> Result<SendToCommon, RolesLogicError> {
        todo!()
    }

    fn handle_channel_endpoint_changed(
        &mut self,
        _: roles_logic_sv2::common_messages_sv2::ChannelEndpointChanged,
    ) -> Result<SendToCommon, RolesLogicError> {
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
        false
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

    /// Handles the SV2 `OpenExtendedMiningChannelSuccess` message which provides important
    /// parameters including the `target` which is sent to the Downstream role in a SV1
    /// `mining.set_difficulty` message, and the extranonce values which is sent to the Downstream
    /// role in a SV1 `mining.subscribe` message response.
    fn handle_open_extended_mining_channel_success(
        &mut self,
        mut m: roles_logic_sv2::mining_sv2::OpenExtendedMiningChannelSuccess,
    ) -> Result<SendTo<Downstream>, RolesLogicError> {
        info!(
            "Handling OpenExtendedMiningChannelSuccess message from Pool for Channel Id: {}",
            m.channel_id
        );
        let mut prefix = m.extranonce_prefix.to_vec();
        prefix.extend_from_slice(self.signature.as_bytes());
        m.extranonce_prefix = prefix.try_into().unwrap();
        m.extranonce_size -= self.signature.len() as u16;
        let tproxy_e1_len =
            proxy_extranonce1_len(m.extranonce_size as usize, self.min_extranonce_size.into())
                as u16;
        if self.min_extranonce_size + tproxy_e1_len < m.extranonce_size {
            error!(
                "Invalid extranonce size for Channel Id {}: expected at least {} but got {}",
                m.channel_id,
                self.min_extranonce_size + tproxy_e1_len,
                m.extranonce_size
            );
            return Err(RolesLogicError::InvalidExtranonceSize(
                self.min_extranonce_size,
                m.extranonce_size,
            ));
        }
        info!(
            "Extended Channel with Channel Id {} has Target: {:?}",
            m.channel_id, m.target
        );
        self.target
            .safe_lock(|t| *t = m.target.to_vec())
            .map_err(|e| RolesLogicError::PoisonLock(e.to_string()))?;
        self.channel_id = Some(m.channel_id);
        self.extranonce_prefix = Some(m.extranonce_prefix.to_vec());
        let m = Mining::OpenExtendedMiningChannelSuccess(m.into_static());
        Ok(SendTo::None(Some(m)))
    }

    /// Handles the SV2 `OpenExtendedMiningChannelError` message (TODO).
    fn handle_open_mining_channel_error(
        &mut self,
        m: roles_logic_sv2::mining_sv2::OpenMiningChannelError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        Ok(SendTo::None(Some(Mining::OpenMiningChannelError(
            m.as_static(),
        ))))
    }

    /// Handles the SV2 `UpdateChannelError` message (TODO).
    fn handle_update_channel_error(
        &mut self,
        m: roles_logic_sv2::mining_sv2::UpdateChannelError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        Ok(SendTo::None(Some(Mining::UpdateChannelError(
            m.as_static(),
        ))))
    }

    /// Handles the SV2 `CloseChannel` message (TODO).
    fn handle_close_channel(
        &mut self,
        m: roles_logic_sv2::mining_sv2::CloseChannel,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        Ok(SendTo::None(Some(Mining::CloseChannel(m.as_static()))))
    }

    /// Handles the SV2 `SetExtranoncePrefix` message (TODO).
    fn handle_set_extranonce_prefix(
        &mut self,
        _: roles_logic_sv2::mining_sv2::SetExtranoncePrefix,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        todo!()
    }

    /// Handles the SV2 `SubmitSharesSuccess` message.
    fn handle_submit_shares_success(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SubmitSharesSuccess,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        self.resolve_submit_success(m.last_sequence_number, m.new_submits_accepted_count);
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `SubmitSharesError` message.
    fn handle_submit_shares_error(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SubmitSharesError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        self.rejected += 1;
        let error_code = std::str::from_utf8(&m.error_code.to_vec())
            .unwrap_or("unparsable error code")
            .to_string();
        self.resolve_submit_error(
            m.sequence_number,
            Err(submit_error_to_rejection_reason(&error_code)),
        );
        error!("Pool rejected share with error code {}", error_code);
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
        m: roles_logic_sv2::mining_sv2::NewExtendedMiningJob,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        if self.is_work_selection_enabled() {
            Ok(SendTo::None(None))
        } else {
            IS_NEW_JOB_HANDLED.store(false, std::sync::atomic::Ordering::SeqCst);
            if !m.version_rolling_allowed {
                warn!("VERSION ROLLING NOT ALLOWED IS A TODO");
                // todo!()
            }

            let message = Mining::NewExtendedMiningJob(m.into_static());

            Ok(SendTo::None(Some(message)))
        }
    }

    /// Handles the SV2 `SetNewPrevHash` message which is used (along with the SV2
    /// `NewExtendedMiningJob` message) to later create a SV1 `mining.notify` for the Downstream
    /// role.
    fn handle_set_new_prev_hash(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetNewPrevHash,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        if self.is_work_selection_enabled() {
            Ok(SendTo::None(None))
        } else {
            let now = std::time::Instant::now();
            self.toa.push(now);
            let message = Mining::SetNewPrevHash(m.into_static());
            Ok(SendTo::None(Some(message)))
        }
    }

    /// Handles the SV2 `SetCustomMiningJobSuccess` message (TODO).
    fn handle_set_custom_mining_job_success(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetCustomMiningJobSuccess,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        self.last_job_id = Some(m.job_id);
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `SetCustomMiningJobError` message (TODO).
    fn handle_set_custom_mining_job_error(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::SetCustomMiningJobError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        unimplemented!()
    }

    /// Handles the SV2 `SetTarget` message which updates the Downstream role(s) target
    /// difficulty via the SV1 `mining.set_difficulty` message.
    fn handle_set_target(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetTarget,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        let m = m.into_static();
        self.target
            .safe_lock(|t| *t = m.maximum_target.to_vec())
            .map_err(|e| RolesLogicError::PoisonLock(e.to_string()))?;
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `Reconnect` message (TODO).
    fn handle_reconnect(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::Reconnect,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        unimplemented!()
    }
}

/// currently the pool only supports 16 bytes exactly for its channels
/// to use but that may change
pub fn proxy_extranonce1_len(
    channel_extranonce2_size: usize,
    downstream_extranonce2_len: usize,
) -> usize {
    // full_extranonce_len - pool_extranonce1_len - miner_extranonce2 = tproxy_extranonce1_len
    channel_extranonce2_size - downstream_extranonce2_len
}
fn u256_max() -> U256<'static> {
    // initialize u256 as a bytes vec of len 24
    let u256 = vec![255_u8; 32];
    let u256: U256 = u256.try_into().expect("Internal error: this operation can not fail because the vec![255_u8; 32] can always be converted into U256");
    u256
}

fn avg_seconds_between(instants: &[std::time::Instant]) -> f64 {
    if instants.len() < 2 {
        return 0.0;
    };

    let total_secs: f64 = instants
        .windows(2)
        .map(|w| w[1].duration_since(w[0]).as_secs_f64())
        .sum();

    total_secs / (instants.len() - 1) as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::shares::RejectionReason;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::{timeout, Duration};

    async fn test_upstream() -> Arc<Mutex<Upstream>> {
        let (tx_sv2_set_new_prev_hash, _rx_sv2_set_new_prev_hash) = mpsc::channel(1);
        let (tx_sv2_new_ext_mining_job, _rx_sv2_new_ext_mining_job) = mpsc::channel(1);
        let (tx_sv2_extranonce, _rx_sv2_extranonce) = mpsc::channel(1);
        let (sender, _receiver) = mpsc::channel(1);

        Upstream::new(
            tx_sv2_set_new_prev_hash,
            tx_sv2_new_ext_mining_job,
            crate::MIN_EXTRANONCE_SIZE - 1,
            tx_sv2_extranonce,
            Arc::new(Mutex::new(vec![0; 32])),
            Arc::new(Mutex::new(
                UpstreamDifficultyConfig::new(crate::CHANNEL_DIFF_UPDTATE_INTERVAL, 0.0).0,
            )),
            sender,
            "sig".to_string(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn submit_success_resolves_pending_submit() {
        let upstream = test_upstream().await;
        let (result_tx, result_rx) = oneshot::channel();

        let sequence_number = upstream
            .safe_lock(|u| u.register_pending_submit(result_tx))
            .unwrap();
        assert_eq!(sequence_number, 0);

        upstream
            .safe_lock(|u| u.resolve_submit_success(sequence_number, 1))
            .unwrap();

        assert_eq!(result_rx.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn submit_success_resolves_exact_sequence_when_earlier_pending_exists() {
        let upstream = test_upstream().await;
        let (first_result_tx, first_result_rx) = oneshot::channel();
        let (second_result_tx, second_result_rx) = oneshot::channel();

        let (first_sequence, second_sequence) = upstream
            .safe_lock(|u| {
                (
                    u.register_pending_submit(first_result_tx),
                    u.register_pending_submit(second_result_tx),
                )
            })
            .unwrap();

        upstream
            .safe_lock(|u| u.resolve_submit_success(second_sequence, 1))
            .unwrap();

        assert_eq!(second_result_rx.await.unwrap(), Ok(()));
        assert!(
            timeout(Duration::from_millis(50), first_result_rx)
                .await
                .is_err(),
            "success for second sequence must not resolve first sequence"
        );

        upstream
            .safe_lock(|u| {
                u.resolve_submit_error(first_sequence, Err(RejectionReason::UpstreamRejected))
            })
            .unwrap();
    }

    #[tokio::test]
    async fn submit_error_resolves_pending_submit() {
        let upstream = test_upstream().await;
        let (result_tx, result_rx) = oneshot::channel();

        let sequence_number = upstream
            .safe_lock(|u| u.register_pending_submit(result_tx))
            .unwrap();
        upstream
            .safe_lock(|u| {
                u.resolve_submit_error(sequence_number, Err(RejectionReason::JobIdNotFound))
            })
            .unwrap();

        assert_eq!(
            result_rx.await.unwrap(),
            Err(RejectionReason::JobIdNotFound)
        );
    }

    #[tokio::test]
    async fn submit_success_skips_already_rejected_sequence() {
        let upstream = test_upstream().await;
        let (first_result_tx, first_result_rx) = oneshot::channel();
        let (second_result_tx, second_result_rx) = oneshot::channel();

        let (first_sequence, second_sequence) = upstream
            .safe_lock(|u| {
                (
                    u.register_pending_submit(first_result_tx),
                    u.register_pending_submit(second_result_tx),
                )
            })
            .unwrap();

        upstream
            .safe_lock(|u| {
                u.resolve_submit_error(first_sequence, Err(RejectionReason::JobIdNotFound));
                u.resolve_submit_success(second_sequence, 1);
            })
            .unwrap();

        assert_eq!(
            first_result_rx.await.unwrap(),
            Err(RejectionReason::JobIdNotFound)
        );
        assert_eq!(second_result_rx.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn immediate_signal_emits_update_channel_before_periodic_interval() {
        let (tx_sv2_set_new_prev_hash, _rx_sv2_set_new_prev_hash) = mpsc::channel(1);
        let (tx_sv2_new_ext_mining_job, _rx_sv2_new_ext_mining_job) = mpsc::channel(1);
        let (tx_sv2_extranonce, _rx_sv2_extranonce) = mpsc::channel(1);
        let (sender, mut receiver) = mpsc::channel(4);
        let (difficulty_config, diff_update_rx) =
            UpstreamDifficultyConfig::new(crate::CHANNEL_DIFF_UPDTATE_INTERVAL, 0.0);
        let difficulty_config = Arc::new(Mutex::new(difficulty_config));
        let upstream = Upstream::new(
            tx_sv2_set_new_prev_hash,
            tx_sv2_new_ext_mining_job,
            crate::MIN_EXTRANONCE_SIZE - 1,
            tx_sv2_extranonce,
            Arc::new(Mutex::new(vec![0; 32])),
            difficulty_config.clone(),
            sender,
            "sig".to_string(),
        )
        .await
        .unwrap();
        upstream.safe_lock(|u| u.channel_id = Some(42)).unwrap();

        let (_incoming_tx, incoming_rx) = mpsc::channel(1);
        let (diff_manager_abortable, main_loop_abortable) =
            Upstream::parse_incoming(upstream.clone(), incoming_rx, diff_update_rx).unwrap();

        difficulty_config
            .safe_lock(|c| {
                c.channel_nominal_hashrate = 12_345.0;
                c.request_immediate_update();
            })
            .unwrap();

        let message = timeout(Duration::from_millis(250), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        match message {
            Mining::UpdateChannel(update) => {
                assert_eq!(update.channel_id, 42);
                assert_eq!(update.nominal_hash_rate, 12_345.0);
            }
            other => panic!("expected UpdateChannel, got {other:?}"),
        }

        drop(diff_manager_abortable);
        drop(main_loop_abortable);
    }
}
