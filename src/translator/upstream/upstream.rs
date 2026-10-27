use super::super::{
    downstream::{SubmitShareResult, SubmitShareResultSender, UpstreamSubmitShare},
    error::{Error, ProxyResult},
    upstream::diff_management::UpstreamDifficultyConfig,
};
use binary_sv2::U256Owned;
use roles_logic_sv2::{
    mining::{ExtendedExtranonce, Extranonce},
    mining_sv2::{
        OpenExtendedMiningChannelOwned, OpenExtendedMiningChannelSuccessOwned,
        SetCustomMiningJobOwned,
    },
    parsers::{IsSv2Message, MiningOwned},
    utils::Mutex,
    Error as RolesLogicError,
};
use std::{collections::BTreeMap, sync::Arc};
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
    translator::{utils::submit_error_to_rejection_reason, BridgeWork},
};
use bitcoin::BlockHash;

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
    /// Bytes used as implicit first part of `extranonce`.
    extranonce_prefix: Option<Vec<u8>>,
    /// Preserves the upstream order between jobs and prevhash transitions until the `Bridge`
    /// translates them into SV1 `mining.notify` messages.
    tx_bridge_work: tokio::sync::mpsc::Sender<BridgeWork>,
    /// Sends the extranonce1 and the channel id received in the SV2 `OpenExtendedMiningChannelSuccess` message to be
    /// used by the `Downstream` and sent to the Downstream role in a SV2 `mining.subscribe`
    /// response message. Passed to the `Downstream` on connection creation.
    tx_sv2_extranonce:
        tokio::sync::mpsc::Sender<(ExtendedExtranonce, OpenExtendedMiningChannelSuccessOwned)>,
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
    pub sender: TSender<MiningOwned>,
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
        tx_bridge_work: tokio::sync::mpsc::Sender<BridgeWork>,
        min_extranonce_size: u16,
        tx_sv2_extranonce: tokio::sync::mpsc::Sender<(
            ExtendedExtranonce,
            OpenExtendedMiningChannelSuccessOwned,
        )>,
        target: Arc<Mutex<Vec<u8>>>,
        difficulty_config: Arc<Mutex<UpstreamDifficultyConfig>>,
        sender: TSender<MiningOwned>,
    ) -> ProxyResult<Arc<Mutex<Self>>> {
        Ok(Arc::new(Mutex::new(Self {
            extranonce_prefix: None,
            tx_bridge_work,
            channel_id: None,
            job_id: None,
            min_extranonce_size,
            upstream_extranonce1_size: crate::UPSTREAM_EXTRANONCE1_SIZE,
            tx_sv2_extranonce,
            target,
            difficulty_config,
            sender,
            sent_up: 0,
            rejected: 0,
            toa: Vec::new(),
            next_submit_sequence: 0,
            pending_submits: BTreeMap::new(),
        })))
    }

    pub async fn start(
        self_: Arc<Mutex<Self>>,
        incoming_receiver: TReceiver<MiningOwned>,
        rx_sv2_submit_shares_ext: TReceiver<UpstreamSubmitShare>,
        rx_update_token: TReceiver<String>,
        diff_update_rx: watch::Receiver<u64>,
    ) -> Result<AbortOnDrop, Error> {
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
    async fn connect(self_: Arc<Mutex<Self>>) -> ProxyResult<()> {
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
        let open_channel = MiningOwned::OpenExtendedMiningChannel(OpenExtendedMiningChannelOwned {
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
    ) -> ProxyResult<AbortOnDrop> {
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
                let special_message = MiningOwned::SetCustomMiningJob(SetCustomMiningJobOwned {
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
                    coinbase_tx_outputs: vec![0].try_into().expect(
                        "Internal error: this operation can not fail because the empty output count can be converted into Inner",
                    ),
                    coinbase_tx_locktime: 0,
                    merkle_path: Vec::new().try_into().expect("Empty merkle path"),
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
        mut receiver: TReceiver<MiningOwned>,
        diff_update_rx: watch::Receiver<u64>,
    ) -> ProxyResult<(AbortOnDrop, AbortOnDrop)> {
        let clone = self_.clone();
        let (tx_sv2_extranonce, tx_bridge_work) = clone
            .safe_lock(|s| (s.tx_sv2_extranonce.clone(), s.tx_bridge_work.clone()))
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
                    let next_message_to_send = self_
                        .safe_lock(|upstream| upstream.handle_mining_message(m))
                        .map_err(|error| RolesLogicError::PoisonLock(error.to_string()))
                        .and_then(|result| result);

                    // Routes the incoming messages accordingly
                    match next_message_to_send {
                        // Does not send the messages anywhere, but instead handle them internally
                        Ok(Some(m)) => {
                            match m {
                                MiningOwned::OpenExtendedMiningChannelSuccess(m) => {
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
                                    let extranonce_prefix: Extranonce = m.extranonce_prefix.clone().into();
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
                                        .send((extended, m))
                                        .await
                                        .is_err()
                                    {
                                        error!("Failed to send extended extranounce");
                                        return;
                                    };
                                }
                                MiningOwned::NewExtendedMiningJob(m) => {
                                    info!("Parsing incoming NewExtendedMiningJob message from Pool for Channel Id: {}", m.channel_id);
                                    // Claim feature metadata at the same point as the job itself.
                                    // The legacy translator coalesces future jobs, so deferring this
                                    // claim until Bridge delivery would leave bindings for dropped
                                    // futures at the head of the queue.
                                    let merge_mining_binding_id =
                                        crate::merge_mining::claim_job_binding_pending(m.job_id);
                                    if m.is_future() {
                                        if let Some((_, replaced_binding_id)) =
                                            future_j.replace((m, merge_mining_binding_id))
                                        {
                                            crate::merge_mining::clear_pending_job_binding(
                                                replaced_binding_id,
                                            );
                                        }
                                    } else {
                                        let job_id = m.job_id;
                                        if let Err(e) = self_.safe_lock(|s| {
                                            let _ = s.job_id.insert(job_id);
                                        }) {
                                            crate::merge_mining::clear_pending_job_binding(
                                                merge_mining_binding_id,
                                            );
                                            error!("Translator upstream mutex poisoned: {e}");
                                            return;
                                        };
                                        if tx_bridge_work
                                            .send(BridgeWork::NewExtendedMiningJob {
                                                job: m,
                                                merge_mining_binding_id,
                                            })
                                            .await
                                            .is_err()
                                        {
                                            crate::merge_mining::clear_pending_job_binding(
                                                merge_mining_binding_id,
                                            );
                                            error!("Failed to send NewExtendedMiningJob");
                                            return;
                                        };
                                    }
                                }
                                MiningOwned::SetNewPrevHash(m) => {
                                    info!("Parsing incoming SetNewPrevHash message from Pool for Channel Id: {}", m.channel_id);
                                    if let Some((j, merge_mining_binding_id)) = future_j.clone() {
                                        future_j = None;
                                        let job_id = m.job_id;
                                        if let Err(e) = self_.safe_lock(|s| {
                                            let _ = s.job_id.insert(job_id);
                                        }) {
                                            crate::merge_mining::clear_pending_job_binding(
                                                merge_mining_binding_id,
                                            );
                                            error!("Translator upstream mutex poisoned: {e}");
                                            return;
                                        };
                                        if tx_bridge_work
                                            .send(BridgeWork::NewExtendedMiningJob {
                                                job: j,
                                                merge_mining_binding_id,
                                            })
                                            .await
                                            .is_err()
                                        {
                                            crate::merge_mining::clear_pending_job_binding(
                                                merge_mining_binding_id,
                                            );
                                            error!("Failed to send NewExtendedMiningJob");
                                            return;
                                        };
                                        if tx_bridge_work
                                            .send(BridgeWork::SetNewPrevHash(m))
                                            .await
                                            .is_err()
                                        {
                                            crate::merge_mining::clear_pending_job_binding(
                                                merge_mining_binding_id,
                                            );
                                            error!("Failed to send SetNewPrevHash");
                                            return;
                                        };
                                    } else if tx_bridge_work
                                        .send(BridgeWork::SetNewPrevHash(m))
                                        .await
                                        .is_err()
                                    {
                                        error!("Failed to send SetNewPrevHash");
                                        return;
                                    }
                                }
                                MiningOwned::SetGroupChannel(message) => {
                                    if tx_bridge_work.send(BridgeWork::SetGroupChannel(message)).await.is_err() {
                                        return;
                                    }
                                }
                                MiningOwned::CloseChannel(_) => {
                                    ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                                    return;
                                }
                                MiningOwned::OpenMiningChannelError(_)
                                | MiningOwned::UpdateChannelError(_)
                                //| MiningOwned::SubmitSharesError(_)
                                | MiningOwned::SetCustomMiningJobError(_) => {
                                    ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                                    return;
                                }
                                _ => {
                                    ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                                    return;
                                }
                            };
                        }
                        Ok(None) => (),
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
    ) -> ProxyResult<AbortOnDrop> {
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

                    let channel_id = match self_
                        .safe_lock(|s| s.channel_id.ok_or(RolesLogicError::NotFoundChannelId))
                    {
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

                    let message =
                        roles_logic_sv2::parsers::MiningOwned::SubmitSharesExtended(share);

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
}

impl Upstream {
    fn handle_mining_message(
        &mut self,
        message: MiningOwned,
    ) -> Result<Option<MiningOwned>, RolesLogicError> {
        match message {
            MiningOwned::OpenExtendedMiningChannelSuccess(message) => {
                if usize::from(message.extranonce_size) < usize::from(self.min_extranonce_size) {
                    return Err(RolesLogicError::InvalidExtranonceSize(
                        self.min_extranonce_size,
                        message.extranonce_size,
                    ));
                }
                self.target
                    .safe_lock(|target| *target = message.target.as_bytes().to_vec())
                    .map_err(|error| RolesLogicError::PoisonLock(error.to_string()))?;
                self.channel_id = Some(message.channel_id);
                self.extranonce_prefix = Some(message.extranonce_prefix.as_bytes().to_vec());
                Ok(Some(MiningOwned::OpenExtendedMiningChannelSuccess(message)))
            }
            MiningOwned::SubmitSharesSuccess(message) => {
                self.resolve_submit_success(
                    message.last_sequence_number,
                    message.new_submits_accepted_count,
                );
                Ok(None)
            }
            MiningOwned::SubmitSharesError(message) => {
                self.rejected += 1;
                let error_code = std::str::from_utf8(message.error_code.as_bytes())
                    .unwrap_or("unparsable error code");
                self.resolve_submit_error(
                    message.sequence_number,
                    Err(submit_error_to_rejection_reason(error_code)),
                );
                warn!("Pool rejected share with error code {}", error_code);
                Ok(None)
            }
            MiningOwned::SetTarget(message) => {
                self.target
                    .safe_lock(|target| *target = message.maximum_target.as_bytes().to_vec())
                    .map_err(|error| RolesLogicError::PoisonLock(error.to_string()))?;
                Ok(None)
            }
            MiningOwned::SetNewPrevHash(message) => {
                self.toa.push(std::time::Instant::now());
                Ok(Some(MiningOwned::SetNewPrevHash(message)))
            }
            MiningOwned::NewExtendedMiningJob(_)
            | MiningOwned::CloseChannel(_)
            | MiningOwned::OpenMiningChannelError(_)
            | MiningOwned::UpdateChannelError(_)
            | MiningOwned::SetCustomMiningJobError(_)
            | MiningOwned::SetGroupChannel(_) => Ok(Some(message)),
            _ => Err(RolesLogicError::UnexpectedMessage(message.message_type())),
        }
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
fn u256_max() -> U256Owned {
    [255; 32].into()
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

    fn bridge_job(
        job_id: u32,
        future: bool,
    ) -> roles_logic_sv2::mining_sv2::NewExtendedMiningJobOwned {
        roles_logic_sv2::mining_sv2::NewExtendedMiningJobOwned {
            channel_id: 1,
            job_id,
            min_ntime: binary_sv2::Sv2OptionOwned::new(if future {
                None
            } else {
                Some(1_700_000_000)
            }),
            version: 0,
            version_rolling_allowed: false,
            merkle_path: vec![].try_into().expect("Empty merkle path"),
            coinbase_tx_prefix: Vec::new().try_into().expect("empty prefix"),
            coinbase_tx_suffix: Vec::new().try_into().expect("empty suffix"),
        }
    }

    fn bridge_prev_hash(job_id: u32) -> roles_logic_sv2::mining_sv2::SetNewPrevHashOwned {
        roles_logic_sv2::mining_sv2::SetNewPrevHashOwned {
            channel_id: 1,
            job_id,
            prev_hash: [3; 32].into(),
            min_ntime: 1_700_000_000,
            nbits: 0x1d00ffff,
        }
    }

    async fn test_upstream() -> Arc<Mutex<Upstream>> {
        let (tx_bridge_work, _rx_bridge_work) = mpsc::channel(1);
        let (tx_sv2_extranonce, _rx_sv2_extranonce) = mpsc::channel(1);
        let (sender, _receiver) = mpsc::channel(1);

        Upstream::new(
            tx_bridge_work,
            crate::MIN_EXTRANONCE_SIZE - 1,
            tx_sv2_extranonce,
            Arc::new(Mutex::new(vec![0; 32])),
            Arc::new(Mutex::new(
                UpstreamDifficultyConfig::new(crate::CHANNEL_DIFF_UPDTATE_INTERVAL, 0.0).0,
            )),
            sender,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn update_channel_preserves_maximum_target_bytes() {
        let upstream = test_upstream().await;
        let (sender, mut receiver) = mpsc::channel(1);
        upstream
            .safe_lock(|upstream| {
                upstream.channel_id = Some(1);
                upstream.sender = sender;
            })
            .unwrap();
        Upstream::try_update_hashrate(upstream).await.unwrap();
        let MiningOwned::UpdateChannel(update) = receiver.recv().await.unwrap() else {
            panic!("Expected UpdateChannel");
        };
        let mut expected = [0; 32];
        expected[24..].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(update.maximum_target.as_bytes(), expected);
    }

    #[tokio::test]
    async fn native_channel_assignment_retains_group_and_rejects_short_extranonce() {
        let upstream = test_upstream().await;
        let assignment = OpenExtendedMiningChannelSuccessOwned {
            request_id: 0,
            channel_id: 1,
            group_channel_id: 42,
            target: [255; 32].into(),
            extranonce_size: 4,
            extranonce_prefix: vec![0; 6].try_into().unwrap(),
        };
        upstream.safe_lock(|upstream| {
            let result = upstream.handle_mining_message(MiningOwned::OpenExtendedMiningChannelSuccess(assignment.clone()));
            assert!(matches!(result, Err(RolesLogicError::InvalidExtranonceSize(5, 4))));
            assert!(upstream.channel_id.is_none());
            let mut assignment = assignment;
            assignment.extranonce_size = 10;
            let result = upstream.handle_mining_message(MiningOwned::OpenExtendedMiningChannelSuccess(assignment)).unwrap();
            assert!(matches!(result, Some(MiningOwned::OpenExtendedMiningChannelSuccess(message)) if message.group_channel_id == 42 && message.extranonce_size == 10));
        }).unwrap();
    }

    #[tokio::test]
    async fn bridge_work_preserves_cross_type_source_order() {
        let (tx_bridge_work, mut rx_bridge_work) = mpsc::channel(4);
        let (tx_sv2_extranonce, _rx_sv2_extranonce) = mpsc::channel(1);
        let (sender, _receiver) = mpsc::channel(1);
        let (difficulty_config, diff_update_rx) =
            UpstreamDifficultyConfig::new(crate::CHANNEL_DIFF_UPDTATE_INTERVAL, 0.0);
        let upstream = Upstream::new(
            tx_bridge_work,
            crate::MIN_EXTRANONCE_SIZE - 1,
            tx_sv2_extranonce,
            Arc::new(Mutex::new(vec![0; 32])),
            Arc::new(Mutex::new(difficulty_config)),
            sender,
        )
        .await
        .expect("upstream");
        let (incoming_tx, incoming_rx) = mpsc::channel(4);
        let (diff_manager, main_loop) =
            Upstream::parse_incoming(upstream, incoming_rx, diff_update_rx).expect("parser");

        incoming_tx
            .send(MiningOwned::SetNewPrevHash(bridge_prev_hash(10)))
            .await
            .expect("prevhash");
        incoming_tx
            .send(MiningOwned::NewExtendedMiningJob(bridge_job(10, false)))
            .await
            .expect("nonfuture job");

        assert!(matches!(
            timeout(Duration::from_secs(1), rx_bridge_work.recv())
                .await
                .expect("work timeout"),
            Some(BridgeWork::SetNewPrevHash(message)) if message.job_id == 10
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), rx_bridge_work.recv())
                .await
                .expect("work timeout"),
            Some(BridgeWork::NewExtendedMiningJob { job, .. }) if job.job_id == 10
        ));

        incoming_tx
            .send(MiningOwned::NewExtendedMiningJob(bridge_job(11, true)))
            .await
            .expect("future job");
        incoming_tx
            .send(MiningOwned::SetNewPrevHash(bridge_prev_hash(11)))
            .await
            .expect("prevhash");

        assert!(matches!(
            timeout(Duration::from_secs(1), rx_bridge_work.recv())
                .await
                .expect("work timeout"),
            Some(BridgeWork::NewExtendedMiningJob { job, .. }) if job.job_id == 11
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), rx_bridge_work.recv())
                .await
                .expect("work timeout"),
            Some(BridgeWork::SetNewPrevHash(message)) if message.job_id == 11
        ));

        drop(diff_manager);
        drop(main_loop);
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
        let (tx_bridge_work, _rx_bridge_work) = mpsc::channel(1);
        let (tx_sv2_extranonce, _rx_sv2_extranonce) = mpsc::channel(1);
        let (sender, mut receiver) = mpsc::channel(4);
        let (difficulty_config, diff_update_rx) =
            UpstreamDifficultyConfig::new(crate::CHANNEL_DIFF_UPDTATE_INTERVAL, 0.0);
        let difficulty_config = Arc::new(Mutex::new(difficulty_config));
        let upstream = Upstream::new(
            tx_bridge_work,
            crate::MIN_EXTRANONCE_SIZE - 1,
            tx_sv2_extranonce,
            Arc::new(Mutex::new(vec![0; 32])),
            difficulty_config.clone(),
            sender,
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
            MiningOwned::UpdateChannel(update) => {
                assert_eq!(update.channel_id, 42);
                assert_eq!(update.nominal_hash_rate, 12_345.0);
            }
            other => panic!("expected UpdateChannel, got {other:?}"),
        }

        drop(diff_manager_abortable);
        drop(main_loop_abortable);
    }
}
