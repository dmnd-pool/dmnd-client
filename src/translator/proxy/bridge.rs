use bitcoin::hex::DisplayHex;
use tokio::task::JoinHandle;

use roles_logic_sv2::{
    channel_logic::channel_factory::Share,
    mining::{ExtendedExtranonce, Target},
    mining_sv2::{
        NewExtendedMiningJobOwned, OpenExtendedMiningChannelSuccessOwned, SetNewPrevHashOwned,
        SubmitSharesExtendedOwned,
    },
    utils::Mutex,
};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use sv1_api::{client_to_server::Submit, utils::HexU32Be};
use tokio::sync::broadcast;

use super::{
    super::{
        downstream::{
            DownstreamMessages, SetDownstreamTarget, SubmitShareResult, SubmitShareWithChannelId,
            UpstreamSubmitShare,
        },
        error::{Error, ProxyResult},
        MiningNotify,
    },
    channel_factory::ChannelFactory,
    task_manager::TaskManager,
};
use crate::{
    proxy_state::{ProxyState, TranslatorState, UpstreamType},
    share_log_enabled,
    shared::utils::AbortOnDrop,
    translator::{
        utils::{allow_submit_share, submit_error_to_rejection_reason},
        BridgeWork,
    },
};
use lazy_static::lazy_static;
use roles_logic_sv2::{channel_logic::channel_factory::OnNewShare, Error as RolesLogicError};
use tracing::{debug, error, info, warn};

lazy_static! {
    static ref SUBMIT_FAIL_COUNTER: AtomicU32 = AtomicU32::new(0);
}

#[derive(Debug, Clone)]
struct BoundExtendedJob {
    job: NewExtendedMiningJobOwned,
    merge_mining_binding_id: Option<u64>,
}

/// Bridge between the SV2 `Upstream` and SV1 `Downstream` responsible for the following messaging
/// translation:
/// 1. SV1 `mining.submit` -> SV2 `SubmitSharesExtended`
/// 2. SV2 `SetNewPrevHash` + `NewExtendedMiningJob` -> SV1 `mining.notify`
#[derive(Debug)]
pub struct Bridge {
    /// Sends SV2 `SubmitSharesExtended` messages translated from SV1 `mining.submit` messages to
    /// the `Upstream`.
    tx_sv2_submit_shares_ext: tokio::sync::mpsc::Sender<UpstreamSubmitShare>,
    /// Sends SV1 `mining.notify` message (translated from the SV2 `SetNewPrevHash` and
    /// `NewExtendedMiningJob` messages stored in the `NextMiningNotify`) to the `Downstream`.
    tx_sv1_notify: broadcast::Sender<MiningNotify>,
    /// Stores the most recent SV1 `mining.notify` values to be sent to the `Downstream` upon
    /// receiving a new SV2 `SetNewPrevHash` and `NewExtendedMiningJob` messages **before** any
    /// Downstream role connects to the proxy.
    ///
    /// Once the proxy establishes a connection with the SV2 Upstream role, it immediately receives
    /// a SV2 `SetNewPrevHash` and `NewExtendedMiningJob` message. This happens before the
    /// connection to the Downstream role(s) occur. The `last_notify` member fields allows these
    /// first notify values to be relayed to the `Downstream` once a Downstream role connects. Once
    /// a Downstream role connects and receives the first notify values, this member field is no
    /// longer used.
    last_notify: Option<MiningNotify>,
    pub(self) channel_factory: ChannelFactory,
    future_jobs: Vec<BoundExtendedJob>,
    last_p_hash: Option<SetNewPrevHashOwned>,
    target: Arc<Mutex<Vec<u8>>>,
}

impl Bridge {
    pub async fn ready(self_: &'_ Arc<Mutex<Self>>) -> Result<(), Error> {
        let started_at = std::time::Instant::now();
        let mut last_log_at = started_at;
        while self_
            .safe_lock(|b| b.last_notify.is_none())
            .map_err(|_| Error::BridgeMutexPoisoned)?
        {
            if last_log_at.elapsed() >= std::time::Duration::from_secs(5) {
                info!(
                    "dmnd-client-debug bridge_waiting_for_first_notify elapsed_ms={}",
                    started_at.elapsed().as_millis()
                );
                last_log_at = std::time::Instant::now();
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        info!(
            "dmnd-client-debug bridge_ready elapsed_ms={}",
            started_at.elapsed().as_millis()
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    /// Instantiate a new `Bridge`.
    pub fn new(
        tx_sv2_submit_shares_ext: tokio::sync::mpsc::Sender<UpstreamSubmitShare>,
        tx_sv1_notify: broadcast::Sender<MiningNotify>,
        extranonces: ExtendedExtranonce,
        target: Arc<Mutex<Vec<u8>>>,
        upstream_channel: OpenExtendedMiningChannelSuccessOwned,
    ) -> Result<Arc<Mutex<Self>>, Error> {
        info!(
            "Creating new bridge for channel_id {}:",
            upstream_channel.channel_id
        );
        let upstream_target: [u8; 32] =  target.safe_lock(|t| {
    t.clone().try_into().expect("Internal error: this operation can not fail because Vec<U8> can always be converted into [u8; 32]")
}).map_err(|e| Error::TargetError(RolesLogicError::PoisonLock(e.to_string())))?;
        let upstream_target: Target = upstream_target.into();
        Ok(Arc::new(Mutex::new(Self {
            tx_sv2_submit_shares_ext,
            tx_sv1_notify,
            last_notify: None,
            channel_factory: ChannelFactory::new(
                extranonces,
                *crate::SHARE_PER_MIN,
                upstream_target,
                upstream_channel,
            )?,
            future_jobs: vec![],
            last_p_hash: None,
            target,
        })))
    }

    #[allow(clippy::result_large_err)]
    pub fn on_new_sv1_connection(&mut self, hash_rate: f32) -> ProxyResult<OpenSv1Downstream> {
        info!(
            "dmnd-client-debug open_sv1_downstream_start hash_rate={} last_notify_present={}",
            hash_rate,
            self.last_notify.is_some()
        );
        let success = self.channel_factory.open_channel(0, hash_rate)?;
        info!(
            "dmnd-client-debug open_sv1_downstream_success channel_id={} extranonce_len={} extranonce2_len={} last_notify_present={}",
            success.channel_id,
            success.extranonce_prefix.len(),
            success.extranonce_size,
            self.last_notify.is_some()
        );
        Ok(OpenSv1Downstream {
            channel_id: success.channel_id,
            last_notify: self.last_notify.clone(),
            extranonce: success.extranonce_prefix.into_bytes(),
            extranonce2_len: success.extranonce_size,
        })
    }

    /// Starts the tasks that receive SV1 and SV2 messages to be translated and sent to their
    /// respective roles.
    pub async fn start(
        self_: Arc<Mutex<Self>>,
        rx_bridge_work: tokio::sync::mpsc::Receiver<BridgeWork>,
        rx_sv1_downstream: tokio::sync::mpsc::Receiver<DownstreamMessages>,
    ) -> Result<AbortOnDrop, Error> {
        let task_manager = TaskManager::initialize();
        let abortable = task_manager
            .safe_lock(|t| t.get_aborter())
            .map_err(|_| Error::BridgeTaskManagerMutexPoisoned)?
            .ok_or(Error::BridgeTaskManagerFailed)?;

        let upstream_work_handler = Self::handle_upstream_work(self_.clone(), rx_bridge_work)?;
        let downs_message_handler = Self::handle_downstream_messages(self_, rx_sv1_downstream);
        TaskManager::add_handle_upstream_work(task_manager.clone(), upstream_work_handler.into())
            .await
            .map_err(|_| Error::BridgeTaskManagerFailed)?;
        TaskManager::add_handle_downstream_messages(
            task_manager.clone(),
            downs_message_handler.into(),
        )
        .await
        .map_err(|_| Error::BridgeTaskManagerFailed)?;
        Ok(abortable)
    }

    /// Receives a `DownstreamMessages` message from the `Downstream`, handles based on the
    /// variant received.
    fn handle_downstream_messages(
        self_: Arc<Mutex<Self>>,
        mut rx_sv1_downstream: tokio::sync::mpsc::Receiver<DownstreamMessages>,
    ) -> JoinHandle<()> {
        tokio::task::spawn(async move {
            loop {
                let msg = match rx_sv1_downstream.recv().await {
                    Some(msg) => msg,
                    None => {
                        error!("Failed to receive message from downstream");
                        ProxyState::update_translator_state(TranslatorState::Down);
                        break;
                    }
                };

                match msg {
                    DownstreamMessages::CloseChannel(channel_id) => {
                        if self_
                            .safe_lock(|bridge| bridge.channel_factory.close_channel(channel_id))
                            .is_err()
                        {
                            ProxyState::update_translator_state(TranslatorState::Down);
                            break;
                        }
                    }
                    DownstreamMessages::SubmitShares(share) => {
                        if let Err(e) = Self::handle_submit_shares(self_.clone(), share).await {
                            error!("Failed to handle SubmitShareWithChannelId: {e}");
                            ProxyState::update_translator_state(TranslatorState::Down);
                            break;
                        }
                    }
                    DownstreamMessages::SetDownstreamTarget(new_target) => {
                        if let Err(e) =
                            Self::handle_update_downstream_target(self_.clone(), new_target)
                        {
                            error!("Failed to handle SetDownstreamTarget: {e}");
                            ProxyState::update_translator_state(TranslatorState::Down);
                            break;
                        };
                    }
                };
            }
        })
    }
    /// receives a `SetDownstreamTarget` and updates the downstream target for the channel
    #[allow(clippy::result_large_err)]
    fn handle_update_downstream_target(
        self_: Arc<Mutex<Self>>,
        new_target: SetDownstreamTarget,
    ) -> ProxyResult<()> {
        self_
            .safe_lock(|b| {
                b.channel_factory
                    .update_target_for_channel(new_target.channel_id, new_target.new_target);
            })
            .map_err(|_| Error::BridgeMutexPoisoned)?;
        Ok(())
    }

    fn resolve_submit(
        result_tx: tokio::sync::oneshot::Sender<SubmitShareResult>,
        result: SubmitShareResult,
    ) {
        let _ = result_tx.send(result);
    }

    /// receives a `SubmitShareWithChannelId` and validates the shares and sends to `Upstream` if
    /// the share meets the upstream target
    async fn handle_submit_shares(
        self_: Arc<Mutex<Self>>,
        share: SubmitShareWithChannelId,
    ) -> ProxyResult<()> {
        let SubmitShareWithChannelId {
            channel_id,
            share,
            version_rolling_mask,
            result_tx,
            ..
        } = share;
        let job_id = share.job_id.clone();
        let share_id = share.id;
        if share_log_enabled() {
            info!(
                "Bridge received share {:?} for channel {:?} and job {:?}",
                &share_id, &channel_id, &job_id
            );
        }
        let (tx_sv2_submit_shares_ext, target_mutex) = self_
            .safe_lock(|s| (s.tx_sv2_submit_shares_ext.clone(), s.target.clone()))
            .map_err(|_| Error::BridgeMutexPoisoned)?;
        let upstream_target: [u8; 32] =  target_mutex
            .safe_lock(|t| t.clone())
            .map_err(|_| Error::BridgeMutexPoisoned)?
            .try_into()
            .expect("Internal error: this operation can not fail because the Vec<U8> can always be converted into Inner");

        let mut dbg_target = upstream_target.clone().to_vec();
        dbg_target.reverse();
        debug!("Pool target: {:?}", dbg_target.as_hex());
        let mut upstream_target: Target = upstream_target.into();
        let translated_share = match self_
            .safe_lock(|s| {
                let job_id = share.job_id.parse::<u32>().expect("Invalid job_id");
                if s.channel_factory.job(job_id).is_none() {
                    warn!(
                        "Share rejected: job_id {} not in retained job cache",
                        job_id
                    );
                    return Err(roles_logic_sv2::Error::ShareDoNotMatchAnyJob); // rejected
                }
                s.channel_factory.set_target(&mut upstream_target);
                s.translate_submit(channel_id, share.clone(), version_rolling_mask.clone())
                    .map_err(|_| roles_logic_sv2::Error::NoValidJob)
            })
            .map_err(|_| Error::BridgeMutexPoisoned)?
        {
            Ok(share) => share,
            Err(roles_logic_sv2::Error::NoValidJob) => {
                let count = SUBMIT_FAIL_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= 10 {
                    error!("Failed to translate SV1 mining.submit message to SV2 SubmitSharesExtended message after 10 attempts");
                    Self::resolve_submit(
                        result_tx,
                        Err(crate::monitor::shares::RejectionReason::JobIdNotFound),
                    );
                    return Err(Error::RolesSv2Logic(roles_logic_sv2::Error::NoValidJob));
                }
                warn!(
                    "Failed to translate SV1 mining.submit message to SV2 SubmitSharesExtended message, attempt {}",
                    count
                );
                Self::resolve_submit(
                    result_tx,
                    Err(crate::monitor::shares::RejectionReason::JobIdNotFound),
                );
                return Ok(());
            }
            Err(roles_logic_sv2::Error::ShareDoNotMatchAnyJob) => {
                warn!(
                    "Channel factory can not get this share's job_id: {}",
                    job_id
                );
                Self::resolve_submit(
                    result_tx,
                    Err(crate::monitor::shares::RejectionReason::JobIdNotFound),
                );
                return Ok(());
            }
            Err(e) => {
                Self::resolve_submit(
                    result_tx,
                    Err(crate::monitor::shares::RejectionReason::UpstreamRejected),
                );
                return Err(Error::RolesSv2Logic(e));
            }
        };

        let res = self_
            .safe_lock(|s| {
                // Ordering::Relaxed is safe here because we only need simple counter updates.
                // No need for strict ordering since it just tracks failures.
                SUBMIT_FAIL_COUNTER.store(0, Ordering::Relaxed);
                s.channel_factory
                    .on_submit_shares_extended(translated_share.clone())
            })
            .map_err(|_| Error::BridgeMutexPoisoned)?;

        match res {
            Ok(OnNewShare::SendErrorDownstream(e)) => {
                let error_code = std::str::from_utf8(&e.error_code.as_bytes().to_vec()[..])
                    .unwrap_or("unparsable error code")
                    .to_string();
                error!(
                    "Submit share {} from channel {} and job {} error {}",
                    &share_id, &channel_id, &job_id, error_code
                );
                Self::resolve_submit(
                    result_tx,
                    Err(submit_error_to_rejection_reason(&error_code)),
                );
                Ok(())
            }
            Ok(OnNewShare::SendSubmitShareUpstream((s, _))) => {
                if share_log_enabled() {
                    info!(
                        "Share with id {} meets upstream target from channel {} and job {}",
                        &share_id, &channel_id, &job_id
                    );
                }
                let upstream_share = match s {
                    Share::Extended(share) => share,
                    // We are in an extended channel shares are extended
                    Share::Standard(_) => unreachable!(),
                };

                if let Ok(allowed_by_rate_limit) = allow_submit_share() {
                    if !allowed_by_rate_limit {
                        warn!("Share will not be sent upstream: Exceeded 70 shares/min limit");
                        Self::resolve_submit(
                            result_tx,
                            Err(crate::monitor::shares::RejectionReason::RateLimited),
                        );
                        return Ok(());
                    }
                } else {
                    error!("Failed to reserve upstream share rate-limit slot");
                    ProxyState::update_inconsistency(Some(1));
                    Self::resolve_submit(
                        result_tx,
                        Err(crate::monitor::shares::RejectionReason::UpstreamRejected),
                    );
                    return Err(Error::BridgeMutexPoisoned);
                }

                if let Err(e) = tx_sv2_submit_shares_ext
                    .send(UpstreamSubmitShare {
                        share: upstream_share,
                        result_tx,
                    })
                    .await
                {
                    error!("Failed to send SubmitShareExtended upstream");
                    Self::resolve_submit(
                        e.0.result_tx,
                        Err(crate::monitor::shares::RejectionReason::UpstreamRejected),
                    );
                    return Err(Error::AsyncChannelError);
                }
                Ok(())
            }
            // We are in an extended channel this variant is group channle only
            Ok(OnNewShare::RelaySubmitShareUpstream) => unreachable!(),
            Ok(OnNewShare::ShareMeetDownstreamTarget) => {
                if share_log_enabled() {
                    info!(
                        "Share with id {} meets downstream target from channel {} and job {}; resolving locally",
                        &share_id, &channel_id, &job_id
                    );
                }
                Self::resolve_submit(result_tx, Ok(()));
                Ok(())
            }
            // Proxy do not have JD capabilities
            Ok(OnNewShare::ShareMeetBitcoinTarget(..)) => unreachable!(),
            Err(RolesLogicError::ShareDoNotMatchAnyChannel) => {
                debug!("Discarding share from closed channel {channel_id}");
                Self::resolve_submit(
                    result_tx,
                    Err(crate::monitor::shares::RejectionReason::UpstreamRejected),
                );
                Ok(())
            }
            Err(e) => {
                Self::resolve_submit(
                    result_tx,
                    Err(crate::monitor::shares::RejectionReason::UpstreamRejected),
                );
                Err(Error::RolesSv2Logic(e))
            }
        }
    }

    /// Translates a SV1 `mining.submit` message to a SV2 `SubmitSharesExtended` message.
    #[allow(clippy::result_large_err)]
    fn translate_submit(
        &self,
        channel_id: u32,
        sv1_submit: Submit,
        version_rolling_mask: Option<HexU32Be>,
    ) -> ProxyResult<SubmitSharesExtendedOwned> {
        if share_log_enabled() {
            info!(
                "Bridge translating mining.submit {} from downstream with channel {} and job {}",
                sv1_submit.id, channel_id, sv1_submit.job_id
            );
        }
        let last_version = self
            .channel_factory
            .last_valid_job_version()
            .ok_or(Error::Unrecoverable)?;
        debug!("Last valid job version: {}", last_version);
        let version = match (&sv1_submit.version_bits, &version_rolling_mask) {
            // regarding version masking see https://github.com/slushpool/stratumprotocol/blob/master/stratum-extensions.mediawiki#changes-in-request-miningsubmit
            (Some(vb), Some(mask)) => {
                debug!("Version bits and mask provided: {:?}, {:?}", vb, mask);
                (last_version & !mask.0) | (vb.0 & mask.0)
            }
            (None, None) => {
                debug!(
                    "No version bits or mask provided, using last valid job version: {}",
                    last_version
                );
                last_version
            }
            _ => {
                error!(
                    "Invalid version bits {:?} or mask {:?} provided",
                    &sv1_submit.version_bits, &version_rolling_mask
                );
                return Err(Error::V1Protocol(Box::new(
                    sv1_api::error::Error::UnexpectedMessage("Invalid submission".to_string()),
                )));
            }
        };
        let mining_device_extranonce: Vec<u8> = sv1_submit.extra_nonce2.into();
        debug!(
            "MiningOwned device extranonce: {}",
            mining_device_extranonce.to_vec().as_hex()
        );
        let extranonce2 = mining_device_extranonce;
        debug!("Extranonce2: {}", extranonce2.to_vec().as_hex());
        Ok(SubmitSharesExtendedOwned {
            channel_id,
            // I put 0 below cause sequence_number is not what should be TODO
            sequence_number: 0,
            job_id: sv1_submit.job_id.parse::<u32>().expect("Internal error: this operation can not fail because job_id can always be converted into U32"),
            nonce: sv1_submit.nonce.0,
            ntime: sv1_submit.time.0,
            version,
            extranonce: extranonce2.try_into().expect("Internal error: this operation can not fail because Vec<U8> can always be converted into Inner"),
        })
    }

    async fn handle_new_prev_hash_(
        self_: Arc<Mutex<Self>>,
        sv2_set_new_prev_hash: SetNewPrevHashOwned,
        tx_sv1_notify: broadcast::Sender<MiningNotify>,
    ) -> Result<(), Error> {
        self_
            .safe_lock(|s| s.last_p_hash = Some(sv2_set_new_prev_hash.clone()))
            .map_err(|_| Error::BridgeMutexPoisoned)?;

        self_
            .safe_lock(|s| {
                s.channel_factory
                    .on_new_prev_hash(sv2_set_new_prev_hash.clone())
            })
            .map_err(|_| Error::BridgeMutexPoisoned)??;

        let mut future_jobs = self_
            .safe_lock(|s| {
                let future_jobs = s.future_jobs.clone();
                s.future_jobs = vec![];
                future_jobs
            })
            .map_err(|_| Error::BridgeMutexPoisoned)?;

        let extranonce_len = self_
            .safe_lock(|s| s.channel_factory.get_extranonce_len())
            .map_err(|_| Error::BridgeMutexPoisoned)?;

        let mut match_a_future_job = false;
        while let Some(job) = future_jobs.pop() {
            if job.job.job_id == sv2_set_new_prev_hash.job_id {
                // Create the mining.notify to be sent to the Downstream.
                let notify = super::super::proxy::next_mining_notify::create_notify(
                    sv2_set_new_prev_hash.clone(),
                    job.job,
                    true,
                    extranonce_len,
                );

                // Get the sender to send the mining.notify to the Downstream
                let notify = MiningNotify {
                    notify,
                    merge_mining_binding_id: job.merge_mining_binding_id,
                };
                crate::merge_mining::set_active_job_binding(notify.merge_mining_binding_id);
                if tx_sv1_notify.send(notify.clone()).is_err() {
                    debug!("mining.notify has no current listeners");
                };
                match_a_future_job = true;
                self_
                    .safe_lock(|s| {
                        s.last_notify = Some(notify);
                    })
                    .map_err(|_| Error::BridgeMutexPoisoned)?;
                for discarded in future_jobs.drain(..) {
                    crate::merge_mining::clear_pending_job_binding(
                        discarded.merge_mining_binding_id,
                    );
                }
                break;
            } else {
                crate::merge_mining::clear_pending_job_binding(job.merge_mining_binding_id);
            }
        }
        if !match_a_future_job {
            debug!("No future jobs for {:?}", sv2_set_new_prev_hash);
        }
        Ok(())
    }

    async fn handle_new_extended_mining_job_(
        self_: Arc<Mutex<Self>>,
        sv2_new_extended_mining_job: NewExtendedMiningJobOwned,
        tx_sv1_notify: broadcast::Sender<MiningNotify>,
        merge_mining_binding_id: Option<u64>,
    ) -> Result<(), Error> {
        // convert to non segwit jobs so we dont have to depend if miner's support segwit or not
        self_
            .safe_lock(|s| {
                s.channel_factory
                    .on_new_extended_mining_job(sv2_new_extended_mining_job.clone())
            })
            .map_err(|_| Error::BridgeMutexPoisoned)?
            .map_err(|_| {
                Error::RolesSv2Logic(RolesLogicError::JobIsNotFutureButPrevHashNotPresent)
            })?;

        let extranonce_len = self_
            .safe_lock(|s| s.channel_factory.get_extranonce_len())
            .map_err(|_| Error::BridgeMutexPoisoned)?;

        // If future_job=true, this job is meant for a future SetNewPrevHashOwned that the proxy
        // has yet to receive. Insert this new job into the job_mapper .
        if sv2_new_extended_mining_job.is_future() {
            self_
                .safe_lock(|s| {
                    s.future_jobs.push(BoundExtendedJob {
                        job: sv2_new_extended_mining_job.clone(),
                        merge_mining_binding_id,
                    })
                })
                .map_err(|_| Error::BridgeMutexPoisoned)?;
            Ok(())

        // If future_job=false, this job is meant for the current SetNewPrevHashOwned.
        } else {
            let last_p_hash_option = self_
                .safe_lock(|s| s.last_p_hash.clone())
                .map_err(|_| Error::BridgeMutexPoisoned)?;

            // last_p_hash is an Option<SetNewPrevHashOwned> so we need to map to the correct error type to be handled
            let last_p_hash = last_p_hash_option.ok_or(Error::RolesSv2Logic(
                RolesLogicError::JobIsNotFutureButPrevHashNotPresent,
            ))?;

            // Create the mining.notify to be sent to the Downstream.
            // We always set to true cause we do not cache old jobs and we can not verify shares
            // for them
            let notify = super::super::proxy::next_mining_notify::create_notify(
                last_p_hash,
                sv2_new_extended_mining_job.clone(),
                true,
                extranonce_len,
            );
            // Get the sender to send the mining.notify to the Downstream
            let notify = MiningNotify {
                notify,
                merge_mining_binding_id,
            };
            crate::merge_mining::set_active_job_binding(notify.merge_mining_binding_id);
            if tx_sv1_notify.send(notify.clone()).is_err() {
                debug!(
                    job_id = sv2_new_extended_mining_job.job_id,
                    "mining.notify has no current listeners"
                );
            }

            self_
                .safe_lock(|s| {
                    s.last_notify = Some(notify);
                })
                .map_err(|_| Error::BridgeMutexPoisoned)?;
            Ok(())
        }
    }

    /// Processes jobs and prevhash transitions from one FIFO so their upstream ordering cannot
    /// be inverted by independently scheduled bridge tasks.
    fn handle_upstream_work(
        self_: Arc<Mutex<Self>>,
        mut receiver: tokio::sync::mpsc::Receiver<BridgeWork>,
    ) -> Result<JoinHandle<()>, Error> {
        let tx_sv1_notify = self_
            .safe_lock(|s| s.tx_sv1_notify.clone())
            .map_err(|_| Error::BridgeMutexPoisoned)?;
        debug!("Starting ordered upstream work handler");
        Ok(tokio::task::spawn(async move {
            while let Some(work) = receiver.recv().await {
                match work {
                    BridgeWork::SetGroupChannel(message) => {
                        if self_
                            .safe_lock(|bridge| {
                                let channel_ids = message.channel_ids.into_inner();
                                let message = roles_logic_sv2::mining_sv2::SetGroupChannel {
                                    group_channel_id: message.group_channel_id,
                                    channel_ids: channel_ids
                                        .try_into()
                                        .expect("Channel count was checked by the decoder"),
                                };
                                bridge.channel_factory.set_group_channel(&message);
                            })
                            .is_err()
                        {
                            ProxyState::update_translator_state(TranslatorState::Down);
                            return;
                        }
                    }
                    BridgeWork::SetNewPrevHash(set_new_prev_hash) => {
                        let mut dbg_prev_hash = set_new_prev_hash.prev_hash.as_bytes().to_vec();
                        dbg_prev_hash.reverse();
                        debug!(
                            "Received NewPrevHash {} for channel {} with job {}",
                            dbg_prev_hash.as_hex(),
                            set_new_prev_hash.channel_id,
                            set_new_prev_hash.job_id
                        );
                        if let Err(e) = Self::handle_new_prev_hash_(
                            self_.clone(),
                            set_new_prev_hash,
                            tx_sv1_notify.clone(),
                        )
                        .await
                        {
                            error!("Failed to handle SetNewPrevHash: {e}");
                            ProxyState::update_upstream_state(UpstreamType::TranslatorUpstream);
                            return;
                        }
                    }
                    BridgeWork::NewExtendedMiningJob {
                        job: new_job,
                        merge_mining_binding_id,
                    } => {
                        if let Err(e) = Self::handle_new_extended_mining_job_(
                            self_.clone(),
                            new_job,
                            tx_sv1_notify.clone(),
                            merge_mining_binding_id,
                        )
                        .await
                        {
                            error!("Failed to handle NewExtendedMiningJob {e}",);
                            ProxyState::update_translator_state(TranslatorState::Down);
                            return;
                        };
                    }
                }
            }
            error!("Failed to receive ordered upstream work");
            ProxyState::update_translator_state(TranslatorState::Down);
        }))
    }
}
#[derive(Debug)]
pub struct OpenSv1Downstream {
    pub channel_id: u32,
    pub last_notify: Option<MiningNotify>,
    pub extranonce: Vec<u8>,
    pub extranonce2_len: u16,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::translator::downstream::Downstream;
    use bitcoin::{blockdata::witness::Witness, hashes::Hash};
    use tokio::sync::mpsc;

    const TEST_JOB_ID: u32 = 0;
    const TEST_NTIME: u32 = 1_700_000_000;

    pub mod test_utils {
        use super::*;

        pub fn create_bridge(extranonces: ExtendedExtranonce) -> Result<Arc<Mutex<Bridge>>, ()> {
            create_bridge_with_upstream_target(
                extranonces,
                [
                    0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                ],
            )
        }

        pub fn create_bridge_with_upstream_target(
            extranonces: ExtendedExtranonce,
            upstream_target: [u8; 32],
        ) -> Result<Arc<Mutex<Bridge>>, ()> {
            let (tx_sv2_submit_shares_ext, _rx_sv2_submit_shares_ext) = mpsc::channel(1);
            let (tx_sv1_notify, _rx_sv1_notify) = broadcast::channel(1);

            let b = Bridge::new(
                tx_sv2_submit_shares_ext.clone(),
                tx_sv1_notify,
                extranonces.clone(),
                Arc::new(Mutex::new(upstream_target.to_vec())),
                OpenExtendedMiningChannelSuccessOwned {
                    request_id: 0,
                    channel_id: 1,
                    group_channel_id: 0,
                    target: upstream_target.into(),
                    extranonce_size: (extranonces.get_len() - extranonces.get_range0_len()) as u16,
                    extranonce_prefix: extranonces.upstream_part().into_b032(),
                },
            )
            .map_err(|_| ())?;
            Ok(b)
        }

        pub fn create_sv1_submit(job_id: u32) -> Submit {
            Submit {
                user_name: "test_user".to_string(),
                job_id: job_id.to_string(),
                extra_nonce2: sv1_api::utils::Extranonce::try_from([0; 32].to_vec()).unwrap(),
                time: sv1_api::utils::HexU32Be(1),
                nonce: sv1_api::utils::HexU32Be(1),
                version_bits: None,
                id: 0,
            }
        }

        pub fn create_sv1_submit_with_fields(
            job_id: u32,
            extranonce2_len: usize,
            ntime: u32,
            nonce: u32,
        ) -> Submit {
            Submit {
                user_name: "test_user".to_string(),
                job_id: job_id.to_string(),
                extra_nonce2: sv1_api::utils::Extranonce::try_from(vec![0; extranonce2_len])
                    .unwrap(),
                time: sv1_api::utils::HexU32Be(ntime),
                nonce: sv1_api::utils::HexU32Be(nonce),
                version_bits: None,
                id: 0,
            }
        }
    }

    fn test_extended_job(job_id: u32) -> NewExtendedMiningJobOwned {
        let out_id = bitcoin::hashes::sha256d::Hash::from_slice(&[0_u8; 32]).unwrap();
        let previous_output = bitcoin::OutPoint {
            txid: bitcoin::Txid::from_raw_hash(out_id),
            vout: 0xffff_ffff,
        };
        let input = bitcoin::TxIn {
            previous_output,
            script_sig: vec![89_u8; 16].into(),
            sequence: bitcoin::Sequence(0),
            witness: Witness::new(),
        };
        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version(1),
            lock_time: bitcoin::locktime::absolute::LockTime::from_time(TEST_NTIME).unwrap(),
            input: vec![input],
            output: vec![],
        };
        let tx = bitcoin::consensus::serialize(&tx);
        NewExtendedMiningJobOwned {
            channel_id: 1,
            job_id,
            min_ntime: binary_sv2::Sv2OptionOwned::new(Some(TEST_NTIME)),
            version: 0,
            version_rolling_allowed: false,
            merkle_path: vec![].try_into().expect("Empty merkle path"),
            coinbase_tx_prefix: tx[0..42].to_vec().try_into().unwrap(),
            coinbase_tx_suffix: tx[58..].to_vec().try_into().unwrap(),
        }
    }

    fn seed_bridge_job(bridge: &mut Bridge) {
        let prev_hash = SetNewPrevHashOwned {
            channel_id: 1,
            job_id: TEST_JOB_ID,
            prev_hash: [3; 32].into(),
            min_ntime: TEST_NTIME,
            nbits: 0x1d00ffff,
        };
        bridge.channel_factory.on_new_prev_hash(prev_hash).unwrap();
        bridge
            .channel_factory
            .on_new_extended_mining_job(test_extended_job(TEST_JOB_ID))
            .unwrap();
    }

    #[test]
    fn miner_channels_do_not_expand_native_job_fanout() {
        let bridge = test_utils::create_bridge(ExtendedExtranonce::new(0..6, 6..8, 8..16)).unwrap();
        bridge
            .safe_lock(|bridge| {
                seed_bridge_job(bridge);
                for _ in 0..64 {
                    bridge.on_new_sv1_connection(1_000.).unwrap();
                }
                let messages = bridge
                    .channel_factory
                    .on_new_extended_mining_job(test_extended_job(1))
                    .unwrap();
                assert!(
                    messages.len() <= 1,
                    "SV1 miners receive one broadcast; native per-miner channel storage creates {} unused job copies",
                    messages.len()
                );
            })
            .unwrap();
    }

    #[tokio::test]
    async fn group_updates_preserve_order_and_reject_other_groups() {
        let bridge = test_utils::create_bridge(ExtendedExtranonce::new(0..6, 6..8, 8..16)).unwrap();
        let (work_tx, work_rx) = mpsc::channel(4);
        let handler = Bridge::handle_upstream_work(bridge.clone(), work_rx).unwrap();
        let group = roles_logic_sv2::mining_sv2::SetGroupChannelOwned {
            group_channel_id: 42,
            channel_ids: vec![1].try_into().unwrap(),
        };
        work_tx
            .send(BridgeWork::SetGroupChannel(group))
            .await
            .unwrap();
        let prev_hash = SetNewPrevHashOwned {
            channel_id: 42,
            job_id: TEST_JOB_ID,
            prev_hash: [3; 32].into(),
            min_ntime: TEST_NTIME,
            nbits: 0x1d00ffff,
        };
        work_tx
            .send(BridgeWork::SetNewPrevHash(prev_hash))
            .await
            .unwrap();
        let mut job = test_extended_job(TEST_JOB_ID);
        job.channel_id = 42;
        work_tx
            .send(BridgeWork::NewExtendedMiningJob {
                job,
                merge_mining_binding_id: None,
            })
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if bridge
                    .safe_lock(|bridge| bridge.last_notify.is_some())
                    .unwrap()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Group work must create a notification");
        bridge
            .safe_lock(|bridge| {
                assert_eq!(
                    bridge.channel_factory.upstream_channel().group_channel_id,
                    42
                );
                let mut job = test_extended_job(TEST_JOB_ID + 1);
                job.channel_id = 43;
                assert!(matches!(
                    bridge.channel_factory.on_new_extended_mining_job(job),
                    Err(RolesLogicError::NotFoundChannelId)
                ));
            })
            .unwrap();
        handler.abort();
        let _ = handler.await;
    }

    #[tokio::test]
    async fn bound_job_without_notify_receivers_does_not_fail_the_bridge() {
        let extranonces = ExtendedExtranonce::new(0..6, 6..8, 8..16);
        let bridge = test_utils::create_bridge(extranonces).expect("bridge");
        let prev_hash = SetNewPrevHashOwned {
            channel_id: 1,
            job_id: TEST_JOB_ID,
            prev_hash: [3; 32].into(),
            min_ntime: TEST_NTIME,
            nbits: 0x1d00ffff,
        };
        bridge
            .safe_lock(|bridge| {
                bridge.last_p_hash = Some(prev_hash.clone());
                bridge.channel_factory.on_new_prev_hash(prev_hash)
            })
            .expect("bridge lock")
            .expect("prevhash");
        let (notify_tx, notify_rx) = broadcast::channel(1);
        drop(notify_rx);

        Bridge::handle_new_extended_mining_job_(
            bridge.clone(),
            test_extended_job(TEST_JOB_ID),
            notify_tx,
            Some(42),
        )
        .await
        .expect("a job with MM metadata must follow the normal bridge path");

        let binding_id = bridge
            .safe_lock(|bridge| {
                bridge
                    .last_notify
                    .as_ref()
                    .and_then(|notify| notify.merge_mining_binding_id)
            })
            .expect("bridge lock");
        assert_eq!(binding_id, Some(42));
    }

    fn classify_submit(
        bridge: &mut Bridge,
        channel_id: u32,
        extranonce2_len: usize,
        nonce: u32,
    ) -> OnNewShare {
        let upstream_target: [u8; 32] = bridge
            .target
            .safe_lock(|t| t.clone())
            .unwrap()
            .try_into()
            .unwrap();
        let mut upstream_target: Target = upstream_target.into();
        bridge.channel_factory.set_target(&mut upstream_target);
        let translated = bridge
            .translate_submit(
                channel_id,
                test_utils::create_sv1_submit_with_fields(
                    TEST_JOB_ID,
                    extranonce2_len,
                    TEST_NTIME,
                    nonce,
                ),
                None,
            )
            .unwrap();
        bridge
            .channel_factory
            .on_submit_shares_extended(translated)
            .unwrap()
    }

    #[test]
    fn test_version_bits_insert() {
        let extranonces = ExtendedExtranonce::new(0..6, 6..8, 8..16);
        let bridge = match test_utils::create_bridge(extranonces) {
            Ok(bridge) => bridge,
            Err(_) => return,
        };
        bridge
            .safe_lock(|bridge| {
                let channel_id = 1;
                let out_id = bitcoin::hashes::sha256d::Hash::from_slice(&[
                    0_u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0,
                ])
                .unwrap();
                let p_out = bitcoin::OutPoint {
                    txid: bitcoin::Txid::from_raw_hash(out_id),
                    vout: 0xffff_ffff,
                };
                let in_ = bitcoin::TxIn {
                    previous_output: p_out,
                    script_sig: vec![89_u8; 16].into(),
                    sequence: bitcoin::Sequence(0),
                    witness: Witness::new(),
                };
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as u32;

                let tx = bitcoin::Transaction {
                    version: bitcoin::transaction::Version(1),
                    lock_time: bitcoin::locktime::absolute::LockTime::from_time(now).unwrap(),
                    input: vec![in_],
                    output: vec![],
                };
                let tx = bitcoin::consensus::serialize(&tx);
                let _down = bridge
                    .channel_factory
                    .open_channel(0, 10_000_000_000.0)
                    .unwrap();
                let prev_hash = SetNewPrevHashOwned {
                    channel_id,
                    job_id: 0,
                    prev_hash: [
                        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
                        3, 3, 3, 3, 3, 3, 3,
                    ]
                    .into(),
                    min_ntime: 989898,
                    nbits: 9,
                };
                bridge.channel_factory.on_new_prev_hash(prev_hash).unwrap();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as u32;
                let new_mining_job = NewExtendedMiningJobOwned {
                    channel_id,
                    job_id: 0,
                    min_ntime: binary_sv2::Sv2OptionOwned::new(Some(now)),
                    version: 0b0000_0000_0000_0000,
                    version_rolling_allowed: false,
                    merkle_path: vec![].try_into().expect("Empty merkle path"),
                    coinbase_tx_prefix: tx[0..42].to_vec().try_into().unwrap(),
                    coinbase_tx_suffix: tx[58..].to_vec().try_into().unwrap(),
                };
                bridge
                    .channel_factory
                    .on_new_extended_mining_job(new_mining_job.clone())
                    .unwrap();

                // pass sv1_submit into Bridge::translate_submit
                let sv1_submit = test_utils::create_sv1_submit(0);
                let sv2_message = bridge
                    .translate_submit(channel_id, sv1_submit, None)
                    .unwrap();
                // assert sv2 message equals sv1 with version bits added
                assert_eq!(
                    new_mining_job.version, sv2_message.version,
                    "Version bits were not inserted for non version rolling sv1 message"
                );
            })
            .unwrap();
    }

    #[test]
    fn opening_second_downstream_does_not_clobber_shared_upstream_target() {
        let extranonces = ExtendedExtranonce::new(0..6, 6..8, 8..16);
        let upstream_target = Downstream::difficulty_to_target(1_024.0);
        let bridge =
            test_utils::create_bridge_with_upstream_target(extranonces, upstream_target).unwrap();

        bridge
            .safe_lock(|bridge| {
                bridge.on_new_sv1_connection(1_000_000_000_000.0).unwrap();
                bridge.on_new_sv1_connection(10_000_000_000_000.0).unwrap();
            })
            .unwrap();

        let shared_target = bridge
            .safe_lock(|bridge| bridge.target.safe_lock(|t| t.clone()).unwrap())
            .unwrap();
        assert_eq!(shared_target, upstream_target.to_vec());
    }

    #[test]
    fn repeated_downstream_opens_leave_shared_upstream_target_unchanged() {
        let extranonces = ExtendedExtranonce::new(0..6, 6..8, 8..16);
        let upstream_target = Downstream::difficulty_to_target(2_048.0);
        let bridge =
            test_utils::create_bridge_with_upstream_target(extranonces, upstream_target).unwrap();

        bridge
            .safe_lock(|bridge| {
                for index in 0..32 {
                    let hash_rate = 1_000_000_000_000.0 + (index as f32 * 250_000_000_000.0);
                    bridge.on_new_sv1_connection(hash_rate).unwrap();
                }
            })
            .unwrap();

        let shared_target = bridge
            .safe_lock(|bridge| bridge.target.safe_lock(|t| t.clone()).unwrap())
            .unwrap();
        assert_eq!(shared_target, upstream_target.to_vec());
    }

    #[test]
    fn share_that_only_meets_downstream_target_stays_local_with_two_channels() {
        let extranonces = ExtendedExtranonce::new(0..6, 6..8, 8..16);
        let upstream_target = [0; 32];
        let bridge =
            test_utils::create_bridge_with_upstream_target(extranonces, upstream_target).unwrap();

        let (channel_two_id, channel_two_extranonce2_len) = bridge
            .safe_lock(|bridge| {
                seed_bridge_job(bridge);
                let channel_one = bridge.on_new_sv1_connection(1_000_000_000_000.0).unwrap();
                let channel_two = bridge.on_new_sv1_connection(2_000_000_000_000.0).unwrap();
                bridge.channel_factory.update_target_for_channel(
                    channel_one.channel_id,
                    Downstream::difficulty_to_target(16_384.0).into(),
                );
                bridge
                    .channel_factory
                    .update_target_for_channel(channel_two.channel_id, [255; 32].into());
                (channel_two.channel_id, channel_two.extranonce2_len as usize)
            })
            .unwrap();

        let (found_local_only_share, downstream_hits, upstream_hits, bitcoin_hits, errors) = bridge
            .safe_lock(|bridge| {
                let mut downstream_hits = 0;
                let mut upstream_hits = 0;
                let mut bitcoin_hits = 0;
                let mut errors = 0;

                let found = (0..4_096).any(|nonce| {
                    match classify_submit(
                        bridge,
                        channel_two_id,
                        channel_two_extranonce2_len,
                        nonce,
                    ) {
                        OnNewShare::ShareMeetDownstreamTarget => {
                            downstream_hits += 1;
                            true
                        }
                        OnNewShare::SendSubmitShareUpstream(_) => {
                            upstream_hits += 1;
                            false
                        }
                        OnNewShare::ShareMeetBitcoinTarget(_) => {
                            bitcoin_hits += 1;
                            false
                        }
                        OnNewShare::SendErrorDownstream(_) => {
                            errors += 1;
                            false
                        }
                        OnNewShare::RelaySubmitShareUpstream => false,
                    }
                });

                (found, downstream_hits, upstream_hits, bitcoin_hits, errors)
            })
            .unwrap();

        assert!(
            found_local_only_share,
            "expected at least one share to stay local; downstream={downstream_hits}, upstream={upstream_hits}, bitcoin={bitcoin_hits}, errors={errors}"
        );
    }
}
