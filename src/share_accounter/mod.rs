mod errors;
pub mod ext_negotiation;
mod task_manager;
mod verfication;

use errors::Error;
use std::sync::Arc;
use tracing::{error, info, warn};

use binary_sv2::Sv2DataType;
use dashmap::DashMap;
use demand_share_accounting_ext::*;
use parser::{PoolExtMessages, ShareAccountingMessages};
use roles_logic_sv2::{mining_sv2::SubmitSharesSuccess, parsers::Mining};
use task_manager::TaskManager;
use verfication::{VerificationConfig, VerificationService};

use crate::{
    proxy_state::{ProxyState, ShareAccounterState},
    shared::utils::AbortOnDrop,
    PoolState,
};

pub async fn start(
    receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    sender: tokio::sync::mpsc::Sender<Mining<'static>>,
    mut up_receiver: tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>,
    up_sender: tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
) -> Result<AbortOnDrop, Error> {
    // Negotiate extensions first
    let ext_handler = ext_negotiation::negotiate_extension_after_connection(
        up_sender.clone(),
        &mut up_receiver,
        std::time::Duration::from_secs(10),
    )
    .await?;

    if !ext_handler.is_share_accounting_enabled() {
        warn!("Share accounting extension not enabled - running without verification");
    }

    let task_manager = TaskManager::initialize();
    let shares_sent_up = Arc::new(DashMap::with_capacity(100));
    let verification_service = VerificationService::new(VerificationConfig::default());

    let abortable = task_manager
        .safe_lock(|t| t.get_aborter())
        .map_err(|_| Error::ShareAccounterTaskManagerMutexCorrupted)?
        .ok_or(Error::ShareAccounterTaskManagerError)?;

    let relay_up_task = relay_up(
        receiver,
        up_sender,
        shares_sent_up.clone(),
        verification_service.clone(),
    );
    TaskManager::add_relay_up(task_manager.clone(), relay_up_task)
        .await
        .map_err(|_| Error::ShareAccounterTaskManagerError)?;

    let relay_down_task = relay_down(
        up_receiver,
        sender,
        shares_sent_up.clone(),
        verification_service.clone(),
    );
    TaskManager::add_relay_down(task_manager.clone(), relay_down_task)
        .await
        .map_err(|_| Error::ShareAccounterTaskManagerError)?;

    Ok(abortable)
}

struct ShareSentUp {
    channel_id: u32,
    sequence_number: u32,
    share_data: Option<Share<'static>>, // Store share for verification
}

fn relay_up(
    mut receiver: tokio::sync::mpsc::Receiver<Mining<'static>>,
    up_sender: tokio::sync::mpsc::Sender<PoolExtMessages<'static>>,
    shares_sent_up: Arc<DashMap<u32, ShareSentUp>>,
    verification_service: Arc<VerificationService>,
) -> AbortOnDrop {
    let task = tokio::spawn(async move {
        while let Some(msg) = receiver.recv().await {
            if let Mining::SubmitSharesExtended(m) = &msg {
                // Extract share data if available for verification
                let share_data = verification_service.extract_share_from_submit(&m).ok();

                shares_sent_up.insert(
                    m.job_id,
                    ShareSentUp {
                        channel_id: m.channel_id,
                        sequence_number: m.sequence_number,
                        share_data,
                    },
                );
            }

            let msg = PoolExtMessages::Mining(msg);
            if up_sender.send(msg).await.is_err() {
                break;
            }
        }
    });
    task.into()
}

fn relay_down(
    mut up_receiver: tokio::sync::mpsc::Receiver<PoolExtMessages<'static>>,
    sender: tokio::sync::mpsc::Sender<Mining<'static>>,
    shares_sent_up: Arc<DashMap<u32, ShareSentUp>>,
    verification_service: Arc<VerificationService>,
) -> AbortOnDrop {
    let task = tokio::spawn(async move {
        while let Some(msg) = up_receiver.recv().await {
            match msg {
                PoolExtMessages::ShareAccountingMessages(ref share_msg) => {
                    if let Err(e) = handle_share_accounting_message(
                        share_msg,
                        &sender,
                        &shares_sent_up,
                        &verification_service,
                    )
                    .await
                    {
                        error!("Error handling share accounting message: {}", e);
                        ProxyState::update_share_accounter_state(ShareAccounterState::Down);
                        break;
                    }
                }
                PoolExtMessages::Mining(msg) => {
                    if let Err(e) = sender.send(msg).await {
                        error!("{e}");
                        ProxyState::update_share_accounter_state(ShareAccounterState::Down);
                        break;
                    }
                }
                _ => {
                    error!("Pool sent unexpected message on mining connection");
                    ProxyState::update_pool_state(PoolState::Down);
                    break;
                }
            }
        }
    });
    task.into()
}

async fn handle_share_accounting_message(
    msg: &ShareAccountingMessages<'_>,
    sender: &tokio::sync::mpsc::Sender<Mining<'static>>,
    shares_sent_up: &Arc<DashMap<u32, ShareSentUp>>,
    verification_service: &Arc<VerificationService>,
) -> Result<(), Error> {
    match msg {
        ShareAccountingMessages::ShareOk(share_ok) => {
            handle_share_ok(share_ok, sender, shares_sent_up, verification_service).await
        }
        ShareAccountingMessages::GetWindowSuccess(window_success) => {
            handle_window_success(window_success, verification_service).await
        }
        ShareAccountingMessages::GetSharesSuccess(shares_success) => {
            handle_shares_success(shares_success, verification_service).await
        }
        ShareAccountingMessages::NewBlockFound(block_found) => {
            handle_new_block_found(block_found, verification_service).await
        }
        _ => {
            info!("Received other share accounting message: {:?}", msg);
            Ok(())
        }
    }
}

async fn handle_share_ok(
    share_ok: &ShareOk,
    sender: &tokio::sync::mpsc::Sender<Mining<'static>>,
    shares_sent_up: &Arc<DashMap<u32, ShareSentUp>>,
    verification_service: &Arc<VerificationService>,
) -> Result<(), Error> {
    let job_id_bytes = share_ok.ref_job_id.to_le_bytes();
    let job_id = u32::from_le_bytes(
        job_id_bytes[4..8]
            .try_into()
            .expect("Internal error: job_id_bytes[4..8] can always be convertible into a u32"),
    );

    let share_sent_up = match shares_sent_up.remove(&job_id) {
        Some(shares) => shares.1,
        None => {
            // error!("Pool sent invalid share success for job_id: {}", job_id);
            ShareSentUp {
                channel_id: 1,
                sequence_number: 1,
                share_data: None,
            }
        }
    };

    let success = Mining::SubmitSharesSuccess(SubmitSharesSuccess {
        channel_id: share_sent_up.channel_id,
        last_sequence_number: share_sent_up.sequence_number,
        new_submits_accepted_count: 1,
        new_shares_sum: 1,
    });

    sender.send(success).await.map_err(|e| {
        error!("{e:?}");
        ProxyState::update_share_accounter_state(ShareAccounterState::Down);
        Error::SendError
    })?;

    // info!(
    //     "📥 Storing ShareOk data: ref_job_id={}, share_index={}",
    //     share_ok.ref_job_id, share_ok.share_index
    // );
    verification_service.store_share_ok_data(share_ok).await;

    Ok(())
}

async fn handle_window_success(
    window_success: &GetWindowSuccess<'_>,
    verification_service: &Arc<VerificationService>,
) -> Result<(), Error> {
    info!(
        "Received window success with {} slices",
        window_success.slices.to_owned().into_inner().len(),
    );

    match verification_service.verify_window(window_success).await {
        Ok(result) => {
            info!("🔍 WINDOW VERIFICATION RESULT:");
            info!("  ✅ Window Valid: {}", result.window_valid);
            info!("  📊 Total Difficulty: {}", result.total_difficulty);
            info!("  🔗 PHash Consistency: {}", result.phash_consistency);
            info!("  📏 Window Size Valid: {}", result.window_size_valid);
            info!(
                "  📋 Slice Results: {}/{} slices valid",
                result
                    .slice_results
                    .iter()
                    .filter(|s| s.total_shares_valid)
                    .count(),
                result.slice_results.len()
            );

            for (i, slice_result) in result.slice_results.iter().enumerate() {
                info!(
                    "    Slice {}: job_id={}, shares_valid={}, difficulty_valid={}",
                    i,
                    slice_result.slice_job_id,
                    slice_result.total_shares_valid,
                    slice_result.difficulty_sum_valid
                );
            }
        }
        Err(e) => {
            error!("❌ WINDOW VERIFICATION FAILED: {:?}", e);
        }
    }

    Ok(())
}

async fn handle_shares_success(
    shares_success: &GetSharesSuccess<'_>,
    verification_service: &Arc<VerificationService>,
) -> Result<(), Error> {
    let share_count = shares_success.shares.to_owned().into_inner().len();
    info!("Received shares success with {} shares", share_count);

    // Store shares first
    verification_service.store_shares(shares_success).await;

    // **TRIGGER INDIVIDUAL SHARE VERIFICATION**
    let mut valid_shares = 0;
    let mut total_shares = 0;

    for share in shares_success.shares.to_owned().into_inner().iter() {
        total_shares += 1;

        // Try to verify each share against any slice it might belong to
        if let Some(verification_result) = verification_service
            .verify_share_with_stored_slices(share)
            .await
        {
            match verification_result {
                Ok(result) => {
                    if result.merkle_valid && result.difficulty_valid && result.fees_valid {
                        valid_shares += 1;
                        info!(
                            "  ✅ Share {} valid: merkle={}, difficulty={}, fees={}",
                            share.share_index,
                            result.merkle_valid,
                            result.difficulty_valid,
                            result.fees_valid
                        );
                    } else {
                        warn!(
                            "  ❌ Share {} invalid: merkle={}, difficulty={}, fees={}",
                            share.share_index,
                            result.merkle_valid,
                            result.difficulty_valid,
                            result.fees_valid
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "  💥 Share {} verification error: {:?}",
                        share.share_index, e
                    );
                }
            }
        }
    }

    info!(
        "🔍 SHARE VERIFICATION SUMMARY: {}/{} shares valid",
        valid_shares, total_shares
    );

    Ok(())
}

async fn handle_new_block_found(
    block_found: &NewBlockFound<'_>,
    verification_service: &Arc<VerificationService>,
) -> Result<(), Error> {
    info!("NEW BLOCK FOUND - Triggering comprehensive verification");

    // Wait a bit for response
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // **TRIGGER COMPREHENSIVE BLOCK VERIFICATION**
    verification_service
        .verify_block_completion(block_found)
        .await;

    // Print verification stats
    let stats = verification_service.get_verification_stats().await;
    info!("VERIFICATION STATS:");
    info!("  Total Shares: {}", stats.total_shares);
    info!("  Total Jobs: {}", stats.total_jobs);
    info!("  Cached Windows: {}", stats.cached_windows);
    info!("  Verification Enabled: {}", stats.verification_enabled);

    Ok(())
}
