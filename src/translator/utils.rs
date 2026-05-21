use std::{
    collections::VecDeque,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use crate::{
    config::Configuration,
    monitor::shares::RejectionReason,
    proxy_state::{DownstreamType, ProxyState},
    share_log_enabled,
    translator::error::Error,
};
use binary_sv2::Sv2DataType;
use bitcoin::{
    block::{Header, Version},
    hashes::{sha256d, Hash as BHash},
    hex::DisplayHex,
    BlockHash, CompactTarget,
};
use lazy_static::lazy_static;
use roles_logic_sv2::utils::Mutex;
use sv1_api::{client_to_server, server_to_client::Notify};
use tracing::{debug, error, info};

use super::downstream::Downstream;

const SHARE_RATE_LIMIT_PER_MINUTE: usize = 70;
const SHARE_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);

lazy_static! {
    pub static ref SHARE_TIMESTAMPS: Arc<Mutex<VecDeque<tokio::time::Instant>>> = Arc::new(
        Mutex::new(VecDeque::with_capacity(SHARE_RATE_LIMIT_PER_MINUTE))
    );
    pub static ref IS_RATE_LIMITED: AtomicBool = AtomicBool::new(false);
    static ref SHARE_COUNTS: Arc<Mutex<std::collections::HashMap<u32, (u32, tokio::time::Instant)>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
}

fn prune_expired_share_timestamps(
    timestamps: &mut VecDeque<tokio::time::Instant>,
    now: tokio::time::Instant,
) {
    while let Some(&front) = timestamps.front() {
        if now.duration_since(front) >= SHARE_RATE_LIMIT_WINDOW {
            timestamps.pop_front();
        } else {
            break;
        }
    }
}

fn reserve_share_slot(
    timestamps: &mut VecDeque<tokio::time::Instant>,
    now: tokio::time::Instant,
) -> bool {
    prune_expired_share_timestamps(timestamps, now);
    if timestamps.len() >= SHARE_RATE_LIMIT_PER_MINUTE {
        return false;
    }

    timestamps.push_back(now);
    true
}

/// Checks if a share can be sent upstream based on a rate limit of 70 shares per minute.
/// Returns `true` if the share can be sent, `false` if the limit is exceeded.
pub async fn check_share_rate_limit(downstream: Arc<Mutex<Downstream>>) {
    if Configuration::difficulty_updates_disabled() {
        return;
    }

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut last_update = tokio::time::Instant::now(); // Track last difficulty update
    let mut rate_limit_hit_count = 0;

    loop {
        interval.tick().await;
        let now = tokio::time::Instant::now();
        let count = SHARE_TIMESTAMPS
            .safe_lock(|timestamps| {
                prune_expired_share_timestamps(timestamps, now);
                timestamps.len()
            })
            .unwrap_or_else(|e| {
                error!("Failed to lock SHARE_TIMESTAMPS: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                0
            });

        let is_limited = count >= SHARE_RATE_LIMIT_PER_MINUTE;
        IS_RATE_LIMITED.store(is_limited, std::sync::atomic::Ordering::SeqCst);

        if is_limited {
            rate_limit_hit_count += 1;
        } else {
            rate_limit_hit_count = 0;
        }

        if rate_limit_hit_count >= 5 && now.duration_since(last_update).as_secs() >= 2 {
            debug!("Rate limited. Updating difficulty");
            if let Err(e) = Downstream::try_update_difficulty_settings(&downstream).await {
                error!("Failed to update difficulty: {e}");
            }
            last_update = now;
            rate_limit_hit_count = 0;
        }
    }
}

/// Reserves one upstream-share slot in the rolling rate-limit window.
pub fn allow_submit_share() -> crate::translator::error::ProxyResult<'static, bool> {
    if Configuration::difficulty_updates_disabled() {
        return Ok(true);
    }

    let now = tokio::time::Instant::now();
    let allowed = SHARE_TIMESTAMPS
        .safe_lock(|timestamps| {
            let allowed = reserve_share_slot(timestamps, now);
            IS_RATE_LIMITED.store(
                !allowed || timestamps.len() >= SHARE_RATE_LIMIT_PER_MINUTE,
                std::sync::atomic::Ordering::SeqCst,
            );
            allowed
        })
        .map_err(|e| {
            error!("Failed to lock SHARE_TIMESTAMPS: {:?}", e);
            Error::TranslatorDiffConfigMutexPoisoned
        })?;

    Ok(allowed)
}

pub fn validate_share(
    request: &client_to_server::Submit<'static>,
    job: &Notify<'static>,
    difficulty: f32,
    extranonce1: Vec<u8>,
    version_rolling_mask: Option<sv1_api::utils::HexU32Be>,
) -> bool {
    if share_log_enabled() {
        info!(
            "Validating share from request {} and job {}",
            request.id, request.job_id
        );
    }

    let prev_hash_vec: Vec<u8> = job.prev_hash.clone().into();
    let prev_hash = match binary_sv2::U256::from_vec_(prev_hash_vec) {
        Ok(hash) => hash,
        Err(e) => {
            error!("Share rejected: Invalid previous hash: {:?}", e);
            return false;
        }
    };
    let mut merkle_branch = Vec::new();
    for branch in &job.merkle_branch {
        merkle_branch.push(branch.0.to_vec());
    }

    let mut extranonce = Vec::new();
    extranonce.extend_from_slice(extranonce1.as_ref());
    extranonce.extend_from_slice(request.extra_nonce2.0.as_ref());
    let extranonce: &[u8] = extranonce.as_ref();

    let job_version = job.version.0;
    let request_version = request
        .version_bits
        .clone()
        .map(|vb| vb.0)
        .unwrap_or(job_version);
    let mask = version_rolling_mask
        .unwrap_or(sv1_api::utils::HexU32Be(0x1FFFE000_u32))
        .0;
    let version = (job_version & !mask) | (request_version & mask);

    let mut hash = get_hash(
        request.nonce.0,
        version,
        request.time.0,
        extranonce,
        job,
        roles_logic_sv2::utils::u256_to_block_hash(prev_hash),
        merkle_branch,
    );

    hash.reverse(); //convert to little-endian
    if share_log_enabled() {
        info!("Share Hash: {:?}", hash.to_vec().as_hex());
    }
    let target = Downstream::difficulty_to_target(difficulty);
    debug!(
        "Checking job difficulty: {}, Target: {:?}",
        difficulty,
        target.to_vec().as_hex()
    );
    if hash <= target {
        if share_log_enabled() {
            info!("Share met Target: {:?}", target.to_vec().as_hex());
        }
        return true;
    }

    error!("Share rejected: Does not meet job difficulty");
    false
}

pub fn submit_error_to_rejection_reason(error_code: &str) -> RejectionReason {
    match error_code {
        code if code
            == roles_logic_sv2::mining_sv2::SubmitSharesError::difficulty_too_low_error_code() =>
        {
            RejectionReason::DifficultyMismatch
        }
        code if code
            == roles_logic_sv2::mining_sv2::SubmitSharesError::stale_share_error_code()
            || code
                == roles_logic_sv2::mining_sv2::SubmitSharesError::invalid_job_id_error_code() =>
        {
            RejectionReason::JobIdNotFound
        }
        _ => RejectionReason::UpstreamRejected,
    }
}

pub fn get_hash(
    nonce: u32,
    version: u32,
    ntime: u32,
    extranonce: &[u8],
    job: &Notify,
    prev_hash: BlockHash,
    merkle_path: Vec<Vec<u8>>,
) -> [u8; 32] {
    // Construct coinbase
    let mut coinbase = Vec::new();
    coinbase.extend_from_slice(job.coin_base1.as_ref());
    coinbase.extend_from_slice(extranonce);
    coinbase.extend_from_slice(job.coin_base2.as_ref());

    // Calculate the Merkle root
    let coinbase_hash = <sha256d::Hash as bitcoin::hashes::Hash>::hash(&coinbase);
    let mut merkle_root = coinbase_hash.to_byte_array();

    for path in merkle_path {
        let mut combined = Vec::with_capacity(64);
        combined.extend_from_slice(&merkle_root);
        combined.extend_from_slice(path.as_ref());
        merkle_root = <sha256d::Hash as bitcoin::hashes::Hash>::hash(&combined).to_byte_array();
    }

    // Construct the block header
    let header = Header {
        version: Version::from_consensus(version.try_into().unwrap()),
        prev_blockhash: prev_hash,
        merkle_root: bitcoin::TxMerkleNode::from_byte_array(merkle_root),
        time: ntime,
        bits: CompactTarget::from_consensus(job.bits.0),
        nonce,
    };

    // Calculate the block hash
    let block_hash: [u8; 32] = header.block_hash().to_raw_hash().to_byte_array();
    block_hash
}

// Update share count for each miner
pub fn update_share_count(connection_id: u32) {
    SHARE_COUNTS
        .safe_lock(|share_counts| {
            let now = tokio::time::Instant::now();
            if let Some((count, last_update)) = share_counts.get_mut(&connection_id) {
                if now.duration_since(*last_update) < std::time::Duration::from_secs(60) {
                    *count += 1;
                } else {
                    *count = 1;
                    *last_update = now;
                }
            } else {
                share_counts.insert(connection_id, (1, now));
            }
        })
        .unwrap_or_else(|_| {
            error!("Failed to lock SHARE_COUNTS");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream)
        });
}

// Get share count for the last 60 secs
pub fn get_share_count(connection_id: u32) -> f32 {
    let now = tokio::time::Instant::now();
    let share_counts = SHARE_COUNTS
        .safe_lock(|share_counts| {
            if let Some((count, last_update)) = share_counts.get(&connection_id) {
                if now.duration_since(*last_update) < tokio::time::Duration::from_secs(60) {
                    *count as f32 // Shares per minute
                } else {
                    0.0 // More than 60 seconds since the last share, so return 0.
                }
            } else {
                0.0
            }
        })
        .unwrap_or_else(|_| {
            error!("Failed to lock SHARE_COUNTS");
            ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
            0.0
        });
    share_counts
}

// /// currently the pool only supports 16 bytes exactly for its channels
// /// to use but that may change
// pub fn proxy_extranonce1_len(
//     channel_extranonce2_size: usize,
//     downstream_extranonce2_len: usize,
// ) -> usize {
//     // full_extranonce_len - pool_extranonce1_len - miner_extranonce2 = tproxy_extranonce1_len
//     channel_extranonce2_size - downstream_extranonce2_len
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Mutex as StdMutex,
    };

    static RATE_LIMIT_TEST_MUTEX: StdMutex<()> = StdMutex::new(());

    fn reset_global_share_limiter_for_test() {
        SHARE_TIMESTAMPS
            .safe_lock(|timestamps| timestamps.clear())
            .unwrap();
        IS_RATE_LIMITED.store(false, Ordering::SeqCst);
    }

    #[test]
    fn allow_submit_share_rejects_71st_share_without_background_tick() {
        let _guard = RATE_LIMIT_TEST_MUTEX.lock().unwrap();
        reset_global_share_limiter_for_test();

        for _ in 0..SHARE_RATE_LIMIT_PER_MINUTE {
            assert!(allow_submit_share().unwrap());
        }

        assert!(!allow_submit_share().unwrap());

        reset_global_share_limiter_for_test();
    }

    #[test]
    fn reserve_share_slot_prunes_expired_shares() {
        let now = tokio::time::Instant::now();
        let mut timestamps = VecDeque::from(vec![
            now - SHARE_RATE_LIMIT_WINDOW;
            SHARE_RATE_LIMIT_PER_MINUTE
        ]);

        assert!(reserve_share_slot(&mut timestamps, now));
        assert_eq!(timestamps.len(), 1);
    }

    #[test]
    fn reserve_share_slot_caps_concurrent_bursts() {
        let timestamps = Arc::new(StdMutex::new(VecDeque::with_capacity(
            SHARE_RATE_LIMIT_PER_MINUTE,
        )));
        let barrier = Arc::new(Barrier::new(SHARE_RATE_LIMIT_PER_MINUTE * 2));
        let admitted = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..(SHARE_RATE_LIMIT_PER_MINUTE * 2) {
            let timestamps = timestamps.clone();
            let barrier = barrier.clone();
            let admitted = admitted.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let mut timestamps = timestamps.lock().unwrap();
                if reserve_share_slot(&mut timestamps, tokio::time::Instant::now()) {
                    admitted.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(
            admitted.load(Ordering::Relaxed),
            SHARE_RATE_LIMIT_PER_MINUTE
        );
        assert_eq!(
            timestamps.lock().unwrap().len(),
            SHARE_RATE_LIMIT_PER_MINUTE
        );
    }
}
