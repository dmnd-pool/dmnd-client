use std::{
    collections::VecDeque,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    proxy_state::{DownstreamType, ProxyState},
    translator::error::Error,
};
use binary_sv2::Sv2DataType;
use bitcoin::{
    block::{Header, Version},
    hashes::{sha256d, Hash as BHash},
    BlockHash, CompactTarget,
};
use lazy_static::lazy_static;
use roles_logic_sv2::utils::Mutex;
use sv1_api::{client_to_server, server_to_client::Notify};
use tracing::error;

use super::downstream::Downstream;
lazy_static! {
    pub static ref SHARE_TIMESTAMPS: Arc<Mutex<VecDeque<tokio::time::Instant>>> =
        Arc::new(Mutex::new(VecDeque::with_capacity(70)));
    pub static ref IS_RATE_LIMITED: AtomicBool = AtomicBool::new(false);
    static ref SHARE_COUNTS: Arc<Mutex<std::collections::HashMap<u32, (u32, tokio::time::Instant)>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
}

/// Checks if a share can be sent upstream based on a rate limit of 70 shares per minute.
/// Returns `true` if the share can be sent, `false` if the limit is exceeded.
pub async fn check_share_rate_limit() {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        interval.tick().await;
        let now = tokio::time::Instant::now();
        let count = SHARE_TIMESTAMPS
            .safe_lock(|timestamps| {
                while let Some(&front) = timestamps.front() {
                    if now.duration_since(front).as_secs() >= 60 {
                        timestamps.pop_front();
                    } else {
                        break;
                    }
                }
                timestamps.len()
            })
            .unwrap_or_else(|e| {
                error!("Failed to lock SHARE_TIMESTAMPS: {:?}", e);
                ProxyState::update_downstream_state(DownstreamType::TranslatorDownstream);
                0
            });

        IS_RATE_LIMITED.store(count >= 70, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Checks if a share can be sent by checking if rate is limited
pub fn allow_submit_share() -> crate::translator::error::ProxyResult<'static, bool> {
    // Check if rate-limited
    let is_rate_limited = IS_RATE_LIMITED.load(std::sync::atomic::Ordering::SeqCst);

    if is_rate_limited {
        return Ok(false); // Rate limit exceeded, don’t send
    }

    SHARE_TIMESTAMPS
        .safe_lock(|timestamps| {
            timestamps.push_back(tokio::time::Instant::now());
        })
        .map_err(|e| {
            error!("Failed to lock SHARE_TIMESTAMPS: {:?}", e);
            Error::TranslatorDiffConfigMutexPoisoned
        })?;

    Ok(true) // Share can be sent
}

pub fn validate_share(
    request: &client_to_server::Submit<'static>,
    job: &Notify,
    difficulty: f32,
    extranonce1: Vec<u8>,
    version_rolling_mask: Option<sv1_api::utils::HexU32Be>,
) -> bool {
    // Check job ID match
    if request.job_id != job.job_id {
        error!("Share rejected: Job ID mismatch");
        return false;
    }

    let prev_hash_vec: Vec<u8> = job.prev_hash.clone().into();
    let prev_hash = binary_sv2::U256::from_vec_(prev_hash_vec).unwrap();
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
    println!("Hash: {:?}", hex::encode(hash));
    let target = Downstream::difficulty_to_target(difficulty);
    println!("Target: {:?}", hex::encode(target));
    hash <= target
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
