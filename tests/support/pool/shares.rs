use super::{
    jobs::{JobKey, JobRecord},
    mining::ChannelState,
    protocol::SubmitSharesExtended,
    transport::PeerId,
};
use bitcoin::{
    block::{Header, Version},
    consensus::{deserialize, serialize},
    hashes::{sha256d, Hash},
    BlockHash, CompactTarget, Target, Transaction, TxMerkleNode,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Serialize)]
pub struct ValidatedShare {
    pub key: JobKey,
    pub sequence_number: u32,
    pub header: Vec<u8>,
    pub hash: String,
    pub target: [u8; 32],
    pub network_target: [u8; 32],
    pub block_candidate: bool,
    pub extranonce: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareRejection {
    UnknownChannel,
    UnknownJob,
    Stale,
    Extranonce,
    Timestamp,
    Version,
    Coinbase,
    LowDifficulty,
    Duplicate,
    DuplicateLimit,
}

impl ShareRejection {
    pub fn code(self) -> &'static str {
        match self {
            Self::UnknownChannel => "invalid-channel-id",
            Self::UnknownJob => "invalid-job-id",
            Self::Stale => "stale-share",
            Self::Extranonce => "invalid-extranonce-size",
            Self::Timestamp => "invalid-ntime",
            Self::Version => "invalid-version",
            Self::Coinbase => "invalid-coinbase",
            Self::LowDifficulty => "difficulty-too-low",
            Self::Duplicate => "duplicate-share",
            Self::DuplicateLimit => "duplicate-cache-full",
        }
    }
}

#[derive(Debug)]
pub enum ShareOutcome {
    Accepted(ValidatedShare),
    BlockCandidate(ValidatedShare),
    Rejected(ShareRejection),
}

#[derive(Default)]
pub struct ShareValidator {
    seen: HashMap<JobKey, HashSet<[u8; 32]>>,
}

pub fn merkle_root(mut root: [u8; 32], path: &[[u8; 32]]) -> [u8; 32] {
    for sibling in path {
        let mut bytes = [0; 64];
        bytes[..32].copy_from_slice(&root);
        bytes[32..].copy_from_slice(sibling);
        root = sha256d::Hash::hash(&bytes).to_byte_array();
    }
    root
}

impl ShareValidator {
    pub fn validate(
        &mut self,
        peer: PeerId,
        channel: Option<&ChannelState>,
        job: Option<&JobRecord>,
        share: &SubmitSharesExtended,
    ) -> ShareOutcome {
        let reject = ShareOutcome::Rejected;
        let Some(channel) =
            channel.filter(|c| c.key.peer == peer && c.key.channel == share.channel_id)
        else {
            return reject(ShareRejection::UnknownChannel);
        };
        let key = JobKey {
            peer,
            channel: share.channel_id,
            job: share.job_id,
        };
        let Some(job) = job.filter(|j| j.key == key) else {
            return reject(ShareRejection::UnknownJob);
        };
        let Some(prev_hash) = job.prev_hash.filter(|_| job.valid) else {
            return reject(ShareRejection::Stale);
        };
        if share.extranonce.as_ref().len() != channel.extranonce_size as usize {
            return reject(ShareRejection::Extranonce);
        }
        if share.ntime < job.min_ntime || share.ntime > job.min_ntime.saturating_add(7200) {
            return reject(ShareRejection::Timestamp);
        }
        let mask = if job.message.version_rolling_allowed {
            0x1fff_e000
        } else {
            0
        };
        if (share.version ^ job.message.version) & !mask != 0 {
            return reject(ShareRejection::Version);
        }
        let bytes = [
            job.message.coinbase_tx_prefix.as_ref(),
            &channel.extranonce_prefix,
            share.extranonce.as_ref(),
            job.message.coinbase_tx_suffix.as_ref(),
        ]
        .concat();
        let Ok(coinbase) = deserialize::<Transaction>(&bytes) else {
            return reject(ShareRejection::Coinbase);
        };
        if !coinbase.is_coinbase() {
            return reject(ShareRejection::Coinbase);
        }
        let path = job
            .message
            .merkle_path
            .clone()
            .into_inner()
            .into_iter()
            .map(|h| h.as_ref().try_into().expect("U256"))
            .collect::<Vec<_>>();
        let root = merkle_root(coinbase.compute_txid().to_byte_array(), &path);
        let header = Header {
            version: Version::from_consensus(share.version as i32),
            prev_blockhash: BlockHash::from_byte_array(prev_hash),
            merkle_root: TxMerkleNode::from_byte_array(root),
            time: share.ntime,
            bits: CompactTarget::from_consensus(job.nbits),
            nonce: share.nonce,
        };
        let hash = header.block_hash();
        let target = Target::from_le_bytes(channel.target);
        if !target.is_met_by(hash) {
            return reject(ShareRejection::LowDifficulty);
        }
        // Retain duplicate protection for every live job, and prune jobs outside the bounded job window.
        let seen = self.seen.entry(key).or_default();
        if seen.contains(&hash.to_byte_array()) {
            return reject(ShareRejection::Duplicate);
        }
        if seen.len() >= 8192 {
            return reject(ShareRejection::DuplicateLimit);
        }
        seen.insert(hash.to_byte_array());
        let network = Target::from_compact(header.bits);
        let block_candidate = network.is_met_by(hash);
        let validated = ValidatedShare {
            key,
            sequence_number: share.sequence_number,
            header: serialize(&header),
            hash: hash.to_string(),
            target: channel.target,
            network_target: network.to_le_bytes(),
            block_candidate,
            extranonce: [&channel.extranonce_prefix[..], share.extranonce.as_ref()].concat(),
        };
        if block_candidate {
            ShareOutcome::BlockCandidate(validated)
        } else {
            ShareOutcome::Accepted(validated)
        }
    }

    pub fn retain_jobs(&mut self, jobs: &HashMap<JobKey, JobRecord>) {
        self.seen.retain(|key, _| jobs.contains_key(key));
    }
}
