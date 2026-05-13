use crate::op_return_injector::{OpReturnInjector, RSK_MERGED_MINING_TAG};
#[cfg(test)]
use bitcoin::consensus::Encodable;
#[cfg(test)]
use bitcoin::hashes::sha256d;
#[cfg(test)]
use bitcoin::hex::FromHex;
use bitcoin::{
    block::{Header, Version},
    consensus::{deserialize, serialize},
    hashes::Hash,
    hex::DisplayHex,
    opcodes::all::OP_RETURN,
    script::Instruction,
    BlockHash, CompactTarget, Transaction, TxMerkleNode, Txid,
};
use roles_logic_sv2::{mining_sv2::Target as Sv2Target, utils::merkle_root_from_path_};
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{debug, warn};

const MAX_PENDING_VALID_JOBS: usize = 32;
const MAX_CACHED_TEMPLATE_CONTEXTS: usize = 128;
const MAX_CACHED_TEMPLATE_COINBASE_CONTEXTS: usize = 128;
const MAX_RECENT_FOUND_JOB_IDENTITIES: usize = 128;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FoundValidJob {
    pub id: u64,
    pub observed_at_unix_ts: u64,
    pub template_id: u64,
    pub version: u32,
    pub header_timestamp: u32,
    pub header_nonce: u32,
    pub bitcoin_block_hash_hex: String,
    pub block_header_hex: String,
    pub coinbase_tx_hex: String,
    pub merkle_hashes_hex: Vec<String>,
    pub block_tx_count: u32,
    pub op_return_payload_hex: Option<String>,
    pub rsk_target_hex: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MergeMiningTemplateContext {
    template_id: u64,
    prev_hash: [u8; 32],
    n_bits: u32,
    merkle_path: Vec<[u8; 32]>,
    block_tx_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MergeMiningCoinbaseContext {
    template_id: u64,
    coinbase_tx_prefix: Vec<u8>,
    coinbase_tx_suffix: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitcoinBlockCandidate {
    pub header: Header,
    pub block_hash_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitcoinBlockCandidateValidation {
    MissingTemplateContext,
    InvalidPow(BitcoinBlockCandidate),
    Valid(BitcoinBlockCandidate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FoundJobIdentity {
    block_header_hex: String,
    coinbase_tx_hex: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ValidJobTrackerError {
    MutexCorrupted,
    InvalidCoinbaseTransaction(String),
}

impl std::fmt::Display for ValidJobTrackerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidJobTrackerError::MutexCorrupted => {
                f.write_str("Valid job tracker mutex corrupted")
            }
            ValidJobTrackerError::InvalidCoinbaseTransaction(error) => {
                write!(f, "Invalid coinbase transaction: {error}")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValidJobTracker {
    next_id: Arc<AtomicU64>,
    pending: Arc<Mutex<VecDeque<FoundValidJob>>>,
    template_contexts: Arc<Mutex<VecDeque<MergeMiningTemplateContext>>>,
    template_coinbase_contexts: Arc<Mutex<VecDeque<MergeMiningCoinbaseContext>>>,
    recent_found_job_identities: Arc<Mutex<VecDeque<FoundJobIdentity>>>,
    op_return_injector: OpReturnInjector,
}

impl Default for ValidJobTracker {
    fn default() -> Self {
        Self::new(OpReturnInjector::default())
    }
}

impl ValidJobTracker {
    pub fn new(op_return_injector: OpReturnInjector) -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(0)),
            pending: Arc::new(Mutex::new(VecDeque::new())),
            template_contexts: Arc::new(Mutex::new(VecDeque::new())),
            template_coinbase_contexts: Arc::new(Mutex::new(VecDeque::new())),
            recent_found_job_identities: Arc::new(Mutex::new(VecDeque::new())),
            op_return_injector,
        }
    }

    pub fn cache_template_context(
        &self,
        template_id: u64,
        prev_hash: [u8; 32],
        n_bits: u32,
        merkle_path: Vec<[u8; 32]>,
        block_tx_count: u32,
    ) -> Result<(), ValidJobTrackerError> {
        let context = MergeMiningTemplateContext {
            template_id,
            prev_hash,
            n_bits,
            merkle_path,
            block_tx_count,
        };

        let mut template_contexts = self
            .template_contexts
            .lock()
            .map_err(|_| ValidJobTrackerError::MutexCorrupted)?;

        if let Some(index) = template_contexts
            .iter()
            .position(|cached| cached.template_id == template_id)
        {
            template_contexts.remove(index);
        } else if template_contexts.len() >= MAX_CACHED_TEMPLATE_CONTEXTS {
            template_contexts.pop_front();
        }

        template_contexts.push_back(context);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn cache_template_context_with_coinbase_parts(
        &self,
        template_id: u64,
        prev_hash: [u8; 32],
        n_bits: u32,
        merkle_path: Vec<[u8; 32]>,
        block_tx_count: u32,
        coinbase_tx_prefix: Vec<u8>,
        coinbase_tx_suffix: Vec<u8>,
    ) -> Result<(), ValidJobTrackerError> {
        self.cache_template_context(template_id, prev_hash, n_bits, merkle_path, block_tx_count)?;
        self.cache_template_coinbase_context(template_id, coinbase_tx_prefix, coinbase_tx_suffix)
    }

    fn cache_template_coinbase_context(
        &self,
        template_id: u64,
        coinbase_tx_prefix: Vec<u8>,
        coinbase_tx_suffix: Vec<u8>,
    ) -> Result<(), ValidJobTrackerError> {
        let context = MergeMiningCoinbaseContext {
            template_id,
            coinbase_tx_prefix,
            coinbase_tx_suffix,
        };

        let mut template_coinbase_contexts = self
            .template_coinbase_contexts
            .lock()
            .map_err(|_| ValidJobTrackerError::MutexCorrupted)?;

        if let Some(index) = template_coinbase_contexts
            .iter()
            .position(|cached| cached.template_id == template_id)
        {
            template_coinbase_contexts.remove(index);
        } else if template_coinbase_contexts.len() >= MAX_CACHED_TEMPLATE_COINBASE_CONTEXTS {
            template_coinbase_contexts.pop_front();
        }

        template_coinbase_contexts.push_back(context);
        Ok(())
    }

    pub fn refresh_template_chain_state(
        &self,
        template_id: u64,
        prev_hash: [u8; 32],
        n_bits: u32,
    ) -> Result<bool, ValidJobTrackerError> {
        let mut template_contexts = self
            .template_contexts
            .lock()
            .map_err(|_| ValidJobTrackerError::MutexCorrupted)?;

        let Some(context) = template_contexts
            .iter_mut()
            .rev()
            .find(|cached| cached.template_id == template_id)
        else {
            return Ok(false);
        };

        context.prev_hash = prev_hash;
        context.n_bits = n_bits;
        Ok(true)
    }

    pub fn record_found_job(
        &self,
        template_id: u64,
        version: u32,
        header_timestamp: u32,
        header_nonce: u32,
        coinbase_tx: &[u8],
    ) -> Result<Option<FoundValidJob>, ValidJobTrackerError> {
        let Some(template_context) = self.get_template_context(template_id)? else {
            warn!(
                "Dropping found merge-mining job for template_id={} because merge-mining template context was missing",
                template_id
            );
            return Ok(None);
        };

        let Some(applied_payload) = self
            .op_return_injector
            .applied_payload_for_template(template_id)
        else {
            warn!(
                "Dropping found merge-mining job for template_id={} because no template-scoped OP_RETURN payload metadata was recorded",
                template_id
            );
            return Ok(None);
        };

        if !applied_payload
            .payload_bytes
            .starts_with(RSK_MERGED_MINING_TAG)
        {
            warn!(
                "Dropping found merge-mining job for template_id={} because the recorded OP_RETURN payload does not start with the RSKBLOCK: tag",
                template_id
            );
            return Ok(None);
        }

        let coinbase_transaction: Transaction = deserialize(coinbase_tx)
            .map_err(|error| ValidJobTrackerError::InvalidCoinbaseTransaction(error.to_string()))?;

        if !coinbase_contains_expected_op_return_payload(
            &coinbase_transaction,
            &applied_payload.payload_bytes,
        ) {
            warn!(
                "Dropping found merge-mining job for template_id={} because the serialized coinbase transaction does not contain the expected template-scoped RSK OP_RETURN payload",
                template_id
            );
            return Ok(None);
        }

        let stripped_coinbase_transaction = strip_witness_from_transaction(&coinbase_transaction);
        let stripped_coinbase_tx = serialize(&stripped_coinbase_transaction);
        let coinbase_txid = stripped_coinbase_transaction.compute_txid();
        let candidate = build_bitcoin_block_candidate(
            version,
            header_timestamp,
            header_nonce,
            &template_context,
            coinbase_txid,
        );
        if !candidate
            .header
            .target()
            .is_met_by(candidate.header.block_hash())
        {
            warn!(
                template_id,
                bitcoin_block_hash_hex = %candidate.block_hash_hex,
                n_bits = format_args!("{:#010x}", template_context.n_bits),
                "Dropping found merge-mining job because the reconstructed block header does not satisfy its own nBits target"
            );
            return Ok(None);
        }
        let merkle_hashes_hex = serialize_rskj_merkle_hashes_hex(
            &template_context.merkle_path,
            template_context.block_tx_count,
        );

        let found_job = FoundValidJob {
            id: self.next_id.fetch_add(1, Ordering::Relaxed) + 1,
            observed_at_unix_ts: unix_timestamp_now(),
            template_id,
            version,
            header_timestamp,
            header_nonce,
            bitcoin_block_hash_hex: candidate.block_hash_hex,
            block_header_hex: serialize(&candidate.header).as_hex().to_string(),
            coinbase_tx_hex: stripped_coinbase_tx.as_hex().to_string(),
            merkle_hashes_hex,
            block_tx_count: template_context.block_tx_count,
            op_return_payload_hex: Some(applied_payload.payload_hex),
            rsk_target_hex: applied_payload.rsk_target_hex,
        };

        if !self.enqueue_found_job(found_job.clone())? {
            debug!(
                template_id,
                bitcoin_block_hash_hex = %found_job.bitcoin_block_hash_hex,
                "Dropping duplicate found merge-mining job"
            );
            return Ok(None);
        }
        Ok(Some(found_job))
    }

    pub fn record_rsk_target_share(
        &self,
        template_id: u64,
        version: u32,
        header_timestamp: u32,
        header_nonce: u32,
        full_extranonce: &[u8],
    ) -> Result<Option<FoundValidJob>, ValidJobTrackerError> {
        let Some(template_context) = self.get_template_context(template_id)? else {
            debug!(
                template_id,
                "Skipping RSK-target share because merge-mining template context was missing"
            );
            return Ok(None);
        };

        let Some(coinbase_context) = self.get_template_coinbase_context(template_id)? else {
            debug!(
                template_id,
                "Skipping RSK-target share because coinbase template context was missing"
            );
            return Ok(None);
        };

        let Some(applied_payload) = self
            .op_return_injector
            .applied_payload_for_template(template_id)
        else {
            debug!(
                template_id,
                "Skipping RSK-target share because no template-scoped OP_RETURN payload metadata was recorded"
            );
            return Ok(None);
        };

        if !applied_payload
            .payload_bytes
            .starts_with(RSK_MERGED_MINING_TAG)
        {
            debug!(
                template_id,
                "Skipping RSK-target share because the recorded OP_RETURN payload does not start with the RSKBLOCK: tag"
            );
            return Ok(None);
        }

        let Some(rsk_target) = applied_payload.rsk_target.clone() else {
            debug!(
                template_id,
                "Skipping RSK-target share because the template-scoped RSK target was unavailable"
            );
            return Ok(None);
        };

        let coinbase_tx = build_coinbase_transaction_from_parts(&coinbase_context, full_extranonce);
        let coinbase_transaction: Transaction = match deserialize(&coinbase_tx) {
            Ok(transaction) => transaction,
            Err(error) => {
                warn!(
                    template_id,
                    "Dropping RSK-target share because the reconstructed coinbase transaction was invalid: {error}"
                );
                return Ok(None);
            }
        };

        if !coinbase_contains_expected_op_return_payload(
            &coinbase_transaction,
            &applied_payload.payload_bytes,
        ) {
            warn!(
                template_id,
                "Dropping RSK-target share because the reconstructed coinbase transaction does not contain the expected template-scoped RSK OP_RETURN payload"
            );
            return Ok(None);
        }

        let stripped_coinbase_transaction = strip_witness_from_transaction(&coinbase_transaction);
        let stripped_coinbase_tx = serialize(&stripped_coinbase_transaction);
        let coinbase_txid = stripped_coinbase_transaction.compute_txid();
        let candidate = build_bitcoin_block_candidate(
            version,
            header_timestamp,
            header_nonce,
            &template_context,
            coinbase_txid,
        );
        let candidate_hash: Sv2Target = candidate
            .header
            .block_hash()
            .to_raw_hash()
            .to_byte_array()
            .into();
        if candidate_hash > rsk_target {
            return Ok(None);
        }

        let merkle_hashes_hex = serialize_rskj_merkle_hashes_hex(
            &template_context.merkle_path,
            template_context.block_tx_count,
        );

        let found_job = FoundValidJob {
            id: self.next_id.fetch_add(1, Ordering::Relaxed) + 1,
            observed_at_unix_ts: unix_timestamp_now(),
            template_id,
            version,
            header_timestamp,
            header_nonce,
            bitcoin_block_hash_hex: candidate.block_hash_hex,
            block_header_hex: serialize(&candidate.header).as_hex().to_string(),
            coinbase_tx_hex: stripped_coinbase_tx.as_hex().to_string(),
            merkle_hashes_hex,
            block_tx_count: template_context.block_tx_count,
            op_return_payload_hex: Some(applied_payload.payload_hex),
            rsk_target_hex: applied_payload.rsk_target_hex,
        };

        if !self.enqueue_found_job(found_job.clone())? {
            debug!(
                template_id,
                bitcoin_block_hash_hex = %found_job.bitcoin_block_hash_hex,
                "Dropping duplicate found merge-mining job"
            );
            return Ok(None);
        }
        Ok(Some(found_job))
    }

    pub fn validate_bitcoin_block_candidate(
        &self,
        template_id: u64,
        version: u32,
        header_timestamp: u32,
        header_nonce: u32,
        coinbase_tx: &[u8],
    ) -> Result<BitcoinBlockCandidateValidation, ValidJobTrackerError> {
        let Some(template_context) = self.get_template_context(template_id)? else {
            return Ok(BitcoinBlockCandidateValidation::MissingTemplateContext);
        };

        let coinbase_transaction: Transaction = deserialize(coinbase_tx)
            .map_err(|error| ValidJobTrackerError::InvalidCoinbaseTransaction(error.to_string()))?;
        let candidate = build_bitcoin_block_candidate(
            version,
            header_timestamp,
            header_nonce,
            &template_context,
            coinbase_transaction.compute_txid(),
        );

        if candidate
            .header
            .target()
            .is_met_by(candidate.header.block_hash())
        {
            Ok(BitcoinBlockCandidateValidation::Valid(candidate))
        } else {
            Ok(BitcoinBlockCandidateValidation::InvalidPow(candidate))
        }
    }

    pub fn take_found_job(&self) -> Result<Option<FoundValidJob>, ValidJobTrackerError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ValidJobTrackerError::MutexCorrupted)?;
        Ok(pending.pop_front())
    }

    fn enqueue_found_job(&self, found_job: FoundValidJob) -> Result<bool, ValidJobTrackerError> {
        let identity = FoundJobIdentity {
            block_header_hex: found_job.block_header_hex.clone(),
            coinbase_tx_hex: found_job.coinbase_tx_hex.clone(),
        };

        {
            let mut recent_found_job_identities = self
                .recent_found_job_identities
                .lock()
                .map_err(|_| ValidJobTrackerError::MutexCorrupted)?;
            if recent_found_job_identities
                .iter()
                .any(|cached| cached == &identity)
            {
                return Ok(false);
            }
            if recent_found_job_identities.len() >= MAX_RECENT_FOUND_JOB_IDENTITIES {
                recent_found_job_identities.pop_front();
            }
            recent_found_job_identities.push_back(identity);
        }

        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ValidJobTrackerError::MutexCorrupted)?;
        if pending.len() >= MAX_PENDING_VALID_JOBS {
            pending.pop_front();
        }
        pending.push_back(found_job);
        Ok(true)
    }

    fn get_template_context(
        &self,
        template_id: u64,
    ) -> Result<Option<MergeMiningTemplateContext>, ValidJobTrackerError> {
        let template_contexts = self
            .template_contexts
            .lock()
            .map_err(|_| ValidJobTrackerError::MutexCorrupted)?;
        Ok(template_contexts
            .iter()
            .rev()
            .find(|cached| cached.template_id == template_id)
            .cloned())
    }

    fn get_template_coinbase_context(
        &self,
        template_id: u64,
    ) -> Result<Option<MergeMiningCoinbaseContext>, ValidJobTrackerError> {
        let template_coinbase_contexts = self
            .template_coinbase_contexts
            .lock()
            .map_err(|_| ValidJobTrackerError::MutexCorrupted)?;
        Ok(template_coinbase_contexts
            .iter()
            .rev()
            .find(|cached| cached.template_id == template_id)
            .cloned())
    }
}

fn strip_witness_from_transaction(transaction: &Transaction) -> Transaction {
    let mut stripped = transaction.clone();
    stripped
        .input
        .iter_mut()
        .for_each(|input| input.witness.clear());
    stripped
}

fn build_coinbase_transaction_from_parts(
    coinbase_context: &MergeMiningCoinbaseContext,
    full_extranonce: &[u8],
) -> Vec<u8> {
    [
        coinbase_context.coinbase_tx_prefix.as_slice(),
        full_extranonce,
        coinbase_context.coinbase_tx_suffix.as_slice(),
    ]
    .concat()
}

fn build_block_header(
    version: u32,
    header_timestamp: u32,
    header_nonce: u32,
    template_context: &MergeMiningTemplateContext,
    coinbase_txid: Txid,
) -> Header {
    let merkle_root = merkle_root_from_path_(
        coinbase_txid.to_raw_hash().to_byte_array(),
        &template_context.merkle_path,
    );

    Header {
        version: Version::from_consensus(version as i32),
        // Template-distribution `SetNewPrevHash.prev_hash` is already specified in the exact byte
        // order used inside the next Bitcoin header, so this must stay a direct wrap, not a
        // display-order reversal.
        prev_blockhash: BlockHash::from_byte_array(template_context.prev_hash),
        merkle_root: TxMerkleNode::from_byte_array(merkle_root),
        time: header_timestamp,
        bits: CompactTarget::from_consensus(template_context.n_bits),
        nonce: header_nonce,
    }
}

fn build_bitcoin_block_candidate(
    version: u32,
    header_timestamp: u32,
    header_nonce: u32,
    template_context: &MergeMiningTemplateContext,
    coinbase_txid: Txid,
) -> BitcoinBlockCandidate {
    let header = build_block_header(
        version,
        header_timestamp,
        header_nonce,
        template_context,
        coinbase_txid,
    );
    BitcoinBlockCandidate {
        block_hash_hex: header.block_hash().to_string(),
        header,
    }
}

// The raw bytes used locally to build the Bitcoin header merkle root are not the same strings RskJ
// expects on the wire. `template_context.merkle_path` is already the SV2 bottom-up sibling path in
// the raw byte order consumed by `merkle_root_from_path_`, so local header reconstruction keeps it
// unchanged. For the RSKIP92 partial-merkle RPC we must instead submit only those sibling hashes,
// still in bottom-up order but hex-encoded in display order because RskJ reverses every submitted
// hash after decoding it. Single-transaction blocks bypass the partial-merkle RPC entirely in the
// sibling bridge, so they must emit an empty list.
fn serialize_rskj_merkle_hashes_hex(merkle_path: &[[u8; 32]], block_tx_count: u32) -> Vec<String> {
    if block_tx_count <= 1 {
        return Vec::new();
    }

    merkle_path
        .iter()
        .copied()
        .map(serialize_rskj_wire_hash_hex)
        .collect()
}

fn serialize_rskj_wire_hash_hex(raw_hash: [u8; 32]) -> String {
    raw_hash
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .as_hex()
        .to_string()
}

pub(crate) fn coinbase_contains_expected_op_return_payload(
    coinbase_transaction: &Transaction,
    expected_payload: &[u8],
) -> bool {
    coinbase_transaction
        .output
        .iter()
        .filter_map(|output| extract_single_op_return_pushdata(&output.script_pubkey))
        .any(|payload| payload == expected_payload)
}

fn extract_single_op_return_pushdata(script_pubkey: &bitcoin::ScriptBuf) -> Option<Vec<u8>> {
    if !script_pubkey.is_op_return() {
        return None;
    }

    let mut instructions = script_pubkey.instructions();
    match instructions.next()? {
        Ok(Instruction::Op(op)) if op == OP_RETURN => {}
        _ => return None,
    }

    let payload = match instructions.next()? {
        Ok(Instruction::PushBytes(bytes)) => bytes.as_bytes().to_vec(),
        _ => return None,
    };

    if instructions.next().is_some() {
        return None;
    }

    Some(payload)
}

fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
fn last_index_of_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }

    haystack
        .windows(needle.len())
        .enumerate()
        .rev()
        .find_map(|(index, window)| (window == needle).then_some(index))
}

#[cfg(test)]
fn validate_coinbase_submission_against_rskj_contract_for_tests(
    found_job: &FoundValidJob,
    coinbase_tx_bytes: &[u8],
    coinbase_tx: &Transaction,
) -> Result<(), String> {
    if coinbase_tx_bytes.len() <= 64 {
        return Err(format!(
            "submitted coinbase must be longer than 64 bytes for RskJ compression, got {} bytes",
            coinbase_tx_bytes.len()
        ));
    }

    if let Some(payload_hex) = found_job.op_return_payload_hex.as_ref() {
        let expected_payload = Vec::<u8>::from_hex(payload_hex)
            .map_err(|error| format!("invalid op_return_payload_hex: {error}"))?;
        let expected_payload_pos = last_index_of_subslice(coinbase_tx_bytes, &expected_payload)
            .ok_or_else(|| {
                "submitted coinbase bytes do not contain the expected template-scoped RSK payload"
                    .to_string()
            })?;
        let last_rsk_tag_pos = last_index_of_subslice(coinbase_tx_bytes, RSK_MERGED_MINING_TAG)
            .ok_or_else(|| {
                "submitted coinbase bytes do not contain the RSKBLOCK: tag".to_string()
            })?;
        if expected_payload_pos != last_rsk_tag_pos {
            return Err(
                "submitted coinbase bytes contain another RSKBLOCK: tag after the expected payload"
                    .to_string(),
            );
        }
    }

    // RskJ hashes the submitted coinbase bytes themselves (after a compression step that preserves
    // the effective double-SHA256). For segwit coinbases this means the wire bytes must already be
    // in stripped-txid form, otherwise RskJ will derive the wtxid while the block header merkle
    // root still commits to the txid.
    let submitted_coinbase_hash = sha256d::Hash::hash(coinbase_tx_bytes).to_byte_array();
    let txid_hash = coinbase_tx.compute_txid().to_raw_hash().to_byte_array();
    if submitted_coinbase_hash != txid_hash {
        return Err(
            "submitted coinbase bytes hash to a different identifier than the coinbase txid; witness bytes are still present on the wire"
                .to_string(),
        );
    }

    Ok(())
}

#[cfg(test)]
fn build_bridge_coinbase_only_raw_block_bytes_for_tests(
    found_job: &FoundValidJob,
) -> Result<Vec<u8>, String> {
    if found_job.block_tx_count != 1 {
        return Err(format!(
            "bridge raw-block helper only supports block_tx_count == 1, got {}",
            found_job.block_tx_count
        ));
    }

    let header_bytes = Vec::<u8>::from_hex(&found_job.block_header_hex)
        .map_err(|error| format!("invalid block_header_hex: {error}"))?;
    let coinbase_tx_bytes = Vec::<u8>::from_hex(&found_job.coinbase_tx_hex)
        .map_err(|error| format!("invalid coinbase_tx_hex: {error}"))?;

    let mut raw_block = Vec::with_capacity(header_bytes.len() + 1 + coinbase_tx_bytes.len());
    raw_block.extend_from_slice(&header_bytes);
    bitcoin::consensus::encode::VarInt(1)
        .consensus_encode(&mut raw_block)
        .expect("Vec<u8> should not fail to encode");
    raw_block.extend_from_slice(&coinbase_tx_bytes);
    Ok(raw_block)
}

#[cfg(test)]
fn merkle_tree_height(block_tx_count: u32) -> usize {
    assert!(block_tx_count > 0, "block_tx_count must be non-zero");

    let mut height = 0usize;
    let mut width = block_tx_count as usize;
    while width > 1 {
        width = width.div_ceil(2);
        height += 1;
    }
    height
}

#[cfg(test)]
fn decode_rskj_wire_hash_hex_via_rskip92_builder(wire_hex: &str) -> Result<[u8; 32], String> {
    decode_rskj_wire_hash_hex_with_reason(
        wire_hex,
        "Rskip92MerkleProofBuilder uses Utils.reverseBytes(Hex.decode(mh))",
    )
}

#[cfg(test)]
fn decode_rskj_wire_hash_hex_with_reason(wire_hex: &str, reason: &str) -> Result<[u8; 32], String> {
    let mut wire_bytes = Vec::<u8>::from_hex(wire_hex)
        .map_err(|error| format!("invalid merkle hash hex ({reason}): {error}"))?;
    if wire_bytes.len() != 32 {
        return Err(format!(
            "invalid merkle hash hex ({reason}): expected 32 bytes but got {}",
            wire_bytes.len()
        ));
    }
    wire_bytes.reverse();
    wire_bytes
        .try_into()
        .map_err(|_| format!("invalid merkle hash hex ({reason}): failed to convert to 32 bytes"))
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ExpectedHeaderFieldsForTests {
    pub prev_hash: Option<[u8; 32]>,
    pub version: Option<u32>,
    pub time: Option<u32>,
    pub n_bits: Option<u32>,
    pub nonce: Option<u32>,
}

#[cfg(test)]
pub(crate) fn validate_found_job_against_rskj_contract_for_tests(
    found_job: &FoundValidJob,
    expected_header_fields: Option<ExpectedHeaderFieldsForTests>,
) -> Result<(), String> {
    let block_header_bytes = Vec::<u8>::from_hex(&found_job.block_header_hex)
        .map_err(|error| format!("invalid block_header_hex: {error}"))?;
    if block_header_bytes.len() != 80 {
        return Err(format!(
            "block_header_hex must serialize exactly one 80-byte Bitcoin header, got {} bytes",
            block_header_bytes.len()
        ));
    }
    let block_header: Header = deserialize(&block_header_bytes)
        .map_err(|error| format!("invalid block_header_hex: {error}"))?;
    if block_header.block_hash().to_string() != found_job.bitcoin_block_hash_hex {
        return Err(format!(
            "bitcoin_block_hash_hex {} does not match the serialized block header hash {}",
            found_job.bitcoin_block_hash_hex,
            block_header.block_hash()
        ));
    }
    if let Some(expected) = expected_header_fields {
        if let Some(version) = expected.version {
            let actual_version = block_header.version.to_consensus() as u32;
            if actual_version != version {
                return Err(format!(
                    "block header version {actual_version} did not match expected {version}"
                ));
            }
            if &block_header_bytes[0..4] != version.to_le_bytes().as_slice() {
                return Err("serialized block header version bytes did not match expected little-endian bytes".to_string());
            }
        }
        if let Some(prev_hash) = expected.prev_hash {
            if block_header.prev_blockhash.to_byte_array() != prev_hash {
                return Err(format!(
                    "block header prev_hash {} did not match expected {}",
                    block_header.prev_blockhash,
                    serialize_rskj_wire_hash_hex(prev_hash)
                ));
            }
            if &block_header_bytes[4..36] != prev_hash.as_slice() {
                return Err(
                    "serialized block header prev_hash bytes did not match expected raw bytes"
                        .to_string(),
                );
            }
        }
        if let Some(time) = expected.time {
            if block_header.time != time {
                return Err(format!(
                    "block header time {} did not match expected {}",
                    block_header.time, time
                ));
            }
            if &block_header_bytes[68..72] != time.to_le_bytes().as_slice() {
                return Err(
                    "serialized block header time bytes did not match expected little-endian bytes"
                        .to_string(),
                );
            }
        }
        if let Some(n_bits) = expected.n_bits {
            if block_header.bits.to_consensus() != n_bits {
                return Err(format!(
                    "block header bits {} did not match expected {}",
                    block_header.bits.to_consensus(),
                    n_bits
                ));
            }
            if &block_header_bytes[72..76] != n_bits.to_le_bytes().as_slice() {
                return Err("serialized block header nBits bytes did not match expected little-endian bytes".to_string());
            }
        }
        if let Some(nonce) = expected.nonce {
            if block_header.nonce != nonce {
                return Err(format!(
                    "block header nonce {} did not match expected {}",
                    block_header.nonce, nonce
                ));
            }
            if &block_header_bytes[76..80] != nonce.to_le_bytes().as_slice() {
                return Err("serialized block header nonce bytes did not match expected little-endian bytes".to_string());
            }
        }
    }

    let coinbase_tx_bytes = Vec::<u8>::from_hex(&found_job.coinbase_tx_hex)
        .map_err(|error| format!("invalid coinbase_tx_hex: {error}"))?;
    let coinbase_tx: Transaction = deserialize(&coinbase_tx_bytes)
        .map_err(|error| format!("invalid coinbase_tx_hex: {error}"))?;
    validate_coinbase_submission_against_rskj_contract_for_tests(
        found_job,
        &coinbase_tx_bytes,
        &coinbase_tx,
    )?;
    let coinbase_txid_raw = coinbase_tx.compute_txid().to_raw_hash().to_byte_array();
    let header_merkle_root = block_header.merkle_root.to_raw_hash().to_byte_array();

    if found_job.block_tx_count == 1 {
        if !found_job.merkle_hashes_hex.is_empty() {
            return Err(format!(
                "single-transaction found jobs must emit an empty merkle_hashes_hex list, got {} entries",
                found_job.merkle_hashes_hex.len()
            ));
        }
        if header_merkle_root != coinbase_txid_raw {
            return Err(
                "single-transaction block header merkle root does not match the coinbase txid"
                    .to_string(),
            );
        }
        let raw_block_bytes = build_bridge_coinbase_only_raw_block_bytes_for_tests(found_job)?;
        let raw_block: bitcoin::Block = deserialize(&raw_block_bytes)
            .map_err(|error| format!("bridge-style raw block did not deserialize: {error}"))?;
        if raw_block.txdata.len() != 1 {
            return Err(format!(
                "bridge-style raw block should contain exactly one transaction, got {}",
                raw_block.txdata.len()
            ));
        }
        if raw_block.header != block_header {
            return Err(
                "bridge-style raw block header does not match the submitted block_header_hex"
                    .to_string(),
            );
        }
        if serialize(&raw_block.txdata[0]) != coinbase_tx_bytes {
            return Err(
                "bridge-style raw block coinbase transaction does not match coinbase_tx_hex"
                    .to_string(),
            );
        }
        return Ok(());
    }

    if found_job.merkle_hashes_hex.is_empty() {
        return Err(format!(
            "multi-transaction found job template {} emitted an empty merkle_hashes_hex list",
            found_job.template_id
        ));
    }

    let rskip92_hashes: Vec<[u8; 32]> = found_job
        .merkle_hashes_hex
        .iter()
        .map(|hash| decode_rskj_wire_hash_hex_via_rskip92_builder(hash))
        .collect::<Result<_, _>>()?;

    let expected_hash_count = merkle_tree_height(found_job.block_tx_count);
    if rskip92_hashes.len() != expected_hash_count {
        return Err(format!(
            "submitted merkle_hashes_hex length {} does not match the expected RskJ branch size {} for block_tx_count={}",
            rskip92_hashes.len(),
            expected_hash_count,
            found_job.block_tx_count
        ));
    }
    let rskip92_root = merkle_root_from_path_(coinbase_txid_raw, &rskip92_hashes);
    if rskip92_root != header_merkle_root {
        return Err(
            "RSKIP92-style RskJ merkle reconstruction does not match the block header merkle root"
                .to_string(),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_sv2::{Seq0255, B0255, B064K, U256};
    use bitcoin::{
        absolute::LockTime, hashes::sha256d, hex::FromHex, script::PushBytesBuf, Amount, OutPoint,
        ScriptBuf, Sequence, TxIn, TxOut, Witness,
    };
    use roles_logic_sv2::template_distribution_sv2::NewTemplate;
    use std::convert::TryInto;

    const EASY_TEST_N_BITS: u32 = 0x207f_ffff;

    fn make_tracker() -> (OpReturnInjector, ValidJobTracker) {
        let injector = OpReturnInjector::default();
        let tracker = ValidJobTracker::new(injector.clone());
        (injector, tracker)
    }

    fn make_new_template(template_id: u64) -> NewTemplate<'static> {
        let coinbase_prefix: B0255<'static> = Vec::new()
            .try_into()
            .expect("coinbase prefix should fit in B0255");
        let coinbase_tx_outputs: B064K<'static> = Vec::new()
            .try_into()
            .expect("coinbase outputs should fit in B064K");
        let merkle_path: Seq0255<'static, U256<'static>> = Vec::new().into();
        NewTemplate {
            template_id,
            future_template: false,
            version: 0,
            coinbase_tx_version: 1,
            coinbase_prefix,
            coinbase_tx_input_sequence: 0,
            coinbase_tx_value_remaining: 0,
            coinbase_tx_outputs_count: 0,
            coinbase_tx_outputs,
            coinbase_tx_locktime: 0,
            merkle_path,
        }
    }

    async fn apply_payload_to_template(
        injector: &OpReturnInjector,
        template_id: u64,
        payload_hex: &str,
    ) {
        let mut template = make_new_template(template_id);
        injector
            .queue_from_hex(payload_hex)
            .await
            .expect("payload should queue");
        injector
            .apply_pending_to_template(&mut template)
            .await
            .expect("payload should apply")
            .expect("template should receive payload");
    }

    async fn apply_payload_with_rsk_target_to_template(
        injector: &OpReturnInjector,
        template_id: u64,
        payload_hex: &str,
        rsk_target_hex: &str,
    ) {
        let mut template = make_new_template(template_id);
        injector
            .queue_from_hex_with_rsk_target(payload_hex, Some(rsk_target_hex))
            .await
            .expect("payload should queue");
        injector
            .apply_pending_to_template(&mut template)
            .await
            .expect("payload should apply")
            .expect("template should receive payload");
    }

    fn rsk_payload_hex(suffix_hex: &str) -> String {
        format!("52534b424c4f434b3a{suffix_hex}")
    }

    fn make_coinbase_transaction(tag: u8, payload: Option<&[u8]>) -> Transaction {
        let mut output = vec![TxOut {
            value: Amount::from_sat(5_000_000_000 - tag as u64),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }];

        if let Some(payload) = payload {
            output.push(TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::new_op_return(
                    PushBytesBuf::try_from(payload.to_vec()).expect("payload should fit"),
                ),
            });
        }

        Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: vec![0x03, tag, 0x51].into(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output,
        }
    }

    fn make_coinbase_transaction_with_extranonce(
        tag: u8,
        extranonce: &[u8],
        payload: Option<&[u8]>,
    ) -> Transaction {
        let mut transaction = make_coinbase_transaction(tag, payload);
        transaction.input[0].script_sig = ScriptBuf::from_bytes(
            [vec![0x03, tag, 0x51], extranonce.to_vec(), vec![0x51]].concat(),
        );
        transaction
    }

    fn make_segwit_coinbase_transaction(tag: u8, payload: Option<&[u8]>) -> Transaction {
        let mut transaction = make_coinbase_transaction(tag, payload);
        transaction.input[0].witness = Witness::from_slice(&[vec![tag; 32]]);
        transaction
    }

    fn make_transaction(tag: u8) -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version(2),
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_raw_hash(sha256d::Hash::hash(&[tag; 32])),
                    vout: tag as u32,
                },
                script_sig: vec![tag, tag.wrapping_add(1), tag.wrapping_add(2)].into(),
                sequence: Sequence(tag as u32),
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1_000 + tag as u64),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51, tag]),
            }],
        }
    }

    fn build_merkle_path_for_coinbase(txids: &[Txid]) -> (Vec<[u8; 32]>, [u8; 32]) {
        assert!(
            !txids.is_empty(),
            "at least the coinbase transaction must be present"
        );

        let mut path = Vec::new();
        let mut index = 0usize;
        let mut level: Vec<[u8; 32]> = txids
            .iter()
            .map(|txid| txid.to_raw_hash().to_byte_array())
            .collect();

        while level.len() > 1 {
            let sibling_index = if index % 2 == 0 {
                if index + 1 < level.len() {
                    index + 1
                } else {
                    index
                }
            } else {
                index - 1
            };
            path.push(level[sibling_index]);

            let mut next_level = Vec::with_capacity(level.len().div_ceil(2));
            for pair in level.chunks(2) {
                let left = pair[0];
                let right = if pair.len() == 2 { pair[1] } else { pair[0] };
                let mut combined = Vec::with_capacity(64);
                combined.extend_from_slice(&left);
                combined.extend_from_slice(&right);
                next_level.push(sha256d::Hash::hash(&combined).to_byte_array());
            }

            level = next_level;
            index /= 2;
        }

        (path, level[0])
    }

    fn decode_found_job_header(found_job: &FoundValidJob) -> Header {
        let header_bytes =
            Vec::<u8>::from_hex(&found_job.block_header_hex).expect("header hex should decode");
        deserialize(&header_bytes).expect("header bytes should decode into a block header")
    }

    fn non_symmetric_prev_hash(start: u8) -> [u8; 32] {
        (start..start.saturating_add(32))
            .collect::<Vec<_>>()
            .try_into()
            .expect("expected exactly 32 bytes")
    }

    fn find_nonce_for_template_context(
        template_context: &MergeMiningTemplateContext,
        version: u32,
        header_timestamp: u32,
        coinbase_txid: Txid,
        should_meet_pow: bool,
    ) -> u32 {
        for nonce in 0..=u32::MAX {
            let candidate = build_bitcoin_block_candidate(
                version,
                header_timestamp,
                nonce,
                template_context,
                coinbase_txid,
            );
            let meets_pow = candidate
                .header
                .target()
                .is_met_by(candidate.header.block_hash());
            if meets_pow == should_meet_pow {
                return nonce;
            }
        }

        panic!("failed to find nonce with should_meet_pow={should_meet_pow}");
    }

    fn find_nonce_for_coinbase(
        template_context: &MergeMiningTemplateContext,
        version: u32,
        header_timestamp: u32,
        coinbase: &Transaction,
        should_meet_pow: bool,
    ) -> u32 {
        find_nonce_for_template_context(
            template_context,
            version,
            header_timestamp,
            coinbase.compute_txid(),
            should_meet_pow,
        )
    }

    fn split_serialized_coinbase_around_extranonce(
        serialized_coinbase: &[u8],
        extranonce: &[u8],
    ) -> (Vec<u8>, Vec<u8>) {
        let extranonce_pos = last_index_of_subslice(serialized_coinbase, extranonce)
            .expect("serialized coinbase should contain the extranonce bytes");
        (
            serialized_coinbase[..extranonce_pos].to_vec(),
            serialized_coinbase[extranonce_pos + extranonce.len()..].to_vec(),
        )
    }

    fn rsk_target_hex_for_block_hash(block_hash: BlockHash) -> String {
        let mut target_bytes = block_hash.to_byte_array().to_vec();
        target_bytes.reverse();
        target_bytes.as_hex().to_string()
    }

    #[tokio::test]
    async fn payload_applied_to_template_a_stays_associated_after_template_b_gets_new_payload() {
        let (injector, tracker) = make_tracker();
        let payload_a_hex = rsk_payload_hex("aaaaaaaa");
        let payload_b_hex = rsk_payload_hex("bbbbbbbb");
        let payload_a = Vec::from_hex(&payload_a_hex).expect("payload A should decode");
        let payload_b = Vec::from_hex(&payload_b_hex).expect("payload B should decode");
        let template_a_context = MergeMiningTemplateContext {
            template_id: 11,
            prev_hash: [1; 32],
            n_bits: EASY_TEST_N_BITS,
            merkle_path: vec![],
            block_tx_count: 1,
        };
        let template_b_context = MergeMiningTemplateContext {
            template_id: 22,
            prev_hash: [2; 32],
            n_bits: EASY_TEST_N_BITS,
            merkle_path: vec![],
            block_tx_count: 1,
        };
        let coinbase_a_tx = make_coinbase_transaction(1, Some(&payload_a));
        let coinbase_b_tx = make_coinbase_transaction(2, Some(&payload_b));
        let nonce_a = find_nonce_for_coinbase(&template_a_context, 22, 33, &coinbase_a_tx, true);
        let nonce_b = find_nonce_for_coinbase(&template_b_context, 23, 34, &coinbase_b_tx, true);

        apply_payload_to_template(&injector, 11, &payload_a_hex).await;
        tracker
            .cache_template_context(11, [1; 32], EASY_TEST_N_BITS, vec![], 1)
            .expect("tracker should cache template A context");

        apply_payload_to_template(&injector, 22, &payload_b_hex).await;
        tracker
            .cache_template_context(22, [2; 32], EASY_TEST_N_BITS, vec![], 1)
            .expect("tracker should cache template B context");

        let coinbase_a = serialize(&coinbase_a_tx);
        let coinbase_b = serialize(&coinbase_b_tx);

        let found_job_a = tracker
            .record_found_job(11, 22, 33, nonce_a, &coinbase_a)
            .expect("tracker should record template A event")
            .expect("template A should remain associated with payload A");
        let found_job_b = tracker
            .record_found_job(22, 23, 34, nonce_b, &coinbase_b)
            .expect("tracker should record template B event")
            .expect("template B should remain associated with payload B");

        assert_eq!(found_job_a.op_return_payload_hex, Some(payload_a_hex));
        assert_eq!(found_job_b.op_return_payload_hex, Some(payload_b_hex));
    }

    #[tokio::test]
    async fn take_found_job_returns_oldest_pending_event() {
        let (injector, tracker) = make_tracker();
        let payload_a_hex = rsk_payload_hex("01");
        let payload_b_hex = rsk_payload_hex("02");
        let payload_a = Vec::from_hex(&payload_a_hex).expect("payload A should decode");
        let payload_b = Vec::from_hex(&payload_b_hex).expect("payload B should decode");
        let template_11_context = MergeMiningTemplateContext {
            template_id: 11,
            prev_hash: [1; 32],
            n_bits: EASY_TEST_N_BITS,
            merkle_path: vec![],
            block_tx_count: 1,
        };
        let template_12_context = MergeMiningTemplateContext {
            template_id: 12,
            prev_hash: [2; 32],
            n_bits: EASY_TEST_N_BITS,
            merkle_path: vec![],
            block_tx_count: 1,
        };
        let coinbase_11_tx = make_coinbase_transaction(1, Some(&payload_a));
        let coinbase_12_tx = make_coinbase_transaction(2, Some(&payload_b));
        let nonce_11 = find_nonce_for_coinbase(&template_11_context, 22, 33, &coinbase_11_tx, true);
        let nonce_12 = find_nonce_for_coinbase(&template_12_context, 23, 34, &coinbase_12_tx, true);

        apply_payload_to_template(&injector, 11, &payload_a_hex).await;
        apply_payload_to_template(&injector, 12, &payload_b_hex).await;
        tracker
            .cache_template_context(11, [1; 32], EASY_TEST_N_BITS, vec![], 1)
            .expect("tracker should cache template 11");
        tracker
            .cache_template_context(12, [2; 32], EASY_TEST_N_BITS, vec![], 1)
            .expect("tracker should cache template 12");

        let first = tracker
            .record_found_job(11, 22, 33, nonce_11, &serialize(&coinbase_11_tx))
            .expect("tracker should record first event")
            .expect("template context should exist");
        let second = tracker
            .record_found_job(12, 23, 34, nonce_12, &serialize(&coinbase_12_tx))
            .expect("tracker should record second event")
            .expect("template context should exist");

        assert_eq!(
            tracker.take_found_job().expect("tracker should read queue"),
            Some(first)
        );
        assert_eq!(
            tracker.take_found_job().expect("tracker should read queue"),
            Some(second)
        );
        assert_eq!(
            tracker.take_found_job().expect("tracker should read queue"),
            None
        );
    }

    #[tokio::test]
    async fn record_found_job_builds_coinbase_only_submission_for_single_tx_block() {
        let (injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("01020304");
        let payload = Vec::from_hex(&payload_hex).expect("payload should decode");
        let prev_hash = non_symmetric_prev_hash(0x10);
        let version = 0x2000_0000;
        let header_timestamp = 1_700_000_000;
        let n_bits = EASY_TEST_N_BITS;

        apply_payload_to_template(&injector, 7, &payload_hex).await;
        tracker
            .cache_template_context(7, prev_hash, n_bits, vec![], 1)
            .expect("tracker should cache template context");

        let coinbase_tx = make_coinbase_transaction(7, Some(&payload));
        let header_nonce = find_nonce_for_coinbase(
            &MergeMiningTemplateContext {
                template_id: 7,
                prev_hash,
                n_bits,
                merkle_path: vec![],
                block_tx_count: 1,
            },
            version,
            header_timestamp,
            &coinbase_tx,
            true,
        );
        let coinbase_bytes = serialize(&coinbase_tx);
        let found_job = tracker
            .record_found_job(7, version, header_timestamp, header_nonce, &coinbase_bytes)
            .expect("tracker should build event")
            .expect("template context should exist");

        assert_eq!(found_job.block_tx_count, 1);
        assert!(found_job.merkle_hashes_hex.is_empty());
        assert_eq!(found_job.op_return_payload_hex, Some(payload_hex.clone()));
        validate_found_job_against_rskj_contract_for_tests(
            &found_job,
            Some(ExpectedHeaderFieldsForTests {
                prev_hash: Some(prev_hash),
                version: Some(version),
                time: Some(header_timestamp),
                n_bits: Some(n_bits),
                nonce: Some(header_nonce),
            }),
        )
        .expect("single-tx found job should satisfy the bridge/RskJ contract");
        let coinbase_tx: Transaction =
            deserialize(&coinbase_bytes).expect("coinbase bytes should decode");
        assert!(
            coinbase_contains_expected_op_return_payload(&coinbase_tx, &payload),
            "single-tx coinbase must still contain the injected RSK payload"
        );
    }

    #[tokio::test]
    async fn record_found_job_exports_stripped_coinbase_for_single_tx_segwit_block() {
        let (injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("11223344");
        let payload = Vec::from_hex(&payload_hex).expect("payload should decode");
        let prev_hash = non_symmetric_prev_hash(0x30);
        let version = 0x2000_0000;
        let header_timestamp = 1_700_000_321;
        let n_bits = EASY_TEST_N_BITS;

        apply_payload_to_template(&injector, 17, &payload_hex).await;
        tracker
            .cache_template_context(17, prev_hash, n_bits, vec![], 1)
            .expect("tracker should cache template context");

        let solved_coinbase = make_segwit_coinbase_transaction(17, Some(&payload));
        let header_nonce = find_nonce_for_coinbase(
            &MergeMiningTemplateContext {
                template_id: 17,
                prev_hash,
                n_bits,
                merkle_path: vec![],
                block_tx_count: 1,
            },
            version,
            header_timestamp,
            &solved_coinbase,
            true,
        );
        let solved_coinbase_bytes = serialize(&solved_coinbase);
        let found_job = tracker
            .record_found_job(
                17,
                version,
                header_timestamp,
                header_nonce,
                &solved_coinbase_bytes,
            )
            .expect("tracker should build event")
            .expect("template context should exist");

        assert_ne!(
            found_job.coinbase_tx_hex,
            solved_coinbase_bytes.as_hex().to_string(),
            "RskJ-facing coinbase export must not keep witness bytes",
        );
        let exported_coinbase_bytes =
            Vec::<u8>::from_hex(&found_job.coinbase_tx_hex).expect("coinbase hex should decode");
        let exported_coinbase: Transaction =
            deserialize(&exported_coinbase_bytes).expect("coinbase should decode");
        assert!(
            exported_coinbase.input[0].witness.is_empty(),
            "found-job coinbase export must be witness-stripped",
        );
        validate_found_job_against_rskj_contract_for_tests(
            &found_job,
            Some(ExpectedHeaderFieldsForTests {
                prev_hash: Some(prev_hash),
                version: Some(version),
                time: Some(header_timestamp),
                n_bits: Some(n_bits),
                nonce: Some(header_nonce),
            }),
        )
        .expect("segwit single-tx found job should satisfy the bridge/RskJ contract");
    }

    #[tokio::test]
    async fn record_found_job_uses_latest_refreshed_prev_hash_and_bits_in_serialized_header() {
        let (injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("0badc0de");
        let payload = Vec::from_hex(&payload_hex).expect("payload should decode");
        let stale_prev_hash = [0xaa; 32];
        let refreshed_prev_hash = non_symmetric_prev_hash(0x00);
        let refreshed_n_bits = EASY_TEST_N_BITS;
        let version = 0x2000_0000;
        let header_timestamp = 1_700_000_123;

        apply_payload_to_template(&injector, 99, &payload_hex).await;
        tracker
            .cache_template_context(99, stale_prev_hash, 0x1d00ffff, vec![], 1)
            .expect("tracker should cache template context");
        assert!(
            tracker
                .refresh_template_chain_state(99, refreshed_prev_hash, refreshed_n_bits)
                .expect("tracker should refresh template context"),
            "expected the cached template context to refresh"
        );

        let coinbase_tx = make_coinbase_transaction(9, Some(&payload));
        let header_nonce = find_nonce_for_coinbase(
            &MergeMiningTemplateContext {
                template_id: 99,
                prev_hash: refreshed_prev_hash,
                n_bits: refreshed_n_bits,
                merkle_path: vec![],
                block_tx_count: 1,
            },
            version,
            header_timestamp,
            &coinbase_tx,
            true,
        );
        let coinbase_bytes = serialize(&coinbase_tx);
        let found_job = tracker
            .record_found_job(99, version, header_timestamp, header_nonce, &coinbase_bytes)
            .expect("tracker should build event")
            .expect("template context should exist");

        let header = decode_found_job_header(&found_job);
        let header_bytes =
            Vec::<u8>::from_hex(&found_job.block_header_hex).expect("header hex should decode");

        assert_eq!(
            header.prev_blockhash.to_byte_array(),
            refreshed_prev_hash,
            "the serialized header must use the latest SetNewPrevHash bytes, not a stale snapshot",
        );
        assert_ne!(
            header.prev_blockhash.to_byte_array(),
            stale_prev_hash,
            "a stale prev_hash snapshot would build the wrong Bitcoin header",
        );
        assert_eq!(
            &header_bytes[4..36],
            refreshed_prev_hash.as_slice(),
            "Bitcoin headers serialize prev_hash exactly as cached internal bytes",
        );
        assert_eq!(header.bits.to_consensus(), refreshed_n_bits);
        validate_found_job_against_rskj_contract_for_tests(
            &found_job,
            Some(ExpectedHeaderFieldsForTests {
                prev_hash: Some(refreshed_prev_hash),
                version: Some(version),
                time: Some(header_timestamp),
                n_bits: Some(refreshed_n_bits),
                nonce: Some(header_nonce),
            }),
        )
        .expect("refreshed single-tx found job should satisfy the bridge/RskJ contract");
    }

    #[test]
    fn record_found_job_returns_none_when_template_payload_metadata_is_missing() {
        let (_injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("deadbeef");
        let payload = Vec::from_hex(&payload_hex).expect("payload should decode");
        let coinbase = serialize(&make_coinbase_transaction(1, Some(&payload)));

        tracker
            .cache_template_context(404, [4; 32], 0x1d00ffff, vec![], 1)
            .expect("tracker should cache template context");

        assert_eq!(
            tracker
                .record_found_job(404, 2, 3, 4, &coinbase)
                .expect("tracker should handle missing payload metadata"),
            None
        );
        assert_eq!(
            tracker.take_found_job().expect("tracker should read queue"),
            None
        );
    }

    #[tokio::test]
    async fn record_found_job_returns_none_when_coinbase_does_not_contain_expected_rsk_payload() {
        let (injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("cafebabe");

        apply_payload_to_template(&injector, 77, &payload_hex).await;
        tracker
            .cache_template_context(77, [7; 32], 0x1d00ffff, vec![], 1)
            .expect("tracker should cache template context");

        let coinbase_without_payload = serialize(&make_coinbase_transaction(7, None));

        assert_eq!(
            tracker
                .record_found_job(77, 2, 3, 4, &coinbase_without_payload)
                .expect("tracker should reject mismatched coinbase"),
            None
        );
        assert_eq!(
            tracker.take_found_job().expect("tracker should read queue"),
            None
        );
    }

    #[tokio::test]
    async fn record_found_job_builds_rskj_compatible_partial_merkle_payload() {
        let (injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("00112233");
        let payload = Vec::from_hex(&payload_hex).expect("payload should decode");
        let refreshed_prev_hash = non_symmetric_prev_hash(0x20);
        let refreshed_n_bits = EASY_TEST_N_BITS;
        let version = 0x2000_0000;
        let header_timestamp = 1_700_000_000;

        apply_payload_to_template(&injector, 77, &payload_hex).await;

        let coinbase = make_coinbase_transaction(9, Some(&payload));
        let tx1 = make_transaction(1);
        let tx2 = make_transaction(2);
        let tx3 = make_transaction(3);
        let txids = vec![
            coinbase.compute_txid(),
            tx1.compute_txid(),
            tx2.compute_txid(),
            tx3.compute_txid(),
        ];
        let (merkle_path, expected_merkle_root) = build_merkle_path_for_coinbase(&txids);

        tracker
            .cache_template_context(77, [0x42; 32], 0x1d00ffff, merkle_path.clone(), 4)
            .expect("tracker should cache template context");
        assert!(
            tracker
                .refresh_template_chain_state(77, refreshed_prev_hash, refreshed_n_bits)
                .expect("tracker should refresh template context"),
            "expected the cached template context to refresh",
        );

        let header_nonce = find_nonce_for_coinbase(
            &MergeMiningTemplateContext {
                template_id: 77,
                prev_hash: refreshed_prev_hash,
                n_bits: refreshed_n_bits,
                merkle_path: merkle_path.clone(),
                block_tx_count: 4,
            },
            version,
            header_timestamp,
            &coinbase,
            true,
        );
        let coinbase_bytes = serialize(&coinbase);
        let found_job = tracker
            .record_found_job(77, version, header_timestamp, header_nonce, &coinbase_bytes)
            .expect("tracker should build event")
            .expect("template context should exist");

        let header_bytes =
            Vec::<u8>::from_hex(&found_job.block_header_hex).expect("header hex should decode");
        let header: Header =
            deserialize(&header_bytes).expect("header hex should decode into a block header");

        assert_eq!(
            header.block_hash().to_string(),
            found_job.bitcoin_block_hash_hex
        );
        assert_eq!(header.prev_blockhash.to_byte_array(), refreshed_prev_hash);
        assert_eq!(header.bits.to_consensus(), refreshed_n_bits);
        assert_eq!(
            header.merkle_root.to_raw_hash().to_byte_array(),
            expected_merkle_root
        );
        assert_eq!(
            found_job.coinbase_tx_hex,
            coinbase_bytes.as_hex().to_string()
        );
        let expected_sibling_wire_hashes =
            serialize_rskj_merkle_hashes_hex(&merkle_path, txids.len() as u32);
        assert_eq!(found_job.merkle_hashes_hex, expected_sibling_wire_hashes);
        assert_eq!(
            found_job.merkle_hashes_hex.len(),
            merkle_tree_height(found_job.block_tx_count),
            "multi-tx found jobs must emit only the bottom-up sibling hashes expected by RSKIP92",
        );
        assert_eq!(found_job.block_tx_count, txids.len() as u32);
        assert_eq!(found_job.op_return_payload_hex, Some(payload_hex));
        assert_eq!(
            found_job.merkle_hashes_hex,
            merkle_path
                .iter()
                .copied()
                .map(serialize_rskj_wire_hash_hex)
                .collect::<Vec<_>>(),
            "RskJ partial-merkle submissions must emit only sibling hashes in bottom-up order",
        );
        validate_found_job_against_rskj_contract_for_tests(
            &found_job,
            Some(ExpectedHeaderFieldsForTests {
                prev_hash: Some(refreshed_prev_hash),
                version: Some(version),
                time: Some(header_timestamp),
                n_bits: Some(refreshed_n_bits),
                nonce: Some(header_nonce),
            }),
        )
        .expect("multi-tx found job should satisfy the bridge/RskJ contract");
        assert_eq!(
            header.merkle_root.to_raw_hash().to_byte_array(),
            expected_merkle_root,
            "the serialized block header must commit to the merkle root proven to RskJ",
        );
    }

    #[tokio::test]
    async fn record_found_job_exports_stripped_coinbase_for_multi_tx_segwit_block() {
        let (injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("44556677");
        let payload = Vec::from_hex(&payload_hex).expect("payload should decode");
        let prev_hash = non_symmetric_prev_hash(0x60);
        let version = 0x2000_0000;
        let header_timestamp = 1_700_000_654;
        let n_bits = EASY_TEST_N_BITS;

        apply_payload_to_template(&injector, 88, &payload_hex).await;

        let coinbase = make_segwit_coinbase_transaction(21, Some(&payload));
        let tx1 = make_transaction(4);
        let tx2 = make_transaction(5);
        let txids = vec![
            coinbase.compute_txid(),
            tx1.compute_txid(),
            tx2.compute_txid(),
        ];
        let (merkle_path, _) = build_merkle_path_for_coinbase(&txids);

        tracker
            .cache_template_context(88, prev_hash, n_bits, merkle_path, 3)
            .expect("tracker should cache template context");

        let header_nonce = find_nonce_for_coinbase(
            &MergeMiningTemplateContext {
                template_id: 88,
                prev_hash,
                n_bits,
                merkle_path: build_merkle_path_for_coinbase(&txids).0,
                block_tx_count: 3,
            },
            version,
            header_timestamp,
            &coinbase,
            true,
        );
        let solved_coinbase_bytes = serialize(&coinbase);
        let found_job = tracker
            .record_found_job(
                88,
                version,
                header_timestamp,
                header_nonce,
                &solved_coinbase_bytes,
            )
            .expect("tracker should build event")
            .expect("template context should exist");

        assert_ne!(
            found_job.coinbase_tx_hex,
            solved_coinbase_bytes.as_hex().to_string(),
            "RskJ-facing partial-merkle coinbase export must not keep witness bytes",
        );
        assert_eq!(
            found_job.merkle_hashes_hex,
            build_merkle_path_for_coinbase(&txids)
                .0
                .into_iter()
                .map(serialize_rskj_wire_hash_hex)
                .collect::<Vec<_>>(),
            "segwit multi-tx found jobs must still emit only sibling hashes for RSKIP92",
        );
        assert_eq!(
            found_job.merkle_hashes_hex.len(),
            merkle_tree_height(found_job.block_tx_count),
        );
        validate_found_job_against_rskj_contract_for_tests(
            &found_job,
            Some(ExpectedHeaderFieldsForTests {
                prev_hash: Some(prev_hash),
                version: Some(version),
                time: Some(header_timestamp),
                n_bits: Some(n_bits),
                nonce: Some(header_nonce),
            }),
        )
        .expect("segwit multi-tx found job should satisfy the RskJ partial-merkle contract");
    }

    #[tokio::test]
    async fn record_rsk_target_share_enqueues_multi_tx_found_job_when_hash_meets_live_target() {
        let (injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("99887766");
        let payload = Vec::from_hex(&payload_hex).expect("payload should decode");
        let template_id = 144;
        let prev_hash = non_symmetric_prev_hash(0x70);
        let version = 0x2000_0000;
        let header_timestamp = 1_700_000_777;
        let full_extranonce = vec![0xfa, 0xce, 0xb0, 0x0c];

        let coinbase =
            make_coinbase_transaction_with_extranonce(31, &full_extranonce, Some(&payload));
        let tx1 = make_transaction(7);
        let tx2 = make_transaction(8);
        let txids = vec![
            coinbase.compute_txid(),
            tx1.compute_txid(),
            tx2.compute_txid(),
        ];
        let (merkle_path, _) = build_merkle_path_for_coinbase(&txids);
        let template_context = MergeMiningTemplateContext {
            template_id,
            prev_hash,
            n_bits: 0x1d00ffff,
            merkle_path: merkle_path.clone(),
            block_tx_count: txids.len() as u32,
        };
        let header_nonce = find_nonce_for_coinbase(
            &template_context,
            version,
            header_timestamp,
            &coinbase,
            false,
        );
        let candidate = build_bitcoin_block_candidate(
            version,
            header_timestamp,
            header_nonce,
            &template_context,
            coinbase.compute_txid(),
        );
        let rsk_target_hex = rsk_target_hex_for_block_hash(candidate.header.block_hash());
        let serialized_coinbase = serialize(&coinbase);
        let (coinbase_tx_prefix, coinbase_tx_suffix) =
            split_serialized_coinbase_around_extranonce(&serialized_coinbase, &full_extranonce);

        apply_payload_with_rsk_target_to_template(
            &injector,
            template_id,
            &payload_hex,
            &rsk_target_hex,
        )
        .await;
        tracker
            .cache_template_context_with_coinbase_parts(
                template_id,
                prev_hash,
                template_context.n_bits,
                merkle_path.clone(),
                txids.len() as u32,
                coinbase_tx_prefix,
                coinbase_tx_suffix,
            )
            .expect("tracker should cache template and coinbase context");

        let found_job = tracker
            .record_rsk_target_share(
                template_id,
                version,
                header_timestamp,
                header_nonce,
                &full_extranonce,
            )
            .expect("tracker should evaluate the RSK target share")
            .expect("share meeting the live RSK target should be enqueued");

        assert_eq!(found_job.block_tx_count, txids.len() as u32);
        assert_eq!(
            found_job.merkle_hashes_hex,
            merkle_path
                .iter()
                .copied()
                .map(serialize_rskj_wire_hash_hex)
                .collect::<Vec<_>>()
        );
        assert_eq!(found_job.op_return_payload_hex, Some(payload_hex));
        validate_found_job_against_rskj_contract_for_tests(
            &found_job,
            Some(ExpectedHeaderFieldsForTests {
                prev_hash: Some(prev_hash),
                version: Some(version),
                time: Some(header_timestamp),
                n_bits: Some(template_context.n_bits),
                nonce: Some(header_nonce),
            }),
        )
        .expect("RSK-target share should satisfy the bridge/RskJ contract");
    }

    #[tokio::test]
    async fn duplicate_found_jobs_are_suppressed_before_reaching_the_queue() {
        let (injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("abc12345");
        let payload = Vec::from_hex(&payload_hex).expect("payload should decode");
        let coinbase_tx = make_segwit_coinbase_transaction(5, Some(&payload));
        let nonce = find_nonce_for_coinbase(
            &MergeMiningTemplateContext {
                template_id: 55,
                prev_hash: [5; 32],
                n_bits: EASY_TEST_N_BITS,
                merkle_path: vec![],
                block_tx_count: 1,
            },
            7,
            8,
            &coinbase_tx,
            true,
        );

        apply_payload_to_template(&injector, 55, &payload_hex).await;
        tracker
            .cache_template_context(55, [5; 32], EASY_TEST_N_BITS, vec![], 1)
            .expect("tracker should cache template context");

        let coinbase_bytes = serialize(&coinbase_tx);
        let first = tracker
            .record_found_job(55, 7, 8, nonce, &coinbase_bytes)
            .expect("tracker should record first event");
        let second = tracker
            .record_found_job(55, 7, 8, nonce, &coinbase_bytes)
            .expect("tracker should handle duplicate event");

        assert!(first.is_some(), "the first found job should be queued");
        assert_eq!(second, None, "duplicate found jobs should be suppressed");
        assert!(
            tracker
                .take_found_job()
                .expect("tracker should read queue")
                .is_some(),
            "the first found job should still be in the queue",
        );
        assert_eq!(
            tracker.take_found_job().expect("tracker should read queue"),
            None,
            "the duplicate found job must not reach the queue",
        );
    }

    #[tokio::test]
    async fn validate_bitcoin_block_candidate_rejects_high_hash_headers() {
        let (injector, tracker) = make_tracker();
        let payload_hex = rsk_payload_hex("deadc0de");
        let payload = Vec::from_hex(&payload_hex).expect("payload should decode");
        let prev_hash = non_symmetric_prev_hash(0x70);
        let version = 0x2000_0000;
        let header_timestamp = 1_700_001_000;
        let coinbase_tx = make_coinbase_transaction(33, Some(&payload));
        let template_context = MergeMiningTemplateContext {
            template_id: 66,
            prev_hash,
            n_bits: EASY_TEST_N_BITS,
            merkle_path: vec![],
            block_tx_count: 1,
        };
        let invalid_nonce = find_nonce_for_coinbase(
            &template_context,
            version,
            header_timestamp,
            &coinbase_tx,
            false,
        );

        apply_payload_to_template(&injector, 66, &payload_hex).await;
        tracker
            .cache_template_context(66, prev_hash, EASY_TEST_N_BITS, vec![], 1)
            .expect("tracker should cache template context");

        let validation = tracker
            .validate_bitcoin_block_candidate(
                66,
                version,
                header_timestamp,
                invalid_nonce,
                &serialize(&coinbase_tx),
            )
            .expect("validation should succeed");

        assert!(matches!(
            validation,
            BitcoinBlockCandidateValidation::InvalidPow(_)
        ));
    }
}
