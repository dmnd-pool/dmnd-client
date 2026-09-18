use axum::{extract::Query, http::StatusCode, Json};
use bitcoin::{
    block::{Header, Version},
    consensus::{deserialize, serialize, Decodable, Encodable},
    hashes::Hash,
    hex::{DisplayHex, FromHex},
    opcodes::all::OP_RETURN,
    script::{Instruction, PushBytesBuf},
    Amount, BlockHash, CompactTarget, ScriptBuf, Transaction, TxMerkleNode, TxOut,
};
use roles_logic_sv2::{
    mining_sv2::Target, template_distribution_sv2::NewTemplate, utils::merkle_root_from_path_,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    convert::TryInto,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, info, warn};

pub(crate) const RESERVED_COINBASE_OUTPUT_BYTES: u32 = 100;
const MAX_PAYLOAD_BYTES: usize = 80;
const MAX_TEMPLATES: usize = 128;
const MAX_JOBS: usize = 256;
const MAX_JOB_ANNOUNCEMENTS: usize = 256;
const MAX_PENDING_SHARES: usize = 128;
const MAX_EARLY_SHARES: usize = 64;
const MAX_FOUND_JOBS: usize = 32;
const MAX_DEDUP_IDENTITIES: usize = 128;
// The pinned converter prepends one pool output and encodes the total count in one byte.
const MAX_SINGLE_BYTE_COINBASE_OUTPUTS: u32 = 252;
const RSK_TAG: &[u8] = b"RSKBLOCK:";
const RSK_HASH_BYTES: usize = 32;
const RSK_MAX_TRAILING_COINBASE_BYTES: usize = 128;
static GLOBAL: OnceLock<MergeMining> = OnceLock::new();

pub(crate) fn global() -> &'static MergeMining {
    GLOBAL.get_or_init(MergeMining::new)
}

pub(crate) fn claim_job_binding_pending(job_id: u32) -> Option<u64> {
    if !enabled() {
        return None;
    }
    GLOBAL.get()?.claim_job_binding_pending(job_id)
}

pub(crate) fn set_active_job_binding(binding_id: Option<u64>) {
    if !enabled() {
        return;
    }
    if let Some(merge_mining) = GLOBAL.get() {
        merge_mining.set_active_job_binding(binding_id);
    }
}

pub(crate) fn clear_pending_job_binding(binding_id: Option<u64>) {
    if !enabled() {
        return;
    }
    if let (Some(merge_mining), Some(binding_id)) = (GLOBAL.get(), binding_id) {
        merge_mining.clear_pending_job_binding(binding_id);
    }
}

pub(crate) fn configured() -> bool {
    std::env::var("API_SECRET").is_ok_and(|secret| !secret.is_empty())
}

pub(crate) fn enabled() -> bool {
    configured()
}

#[derive(Clone)]
pub(crate) struct MergeMining {
    core: Arc<Core>,
    observer: Arc<Mutex<ObserverState>>,
}

struct Core {
    context: Mutex<ContextState>,
    found: Mutex<FoundState>,
    next_template_generation: AtomicU64,
    next_binding_id: AtomicU64,
    next_found_id: AtomicU64,
    dropped_observations: AtomicU64,
    queue_overflows: AtomicU64,
    announcement_mismatches: AtomicU64,
    observer_ready: AtomicBool,
}

struct ObserverState {
    work_tx: Option<SyncSender<Work>>,
    retry_after: Option<Instant>,
}

struct ObserverLiveness {
    core: Arc<Core>,
}

impl Drop for ObserverLiveness {
    fn drop(&mut self) {
        self.core.observer_ready.store(false, Ordering::Release);
    }
}

#[derive(Default)]
struct ContextState {
    desired: Option<DesiredPair>,
    templates: HashMap<u64, TemplateDraft>,
    current_templates: HashMap<u64, u64>,
    template_order: VecDeque<u64>,
    active_chain: Option<ChainState>,
    jobs: HashMap<u64, JobBinding>,
    current_jobs: HashMap<u32, u64>,
    job_order: VecDeque<u64>,
    job_announcements: VecDeque<(u32, Option<u64>)>,
    active_binding: Option<u64>,
    pending_activation_bindings: HashSet<u64>,
}

#[derive(Default)]
struct FoundState {
    pending: VecDeque<FoundJob>,
    recent: VecDeque<(String, String)>,
    total_found: u64,
}

#[derive(Default, Serialize)]
pub(crate) struct MergeMiningSnapshot {
    observer_ready: bool,
    work_available: bool,
    active_template_id: Option<u64>,
    proofs_found: u64,
    pending_proofs: usize,
}

pub(crate) fn dashboard_snapshot() -> Result<MergeMiningSnapshot, MergeMiningError> {
    GLOBAL
        .get()
        .map_or_else(|| Ok(MergeMiningSnapshot::default()), MergeMining::snapshot)
}

#[derive(Clone)]
struct DesiredPair {
    payload: Vec<u8>,
    payload_hex: String,
    target_le: [u8; 32],
    target_hex: String,
    output: Vec<u8>,
}

struct TemplateDraft {
    template_id: u64,
    desired: DesiredPair,
    merkle_path: Vec<[u8; 32]>,
    block_tx_count: Option<u32>,
    chain: Option<ChainState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ChainState {
    prev_hash: [u8; 32],
    n_bits: u32,
}

struct JobBinding {
    binding_id: u64,
    job_id: u32,
    template_id: u64,
    template_generation: u64,
    chain: Option<ChainState>,
    coinbase_prefix: Vec<u8>,
    coinbase_suffix: Vec<u8>,
}

enum Work {
    Share(ShareObservation),
    Wake,
}

struct ShareObservation {
    binding_id: u64,
    observed_at_unix_ts: u64,
    version: u32,
    timestamp: u32,
    nonce: u32,
    full_extranonce: Vec<u8>,
}

struct CandidateContext {
    template_id: u64,
    desired: DesiredPair,
    merkle_path: Vec<[u8; 32]>,
    block_tx_count: u32,
    chain: ChainState,
    coinbase_prefix: Vec<u8>,
    coinbase_suffix: Vec<u8>,
}

enum ResolveContext {
    Ready(Box<CandidateContext>),
    Incomplete,
    Obsolete,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct FoundJob {
    id: u64,
    observed_at_unix_ts: u64,
    template_id: u64,
    version: u32,
    header_timestamp: u32,
    header_nonce: u32,
    bitcoin_block_hash_hex: String,
    block_header_hex: String,
    coinbase_tx_hex: String,
    merkle_hashes_hex: Vec<String>,
    block_tx_count: u32,
    op_return_payload_hex: String,
    rsk_target_hex: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AcceptedPair {
    payload_len_bytes: usize,
    tx_out_len_bytes: usize,
    replaced_pending: bool,
}

#[derive(Debug)]
pub(crate) enum MergeMiningError {
    EmptyPayload,
    InvalidPayloadHex,
    AmbiguousRskPayload,
    PayloadTooLarge(usize),
    InvalidTarget,
    OutputTooLarge(usize),
    InvalidTemplateOutputs,
    InvalidRskCommitment,
    TooManyTemplateOutputs,
    TemplateOutputsTooLarge,
    StateUnavailable,
}

impl std::fmt::Display for MergeMiningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPayload => f.write_str("OP_RETURN payload cannot be empty"),
            Self::InvalidPayloadHex => f.write_str("OP_RETURN payload must be even-length hex"),
            Self::AmbiguousRskPayload => {
                f.write_str("RSK work hash contains a second RSKBLOCK: marker")
            }
            Self::PayloadTooLarge(len) => write!(
                f,
                "OP_RETURN payload is {len} bytes; the maximum is {MAX_PAYLOAD_BYTES}"
            ),
            Self::InvalidTarget => {
                f.write_str("RSK target must be exactly 32 bytes of big-endian hex")
            }
            Self::OutputTooLarge(len) => write!(
                f,
                "serialized OP_RETURN output is {len} bytes; only {RESERVED_COINBASE_OUTPUT_BYTES} bytes are reserved"
            ),
            Self::InvalidTemplateOutputs => {
                f.write_str("template contains invalid serialized coinbase outputs")
            }
            Self::InvalidRskCommitment => {
                f.write_str("appended RSK commitment would not be selected by RskJ")
            }
            Self::TooManyTemplateOutputs => {
                f.write_str("template coinbase output count cannot be safely increased")
            }
            Self::TemplateOutputsTooLarge => {
                f.write_str("modified template outputs exceed the SV2 field limit")
            }
            Self::StateUnavailable => f.write_str("merge-mining state is unavailable"),
        }
    }
}

impl MergeMining {
    fn snapshot(&self) -> Result<MergeMiningSnapshot, MergeMiningError> {
        let context = self
            .core
            .context
            .lock()
            .map_err(|_| MergeMiningError::StateUnavailable)?;
        let work_available = context
            .desired
            .as_ref()
            .is_some_and(|pair| is_rsk_payload(&pair.payload));
        let active_template_id = context
            .active_binding
            .and_then(|id| context.jobs.get(&id))
            .filter(|job| {
                context
                    .templates
                    .get(&job.template_generation)
                    .is_some_and(|template| is_rsk_payload(&template.desired.payload))
            })
            .map(|job| job.template_id);
        drop(context);
        let found = self
            .core
            .found
            .lock()
            .map_err(|_| MergeMiningError::StateUnavailable)?;
        Ok(MergeMiningSnapshot {
            observer_ready: self.core.observer_ready.load(Ordering::Acquire),
            work_available,
            active_template_id,
            proofs_found: found.total_found,
            pending_proofs: found.pending.len(),
        })
    }

    fn new() -> Self {
        let core = Arc::new(Core {
            context: Mutex::new(ContextState::default()),
            found: Mutex::new(FoundState::default()),
            next_template_generation: AtomicU64::new(1),
            next_binding_id: AtomicU64::new(1),
            next_found_id: AtomicU64::new(1),
            dropped_observations: AtomicU64::new(0),
            queue_overflows: AtomicU64::new(0),
            announcement_mismatches: AtomicU64::new(0),
            observer_ready: AtomicBool::new(false),
        });
        let merge_mining = Self {
            core,
            observer: Arc::new(Mutex::new(ObserverState {
                work_tx: None,
                retry_after: None,
            })),
        };
        merge_mining.ensure_observer();
        merge_mining
    }

    pub(crate) fn set_desired_pair(
        &self,
        data_hex: &str,
        target_hex: &str,
    ) -> Result<AcceptedPair, MergeMiningError> {
        let desired = DesiredPair::parse(data_hex, target_hex)?;
        if !self.ensure_observer() {
            return Err(MergeMiningError::StateUnavailable);
        }
        let accepted = AcceptedPair {
            payload_len_bytes: desired.payload.len(),
            tx_out_len_bytes: desired.output.len(),
            replaced_pending: false,
        };
        let mut state = self
            .core
            .context
            .lock()
            .map_err(|_| MergeMiningError::StateUnavailable)?;
        let mut accepted = accepted;
        accepted.replaced_pending = state.desired.replace(desired).is_some();
        drop(state);
        info!(
            payload_len_bytes = accepted.payload_len_bytes,
            tx_out_len_bytes = accepted.tx_out_len_bytes,
            replaced_pending = accepted.replaced_pending,
            "accepted desired RSK merge-mining pair"
        );
        Ok(accepted)
    }

    pub(crate) fn begin_template_session(&self) {
        self.clear_template_session();
        self.wake_worker();
    }

    fn clear_template_session(&self) {
        if let Ok(mut state) = self.core.context.lock() {
            state.current_templates.clear();
            state.active_chain = None;
            state.current_jobs.clear();
            state.job_announcements.clear();
            state.pending_activation_bindings.clear();
        }
    }

    #[cfg(test)]
    pub(crate) fn apply_to_template(
        &self,
        template: &mut NewTemplate<'static>,
        reserved_bytes: usize,
    ) -> Result<bool, MergeMiningError> {
        self.apply_to_template_with_pool_output_count(template, reserved_bytes, 1)
    }

    pub(crate) fn apply_to_template_with_pool_output_count(
        &self,
        template: &mut NewTemplate<'static>,
        reserved_bytes: usize,
        pool_output_count: usize,
    ) -> Result<bool, MergeMiningError> {
        let pool_output_count = u32::try_from(pool_output_count)
            .map_err(|_| MergeMiningError::TooManyTemplateOutputs)?;
        let max_template_outputs = MAX_SINGLE_BYTE_COINBASE_OUTPUTS
            .checked_sub(pool_output_count)
            .ok_or(MergeMiningError::TooManyTemplateOutputs)?;
        self.apply_to_template_with_output_limit(template, reserved_bytes, max_template_outputs)
    }

    fn apply_to_template_with_output_limit(
        &self,
        template: &mut NewTemplate<'static>,
        reserved_bytes: usize,
        max_template_outputs: u32,
    ) -> Result<bool, MergeMiningError> {
        {
            let mut state = self
                .core
                .context
                .lock()
                .map_err(|_| MergeMiningError::StateUnavailable)?;
            invalidate_current_template(&mut state, template.template_id);
        }
        if !self.ensure_observer() {
            return Err(MergeMiningError::StateUnavailable);
        }
        let desired = {
            let state = self
                .core
                .context
                .lock()
                .map_err(|_| MergeMiningError::StateUnavailable)?;
            state.desired.clone()
        };
        let Some(desired) = desired else {
            return Ok(false);
        };
        if desired.output.len() > reserved_bytes {
            return Err(MergeMiningError::OutputTooLarge(desired.output.len()));
        }

        let output_bytes = template.coinbase_tx_outputs.as_ref();
        let mut cursor = output_bytes;
        let desired_tag_offset = desired
            .output
            .windows(RSK_TAG.len())
            .position(|window| window == RSK_TAG);
        let mut last_desired_canonical_tag = None;
        for _ in 0..template.coinbase_tx_outputs_count {
            let output_start = output_bytes.len() - cursor.len();
            let output = TxOut::consensus_decode(&mut cursor)
                .map_err(|_| MergeMiningError::InvalidTemplateOutputs)?;
            if serialize(&output) == desired.output {
                last_desired_canonical_tag = desired_tag_offset.map(|offset| output_start + offset);
            }
        }
        if !cursor.is_empty() {
            return Err(MergeMiningError::InvalidTemplateOutputs);
        }

        let mut output_tail = output_bytes.to_vec();
        output_tail.extend_from_slice(&template.coinbase_tx_locktime.to_le_bytes());
        let already_last = is_rsk_payload(&desired.payload)
            && raw_rsk_commitment_matches(&output_tail, &desired)
            && last_raw_tag_position(&output_tail) == last_desired_canonical_tag;
        let (next_count, next_outputs) = if already_last {
            if template.coinbase_tx_outputs_count > max_template_outputs {
                return Err(MergeMiningError::TooManyTemplateOutputs);
            }
            (template.coinbase_tx_outputs_count, None)
        } else {
            let count = template
                .coinbase_tx_outputs_count
                .checked_add(1)
                .ok_or(MergeMiningError::TooManyTemplateOutputs)?;
            if count > max_template_outputs {
                return Err(MergeMiningError::TooManyTemplateOutputs);
            }
            let mut outputs = template.coinbase_tx_outputs.to_vec();
            outputs.extend_from_slice(&desired.output);
            if is_rsk_payload(&desired.payload) {
                let expected_tag_position =
                    desired_tag_offset.map(|offset| output_bytes.len() + offset);
                let mut prospective_tail = outputs.clone();
                prospective_tail.extend_from_slice(&template.coinbase_tx_locktime.to_le_bytes());
                if !raw_rsk_commitment_matches(&prospective_tail, &desired)
                    || last_raw_tag_position(&prospective_tail) != expected_tag_position
                {
                    return Err(MergeMiningError::InvalidRskCommitment);
                }
            }
            let outputs = outputs
                .try_into()
                .map_err(|_| MergeMiningError::TemplateOutputsTooLarge)?;
            (count, Some(outputs))
        };

        let template_merkle_path = template.merkle_path.to_vec();
        let mut merkle_path = Vec::with_capacity(template_merkle_path.len());
        for sibling in template_merkle_path {
            let sibling: [u8; 32] = sibling
                .to_vec()
                .try_into()
                .map_err(|_| MergeMiningError::InvalidTemplateOutputs)?;
            merkle_path.push(sibling);
        }

        let generation = next_id(&self.core.next_template_generation)
            .ok_or(MergeMiningError::StateUnavailable)?;
        let mut state = self
            .core
            .context
            .lock()
            .map_err(|_| MergeMiningError::StateUnavailable)?;
        let chain = if template.future_template {
            None
        } else {
            state.active_chain
        };
        if !insert_template(
            &mut state,
            generation,
            TemplateDraft {
                template_id: template.template_id,
                desired,
                merkle_path,
                block_tx_count: None,
                chain,
            },
        ) {
            return Ok(false);
        }
        if let Some(outputs) = next_outputs {
            template.coinbase_tx_outputs = outputs;
            template.coinbase_tx_outputs_count = next_count;
        }
        Ok(true)
    }

    pub(crate) fn discard_template_generation(&self, generation: u64) {
        if let Ok(mut state) = self.core.context.lock() {
            remove_template_generation(&mut state, generation);
        }
    }

    pub(crate) fn record_transaction_count(
        &self,
        template_generation: u64,
        transaction_count: usize,
    ) {
        let Some(block_tx_count) = transaction_count
            .checked_add(1)
            .and_then(|count| u32::try_from(count).ok())
            .filter(|count| *count <= i32::MAX as u32)
        else {
            warn!(
                template_generation,
                transaction_count, "ignoring invalid merge-mining transaction count"
            );
            return;
        };
        if let Ok(mut state) = self.core.context.lock() {
            if let Some(template) = state.templates.get_mut(&template_generation) {
                if template.block_tx_count.is_none() {
                    template.block_tx_count = Some(block_tx_count);
                }
            }
        }
        self.wake_worker();
    }

    pub(crate) fn record_chain_state(&self, template_id: u64, prev_hash: [u8; 32], n_bits: u32) {
        if let Ok(mut state) = self.core.context.lock() {
            let chain = ChainState { prev_hash, n_bits };
            state.active_chain = Some(chain);
            let generation = state.current_templates.get(&template_id).copied();
            if let Some(template) =
                generation.and_then(|generation| state.templates.get_mut(&generation))
            {
                template.chain = Some(chain);
            }
            // A job must always be reconstructed against the exact chain context that was
            // active when it became usable. A later same-template-id prevhash refresh may update
            // the template draft, but it must not silently rewrite existing job bindings.
            if let Some(generation) = generation {
                for job in state
                    .jobs
                    .values_mut()
                    .filter(|job| job.template_generation == generation && job.chain.is_none())
                {
                    job.chain = Some(chain);
                }
            }
        }
        self.wake_worker();
    }

    pub(crate) fn template_generation(&self, template_id: u64) -> Option<u64> {
        self.core
            .context
            .lock()
            .ok()?
            .current_templates
            .get(&template_id)
            .copied()
    }

    #[cfg(test)]
    fn bind_job(
        &self,
        job_id: u32,
        template_id: u64,
        coinbase_prefix: Vec<u8>,
        coinbase_suffix: Vec<u8>,
    ) {
        let mut state = match self.core.context.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let Some(template_generation) = state.current_templates.get(&template_id).copied() else {
            announce_job(&mut state, job_id, None);
            return;
        };
        let binding_id = create_job_binding(
            &self.core,
            &mut state,
            job_id,
            template_generation,
            coinbase_prefix,
            coinbase_suffix,
        );
        announce_job(&mut state, job_id, binding_id);
        drop(state);
        self.wake_worker();
    }

    pub(crate) fn bind_job_generation(
        &self,
        job_id: u32,
        template_generation: u64,
        coinbase_prefix: Vec<u8>,
        coinbase_suffix: Vec<u8>,
    ) -> bool {
        let mut state = match self.core.context.lock() {
            Ok(state) => state,
            Err(_) => return false,
        };
        let Some(binding_id) = create_job_binding(
            &self.core,
            &mut state,
            job_id,
            template_generation,
            coinbase_prefix,
            coinbase_suffix,
        ) else {
            return false;
        };
        announce_job(&mut state, job_id, Some(binding_id));
        drop(state);
        self.wake_worker();
        true
    }

    pub(crate) fn announce_unbound_job(&self, job_id: u32) {
        if let Ok(mut state) = self.core.context.lock() {
            state.current_jobs.remove(&job_id);
            announce_job(&mut state, job_id, None);
        }
    }

    #[cfg(test)]
    pub(crate) fn claim_job_binding(&self, job_id: u32) -> Option<u64> {
        self.claim_job_binding_(job_id, false)
    }

    fn claim_job_binding_pending(&self, job_id: u32) -> Option<u64> {
        self.claim_job_binding_(job_id, true)
    }

    fn claim_job_binding_(&self, job_id: u32, mark_pending: bool) -> Option<u64> {
        let mut state = self.core.context.lock().ok()?;
        let Some(position) = state
            .job_announcements
            .iter()
            .position(|(announced_job_id, _)| *announced_job_id == job_id)
        else {
            let mismatches = self
                .core
                .announcement_mismatches
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            if mismatches.is_power_of_two() {
                warn!(
                    mismatches,
                    received_job_id = job_id,
                    queued_announcements = state.job_announcements.len(),
                    "merge-mining job has no matching announcement; keeping it Bitcoin-only"
                );
            }
            return None;
        };
        let (_, binding_id) = state.job_announcements.drain(..=position).next_back()?;
        if mark_pending {
            if let Some(binding_id) = binding_id {
                if state.jobs.contains_key(&binding_id) {
                    state.pending_activation_bindings.insert(binding_id);
                }
            }
        }
        binding_id
    }

    fn set_active_job_binding(&self, binding_id: Option<u64>) {
        if let Ok(mut state) = self.core.context.lock() {
            if let Some(binding_id) = binding_id {
                state.pending_activation_bindings.remove(&binding_id);
            }
            let retained_binding =
                binding_id.filter(|binding_id| state.jobs.contains_key(binding_id));
            if binding_id.is_some() && retained_binding.is_none() {
                warn!(
                    ?binding_id,
                    "merge-mining binding for active miner job is no longer available"
                );
            }
            state.active_binding = retained_binding;
        }
    }

    #[cfg(test)]
    fn mark_job_binding_pending(&self, binding_id: u64) {
        if let Ok(mut state) = self.core.context.lock() {
            if state.jobs.contains_key(&binding_id) {
                state.pending_activation_bindings.insert(binding_id);
            }
        }
    }

    fn clear_pending_job_binding(&self, binding_id: u64) {
        if let Ok(mut state) = self.core.context.lock() {
            state.pending_activation_bindings.remove(&binding_id);
        }
    }

    pub(crate) fn try_observe_share(
        &self,
        binding_id: u64,
        version: u32,
        timestamp: u32,
        nonce: u32,
        full_extranonce: Vec<u8>,
    ) {
        let work = Work::Share(ShareObservation {
            binding_id,
            observed_at_unix_ts: unix_timestamp(),
            version,
            timestamp,
            nonce,
            full_extranonce,
        });
        let sender = self.observer_sender();
        let reason = match sender {
            Some(sender) => match sender.try_send(work) {
                Ok(()) => return,
                Err(error) => work_send_error(&error),
            },
            None => "worker-unavailable",
        };
        let dropped = self
            .core
            .dropped_observations
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if dropped.is_power_of_two() {
            debug!(
                dropped,
                reason, "merge-mining observation skipped without delaying Bitcoin share handling"
            );
        }
    }

    pub(crate) fn take_found_job(&self) -> Result<Option<FoundJob>, MergeMiningError> {
        if !self.ensure_observer() {
            return Err(MergeMiningError::StateUnavailable);
        }
        self.core
            .found
            .lock()
            .map(|mut found| found.pending.pop_front())
            .map_err(|_| MergeMiningError::StateUnavailable)
    }

    fn wake_worker(&self) {
        if !self.ensure_observer() {
            return;
        }
        if let Some(sender) = self.observer_sender() {
            let _ = sender.try_send(Work::Wake);
        }
    }

    fn observer_sender(&self) -> Option<SyncSender<Work>> {
        if !self.core.observer_ready.load(Ordering::Acquire) {
            return None;
        }
        self.observer.try_lock().ok()?.work_tx.clone()
    }

    fn ensure_observer(&self) -> bool {
        if self.core.observer_ready.load(Ordering::Acquire) {
            return true;
        }
        let Ok(mut observer) = self.observer.try_lock() else {
            return false;
        };
        if self.core.observer_ready.load(Ordering::Acquire) {
            return true;
        }
        let now = Instant::now();
        if observer
            .retry_after
            .is_some_and(|retry_after| retry_after > now)
        {
            return false;
        }

        let (work_tx, work_rx) = mpsc::sync_channel(MAX_PENDING_SHARES);
        let worker_core = self.core.clone();
        match std::thread::Builder::new()
            .name("merge-mining-observer".to_string())
            .spawn(move || {
                let _liveness = ObserverLiveness {
                    core: worker_core.clone(),
                };
                worker_loop(worker_core, work_rx);
            }) {
            Ok(_) => {
                observer.work_tx = Some(work_tx);
                observer.retry_after = None;
                self.core.observer_ready.store(true, Ordering::Release);
                true
            }
            Err(error) => {
                observer.work_tx = None;
                observer.retry_after = now.checked_add(Duration::from_secs(5));
                warn!(%error, "merge-mining observer could not start; continuing Bitcoin-only and retrying later");
                false
            }
        }
    }
}

impl DesiredPair {
    fn parse(data_hex: &str, target_hex: &str) -> Result<Self, MergeMiningError> {
        let payload_hex = data_hex.trim().to_ascii_lowercase();
        if payload_hex.is_empty() {
            return Err(MergeMiningError::EmptyPayload);
        }
        if payload_hex.len() % 2 != 0 {
            return Err(MergeMiningError::InvalidPayloadHex);
        }
        let payload =
            Vec::from_hex(&payload_hex).map_err(|_| MergeMiningError::InvalidPayloadHex)?;
        if payload.is_empty() {
            return Err(MergeMiningError::EmptyPayload);
        }
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(MergeMiningError::PayloadTooLarge(payload.len()));
        }
        if is_rsk_payload(&payload)
            && payload[RSK_TAG.len()..]
                .windows(RSK_TAG.len())
                .any(|window| window == RSK_TAG)
        {
            return Err(MergeMiningError::AmbiguousRskPayload);
        }

        let normalized_target = target_hex
            .trim()
            .strip_prefix("0x")
            .or_else(|| target_hex.trim().strip_prefix("0X"))
            .unwrap_or(target_hex.trim())
            .to_ascii_lowercase();
        let target_be =
            Vec::from_hex(&normalized_target).map_err(|_| MergeMiningError::InvalidTarget)?;
        let target_be: [u8; 32] = target_be
            .try_into()
            .map_err(|_| MergeMiningError::InvalidTarget)?;
        let mut target_le = target_be;
        target_le.reverse();

        let push = PushBytesBuf::try_from(payload.clone())
            .map_err(|_| MergeMiningError::InvalidPayloadHex)?;
        let output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(push),
        };
        let mut encoded = Vec::new();
        output
            .consensus_encode(&mut encoded)
            .map_err(|_| MergeMiningError::InvalidPayloadHex)?;
        if encoded.len() > RESERVED_COINBASE_OUTPUT_BYTES as usize {
            return Err(MergeMiningError::OutputTooLarge(encoded.len()));
        }

        Ok(Self {
            payload,
            payload_hex,
            target_le,
            target_hex: normalized_target,
            output: encoded,
        })
    }
}

fn next_id(counter: &AtomicU64) -> Option<u64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .ok()
        .filter(|id| *id != 0)
}

fn invalidate_current_template(state: &mut ContextState, template_id: u64) {
    state.current_templates.remove(&template_id);
    state.current_jobs.retain(|_, binding_id| {
        state
            .jobs
            .get(binding_id)
            .is_some_and(|job| job.template_id != template_id)
    });
}

fn generation_is_protected(state: &ContextState, generation: u64) -> bool {
    let is_active = state
        .active_binding
        .and_then(|binding_id| state.jobs.get(&binding_id))
        .is_some_and(|job| job.template_generation == generation);
    is_active
        || state.pending_activation_bindings.iter().any(|binding_id| {
            state
                .jobs
                .get(binding_id)
                .is_some_and(|job| job.template_generation == generation)
        })
}

fn insert_template(state: &mut ContextState, generation: u64, template: TemplateDraft) -> bool {
    let template_id = template.template_id;
    state.templates.insert(generation, template);
    state.current_templates.insert(template_id, generation);
    state.template_order.push_back(generation);
    while state.template_order.len() > MAX_TEMPLATES {
        let expired_index = state
            .template_order
            .iter()
            .position(|candidate| {
                *candidate != generation
                    && !state
                        .jobs
                        .values()
                        .any(|job| job.template_generation == *candidate)
            })
            .or_else(|| {
                state.template_order.iter().position(|candidate| {
                    *candidate != generation
                        && !generation_is_protected(state, *candidate)
                        && !state.job_announcements.iter().any(|(_, binding_id)| {
                            binding_id
                                .and_then(|binding_id| state.jobs.get(&binding_id))
                                .is_some_and(|job| job.template_generation == *candidate)
                        })
                })
            });
        if let Some(expired) = expired_index.and_then(|index| state.template_order.remove(index)) {
            remove_template_generation(state, expired);
        } else {
            remove_template_generation(state, generation);
            return false;
        }
    }
    true
}

fn remove_template_generation(state: &mut ContextState, generation: u64) {
    state
        .template_order
        .retain(|candidate| *candidate != generation);
    if let Some(template) = state.templates.remove(&generation) {
        if state.current_templates.get(&template.template_id) == Some(&generation) {
            state.current_templates.remove(&template.template_id);
        }
    }

    let expired_bindings: Vec<u64> = state
        .jobs
        .iter()
        .filter_map(|(binding_id, job)| {
            (job.template_generation == generation).then_some(*binding_id)
        })
        .collect();
    for binding_id in &expired_bindings {
        if let Some(job) = state.jobs.remove(binding_id) {
            if state.current_jobs.get(&job.job_id) == Some(binding_id) {
                state.current_jobs.remove(&job.job_id);
            }
        }
    }
    if state
        .active_binding
        .is_some_and(|binding_id| expired_bindings.contains(&binding_id))
    {
        state.active_binding = None;
    }
    state
        .pending_activation_bindings
        .retain(|binding_id| !expired_bindings.contains(binding_id));
    state
        .job_order
        .retain(|binding_id| !expired_bindings.contains(binding_id));
}

fn insert_job(state: &mut ContextState, job: JobBinding) {
    let binding_id = job.binding_id;
    state.current_jobs.insert(job.job_id, binding_id);
    state.jobs.insert(binding_id, job);
    state.job_order.push_back(binding_id);
    while state.job_order.len() > MAX_JOBS {
        let expired = state
            .job_order
            .iter()
            .position(|candidate| {
                Some(*candidate) != state.active_binding
                    && !state.pending_activation_bindings.contains(candidate)
            })
            .and_then(|index| state.job_order.remove(index));
        if let Some(expired) = expired {
            if let Some(expired_job) = state.jobs.remove(&expired) {
                if state.current_jobs.get(&expired_job.job_id) == Some(&expired) {
                    state.current_jobs.remove(&expired_job.job_id);
                }
            }
        } else {
            break;
        }
    }
}

fn create_job_binding(
    core: &Core,
    state: &mut ContextState,
    job_id: u32,
    template_generation: u64,
    coinbase_prefix: Vec<u8>,
    coinbase_suffix: Vec<u8>,
) -> Option<u64> {
    state.current_jobs.remove(&job_id);
    let template = state.templates.get(&template_generation)?;
    if !is_rsk_payload(&template.desired.payload) {
        return None;
    }
    let Some(binding_id) = next_id(&core.next_binding_id) else {
        warn!("merge-mining job binding identifier space exhausted");
        return None;
    };
    insert_job(
        state,
        JobBinding {
            binding_id,
            job_id,
            template_id: template.template_id,
            template_generation,
            chain: template.chain,
            coinbase_prefix,
            coinbase_suffix,
        },
    );
    Some(binding_id)
}

fn announce_job(state: &mut ContextState, job_id: u32, binding_id: Option<u64>) {
    if state.job_announcements.len() >= MAX_JOB_ANNOUNCEMENTS {
        state.job_announcements.pop_front();
    }
    state.job_announcements.push_back((job_id, binding_id));
}

fn worker_loop(core: Arc<Core>, work_rx: mpsc::Receiver<Work>) {
    let mut early = VecDeque::new();
    loop {
        match work_rx.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(Work::Share(observation)) => {
                handle_observation(&core, observation, &mut early);
                retry_early(&core, &mut early);
            }
            Ok(Work::Wake) | Err(mpsc::RecvTimeoutError::Timeout) => retry_early(&core, &mut early),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn handle_observation(
    core: &Arc<Core>,
    observation: ShareObservation,
    early: &mut VecDeque<ShareObservation>,
) {
    match resolve_context(core, &observation) {
        ResolveContext::Ready(context) => process_candidate(core, observation, *context),
        ResolveContext::Incomplete => {
            if early.len() >= MAX_EARLY_SHARES {
                early.pop_front();
                core.dropped_observations.fetch_add(1, Ordering::Relaxed);
            }
            early.push_back(observation);
        }
        ResolveContext::Obsolete => {}
    }
}

fn retry_early(core: &Arc<Core>, early: &mut VecDeque<ShareObservation>) {
    let attempts = early.len();
    for _ in 0..attempts {
        let Some(observation) = early.pop_front() else {
            break;
        };
        handle_observation(core, observation, early);
    }
}

fn resolve_context(core: &Core, observation: &ShareObservation) -> ResolveContext {
    let state = match core.context.lock() {
        Ok(state) => state,
        Err(_) => return ResolveContext::Obsolete,
    };
    let Some(job) = state.jobs.get(&observation.binding_id) else {
        return ResolveContext::Obsolete;
    };
    let Some(template) = state.templates.get(&job.template_generation) else {
        return ResolveContext::Obsolete;
    };
    if !is_rsk_payload(&template.desired.payload) {
        return ResolveContext::Obsolete;
    }
    let (Some(block_tx_count), Some(chain)) = (template.block_tx_count, job.chain) else {
        return ResolveContext::Incomplete;
    };
    if tree_height(block_tx_count) != Some(template.merkle_path.len()) {
        warn!(
            template_id = job.template_id,
            block_tx_count,
            merkle_path_len = template.merkle_path.len(),
            "skipping incoherent merge-mining merkle context"
        );
        return ResolveContext::Obsolete;
    }
    ResolveContext::Ready(Box::new(CandidateContext {
        template_id: job.template_id,
        desired: template.desired.clone(),
        merkle_path: template.merkle_path.clone(),
        block_tx_count,
        chain,
        coinbase_prefix: job.coinbase_prefix.clone(),
        coinbase_suffix: job.coinbase_suffix.clone(),
    }))
}

fn process_candidate(core: &Core, observation: ShareObservation, context: CandidateContext) {
    let mut coinbase_bytes = Vec::with_capacity(
        context.coinbase_prefix.len()
            + observation.full_extranonce.len()
            + context.coinbase_suffix.len(),
    );
    coinbase_bytes.extend_from_slice(&context.coinbase_prefix);
    coinbase_bytes.extend_from_slice(&observation.full_extranonce);
    coinbase_bytes.extend_from_slice(&context.coinbase_suffix);
    let mut coinbase: Transaction = match deserialize(&coinbase_bytes) {
        Ok(coinbase) => coinbase,
        Err(error) => {
            debug!(template_id = context.template_id, %error, "skipping invalid merge-mining coinbase reconstruction");
            return;
        }
    };
    for input in &mut coinbase.input {
        input.witness.clear();
    }
    let stripped_coinbase = serialize(&coinbase);
    if !has_canonical_last_rsk_commitment(&coinbase, &context.desired)
        || !raw_rsk_commitment_matches(&stripped_coinbase, &context.desired)
    {
        warn!(
            template_id = context.template_id,
            "skipping merge-mining share whose coinbase commitment does not match its template"
        );
        return;
    }
    let coinbase_txid = coinbase.compute_txid();
    let merkle_root = merkle_root_from_path_(
        coinbase_txid.to_raw_hash().to_byte_array(),
        &context.merkle_path,
    );
    let header = Header {
        version: Version::from_consensus(observation.version as i32),
        prev_blockhash: BlockHash::from_byte_array(context.chain.prev_hash),
        merkle_root: TxMerkleNode::from_byte_array(merkle_root),
        time: observation.timestamp,
        bits: CompactTarget::from_consensus(context.chain.n_bits),
        nonce: observation.nonce,
    };
    let candidate_target = Target::from(header.block_hash().to_raw_hash().to_byte_array());
    if candidate_target > Target::from(context.desired.target_le) {
        return;
    }

    let header_hex = serialize(&header).as_hex().to_string();
    let coinbase_hex = stripped_coinbase.as_hex().to_string();
    let Some(id) = next_id(&core.next_found_id) else {
        warn!("merge-mining found-job identifier space exhausted");
        return;
    };
    let job = FoundJob {
        id,
        observed_at_unix_ts: observation.observed_at_unix_ts,
        template_id: context.template_id,
        version: observation.version,
        header_timestamp: observation.timestamp,
        header_nonce: observation.nonce,
        bitcoin_block_hash_hex: header.block_hash().to_string(),
        block_header_hex: header_hex.clone(),
        coinbase_tx_hex: coinbase_hex.clone(),
        merkle_hashes_hex: context
            .merkle_path
            .iter()
            .map(|hash| {
                hash.iter()
                    .rev()
                    .copied()
                    .collect::<Vec<_>>()
                    .as_hex()
                    .to_string()
            })
            .collect(),
        block_tx_count: context.block_tx_count,
        op_return_payload_hex: context.desired.payload_hex,
        rsk_target_hex: context.desired.target_hex,
    };

    let mut found = match core.found.lock() {
        Ok(found) => found,
        Err(_) => return,
    };
    if found
        .recent
        .iter()
        .any(|(header, coinbase)| header == &header_hex && coinbase == &coinbase_hex)
    {
        return;
    }
    if found.recent.len() >= MAX_DEDUP_IDENTITIES {
        found.recent.pop_front();
    }
    found.recent.push_back((header_hex, coinbase_hex));
    if found.pending.len() >= MAX_FOUND_JOBS {
        found.pending.pop_front();
        let overflows = core
            .queue_overflows
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if overflows.is_power_of_two() {
            warn!(overflows, "merge-mining found-job queue full; dropped oldest candidate without affecting Bitcoin mining");
        }
    }
    debug!(template_id = job.template_id, bitcoin_block_hash = %job.bitcoin_block_hash_hex, "queued RSK merge-mining proof candidate");
    found.total_found = found.total_found.saturating_add(1);
    found.pending.push_back(job);
}

fn is_rsk_payload(payload: &[u8]) -> bool {
    payload.len() == RSK_TAG.len() + RSK_HASH_BYTES && payload.starts_with(RSK_TAG)
}

fn last_raw_tag_position(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(RSK_TAG.len())
        .rposition(|window| window == RSK_TAG)
}

fn raw_rsk_commitment_matches(bytes: &[u8], desired: &DesiredPair) -> bool {
    if !is_rsk_payload(&desired.payload) {
        return false;
    }
    let Some(position) = last_raw_tag_position(bytes) else {
        return false;
    };
    let Some(end) = position.checked_add(desired.payload.len()) else {
        return false;
    };
    bytes.get(position..end) == Some(desired.payload.as_slice())
        && bytes
            .len()
            .checked_sub(end)
            .is_some_and(|trailing| trailing <= RSK_MAX_TRAILING_COINBASE_BYTES)
}

fn tree_height(block_tx_count: u32) -> Option<usize> {
    if block_tx_count == 0 || block_tx_count > i32::MAX as u32 {
        return None;
    }
    let mut width = block_tx_count as usize;
    let mut height = 0;
    while width > 1 {
        width = width.div_ceil(2);
        height += 1;
    }
    Some(height)
}

fn single_op_return_payload(script: &ScriptBuf) -> Option<Vec<u8>> {
    if !script.is_op_return() {
        return None;
    }
    let mut instructions = script.instructions();
    match instructions.next()? {
        Ok(Instruction::Op(op)) if op == OP_RETURN => {}
        _ => return None,
    }
    let payload = match instructions.next()? {
        Ok(Instruction::PushBytes(bytes)) => bytes.as_bytes().to_vec(),
        _ => return None,
    };
    instructions.next().is_none().then_some(payload)
}

fn has_canonical_last_rsk_commitment(transaction: &Transaction, desired: &DesiredPair) -> bool {
    transaction
        .output
        .iter()
        .filter_map(|output| {
            single_op_return_payload(&output.script_pubkey)
                .filter(|payload| payload.starts_with(RSK_TAG))
                .map(|_| serialize(output) == desired.output)
        })
        .next_back()
        .unwrap_or(false)
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn work_send_error(error: &TrySendError<Work>) -> &'static str {
    match error {
        TrySendError::Full(_) => "queue-full",
        TrySendError::Disconnected(_) => "worker-unavailable",
    }
}

#[derive(Deserialize)]
pub(crate) struct SetPairRequest {
    secret: String,
    data_hex: String,
    #[serde(default)]
    rsk_target_hex: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct PollRequest {
    secret: String,
}

#[derive(Serialize)]
pub(crate) struct ApiEnvelope<T> {
    success: bool,
    message: Option<String>,
    data: Option<T>,
}

impl<T> ApiEnvelope<T> {
    fn success(data: Option<T>) -> Self {
        Self {
            success: true,
            message: None,
            data,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: Some(message.into()),
            data: None,
        }
    }
}

pub(crate) async fn set_pair_api(
    Json(request): Json<SetPairRequest>,
) -> (StatusCode, Json<ApiEnvelope<AcceptedPair>>) {
    if let Err((status, message)) = authorize(&request.secret) {
        return (status, Json(ApiEnvelope::error(message)));
    }
    let Some(rsk_target_hex) = request.rsk_target_hex.as_deref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiEnvelope::error(
                MergeMiningError::InvalidTarget.to_string(),
            )),
        );
    };
    match global().set_desired_pair(&request.data_hex, rsk_target_hex) {
        Ok(accepted) => (
            StatusCode::ACCEPTED,
            Json(ApiEnvelope::success(Some(accepted))),
        ),
        Err(MergeMiningError::StateUnavailable) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiEnvelope::error("merge-mining state is unavailable")),
        ),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(ApiEnvelope::error(error.to_string())),
        ),
    }
}

pub(crate) async fn poll_found_job_api(
    Query(request): Query<PollRequest>,
) -> (StatusCode, Json<ApiEnvelope<FoundJob>>) {
    if let Err((status, message)) = authorize(&request.secret) {
        return (status, Json(ApiEnvelope::error(message)));
    }
    match global().take_found_job() {
        Ok(job) => (StatusCode::OK, Json(ApiEnvelope::success(job))),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiEnvelope::error(error.to_string())),
        ),
    }
}

fn authorize(supplied: &str) -> Result<(), (StatusCode, &'static str)> {
    let expected = std::env::var("API_SECRET")
        .ok()
        .filter(|secret| !secret.is_empty())
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "merge-mining API secret is not configured",
        ))?;
    if constant_time_eq(expected.as_bytes(), supplied.as_bytes()) {
        Ok(())
    } else {
        Err((StatusCode::UNAUTHORIZED, "unauthorized"))
    }
}

fn constant_time_eq(expected: &[u8], supplied: &[u8]) -> bool {
    let mut difference = expected.len() ^ supplied.len();
    let length = expected.len().max(supplied.len());
    for index in 0..length {
        let left = expected.get(index).copied().unwrap_or(0);
        let right = supplied.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_sv2::{Seq0255, B0255, B064K, U256};
    use bitcoin::{absolute::LockTime, transaction, OutPoint, Sequence, TxIn, Witness};
    use std::time::Duration;

    const PAYLOAD_HEX: &str =
        "52534b424c4f434b3a000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const EASY_TARGET: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    fn template(template_id: u64) -> NewTemplate<'static> {
        NewTemplate {
            template_id,
            future_template: false,
            version: 0x2000_0000,
            coinbase_tx_version: 2,
            coinbase_prefix: B0255::try_from(Vec::new()).expect("empty prefix"),
            coinbase_tx_input_sequence: u32::MAX,
            coinbase_tx_value_remaining: 0,
            coinbase_tx_outputs_count: 0,
            coinbase_tx_outputs: B064K::try_from(Vec::new()).expect("empty outputs"),
            coinbase_tx_locktime: 0,
            merkle_path: Seq0255::<U256>::from(Vec::new()),
        }
    }

    fn current_binding(merge: &MergeMining, job_id: u32) -> Option<u64> {
        merge
            .core
            .context
            .lock()
            .expect("state")
            .current_jobs
            .get(&job_id)
            .copied()
    }

    fn candidate(
        target_hex: &str,
        merkle_path: Vec<[u8; 32]>,
        block_tx_count: u32,
        nonce: u32,
    ) -> (ShareObservation, CandidateContext) {
        let desired = DesiredPair::parse(PAYLOAD_HEX, target_hex).expect("valid desired pair");
        let payload = Vec::from_hex(PAYLOAD_HEX).expect("payload hex");
        let output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(
                PushBytesBuf::try_from(payload).expect("push bytes"),
            ),
        };
        let marker = [0x51_u8; 8];
        let coinbase = Transaction {
            version: transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(marker.to_vec()),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![output],
        };
        let encoded = serialize(&coinbase);
        let marker_at = encoded
            .windows(marker.len())
            .position(|window| window == marker)
            .expect("marker in coinbase");
        (
            ShareObservation {
                binding_id: 1,
                observed_at_unix_ts: 1_800_000_000,
                version: 0x2000_0000,
                timestamp: 1_800_000_000,
                nonce,
                full_extranonce: marker.to_vec(),
            },
            CandidateContext {
                template_id: 9,
                desired,
                merkle_path,
                block_tx_count,
                chain: ChainState {
                    prev_hash: [3; 32],
                    n_bits: 0x207f_ffff,
                },
                coinbase_prefix: encoded[..marker_at].to_vec(),
                coinbase_suffix: encoded[marker_at + marker.len()..].to_vec(),
            },
        )
    }

    fn disconnect_observer(merge: &MergeMining) {
        let sender = merge
            .observer
            .lock()
            .expect("observer state")
            .work_tx
            .take();
        drop(sender);
        for _ in 0..1_000 {
            if !merge.core.observer_ready.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("observer did not stop after its channel disconnected");
    }

    #[test]
    fn unavailable_observer_leaves_the_bitcoin_template_pristine() {
        let merge = MergeMining::new();
        disconnect_observer(&merge);
        merge.observer.lock().expect("observer state").retry_after =
            Some(Instant::now() + Duration::from_secs(60));

        assert!(matches!(
            merge.set_desired_pair(PAYLOAD_HEX, EASY_TARGET),
            Err(MergeMiningError::StateUnavailable)
        ));
        let mut bitcoin_template = template(7);
        assert!(matches!(
            merge.apply_to_template(
                &mut bitcoin_template,
                RESERVED_COINBASE_OUTPUT_BYTES as usize,
            ),
            Err(MergeMiningError::StateUnavailable)
        ));
        assert_eq!(bitcoin_template.coinbase_tx_outputs_count, 0);
        assert!(matches!(
            merge.take_found_job(),
            Err(MergeMiningError::StateUnavailable)
        ));
        assert!(merge.core.context.lock().expect("state").desired.is_none());
    }

    #[test]
    fn unavailable_observer_invalidates_a_reused_template_id() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        merge
            .apply_to_template(&mut template(70), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("first generation");
        merge.bind_job(7, 70, Vec::new(), Vec::new());
        assert!(current_binding(&merge, 7).is_some());

        disconnect_observer(&merge);
        merge.observer.lock().expect("observer state").retry_after =
            Some(Instant::now() + Duration::from_secs(60));
        let mut replacement = template(70);
        assert!(matches!(
            merge.apply_to_template(&mut replacement, RESERVED_COINBASE_OUTPUT_BYTES as usize,),
            Err(MergeMiningError::StateUnavailable)
        ));

        let state = merge.core.context.lock().expect("state");
        assert!(!state.current_templates.contains_key(&70));
        assert!(!state.current_jobs.contains_key(&7));
    }

    #[test]
    fn failed_job_preflight_discards_the_unpublished_template_generation() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let mut candidate = template(71);
        assert!(merge
            .apply_to_template(&mut candidate, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("merge candidate"));
        let generation = merge.template_generation(71).expect("template generation");

        merge.discard_template_generation(generation);

        let state = merge.core.context.lock().expect("state");
        assert!(!state.current_templates.contains_key(&71));
        assert!(!state.templates.contains_key(&generation));
    }

    #[test]
    fn observer_failure_after_injection_does_not_block_job_binding() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let mut merge_template = template(7);
        assert!(merge
            .apply_to_template(&mut merge_template, RESERVED_COINBASE_OUTPUT_BYTES as usize,)
            .expect("merge template"));
        let generation = merge.template_generation(7).expect("template generation");

        disconnect_observer(&merge);
        merge.observer.lock().expect("observer state").retry_after =
            Some(Instant::now() + Duration::from_secs(60));
        assert!(merge.bind_job_generation(9, generation, Vec::new(), Vec::new()));

        let state = merge.core.context.lock().expect("state");
        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.job_announcements.len(), 1);
    }

    #[test]
    fn observer_restarts_after_disconnection() {
        let merge = MergeMining::new();
        disconnect_observer(&merge);

        assert!(merge.ensure_observer());
        assert!(merge.core.observer_ready.load(Ordering::Acquire));
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("observer recovered");
    }

    #[test]
    fn validates_and_atomically_replaces_desired_pair() {
        let merge = MergeMining::new();
        let first = merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        assert_eq!(first.payload_len_bytes, 41);
        assert_eq!(first.tx_out_len_bytes, 52);
        assert!(!first.replaced_pending);

        assert!(merge.set_desired_pair("0", EASY_TARGET).is_err());
        let replacement = merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("replacement");
        assert!(replacement.replaced_pending);
    }

    #[test]
    fn rejects_an_rsk_work_hash_containing_another_tag() {
        let merge = MergeMining::new();
        let mut payload = RSK_TAG.to_vec();
        payload.extend_from_slice(RSK_TAG);
        payload.resize(RSK_TAG.len() + RSK_HASH_BYTES, 0);

        assert!(matches!(
            merge.set_desired_pair(&payload.as_hex().to_string(), EASY_TARGET),
            Err(MergeMiningError::AmbiguousRskPayload)
        ));
    }

    #[test]
    fn rejects_a_raw_tag_formed_across_the_hash_locktime_boundary() {
        let merge = MergeMining::new();
        let mut work_hash = vec![0; RSK_HASH_BYTES];
        work_hash[RSK_HASH_BYTES - 5..].copy_from_slice(b"RSKBL");
        let mut payload = RSK_TAG.to_vec();
        payload.extend_from_slice(&work_hash);
        merge
            .set_desired_pair(&payload.as_hex().to_string(), EASY_TARGET)
            .expect("the hash itself contains no complete second tag");

        let mut candidate = template(701);
        candidate.coinbase_tx_locktime = u32::from_le_bytes(*b"OCK:");
        let original_outputs = candidate.coinbase_tx_outputs.to_vec();
        assert!(matches!(
            merge.apply_to_template(&mut candidate, RESERVED_COINBASE_OUTPUT_BYTES as usize),
            Err(MergeMiningError::InvalidRskCommitment)
        ));
        assert_eq!(candidate.coinbase_tx_outputs_count, 0);
        assert_eq!(candidate.coinbase_tx_outputs.to_vec(), original_outputs);
        assert!(merge.template_generation(701).is_none());
    }

    #[test]
    fn injection_is_idempotent_and_keeps_desired_rskblock_last() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let mut template = template(7);
        assert!(merge
            .apply_to_template(&mut template, RESERVED_COINBASE_OUTPUT_BYTES as usize,)
            .expect("injection"));
        assert_eq!(template.coinbase_tx_outputs_count, 1);
        assert!(merge
            .apply_to_template(&mut template, RESERVED_COINBASE_OUTPUT_BYTES as usize,)
            .expect("idempotent injection"));
        assert_eq!(template.coinbase_tx_outputs_count, 1);
    }

    #[test]
    fn idempotence_obeys_rskj_raw_tag_and_trailing_byte_rules() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let desired_output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(
                PushBytesBuf::try_from(Vec::from_hex(PAYLOAD_HEX).expect("payload"))
                    .expect("push bytes"),
            ),
        };

        let with_filler = |template_id, filler_len| {
            let filler = TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::from_bytes(vec![0x51; filler_len]),
            };
            let mut outputs = serialize(&desired_output);
            outputs.extend_from_slice(&serialize(&filler));
            let mut template = template(template_id);
            template.coinbase_tx_outputs_count = 2;
            template.coinbase_tx_outputs = B064K::try_from(outputs).expect("outputs");
            template
        };

        let mut exactly_128 = with_filler(72, 115);
        assert!(merge
            .apply_to_template(&mut exactly_128, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("128 trailing bytes"));
        assert_eq!(exactly_128.coinbase_tx_outputs_count, 2);

        let mut trailing_129 = with_filler(73, 116);
        assert!(merge
            .apply_to_template(&mut trailing_129, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("reappend after 129 trailing bytes"));
        assert_eq!(trailing_129.coinbase_tx_outputs_count, 3);
    }

    #[test]
    fn hidden_raw_tag_at_output_boundary_uses_pristine_fallback() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let ordinary = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new(),
        };
        let desired_output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(
                PushBytesBuf::try_from(Vec::from_hex(PAYLOAD_HEX).expect("payload"))
                    .expect("push bytes"),
            ),
        };
        let mut hidden_script = vec![0x51];
        hidden_script.extend_from_slice(RSK_TAG);
        hidden_script.extend_from_slice(&[0xff; RSK_HASH_BYTES]);
        let hidden = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(hidden_script),
        };
        let mut low_count_outputs = serialize(&desired_output);
        low_count_outputs.extend_from_slice(&serialize(&hidden));
        let mut low_count = template(741);
        low_count.coinbase_tx_outputs_count = 2;
        low_count.coinbase_tx_outputs =
            B064K::try_from(low_count_outputs).expect("low-count outputs");
        assert!(merge
            .apply_to_template(&mut low_count, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("hidden tag requires a final canonical commitment"));
        assert_eq!(low_count.coinbase_tx_outputs_count, 3);

        let mut outputs = (0..249)
            .flat_map(|_| serialize(&ordinary))
            .collect::<Vec<_>>();
        outputs.extend_from_slice(&serialize(&desired_output));
        outputs.extend_from_slice(&serialize(&hidden));
        let mut boundary = template(74);
        boundary.coinbase_tx_outputs_count = 251;
        boundary.coinbase_tx_outputs = B064K::try_from(outputs.clone()).expect("outputs");

        assert!(matches!(
            merge.apply_to_template(&mut boundary, RESERVED_COINBASE_OUTPUT_BYTES as usize),
            Err(MergeMiningError::TooManyTemplateOutputs)
        ));
        assert_eq!(boundary.coinbase_tx_outputs_count, 251);
        assert_eq!(boundary.coinbase_tx_outputs.to_vec(), outputs);
    }

    #[test]
    fn noncanonical_matching_rsk_push_is_not_treated_as_applied() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let payload = Vec::from_hex(PAYLOAD_HEX).expect("payload hex");
        let mut script = vec![0x6a, 0x4c, payload.len() as u8];
        script.extend_from_slice(&payload);
        let noncanonical = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::from_bytes(script),
        };
        let mut template = template(71);
        template.coinbase_tx_outputs_count = 1;
        template.coinbase_tx_outputs =
            B064K::try_from(serialize(&noncanonical)).expect("serialized output");

        assert!(merge
            .apply_to_template(&mut template, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("injection"));
        assert_eq!(template.coinbase_tx_outputs_count, 2);
    }

    #[test]
    fn insufficient_reservation_leaves_template_unchanged() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let mut template = template(8);
        let original = template.coinbase_tx_outputs.to_vec();
        assert!(matches!(
            merge.apply_to_template(&mut template, 51),
            Err(MergeMiningError::OutputTooLarge(52))
        ));
        assert_eq!(template.coinbase_tx_outputs_count, 0);
        assert_eq!(template.coinbase_tx_outputs.to_vec(), original);
    }

    #[test]
    fn injection_refuses_to_cross_compact_size_output_boundary() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new(),
        };
        let outputs = (0..251)
            .flat_map(|_| serialize(&output))
            .collect::<Vec<_>>();
        let mut boundary_template = template(80);
        boundary_template.coinbase_tx_outputs_count = 251;
        boundary_template.coinbase_tx_outputs =
            B064K::try_from(outputs.clone()).expect("serialized outputs");

        assert!(matches!(
            merge.apply_to_template(
                &mut boundary_template,
                RESERVED_COINBASE_OUTPUT_BYTES as usize,
            ),
            Err(MergeMiningError::TooManyTemplateOutputs)
        ));
        assert_eq!(boundary_template.coinbase_tx_outputs_count, 251);
        assert_eq!(boundary_template.coinbase_tx_outputs.to_vec(), outputs);

        let outputs = (0..250)
            .flat_map(|_| serialize(&output))
            .collect::<Vec<_>>();
        let mut safe_template = template(801);
        safe_template.coinbase_tx_outputs_count = 250;
        safe_template.coinbase_tx_outputs = B064K::try_from(outputs).expect("serialized outputs");
        assert!(merge
            .apply_to_template(&mut safe_template, RESERVED_COINBASE_OUTPUT_BYTES as usize,)
            .expect("safe final output count"));
        assert_eq!(safe_template.coinbase_tx_outputs_count, 251);

        let outputs = (0..250)
            .flat_map(|_| serialize(&output))
            .collect::<Vec<_>>();
        let mut two_pool_output_template = template(803);
        two_pool_output_template.coinbase_tx_outputs_count = 250;
        two_pool_output_template.coinbase_tx_outputs =
            B064K::try_from(outputs.clone()).expect("serialized outputs");
        assert!(matches!(
            merge.apply_to_template_with_pool_output_count(
                &mut two_pool_output_template,
                RESERVED_COINBASE_OUTPUT_BYTES as usize,
                2,
            ),
            Err(MergeMiningError::TooManyTemplateOutputs)
        ));
        assert_eq!(two_pool_output_template.coinbase_tx_outputs_count, 250);
        assert_eq!(
            two_pool_output_template.coinbase_tx_outputs.to_vec(),
            outputs
        );
    }

    #[test]
    fn canonical_last_rsk_output_is_idempotent_at_the_output_boundary() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let ordinary_output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new(),
        };
        let payload = Vec::from_hex(PAYLOAD_HEX).expect("payload hex");
        let rsk_output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(
                PushBytesBuf::try_from(payload).expect("push bytes"),
            ),
        };
        let mut outputs = (0..250)
            .flat_map(|_| serialize(&ordinary_output))
            .collect::<Vec<_>>();
        outputs.extend_from_slice(&serialize(&rsk_output));
        let mut boundary_template = template(802);
        boundary_template.coinbase_tx_outputs_count = 251;
        boundary_template.coinbase_tx_outputs =
            B064K::try_from(outputs.clone()).expect("serialized outputs");

        assert!(merge
            .apply_to_template(
                &mut boundary_template,
                RESERVED_COINBASE_OUTPUT_BYTES as usize,
            )
            .expect("idempotent boundary output"));
        assert_eq!(boundary_template.coinbase_tx_outputs_count, 251);
        assert_eq!(boundary_template.coinbase_tx_outputs.to_vec(), outputs);

        let mut too_many_outputs = serialize(&ordinary_output);
        too_many_outputs.extend_from_slice(&outputs);
        let mut invalid_template = template(804);
        invalid_template.coinbase_tx_outputs_count = 252;
        invalid_template.coinbase_tx_outputs =
            B064K::try_from(too_many_outputs.clone()).expect("serialized outputs");

        assert!(matches!(
            merge.apply_to_template(
                &mut invalid_template,
                RESERVED_COINBASE_OUTPUT_BYTES as usize,
            ),
            Err(MergeMiningError::TooManyTemplateOutputs)
        ));
        assert_eq!(invalid_template.coinbase_tx_outputs_count, 252);
        assert_eq!(
            invalid_template.coinbase_tx_outputs.to_vec(),
            too_many_outputs
        );
    }

    #[test]
    fn reused_template_id_failure_cannot_reactivate_stale_context() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        merge
            .apply_to_template(&mut template(81), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("first generation");
        merge.bind_job(18, 81, Vec::new(), Vec::new());
        let old_binding = current_binding(&merge, 18).expect("old binding");
        assert_eq!(merge.claim_job_binding(18), Some(old_binding));

        let mut invalid_reuse = template(81);
        invalid_reuse.coinbase_tx_outputs_count = 1;
        assert!(matches!(
            merge.apply_to_template(&mut invalid_reuse, RESERVED_COINBASE_OUTPUT_BYTES as usize),
            Err(MergeMiningError::InvalidTemplateOutputs)
        ));
        assert!(current_binding(&merge, 18).is_none());

        merge.bind_job(19, 81, Vec::new(), Vec::new());
        assert_eq!(merge.claim_job_binding(19), None);
        let state = merge.core.context.lock().expect("state");
        assert!(state.jobs.contains_key(&old_binding));
        assert!(!state.current_templates.contains_key(&81));
    }

    #[test]
    fn lagging_consumer_claims_exact_binding_across_raw_id_reuse() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("first pair");
        merge
            .apply_to_template(&mut template(82), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("first generation");
        merge.bind_job(20, 82, vec![1], vec![2]);
        let old_binding = current_binding(&merge, 20).expect("old binding");

        merge
            .set_desired_pair(PAYLOAD_HEX, &format!("{}01", "00".repeat(31)))
            .expect("second pair");
        merge
            .apply_to_template(&mut template(82), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("second generation");
        merge.bind_job(20, 82, vec![3], vec![4]);
        let new_binding = current_binding(&merge, 20).expect("new binding");

        assert_ne!(old_binding, new_binding);
        assert_eq!(merge.claim_job_binding(20), Some(old_binding));
        assert_eq!(merge.claim_job_binding(20), Some(new_binding));
        let state = merge.core.context.lock().expect("state");
        assert_ne!(
            state.jobs[&old_binding].template_generation,
            state.jobs[&new_binding].template_generation
        );
    }

    #[test]
    fn an_unbound_job_announcement_preserves_the_following_binding() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        merge
            .apply_to_template(&mut template(84), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("merge template");
        let generation = merge.template_generation(84).expect("generation");

        merge.announce_unbound_job(30);
        merge.bind_job_generation(31, generation, vec![1], vec![2]);
        let merge_binding = current_binding(&merge, 31).expect("merge binding");

        assert_eq!(merge.claim_job_binding(30), None);
        assert_eq!(merge.claim_job_binding(31), Some(merge_binding));
    }

    #[test]
    fn missing_or_reordered_announcement_does_not_shift_later_bindings() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        merge
            .apply_to_template(&mut template(85), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("merge template");
        let generation = merge.template_generation(85).expect("generation");

        merge.announce_unbound_job(40);
        assert!(merge.bind_job_generation(41, generation, vec![1], vec![2]));
        let expected = current_binding(&merge, 41).expect("binding");

        assert_eq!(merge.claim_job_binding(999), None);
        assert_eq!(merge.claim_job_binding(41), Some(expected));
        assert_eq!(merge.claim_job_binding(40), None);
        assert!(merge
            .core
            .context
            .lock()
            .expect("state")
            .job_announcements
            .is_empty());
    }

    #[test]
    fn transaction_count_is_generation_scoped_and_write_once() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        merge
            .apply_to_template(&mut template(86), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("first template");
        let first = merge.template_generation(86).expect("first generation");
        merge.record_transaction_count(first, 2);

        merge
            .apply_to_template(&mut template(86), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("replacement template");
        let second = merge.template_generation(86).expect("second generation");
        assert_ne!(first, second);

        merge.record_transaction_count(first, 99);
        merge.record_transaction_count(second, 3);
        let state = merge.core.context.lock().expect("state");
        assert_eq!(state.templates[&first].block_tx_count, Some(3));
        assert_eq!(state.templates[&second].block_tx_count, Some(4));
    }

    #[test]
    fn missing_reconstruction_context_cannot_create_a_binding() {
        let merge = MergeMining::new();

        assert!(!merge.bind_job_generation(32, 999, vec![1], vec![2]));
        assert!(merge
            .core
            .context
            .lock()
            .expect("state")
            .job_announcements
            .is_empty());
    }

    #[test]
    fn new_template_session_clears_only_mutable_session_indexes() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("pair");
        merge
            .apply_to_template(&mut template(83), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("template");
        merge.record_chain_state(83, [7; 32], 0x207f_ffff);
        merge.bind_job(21, 83, Vec::new(), Vec::new());
        let binding = current_binding(&merge, 21).expect("binding");

        merge.begin_template_session();

        let state = merge.core.context.lock().expect("state");
        assert!(state.current_templates.is_empty());
        assert!(state.current_jobs.is_empty());
        assert!(state.active_chain.is_none());
        assert!(state.job_announcements.is_empty());
        assert!(state.jobs.contains_key(&binding));
        assert!(state
            .templates
            .contains_key(&state.jobs[&binding].template_generation));
    }

    #[test]
    fn future_template_churn_does_not_evict_an_active_job_context() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("pair");
        merge
            .apply_to_template(&mut template(90), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("active template");
        merge.bind_job(40, 90, Vec::new(), Vec::new());
        let binding = current_binding(&merge, 40).expect("active binding");
        assert_eq!(merge.claim_job_binding(40), Some(binding));
        merge.set_active_job_binding(Some(binding));
        let active_generation =
            merge.core.context.lock().expect("state").jobs[&binding].template_generation;

        for template_id in 1_000..1_000 + MAX_TEMPLATES as u64 + 8 {
            let mut future = template(template_id);
            future.future_template = true;
            merge
                .apply_to_template(&mut future, RESERVED_COINBASE_OUTPUT_BYTES as usize)
                .expect("future template");
            let generation = merge
                .template_generation(template_id)
                .expect("future generation");
            let job_id = u32::try_from(template_id).expect("test job id");
            assert!(merge.bind_job_generation(job_id, generation, Vec::new(), Vec::new(),));
            assert!(merge.claim_job_binding(job_id).is_some());
        }

        let state = merge.core.context.lock().expect("state");
        assert!(state.templates.contains_key(&active_generation));
        assert!(state.jobs.contains_key(&binding));
        assert_eq!(state.active_binding, Some(binding));
        assert!(state.template_order.len() <= MAX_TEMPLATES);
    }

    #[test]
    fn queued_job_context_is_protected_until_it_becomes_active() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("pair");

        let bind = |merge: &MergeMining, template_id: u64, job_id: u32| {
            merge
                .apply_to_template(
                    &mut template(template_id),
                    RESERVED_COINBASE_OUTPUT_BYTES as usize,
                )
                .expect("template");
            let generation = merge.template_generation(template_id).expect("generation");
            assert!(merge.bind_job_generation(job_id, generation, Vec::new(), Vec::new(),));
            let binding = merge.claim_job_binding(job_id).expect("binding");
            (generation, binding)
        };

        let (old_generation, old_binding) = bind(&merge, 91, 41);
        merge.set_active_job_binding(Some(old_binding));
        let (queued_generation, queued_binding) = bind(&merge, 92, 42);
        merge.mark_job_binding_pending(queued_binding);

        for index in 0..MAX_TEMPLATES as u64 + 8 {
            let template_id = 4_000 + index;
            let job_id = u32::try_from(template_id).expect("test job id");
            bind(&merge, template_id, job_id);
        }

        {
            let state = merge.core.context.lock().expect("state");
            assert!(state.templates.contains_key(&old_generation));
            assert!(state.templates.contains_key(&queued_generation));
            assert_eq!(state.active_binding, Some(old_binding));
            assert!(state.pending_activation_bindings.contains(&queued_binding));
        }

        merge.set_active_job_binding(Some(queued_binding));
        merge
            .apply_to_template(
                &mut template(5_000),
                RESERVED_COINBASE_OUTPUT_BYTES as usize,
            )
            .expect("next template");
        let state = merge.core.context.lock().expect("state");
        assert!(!state.templates.contains_key(&old_generation));
        assert!(state.templates.contains_key(&queued_generation));
        assert_eq!(state.active_binding, Some(queued_binding));
        assert!(state.pending_activation_bindings.is_empty());
    }

    #[test]
    fn claimed_future_stays_protected_before_prevhash_delivery() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("pair");

        merge
            .apply_to_template(&mut template(93), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("active template");
        merge.bind_job(43, 93, Vec::new(), Vec::new());
        let active_binding = merge.claim_job_binding(43).expect("active binding");
        merge.set_active_job_binding(Some(active_binding));

        merge
            .apply_to_template(&mut template(94), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("future template");
        merge.bind_job(44, 94, Vec::new(), Vec::new());
        let future_binding = merge
            .claim_job_binding_pending(44)
            .expect("claimed future binding");
        let future_generation =
            merge.core.context.lock().expect("state").jobs[&future_binding].template_generation;

        for index in 0..MAX_TEMPLATES as u64 - 2 {
            let template_id = 6_000 + index;
            merge
                .apply_to_template(
                    &mut template(template_id),
                    RESERVED_COINBASE_OUTPUT_BYTES as usize,
                )
                .expect("protected announced template");
            let generation = merge.template_generation(template_id).expect("generation");
            let job_id = u32::try_from(template_id).expect("test job id");
            assert!(merge.bind_job_generation(job_id, generation, Vec::new(), Vec::new(),));
        }

        let mut overflow = template(7_000);
        assert!(!merge
            .apply_to_template(&mut overflow, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("bounded fallback"));
        assert_eq!(overflow.coinbase_tx_outputs_count, 0);
        assert!(merge
            .core
            .context
            .lock()
            .expect("state")
            .templates
            .contains_key(&future_generation));

        merge.clear_pending_job_binding(future_binding);
        assert!(merge
            .apply_to_template(&mut overflow, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("released future can retire"));
        assert!(!merge
            .core
            .context
            .lock()
            .expect("state")
            .templates
            .contains_key(&future_generation));
    }

    #[test]
    fn published_history_pressure_keeps_new_merge_templates_live() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("pair");

        let mut newest_generation = 0;
        for index in 0..MAX_TEMPLATES as u64 + 8 {
            let template_id = 3_000 + index;
            merge
                .apply_to_template(
                    &mut template(template_id),
                    RESERVED_COINBASE_OUTPUT_BYTES as usize,
                )
                .expect("template");
            newest_generation = merge
                .template_generation(template_id)
                .expect("new template must not evict itself");
            merge.bind_job(index as u32, template_id, Vec::new(), Vec::new());
            assert!(merge.claim_job_binding(index as u32).is_some());
        }

        let state = merge.core.context.lock().expect("state");
        assert!(state.templates.contains_key(&newest_generation));
        assert!(state.template_order.len() <= MAX_TEMPLATES);
        assert!(state.jobs.len() <= MAX_TEMPLATES);
    }

    #[test]
    fn early_share_is_completed_after_template_context_arrives() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        let mut template = template(9);
        merge
            .apply_to_template(&mut template, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("injection");

        let payload = Vec::from_hex(PAYLOAD_HEX).expect("payload hex");
        let output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(
                PushBytesBuf::try_from(payload).expect("push bytes"),
            ),
        };
        let marker = [0x51_u8; 8];
        let coinbase = Transaction {
            version: transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(marker.to_vec()),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![output],
        };
        let encoded = serialize(&coinbase);
        let marker_at = encoded
            .windows(marker.len())
            .position(|window| window == marker)
            .expect("marker in coinbase");
        merge.bind_job(
            17,
            9,
            encoded[..marker_at].to_vec(),
            encoded[marker_at + marker.len()..].to_vec(),
        );
        let binding_id = current_binding(&merge, 17).expect("bound job");
        merge.try_observe_share(binding_id, 0x2000_0000, 1_800_000_000, 1, marker.to_vec());
        let generation = merge.template_generation(9).expect("generation");
        merge.record_transaction_count(generation, 0);
        merge.record_chain_state(9, [3; 32], 0x207f_ffff);

        let mut found = None;
        for _ in 0..50 {
            found = merge.take_found_job().expect("queue");
            if found.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let found = found.expect("found job");
        assert_eq!(found.template_id, 9);
        assert_eq!(found.block_header_hex.len(), 160);
        assert!(found.merkle_hashes_hex.is_empty());
        let exported: Transaction =
            deserialize(&Vec::from_hex(&found.coinbase_tx_hex).expect("coinbase hex"))
                .expect("coinbase");
        assert!(exported.input.iter().all(|input| input.witness.is_empty()));
    }

    #[test]
    fn exports_multiple_merkle_siblings_in_template_order() {
        let merge = MergeMining::new();
        let first = std::array::from_fn(|index| index as u8);
        let second = std::array::from_fn(|index| (index as u8).wrapping_add(32));
        let (observation, context) = candidate(EASY_TARGET, vec![first, second], 4, 1);

        process_candidate(&merge.core, observation, context);

        let found = merge.take_found_job().expect("queue").expect("candidate");
        assert_eq!(found.block_tx_count, 4);
        assert_eq!(
            found.merkle_hashes_hex,
            vec![
                first
                    .iter()
                    .rev()
                    .copied()
                    .collect::<Vec<_>>()
                    .as_hex()
                    .to_string(),
                second
                    .iter()
                    .rev()
                    .copied()
                    .collect::<Vec<_>>()
                    .as_hex()
                    .to_string(),
            ]
        );
    }

    #[test]
    fn candidate_above_rsk_target_is_not_queued() {
        let merge = MergeMining::new();
        let (observation, context) = candidate(&"00".repeat(32), Vec::new(), 1, 1);

        process_candidate(&merge.core, observation, context);

        assert!(merge.take_found_job().expect("queue").is_none());
    }

    #[test]
    fn reconstructed_coinbase_rejects_a_later_hidden_raw_tag() {
        let merge = MergeMining::new();
        let (observation, mut context) = candidate(EASY_TARGET, Vec::new(), 1, 1);
        let desired_output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(
                PushBytesBuf::try_from(context.desired.payload.clone()).expect("push bytes"),
            ),
        };
        let mut hidden_script = vec![0x51];
        hidden_script.extend_from_slice(RSK_TAG);
        hidden_script.extend_from_slice(&[0xff; RSK_HASH_BYTES]);
        let marker = [0x51_u8; 8];
        let coinbase = Transaction {
            version: transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(marker.to_vec()),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![
                desired_output,
                TxOut {
                    value: Amount::ZERO,
                    script_pubkey: ScriptBuf::from_bytes(hidden_script),
                },
            ],
        };
        assert!(has_canonical_last_rsk_commitment(
            &coinbase,
            &context.desired
        ));
        let encoded = serialize(&coinbase);
        assert!(!raw_rsk_commitment_matches(&encoded, &context.desired));
        let marker_at = encoded
            .windows(marker.len())
            .position(|window| window == marker)
            .expect("marker");
        context.coinbase_prefix = encoded[..marker_at].to_vec();
        context.coinbase_suffix = encoded[marker_at + marker.len()..].to_vec();

        process_candidate(&merge.core, observation, context);
        assert!(merge.take_found_job().expect("queue").is_none());
    }

    #[test]
    fn raw_rsk_rule_is_inclusive_and_ignores_stripped_witness() {
        let desired = DesiredPair::parse(PAYLOAD_HEX, EASY_TARGET).expect("desired");
        let desired_output = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(
                PushBytesBuf::try_from(desired.payload.clone()).expect("push bytes"),
            ),
        };
        let transaction_with_filler = |filler_len| Transaction {
            version: transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![
                desired_output.clone(),
                TxOut {
                    value: Amount::ZERO,
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51; filler_len]),
                },
            ],
        };
        assert!(raw_rsk_commitment_matches(
            &serialize(&transaction_with_filler(115)),
            &desired
        ));
        assert!(!raw_rsk_commitment_matches(
            &serialize(&transaction_with_filler(116)),
            &desired
        ));

        let mut witness_only_tag = transaction_with_filler(0);
        let mut hidden = RSK_TAG.to_vec();
        hidden.extend_from_slice(&[0xff; RSK_HASH_BYTES]);
        witness_only_tag.input[0].witness.push(hidden);
        assert!(!raw_rsk_commitment_matches(
            &serialize(&witness_only_tag),
            &desired
        ));
        witness_only_tag.input[0].witness.clear();
        assert!(raw_rsk_commitment_matches(
            &serialize(&witness_only_tag),
            &desired
        ));
    }

    #[test]
    fn chain_context_is_coherent_in_either_arrival_order() {
        let before = MergeMining::new();
        before
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        before.record_chain_state(21, [1; 32], 0x1d00_ffff);
        before
            .apply_to_template(&mut template(21), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("template");

        let after = MergeMining::new();
        after
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        after
            .apply_to_template(&mut template(22), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("template");
        after.record_chain_state(22, [2; 32], 0x207f_ffff);

        let before_state = before.core.context.lock().expect("state");
        let before_generation = before_state.current_templates[&21];
        let before_chain = before_state.templates[&before_generation]
            .chain
            .expect("chain");
        assert_eq!(before_chain.prev_hash, [1; 32]);
        assert_eq!(before_chain.n_bits, 0x1d00_ffff);
        let after_state = after.core.context.lock().expect("state");
        let after_generation = after_state.current_templates[&22];
        let after_chain = after_state.templates[&after_generation]
            .chain
            .expect("chain");
        assert_eq!(after_chain.prev_hash, [2; 32]);
        assert_eq!(after_chain.n_bits, 0x207f_ffff);
    }

    #[test]
    fn same_template_prevhash_refresh_does_not_rewrite_bound_job_context() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        merge
            .apply_to_template(&mut template(35), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("template");
        merge.record_chain_state(35, [5; 32], 0x1d00_ffff);
        merge.bind_job(40, 35, vec![1], vec![2]);
        let first_binding = current_binding(&merge, 40).expect("first binding");

        merge.record_chain_state(35, [6; 32], 0x207f_ffff);
        merge.bind_job(41, 35, vec![3], vec![4]);
        let second_binding = current_binding(&merge, 41).expect("second binding");

        let state = merge.core.context.lock().expect("state");
        assert_eq!(
            state.jobs[&first_binding].chain,
            Some(ChainState {
                prev_hash: [5; 32],
                n_bits: 0x1d00_ffff,
            })
        );
        assert_eq!(
            state.jobs[&second_binding].chain,
            Some(ChainState {
                prev_hash: [6; 32],
                n_bits: 0x207f_ffff,
            })
        );
    }

    #[test]
    fn non_future_template_inherits_active_chain_but_future_template_waits() {
        let merge = MergeMining::new();
        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        merge.record_chain_state(31, [3; 32], 0x1d00_ffff);

        let mut non_future = template(32);
        merge
            .apply_to_template(&mut non_future, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("non-future template");

        let mut future = template(33);
        future.future_template = true;
        merge
            .apply_to_template(&mut future, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("future template");

        let mut same_id_future = template(31);
        same_id_future.future_template = true;
        merge
            .apply_to_template(&mut same_id_future, RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("same-ID future template");

        {
            let state = merge.core.context.lock().expect("state");
            let non_future_generation = state.current_templates[&32];
            let inherited = state.templates[&non_future_generation]
                .chain
                .expect("active chain");
            assert_eq!(inherited.prev_hash, [3; 32]);
            assert_eq!(inherited.n_bits, 0x1d00_ffff);
            let future_generation = state.current_templates[&33];
            assert!(state.templates[&future_generation].chain.is_none());
            let same_id_future_generation = state.current_templates[&31];
            assert!(state.templates[&same_id_future_generation].chain.is_none());
        }

        merge.record_chain_state(34, [4; 32], 0x207f_ffff);
        let mut reused_non_future = template(31);
        merge
            .apply_to_template(
                &mut reused_non_future,
                RESERVED_COINBASE_OUTPUT_BYTES as usize,
            )
            .expect("reused-ID non-future template");

        let state = merge.core.context.lock().expect("state");
        let reused_generation = state.current_templates[&31];
        let reused_chain = state.templates[&reused_generation]
            .chain
            .expect("latest active chain");
        assert_eq!(reused_chain.prev_hash, [4; 32]);
        assert_eq!(reused_chain.n_bits, 0x207f_ffff);
    }

    #[test]
    fn found_job_poll_is_fifo_and_destructive() {
        let merge = MergeMining::new();
        let (first_observation, first_context) = candidate(EASY_TARGET, Vec::new(), 1, 1);
        let (second_observation, second_context) = candidate(EASY_TARGET, Vec::new(), 1, 2);
        process_candidate(&merge.core, first_observation, first_context);
        process_candidate(&merge.core, second_observation, second_context);

        let first = merge.take_found_job().expect("queue").expect("first");
        let second = merge.take_found_job().expect("queue").expect("second");
        assert_eq!(first.header_nonce, 1);
        assert_eq!(second.header_nonce, 2);
        assert!(first.id < second.id);
        assert!(merge.take_found_job().expect("queue").is_none());
    }

    #[test]
    fn dashboard_snapshot_reports_state_without_consuming_it() {
        let merge = MergeMining::new();

        let idle = merge.snapshot().expect("idle snapshot");
        assert!(!idle.work_available);
        assert!(idle.active_template_id.is_none());
        assert_eq!(idle.proofs_found, 0);
        assert_eq!(idle.pending_proofs, 0);

        merge
            .set_desired_pair(PAYLOAD_HEX, EASY_TARGET)
            .expect("valid pair");
        merge
            .apply_to_template(&mut template(93), RESERVED_COINBASE_OUTPUT_BYTES as usize)
            .expect("active template");
        merge.bind_job(43, 93, Vec::new(), Vec::new());
        let binding = merge.claim_job_binding(43).expect("active binding");
        merge.set_active_job_binding(Some(binding));
        let (observation, context) = candidate(EASY_TARGET, Vec::new(), 1, 1);
        process_candidate(&merge.core, observation, context);

        let active = merge.snapshot().expect("active snapshot");
        assert!(active.work_available);
        assert_eq!(active.active_template_id, Some(93));
        assert_eq!(active.proofs_found, 1);
        assert_eq!(active.pending_proofs, 1);

        // The bridge stays the only consumer: reading the dashboard twice must
        // leave the queue for it, and draining it must not rewrite the total.
        assert_eq!(merge.snapshot().expect("repeat snapshot").pending_proofs, 1);
        assert!(merge.take_found_job().expect("queue").is_some());
        let drained = merge.snapshot().expect("drained snapshot");
        assert_eq!(drained.pending_proofs, 0);
        assert_eq!(drained.proofs_found, 1);
    }

    #[test]
    fn secret_comparison_checks_content_and_length() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secrex"));
        assert!(!constant_time_eq(b"secret", b"secret-longer"));
    }
}
