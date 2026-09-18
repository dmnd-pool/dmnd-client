use std::{
    collections::{HashMap, VecDeque},
    sync::{atomic::Ordering::Relaxed, OnceLock, RwLock},
    time::Instant,
    time::{SystemTime, UNIX_EPOCH},
};

use bitcoin::{Transaction, Txid};
use roles_logic_sv2::template_distribution_sv2::NewTemplate;

/// One transaction in the node's template.
#[derive(Debug, Clone)]
pub struct TemplateTx {
    pub txid: Txid,
    pub weight: u64,
    pub vsize: u64,
    pub fee_sat: Option<u64>,
}

/// A received template, as the dashboard sees it.
#[derive(Debug, Clone)]
pub struct TemplateSnapshot {
    pub template_id: u64,
    pub future_template: bool,
    pub version: u32,
    /// Subsidy plus fees.
    pub coinbase_tx_value_remaining: u64,
    /// Decoded from the coinbase prefix.
    pub height: Option<u32>,
    /// `None` when the height would not decode.
    pub subsidy_sat: Option<u64>,
    pub total_fees_sat: Option<u64>,
    pub transactions: Vec<TemplateTx>,
    pub total_weight: u64,
    pub received_at: u64,
}

/// How the candidate to declare gets chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeclarationPolicy {
    #[default]
    HighestFees,
    BlockWeight,
}

impl DeclarationPolicy {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "highest_fees" => Some(Self::HighestFees),
            "block_weight" => Some(Self::BlockWeight),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::HighestFees => "highest_fees",
            Self::BlockWeight => "block_weight",
        }
    }

    /// Rank a candidate; the highest score wins.
    fn score(self, snapshot: &TemplateSnapshot) -> u128 {
        match self {
            Self::HighestFees => u128::from(snapshot.total_fees_sat.unwrap_or(0)),
            Self::BlockWeight => u128::from(snapshot.total_weight),
        }
    }
}

/// How many candidates to keep for the current tip.
pub const CANDIDATE_LIMIT: usize = 3;

static POLICY: OnceLock<RwLock<DeclarationPolicy>> = OnceLock::new();

struct Pending {
    sent_at: Instant,
}

#[derive(Default)]
struct TemplateState {
    candidates: VecDeque<TemplateSnapshot>,
    pending: HashMap<u64, Pending>,
    active: Option<u64>,
}

static STATE: OnceLock<RwLock<TemplateState>> = OnceLock::new();

fn state() -> &'static RwLock<TemplateState> {
    STATE.get_or_init(|| RwLock::new(TemplateState::default()))
}

/// The policy in use.
fn policy_slot() -> &'static RwLock<DeclarationPolicy> {
    POLICY.get_or_init(|| {
        let configured = std::env::var("DECLARATION_POLICY")
            .ok()
            .and_then(|value| DeclarationPolicy::parse(&value));
        RwLock::new(configured.unwrap_or_default())
    })
}

/// The declaration the pool has accepted, if any.
pub fn active_declaration() -> Option<u64> {
    state()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .active
}

pub fn declaration_policy() -> DeclarationPolicy {
    *policy_slot()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn set_declaration_policy(policy: DeclarationPolicy) {
    *policy_slot()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = policy;
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Decode the block height from the coinbase prefix, as BIP34 requires.
fn bip34_height(coinbase_prefix: &[u8]) -> Option<u32> {
    let (&push_len, rest) = coinbase_prefix.split_first()?;
    let push_len = push_len as usize;
    if push_len == 0 || push_len > 5 || rest.len() < push_len {
        return None;
    }
    let mut height: u64 = 0;
    for (index, byte) in rest[..push_len].iter().enumerate() {
        height |= u64::from(*byte) << (8 * index);
    }
    u32::try_from(height).ok()
}

/// The block subsidy at `height`, in satoshis.
fn block_subsidy_sat(height: u32) -> u64 {
    const HALVING_INTERVAL: u32 = 210_000;
    const INITIAL_SUBSIDY_SAT: u64 = 50 * 100_000_000;
    let halvings = height / HALVING_INTERVAL;
    if halvings >= 64 {
        return 0;
    }
    INITIAL_SUBSIDY_SAT >> halvings
}

/// Fees provable from the template alone: txs spending outputs of txs in it.
fn price_from_template(transactions: &[Transaction]) -> HashMap<Txid, u64> {
    let outputs_by_txid: HashMap<Txid, &Vec<bitcoin::TxOut>> = transactions
        .iter()
        .map(|tx| (tx.compute_txid(), &tx.output))
        .collect();

    let mut fees = HashMap::new();
    for tx in transactions {
        let mut input_total: u64 = 0;
        let mut all_resolved = true;
        for input in &tx.input {
            let previous = outputs_by_txid
                .get(&input.previous_output.txid)
                .and_then(|outputs| outputs.get(input.previous_output.vout as usize));
            match previous {
                Some(output) => input_total = input_total.saturating_add(output.value.to_sat()),
                None => {
                    all_resolved = false;
                    break;
                }
            }
        }
        if !all_resolved {
            continue;
        }
        let output_total: u64 = tx.output.iter().map(|out| out.value.to_sat()).sum();
        // Guard against underflow.
        if let Some(fee) = input_total.checked_sub(output_total) {
            fees.insert(tx.compute_txid(), fee);
        }
    }
    fees
}

/// Record a template as the newest candidate.
pub fn template_received(template: &NewTemplate<'_>, transactions: &[Transaction]) {
    record_template(
        template.template_id,
        template.future_template,
        template.version,
        template.coinbase_tx_value_remaining,
        template.coinbase_prefix.inner_as_ref(),
        transactions,
    );
}

fn record_template(
    template_id: u64,
    future_template: bool,
    version: u32,
    coinbase_tx_value_remaining: u64,
    coinbase_prefix: &[u8],
    transactions: &[Transaction],
) {
    let template_fees = price_from_template(transactions);
    let transactions: Vec<TemplateTx> = transactions
        .iter()
        .map(|tx| {
            let txid = tx.compute_txid();
            TemplateTx {
                fee_sat: template_fees.get(&txid).copied(),
                txid,
                weight: tx.weight().to_wu(),
                vsize: tx.vsize() as u64,
            }
        })
        .collect();
    let total_weight = transactions.iter().map(|tx| tx.weight).sum();

    // Coinbase value is subsidy + fees, so fees follow from the height.
    let height = bip34_height(coinbase_prefix);
    let subsidy_sat = height.map(block_subsidy_sat);
    let total_fees_sat =
        subsidy_sat.map(|subsidy| coinbase_tx_value_remaining.saturating_sub(subsidy));

    let snapshot = TemplateSnapshot {
        template_id,
        future_template,
        version,
        coinbase_tx_value_remaining,
        height,
        subsidy_sat,
        total_fees_sat,
        transactions,
        total_weight,
        received_at: unix_now(),
    };

    let mut state = state()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state
        .candidates
        .retain(|candidate| candidate.template_id != template_id);
    state.candidates.push_front(snapshot);
    state.candidates.truncate(CANDIDATE_LIMIT);
}

/// Read the candidate set in place, newest first, without cloning.
pub fn with_candidates<T>(f: impl FnOnce(&VecDeque<TemplateSnapshot>) -> T) -> T {
    let state = state()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f(&state.candidates)
}

#[cfg(test)]
fn current() -> Option<TemplateSnapshot> {
    with_candidates(|held| held.front().cloned())
}

#[cfg(test)]
fn recent() -> Vec<TemplateSnapshot> {
    with_candidates(|held| held.iter().cloned().collect())
}

#[cfg(test)]
fn has_pending_declaration(template_id: u64) -> bool {
    state()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .pending
        .contains_key(&template_id)
}

pub fn reset() {
    *state()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = TemplateState::default();
    crate::api::stats::CONNECTION_STARTED_AT.store(unix_now(), Relaxed);
    crate::api::stats::SENT_BYTES.store(0, Relaxed);
    crate::api::stats::RECEIVED_BYTES.store(0, Relaxed);
    crate::api::stats::DECLARATION_MS.store(0, Relaxed);
}

#[cfg(test)]
pub static TEST_HISTORY_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub fn clear_history_for_tests() {
    reset();
    crate::api::stats::clear_declaration_latency_for_tests();
}

/// Which candidate `policy` would declare, if any are held.
pub fn policy_pick(policy: DeclarationPolicy) -> Option<u64> {
    let state = state()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state
        .candidates
        .iter()
        .rev()
        .max_by_key(|snapshot| policy.score(snapshot))
        .map(|snapshot| snapshot.template_id)
}

/// Drop all candidates and pending entries whose template differs from `template_id`,
/// and clear the active template if it no longer matches.
///
/// This is called when a new best tip arrives so stale templates from the previous tip are not
/// accidentally declared.
pub fn clear_for_new_tip(template_id: u64) {
    let mut state = state()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state
        .candidates
        .retain(|candidate| candidate.template_id == template_id);
    state
        .pending
        .retain(|pending_id, _| *pending_id == template_id);
    if state.active != Some(template_id) {
        state.active = None;
    }
}

/// A `DeclareMiningJob` has gone out for this template.
pub fn declaration_sent(template_id: u64) {
    state()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .pending
        .insert(
            template_id,
            Pending {
                sent_at: Instant::now(),
            },
        );
}
/// Remove the pending entry for a template if the pool rejected it.
pub fn declaration_rejected(template_id: u64) {
    state()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .pending
        .remove(&template_id);
}

/// The pool accepted the declaration: mark it active.
pub fn declaration_accepted(template_id: u64) {
    let mut state = state()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(pending) = state.pending.remove(&template_id) else {
        return;
    };
    if state
        .candidates
        .iter()
        .any(|candidate| candidate.template_id == template_id)
    {
        state.active = Some(template_id);
    }

    crate::api::stats::record_declaration_latency(pending.sent_at.elapsed().as_millis() as u64);
}

/// One candidate by id.
pub fn by_id(template_id: u64) -> Option<TemplateSnapshot> {
    state()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .candidates
        .iter()
        .find(|snapshot| snapshot.template_id == template_id)
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{
        absolute::LockTime, transaction::Version, Amount, OutPoint, ScriptBuf, Sequence, TxIn,
        TxOut, Witness,
    };

    fn tx(nonce: u32) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()),
                    vout: nonce,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1_000 + nonce as u64),
                script_pubkey: ScriptBuf::new(),
            }],
        }
    }

    #[test]
    fn bip34_height_decodes_the_coinbase_prefix() {
        // Mainnet height 961_143 is a three-byte little-endian push.
        assert_eq!(bip34_height(&[0x03, 0x77, 0xaa, 0x0e]), Some(961_143));
        // A four-byte push.
        assert_eq!(
            bip34_height(&[0x04, 0x80, 0x00, 0x80, 0x00]),
            Some(8_388_736)
        );
        // Trailing bytes (miner tag) are ignored.
        assert_eq!(
            bip34_height(&[0x03, 0x77, 0xaa, 0x0e, 0xde, 0xad, 0xbe, 0xef]),
            Some(961_143)
        );
        // Invalid prefixes decode to nothing.
        assert_eq!(bip34_height(&[]), None);
        assert_eq!(bip34_height(&[0x00]), None);
        assert_eq!(
            bip34_height(&[0x03, 0x77]),
            None,
            "push runs past the prefix"
        );
        assert_eq!(bip34_height(&[0x51]), None, "OP_1 is not a direct push");
    }

    #[test]
    fn block_subsidy_follows_the_halving_schedule() {
        assert_eq!(block_subsidy_sat(0), 5_000_000_000);
        assert_eq!(block_subsidy_sat(209_999), 5_000_000_000);
        assert_eq!(block_subsidy_sat(210_000), 2_500_000_000);
        assert_eq!(block_subsidy_sat(630_000), 625_000_000);
        // The epoch mainnet is in at the time of writing: 3.125 BTC.
        assert_eq!(block_subsidy_sat(961_143), 312_500_000);
        assert_eq!(block_subsidy_sat(64 * 210_000), 0);
    }

    // Only a child spending a parent in the same template has a provable fee.
    #[test]
    fn template_prices_only_fully_resolvable_chains() {
        let parent = tx(40);
        let parent_txid = parent.compute_txid();
        let parent_value = parent.output[0].value.to_sat();

        let mut child = tx(41);
        child.input[0].previous_output = OutPoint {
            txid: parent_txid,
            vout: 0,
        };
        child.output[0].value = Amount::from_sat(parent_value - 700);

        let fees = price_from_template(&[parent.clone(), child.clone()]);

        // The child's only input is a parent output, so its fee is exact.
        assert_eq!(fees.get(&child.compute_txid()), Some(&700));
        // The parent's input value is unknown, so it stays unpriced.
        assert_eq!(fees.get(&parent_txid), None);
    }

    #[test]
    fn policies_rank_candidates() {
        let _guard = TEST_HISTORY_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clear_history_for_tests();

        let prefix = [0x03, 0x77, 0xaa, 0x0e];
        let subsidy = block_subsidy_sat(961_143);

        // Large witness makes its template the heaviest.
        let mut heavy = tx(4);
        heavy.input[0].witness.push([0u8; 400]);

        // Oldest: least fees.
        record_template(1, true, 0, subsidy + 1_000, &prefix, &[tx(1), tx(2), tx(3)]);
        // Fullest block, but not the richest.
        record_template(
            2,
            false,
            0,
            subsidy + 2_000,
            &prefix,
            &[heavy, tx(5), tx(7)],
        );
        // Newest: richest, on one small transaction.
        record_template(3, false, 0, subsidy + 9_000, &prefix, &[tx(6)]);

        assert_eq!(policy_pick(DeclarationPolicy::HighestFees), Some(3));
        assert_eq!(policy_pick(DeclarationPolicy::BlockWeight), Some(2));

        // Wire-name round trip.
        for policy in [
            DeclarationPolicy::HighestFees,
            DeclarationPolicy::BlockWeight,
        ] {
            assert_eq!(DeclarationPolicy::parse(policy.as_str()), Some(policy));
        }
        assert_eq!(DeclarationPolicy::parse("nonsense"), None);
        assert_eq!(DeclarationPolicy::parse("manual"), None);
        assert_eq!(DeclarationPolicy::parse("best_fee_rate"), None);
        assert_eq!(DeclarationPolicy::parse("most_transactions"), None);
    }

    #[test]
    fn the_tip_moving_keeps_only_the_template_it_names() {
        let _guard = TEST_HISTORY_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clear_history_for_tests();

        let prefix = [0x03, 0x77, 0xaa, 0x0e];
        record_template(20, true, 0, 0, &prefix, &[tx(1)]);
        record_template(21, false, 0, 0, &prefix, &[tx(2)]);
        declaration_sent(20);
        declaration_accepted(20);
        assert_eq!(active_declaration(), Some(20));

        // The tip moves to 21, so only 20 went stale.
        clear_for_new_tip(21);
        assert_eq!(
            recent().iter().map(|s| s.template_id).collect::<Vec<_>>(),
            vec![21]
        );
        assert_eq!(policy_pick(DeclarationPolicy::HighestFees), Some(21));
        assert_eq!(active_declaration(), None, "20 belonged to the old tip");

        declaration_sent(20);
        declaration_accepted(20);
        assert_eq!(active_declaration(), None, "20 is not a candidate any more");

        // A candidate for the new tip is accepted as normal.
        record_template(31, true, 0, 0, &prefix, &[tx(2)]);
        declaration_sent(31);
        declaration_accepted(31);
        assert_eq!(active_declaration(), Some(31));

        // A tip naming a template that was never recorded leaves nothing.
        clear_for_new_tip(999);
        assert!(recent().is_empty());
        assert_eq!(active_declaration(), None);
    }

    // The pool only ever accepts the custom job after `SetNewPrevHash` for the
    // same template, so the tip change must not drop the pending declaration.
    #[test]
    fn a_declaration_accepted_after_the_tip_moved_still_goes_active() {
        let _guard = TEST_HISTORY_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clear_history_for_tests();

        let prefix = [0x03, 0x77, 0xaa, 0x0e];
        record_template(40, true, 0, 0, &prefix, &[tx(1)]);
        record_template(41, true, 0, 0, &prefix, &[tx(2)]);
        declaration_sent(40);
        declaration_sent(41);

        // The template provider promotes 41 to the tip.
        clear_for_new_tip(41);
        assert!(has_pending_declaration(41));
        assert!(
            !has_pending_declaration(40),
            "40 lost its tip, so its declaration is moot"
        );

        // The pool's acceptance arrives afterwards, and still counts.
        declaration_accepted(41);
        assert_eq!(active_declaration(), Some(41));
        assert!(crate::api::stats::declaration_latency_ms().is_some());
        assert!(
            !has_pending_declaration(41),
            "acceptance consumes the pending entry"
        );

        // A stale acceptance cannot revive a template the tip change dropped.
        declaration_accepted(40);
        assert_eq!(active_declaration(), Some(41));
    }

    // One process-global candidate set, so all cases share one test.
    #[test]
    fn recording_templates_keeps_a_bounded_newest_first_history() {
        let _guard = TEST_HISTORY_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clear_history_for_tests();

        // Height 961_143, so a 3.125 BTC subsidy and the rest fees.
        let prefix = [0x03, 0x77, 0xaa, 0x0e];
        let txs = vec![tx(1), tx(2), tx(3)];
        record_template(7, true, 0x2000_0000, 316_299_592, &prefix, &txs);

        let snapshot = current().expect("a template was recorded");
        assert_eq!(snapshot.template_id, 7);
        assert!(snapshot.future_template);
        assert_eq!(snapshot.total_fees_sat, Some(3_799_592));
        assert_eq!(snapshot.transactions.len(), 3);
        assert_eq!(snapshot.transactions[0].txid, txs[0].compute_txid());
        assert_eq!(
            snapshot.total_weight,
            txs.iter().map(|t| t.weight().to_wu()).sum::<u64>()
        );

        // The earlier template stays on record.
        record_template(8, false, 0, 0, &prefix, &[tx(10), tx(11)]);
        let snapshot = current().expect("a template was recorded");
        assert_eq!(snapshot.template_id, 8);
        assert!(!snapshot.future_template);
        assert_eq!(snapshot.transactions.len(), 2);
        assert_eq!(by_id(7).expect("template 7 is held").transactions.len(), 3);
        assert_eq!(
            recent().iter().map(|s| s.template_id).collect::<Vec<_>>(),
            vec![8, 7]
        );

        // A repeated id replaces the held copy.
        record_template(8, false, 0, 0, &prefix, &[tx(12)]);
        assert_eq!(by_id(8).expect("template 8 is held").transactions.len(), 1);
        assert_eq!(
            recent().iter().map(|s| s.template_id).collect::<Vec<_>>(),
            vec![8, 7]
        );

        // An undecodable prefix leaves the fee total unknown rather than wrong.
        record_template(9, false, 0, 316_299_592, &[], &[tx(13)]);
        let snapshot = by_id(9).expect("template 9 is held");
        assert_eq!(snapshot.height, None);
        assert_eq!(snapshot.total_fees_sat, None);

        // The history is bounded, and drops the oldest first.
        for id in 100..100 + CANDIDATE_LIMIT as u64 {
            record_template(id, false, 0, 0, &prefix, &[tx(1)]);
        }
        let held = recent();
        assert_eq!(held.len(), CANDIDATE_LIMIT);
        assert_eq!(held[0].template_id, 100 + CANDIDATE_LIMIT as u64 - 1);
        assert!(by_id(7).is_none(), "the oldest templates are evicted");
    }
}
