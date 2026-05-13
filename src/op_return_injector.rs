use bitcoin::{
    consensus::{Decodable, Encodable},
    hex::FromHex,
    opcodes::all::OP_RETURN,
    script::{Instruction, PushBytesBuf},
    Amount, ScriptBuf, TxOut,
};
use roles_logic_sv2::{mining_sv2::Target as Sv2Target, template_distribution_sv2::NewTemplate};
use std::{
    collections::VecDeque,
    convert::TryInto,
    fmt,
    sync::{Arc, Mutex as StdMutex, OnceLock},
};
use tokio::sync::Mutex;

const MAX_OP_RETURN_PAYLOAD_BYTES: usize = 80;
const MAX_ADDITIONAL_COINBASE_OUTPUT_SIZE: usize = 100;
const MAX_APPLIED_TEMPLATE_PAYLOADS: usize = 128;
static ACTIVE_OP_RETURN_PAYLOAD_HEX: OnceLock<StdMutex<Option<String>>> = OnceLock::new();
pub const RSK_MERGED_MINING_TAG: &[u8] = b"RSKBLOCK:";

#[derive(Debug, Clone, Default)]
pub struct OpReturnInjector {
    desired: Arc<Mutex<Option<DesiredOpReturn>>>,
    applied_to_templates: Arc<StdMutex<VecDeque<AppliedTemplatePayload>>>,
}

#[derive(Clone, Debug)]
struct DesiredOpReturn {
    payload_hex: String,
    payload_bytes: Vec<u8>,
    payload_len_bytes: usize,
    tx_out_len_bytes: usize,
    tx_out_bytes: Vec<u8>,
    rsk_target_hex: Option<String>,
    rsk_target: Option<Sv2Target>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AppliedTemplatePayload {
    pub(crate) template_id: u64,
    pub(crate) payload_hex: String,
    pub(crate) payload_bytes: Vec<u8>,
    pub(crate) rsk_target_hex: Option<String>,
    pub(crate) rsk_target: Option<Sv2Target>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueuedOpReturn {
    pub payload_hex: String,
    pub payload_len_bytes: usize,
    pub tx_out_len_bytes: usize,
    pub replaced_pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedOpReturn {
    pub payload_hex: String,
    pub payload_len_bytes: usize,
    pub tx_out_len_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum OpReturnInjectorError {
    EmptyPayload,
    InvalidHex(String),
    InvalidRskTarget(String),
    PayloadTooLarge(usize),
    TxOutTooLarge(usize),
    TooManyCoinbaseOutputs,
    CoinbaseOutputsTooLarge,
    Encoding(String),
    AppliedPayloadCacheCorrupted,
}

impl fmt::Display for OpReturnInjectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpReturnInjectorError::EmptyPayload => {
                f.write_str("OP_RETURN payload cannot be empty")
            }
            OpReturnInjectorError::InvalidHex(error) => {
                write!(f, "OP_RETURN payload must be hex encoded: {error}")
            }
            OpReturnInjectorError::InvalidRskTarget(error) => {
                write!(f, "RSK target must be a 32-byte big-endian hex string: {error}")
            }
            OpReturnInjectorError::PayloadTooLarge(len) => write!(
                f,
                "OP_RETURN payload is {len} bytes, maximum supported payload is {MAX_OP_RETURN_PAYLOAD_BYTES} bytes"
            ),
            OpReturnInjectorError::TxOutTooLarge(len) => write!(
                f,
                "Serialized OP_RETURN output is {len} bytes, maximum supported size is {MAX_ADDITIONAL_COINBASE_OUTPUT_SIZE} bytes"
            ),
            OpReturnInjectorError::TooManyCoinbaseOutputs => {
                f.write_str("Coinbase output count overflowed")
            }
            OpReturnInjectorError::CoinbaseOutputsTooLarge => {
                f.write_str("Coinbase outputs exceed the SV2 B064K size limit")
            }
            OpReturnInjectorError::Encoding(error) => {
                write!(f, "Failed to encode OP_RETURN output: {error}")
            }
            OpReturnInjectorError::AppliedPayloadCacheCorrupted => {
                f.write_str("Applied OP_RETURN template cache corrupted")
            }
        }
    }
}

impl OpReturnInjector {
    pub async fn queue_from_hex(
        &self,
        data_hex: &str,
    ) -> Result<QueuedOpReturn, OpReturnInjectorError> {
        self.queue_from_hex_with_rsk_target(data_hex, None).await
    }

    pub async fn queue_from_hex_with_rsk_target(
        &self,
        data_hex: &str,
        rsk_target_hex: Option<&str>,
    ) -> Result<QueuedOpReturn, OpReturnInjectorError> {
        let desired = DesiredOpReturn::from_hex_with_rsk_target(data_hex, rsk_target_hex)?;
        let mut guard = self.desired.lock().await;
        let replaced_pending = guard.replace(desired.clone()).is_some();
        set_active_op_return_payload_hex(desired.payload_hex.clone());

        Ok(QueuedOpReturn {
            payload_hex: desired.payload_hex.clone(),
            payload_len_bytes: desired.payload_len_bytes,
            tx_out_len_bytes: desired.tx_out_len_bytes,
            replaced_pending,
        })
    }

    pub async fn apply_pending_to_template<'a>(
        &self,
        template: &mut NewTemplate<'a>,
    ) -> Result<Option<AppliedOpReturn>, OpReturnInjectorError> {
        let desired = {
            let guard = self.desired.lock().await;
            guard.as_ref().cloned()
        };
        let Some(desired) = desired else {
            return Ok(None);
        };

        append_to_template(template, &desired)?;
        self.cache_applied_payload(template.template_id, &desired)?;

        Ok(Some(AppliedOpReturn {
            payload_hex: desired.payload_hex,
            payload_len_bytes: desired.payload_len_bytes,
            tx_out_len_bytes: desired.tx_out_len_bytes,
        }))
    }

    pub(crate) fn applied_payload_for_template(
        &self,
        template_id: u64,
    ) -> Option<AppliedTemplatePayload> {
        self.applied_to_templates.lock().ok().and_then(|applied| {
            applied
                .iter()
                .rev()
                .find(|payload| payload.template_id == template_id)
                .cloned()
        })
    }

    fn cache_applied_payload(
        &self,
        template_id: u64,
        desired: &DesiredOpReturn,
    ) -> Result<(), OpReturnInjectorError> {
        let mut applied = self
            .applied_to_templates
            .lock()
            .map_err(|_| OpReturnInjectorError::AppliedPayloadCacheCorrupted)?;

        if let Some(index) = applied
            .iter()
            .position(|payload| payload.template_id == template_id)
        {
            applied.remove(index);
        } else if applied.len() >= MAX_APPLIED_TEMPLATE_PAYLOADS {
            applied.pop_front();
        }

        applied.push_back(AppliedTemplatePayload {
            template_id,
            payload_hex: desired.payload_hex.clone(),
            payload_bytes: desired.payload_bytes.clone(),
            rsk_target_hex: desired.rsk_target_hex.clone(),
            rsk_target: desired.rsk_target.clone(),
        });
        Ok(())
    }
}

fn active_op_return_payload_hex() -> &'static StdMutex<Option<String>> {
    ACTIVE_OP_RETURN_PAYLOAD_HEX.get_or_init(|| StdMutex::new(None))
}

fn set_active_op_return_payload_hex(payload_hex: String) {
    if let Ok(mut active) = active_op_return_payload_hex().lock() {
        *active = Some(payload_hex);
    }
}

#[allow(dead_code)]
pub fn current_active_op_return_payload_hex() -> Option<String> {
    active_op_return_payload_hex()
        .lock()
        .ok()
        .and_then(|active| active.clone())
}

impl DesiredOpReturn {
    fn from_hex_with_rsk_target(
        data_hex: &str,
        rsk_target_hex: Option<&str>,
    ) -> Result<Self, OpReturnInjectorError> {
        let payload_hex = data_hex.trim().to_ascii_lowercase();
        if payload_hex.is_empty() {
            return Err(OpReturnInjectorError::EmptyPayload);
        }

        let payload_bytes = Vec::from_hex(&payload_hex)
            .map_err(|error| OpReturnInjectorError::InvalidHex(error.to_string()))?;
        if payload_bytes.is_empty() {
            return Err(OpReturnInjectorError::EmptyPayload);
        }
        let payload_len_bytes = payload_bytes.len();
        if payload_len_bytes > MAX_OP_RETURN_PAYLOAD_BYTES {
            return Err(OpReturnInjectorError::PayloadTooLarge(payload_len_bytes));
        }

        let push_bytes = PushBytesBuf::try_from(payload_bytes.clone())
            .map_err(|error| OpReturnInjectorError::Encoding(error.to_string()))?;
        let tx_out = TxOut {
            value: Amount::ZERO,
            script_pubkey: ScriptBuf::new_op_return(push_bytes),
        };
        let mut tx_out_bytes = Vec::new();
        tx_out
            .consensus_encode(&mut tx_out_bytes)
            .map_err(|error| OpReturnInjectorError::Encoding(error.to_string()))?;

        if tx_out_bytes.len() > MAX_ADDITIONAL_COINBASE_OUTPUT_SIZE {
            return Err(OpReturnInjectorError::TxOutTooLarge(tx_out_bytes.len()));
        }

        let (rsk_target_hex, rsk_target) = parse_rsk_target(rsk_target_hex)?;

        Ok(Self {
            payload_hex,
            payload_bytes,
            payload_len_bytes,
            tx_out_len_bytes: tx_out_bytes.len(),
            tx_out_bytes,
            rsk_target_hex,
            rsk_target,
        })
    }
}

fn parse_rsk_target(
    rsk_target_hex: Option<&str>,
) -> Result<(Option<String>, Option<Sv2Target>), OpReturnInjectorError> {
    let Some(rsk_target_hex) = rsk_target_hex
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok((None, None));
    };

    let normalized = rsk_target_hex
        .trim_start_matches("0x")
        .trim_start_matches("0X")
        .to_ascii_lowercase();
    let mut target_bytes = Vec::from_hex(&normalized)
        .map_err(|error| OpReturnInjectorError::InvalidRskTarget(error.to_string()))?;
    if target_bytes.len() != 32 {
        return Err(OpReturnInjectorError::InvalidRskTarget(format!(
            "expected 32 bytes but got {}",
            target_bytes.len()
        )));
    }

    target_bytes.reverse();
    let target = Sv2Target::from(
        <[u8; 32]>::try_from(target_bytes)
            .expect("32-byte target should always convert into a fixed array"),
    );

    Ok((Some(normalized), Some(target)))
}

fn append_to_template<'a>(
    template: &mut NewTemplate<'a>,
    desired: &DesiredOpReturn,
) -> Result<(), OpReturnInjectorError> {
    if template_already_contains_payload(template, desired)? {
        return Ok(());
    }

    let next_outputs_count = template
        .coinbase_tx_outputs_count
        .checked_add(1)
        .ok_or(OpReturnInjectorError::TooManyCoinbaseOutputs)?;

    let mut outputs = template.coinbase_tx_outputs.to_vec();
    outputs.extend_from_slice(&desired.tx_out_bytes);
    template.coinbase_tx_outputs = outputs
        .try_into()
        .map_err(|_| OpReturnInjectorError::CoinbaseOutputsTooLarge)?;
    template.coinbase_tx_outputs_count = next_outputs_count;
    Ok(())
}

fn template_already_contains_payload(
    template: &NewTemplate<'_>,
    desired: &DesiredOpReturn,
) -> Result<bool, OpReturnInjectorError> {
    let bytes = template.coinbase_tx_outputs.to_vec();
    let mut cursor = bytes.as_slice();
    let expect_last_rsk_payload = desired.payload_bytes.starts_with(RSK_MERGED_MINING_TAG);
    let mut matching_payload_seen = false;
    let mut last_rsk_payload = None;

    for _ in 0..template.coinbase_tx_outputs_count {
        let tx_out = TxOut::consensus_decode(&mut cursor)
            .map_err(|error| OpReturnInjectorError::Encoding(error.to_string()))?;
        if let Some(payload) = extract_single_op_return_pushdata(&tx_out.script_pubkey) {
            if payload == desired.payload_bytes {
                matching_payload_seen = true;
            }
            if expect_last_rsk_payload && payload.starts_with(RSK_MERGED_MINING_TAG) {
                last_rsk_payload = Some(payload);
            }
        }
    }

    if expect_last_rsk_payload {
        Ok(last_rsk_payload
            .as_deref()
            .is_some_and(|payload| payload == desired.payload_bytes.as_slice()))
    } else {
        Ok(matching_payload_seen)
    }
}

fn extract_single_op_return_pushdata(script_pubkey: &ScriptBuf) -> Option<Vec<u8>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use binary_sv2::{Seq0255, B0255, B064K, U256};
    use bitcoin::{consensus::Decodable, hex::FromHex};

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

    fn decode_only_tx_out(bytes: &[u8]) -> TxOut {
        let mut bytes = bytes;
        let tx_out = TxOut::consensus_decode(&mut bytes).expect("valid tx out");
        assert!(bytes.is_empty(), "expected a single serialized tx out");
        tx_out
    }

    #[tokio::test]
    async fn queued_payload_remains_applied_across_successive_templates_until_replaced() {
        let injector = OpReturnInjector::default();
        injector
            .queue_from_hex("deadbeef")
            .await
            .expect("queue should succeed");

        let mut first_template = make_new_template(11);
        let mut second_template = make_new_template(12);

        let first_apply = injector
            .apply_pending_to_template(&mut first_template)
            .await
            .expect("first apply should succeed")
            .expect("desired output should be applied");
        let second_apply = injector
            .apply_pending_to_template(&mut second_template)
            .await
            .expect("second apply should succeed")
            .expect("desired output should still be applied");

        assert_eq!(first_apply.payload_hex, "deadbeef");
        assert_eq!(second_apply.payload_hex, "deadbeef");
        assert_eq!(first_template.coinbase_tx_outputs_count, 1);
        assert_eq!(second_template.coinbase_tx_outputs_count, 1);
        assert_eq!(
            injector
                .applied_payload_for_template(11)
                .expect("template 11 should be tracked")
                .payload_hex,
            "deadbeef"
        );
        assert_eq!(
            injector
                .applied_payload_for_template(12)
                .expect("template 12 should be tracked")
                .payload_hex,
            "deadbeef"
        );
    }

    #[tokio::test]
    async fn replacing_payload_updates_later_templates_without_mutating_older_templates() {
        let injector = OpReturnInjector::default();
        let mut template_a = make_new_template(21);
        let mut template_b = make_new_template(22);

        injector
            .queue_from_hex("aa")
            .await
            .expect("first payload should queue");
        injector
            .apply_pending_to_template(&mut template_a)
            .await
            .expect("template A apply should succeed")
            .expect("template A should get first payload");

        injector
            .queue_from_hex("bb")
            .await
            .expect("replacement payload should queue");
        injector
            .apply_pending_to_template(&mut template_b)
            .await
            .expect("template B apply should succeed")
            .expect("template B should get replacement payload");

        assert_eq!(
            decode_only_tx_out(&template_a.coinbase_tx_outputs.to_vec()).script_pubkey,
            ScriptBuf::new_op_return(PushBytesBuf::try_from(Vec::from_hex("aa").unwrap()).unwrap())
        );
        assert_eq!(
            decode_only_tx_out(&template_b.coinbase_tx_outputs.to_vec()).script_pubkey,
            ScriptBuf::new_op_return(PushBytesBuf::try_from(Vec::from_hex("bb").unwrap()).unwrap())
        );
        assert_eq!(
            injector
                .applied_payload_for_template(21)
                .expect("template A should stay associated to old payload")
                .payload_hex,
            "aa"
        );
        assert_eq!(
            injector
                .applied_payload_for_template(22)
                .expect("template B should use replacement payload")
                .payload_hex,
            "bb"
        );
    }

    #[tokio::test]
    async fn incompatible_template_is_left_untouched_and_later_template_still_gets_desired_payload()
    {
        let injector = OpReturnInjector::default();
        injector
            .queue_from_hex("deadbeef")
            .await
            .expect("payload should queue");

        let mut incompatible_template = make_new_template(31);
        incompatible_template.coinbase_tx_outputs = vec![0_u8; u16::MAX as usize]
            .try_into()
            .expect("max-sized B064K should be allowed");

        let apply_error = injector
            .apply_pending_to_template(&mut incompatible_template)
            .await
            .expect_err("oversized template must be rejected");

        assert_eq!(apply_error, OpReturnInjectorError::CoinbaseOutputsTooLarge);
        assert_eq!(incompatible_template.coinbase_tx_outputs_count, 0);
        assert_eq!(
            incompatible_template.coinbase_tx_outputs.to_vec().len(),
            u16::MAX as usize
        );
        assert!(
            injector.applied_payload_for_template(31).is_none(),
            "failed application must not mark template as injected"
        );

        let mut later_template = make_new_template(32);
        let later_apply = injector
            .apply_pending_to_template(&mut later_template)
            .await
            .expect("later compatible template should succeed")
            .expect("desired payload should remain active");

        assert_eq!(later_apply.payload_hex, "deadbeef");
        assert_eq!(later_template.coinbase_tx_outputs_count, 1);
        assert_eq!(
            injector
                .applied_payload_for_template(32)
                .expect("later template should be tracked")
                .payload_hex,
            "deadbeef"
        );
    }

    #[tokio::test]
    async fn does_not_duplicate_payload_within_same_template() {
        let injector = OpReturnInjector::default();
        injector
            .queue_from_hex("deadbeef")
            .await
            .expect("payload should queue");

        let mut template = make_new_template(41);
        injector
            .apply_pending_to_template(&mut template)
            .await
            .expect("first apply should succeed")
            .expect("payload should apply");
        injector
            .apply_pending_to_template(&mut template)
            .await
            .expect("second apply should succeed")
            .expect("payload should still be treated as applied");

        assert_eq!(template.coinbase_tx_outputs_count, 1);
    }

    #[tokio::test]
    async fn appends_desired_rsk_payload_when_an_older_matching_tag_is_not_last() {
        let injector = OpReturnInjector::default();
        let desired_payload_hex = "52534b424c4f434b3adeadbeef";
        let desired_payload =
            Vec::from_hex(desired_payload_hex).expect("desired payload should decode");
        let later_payload =
            Vec::from_hex("52534b424c4f434b3acafebabe").expect("later payload should decode");
        let mut template = make_new_template(51);
        let mut outputs = Vec::new();

        for payload in [&desired_payload, &later_payload] {
            let tx_out = TxOut {
                value: Amount::ZERO,
                script_pubkey: ScriptBuf::new_op_return(
                    PushBytesBuf::try_from(payload.clone()).expect("payload should fit"),
                ),
            };
            tx_out
                .consensus_encode(&mut outputs)
                .expect("Vec<u8> should encode");
        }
        template.coinbase_tx_outputs_count = 2;
        template.coinbase_tx_outputs = outputs
            .try_into()
            .expect("serialized outputs should fit in B064K");

        injector
            .queue_from_hex(desired_payload_hex)
            .await
            .expect("payload should queue");
        injector
            .apply_pending_to_template(&mut template)
            .await
            .expect("template apply should succeed")
            .expect("payload should still be reported as applied");

        assert_eq!(
            template.coinbase_tx_outputs_count, 3,
            "the desired payload must be appended again so it becomes the last RSKBLOCK tag",
        );

        let mut cursor = template.coinbase_tx_outputs.as_ref();
        let mut last_payload = None;
        for _ in 0..template.coinbase_tx_outputs_count {
            let tx_out = TxOut::consensus_decode(&mut cursor).expect("valid tx out");
            last_payload = extract_single_op_return_pushdata(&tx_out.script_pubkey);
        }
        assert_eq!(last_payload, Some(desired_payload));
    }

    #[tokio::test]
    async fn rejects_invalid_hex_payload() {
        let injector = OpReturnInjector::default();
        let error = injector
            .queue_from_hex("zz")
            .await
            .expect_err("invalid hex should be rejected");

        assert!(matches!(error, OpReturnInjectorError::InvalidHex(_)));
    }

    #[tokio::test]
    async fn queue_from_hex_with_rsk_target_tracks_normalized_target_per_template() {
        let injector = OpReturnInjector::default();
        let mut template = make_new_template(61);
        let rsk_target_hex = "0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

        injector
            .queue_from_hex_with_rsk_target("deadbeef", Some(rsk_target_hex))
            .await
            .expect("payload should queue with an RSK target");
        injector
            .apply_pending_to_template(&mut template)
            .await
            .expect("template apply should succeed")
            .expect("template should receive payload");

        let applied = injector
            .applied_payload_for_template(61)
            .expect("template should retain applied payload metadata");
        assert_eq!(applied.payload_hex, "deadbeef");
        assert_eq!(
            applied.rsk_target_hex.as_deref(),
            Some("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
        );
        assert!(applied.rsk_target.is_some());
    }

    #[tokio::test]
    async fn rejects_payloads_larger_than_standard_op_return() {
        let injector = OpReturnInjector::default();
        let payload = "aa".repeat(MAX_OP_RETURN_PAYLOAD_BYTES + 1);
        let error = injector
            .queue_from_hex(&payload)
            .await
            .expect_err("oversized payload should be rejected");

        assert_eq!(
            error,
            OpReturnInjectorError::PayloadTooLarge(MAX_OP_RETURN_PAYLOAD_BYTES + 1)
        );
    }
}
