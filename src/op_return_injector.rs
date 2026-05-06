use bitcoin::{consensus::Encodable, hex::FromHex, script::PushBytesBuf, Amount, ScriptBuf, TxOut};
use roles_logic_sv2::template_distribution_sv2::NewTemplate;
use std::{
    convert::TryInto,
    fmt,
    sync::{Arc, Mutex as StdMutex, OnceLock},
};
use tokio::sync::Mutex;

const MAX_OP_RETURN_PAYLOAD_BYTES: usize = 80;
const MAX_ADDITIONAL_COINBASE_OUTPUT_SIZE: usize = 100;
static ACTIVE_OP_RETURN_PAYLOAD_HEX: OnceLock<StdMutex<Option<String>>> = OnceLock::new();

#[derive(Clone, Default)]
pub struct OpReturnInjector {
    pending: Arc<Mutex<Option<PendingOpReturn>>>,
}

#[derive(Clone, Debug)]
struct PendingOpReturn {
    payload_hex: String,
    payload_len_bytes: usize,
    tx_out_len_bytes: usize,
    tx_out_bytes: Vec<u8>,
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
    PayloadTooLarge(usize),
    TxOutTooLarge(usize),
    TooManyCoinbaseOutputs,
    CoinbaseOutputsTooLarge,
    Encoding(String),
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
        }
    }
}

impl OpReturnInjector {
    pub async fn queue_from_hex(
        &self,
        data_hex: &str,
    ) -> Result<QueuedOpReturn, OpReturnInjectorError> {
        let pending = PendingOpReturn::from_hex(data_hex)?;
        let mut guard = self.pending.lock().await;
        let replaced_pending = guard.replace(pending.clone()).is_some();

        Ok(QueuedOpReturn {
            payload_hex: pending.payload_hex.clone(),
            payload_len_bytes: pending.payload_len_bytes,
            tx_out_len_bytes: pending.tx_out_len_bytes,
            replaced_pending,
        })
    }

    pub async fn apply_pending_to_template<'a>(
        &self,
        template: &mut NewTemplate<'a>,
    ) -> Result<Option<AppliedOpReturn>, OpReturnInjectorError> {
        let mut guard = self.pending.lock().await;
        let Some(pending) = guard.as_ref().cloned() else {
            return Ok(None);
        };

        append_to_template(template, &pending)?;
        set_active_op_return_payload_hex(pending.payload_hex.clone());
        *guard = None;

        Ok(Some(AppliedOpReturn {
            payload_hex: pending.payload_hex,
            payload_len_bytes: pending.payload_len_bytes,
            tx_out_len_bytes: pending.tx_out_len_bytes,
        }))
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

impl PendingOpReturn {
    fn from_hex(data_hex: &str) -> Result<Self, OpReturnInjectorError> {
        let payload_hex = data_hex.trim().to_ascii_lowercase();
        if payload_hex.is_empty() {
            return Err(OpReturnInjectorError::EmptyPayload);
        }

        let payload = Vec::from_hex(&payload_hex)
            .map_err(|error| OpReturnInjectorError::InvalidHex(error.to_string()))?;
        if payload.is_empty() {
            return Err(OpReturnInjectorError::EmptyPayload);
        }
        let payload_len_bytes = payload.len();
        if payload_len_bytes > MAX_OP_RETURN_PAYLOAD_BYTES {
            return Err(OpReturnInjectorError::PayloadTooLarge(payload_len_bytes));
        }

        let push_bytes = PushBytesBuf::try_from(payload)
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

        Ok(Self {
            payload_hex,
            payload_len_bytes,
            tx_out_len_bytes: tx_out_bytes.len(),
            tx_out_bytes,
        })
    }
}

fn append_to_template<'a>(
    template: &mut NewTemplate<'a>,
    pending: &PendingOpReturn,
) -> Result<(), OpReturnInjectorError> {
    let next_outputs_count = template
        .coinbase_tx_outputs_count
        .checked_add(1)
        .ok_or(OpReturnInjectorError::TooManyCoinbaseOutputs)?;

    let mut outputs = template.coinbase_tx_outputs.to_vec();
    outputs.extend_from_slice(&pending.tx_out_bytes);
    template.coinbase_tx_outputs = outputs
        .try_into()
        .map_err(|_| OpReturnInjectorError::CoinbaseOutputsTooLarge)?;
    template.coinbase_tx_outputs_count = next_outputs_count;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_sv2::{Seq0255, B0255, B064K, U256};
    use bitcoin::{consensus::Decodable, hex::FromHex};

    fn make_new_template() -> NewTemplate<'static> {
        let coinbase_prefix: B0255<'static> = Vec::new()
            .try_into()
            .expect("coinbase prefix should fit in B0255");
        let coinbase_tx_outputs: B064K<'static> = Vec::new()
            .try_into()
            .expect("coinbase outputs should fit in B064K");
        let merkle_path: Seq0255<'static, U256<'static>> = Vec::new().into();
        NewTemplate {
            template_id: 1,
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
    async fn queue_and_apply_appends_zero_value_op_return() {
        let injector = OpReturnInjector::default();
        let queued = injector
            .queue_from_hex("deadbeef")
            .await
            .expect("queue should succeed");
        let mut template = make_new_template();

        let applied = injector
            .apply_pending_to_template(&mut template)
            .await
            .expect("apply should succeed")
            .expect("pending output should be applied");

        assert_eq!(queued.payload_len_bytes, 4);
        assert_eq!(queued.tx_out_len_bytes, applied.tx_out_len_bytes);
        assert_eq!(template.coinbase_tx_outputs_count, 1);

        let tx_out = decode_only_tx_out(&template.coinbase_tx_outputs.to_vec());
        assert_eq!(tx_out.value, Amount::ZERO);
        assert_eq!(
            tx_out.script_pubkey,
            ScriptBuf::new_op_return(
                PushBytesBuf::try_from(Vec::from_hex("deadbeef").unwrap()).unwrap()
            )
        );

        let second_apply = injector
            .apply_pending_to_template(&mut make_new_template())
            .await
            .expect("second apply should succeed");
        assert!(second_apply.is_none(), "pending output should be cleared");
    }

    #[tokio::test]
    async fn newest_queue_replaces_pending_output() {
        let injector = OpReturnInjector::default();
        let first = injector
            .queue_from_hex("aa")
            .await
            .expect("first queue should succeed");
        let second = injector
            .queue_from_hex("bb")
            .await
            .expect("second queue should succeed");
        let mut template = make_new_template();

        let applied = injector
            .apply_pending_to_template(&mut template)
            .await
            .expect("apply should succeed")
            .expect("pending output should be applied");

        assert!(!first.replaced_pending);
        assert!(second.replaced_pending);
        assert_eq!(applied.payload_len_bytes, 1);

        let tx_out = decode_only_tx_out(&template.coinbase_tx_outputs.to_vec());
        assert_eq!(
            tx_out.script_pubkey,
            ScriptBuf::new_op_return(PushBytesBuf::try_from(Vec::from_hex("bb").unwrap()).unwrap())
        );
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
