use std::convert::TryInto;

use binary_sv2::B0255;
use roles_logic_sv2::template_distribution_sv2::NewTemplate;

// DO NOT CHANGE IT WTHOUT TESTING CAN BREACK EVERYTHING
pub const MAX_MINER_NAME_LEN: usize = 10;
const MAX_COINBASE_SCRIPT_LEN: usize = 100;
const MAX_EXTRANONCE_LEN: usize = 32;
const MAX_DIRECT_PUSH_LEN: usize = 75;

const TAG_PREFIX: &str = "/DMND/";
const TAG_SUFFIX: &str = "/";

pub fn validate_miner_name(name: &str) -> Result<(), String> {
    if name.len() > MAX_MINER_NAME_LEN {
        Err(format!(
            "MINER_NAME must be at most {MAX_MINER_NAME_LEN} bytes, got {}",
            name.len()
        ))
    } else {
        Ok(())
    }
}

pub fn format_miner_tag(miner_name: Option<&str>) -> String {
    format!("{TAG_PREFIX}{}{TAG_SUFFIX}", miner_name.unwrap_or_default())
}

pub fn append_tag_to_script_prefix<'a>(
    coinbase_prefix: B0255<'a>,
    tag: &[u8],
) -> Result<B0255<'a>, String> {
    if tag.len() > MAX_DIRECT_PUSH_LEN {
        return Err(format!(
            "coinbase script tag must fit in a direct push, got {} bytes",
            tag.len()
        ));
    }

    let tagged_prefix_len = coinbase_prefix.len() + tag.len() + 1;
    let max_prefix_len = MAX_COINBASE_SCRIPT_LEN - MAX_EXTRANONCE_LEN;
    if tagged_prefix_len > max_prefix_len {
        return Err(format!(
            "coinbase script prefix too long after miner tag: \
             prefix_len={} tag_push_len={} max_prefix_len={} max_script_len={} max_extranonce_len={}",
            coinbase_prefix.len(),
            tag.len() + 1,
            max_prefix_len,
            MAX_COINBASE_SCRIPT_LEN,
            MAX_EXTRANONCE_LEN
        ));
    }

    let mut prefix = coinbase_prefix.to_vec();
    prefix.push(tag.len() as u8);
    prefix.extend_from_slice(tag);
    prefix
        .try_into()
        .map_err(|_| "tagged coinbase prefix does not fit in B0255".to_string())
}

pub fn tag_new_template<'a>(
    template: &mut NewTemplate<'a>,
    miner_name: Option<&str>,
) -> Result<(), String> {
    let tag = format_miner_tag(miner_name).into_bytes();
    template.coinbase_prefix = append_tag_to_script_prefix(template.coinbase_prefix.clone(), &tag)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_sv2::{Seq0255, B064K, U256};

    #[test]
    fn formats_empty_miner_name_tag() {
        assert_eq!(format_miner_tag(None), "/DMND//");
    }

    #[test]
    fn rejects_names_longer_than_max_len() {
        let too_long = "a".repeat(MAX_MINER_NAME_LEN + 1);
        assert!(validate_miner_name(&too_long).is_err());
    }

    #[test]
    fn appends_tag_to_script_prefix() {
        let prefix: B0255<'static> = vec![0x03, 0x5a, 0x59, 0x0e, 0x00]
            .try_into()
            .expect("sample script prefix should fit in B0255");
        let tag = b"/DMND//";
        let tagged = append_tag_to_script_prefix(prefix, tag)
            .expect("tagged script prefix should fit")
            .to_vec();

        assert_eq!(tagged, b"\x03\x5a\x59\x0e\x00\x07/DMND//".to_vec());
    }

    #[test]
    fn rejects_tag_that_does_not_fit_coinbase_script_budget() {
        let prefix: B0255<'static> = vec![0_u8; MAX_COINBASE_SCRIPT_LEN - MAX_EXTRANONCE_LEN]
            .try_into()
            .expect("sample script prefix should fit in B0255");

        let err = append_tag_to_script_prefix(prefix, b"/DMND//")
            .expect_err("tagged script prefix should exceed the budget");

        assert!(err.contains("coinbase script prefix too long"));
    }

    #[test]
    fn rejects_tag_that_needs_pushdata_encoding() {
        let prefix: B0255<'static> = vec![0_u8; 1]
            .try_into()
            .expect("sample script prefix should fit in B0255");
        let tag = vec![0_u8; MAX_DIRECT_PUSH_LEN + 1];

        let err = append_tag_to_script_prefix(prefix, &tag)
            .expect_err("oversized tag should be rejected");

        assert!(err.contains("direct push"));
    }

    #[test]
    fn tags_new_template_coinbase_prefix() {
        let coinbase_prefix: B0255<'static> = vec![0x03, 0x5a, 0x59, 0x0e, 0x00]
            .try_into()
            .expect("sample script prefix should fit in B0255");
        let coinbase_tx_outputs: B064K<'static> = Vec::new()
            .try_into()
            .expect("coinbase outputs should fit in B064K");
        let merkle_path: Seq0255<'static, U256<'static>> = Vec::new().into();
        let mut template = NewTemplate {
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
        };

        tag_new_template(&mut template, Some("miner4")).expect("tagged template should fit");
        assert_eq!(
            template.coinbase_prefix.to_vec(),
            b"\x03\x5a\x59\x0e\x00\x0d/DMND/miner4/".to_vec()
        );
    }
}
