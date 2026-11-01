use super::{parser::ShareAccountingMessages, *};
use roles_logic_sv2::parsers::IsSv2Message;

fn accounting_payloads() -> Vec<(u8, Vec<u8>)> {
    vec![
        (0x02, (1..=12).collect()),
        (0x03, vec![0xa5; 32]),
        (0x04, vec![0x5a; 32]),
        (0x05, vec![0, 0, 0, 0]),
        (0x06, (1..=8).collect()),
        (0x07, vec![1, 0, 0, 0, 2, 0, 7, 0, 0, 0, 9, 0, 0, 0]),
        (0x08, vec![0, 0]),
        (0x0f, vec![1, 0, 3, 0, 0, b'A', b'B', b'C']),
        (0x10, vec![3, b'A', b'B', b'C']),
    ]
}

#[test]
fn accounting_parser_preserves_all_supported_message_bytes() {
    for (id, mut payload) in accounting_payloads() {
        let expected = payload.clone();
        let message = ShareAccountingMessages::try_from((id, payload.as_mut_slice())).unwrap();
        assert_eq!(message.message_type(), id);
        assert!(!message.channel_bit());
        assert_eq!(binary_sv2::to_bytes(message).unwrap(), expected);
    }
}

#[test]
fn accounting_parser_rejects_truncated_and_trailing_fields() {
    for (id, payload) in accounting_payloads() {
        for length in 0..payload.len() {
            let mut truncated = payload[..length].to_vec();
            assert!(ShareAccountingMessages::try_from((id, truncated.as_mut_slice())).is_err());
        }
        let mut extended = payload;
        extended.push(0);
        assert!(ShareAccountingMessages::try_from((id, extended.as_mut_slice())).is_err());
    }
}

#[test]
fn composite_sequences_preserve_nonuniform_hash_bytes_and_element_order() {
    let expected_hash = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31,
    ];
    let first = Slice {
        number_of_shares: 1,
        difficulty: 2,
        fees: 3,
        root: expected_hash.into(),
        job_id: 4,
    };
    let second = Slice {
        number_of_shares: 5,
        root: [0xa5; 32].into(),
        ..first.clone()
    };
    let value = GetWindowSuccess {
        slices: SequenceOwned::new(vec![first.clone(), second]).unwrap(),
        phashes: SequenceOwned::new(vec![]).unwrap(),
    };
    let mut bytes = binary_sv2::to_bytes(value.clone()).unwrap();
    assert_eq!(&bytes[..2], &[2, 0]);
    assert_eq!(&bytes[22..54], &expected_hash);
    assert_eq!(&bytes[82..114], &[0xa5; 32]);
    let decoded: GetWindowSuccess = binary_sv2::from_bytes(&mut bytes).unwrap();
    assert_eq!(decoded, value);
}

#[test]
fn composite_sequence_rejects_counts_above_the_protocol_limit() {
    assert!(SequenceOwned::new(vec![0u8; usize::from(u16::MAX) + 1]).is_err());
}
