use binary_sv2::{encodable::EncodableField, B032Owned};

fn nested_fields() -> EncodableField<'static> {
    EncodableField::Struct(vec![
        7u8.into(),
        EncodableField::Struct(vec![
            0x7856_3412u32.into(),
            B032Owned::try_from(vec![8, 9]).unwrap().into(),
            EncodableField::Struct(vec![]),
        ]),
        false.into(),
    ])
}

#[test]
fn nested_encoding_preserves_bytes_and_unused_destination() {
    let mut destination = [0xa5; 12];
    let written = binary_sv2::to_writer(nested_fields(), &mut destination).unwrap();
    assert_eq!(written, 9);
    assert_eq!(
        &destination[..written],
        &[7, 0x12, 0x34, 0x56, 0x78, 2, 8, 9, 0]
    );
    assert_eq!(&destination[written..], &[0xa5; 3]);
}

#[test]
fn nested_encoding_rejects_each_short_destination() {
    for length in 0..9 {
        assert!(binary_sv2::to_writer(nested_fields(), &mut vec![0; length]).is_err());
    }
}

fn deep_fields() -> EncodableField<'static> {
    let mut field = EncodableField::from(0x1234_5678u32);
    for _ in 0..20_000 {
        field = EncodableField::Struct(vec![field]);
    }
    field
}

#[test]
fn deep_encoding_and_drop_use_bounded_stack_space() {
    std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(|| {
            let mut destination = [0; 4];
            assert_eq!(
                binary_sv2::to_writer(deep_fields(), &mut destination).unwrap(),
                4
            );
            assert_eq!(destination, 0x1234_5678u32.to_le_bytes());
            assert!(binary_sv2::to_writer(deep_fields(), &mut [0; 3]).is_err());
            drop(deep_fields());
            let mut branched = EncodableField::from(7u8);
            for _ in 0..20_000 {
                branched = EncodableField::Struct(vec![branched, 9u8.into()]);
            }
            let mut destination = vec![0; 20_001];
            assert_eq!(
                binary_sv2::to_writer(branched, &mut destination).unwrap(),
                20_001
            );
            assert_eq!(destination[0], 7);
            assert!(destination[1..].iter().all(|value| *value == 9));
        })
        .unwrap()
        .join()
        .unwrap();
}

fn generated_fields(seed: u32, depth: u8) -> EncodableField<'static> {
    let mut state = seed;
    let mut fields = Vec::new();
    for _ in 0..(seed % 5 + 1) {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        fields.push(match state % 8 {
            0 if depth > 0 => generated_fields(state >> 8, depth - 1),
            1 => (state as u8).into(),
            2 => (state as u16).into(),
            3 => state.into(),
            4 => (u64::from(state) << 24).into(),
            5 => (state & 16 != 0).into(),
            _ => B032Owned::try_from(vec![state as u8; (state % 33) as usize])
                .unwrap()
                .into(),
        });
    }
    EncodableField::Struct(fields)
}

#[test]
fn owned_encoding_matches_native_reference_bytes_and_errors() {
    use binary_sv2::GetSize;

    for seed in 0..128 {
        let reference = generated_fields(seed, 4);
        let size = reference.get_size();
        for length in [
            0,
            1,
            2,
            4,
            8,
            size / 2,
            size.saturating_sub(1),
            size,
            size + 1,
            size + 5,
        ] {
            let mut expected_bytes = vec![0xa5; length];
            let mut actual_bytes = expected_bytes.clone();
            let expected = reference.encode(&mut expected_bytes, 0);
            let actual = binary_sv2::to_writer(generated_fields(seed, 4), &mut actual_bytes);
            assert_eq!(
                format!("{actual:?}"),
                format!("{expected:?}"),
                "seed {seed}, length {length}"
            );
            assert_eq!(actual_bytes, expected_bytes, "seed {seed}, length {length}");
        }
    }
}
