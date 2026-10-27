use binary_sv2::GetSize;
use demand_share_accounting_ext::{
    parser::{PoolExtMessages, PoolExtMessagesOwned, ShareAccountingMessages},
    GetSharesSuccess, GetWindowSuccess, Hash256, PHash, SequenceOwned, Share, ShareOk, Slice,
    EXTENSION_TYPE, MESSAGE_TYPE_SHARE_OK,
};
use demand_sv2_connection::StandardSv2Frame;
use roles_logic_sv2::{mining_sv2::SubmitSharesExtendedOwned, parsers::MiningOwned};

#[test]
fn native_share_frame_keeps_protocol_bytes() {
    let message = PoolExtMessagesOwned::Mining(MiningOwned::SubmitSharesExtended(
        SubmitSharesExtendedOwned {
            channel_id: 1,
            sequence_number: 2,
            job_id: 3,
            nonce: 4,
            ntime: 5,
            version: 6,
            extranonce: vec![7; 4].try_into().unwrap(),
        },
    ));
    assert_eq!(message.get_size(), 29);
    let frame: StandardSv2Frame<_> = message.try_into().unwrap();
    assert_eq!(frame.encoded_length(), 35);
    let mut bytes = [0; 35];
    frame.serialize(&mut bytes).unwrap();
    assert_eq!(
        bytes,
        [
            0, 128, 0x1b, 29, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6,
            0, 0, 0, 4, 7, 7, 7, 7,
        ]
    );
    let parsed = PoolExtMessages::try_from((0x8000, 0x1b, &mut bytes[6..]))
        .unwrap()
        .into_owned();
    match parsed {
        PoolExtMessagesOwned::Mining(MiningOwned::SubmitSharesExtended(share)) => {
            assert_eq!(share.extranonce.as_bytes(), &[7; 4]);
            assert_eq!(share.sequence_number, 2);
        }
        _ => panic!("Expected an extended share"),
    }
}

#[test]
fn accounting_frame_keeps_protocol_bytes() {
    let message =
        PoolExtMessagesOwned::ShareAccountingMessages(ShareAccountingMessages::ShareOk(ShareOk {
            ref_job_id: 0x0807060504030201,
            share_index: 0x0c0b0a09,
        }));
    assert_eq!(message.get_size(), 12);
    let frame: StandardSv2Frame<_> = message.try_into().unwrap();
    let mut bytes = [0; 18];
    frame.serialize(&mut bytes).unwrap();
    assert_eq!(
        bytes,
        [32, 0, 2, 12, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    );
}

#[test]
fn accounting_parser_rejects_invalid_payloads_and_headers() {
    for len in 0..12 {
        let mut bytes = vec![0; len];
        assert!(PoolExtMessages::try_from((
            EXTENSION_TYPE,
            MESSAGE_TYPE_SHARE_OK,
            bytes.as_mut_slice()
        ))
        .is_err());
    }
    assert!(
        PoolExtMessages::try_from((EXTENSION_TYPE, MESSAGE_TYPE_SHARE_OK, &mut [0; 13][..]))
            .is_err()
    );
    assert!(PoolExtMessages::try_from((EXTENSION_TYPE, 0xff, &mut [0; 12][..])).is_err());
    assert!(PoolExtMessages::try_from((
        EXTENSION_TYPE + 1,
        MESSAGE_TYPE_SHARE_OK,
        &mut [0; 12][..]
    ))
    .is_err());
    assert!(PoolExtMessages::try_from((
        EXTENSION_TYPE | 0x8000,
        MESSAGE_TYPE_SHARE_OK,
        &mut [0; 12][..]
    ))
    .is_err());
}

#[test]
fn accounting_sequences_preserve_each_field() {
    let window = GetWindowSuccess {
        slices: SequenceOwned::new(vec![Slice {
            number_of_shares: 1,
            difficulty: 2,
            fees: 3,
            root: Hash256::from([4; 32]),
            job_id: 5,
        }])
        .unwrap(),
        phashes: SequenceOwned::new(vec![PHash {
            phash: Hash256::from([6; 32]),
            index_start: 7,
        }])
        .unwrap(),
    };
    let mut bytes = binary_sv2::to_bytes(window.clone()).unwrap();
    assert_eq!(bytes.len(), 100);
    assert_eq!(&bytes[..6], &[1, 0, 1, 0, 0, 0]);
    assert_eq!(&bytes[22..54], &[4; 32]);
    assert_eq!(&bytes[62..64], &[1, 0]);
    assert_eq!(&bytes[64..96], &[6; 32]);
    let decoded: GetWindowSuccess = binary_sv2::from_bytes(&mut bytes).unwrap();
    assert_eq!(decoded, window);
    let shares = GetSharesSuccess {
        shares: SequenceOwned::new(vec![Share {
            nonce: 1,
            ntime: 2,
            version: 3,
            extranonce: vec![4; 8].try_into().unwrap(),
            job_id: 5,
            reference_job_id: 6,
            share_index: 7,
            merkle_path: vec![8; 64].try_into().unwrap(),
        }])
        .unwrap(),
    };
    let mut bytes = binary_sv2::to_bytes(shares.clone()).unwrap();
    assert_eq!(bytes.len(), 109);
    let decoded: GetSharesSuccess = binary_sv2::from_bytes(&mut bytes).unwrap();
    assert_eq!(decoded, shares);
}

#[test]
fn message_and_frame_size_queries_allocate_nothing() {
    let message = PoolExtMessagesOwned::Mining(MiningOwned::SubmitSharesExtended(
        SubmitSharesExtendedOwned {
            channel_id: 1,
            sequence_number: 2,
            job_id: 3,
            nonce: 4,
            ntime: 5,
            version: 6,
            extranonce: vec![7; 32].try_into().unwrap(),
        },
    ));
    let frame: StandardSv2Frame<_> = message.clone().try_into().unwrap();
    let accounting =
        PoolExtMessagesOwned::ShareAccountingMessages(ShareAccountingMessages::ShareOk(ShareOk {
            ref_job_id: 1,
            share_index: 2,
        }));
    let accounting_frame: StandardSv2Frame<_> = accounting.clone().try_into().unwrap();
    let allocations = crate::test_alloc::allocations(|| {
        for _ in 0..1000 {
            assert_eq!(std::hint::black_box(&message).get_size(), 57);
            assert_eq!(std::hint::black_box(&frame).get_size(), 63);
            assert_eq!(std::hint::black_box(&frame).encoded_length(), 63);
            assert_eq!(std::hint::black_box(&accounting).get_size(), 12);
            assert_eq!(std::hint::black_box(&accounting_frame).encoded_length(), 18);
        }
    });
    assert_eq!(allocations, 0);
}

#[tokio::test]
async fn native_transport_interface_roundtrip() {
    use bitcoin::secp256k1::{Keypair, Secp256k1, SecretKey, XOnlyPublicKey};
    use demand_sv2_connection::{
        noise_connection_tokio::Connection, HandshakeRole, Initiator, Responder,
        StandardEitherFrame,
    };
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};

    tokio::time::timeout(Duration::from_secs(5), async {
        let secret = [7; 32];
        let key = SecretKey::from_slice(&secret).unwrap();
        let keypair = Keypair::from_secret_key(&Secp256k1::new(), &key);
        let public = XOnlyPublicKey::from_keypair(&keypair).0.serialize();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, server) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept(),
        );
        let (client, server) = tokio::join!(
            Connection::new::<PoolExtMessagesOwned>(
                client.unwrap(),
                HandshakeRole::Initiator(Initiator::from_raw_k(public).unwrap()),
            ),
            Connection::new::<PoolExtMessagesOwned>(
                server.unwrap().0,
                HandshakeRole::Responder(
                    Responder::from_authority_kp(&public, &secret, Duration::from_secs(60))
                        .unwrap(),
                ),
            ),
        );
        let (mut client_rx, client_tx, client_read, client_write) = client.unwrap();
        let (mut server_rx, server_tx, server_read, server_write) = server.unwrap();
        let message = PoolExtMessagesOwned::Mining(MiningOwned::SubmitSharesExtended(
            SubmitSharesExtendedOwned {
                channel_id: 1,
                sequence_number: 2,
                job_id: 3,
                nonce: 4,
                ntime: 5,
                version: 6,
                extranonce: vec![7; 32].try_into().unwrap(),
            },
        ));
        let frame: StandardSv2Frame<_> = message.try_into().unwrap();
        let envelope: StandardEitherFrame<_> = frame.into();
        assert_eq!(envelope.encoded_length(), 63);
        client_tx.send(envelope).await.unwrap();
        let mut received: StandardSv2Frame<_> = server_rx.recv().await.unwrap().try_into().unwrap();
        let header = received.get_header().unwrap();
        assert_eq!(header.ext_type(), 0x8000);
        assert_eq!(header.msg_type(), 0x1b);
        let message =
            PoolExtMessages::try_from((header.ext_type(), header.msg_type(), received.payload()))
                .unwrap();
        assert!(matches!(
            message,
            PoolExtMessages::Mining(roles_logic_sv2::parsers::Mining::SubmitSharesExtended(_))
        ));
        server_tx.send(received.into()).await.unwrap();
        let returned: StandardSv2Frame<_> = client_rx.recv().await.unwrap().try_into().unwrap();
        let mut bytes = vec![0; returned.encoded_length()];
        returned.serialize(&mut bytes).unwrap();
        let mut decoded = StandardSv2Frame::<PoolExtMessagesOwned>::from_bytes(bytes).unwrap();
        assert_eq!(
            &decoded.payload()[24..],
            &[
                32, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
                7, 7, 7, 7, 7, 7
            ]
        );

        let reply = PoolExtMessagesOwned::ShareAccountingMessages(
            ShareAccountingMessages::ShareOk(ShareOk {
                ref_job_id: 3_u64 << 32,
                share_index: 4,
            }),
        );
        let reply: StandardSv2Frame<_> = reply.try_into().unwrap();
        server_tx.send(reply.into()).await.unwrap();
        let mut reply: StandardSv2Frame<_> = client_rx.recv().await.unwrap().try_into().unwrap();
        let header = reply.get_header().unwrap();
        assert_eq!(header.ext_type(), EXTENSION_TYPE);
        let parsed =
            PoolExtMessages::try_from((header.ext_type(), header.msg_type(), reply.payload()))
                .unwrap();
        assert!(matches!(
            parsed,
            PoolExtMessages::ShareAccountingMessages(ShareAccountingMessages::ShareOk(ShareOk {
                share_index: 4,
                ..
            }))
        ));
        for task in [client_read, client_write, server_read, server_write] {
            task.abort();
        }
    })
    .await
    .expect("Transport exchange must finish");
}
