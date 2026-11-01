#![allow(dead_code, unused_crate_dependencies)]
mod support;

use binary_sv2::{Seq0255Owned as Seq0255, Sv2OptionOwned as Sv2Option};
use bitcoin::{consensus::serialize, hashes::Hash, Network};
use dmnd_client::sv2_connection;
use roles_logic_sv2::mining_sv2::OpenExtendedMiningChannelOwned as OpenExtendedMiningChannel;
use support::pool::protocol::{
    CommonMessages, Mining, NewExtendedMiningJob, NewTemplate, Protocol, Seq064K, SetupConnection,
    SubmitSharesExtended, TemplatePrevHash as SetNewPrevHash,
};
use support::{
    pool::{
        control::{ChannelKey, EventMatcher, PoolCommand, PoolEvent},
        jobs::{JobKey, JobRecord},
        mining::ChannelState,
        shares::{merkle_root, ShareOutcome, ShareRejection, ShareValidator},
        transport::{Message, PeerConnection, PeerId},
        PoolConfig, RunningPool,
    },
    TestResult,
};
use tokio::time::{Duration, Instant};

fn vector() -> (ChannelState, JobRecord, SubmitSharesExtended) {
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin);
    let header = genesis.header;
    let channel = ChannelState {
        key: ChannelKey {
            peer: PeerId(1),
            channel: 1,
        },
        target: bitcoin::Target::from_compact(header.bits).to_le_bytes(),
        extranonce_prefix: vec![],
        extranonce_size: 0,
    };
    let key = JobKey {
        peer: PeerId(1),
        channel: 1,
        job: 1,
    };
    let job = JobRecord {
        key,
        message: NewExtendedMiningJob {
            channel_id: 1,
            job_id: 1,
            min_ntime: Sv2Option::new(Some(header.time)),
            version: header.version.to_consensus() as u32,
            version_rolling_allowed: false,
            merkle_path: Seq0255::new(vec![]).unwrap(),
            coinbase_tx_prefix: serialize(&genesis.txdata[0]).try_into().unwrap(),
            coinbase_tx_suffix: vec![].try_into().unwrap(),
        },
        prev_hash: Some(header.prev_blockhash.to_byte_array()),
        min_ntime: header.time,
        nbits: header.bits.to_consensus(),
        valid: true,
    };
    let share = SubmitSharesExtended {
        channel_id: 1,
        job_id: 1,
        sequence_number: 7,
        nonce: header.nonce,
        ntime: header.time,
        version: 1,
        extranonce: vec![].try_into().unwrap(),
    };
    (channel, job, share)
}

#[test]
fn genesis_known_answer_and_target_byte_order() {
    let (channel, job, share) = vector();
    let mut validator = ShareValidator::default();
    let ShareOutcome::BlockCandidate(validated) =
        validator.validate(PeerId(1), Some(&channel), Some(&job), &share)
    else {
        panic!("genesis must validate");
    };
    assert_eq!(
        validated.hash,
        "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
    );
    assert_eq!(
        validated.header,
        serialize(&bitcoin::blockdata::constants::genesis_block(Network::Bitcoin).header)
    );
    assert!(matches!(
        validator.validate(PeerId(1), Some(&channel), Some(&job), &share),
        ShareOutcome::Rejected(ShareRejection::Duplicate)
    ));
    let mut hard = channel.clone();
    hard.target = [0; 32];
    assert!(matches!(
        ShareValidator::default().validate(PeerId(1), Some(&hard), Some(&job), &share),
        ShareOutcome::Rejected(ShareRejection::LowDifficulty)
    ));
    let mut reversed = channel;
    reversed.target.reverse();
    assert!(matches!(
        ShareValidator::default().validate(PeerId(1), Some(&reversed), Some(&job), &share),
        ShareOutcome::Rejected(ShareRejection::LowDifficulty)
    ));
}

#[test]
fn invalid_work_and_connection_scope() {
    let (channel, job, share) = vector();
    let reject = |channel: &ChannelState,
                  job: &JobRecord,
                  share: &SubmitSharesExtended,
                  expected| {
        assert!(
            matches!(ShareValidator::default().validate(PeerId(1),Some(channel),Some(job),share),ShareOutcome::Rejected(reason) if reason == expected)
        );
    };
    let mut wrong = job.clone();
    wrong.key.peer = PeerId(2);
    reject(&channel, &wrong, &share, ShareRejection::UnknownJob);
    let mut wrong = job.clone();
    wrong.valid = false;
    reject(&channel, &wrong, &share, ShareRejection::Stale);
    let mut wrong = share.clone();
    wrong.ntime -= 1;
    reject(&channel, &job, &wrong, ShareRejection::Timestamp);
    let mut wrong = share.clone();
    wrong.version ^= 2;
    reject(&channel, &job, &wrong, ShareRejection::Version);
    let mut wrong = share.clone();
    wrong.extranonce = vec![0].try_into().unwrap();
    reject(&channel, &job, &wrong, ShareRejection::Extranonce);
    let mut wrong = job;
    wrong.message.coinbase_tx_prefix = vec![0].try_into().unwrap();
    reject(&channel, &wrong, &share, ShareRejection::Coinbase);
}

#[test]
fn merkle_known_answer() {
    // Fixed SHA256d vector for two raw (wire-order) leaves, including odd-tree duplication.
    let root = merkle_root([0; 32], &[[1; 32]]);
    assert_eq!(
        bitcoin::hashes::sha256d::Hash::from_byte_array(root).to_string(),
        "6db7479c5346abc47ef4196df47836190a79ce42b078a9e5c36f47429dde5e70"
    );
    let path = support::pool::jd::coinbase_merkle_path(&[[1; 32], [2; 32]]);
    assert_eq!(path.len(), 2);
    assert_eq!(path[0], [1; 32]);
    assert_eq!(
        path[1],
        bitcoin::hashes::sha256d::Hash::hash(&[2; 64]).to_byte_array()
    );
}

#[test]
fn jd_tokens_missing_transactions_and_custom_job_consistency() -> TestResult {
    use support::pool::{
        jd::{coinbase_merkle_path, TokenRegistry},
        protocol::{
            encode_coinbase_outputs, extended_job_from_custom_job, DeclareMiningJob,
            ProvideMissingTransactionsSuccess, SetCustomMiningJob,
        },
        templates::TemplateStore,
    };
    let mut transaction = bitcoin::blockdata::constants::genesis_block(Network::Bitcoin)
        .txdata
        .remove(0);
    transaction.input[0].previous_output = bitcoin::OutPoint {
        txid: bitcoin::Txid::from_byte_array([7; 32]),
        vout: 0,
    };
    let bytes = serialize(&transaction);
    let wtxid = transaction.compute_wtxid().to_byte_array();
    let merkle_path = coinbase_merkle_path(&[transaction.compute_txid().to_byte_array()]);
    let reward = 5_000_000_000;
    let mut pool_output = support::pool::jobs::pool_output();
    pool_output.value = bitcoin::Amount::from_sat(reward);
    let mut custom = SetCustomMiningJob {
        channel_id: 1,
        request_id: 1,
        token: vec![].try_into().unwrap(),
        version: 0x20000000,
        prev_hash: [3; 32].into(),
        min_ntime: 1_700_000_000,
        nbits: 0x207fffff,
        coinbase_tx_version: 2,
        coinbase_prefix: vec![2, 1, 1, 0].try_into().unwrap(),
        coinbase_tx_input_n_sequence: u32::MAX,
        coinbase_tx_outputs: encode_coinbase_outputs(vec![pool_output])
            .try_into()
            .unwrap(),
        coinbase_tx_locktime: 0,
        merkle_path: Seq0255::new(merkle_path.into_iter().map(Into::into).collect()).unwrap(),
    };
    let job = extended_job_from_custom_job(&custom, 0, 32)
        .map_err(|e| support::error(format!("fixture: {e:?}")))?;
    let mut tokens = TokenRegistry::default();
    let token = tokens.allocate(PeerId(2), "identity");
    let declaration = DeclareMiningJob {
        request_id: 1,
        mining_job_token: token.try_into().unwrap(),
        version: custom.version,
        coinbase_tx_prefix: job.coinbase_tx_prefix,
        coinbase_tx_suffix: job.coinbase_tx_suffix,
        wtxid_list: Seq064K::new(vec![wtxid.into()]).unwrap(),
        excess_data: vec![].try_into().unwrap(),
    };
    let mut store = TemplateStore::default();
    assert!(tokens
        .declare(PeerId(3), "identity", declaration.clone(), &store, true)
        .is_err());
    assert_eq!(
        tokens.declare(PeerId(2), "identity", declaration.clone(), &store, true)?,
        vec![0]
    );
    assert!(tokens
        .declare(PeerId(2), "identity", declaration, &store, true)
        .is_err());
    assert!(tokens.approve(PeerId(2), 1).is_err());
    assert!(tokens
        .provide(
            PeerId(2),
            ProvideMissingTransactionsSuccess {
                request_id: 1,
                transaction_list: Seq064K::new(vec![]).unwrap()
            },
            &mut store
        )
        .is_err());
    tokens.provide(
        PeerId(2),
        ProvideMissingTransactionsSuccess {
            request_id: 1,
            transaction_list: Seq064K::new(vec![bytes.try_into().unwrap()]).unwrap(),
        },
        &mut store,
    )?;
    custom.token = tokens.approve(PeerId(2), 1)?.try_into().unwrap();
    tokens.custom("identity", &custom, &store)?;
    assert!(tokens.custom("other-identity", &custom, &store).is_err());
    let mut invalid = custom.clone();
    invalid.token = vec![0].try_into().unwrap();
    assert!(tokens.custom("identity", &invalid, &store).is_err());
    let mut invalid = custom.clone();
    invalid.merkle_path = Seq0255::new(vec![]).unwrap();
    assert!(tokens.custom("identity", &invalid, &store).is_err());
    let mut invalid = custom.clone();
    invalid.coinbase_tx_locktime = 1;
    assert!(tokens.custom("identity", &invalid, &store).is_err());
    // A matching script with zero (or a token one-satoshi) payment cannot divert the reward.
    let valid_declaration = tokens.declarations[&(PeerId(2), 1)].message.clone();
    for pool_payment in [0, 1] {
        let mut invalid = custom.clone();
        let diverted = bitcoin::TxOut {
            value: bitcoin::Amount::from_sat(reward - pool_payment),
            script_pubkey: bitcoin::ScriptBuf::from_bytes(vec![0x52]),
        };
        let mut pool_output = support::pool::jobs::pool_output();
        pool_output.value = bitcoin::Amount::from_sat(pool_payment);
        invalid.coinbase_tx_outputs = encode_coinbase_outputs(vec![diverted, pool_output])
            .try_into()
            .unwrap();
        let job = extended_job_from_custom_job(&invalid, 0, 32)
            .map_err(|e| support::error(format!("payout fixture: {e:?}")))?;
        let mut declaration = valid_declaration.clone();
        declaration.request_id = 2;
        declaration.mining_job_token = tokens.allocate(PeerId(2), "identity").try_into().unwrap();
        declaration.coinbase_tx_prefix = job.coinbase_tx_prefix;
        declaration.coinbase_tx_suffix = job.coinbase_tx_suffix;
        assert_eq!(
            tokens
                .declare(PeerId(2), "identity", declaration.clone(), &store, false)
                .unwrap_err()
                .to_string(),
            "invalid-declaration-coinbase"
        );
        // Exercise the custom-job payout check independently, even with a matching declaration.
        let registered = &mut tokens
            .declarations
            .get_mut(&(PeerId(2), 1))
            .unwrap()
            .message;
        registered.coinbase_tx_prefix = declaration.coinbase_tx_prefix;
        registered.coinbase_tx_suffix = declaration.coinbase_tx_suffix;
        assert_eq!(
            tokens
                .custom("identity", &invalid, &store)
                .unwrap_err()
                .to_string(),
            "custom-job-payout-missing"
        );
    }
    custom.coinbase_tx_outputs = vec![].try_into().unwrap();
    assert!(tokens.custom("identity", &custom, &store).is_err());
    Ok(())
}

pub async fn setup(
    address: std::net::SocketAddr,
    protocol: Protocol,
) -> TestResult<PeerConnection> {
    let mut connection = PeerConnection::connect(address).await?;
    connection
        .send(Message::Common(CommonMessages::SetupConnection(
            SetupConnection {
                protocol,
                min_version: 2,
                max_version: 2,
                flags: 0,
                endpoint_host: "127.0.0.1".to_string().try_into().unwrap(),
                endpoint_port: address.port(),
                vendor: String::new().try_into().unwrap(),
                hardware_version: String::new().try_into().unwrap(),
                firmware: String::new().try_into().unwrap(),
                device_id: "test::POOLED::e2e-token".to_string().try_into().unwrap(),
            },
        )))
        .await?;
    assert!(matches!(
        connection.recv().await?,
        Some(Message::Common(CommonMessages::SetupConnectionSuccess(_)))
    ));
    Ok(connection)
}

fn template(id: u64) -> NewTemplate {
    NewTemplate {
        template_id: id,
        future_template: true,
        version: 0x20000000,
        coinbase_tx_version: 2,
        coinbase_prefix: vec![2, 1, 1, 0].try_into().unwrap(),
        coinbase_tx_input_sequence: u32::MAX,
        coinbase_tx_value_remaining: 5_000_000_000,
        coinbase_tx_outputs_count: 0,
        coinbase_tx_outputs: vec![].try_into().unwrap(),
        coinbase_tx_locktime: 0,
        merkle_path: Seq0255::new(vec![]).unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_noise_handshakes_complete_independently() -> TestResult {
    let pool = RunningPool::start(PoolConfig::default()).await?;
    let start = std::sync::Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..8 {
        let start = start.clone();
        let address = pool.address;
        tasks.spawn(async move {
            start.wait().await;
            let protocol = if index % 2 == 0 {
                Protocol::MiningProtocol
            } else {
                Protocol::JobDeclarationProtocol
            };
            setup(address, protocol).await
        });
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut connections = Vec::new();
    while let Some(result) = tokio::time::timeout_at(deadline, tasks.join_next()).await? {
        connections.push(result??);
    }
    assert_eq!(connections.len(), 8);
    let snapshot = pool.handle().command(PoolCommand::Snapshot).await?;
    assert_eq!(snapshot.peers.len(), 8);
    assert!(snapshot
        .events
        .iter()
        .all(|event| !matches!(event.event, PoolEvent::Error { .. })));
    drop(connections);
    pool.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_connections_history_controls_and_shutdown() -> TestResult {
    let pool = RunningPool::start(PoolConfig::default()).await?;
    let handle = pool.handle();
    handle
        .command(PoolCommand::PublishTemplate(template(1)))
        .await?;
    handle
        .command(PoolCommand::PublishTip(SetNewPrevHash {
            template_id: 1,
            prev_hash: [3; 32].into(),
            header_timestamp: 1_700_000_000,
            n_bits: 0x207fffff,
            target: [255; 32].into(),
        }))
        .await?;
    let mut probe = setup(pool.address, Protocol::MiningProtocol).await?;
    let mut miner = setup(pool.address, Protocol::MiningProtocol).await?;
    let jd = setup(pool.address, Protocol::JobDeclarationProtocol).await?;
    for connection in [&mut probe, &mut miner] {
        connection
            .send(Message::Mining(Mining::OpenExtendedMiningChannel(
                OpenExtendedMiningChannel {
                    request_id: 1,
                    user_identity: "e2e-token".to_string().try_into().unwrap(),
                    nominal_hash_rate: 100_000.0,
                    max_target: [255; 32].into(),
                    min_extranonce_size: 8,
                },
            )))
            .await?;
        let Some(Message::Mining(Mining::OpenExtendedMiningChannelSuccess(opened))) =
            connection.recv().await?
        else {
            panic!("open");
        };
        assert_eq!(
            opened.extranonce_size as usize + opened.extranonce_prefix.as_bytes().len(),
            32
        );
        assert!(matches!(
            connection.recv().await?,
            Some(Message::Mining(Mining::NewExtendedMiningJob(_)))
        ));
        assert!(matches!(
            connection.recv().await?,
            Some(Message::Mining(Mining::SetNewPrevHash(_)))
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let first = handle
        .wait_for(EventMatcher::ChannelOpened, deadline)
        .await?;
    let PoolEvent::ChannelOpened { channel, .. } = first.event else {
        unreachable!()
    };
    // Both historical and newly arriving events are available through the same API.
    let second = handle
        .wait_after(first.sequence, EventMatcher::ChannelOpened, deadline)
        .await?;
    assert!(second.sequence > first.sequence);
    let snapshot = handle.command(PoolCommand::Snapshot).await?;
    assert_eq!(snapshot.peers.len(), 3);
    assert_eq!(snapshot.channels.len(), 2);
    assert_ne!(snapshot.channels[0], snapshot.channels[1]);
    handle.command(PoolCommand::PauseResponses).await?;
    handle
        .command(PoolCommand::SetTarget {
            channel,
            target: [7; 32],
        })
        .await?;
    handle.command(PoolCommand::ResumeResponses).await?;
    assert!(matches!(
        probe.recv().await?,
        Some(Message::Mining(Mining::SetTarget(_)))
    ));
    handle
        .command(PoolCommand::DisconnectPeer(channel.peer))
        .await?;
    assert!(probe.recv().await?.is_none());
    drop((probe, miner, jd));
    let address = pool.address;
    pool.shutdown().await?;
    let _rebound = tokio::net::TcpListener::bind(address).await?;
    Ok(())
}

#[tokio::test]
async fn resume_delivers_more_responses_than_the_peer_queue_capacity() -> TestResult {
    // A single-thread runtime makes a synchronous 300-message flush fill the 256-message queue.
    let pool = RunningPool::start(PoolConfig::default()).await?;
    let handle = pool.handle();
    let mut peer = setup(pool.address, Protocol::MiningProtocol).await?;
    peer.send(Message::Mining(Mining::OpenExtendedMiningChannel(
        OpenExtendedMiningChannel {
            request_id: 1,
            user_identity: "e2e-token".to_string().try_into().unwrap(),
            nominal_hash_rate: 100_000.0,
            max_target: [255; 32].into(),
            min_extranonce_size: 8,
        },
    )))
    .await?;
    assert!(matches!(
        peer.recv().await?,
        Some(Message::Mining(Mining::OpenExtendedMiningChannelSuccess(_)))
    ));
    let opened = handle
        .wait_for(
            EventMatcher::ChannelOpened,
            Instant::now() + Duration::from_secs(5),
        )
        .await?;
    let PoolEvent::ChannelOpened { channel, .. } = opened.event else {
        unreachable!()
    };
    handle.command(PoolCommand::PauseResponses).await?;
    for index in 0..300_u32 {
        let mut target = [255; 32];
        target[..4].copy_from_slice(&index.to_le_bytes());
        handle
            .command(PoolCommand::SetTarget { channel, target })
            .await?;
    }
    handle.command(PoolCommand::ResumeResponses).await?;
    let limit = Instant::now() + Duration::from_secs(5);
    for index in 0..300_u32 {
        let Some(Message::Mining(Mining::SetTarget(message))) =
            tokio::time::timeout_at(limit, peer.recv()).await??
        else {
            panic!("missing queued response {index}");
        };
        let mut target = [255; 32];
        target[..4].copy_from_slice(&index.to_le_bytes());
        assert_eq!(message.maximum_target.as_bytes(), target);
    }
    drop(peer);
    pool.shutdown().await?;
    Ok(())
}
