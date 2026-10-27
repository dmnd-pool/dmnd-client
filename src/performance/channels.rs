#[cfg(feature = "native")]
use binary_sv2::Sv2OptionOwned as Option32;
use std::{hint::black_box, time::Instant};
#[cfg(not(feature = "native"))]
type Option32 = binary_sv2::Sv2Option<'static, u32>;

#[cfg(feature = "indexed")]
use channel_factory::ChannelFactory as Factory;
#[cfg(not(feature = "native"))]
use roles_logic_sv2::mining_sv2::ExtendedExtranonce;
#[cfg(all(feature = "native", not(feature = "indexed")))]
use roles_logic_sv2::parsers::MiningOwned as Mining;
#[cfg(not(feature = "indexed"))]
use roles_logic_sv2::{
    channel_logic::channel_factory::{ExtendedChannelKind, ProxyExtendedChannelFactory as Factory},
    utils::{GroupId, Mutex},
};
#[cfg(feature = "native")]
use roles_logic_sv2::{
    mining::ExtendedExtranonce,
    mining_sv2::{
        NewExtendedMiningJobOwned as Job, OpenExtendedMiningChannelSuccessOwned,
        SetNewPrevHashOwned as PrevHash, SubmitSharesExtendedOwned as Share,
    },
};
#[cfg(not(feature = "indexed"))]
use std::sync::Arc;
#[cfg(not(feature = "native"))]
type Job = roles_logic_sv2::mining_sv2::NewExtendedMiningJob<'static>;
#[cfg(not(feature = "native"))]
type PrevHash = roles_logic_sv2::mining_sv2::SetNewPrevHash<'static>;
#[cfg(not(feature = "native"))]
type Share = roles_logic_sv2::mining_sv2::SubmitSharesExtended<'static>;
#[cfg(not(feature = "native"))]
use roles_logic_sv2::parsers::Mining;

fn factory() -> Factory {
    let extranonces = ExtendedExtranonce::new(0..6, 6..10, 10..16);
    let target = [255; 32].into();
    #[cfg(feature = "native")]
    let upstream = OpenExtendedMiningChannelSuccessOwned {
        request_id: 0,
        channel_id: 1,
        group_channel_id: 0,
        target: [255; 32].into(),
        extranonce_size: 10,
        extranonce_prefix: vec![0; 6].try_into().unwrap(),
    };
    #[cfg(not(feature = "native"))]
    let upstream = 1;
    #[cfg(feature = "indexed")]
    {
        Factory::new(extranonces, 10., target, upstream).unwrap()
    }
    #[cfg(not(feature = "indexed"))]
    {
        Factory::new(
            Arc::new(Mutex::new(GroupId::new())),
            extranonces,
            None,
            10.,
            ExtendedChannelKind::Proxy {
                upstream_target: target,
            },
            None,
            upstream,
        )
    }
}

fn opening(count: usize) -> f64 {
    let mut factory = factory();
    let started = Instant::now();
    for request in 0..count {
        #[cfg(feature = "indexed")]
        black_box(factory.open_channel(request as u32, 1_000_000.).unwrap());
        #[cfg(not(feature = "indexed"))]
        black_box(
            factory
                .new_extended_channel(request as u32, 1_000_000., 0)
                .unwrap(),
        );
    }
    started.elapsed().as_nanos() as f64 / count as f64
}

fn shares() -> f64 {
    let mut factory = factory();
    #[cfg(feature = "indexed")]
    let channel_id = factory.open_channel(0, 1_000_000.).unwrap().channel_id;
    #[cfg(not(feature = "indexed"))]
    let opened = factory.new_extended_channel(0, 1_000_000., 0).unwrap();
    #[cfg(not(feature = "indexed"))]
    let channel_id = match &opened[0] {
        Mining::OpenExtendedMiningChannelSuccess(channel) => channel.channel_id,
        _ => panic!("channel rejected"),
    };
    let transaction = bitcoin::consensus::serialize(&bitcoin::Transaction {
        version: bitcoin::transaction::Version::ONE,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![bitcoin::TxIn {
            previous_output: bitcoin::OutPoint::null(),
            script_sig: vec![0; 16].into(),
            sequence: bitcoin::Sequence::MAX,
            witness: bitcoin::Witness::new(),
        }],
        output: vec![],
    });
    factory
        .on_new_prev_hash(PrevHash {
            channel_id: 1,
            job_id: 7,
            prev_hash: [3; 32].to_vec().try_into().unwrap(),
            min_ntime: 1_700_000_000,
            nbits: 0x1d00ffff,
        })
        .unwrap();
    factory
        .on_new_extended_mining_job(Job {
            channel_id: 1,
            job_id: 7,
            min_ntime: Option32::new(Some(1_700_000_000)),
            version: 0,
            version_rolling_allowed: false,
            merkle_path: Vec::new().try_into().unwrap(),
            coinbase_tx_prefix: transaction[..42].to_vec().try_into().unwrap(),
            coinbase_tx_suffix: transaction[58..].to_vec().try_into().unwrap(),
        })
        .unwrap();
    let share = Share {
        channel_id,
        sequence_number: 0,
        job_id: 7,
        nonce: 1,
        ntime: 1_700_000_000,
        version: 0,
        extranonce: vec![0; 6].try_into().unwrap(),
    };
    const COUNT: usize = 100_000;
    let started = Instant::now();
    for _ in 0..COUNT {
        let result = factory
            .on_submit_shares_extended(black_box(share.clone()))
            .unwrap();
        assert!(matches!(
            result,
            roles_logic_sv2::channel_logic::channel_factory::OnNewShare::SendSubmitShareUpstream(_)
        ));
        black_box(result);
    }
    started.elapsed().as_nanos() as f64 / COUNT as f64
}

fn main() {
    for count in [256, 1024, 4096, 16384] {
        println!("open {count} {:.2} ns/channel", opening(count));
    }
    println!("submit {:.2} ns/share", shares());
}
