#![allow(dead_code)]

use std::net::SocketAddr;

use async_channel::Sender;
use hex;
use integration_tests_sv2::{
    mock_roles::{MockUpstream, WithSetup},
    sniffer::Sniffer,
    start_template_provider,
    template_provider::{DifficultyLevel, TemplateProvider},
    utils::get_available_address,
};
use stratum_apps::stratum_core::job_declaration_sv2::AllocateMiningJobTokenSuccess;
use stratum_apps::stratum_core::{
    binary_sv2::{Seq0255, Sv2Option},
    common_messages_sv2::Protocol,
    job_declaration_sv2::DeclareMiningJobSuccess,
    mining_sv2::{NewExtendedMiningJob, OpenExtendedMiningChannelSuccess},
    parsers_sv2::{self, AnyMessage, JobDeclaration},
};
use tokio::task::JoinHandle;

pub fn setup_proxy(
    local_pool_addr: Option<SocketAddr>,
    tp_addr: Option<SocketAddr>,
    staging: bool,
) -> (JoinHandle<()>, SocketAddr) {
    let downstream_listening_addr: SocketAddr = get_available_address();
    let config = dmnd_client::Configuration::new(
        Some("test".to_string()),
        tp_addr.map(|addr| addr.to_string()),
        120_000,
        0,
        100_000_000_000_000.0,
        "info".to_string(),
        "off".to_string(),
        false,
        false,
        staging,
        false,
        local_pool_addr,
        Some(downstream_listening_addr.to_string()),
        "3001".to_string(),
        false,
        false,
        "DDxDD".to_string(),
        None,
    );
    (
        tokio::spawn(dmnd_client::start(config)),
        downstream_listening_addr,
    )
}

pub fn setup_tp_with_sniffer(tp_sniffer_addr: SocketAddr) -> (TemplateProvider, Sniffer<'static>) {
    let (tp, tp_addr) = start_template_provider(Some(1), DifficultyLevel::Low);

    let tp_sniffer = Sniffer::new(
        "proxy-tp",
        tp_sniffer_addr,
        tp_addr,
        false,
        vec![],
        Some(30),
    );

    (tp, tp_sniffer)
}

pub async fn setup_mock_pool_with_sniffer(pool_addr: SocketAddr) -> (Sniffer<'static>, SocketAddr) {
    let mock_upstream_addr: SocketAddr = get_available_address();
    let mock_pool = MockUpstream::new(
        mock_upstream_addr,
        WithSetup::yes_with_defaults(Protocol::MiningProtocol, 0),
    );
    mock_pool.start().await;

    (
        Sniffer::new(
            "proxy-pool",
            pool_addr,
            mock_upstream_addr,
            false,
            vec![],
            Some(30),
        ),
        mock_upstream_addr,
    )
}

pub async fn setup_mock_jd_with_sniffer(mock_pool_addr: SocketAddr) -> Sniffer<'static> {
    let mock_jd_addr: SocketAddr = get_available_address();
    let mock_jd = MockUpstream::new(
        mock_jd_addr,
        WithSetup::yes_with_defaults(Protocol::JobDeclarationProtocol, 0),
    );
    let _jd = mock_jd.start().await;
    let sniffer = Sniffer::new(
        "proxy-pool-jd",
        mock_pool_addr,
        mock_jd_addr,
        false,
        vec![],
        Some(30),
    );
    sniffer
}
/// A thin wrapper around [`MockUpstream`] that holds the message sender
/// and exposes typed helper methods so tests don't have to construct
/// [`AnyMessage`] variants every time.
pub struct ExtendedMockUpstream {
    sender: Sender<AnyMessage<'static>>,
}

impl ExtendedMockUpstream {
    pub async fn new(addr: SocketAddr, setup: WithSetup) -> Self {
        let mock = MockUpstream::new(addr, setup);
        let sender = mock.start().await;
        Self { sender }
    }

    //  Mining messages
    pub async fn send_open_extended_mining_channel_success(
        &self,
        request_id: u32,
        channel_id: u32,
        group_channel_id: u32,
    ) {
        let msg = AnyMessage::Mining(parsers_sv2::Mining::OpenExtendedMiningChannelSuccess(
            OpenExtendedMiningChannelSuccess {
                request_id,
                channel_id,
                target: hex::decode(
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                )
                .unwrap()
                .try_into()
                .unwrap(),
                extranonce_size: 16,
                extranonce_prefix: vec![0x00, 0x01, 0x00, 1].try_into().unwrap(),
                group_channel_id,
            },
        ));
        self.sender
            .send(msg)
            .await
            .expect("send OpenExtendedMiningChannelSuccess");
    }

    pub async fn send_new_extended_mining_job(&self, channel_id: u32) {
        let new_extended_mining_job = AnyMessage::Mining(parsers_sv2::Mining::NewExtendedMiningJob(NewExtendedMiningJob {
            channel_id,
            job_id: 1,
            min_ntime: Sv2Option::new(None),
            version: 0x20000000,
            version_rolling_allowed: true,
            merkle_path: Seq0255::new(vec![]).unwrap(),
            coinbase_tx_prefix: hex::decode("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff225200162f5374726174756d2056322053524920506f6f6c2f2f08").unwrap().try_into().unwrap(),
            coinbase_tx_suffix: hex::decode("feffffff0200f2052a01000000160014ebe1b7dcc293ccaa0ee743a86f89df8258c208fc0000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf901000000").unwrap().try_into().unwrap(),
        }));
        self.sender
            .send(new_extended_mining_job)
            .await
            .expect("send NewExtendedMiningJob");
    }

    //  Job Declaration messages
    pub async fn send_allocate_mining_job_token_success(&self, request_id: u32) {
        let msg = AnyMessage::JobDeclaration(JobDeclaration::AllocateMiningJobTokenSuccess(
            AllocateMiningJobTokenSuccess {
                request_id,
                mining_job_token: vec![0; 32].try_into().unwrap(),
                coinbase_outputs: vec![0; 32].try_into().unwrap(),
            },
        ));

        self.sender
            .send(msg)
            .await
            .expect("send AllocateMiningJobTokenSuccess");
    }

    pub async fn send_declare_mining_job_success(&self, request_id: u32) {
        let msg = AnyMessage::JobDeclaration(JobDeclaration::DeclareMiningJobSuccess(
            DeclareMiningJobSuccess {
                request_id,
                new_mining_job_token: vec![].try_into().expect("new_mining_job_token too long"),
            },
        ));
        self.sender
            .send(msg)
            .await
            .expect("send DeclareMiningJobSuccess");
    }
}
