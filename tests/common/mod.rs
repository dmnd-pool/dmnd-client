#![allow(dead_code)]

use std::net::SocketAddr;

use async_channel::{Receiver, Sender};
use hex;
use integration_tests_sv2::{
    mock_roles::{MockUpstream, WithSetup},
    sniffer::Sniffer,
    utils::{create_downstream, create_upstream, get_available_address},
};
use stratum_apps::stratum_core::job_declaration_sv2::AllocateMiningJobTokenSuccess;
use stratum_apps::stratum_core::{
    common_messages_sv2::Protocol,
    framing_sv2::framing::Frame,
    job_declaration_sv2::DeclareMiningJobSuccess,
    mining_sv2::{
        OpenExtendedMiningChannelSuccess, SetCustomMiningJobSuccess, SubmitSharesSuccess,
        MESSAGE_TYPE_SET_CUSTOM_MINING_JOB,
    },
    parsers_sv2::{self, AnyMessage, JobDeclaration, Mining},
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
        1_000_000.0,
        "info".to_string(),
        "off".to_string(),
        false,
        false,
        staging,
        false,
        false,
        local_pool_addr,
        Some(downstream_listening_addr.to_string()),
        Some(1),
        250,
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

pub async fn setup_mock_jd_with_sniffer(
    mock_pool_addr: SocketAddr,
) -> (ExtendedMockUpstream, Sniffer<'static>) {
    let mock_jd_addr: SocketAddr = get_available_address();
    let mock_jd = ExtendedMockUpstream::new(
        mock_jd_addr,
        WithSetup::yes_with_defaults(Protocol::JobDeclarationProtocol, 0),
        "jd-internal",
    )
    .await;
    let sniffer = Sniffer::new(
        "proxy-pool-jd",
        mock_pool_addr,
        mock_jd_addr,
        false,
        vec![],
        Some(30),
    );
    (mock_jd, sniffer)
}

/// A wrapper around [MockUpstream] that extends its capabilities by:
/// - Sending messages that `MockUpstream` does not support by default.
/// - Intercepting `SetCustomMiningJob` messages before they reach the sniffer to avoid parsing issues in the sniffer.
pub struct ExtendedMockUpstream {
    sender: Sender<AnyMessage<'static>>,
    set_custom_mining_job_rx: Receiver<(u32, u32)>,
    pub sniffer: Sniffer<'static>,
}

impl ExtendedMockUpstream {
    pub async fn new(addr: SocketAddr, setup: WithSetup, sniffer_id: &'static str) -> Self {
        let mock_addr = get_available_address();
        let sender = MockUpstream::new(mock_addr, setup).start().await;

        let (scmj_tx, set_custom_mining_job_rx) = async_channel::unbounded::<(u32, u32)>();

        let sniffer_addr = get_available_address();
        let sniffer = Sniffer::new(sniffer_id, sniffer_addr, mock_addr, false, vec![], Some(30));
        sniffer.start();

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("ExtendedMockUpstream: bind failed");

        tokio::spawn(run_interceptor(listener, sniffer_addr, scmj_tx));

        Self {
            sender,
            set_custom_mining_job_rx,
            sniffer,
        }
    }

    pub async fn wait_for_set_custom_mining_job(&self) -> (u32, u32) {
        self.set_custom_mining_job_rx
            .recv()
            .await
            .expect("channel closed")
    }

    pub async fn send_open_extended_mining_channel_success(
        &self,
        request_id: u32,
        channel_id: u32,
        group_channel_id: u32,
    ) {
        self.sender
            .send(AnyMessage::Mining(
                parsers_sv2::Mining::OpenExtendedMiningChannelSuccess(
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
                ),
            ))
            .await
            .expect("send OpenExtendedMiningChannelSuccess");
    }

    pub async fn send_set_custom_mining_job_success(
        &self,
        channel_id: u32,
        request_id: u32,
        job_id: u32,
    ) {
        self.sender
            .send(AnyMessage::Mining(Mining::SetCustomMiningJobSuccess(
                SetCustomMiningJobSuccess {
                    channel_id,
                    request_id,
                    job_id,
                },
            )))
            .await
            .expect("send SetCustomMiningJobSuccess");
    }

    pub async fn send_submit_shares_success(&self, channel_id: u32, last_sequence_number: u32) {
        self.sender
            .send(AnyMessage::Mining(Mining::SubmitSharesSuccess(
                SubmitSharesSuccess {
                    channel_id,
                    last_sequence_number,
                    new_submits_accepted_count: 1,
                    new_shares_sum: 1,
                },
            )))
            .await
            .expect("send SubmitSharesSuccess");
    }

    pub async fn send_allocate_mining_job_token_success(&self, request_id: u32) {
        // The proxy uses an older JD format with three fields:
        // - coinbase_output_max_additional_size: u32
        // - coinbase_output: B064K
        // - async_mining_allowed: bool

        // while the test crate uses a newer format that merges them into `coinbase_outputs`.
        //
        // We encode 14 bytes so the proxy interprets them as:
        // - max additional size = 14
        // - coinbase_output length = 9
        // - coinbase_output = [0×9]
        // - async_mining_allowed = 1
        self.sender
            .send(AnyMessage::JobDeclaration(
                JobDeclaration::AllocateMiningJobTokenSuccess(AllocateMiningJobTokenSuccess {
                    request_id,
                    mining_job_token: vec![0; 32].try_into().unwrap(),
                    coinbase_outputs: vec![0u8, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
                        .try_into()
                        .unwrap(),
                }),
            ))
            .await
            .expect("send AllocateMiningJobTokenSuccess");
    }

    pub async fn send_declare_mining_job_success(&self, request_id: u32) {
        self.sender
            .send(AnyMessage::JobDeclaration(
                JobDeclaration::DeclareMiningJobSuccess(DeclareMiningJobSuccess {
                    request_id,
                    new_mining_job_token: vec![0; 32].try_into().expect("token too long"),
                }),
            ))
            .await
            .expect("send DeclareMiningJobSuccess");
    }
}

pub async fn setup_mock_pool(pool_addr: SocketAddr) -> ExtendedMockUpstream {
    ExtendedMockUpstream::new(
        pool_addr,
        WithSetup::yes_with_defaults(Protocol::MiningProtocol, 0),
        "pool",
    )
    .await
}

/// Forwards messages between the proxy and a MockUpstream, while intercepting `SetCustomMiningJob`
async fn run_interceptor(
    listener: tokio::net::TcpListener,
    sniffer_addr: SocketAddr,
    scmj_tx: Sender<(u32, u32)>,
) {
    let (stream, _) = listener.accept().await.expect("interceptor: accept failed");
    drop(listener); // Free port; JD sniffer may later bind the same addr.

    let (proxy_rx, proxy_tx) = create_downstream(stream)
        .await
        .expect("interceptor: create_downstream failed");

    let sniffer_stream = loop {
        match tokio::net::TcpStream::connect(sniffer_addr).await {
            Ok(s) => break s,
            Err(_) => tokio::time::sleep(tokio::time::Duration::from_millis(10)).await,
        }
    };
    let (mock_rx, mock_tx) = create_upstream(sniffer_stream)
        .await
        .expect("interceptor: create_upstream failed");

    tokio::spawn(async move {
        while let Ok(mut frame) = proxy_rx.recv().await {
            let mut intercepted = false;
            if let Frame::Sv2(ref mut sv2) = frame {
                if let Some(header) = sv2.get_header() {
                    let payload = sv2.payload().to_vec();
                    if header.msg_type() == MESSAGE_TYPE_SET_CUSTOM_MINING_JOB {
                        scmj_tx
                            .send((
                                u32::from_le_bytes(payload[0..4].try_into().unwrap()), // channel_id
                                u32::from_le_bytes(payload[4..8].try_into().unwrap()), // request_id
                            ))
                            .await
                            .ok();
                        intercepted = true;
                    }
                }
            }
            if !intercepted && mock_tx.send(frame).await.is_err() {
                break;
            }
        }
    });

    while let Ok(frame) = mock_rx.recv().await {
        if proxy_tx.send(frame).await.is_err() {
            break;
        }
    }
}
