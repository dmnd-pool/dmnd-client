#![allow(unused_crate_dependencies)]
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use integration_tests_sv2::mock_roles::WithSetup;
use integration_tests_sv2::sniffer::Sniffer;

use integration_tests_sv2::start_template_provider;
use integration_tests_sv2::template_provider::DifficultyLevel;
use integration_tests_sv2::{interceptor::MessageDirection, utils::get_available_address};

use stratum_apps::stratum_core::common_messages_sv2::Protocol;
use stratum_apps::stratum_core::common_messages_sv2::MESSAGE_TYPE_SETUP_CONNECTION;

use stratum_apps::stratum_core::mining_sv2::{
    OpenExtendedMiningChannel, MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL_SUCCESS,
};
use stratum_apps::stratum_core::parsers_sv2::{AnyMessage, JobDeclaration, Mining};

use stratum_apps::stratum_core::job_declaration_sv2::{
    AllocateMiningJobToken, DeclareMiningJob, MESSAGE_TYPE_ALLOCATE_MINING_JOB_TOKEN_SUCCESS,
    MESSAGE_TYPE_DECLARE_MINING_JOB_SUCCESS,
};

use crate::common::{setup_proxy, ExtendedMockUpstream};

mod common;

#[tokio::test(flavor = "multi_thread")]
async fn test_jd_protocol() {
    let proxy_target: SocketAddr = get_available_address();
    let mock_pool_mining_addr: SocketAddr = get_available_address();
    let mock_pool_jd_addr: SocketAddr = get_available_address();
    let (template_provider, template_provider_addr) =
        start_template_provider(Some(1), DifficultyLevel::Low);

    let mock_pool = ExtendedMockUpstream::new(
        mock_pool_mining_addr,
        WithSetup::yes_with_defaults(Protocol::MiningProtocol, 0),
    )
    .await;

    let mock_jd = ExtendedMockUpstream::new(
        mock_pool_jd_addr,
        WithSetup::yes_with_defaults(Protocol::JobDeclarationProtocol, 0),
    )
    .await;

    let pool_sniffer = Sniffer::new(
        "proxy-pool-mining",
        proxy_target,
        mock_pool_mining_addr,
        false,
        vec![],
        Some(30),
    );
    pool_sniffer.start();

    let jd_sniffer = Sniffer::new(
        "proxy-pool-jd",
        proxy_target,
        mock_pool_jd_addr,
        false,
        vec![],
        Some(30),
    );
    {
        let jd_sniffer = jd_sniffer.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(listener) = TcpListener::bind(proxy_target.to_string()) {
                    drop(listener);
                    jd_sniffer.start();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
    }

    let (proxy, _listening_addr) =
        setup_proxy(Some(proxy_target), Some(template_provider_addr), false);

    let request: OpenExtendedMiningChannel = loop {
        match pool_sniffer.next_message_from_downstream() {
            Some((_, AnyMessage::Mining(Mining::OpenExtendedMiningChannel(msg)))) => {
                break msg;
            }
            // Other messages may sit in the queue ahead of it; skip them.
            _ => continue,
        }
    };

    mock_pool
        .send_open_extended_mining_channel_success(request.request_id, 1, 100)
        .await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    // mock_pool.send_new_extended_mining_job(1).await;
    jd_sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL_SUCCESS,
        )
        .await;

    let request: AllocateMiningJobToken = loop {
        match jd_sniffer.next_message_from_downstream() {
            Some((_, AnyMessage::JobDeclaration(JobDeclaration::AllocateMiningJobToken(msg)))) => {
                break msg;
            }
            // Other messages may sit in the queue ahead of it; skip them.
            _ => continue,
        }
    };
    jd_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_ALLOCATE_MINING_JOB_TOKEN_SUCCESS,
        )
        .await;
    mock_jd
        .send_allocate_mining_job_token_success(request.request_id)
        .await;

    template_provider.generate_blocks(1);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let declare_job: DeclareMiningJob = loop {
        match jd_sniffer.next_message_from_downstream() {
            Some((_, AnyMessage::JobDeclaration(JobDeclaration::DeclareMiningJob(msg)))) => {
                break msg;
            }
            _ => continue,
        }
    };
    mock_jd
        .send_declare_mining_job_success(declare_job.request_id)
        .await;

    jd_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_DECLARE_MINING_JOB_SUCCESS,
        )
        .await;

    proxy.abort();
    drop(template_provider);
}
