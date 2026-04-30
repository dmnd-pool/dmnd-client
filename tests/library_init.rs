#![allow(unused_crate_dependencies)]
mod common;

use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use integration_tests_sv2::{
    interceptor::MessageDirection, start_minerd, start_sv1_sniffer, start_template_provider,
    template_provider::DifficultyLevel, utils::get_available_address,
};
use stratum_apps::stratum_core::{
    common_messages_sv2::{MESSAGE_TYPE_SETUP_CONNECTION, MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS},
    job_declaration_sv2::{
        AllocateMiningJobToken, DeclareMiningJob, MESSAGE_TYPE_ALLOCATE_MINING_JOB_TOKEN,
        MESSAGE_TYPE_ALLOCATE_MINING_JOB_TOKEN_SUCCESS, MESSAGE_TYPE_DECLARE_MINING_JOB_SUCCESS,
    },
    mining_sv2::{
        OpenExtendedMiningChannel, SubmitSharesExtended, MESSAGE_TYPE_SET_CUSTOM_MINING_JOB_SUCCESS,
    },
    parsers_sv2::{AnyMessage, JobDeclaration, Mining},
};

use crate::common::{setup_mock_jd_with_sniffer, setup_mock_pool, setup_proxy};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn library_init_sv2_setup_connection() {
    let mock_pool_addr: SocketAddr = get_available_address();
    let mock_pool = setup_mock_pool(mock_pool_addr).await;

    let (mock_jd, jd_sniffer) = setup_mock_jd_with_sniffer(mock_pool_addr).await;
    {
        // Start JD sniffer once the mock_pool connection has released the .
        let jd_sniffer = jd_sniffer.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            loop {
                if let Ok(listener) = TcpListener::bind(mock_pool_addr) {
                    drop(listener);
                    jd_sniffer.start();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
    }

    // No sniffer here because the sniffer cannot
    // parse CoinbaseOutputDataSize the proxy sends into CoinbaseOutputConstraints (0x70) that it expects.
    let (tp, tp_addr) = start_template_provider(Some(30), DifficultyLevel::High);

    let (proxy, downstream_addr) = setup_proxy(Some(mock_pool_addr), Some(tp_addr), false);

    // Pool SetupConnection
    mock_pool
        .sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    mock_pool
        .sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;

    // OpenExtendedMiningChannel
    let open_channel: OpenExtendedMiningChannel = loop {
        match mock_pool.sniffer.next_message_from_downstream() {
            Some((_, AnyMessage::Mining(Mining::OpenExtendedMiningChannel(msg)))) => break msg,
            _ => tokio::task::yield_now().await,
        }
    };
    mock_pool
        .send_open_extended_mining_channel_success(open_channel.request_id, 1, 100)
        .await;

    // JD SetupConnection
    jd_sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    jd_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;

    jd_sniffer
        .wait_for_message_type(
            MessageDirection::ToUpstream,
            MESSAGE_TYPE_ALLOCATE_MINING_JOB_TOKEN,
        )
        .await;

    // AllocateMiningJobToken
    let allocate: AllocateMiningJobToken = loop {
        match jd_sniffer.next_message_from_downstream() {
            Some((_, AnyMessage::JobDeclaration(JobDeclaration::AllocateMiningJobToken(msg)))) => {
                break msg
            }
            _ => continue,
        }
    };
    mock_jd
        .send_allocate_mining_job_token_success(allocate.request_id)
        .await;
    jd_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_ALLOCATE_MINING_JOB_TOKEN_SUCCESS,
        )
        .await;

    // DeclareMiningJob
    let declare: DeclareMiningJob = loop {
        match jd_sniffer.next_message_from_downstream() {
            Some((_, AnyMessage::JobDeclaration(JobDeclaration::DeclareMiningJob(msg)))) => {
                break msg
            }
            _ => tokio::task::yield_now().await,
        }
    };
    mock_jd
        .send_declare_mining_job_success(declare.request_id)
        .await;
    jd_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_DECLARE_MINING_JOB_SUCCESS,
        )
        .await;

    let (miner_sniffer, miner_sniffer_addr) = start_sv1_sniffer(downstream_addr);
    let (_minerd, _) = start_minerd(
        miner_sniffer_addr,
        Some("test".to_string()),
        Some("test".to_string()),
        true,
    )
    .await;

    // SetCustomMiningJob
    let (channel_id, request_id) = mock_pool.wait_for_set_custom_mining_job().await;
    mock_pool
        .send_set_custom_mining_job_success(channel_id, request_id, 2)
        .await;
    mock_pool
        .sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SET_CUSTOM_MINING_JOB_SUCCESS,
        )
        .await;

    // SubmitSharesExtended
    let submit: SubmitSharesExtended = loop {
        match mock_pool.sniffer.next_message_from_downstream() {
            Some((_, AnyMessage::Mining(Mining::SubmitSharesExtended(msg)))) => break msg,
            _ => tokio::task::yield_now().await,
        }
    };
    mock_pool
        .send_submit_shares_success(submit.channel_id, submit.sequence_number)
        .await;

    // SV1 messages
    miner_sniffer
        .wait_for_message(&["mining.subscribe"], MessageDirection::ToUpstream)
        .await;
    miner_sniffer
        .wait_for_message(&["mining.authorize"], MessageDirection::ToUpstream)
        .await;
    miner_sniffer
        .wait_for_message(&["mining.set_difficulty"], MessageDirection::ToDownstream)
        .await;
    miner_sniffer
        .wait_for_message(&["mining.notify"], MessageDirection::ToDownstream)
        .await;
    miner_sniffer
        .wait_for_message(&["mining.submit"], MessageDirection::ToUpstream)
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    proxy.abort();
    drop(tp);
}
