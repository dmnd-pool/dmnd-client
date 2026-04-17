#![allow(unused_crate_dependencies)]
mod common;
use std::time::Duration;

use integration_tests_sv2::{
    interceptor::MessageDirection, start_minerd, start_sniffer, start_sv1_sniffer,
};
use stratum_apps::stratum_core::{
    common_messages_sv2::{MESSAGE_TYPE_SETUP_CONNECTION, MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS},
    mining_sv2::{
        MESSAGE_TYPE_NEW_EXTENDED_MINING_JOB, MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL,
        MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL_SUCCESS, MESSAGE_TYPE_SUBMIT_SHARES_EXTENDED,
        MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
    },
    template_distribution_sv2::MESSAGE_TYPE_SET_NEW_PREV_HASH,
};

use crate::common::setup_proxy;

#[tokio::test(flavor = "multi_thread")]
async fn test_mining_device() {
    let pool_addr: std::net::SocketAddr = "18.199.29.221:20000".parse().unwrap();
    let (proxy, downstream_addr) = setup_proxy(Some(pool_addr), None, false);

    let (sniffer_sv1, sniffer_sv1_addr) = start_sv1_sniffer(downstream_addr);

    let (_minerd_process, _minerd_addr) = start_minerd(
        sniffer_sv1_addr,
        Some("test".to_string()),
        Some("Password".to_string()),
        true,
    )
    .await;
    sniffer_sv1
        .wait_for_message(&["mining.subscribe"], MessageDirection::ToUpstream)
        .await;
    sniffer_sv1
        .wait_for_message(&["mining.authorize"], MessageDirection::ToUpstream)
        .await;

    sniffer_sv1
        .wait_for_message(&["mining.set_difficulty"], MessageDirection::ToDownstream)
        .await;
    sniffer_sv1
        .wait_for_message(&["mining.notify"], MessageDirection::ToDownstream)
        .await;

    proxy.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn pool_receives_share() {
    let (proxy, downstream_listening_addr) = setup_proxy(None, None, true);
    let (sniffer, sniffer_addr) = start_sniffer(
        "Pool Sniffer",
        downstream_listening_addr,
        false,
        vec![],
        None,
    );

    tokio::time::sleep(Duration::from_secs(5)).await;
    // only starts after OpenExtendedMiningChannel succeeds.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let (_minerd_process, _minerd_addr) = start_minerd(
        sniffer_addr,
        Some("test".to_string()),
        Some("Password".to_string()),
        true,
    )
    .await;
    sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;
    sniffer
        .wait_for_message_type(
            MessageDirection::ToUpstream,
            MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL,
        )
        .await;

    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL_SUCCESS,
        )
        .await;
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_NEW_EXTENDED_MINING_JOB,
        )
        .await;
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SET_NEW_PREV_HASH,
        )
        .await;
    sniffer
        .wait_for_message_type(
            MessageDirection::ToUpstream,
            MESSAGE_TYPE_SUBMIT_SHARES_EXTENDED,
        )
        .await;
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
        )
        .await;
    proxy.abort();
}
