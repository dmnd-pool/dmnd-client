#![allow(unused_crate_dependencies)]
mod common;
use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use integration_tests_sv2::{interceptor::MessageDirection, utils::get_available_address};
use stratum_apps::stratum_core::common_messages_sv2::{
    MESSAGE_TYPE_SETUP_CONNECTION, MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
};

use crate::common::{
    setup_mock_jd_with_sniffer, setup_mock_pool_with_sniffer, setup_proxy, setup_tp_with_sniffer,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn library_init_sv2_setup_connection() {
    let mock_pool_addr: SocketAddr = get_available_address();
    let tp_sniffer_addr = get_available_address();

    let (pool_sniffer, _) = setup_mock_pool_with_sniffer(mock_pool_addr).await;
    pool_sniffer.start();

    let jd_sniffer = setup_mock_jd_with_sniffer(mock_pool_addr).await;
    {
        let jd_sniffer = jd_sniffer.clone();
        tokio::spawn(async move {
            // Sleep briefly to ensure pool_sniffer starts first
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

    let (tp, tp_sniffer) = setup_tp_with_sniffer(tp_sniffer_addr);
    tp_sniffer.start();

    let (proxy, _) = setup_proxy(Some(mock_pool_addr), Some(tp_sniffer_addr), false);

    pool_sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;

    pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;

    jd_sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;

    jd_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;

    tp_sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;

    tp_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;

    proxy.abort();
    drop(tp);
}
