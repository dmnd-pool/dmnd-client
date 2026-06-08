#![allow(unused_crate_dependencies)]

use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use integration_tests_sv2::{
    interceptor::MessageDirection,
    mock_roles::{MockUpstream, WithSetup},
    sniffer::Sniffer,
    start_template_provider,
    template_provider::DifficultyLevel,
    utils::get_available_address,
};
use stratum_apps::stratum_core::common_messages_sv2::{
    Protocol, MESSAGE_TYPE_SETUP_CONNECTION, MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
};

async fn wait_for_port_to_be_bound(address: SocketAddr) {
    let started = std::time::Instant::now();
    loop {
        match TcpListener::bind(address) {
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => return,
            Ok(listener) => drop(listener),
            Err(e) => panic!("failed to probe {address}: {e}"),
        }

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "timed out waiting for {address} to be bound"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn library_init_sv2_setup_connection() {
    let proxy_target = get_available_address();
    let mock_pool_mining_addr = get_available_address();
    let mock_pool_jd_addr = get_available_address();
    let tp_sniffer_addr = get_available_address();
    let sv1_listen_addr = get_available_address();
    let api_server_port = get_available_address().port().to_string();
    let (template_provider, template_provider_addr) =
        start_template_provider(Some(1), DifficultyLevel::Low);

    let _mock_pool_mining = MockUpstream::new(
        mock_pool_mining_addr,
        WithSetup::yes_with_defaults(Protocol::MiningProtocol, 0),
    )
    .start()
    .await;

    let _mock_pool_jd = MockUpstream::new(
        mock_pool_jd_addr,
        WithSetup::yes_with_defaults(Protocol::JobDeclarationProtocol, 0),
    )
    .start()
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
    wait_for_port_to_be_bound(proxy_target).await;

    let jd_pool_sniffer = Sniffer::new(
        "proxy-pool-jd",
        proxy_target,
        mock_pool_jd_addr,
        false,
        vec![],
        Some(30),
    );
    {
        let jd_pool_sniffer = jd_pool_sniffer.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(listener) = TcpListener::bind(proxy_target) {
                    drop(listener);
                    jd_pool_sniffer.start();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
    }

    let tp_sniffer = Sniffer::new(
        "proxy-tp",
        tp_sniffer_addr,
        template_provider_addr,
        false,
        vec![],
        Some(30),
    );
    tp_sniffer.start();

    let config = dmnd_client::Configuration::new(
        Some("test_token".to_string()),
        Some(tp_sniffer_addr.to_string()),
        None,
        vec![proxy_target.to_string()],
        120_000,
        0,
        100_000_000_000_000.0,
        131_072.0,
        "info".to_string(),
        "off".to_string(),
        false,
        false,
        false,
        false,
        false,
        true,
        Some(sv1_listen_addr.to_string()),
        None,
        250,
        16_384,
        None,
        250,
        api_server_port,
        false,
        false,
        None,
        "http://127.0.0.1:8332".to_string(),
        "user".to_string(),
        "password".to_string(),
        "100000000".to_string(),
        "api-token".to_string(),
    );

    let proxy = tokio::spawn(dmnd_client::start(config));

    pool_sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;

    pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;

    jd_pool_sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;

    jd_pool_sniffer
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
    drop(template_provider);
}
