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

fn ensure_port_free_or_skip(address: &str) -> bool {
    match TcpListener::bind(address) {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            eprintln!("skipping: {address} is in use");
            false
        }
        Err(e) => panic!("failed to probe {address}: {e}"),
    }
}

async fn wait_for_port_to_be_bound(address: &str) {
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
    if !ensure_port_free_or_skip("127.0.0.1:20000") {
        return;
    }
    if !ensure_port_free_or_skip("127.0.0.1:20001") {
        return;
    }
    if !ensure_port_free_or_skip("127.0.0.1:20002") {
        return;
    }

    let proxy_target: SocketAddr = "127.0.0.1:20000".parse().unwrap();
    let mock_pool_mining_addr: SocketAddr = "127.0.0.1:20001".parse().unwrap();
    let mock_pool_jd_addr: SocketAddr = "127.0.0.1:20002".parse().unwrap();
    let tp_sniffer_addr = get_available_address();
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
    wait_for_port_to_be_bound("127.0.0.1:20000").await;

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
                if let Ok(listener) = TcpListener::bind("127.0.0.1:20000") {
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
        120_000,
        0,
        100_000_000_000_000.0,
        "info".to_string(),
        "off".to_string(),
        false,
        false,
        false,
        false,
        false,
        true,
        None,
        None,
        250,
        16_384,
        None,
        250,
        "3001".to_string(),
        false,
        false,
        "DDxDD".to_string(),
        None,
        "http://127.0.0.1:8332".to_string(),
        "user".to_string(),
        "password".to_string(),
        None,
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
