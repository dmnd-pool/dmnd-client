#![allow(unused_crate_dependencies)]
#[tokio::test]
async fn basic() {
    let handle = tokio::spawn(async {
        dmnd_client::start().await;
    });

    // wait for the proxy to start listening for downstream connections
    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;

    // try to bind to the same address, should fail
    let stream = tokio::net::TcpListener::bind(dmnd_client::DEFAULT_LISTEN_ADDRESS).await;
    assert!(matches!(
        stream.err(),
        Some(e) if e.kind() == tokio::io::ErrorKind::AddrInUse
    ));
    drop(handle)
}
