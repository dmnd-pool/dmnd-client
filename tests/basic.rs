#![allow(unused_crate_dependencies)] 
#[tokio::test]
async fn basic() {
    // NOTE: Set TOKEN and LOCAL env vars before running: TOKEN=test-token LOCAL=true cargo test
    let handle = tokio::spawn(async {
        dmnd_client::start().await;
    });

    // wait for the proxy to start listening for downstream connections
    let mut connected = false;
    for _ in 0..10 {
        if tokio::net::TcpStream::connect(dmnd_client::DEFAULT_LISTEN_ADDRESS).await.is_ok() {
            connected = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(connected, "Proxy failed to start listener in time");

    // try to bind to the same address, should fail
    let stream = tokio::net::TcpListener::bind(dmnd_client::DEFAULT_LISTEN_ADDRESS).await;
    assert!(matches!(
        stream.err(),
        Some(e) if e.kind() == tokio::io::ErrorKind::AddrInUse
    ));
    // clean up
    handle.abort();
}

#[tokio::test]
async fn verify_proxy_restart(){
    // NOTE: Set TOKEN and LOCAL/STAGING env vars before running: 
    // TOKEN=test Local=true AUTO_UPDATE=false cargo test --test basic
    // Start and drop the proxy after giving it time to bind
    let handle = tokio::spawn(async {
        dmnd_client::start().await;
    });
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    handle.abort();
    //cleanup
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let handle2 = tokio::spawn(async {
        dmnd_client::start().await;
    });
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    //Verify we can connect to the proxy after restart
    let addr = dmnd_client::DEFAULT_LISTEN_ADDRESS;
    match tokio::net::TcpStream::connect(addr).await {
        Ok(_) => println!("Reconnected to proxy after restart"),
        Err(e) => panic!("Failed to connect to proxy after restart:{}", e),
    }
    handle2.abort();
}