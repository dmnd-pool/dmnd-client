#![allow(unused_crate_dependencies)]

use std::sync::{Mutex, OnceLock};

use tokio::{
    net::TcpListener,
    sync::mpsc::{Receiver, Sender},
    task::{AbortHandle, JoinHandle},
    time::Duration,
};

const DEFAULT_LISTEN_ADDRESS: &str = dmnd_client::DEFAULT_LISTEN_ADDRESS;
const MAX_LEN_DOWN_MSG: u32 = 10_000;
type DownstreamConnection = (Sender<String>, Receiver<String>, std::net::IpAddr);

mod config {
    use super::{Mutex, OnceLock};

    static LISTENING_ADDR: OnceLock<Mutex<Option<String>>> = OnceLock::new();

    fn listening_addr() -> &'static Mutex<Option<String>> {
        LISTENING_ADDR.get_or_init(|| Mutex::new(None))
    }

    pub fn set_downstream_listening_addr(addr: String) {
        *listening_addr()
            .lock()
            .expect("listening address mutex poisoned") = Some(addr);
    }

    pub struct Configuration;

    impl Configuration {
        pub fn downstream_listening_addr() -> Option<String> {
            listening_addr()
                .lock()
                .expect("listening address mutex poisoned")
                .clone()
        }

        pub fn sv1_ingress_log() -> bool {
            false
        }
    }
}

mod shared {
    pub mod error {
        #[derive(Debug)]
        pub enum Sv1IngressError {
            DownstreamDropped,
            TranslatorDropped,
            TaskFailed,
        }
    }

    pub mod utils {
        use super::super::{AbortHandle, JoinHandle};

        #[derive(Debug)]
        pub struct AbortOnDrop {
            abort_handle: Vec<AbortHandle>,
        }

        impl AbortOnDrop {
            pub fn new<T: Send + 'static>(handle: JoinHandle<T>) -> Self {
                let abort_handle = vec![handle.abort_handle()];
                Self { abort_handle }
            }
        }

        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                for task in &self.abort_handle {
                    task.abort();
                }
            }
        }

        impl<T: Send + 'static> From<JoinHandle<T>> for AbortOnDrop {
            fn from(value: JoinHandle<T>) -> Self {
                Self::new(value)
            }
        }
    }
}

#[path = "../src/ingress/sv1_ingress.rs"]
mod sv1_ingress;

async fn reserve_free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to reserve local address");
    let address = listener
        .local_addr()
        .expect("reserved listener missing local address");
    drop(listener);
    address.to_string()
}

#[tokio::test]
async fn basic() {
    let listening_addr = reserve_free_addr().await;
    config::set_downstream_listening_addr(listening_addr.clone());

    let (downstreams_tx, _downstreams_rx): (
        Sender<DownstreamConnection>,
        Receiver<DownstreamConnection>,
    ) = tokio::sync::mpsc::channel(1);

    let ingress = sv1_ingress::start_listen_for_downstream(downstreams_tx);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match TcpListener::bind(&listening_addr).await {
                Err(e) if e.kind() == tokio::io::ErrorKind::AddrInUse => break,
                Ok(listener) => {
                    drop(listener);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => panic!("unexpected bind error for {listening_addr}: {e}"),
            }
        }
    })
    .await
    .expect("downstream listener did not bind in time");

    drop(ingress);
}
