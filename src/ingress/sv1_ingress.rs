use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use crate::{
    config::Configuration,
    shared::{error::Sv1IngressError, utils::AbortOnDrop},
};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use roles_logic_sv2::utils::Mutex;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{channel, Receiver, Sender},
};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{error, info, warn};

pub fn start_listen_for_downstream(
    downstreams: Sender<(Sender<String>, Receiver<String>, IpAddr)>,
) -> AbortOnDrop {
    tokio::task::spawn(async move {
        let down_addr: String = Configuration::downstream_listening_addr()
            .unwrap_or(crate::DEFAULT_LISTEN_ADDRESS.to_string());
        let downstream_addr: SocketAddr = down_addr.parse().expect("Invalid listen address");
        info!(
            "Trying to bind to address {} for downstream(miner) connections",
            downstream_addr
        );
        let downstream_listener = TcpListener::bind(downstream_addr)
            .await
            .expect("impossible to bind downstream");
        info!(
            "Listening for downstream connections on {:?}",
            downstream_addr
        );
        while let Ok((stream, addr)) = downstream_listener.accept().await {
            info!("Try to connect {:#?}", addr);
            Downstream::initialize(
                stream,
                crate::MAX_LEN_DOWN_MSG,
                addr.ip(),
                downstreams.clone(),
            );
        }
    })
    .into()
}
struct Downstream {}

impl Downstream {
    pub fn initialize(
        stream: TcpStream,
        max_len_for_downstream_messages: u32,
        address: IpAddr,
        downstreams: Sender<(Sender<String>, Receiver<String>, IpAddr)>,
    ) {
        tokio::spawn(async move {
            info!("spawning downstream");
            let (send_to_upstream, recv) = channel(10);
            let (send, recv_from_upstream) = channel(10);
            downstreams
                .send((send, recv, address))
                .await
                .expect("Translator busy");
            let codec = LinesCodec::new_with_max_length(max_len_for_downstream_messages as usize);
            let framed = Framed::new(stream, codec);
            Self::start(framed, recv_from_upstream, send_to_upstream).await
        });
    }
    async fn start(
        framed: Framed<TcpStream, LinesCodec>,
        receiver: Receiver<String>,
        sender: Sender<String>,
    ) {
        let (writer, reader) = framed.split();
        let firmware = Arc::new(Mutex::new(Firmware::Uninitialized));
        let result = tokio::select! {
            result1 = Self::receive_from_downstream_and_relay_up(reader, sender, firmware.clone()) => result1,
            result2 = Self::receive_from_upstream_and_relay_down(writer, receiver, firmware.clone()) => result2,
        };
        // upstream disconnected make sure to clean everything before exit
        match result {
            Sv1IngressError::DownstreamDropped => (),
            Sv1IngressError::TranslatorDropped => (),
        }
    }
    async fn receive_from_downstream_and_relay_up(
        mut recv: SplitStream<Framed<TcpStream, LinesCodec>>,
        send: Sender<String>,
        firmware: Arc<Mutex<Firmware>>,
    ) -> Sv1IngressError {
        let mut is_subscribed = false;
        while let Some(Ok(message)) = recv.next().await {
            if Configuration::sv1_ingress_log() {
                info!("Sending msg to upstream: {}", message);
            }
            if !is_subscribed && message.contains("mining.subscribe") {
                is_subscribed = true;
                if message.contains("LUXminer") {
                    firmware.safe_lock(|f| *f = Firmware::Luxor).unwrap();
                } else {
                    firmware.safe_lock(|f| *f = Firmware::Other).unwrap();
                }
            }
            if send.send(message).await.is_err() {
                error!("Upstream dropped trying to send");
                return Sv1IngressError::TranslatorDropped;
            }
        }
        warn!("Downstream dropped while trying to send message up");
        Sv1IngressError::DownstreamDropped
    }
    async fn receive_from_upstream_and_relay_down(
        mut send: SplitSink<Framed<TcpStream, LinesCodec>, String>,
        mut recv: Receiver<String>,
        firmware_: Arc<Mutex<Firmware>>,
    ) -> Sv1IngressError {
        let mut firmware = Firmware::Uninitialized;
        while let Some(message) = recv.recv().await {
            let mut message = message.replace(['\n', '\r'], "");
            if !firmware.is_initialized() {
                firmware = firmware_.safe_lock(|f| *f).unwrap();
            } else if firmware.is_luxor() && !message.contains("\"id\"") {
                if let Some(pos) = message.find('{') {
                    message.insert_str(pos + 1, r#""id":null,"#);
                }
            }
            if Configuration::sv1_ingress_log() {
                info!("Sending msg to downstream_: {}", message);
            }
            if send.send(message).await.is_err() {
                warn!("Downstream dropped while trying to send message down");
                return Sv1IngressError::DownstreamDropped;
            };
        }
        if send.close().await.is_err() {
            error!("Failed to close connection");
        };
        error!("Upstream dropped trying to receive");
        Sv1IngressError::TranslatorDropped
    }
}

#[derive(Debug, Clone, Copy)]
enum Firmware {
    Luxor,
    Other,
    Uninitialized,
}

impl Firmware {
    fn is_initialized(&self) -> bool {
        !matches!(self, Firmware::Uninitialized)
    }
    fn is_luxor(&self) -> bool {
        matches!(self, Firmware::Luxor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn closes_upstream_channel_when_translator_drops() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener local addr");

        // Keep the client stream open so the downstream reader task stays pending.
        let client_stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to listener");
        let (server_stream, _) = listener.accept().await.expect("accept connection");

        let framed = tokio_util::codec::Framed::new(
            server_stream,
            tokio_util::codec::LinesCodec::new_with_max_length(1024),
        );

        let (tx_to_translator, mut rx_from_downstream) = tokio::sync::mpsc::channel(10);
        let (tx_to_downstream, rx_from_translator) = tokio::sync::mpsc::channel(10);

        let handle = tokio::spawn(async move {
            Downstream::start(framed, rx_from_translator, tx_to_translator).await;
        });

        // Drop the upstream sender so `receive_from_upstream_and_relay_down` completes and the
        // downstream reader task must be cancelled (and drop its sender) too.
        drop(tx_to_downstream);

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("ingress task should exit")
            .expect("ingress task should not panic");

        let recv_res = timeout(Duration::from_secs(1), rx_from_downstream.recv())
            .await
            .expect("upstream channel should close quickly");
        assert!(
            recv_res.is_none(),
            "upstream channel should be closed when translator drops"
        );

        drop(client_stream);
    }
}
