use std::net::{IpAddr, SocketAddr};

use crate::shared::{error::Sv1IngressError, utils::AbortOnDrop};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{channel, Receiver, Sender},
};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{error, info, warn};

pub fn start_listen_for_downstream(
    downstreams: Sender<(Sender<String>, Receiver<String>, IpAddr)>,
) -> AbortOnDrop {
    info!("Starting downstream listner");
    tokio::task::spawn(async move {
        let down_addr: String = crate::SV1_DOWN_LISTEN_ADDR.to_string();
        let downstream_addr: SocketAddr = down_addr.parse().expect("Invalid listen address");
        let downstream_listener = TcpListener::bind(downstream_addr)
            .await
            .expect("impossible to bind downstream");
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
        let result = tokio::select! {
            result1 = Self::receive_from_downstream_and_relay_up(reader, sender) => result1,
            result2 = Self::receive_from_upstream_and_relay_down(writer, receiver) => result2,
        };
        // upstream disconnected make sure to clean everything before exit
        match result {
            Sv1IngressError::DownstreamDropped => (),
            Sv1IngressError::TranslatorDropped => (),
            Sv1IngressError::TaskFailed => (),
        }
    }
    async fn receive_from_downstream_and_relay_up(
        mut recv: SplitStream<Framed<TcpStream, LinesCodec>>,
        send: Sender<String>,
    ) -> Sv1IngressError {
        let task = tokio::spawn(async move {
            while let Some(Ok(message)) = recv.next().await {
                if send.send(message).await.is_err() {
                    error!("Upstream dropped trying to send");
                    return Sv1IngressError::TranslatorDropped;
                }
            }
            warn!("Downstream dropped while trying to send message up");
            Sv1IngressError::DownstreamDropped
        })
        .await;
        match task {
            Ok(err) => err,
            Err(_) => Sv1IngressError::TaskFailed,
        }
    }
    async fn receive_from_upstream_and_relay_down(
        mut send: SplitSink<Framed<TcpStream, LinesCodec>, String>,
        mut recv: Receiver<String>,
    ) -> Sv1IngressError {
        let task = tokio::spawn(async move {
            while let Some(message) = recv.recv().await {
                let message = message.replace(['\n', '\r'], "");
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
        })
        .await;
        match task {
            Ok(err) => err,
            Err(_) => Sv1IngressError::TaskFailed,
        }
    }
}