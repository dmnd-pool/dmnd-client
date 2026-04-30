use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    config::Configuration,
    shared::{error::Sv1IngressError, utils::AbortOnDrop},
    DownstreamConnection,
};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use roles_logic_sv2::utils::Mutex;
use tokio::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::{
        mpsc::{channel, Receiver, Sender},
        OwnedSemaphorePermit, Semaphore,
    },
};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{debug, error, info, warn};

struct AcceptWindowLimiter {
    max_accepts_per_window: Option<usize>,
    window: Duration,
    window_started_at: Instant,
    accepted_in_window: usize,
}

impl AcceptWindowLimiter {
    fn new(max_accepts_per_window: Option<usize>, window: Duration) -> Self {
        Self {
            max_accepts_per_window: max_accepts_per_window.filter(|value| *value > 0),
            window,
            window_started_at: Instant::now(),
            accepted_in_window: 0,
        }
    }

    fn wait_duration(&mut self) -> Option<Duration> {
        self.refresh_window();
        let limit = self.max_accepts_per_window?;
        if self.accepted_in_window < limit {
            return None;
        }
        Some(self.window.saturating_sub(self.window_started_at.elapsed()))
    }

    fn record_accept(&mut self) {
        self.refresh_window();
        if self.max_accepts_per_window.is_some() {
            self.accepted_in_window = self.accepted_in_window.saturating_add(1);
        }
    }

    fn refresh_window(&mut self) {
        if self.window_started_at.elapsed() >= self.window {
            self.window_started_at = Instant::now();
            self.accepted_in_window = 0;
        }
    }
}

pub fn start_listen_for_downstream(downstreams: Sender<DownstreamConnection>) -> AbortOnDrop {
    tokio::task::spawn(async move {
        let down_addr: String = Configuration::downstream_listening_addr()
            .unwrap_or(crate::DEFAULT_LISTEN_ADDRESS.to_string());
        let downstream_addr: SocketAddr = down_addr.parse().expect("Invalid listen address");
        let max_active_downstreams = Configuration::max_active_downstreams();
        let connection_slots =
            max_active_downstreams.map(|max| Arc::new(Semaphore::new(max)));
        let accept_backoff = Duration::from_millis(Configuration::accept_backoff_ms());
        let accept_window = Duration::from_millis(Configuration::accept_window_ms());
        let max_accepts_per_window = Configuration::max_accepts_per_window();
        let mut accept_window_limiter =
            AcceptWindowLimiter::new(max_accepts_per_window, accept_window);
        let overload_log_interval = Duration::from_secs(5);
        let mut last_overload_log = Instant::now()
            .checked_sub(overload_log_interval)
            .unwrap_or_else(Instant::now);
        info!(
            "Trying to bind to address {} for downstream(miner) connections",
            downstream_addr
        );
        let downstream_listener = bind_downstream_listener(
            downstream_addr,
            Configuration::accept_backlog(),
        )
        .expect("impossible to bind downstream");
        info!(
            "Listening for downstream connections on {:?}",
            downstream_addr
        );
        loop {
            if let Some(slots) = connection_slots.as_ref() {
                if slots.available_permits() == 0 {
                    if last_overload_log.elapsed() >= overload_log_interval {
                        warn!(
                            "Active downstream limit reached ({max} active connections), pausing accepts for {}ms",
                            accept_backoff.as_millis(),
                            max = max_active_downstreams.unwrap_or_default(),
                        );
                        last_overload_log = Instant::now();
                    }
                    tokio::time::sleep(accept_backoff).await;
                    continue;
                }
            }

            if let Some(wait_for) = accept_window_limiter.wait_duration() {
                if last_overload_log.elapsed() >= overload_log_interval {
                    warn!(
                        "Accept rate limit reached ({limit} downstreams per {}ms), pausing accepts for {}ms",
                        accept_window.as_millis(),
                        wait_for.as_millis(),
                        limit = max_accepts_per_window.unwrap_or_default(),
                    );
                    last_overload_log = Instant::now();
                }
                tokio::time::sleep(wait_for).await;
                continue;
            }

            let (stream, addr) = match downstream_listener.accept().await {
                Ok(connection) => connection,
                Err(e) => {
                    error!("Failed to accept downstream connection: {e}");
                    continue;
                }
            };
            let accepted_at = Instant::now();

            debug!("Accepted downstream connection from {:?}", addr);
            let connection_slot = match connection_slots.as_ref() {
                Some(slots) => match slots.clone().acquire_owned().await {
                    Ok(permit) => Some(permit),
                    Err(_) => {
                        error!("Failed to acquire downstream connection slot after accept");
                        drop(stream);
                        continue;
                    }
                },
                None => None,
            };
            accept_window_limiter.record_accept();
            Downstream::initialize(
                stream,
                crate::MAX_LEN_DOWN_MSG,
                addr.ip(),
                accepted_at,
                downstreams.clone(),
                connection_slot,
            );
        }
    })
    .into()
}

fn bind_downstream_listener(addr: SocketAddr, backlog: u32) -> std::io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(backlog)
}
struct Downstream {}

impl Downstream {
    pub fn initialize(
        stream: TcpStream,
        max_len_for_downstream_messages: u32,
        address: IpAddr,
        accepted_at: Instant,
        downstreams: Sender<DownstreamConnection>,
        connection_slot: Option<OwnedSemaphorePermit>,
    ) {
        tokio::spawn(async move {
            debug!("Spawning downstream task for {}", address);
            let (send_to_upstream, recv) = channel(10);
            let (send, recv_from_upstream) = channel(10);
            let codec = LinesCodec::new_with_max_length(max_len_for_downstream_messages as usize);
            let framed = Framed::new(stream, codec);
            let relay_handle = tokio::spawn(Self::start(
                framed,
                recv_from_upstream,
                send_to_upstream,
                connection_slot,
            ));
            if downstreams
                .send(DownstreamConnection {
                    send_to_downstream: send,
                    recv_from_downstream: recv,
                    address,
                    accepted_at,
                })
                .await
                .is_err()
            {
                error!(
                    "Dropping downstream connection from {} because the translator accept queue is closed",
                    address
                );
                relay_handle.abort();
                return;
            }
            if let Err(e) = relay_handle.await {
                debug!("Downstream relay task for {} ended: {}", address, e);
            }
        });
    }
    async fn start(
        framed: Framed<TcpStream, LinesCodec>,
        receiver: Receiver<String>,
        sender: Sender<String>,
        _connection_slot: Option<OwnedSemaphorePermit>,
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
            Sv1IngressError::TaskFailed => (),
        }
    }
    async fn receive_from_downstream_and_relay_up(
        mut recv: SplitStream<Framed<TcpStream, LinesCodec>>,
        send: Sender<String>,
        firmware: Arc<Mutex<Firmware>>,
    ) -> Sv1IngressError {
        let mut is_subscribed = false;
        let task = tokio::spawn(async move {
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
            debug!("Downstream dropped while trying to send message up");
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
        firmware_: Arc<Mutex<Firmware>>,
    ) -> Sv1IngressError {
        let mut firmware = Firmware::Uninitialized;
        let task = tokio::spawn(async move {
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
                    debug!("Downstream dropped while trying to send message down");
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
