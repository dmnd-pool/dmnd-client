use crate::Error;
use binary_sv2::{Deserialize, Serialize};
use futures::lock::Mutex;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{self, AbortHandle},
};

use binary_sv2::GetSize;
use codec_sv2::{HandshakeRole, Initiator, Responder, StandardEitherFrame, StandardNoiseDecoder};

use tracing::{debug, error};

#[derive(Debug)]
pub struct Connection {
    pub state: codec_sv2::State,
}

impl crate::SetState for Connection {
    async fn set_state(self_: Arc<Mutex<Self>>, state: codec_sv2::State) {
        loop {
            if crate::HANDSHAKE_READY.load(std::sync::atomic::Ordering::SeqCst) {
                if let Some(mut connection) = self_.try_lock() {
                    connection.state = state;
                    crate::TRANSPORT_READY.store(true, std::sync::atomic::Ordering::Relaxed);
                    break;
                };
            }
            task::yield_now().await;
        }
    }
}

impl Connection {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new<'a, Message: Serialize + Deserialize<'a> + GetSize + Send + 'static>(
        stream: TcpStream,
        role: HandshakeRole,
    ) -> Result<
        (
            Receiver<StandardEitherFrame<Message>>,
            Sender<StandardEitherFrame<Message>>,
            AbortHandle,
            AbortHandle,
        ),
        Error,
    > {
        let address = stream.peer_addr().map_err(|_| Error::SocketClosed)?;

        let (mut reader, mut writer) = stream.into_split();

        let (sender_incoming, mut receiver_incoming): (
            Sender<StandardEitherFrame<Message>>,
            Receiver<StandardEitherFrame<Message>>,
        ) = channel(10);
        let (mut sender_outgoing, mut receiver_outgoing): (
            Sender<StandardEitherFrame<Message>>,
            Receiver<StandardEitherFrame<Message>>,
        ) = channel(10);

        let state = codec_sv2::State::not_initialized(&role);

        let connection = Arc::new(Mutex::new(Self { state }));

        let cloned1 = connection.clone();
        let cloned2 = connection.clone();

        // RECEIVE AND PARSE INCOMING MESSAGES FROM TCP STREAM
        let recv_task = task::spawn(async move {
            let mut decoder = StandardNoiseDecoder::<Message>::new();

            loop {
                let writable = decoder.writable();
                match reader.read_exact(writable).await {
                    Ok(_) => {
                        let mut connection = cloned1.lock().await;
                        let decoded = decoder.next_frame(&mut connection.state);
                        drop(connection);

                        match decoded {
                            Ok(x) => {
                                if sender_incoming.send(x).await.is_err() {
                                    error!("Shutting down noise stream reader!");
                                    task::yield_now().await;
                                    break;
                                }
                            }
                            Err(e) => {
                                if let codec_sv2::Error::MissingBytes(_) = e {
                                } else {
                                    error!("Shutting down noise stream reader! {:#?}", e);
                                    task::yield_now().await;
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Disconnected from client while reading : {} - {}",
                            e, &address
                        );
                        task::yield_now().await;
                        break;
                    }
                }
            }
            drop(sender_incoming);
            drop(cloned1);
            drop(reader);
            let mut times = 0;
            while !decoder.droppable() {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if times >= 60 {
                    error!("Irrecoverable error impossible to free decoder");
                    std::process::exit(1);
                }
                times += 1;
            }
        });

        // ENCODE AND SEND INCOMING MESSAGES TO TCP STREAM
        let send_task = task::spawn(async move {
            let mut encoder = codec_sv2::NoiseEncoder::<Message>::new();

            loop {
                let received = receiver_outgoing.recv().await;

                match received {
                    Some(frame) => {
                        let mut connection = cloned2.lock().await;
                        match encoder.encode(frame, &mut connection.state) {
                            Ok(b) => {
                                drop(connection);

                                let b = b.as_ref();

                                match (writer).write_all(b).await {
                                    Ok(_) => (),
                                    Err(e) => {
                                        let _ = writer.shutdown().await;
                                        // Just fail and force to reinitialize everything
                                        error!(
                                        "Disconnecting from client due to error writing: {} - {}",
                                        e, &address
                                    );
                                        task::yield_now().await;
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Disconnecting from client due to error encoding: {} - {}",
                                    e, &address
                                );
                                drop(connection);
                                task::yield_now().await;
                                break;
                            }
                        };
                    }
                    None => {
                        // Just fail and force to reinitialize everything
                        let _ = writer.shutdown().await;
                        error!(
                            "Disconnecting from client due to error receiving from: {}",
                            &address
                        );
                        task::yield_now().await;
                        break;
                    }
                };
                crate::HANDSHAKE_READY.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            receiver_outgoing.close();
            drop(receiver_outgoing);
            drop(cloned2);
            drop(writer);
            let mut times = 0;
            while !encoder.droppable() {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if times >= 10 {
                    error!("Irrecoverable error impossible to free encoder");
                    std::process::exit(1);
                }
                times += 1;
            }
        });

        // DO THE NOISE HANDSHAKE
        match role {
            HandshakeRole::Initiator(_) => {
                debug!("Initializing as downstream for - {}", &address);
                crate::initialize_as_downstream(
                    connection.clone(),
                    role,
                    &mut sender_outgoing,
                    &mut receiver_incoming,
                )
                .await?
            }
            HandshakeRole::Responder(_) => {
                debug!("Initializing as upstream for - {}", &address);
                crate::initialize_as_upstream(
                    connection.clone(),
                    role,
                    &mut sender_outgoing,
                    &mut receiver_incoming,
                )
                .await?
            }
        };
        debug!("Noise handshake complete - {}", &address);
        Ok((
            receiver_incoming,
            sender_outgoing,
            recv_task.abort_handle(),
            send_task.abort_handle(),
        ))
    }
}

pub async fn listen(
    address: &str,
    authority_public_key: [u8; 32],
    authority_private_key: [u8; 32],
    cert_validity: Duration,
    sender: Sender<(TcpStream, HandshakeRole)>,
) {
    let listner = TcpListener::bind(address).await.unwrap();
    loop {
        if let Ok((stream, _)) = listner.accept().await {
            let responder = Responder::from_authority_kp(
                &authority_public_key,
                &authority_private_key,
                cert_validity,
            )
            .unwrap();
            let role = HandshakeRole::Responder(responder);
            let _ = sender.send((stream, role)).await;
        }
    }
}

pub async fn connect(
    address: &str,
    authority_public_key: [u8; 32],
) -> Result<(TcpStream, HandshakeRole), ()> {
    let stream = TcpStream::connect(address).await.map_err(|_| ())?;
    let initiator = Initiator::from_raw_k(authority_public_key).unwrap();
    let role = HandshakeRole::Initiator(initiator);
    Ok((stream, role))
}
