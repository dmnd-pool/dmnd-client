use crate::{Error, HandshakeRole, Initiator, Responder, StandardEitherFrame, StandardSv2Frame};
use binary_sv2::{GetSize, Serialize};
use codec_sv2::{
    Decrypted, Handshake, NoiseDecoder, NoiseEncoder, Transport, TransportDecryptState,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream},
    sync::{
        mpsc::{channel, Receiver, Sender},
        watch, Mutex,
    },
    task::{self, AbortHandle},
};
use tracing::{debug, error};

#[derive(Debug)]
pub struct Connection;

type Channels<Message> = (
    Receiver<StandardEitherFrame<Message>>,
    Sender<StandardEitherFrame<Message>>,
    AbortHandle,
    AbortHandle,
);

impl Connection {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new<Message: Serialize + GetSize + Send + 'static>(
        mut stream: TcpStream,
        role: HandshakeRole,
    ) -> Result<Channels<Message>, Error> {
        let transport = handshake(&mut stream, role).await?;
        let (encrypt, decrypt) = transport.split();
        let (reader, writer) = stream.into_split();
        let (incoming, receiver) = channel(10);
        let (sender, outgoing) = channel(10);
        let (closed, _) = watch::channel(false);

        let receiver_closed = incoming.clone();
        let reader_closed = CloseSignal(closed.clone());
        let mut reader_stop = closed.subscribe();
        let recv_task = task::spawn(async move {
            let _closed = reader_closed;
            tokio::select! {
                biased;
                _ = reader_stop.changed() => {},
                _ = receiver_closed.closed() => {},
                result = receive(reader, incoming, decrypt) => {
                    if let Err(error) = result {
                        debug!(?error, "SV2 receive stopped");
                    }
                }
            }
        });

        let writer_closed = CloseSignal(closed.clone());
        let mut writer_stop = closed.subscribe();
        let send_task = task::spawn(async move {
            let _closed = writer_closed;
            tokio::select! {
                biased;
                _ = writer_stop.changed() => {},
                result = send(writer, outgoing, encrypt) => {
                    if let Err(error) = result {
                        debug!(?error, "SV2 send stopped");
                    }
                }
            }
        });

        Ok((
            receiver,
            sender,
            recv_task.abort_handle(),
            send_task.abort_handle(),
        ))
    }
}

struct CloseSignal(watch::Sender<bool>);

impl Drop for CloseSignal {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

async fn handshake(stream: &mut TcpStream, role: HandshakeRole) -> Result<Transport, Error> {
    match role {
        HandshakeRole::Initiator(initiator) => {
            let (message, state) = Handshake::initiator(initiator).step_0()?;
            stream.write_all(message.payload()).await?;
            let mut reply = [0; noise_sv2::INITIATOR_EXPECTED_HANDSHAKE_MESSAGE_SIZE];
            stream.read_exact(&mut reply).await?;
            Ok(state.step_2(reply)?)
        }
        HandshakeRole::Responder(responder) => {
            let mut request = [0; noise_sv2::ELLSWIFT_ENCODING_SIZE];
            stream.read_exact(&mut request).await?;
            let (message, transport) = Handshake::responder(responder).step_1(request)?;
            stream.write_all(message.payload()).await?;
            Ok(transport)
        }
    }
}

async fn receive<Message: Serialize + GetSize + Send + 'static>(
    mut reader: OwnedReadHalf,
    incoming: Sender<StandardEitherFrame<Message>>,
    mut state: TransportDecryptState,
) -> Result<(), Error> {
    let owner = Arc::new(Mutex::new(NoiseDecoder::new()));
    let mut decoder = owner.lock().await;
    loop {
        if decoder.writable_len() != 0 {
            reader.read_exact(decoder.writable()).await?;
        }
        match decoder.next_transport_frame(state)? {
            Decrypted::Frame(frame, next) => {
                state = next;
                incoming
                    .send(StandardSv2Frame::received(frame, owner.clone()).into())
                    .await?;
            }
            Decrypted::Incomplete(_, next) => state = next,
        }
    }
}

async fn send<Message: Serialize + GetSize + Send + 'static>(
    mut writer: OwnedWriteHalf,
    mut outgoing: Receiver<StandardEitherFrame<Message>>,
    mut state: codec_sv2::TransportEncryptState,
) -> Result<(), Error> {
    let mut encoder = NoiseEncoder::new();
    while let Some(frame) = outgoing.recv().await {
        let frame = match frame {
            StandardEitherFrame::Sv2(frame) => frame,
            StandardEitherFrame::HandShake(_) => return Err(Error::HandshakeRemoteInvalidMessage),
        };
        let bytes = encoder.encode_transport(frame, &mut state)?;
        writer.write_all(bytes.as_ref()).await?;
    }
    writer.shutdown().await?;
    Ok(())
}

pub async fn listen(
    address: &str,
    authority_public_key: [u8; 32],
    authority_private_key: [u8; 32],
    cert_validity: Duration,
    sender: Sender<(TcpStream, HandshakeRole)>,
) {
    let listener = match TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(error) => {
            error!(%error, "Cannot bind SV2 listener");
            return;
        }
    };
    while let Ok((stream, _)) = listener.accept().await {
        let responder = match Responder::from_authority_kp(
            &authority_public_key,
            &authority_private_key,
            cert_validity,
        ) {
            Ok(responder) => responder,
            Err(error) => {
                error!(?error, "Cannot create SV2 responder");
                return;
            }
        };
        if sender
            .send((stream, HandshakeRole::Responder(responder)))
            .await
            .is_err()
        {
            return;
        }
    }
}

pub async fn connect(
    address: &str,
    authority_public_key: [u8; 32],
) -> Result<(TcpStream, HandshakeRole), ()> {
    let stream = TcpStream::connect(address).await.map_err(|_| ())?;
    let initiator = Initiator::from_raw_k(authority_public_key).map_err(|_| ())?;
    Ok((stream, HandshakeRole::Initiator(initiator)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use roles_logic_sv2::{
        bitcoin::secp256k1::{Keypair, Secp256k1, SecretKey, XOnlyPublicKey},
        mining_sv2::SubmitSharesStandardOwned,
    };

    type Message = SubmitSharesStandardOwned;

    fn roles() -> (HandshakeRole, HandshakeRole) {
        let secret = [7; 32];
        let key = SecretKey::from_slice(&secret).unwrap();
        let pair = Keypair::from_secret_key(&Secp256k1::new(), &key);
        let public = XOnlyPublicKey::from_keypair(&pair).0.serialize();
        (
            HandshakeRole::Initiator(Initiator::from_raw_k(public).unwrap()),
            HandshakeRole::Responder(
                Responder::from_authority_kp(&public, &secret, Duration::from_secs(60)).unwrap(),
            ),
        )
    }

    async fn sockets() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept(),
        );
        (client.unwrap(), accepted.unwrap().0)
    }

    async fn pair() -> (Channels<Message>, Channels<Message>) {
        let (client, server) = sockets().await;
        let (initiator, responder) = roles();
        let (client, server) = tokio::join!(
            Connection::new(client, initiator),
            Connection::new(server, responder),
        );
        (client.unwrap(), server.unwrap())
    }

    fn frame(sequence: u32) -> StandardSv2Frame<Message> {
        StandardSv2Frame::from_message(
            Message {
                channel_id: 7,
                sequence_number: sequence,
                job_id: 9,
                nonce: 11,
                ntime: 13,
                version: 15,
            },
            0x42,
            0,
            true,
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_connections_keep_handshakes_independent() {
        let mut tasks = tokio::task::JoinSet::new();
        for sequence in 0..24 {
            tasks.spawn(async move {
                let (
                    (mut client_rx, client_tx, client_read, client_write),
                    (mut server_rx, server_tx, server_read, server_write),
                ) = pair().await;
                client_tx.send(frame(sequence).into()).await.unwrap();
                let mut received: StandardSv2Frame<Message> =
                    server_rx.recv().await.unwrap().try_into().unwrap();
                assert_eq!(&received.payload()[4..8], sequence.to_le_bytes());
                server_tx.send(received.into()).await.unwrap();
                let mut received: StandardSv2Frame<Message> =
                    client_rx.recv().await.unwrap().try_into().unwrap();
                assert_eq!(&received.payload()[4..8], sequence.to_le_bytes());
                drop(received);
                client_read.abort();
                client_write.abort();
                server_read.abort();
                server_write.abort();
            });
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn received_frame_survives_connection_cancellation() {
        let (
            (mut client_rx, _client_tx, client_read, client_write),
            (_server_rx, server_tx, server_read, server_write),
        ) = pair().await;
        server_tx.send(frame(17).into()).await.unwrap();
        let mut received: StandardSv2Frame<Message> =
            client_rx.recv().await.unwrap().try_into().unwrap();
        client_read.abort();
        client_write.abort();
        tokio::task::yield_now().await;
        assert_eq!(&received.payload()[4..8], 17_u32.to_le_bytes());
        drop(received);
        server_read.abort();
        server_write.abort();
    }

    #[tokio::test]
    async fn fragmented_and_consecutive_frames_are_complete() {
        let (client, mut server) = sockets().await;
        let (initiator, responder) = roles();
        let (client, server_transport) = tokio::join!(
            Connection::new::<Message>(client, initiator),
            handshake(&mut server, responder),
        );
        let (mut incoming, _outgoing, reader, writer) = client.unwrap();
        let (mut encrypt, _) = server_transport.unwrap().split();
        let sending = tokio::spawn(async move {
            let mut encoder = NoiseEncoder::new();
            for sequence in 0..3 {
                let frame = frame(sequence);
                let bytes = encoder.encode_transport(frame, &mut encrypt).unwrap();
                for piece in bytes.as_ref().chunks(3) {
                    server.write_all(piece).await.unwrap();
                }
            }
        });
        for sequence in 0..3_u32 {
            let received = tokio::time::timeout(Duration::from_secs(3), incoming.recv())
                .await
                .unwrap()
                .unwrap();
            let mut received: StandardSv2Frame<Message> = received.try_into().unwrap();
            assert_eq!(&received.payload()[4..8], sequence.to_le_bytes());
        }
        sending.await.unwrap();
        reader.abort();
        writer.abort();
    }

    #[tokio::test]
    async fn empty_and_multi_chunk_frames_keep_stream_order() {
        let (client, mut server) = sockets().await;
        let (initiator, responder) = roles();
        let (client, server_transport) = tokio::join!(
            Connection::new::<Message>(client, initiator),
            handshake(&mut server, responder),
        );
        let (mut incoming, _outgoing, reader, writer) = client.unwrap();
        let (mut encrypt, _) = server_transport.unwrap().split();
        let sending = tokio::spawn(async move {
            let mut encoder = NoiseEncoder::new();
            let mut large = vec![0xA5; 196_614];
            large[..6].copy_from_slice(&[0, 0, 44, 0, 0, 3]);
            let frames = [
                StandardSv2Frame::<Message>::from_bytes(vec![0, 0, 43, 0, 0, 0]).unwrap(),
                StandardSv2Frame::<Message>::from_bytes(large).unwrap(),
                frame(71),
            ];
            for frame in frames {
                let bytes = encoder.encode_transport(frame, &mut encrypt).unwrap();
                for piece in bytes.as_ref().chunks(4093) {
                    server.write_all(piece).await.unwrap();
                }
            }
        });
        for (kind, length) in [(43, 0), (44, 196_608), (0x42, 24)] {
            let received = tokio::time::timeout(Duration::from_secs(3), incoming.recv())
                .await
                .unwrap()
                .unwrap();
            let mut received: StandardSv2Frame<Message> = received.try_into().unwrap();
            assert_eq!(received.get_header().unwrap().msg_type(), kind);
            assert_eq!(received.payload().len(), length);
            if kind == 44 {
                assert!(received.payload().iter().all(|byte| *byte == 0xA5));
            }
        }
        sending.await.unwrap();
        reader.abort();
        writer.abort();
    }

    #[tokio::test]
    async fn wrong_authority_key_rejects_handshake() {
        let (client, server) = sockets().await;
        let (_, responder) = roles();
        let key = SecretKey::from_slice(&[8; 32]).unwrap();
        let pair = Keypair::from_secret_key(&Secp256k1::new(), &key);
        let public = XOnlyPublicKey::from_keypair(&pair).0.serialize();
        let initiator = HandshakeRole::Initiator(Initiator::from_raw_k(public).unwrap());
        let (client, server) = tokio::join!(
            Connection::new::<Message>(client, initiator),
            Connection::new::<Message>(server, responder),
        );
        assert!(matches!(client, Err(Error::CodecError(_))));
        if let Ok((_, _, reader, writer)) = server {
            reader.abort();
            writer.abort();
        }
    }

    #[tokio::test]
    async fn invalid_ciphertext_closes_both_channels() {
        let (client, mut server) = sockets().await;
        let (initiator, responder) = roles();
        let (client, server_transport) = tokio::join!(
            Connection::new::<Message>(client, initiator),
            handshake(&mut server, responder),
        );
        server_transport.unwrap();
        let (mut incoming, outgoing, reader, writer) = client.unwrap();
        server
            .write_all(&[0; codec_sv2::ENCRYPTED_SV2_FRAME_HEADER_SIZE])
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), incoming.recv())
                .await
                .unwrap()
                .is_none()
        );
        tokio::time::timeout(Duration::from_secs(3), outgoing.closed())
            .await
            .unwrap();
        reader.abort();
        writer.abort();
    }

    #[tokio::test]
    async fn handshake_message_after_setup_closes_connection() {
        let (
            (mut client_rx, client_tx, client_read, client_write),
            (_server_rx, _server_tx, server_read, server_write),
        ) = pair().await;
        let unexpected = framing_sv2::framing::HandshakeMessage::from_message([0; 64]);
        client_tx.send(unexpected.into()).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), client_rx.recv())
                .await
                .unwrap()
                .is_none()
        );
        tokio::time::timeout(Duration::from_secs(3), client_tx.closed())
            .await
            .unwrap();
        client_read.abort();
        client_write.abort();
        server_read.abort();
        server_write.abort();
    }
}
