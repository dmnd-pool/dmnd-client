use crate::support::{error, TestResult};
#[cfg(legacy_sv2_transport)]
use codec_sv2::{
    HandshakeRole, NoiseEncoder, StandardEitherFrame, StandardNoiseDecoder, StandardSv2Frame, State,
};
use demand_share_accounting_ext::parser::PoolExtMessages;
#[cfg(not(legacy_sv2_transport))]
use demand_sv2_connection::{
    noise_connection_tokio::Connection, HandshakeRole, Initiator, Responder, StandardEitherFrame,
    StandardSv2Frame,
};
use key_utils::{Secp256k1PublicKey, Secp256k1SecretKey};
#[cfg(legacy_sv2_transport)]
use noise_sv2::{Initiator, Responder};
use serde::Serialize;
#[cfg(legacy_sv2_transport)]
use std::sync::Arc;
#[cfg(legacy_sv2_transport)]
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};
use tokio::{net::TcpStream, sync::mpsc, task::AbortHandle};

pub const AUTH_PUBLIC: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
// Published test authority from the locked stratum pool config examples.
const AUTH_SECRET: &str = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n";
pub type Message = PoolExtMessages<'static>;
pub type Frame = StandardEitherFrame<Message>;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize)]
pub struct PeerId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerProtocol {
    Mining,
    JobDeclaration,
}

pub struct PeerConnection {
    receiver: mpsc::Receiver<Frame>,
    pub sender: mpsc::Sender<Frame>,
    tasks: Vec<AbortHandle>,
    _socket: SocketGuard,
}

struct SocketGuard(std::net::TcpStream);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = self.0.shutdown(std::net::Shutdown::Both);
    }
}

impl PeerConnection {
    pub async fn accept(stream: TcpStream) -> TestResult<Self> {
        let public: Secp256k1PublicKey = AUTH_PUBLIC
            .parse()
            .map_err(|e| error(format!("test authority: {e:?}")))?;
        let secret: Secp256k1SecretKey = AUTH_SECRET
            .parse()
            .map_err(|e| error(format!("test authority: {e:?}")))?;
        let responder = Responder::from_authority_kp(
            &public.into_bytes(),
            &secret.into_bytes(),
            std::time::Duration::from_secs(3600),
        )
        .map_err(|e| error(format!("Noise responder: {e:?}")))?;
        Self::open(stream, HandshakeRole::Responder(responder)).await
    }

    pub async fn connect(address: std::net::SocketAddr) -> TestResult<Self> {
        let stream = TcpStream::connect(address).await?;
        let initiator =
            Initiator::without_pk().map_err(|e| error(format!("Noise initiator: {e:?}")))?;
        Self::open(stream, HandshakeRole::Initiator(initiator)).await
    }

    #[cfg(legacy_sv2_transport)]
    async fn open(stream: TcpStream, role: HandshakeRole) -> TestResult<Self> {
        let stream = stream.into_std()?;
        let socket = SocketGuard(stream.try_clone()?);
        let mut stream = TcpStream::from_std(stream)?;
        // Complete this socket's handshake before starting transport tasks. The locked
        // Connection::new uses process-wide flags, allowing another peer to advance its state.
        let noise = match role {
            HandshakeRole::Initiator(mut initiator) => {
                let first = initiator
                    .step_0()
                    .map_err(|e| error(format!("Noise handshake: {e:?}")))?;
                stream.write_all(&first).await?;
                initiator
                    .step_2(read_handshake(&mut stream).await?)
                    .map_err(|e| error(format!("Noise handshake: {e:?}")))?
            }
            HandshakeRole::Responder(mut responder) => {
                let (response, noise) = responder
                    .step_1(read_handshake(&mut stream).await?)
                    .map_err(|e| error(format!("Noise handshake: {e:?}")))?;
                stream.write_all(&response).await?;
                noise
            }
        };
        let state = Arc::new(Mutex::new(State::with_transport_mode(noise)));
        let (mut reader, mut writer) = stream.into_split();
        let (incoming, receiver) = mpsc::channel(10);
        let (sender, mut outgoing) = mpsc::channel::<Frame>(10);
        let read_state = state.clone();
        let read = tokio::spawn(async move {
            let mut decoder = StandardNoiseDecoder::<Message>::new();
            loop {
                if reader.read_exact(decoder.writable()).await.is_err() {
                    break;
                }
                let decoded = {
                    let mut state = read_state.lock().await;
                    decoder.next_frame(&mut state)
                };
                match decoded {
                    Ok(frame) => {
                        // Release the decoder's pooled slice before sending across tasks so its
                        // buffer can be dropped safely even when this connection is aborted.
                        let frame: StandardSv2Frame<Message> = frame.try_into().unwrap();
                        let mut bytes = vec![0; frame.encoded_length()];
                        if let Err(e) = frame.serialize(&mut bytes) {
                            tracing::error!("SV2 frame copy: {e:?}");
                            break;
                        }
                        let frame = StandardSv2Frame::from_bytes_unchecked(bytes.into());
                        if incoming.send(frame.into()).await.is_err() {
                            break;
                        }
                    }
                    Err(codec_sv2::Error::MissingBytes(_)) => {}
                    Err(e) => {
                        tracing::error!("SV2 transport decode: {e:?}");
                        break;
                    }
                }
            }
        });
        let write = tokio::spawn(async move {
            let mut encoder = NoiseEncoder::<Message>::new();
            while let Some(frame) = outgoing.recv().await {
                let encoded = {
                    let mut state = state.lock().await;
                    encoder.encode(frame, &mut state)
                };
                match encoded {
                    Ok(bytes) => {
                        if writer.write_all(bytes.as_ref()).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!("SV2 transport encode: {e:?}");
                        break;
                    }
                }
            }
            let _ = writer.shutdown().await;
        });
        Ok(Self {
            receiver,
            sender,
            tasks: vec![read.abort_handle(), write.abort_handle()],
            _socket: socket,
        })
    }

    #[cfg(not(legacy_sv2_transport))]
    async fn open(stream: TcpStream, role: HandshakeRole) -> TestResult<Self> {
        let stream = stream.into_std()?;
        let socket = SocketGuard(stream.try_clone()?);
        let stream = TcpStream::from_std(stream)?;
        let (receiver, sender, read, write) = Connection::new::<Message>(stream, role)
            .await
            .map_err(|e| error(format!("SV2 connection: {e:?}")))?;
        Ok(Self {
            receiver,
            sender,
            tasks: vec![read, write],
            _socket: socket,
        })
    }

    pub async fn recv(&mut self) -> TestResult<Option<Message>> {
        let Some(frame) = self.receiver.recv().await else {
            return Ok(None);
        };
        let mut frame: StandardSv2Frame<Message> = frame
            .try_into()
            .map_err(|e| error(format!("SV2 frame: {e:?}")))?;
        let header = frame
            .get_header()
            .ok_or_else(|| error("missing SV2 header"))?;
        let (extension, kind) = (header.ext_type(), header.msg_type());
        // The locked accounting extension includes TD variants but its TryFrom implementation
        // only dispatches common/mining/JD messages. Decode TD with the same locked SV2 parser.
        if extension & 0x7fff == 0
            && roles_logic_sv2::parsers::TemplateDistributionTypes::try_from(kind).is_ok()
        {
            return roles_logic_sv2::parsers::TemplateDistribution::try_from((
                kind,
                frame.payload(),
            ))
            .map(|message| Some(PoolExtMessages::TemplateDistribution(message).into_static()))
            .map_err(|e| error(format!("decode TD message: {e:?}")));
        }
        PoolExtMessages::try_from((extension, kind, frame.payload()))
            .map(|m| Some(m.into_static()))
            .map_err(|e| error(format!("decode SV2 message: {e:?}")))
    }

    pub async fn send(&self, message: Message) -> TestResult {
        self.sender
            .send(encode(message)?)
            .await
            .map_err(|_| error("peer disconnected"))
    }
}

#[cfg(legacy_sv2_transport)]
async fn read_handshake<const N: usize>(stream: &mut TcpStream) -> TestResult<[u8; N]> {
    let mut message = [0; N];
    stream.read_exact(&mut message).await?;
    Ok(message)
}

pub fn encode(message: Message) -> TestResult<Frame> {
    let frame: StandardSv2Frame<Message> = message
        .try_into()
        .map_err(|e| error(format!("encode SV2 message: {e:?}")))?;
    Ok(frame.into())
}

impl Drop for PeerConnection {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
