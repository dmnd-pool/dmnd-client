use crate::support::{
    error,
    share_accounting::parser::{PoolExtMessages, PoolExtMessagesOwned},
    TestResult,
};
use dmnd_client::sv2_connection::{
    noise_connection_tokio::Connection, HandshakeRole, Initiator, Responder, StandardEitherFrame,
    StandardSv2Frame,
};
use key_utils::{Secp256k1PublicKey, Secp256k1SecretKey};
use serde::Serialize;
use tokio::{net::TcpStream, sync::mpsc, task::AbortHandle};

pub const AUTH_PUBLIC: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
// Published test authority from the locked stratum pool config examples.
const AUTH_SECRET: &str = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n";
pub type Message = PoolExtMessagesOwned;
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
        PoolExtMessages::try_from((extension, kind, frame.payload()))
            .map(|m| Some(m.into_owned()))
            .map_err(|e| error(format!("decode SV2 message: {e:?}")))
    }

    pub async fn send(&self, message: Message) -> TestResult {
        self.sender
            .send(encode(message)?)
            .await
            .map_err(|_| error("peer disconnected"))
    }
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
