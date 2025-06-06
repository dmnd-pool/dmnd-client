pub(crate) mod errors;
mod task_manager;

use std::net::SocketAddr;

use codec_sv2::{HandshakeRole, StandardEitherFrame, StandardSv2Frame};
use demand_share_accounting_ext::parser::PoolExtMessages;
use demand_sv2_connection::noise_connection_tokio::Connection;
use errors::Error;
use key_utils::Secp256k1PublicKey;
use noise_sv2::Initiator;
use roles_logic_sv2::{
    common_messages_sv2::{Protocol, SetupConnection, SetupConnectionSuccess},
    parsers::CommonMessages,
};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info};

use crate::{
    config::Configuration, proxy_state::ProxyState, shared::utils::AbortOnDrop, PoolState,
};
use task_manager::TaskManager;

pub type Message = PoolExtMessages<'static>;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;

const DEFAULT_TIMER: std::time::Duration = std::time::Duration::from_secs(5);

pub async fn connect_pool(
    address: SocketAddr,
    authority_public_key: Secp256k1PublicKey,
    setup_connection_msg: Option<SetupConnection<'static>>,
    timer: Option<std::time::Duration>,
) -> Result<
    (
        Sender<PoolExtMessages<'static>>,
        Receiver<PoolExtMessages<'static>>,
        AbortOnDrop,
    ),
    Error,
> {
    let socket = loop {
        match TcpStream::connect(address).await {
            Ok(socket) => break socket,
            Err(e) => {
                error!(
                    "Failed to connect to Upstream role at {}, retrying in 5s: {}",
                    address, e
                );
                tokio::time::sleep(std::time::Duration::from_secs(5)).await
            }
        }
    };

    let initiator =
        Initiator::from_raw_k(authority_public_key.into_bytes()).expect("Invalid authority key");

    info!(
        "PROXY SERVER - ACCEPTING FROM UPSTREAM: {}",
        socket.peer_addr().expect("Failed to get peer address")
    );

    // Channel to send and receive messages to the SV2 Upstream role
    let (mut receiver, mut sender, _, _) =
        Connection::new(socket, HandshakeRole::Initiator(initiator))
            .await
            .map_err(|e| {
                error!("Failed to create connection");
                Error::SV2Connection(e)
            })?;
    let setup_connection_msg =
        setup_connection_msg.unwrap_or(get_mining_setup_connection_msg(true, false)); //use device_id set by the user
    match mining_setup_connection(
        &mut receiver,
        &mut sender,
        setup_connection_msg,
        timer.unwrap_or(DEFAULT_TIMER),
    )
    .await
    {
        Ok(_) => {
            let task_manager = TaskManager::initialize();
            let abortable = task_manager
                .safe_lock(|t| t.get_aborter())
                .map_err(|_| Error::MiningPoolMutexCorrupted)?
                .ok_or(Error::MiningPoolTaskManagerFailed)?;

            let (send_to_down, recv_from_down) = tokio::sync::mpsc::channel(10);
            let (send_from_down, recv_to_up) = tokio::sync::mpsc::channel(10);
            let relay_up_task = relay_up(recv_to_up, sender);
            TaskManager::add_sv2_relay_up(task_manager.clone(), relay_up_task)
                .await
                .map_err(|_| Error::MiningPoolTaskManagerFailed)?;

            let relay_down_task = relay_down(receiver, send_to_down);
            TaskManager::add_sv2_relay_down(task_manager.clone(), relay_down_task)
                .await
                .map_err(|_| Error::MiningPoolTaskManagerFailed)?;
            Ok((send_from_down, recv_from_down, abortable))
        }
        Err(e) => Err(e),
    }
}

pub fn relay_up(
    mut recv: Receiver<PoolExtMessages<'static>>,
    send: Sender<EitherFrame>,
) -> AbortOnDrop {
    let task = tokio::spawn(async move {
        while let Some(msg) = recv.recv().await {
            let std_frame: Result<StdFrame, _> = msg.try_into();
            if let Ok(std_frame) = std_frame {
                let either_frame: EitherFrame = std_frame.into();
                if send.send(either_frame).await.is_err() {
                    error!("Mining upstream failed");
                    ProxyState::update_pool_state(PoolState::Down);
                    break;
                };
            } else {
                panic!("Internal Mining downstream try to send invalid message");
            }
        }
    });
    task.into()
}

pub fn relay_down(
    mut recv: Receiver<EitherFrame>,
    send: Sender<PoolExtMessages<'static>>,
) -> AbortOnDrop {
    let task = tokio::spawn(async move {
        while let Some(msg) = recv.recv().await {
            let msg: Result<StdFrame, ()> = msg.try_into().map_err(|_| ());
            if let Ok(mut msg) = msg {
                if let Some(header) = msg.get_header() {
                    let message_type = header.msg_type();
                    let payload = msg.payload();
                    let extension = header.ext_type();
                    let msg: Result<PoolExtMessages<'_>, _> =
                        (extension, message_type, payload).try_into();
                    if let Ok(msg) = msg {
                        let msg = msg.into_static();
                        if send.send(msg).await.is_err() {
                            error!("Internal Mining downstream not available");

                            // Update Proxy state to reflect Internal inconsistency
                            ProxyState::update_inconsistency(Some(1));
                        }
                    } else {
                        error!("Mining Upstream send non Mining message. Disconnecting");
                        break;
                    }
                } else {
                    error!("Mining Upstream send invalid message no header. Disconnecting");
                    break;
                }
            } else {
                error!("Mining Upstream down.");
                break;
            }
        }
        error!("Failed to receive msg from Pool");
        ProxyState::update_pool_state(PoolState::Down);
    });
    task.into()
}

pub async fn mining_setup_connection(
    recv: &mut Receiver<EitherFrame>,
    send: &mut Sender<EitherFrame>,
    setup_conection: SetupConnection<'static>,
    timer: std::time::Duration,
) -> Result<SetupConnectionSuccess, Error> {
    let msg = PoolExtMessages::Common(CommonMessages::SetupConnection(setup_conection));
    let std_frame: StdFrame = match msg.try_into() {
        Ok(frame) => frame,
        Err(e) => {
            error!("Failed to convert PoolExtMessages to StdFrame.");
            return Err(Error::RolesSv2Logic(e));
        }
    };
    let either_frame: EitherFrame = std_frame.into();
    if send.send(either_frame).await.is_err() {
        error!("Failed to send Eitherframe");
        return Err(Error::Unrecoverable);
    }
    if let Ok(Some(msg)) = tokio::time::timeout(timer, recv.recv()).await {
        let mut msg: StdFrame = msg.try_into().map_err(Error::FramingSv2)?;
        let header = msg.get_header().ok_or(Error::UnexpectedMessage)?;
        let message_type = header.msg_type();
        let payload = msg.payload();
        let msg: CommonMessages<'_> = match (message_type, payload).try_into() {
            Ok(message) => message,
            Err(e) => {
                error!("Unexpected Message: {e}");
                return Err(Error::UpstreamIncoming(e));
            }
        };
        match msg {
            CommonMessages::SetupConnectionSuccess(s) => Ok(s),
            e => {
                error!("Unexpected Message: {e:?}");
                Err(Error::UnexpectedMessage)
            }
        }
    } else {
        error!("Failed to setup connection: Timeout");
        Err(Error::Timeout)
    }
}

pub fn get_mining_setup_connection_msg(
    work_selection: bool,
    use_random_device_id: bool,
) -> SetupConnection<'static> {
    let endpoint_host = "0.0.0.0".to_string().into_bytes().try_into().expect("Internal error: this operation can not fail because the string 0.0.0.0 can always be converted into Inner");
    let vendor = String::new().try_into().expect("Internal error: this operation can not fail because an empty string can always be converted into Inner");
    let hardware_version = String::new().try_into().expect("Internal error: this operation can not fail because an empty string can always be converted into Inner");
    let firmware = String::new().try_into().expect("Internal error: this operation can not fail because an empty string can always be converted into Inner");
    let flags = match work_selection {
        false => 0b0000_0000_0000_0000_0000_0000_0000_0100,
        true => 0b0000_0000_0000_0000_0000_0000_0000_0110,
    };
    let token = Configuration::token().expect("Checked at initialization");
    let device_id = Configuration::device_id(use_random_device_id);
    let device_id = format!("{}::POOLED::{}", device_id, token)
        .to_string()
        .try_into()
        .expect("Internal error: this operation can not fail because an device_id can always be converted into Inner");
    SetupConnection {
        protocol: Protocol::MiningProtocol,
        min_version: 2,
        max_version: 2,
        flags,
        endpoint_host,
        endpoint_port: 50,
        vendor,
        hardware_version,
        firmware,
        device_id,
    }
}
