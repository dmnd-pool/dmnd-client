pub(crate) mod errors;
mod task_manager;

use std::net::SocketAddr;

use demand_share_accounting_ext::parser::PoolExtMessagesOwned;
use demand_sv2_connection::noise_connection_tokio::Connection;
use demand_sv2_connection::Initiator;
use demand_sv2_connection::{HandshakeRole, StandardEitherFrame, StandardSv2Frame};
use errors::Error;
use key_utils::Secp256k1PublicKey;
use rand::distributions::{Alphanumeric, DistString};
use roles_logic_sv2::{
    common_messages_sv2::{Protocol, SetupConnectionOwned, SetupConnectionSuccess},
    parsers::CommonMessagesOwned,
};
use tokio::net::TcpStream;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    watch,
};
use tracing::{error, info};

use crate::{
    config::Configuration, proxy_state::ProxyState, shared::utils::AbortOnDrop, PoolState,
};
use task_manager::TaskManager;

pub type Message = PoolExtMessagesOwned;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;

const DEFAULT_TIMER: std::time::Duration = std::time::Duration::from_secs(5);

pub async fn connect_pool(
    address: SocketAddr,
    authority_public_key: Secp256k1PublicKey,
    setup_connection_msg: Option<SetupConnectionOwned>,
    timer: Option<std::time::Duration>,
    mut shutdown_signal: watch::Receiver<bool>,
) -> Result<
    (
        Sender<PoolExtMessagesOwned>,
        Receiver<PoolExtMessagesOwned>,
        AbortOnDrop,
    ),
    Error,
> {
    let socket = loop {
        tokio::select! {
            res = shutdown_signal.changed() => {
                if res.is_ok() && *shutdown_signal.borrow() {
                    info!("Shutdown received while connecting to Pool {}", address);
                    return Err(Error::Shutdown);
                }
            }
            res = TcpStream::connect(address) => match res {
                Ok(socket) => {
                    info!("Socket initialized with Pool at {}", address);
                    break socket;
                }
                Err(e) => {
                    error!(
                        "Failed to connect to Upstream role at {}, retrying in 5s: {}",
                        address, e
                    );
                    tokio::select! {
                        res = shutdown_signal.changed() => {
                            if res.is_ok() && *shutdown_signal.borrow() {
                                info!("Shutdown received while waiting to reconnect to Pool {}", address);
                                return Err(Error::Shutdown);
                            }
                        }
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                    }
                }
            }
        }
    };

    info!("Performing SV2 Handshake with Pool at {}", address);
    let initiator =
        Initiator::from_raw_k(authority_public_key.into_bytes()).expect("Invalid authority key");
    // Channel to send and receive messages to the SV2 Upstream role
    let (mut receiver, mut sender, _, _) = tokio::select! {
        res = shutdown_signal.changed() => {
            if res.is_ok() && *shutdown_signal.borrow() {
                info!("Shutdown received during Pool handshake");
                return Err(Error::Shutdown);
            }
            return Err(Error::Unrecoverable);
        }
        res = Connection::new(socket, HandshakeRole::Initiator(initiator)) => {
            res.map_err(|e| {
                error!("Failed to create connection");
                Error::SV2Connection(e)
            })?
        }
    };
    info!("SV2 Handshake with Pool at {} completed", address);
    info!("Sending SetupConnection message to Pool at {}", address);
    let setup_connection_msg =
        setup_connection_msg.unwrap_or(get_mining_setup_connection_msg(true));
    match tokio::select! {
        res = shutdown_signal.changed() => {
            if res.is_ok() && *shutdown_signal.borrow() {
                info!("Shutdown received while setting up Pool connection");
                return Err(Error::Shutdown);
            }
            return Err(Error::Unrecoverable);
        }
        res = mining_setup_connection(
            &mut receiver,
            &mut sender,
            setup_connection_msg,
            timer.unwrap_or(DEFAULT_TIMER),
        ) => res
    } {
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
    mut recv: Receiver<PoolExtMessagesOwned>,
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
                error!("Dropping invalid internal mining message; keeping the proxy alive");
            }
        }
    });
    task.into()
}

pub fn relay_down(
    mut recv: Receiver<EitherFrame>,
    send: Sender<PoolExtMessagesOwned>,
) -> AbortOnDrop {
    let task = tokio::spawn(async move {
        while let Some(msg) = recv.recv().await {
            let msg: Result<StdFrame, ()> = msg.try_into().map_err(|_| ());
            if let Ok(mut msg) = msg {
                if let Some(header) = msg.get_header() {
                    let message_type = header.msg_type();
                    let payload = msg.payload();
                    let extension = header.ext_type();
                    let msg: Result<demand_share_accounting_ext::parser::PoolExtMessages<'_>, _> =
                        (extension, message_type, payload).try_into();
                    if let Ok(msg) = msg {
                        let msg = msg.into_owned();
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
    setup_conection: SetupConnectionOwned,
    timer: std::time::Duration,
) -> Result<SetupConnectionSuccess, Error> {
    let supported_versions = setup_conection.min_version..=setup_conection.max_version;
    let msg = PoolExtMessagesOwned::Common(CommonMessagesOwned::SetupConnection(setup_conection));
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
        if header.ext_type() != 0 {
            return Err(Error::UnexpectedMessage);
        }
        let message_type = header.msg_type();
        let payload = msg.payload();
        let msg: roles_logic_sv2::parsers::CommonMessages<'_> =
            match (message_type, payload).try_into() {
                Ok(message) => message,
                Err(e) => {
                    error!("Unexpected Message: {e}");
                    return Err(Error::UpstreamIncoming(e));
                }
            };
        match msg {
            roles_logic_sv2::parsers::CommonMessages::SetupConnectionSuccess(s)
                if supported_versions.contains(&s.used_version) =>
            {
                Ok(s)
            }
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

pub fn get_mining_setup_connection_msg(work_selection: bool) -> SetupConnectionOwned {
    let endpoint_host = "0.0.0.0".to_string().into_bytes().try_into().expect("Internal error: this operation can not fail because the string 0.0.0.0 can always be converted into Inner");
    let vendor = String::new().try_into().expect("Internal error: this operation can not fail because an empty string can always be converted into Inner");
    let hardware_version = String::new().try_into().expect("Internal error: this operation can not fail because an empty string can always be converted into Inner");
    let firmware = String::new().try_into().expect("Internal error: this operation can not fail because an empty string can always be converted into Inner");
    let flags = match work_selection {
        false => 0b0000_0000_0000_0000_0000_0000_0000_0100,
        true => 0b0000_0000_0000_0000_0000_0000_0000_0110,
    };
    let token = Configuration::token().expect("Checked at initialization");
    let device_id = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
    let device_id = format!("{device_id}::POOLED::{token}")
        .to_string()
        .try_into()
        .expect("Internal error: this operation can not fail because an device_id can always be converted into Inner");
    SetupConnectionOwned {
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_reply(
        extension: u16,
        channel_bit: bool,
        used_version: u16,
    ) -> Result<SetupConnectionSuccess, Error> {
        let response = PoolExtMessagesOwned::Common(CommonMessagesOwned::SetupConnectionSuccess(
            SetupConnectionSuccess {
                used_version,
                flags: 0,
            },
        ));
        let frame = StdFrame::from_message(response, 1, extension, channel_bit).unwrap();
        let mut bytes = vec![0; frame.encoded_length()];
        frame.serialize(&mut bytes).unwrap();
        let frame = StdFrame::from_bytes(bytes).unwrap();
        let (incoming, mut receiver) = tokio::sync::mpsc::channel(1);
        incoming.send(frame.into()).await.unwrap();
        let (mut sender, _outgoing) = tokio::sync::mpsc::channel(1);
        let setup = SetupConnectionOwned {
            protocol: Protocol::MiningProtocol,
            min_version: 2,
            max_version: 2,
            flags: 0,
            endpoint_host: Vec::new().try_into().unwrap(),
            endpoint_port: 0,
            vendor: Vec::new().try_into().unwrap(),
            hardware_version: Vec::new().try_into().unwrap(),
            firmware: Vec::new().try_into().unwrap(),
            device_id: Vec::new().try_into().unwrap(),
        };
        mining_setup_connection(&mut receiver, &mut sender, setup, DEFAULT_TIMER).await
    }

    #[tokio::test]
    async fn pool_setup_rejects_unsupported_versions() {
        assert!(setup_reply(0, false, 1).await.is_err());
        assert!(setup_reply(0, false, 3).await.is_err());
    }

    #[tokio::test]
    async fn pool_setup_rejects_invalid_extension_and_channel_bit() {
        assert!(setup_reply(0, false, 2).await.is_ok());
        assert!(setup_reply(1, false, 2).await.is_err());
        assert!(setup_reply(0, true, 2).await.is_err());
    }
}
