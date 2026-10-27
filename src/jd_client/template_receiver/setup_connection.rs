use crate::jd_client::error::Error;
use demand_sv2_connection::{StandardEitherFrame, StandardSv2Frame};
use roles_logic_sv2::{
    common_messages_sv2::{Protocol, SetupConnectionOwned},
    handlers::common::{ParseUpstreamCommonMessages, SendTo},
    parsers::PoolMessagesOwned,
    routing_logic::{CommonRoutingLogic, NoRouting},
    utils::Mutex,
};
use std::{convert::TryInto, net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::{Receiver as TReceiver, Sender as TSender};
use tracing::error;
pub type Message = PoolMessagesOwned;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;
pub struct SetupConnectionHandler {}

impl SetupConnectionHandler {
    fn get_setup_connection_message(address: SocketAddr) -> SetupConnectionOwned {
        let endpoint_host = address.ip().to_string().into_bytes().try_into().expect("Internal error: this operation can not fail because IP address can always be converted into Inner");
        let vendor = String::new().try_into().expect("Internal error: this operation can not fail empty String can always be converted into Inner");
        let hardware_version = String::new().try_into().expect("Internal error: this operation can not fail empty String can always be converted into Inner");
        let firmware = String::new().try_into().expect("Internal error: this operation can not fail empty String can always be converted into Inner");
        let device_id = String::new().try_into().expect("Internal error: this operation can not fail empty String can always be converted into Inner");
        SetupConnectionOwned {
            protocol: Protocol::TemplateDistributionProtocol,
            min_version: crate::jd_client::protocol::VERSION,
            max_version: crate::jd_client::protocol::VERSION,
            flags: 0b0000_0000_0000_0000_0000_0000_0000_0000,
            endpoint_host,
            endpoint_port: address.port(),
            vendor,
            hardware_version,
            firmware,
            device_id,
        }
    }

    pub async fn setup(
        receiver: &mut TReceiver<EitherFrame>,
        sender: &mut TSender<EitherFrame>,
        address: SocketAddr,
    ) -> Result<(), Error> {
        let setup_connection = Self::get_setup_connection_message(address);

        let sv2_frame: StdFrame = PoolMessagesOwned::Common(setup_connection.into())
            .try_into()
            .expect("Internal error: this operation can not fail because PoolMessage::Common can always be converted into StdFrame");
        let sv2_frame = sv2_frame.into();
        sender
            .send(sv2_frame)
            .await
            .map_err(|_| Error::Unrecoverable)?;

        let mut incoming: StdFrame = match receiver.recv().await {
            Some(msg) => msg.try_into()?,
            None => {
                error!("Failed to parse incoming SetupConnectionResponse");
                return Err(Error::Unrecoverable);
            }
        };

        let message_type = crate::jd_client::protocol::message_type(incoming.get_header(), |id| {
            roles_logic_sv2::parsers::CommonMessageTypes::try_from(id)
                .map(roles_logic_sv2::parsers::CommonMessageTypes::channel_bit)
        })?;
        let payload = incoming.payload();
        ParseUpstreamCommonMessages::handle_message_common(
            Arc::new(Mutex::new(SetupConnectionHandler {})),
            message_type,
            payload,
            CommonRoutingLogic::None,
        )
        .map_err(Error::RolesSv2Logic)?;
        Ok(())
    }
}

impl ParseUpstreamCommonMessages<NoRouting> for SetupConnectionHandler {
    fn handle_setup_connection_success(
        &mut self,
        message: roles_logic_sv2::common_messages_sv2::SetupConnectionSuccess,
    ) -> Result<roles_logic_sv2::handlers::common::SendTo, roles_logic_sv2::errors::Error> {
        crate::jd_client::protocol::validate_version(message.used_version)?;
        Ok(SendTo::None(None))
    }

    fn handle_setup_connection_error(
        &mut self,
        _: roles_logic_sv2::common_messages_sv2::SetupConnectionError,
    ) -> Result<roles_logic_sv2::handlers::common::SendTo, roles_logic_sv2::errors::Error> {
        Err(roles_logic_sv2::Error::UnexpectedMessage(
            roles_logic_sv2::common_messages_sv2::MESSAGE_TYPE_SETUP_CONNECTION_ERROR,
        ))
    }

    fn handle_channel_endpoint_changed(
        &mut self,
        _: roles_logic_sv2::common_messages_sv2::ChannelEndpointChanged,
    ) -> Result<roles_logic_sv2::handlers::common::SendTo, roles_logic_sv2::errors::Error> {
        Err(roles_logic_sv2::Error::UnexpectedMessage(
            roles_logic_sv2::common_messages_sv2::MESSAGE_TYPE_CHANNEL_ENDPOINT_CHANGED,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_rejects_unsupported_version() {
        for used_version in [0, 1, 3, u16::MAX] {
            let mut handler = SetupConnectionHandler {};
            assert!(handler
                .handle_setup_connection_success(
                    roles_logic_sv2::common_messages_sv2::SetupConnectionSuccess {
                        used_version,
                        flags: 0,
                    },
                )
                .is_err());
        }
    }

    #[test]
    fn template_setup_rejection_returns_error() {
        let mut handler = SetupConnectionHandler {};
        let error = roles_logic_sv2::common_messages_sv2::SetupConnectionError {
            flags: 0,
            error_code: b"unsupported-feature-flags".as_slice().try_into().unwrap(),
        };
        assert!(handler.handle_setup_connection_error(error).is_err());
    }

    #[test]
    fn template_setup_rejects_channel_change() {
        let mut handler = SetupConnectionHandler {};
        assert!(handler
            .handle_channel_endpoint_changed(
                roles_logic_sv2::common_messages_sv2::ChannelEndpointChanged { channel_id: 1 },
            )
            .is_err());
    }
}
