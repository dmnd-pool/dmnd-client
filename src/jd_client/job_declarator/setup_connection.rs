use crate::config::Configuration;
use demand_sv2_connection::{StandardEitherFrame, StandardSv2Frame};
use rand::distributions::{Alphanumeric, DistString};
use roles_logic_sv2::{
    common_messages_sv2::{Protocol, SetupConnectionOwned},
    handlers::common::{ParseUpstreamCommonMessages, SendTo},
    parsers::PoolMessagesOwned,
    routing_logic::{CommonRoutingLogic, NoRouting},
    utils::Mutex,
};
use std::{convert::TryInto, net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::{Receiver as TReceiver, Sender as TSender};
use tracing::{error, warn};

pub type Message = PoolMessagesOwned;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;
#[derive(Default)]
pub struct SetupConnectionHandler {
    setup_succeeded: bool,
}

impl SetupConnectionHandler {
    fn get_setup_connection_message(proxy_address: SocketAddr) -> SetupConnectionOwned {
        let endpoint_host = proxy_address
            .ip()
            .to_string()
            .into_bytes()
            .try_into()
            .expect("Internal error: this operation can not fail because IP addr string can always be converted into Inner");
        let vendor = String::new().try_into().expect("Internal error: this operation can not fail because empty string can always be converted into Inner");
        let hardware_version = String::new().try_into().expect("Internal error: this operation can not fail because empty string can always be converted into Inner");
        let firmware = String::new().try_into().expect("Internal error: this operation can not fail because empty string can always be converted into Inner");
        let token = Configuration::token().expect("Checked at initialization");
        let device_id = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let device_id = format!("{device_id}::POOLED::{token}")
            .to_string()
            .try_into()
            .expect("Internal error: this operation can not fail because device_id string can always be converted into Inner");
        SetupConnectionOwned {
            protocol: Protocol::JobDeclarationProtocol,
            min_version: crate::jd_client::protocol::VERSION,
            max_version: crate::jd_client::protocol::VERSION,
            flags: 1,
            endpoint_host,
            endpoint_port: proxy_address.port(),
            vendor,
            hardware_version,
            firmware,
            device_id,
        }
    }

    pub async fn setup(
        receiver: &mut TReceiver<EitherFrame>,
        sender: &mut TSender<EitherFrame>,
        proxy_address: SocketAddr,
    ) -> Result<(), crate::jd_client::error::Error> {
        let setup_connection = Self::get_setup_connection_message(proxy_address);

        let sv2_frame: StdFrame = PoolMessagesOwned::Common(setup_connection.into()).try_into()?;
        let sv2_frame = sv2_frame.into();

        sender
            .send(sv2_frame)
            .await
            .map_err(|_| crate::jd_client::error::Error::Unrecoverable)?;

        let mut incoming: StdFrame = match receiver.recv().await {
            Some(msg) => msg.try_into()?,
            None => {
                error!("Failed to receive msg from pool");
                return Err(crate::jd_client::error::Error::Unrecoverable); // Better Error to return?
            }
        };

        let message_type = crate::jd_client::protocol::message_type(incoming.get_header(), |id| {
            roles_logic_sv2::parsers::CommonMessageTypes::try_from(id)
                .map(roles_logic_sv2::parsers::CommonMessageTypes::channel_bit)
        })?;
        let payload = incoming.payload();
        let handler = Arc::new(Mutex::new(SetupConnectionHandler::default()));
        ParseUpstreamCommonMessages::handle_message_common(
            handler.clone(),
            message_type,
            payload,
            CommonRoutingLogic::None,
        )?;
        let setup_succeeded = handler
            .safe_lock(|state| state.setup_succeeded)
            .map_err(|_| crate::jd_client::error::Error::JobDeclaratorMutexCorrupted)?;
        if setup_succeeded {
            Ok(())
        } else {
            Err(crate::jd_client::error::Error::Unrecoverable)
        }
    }
}

impl ParseUpstreamCommonMessages<NoRouting> for SetupConnectionHandler {
    fn handle_setup_connection_success(
        &mut self,
        message: roles_logic_sv2::common_messages_sv2::SetupConnectionSuccess,
    ) -> Result<roles_logic_sv2::handlers::common::SendTo, roles_logic_sv2::errors::Error> {
        crate::jd_client::protocol::validate_version(message.used_version)?;
        self.setup_succeeded = true;
        Ok(SendTo::None(None))
    }

    fn handle_setup_connection_error(
        &mut self,
        message: roles_logic_sv2::common_messages_sv2::SetupConnectionError,
    ) -> Result<roles_logic_sv2::handlers::common::SendTo, roles_logic_sv2::errors::Error> {
        warn!(?message, "Job Declaration setup was rejected");
        Ok(SendTo::None(None))
    }

    fn handle_channel_endpoint_changed(
        &mut self,
        message: roles_logic_sv2::common_messages_sv2::ChannelEndpointChanged,
    ) -> Result<roles_logic_sv2::handlers::common::SendTo, roles_logic_sv2::errors::Error> {
        warn!(
            ?message,
            "Unexpected channel endpoint change during Job Declaration setup"
        );
        Ok(SendTo::None(None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_rejects_unsupported_version() {
        for used_version in [0, 1, 3, u16::MAX] {
            let mut handler = SetupConnectionHandler::default();
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
    fn setup_success_with_zero_flags_is_accepted() {
        let mut handler = SetupConnectionHandler::default();
        handler
            .handle_setup_connection_success(
                roles_logic_sv2::common_messages_sv2::SetupConnectionSuccess {
                    used_version: 2,
                    flags: 0,
                },
            )
            .expect("setup success");

        assert!(handler.setup_succeeded);
    }
}
