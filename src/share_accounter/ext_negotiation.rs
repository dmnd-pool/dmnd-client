use crate::{
    proxy_state::{ProxyState, ShareAccounterState},
    share_accounter::errors::Error,
};
use binary_sv2::Seq064K;
use demand_share_accounting_ext::{
    parser::{ExtensionNegotiationMessages, PoolExtMessages},
    RequestExtensions, RequestExtensionsError, RequestExtensionsSuccess,
};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info, warn};

const SHARE_ACCOUNTING_EXTENSION_TYPE: u16 = 32;

pub struct ExtensionNegotiationHandler {
    pub request_id: u16,
    pub requested_extensions: Vec<u16>,
    pub negotiated_extensions: Vec<u16>,
}

impl ExtensionNegotiationHandler {
    pub fn new() -> Self {
        Self {
            request_id: 123,
            requested_extensions: vec![SHARE_ACCOUNTING_EXTENSION_TYPE],
            negotiated_extensions: Vec::new(),
        }
    }

    pub async fn negotiate_extensions(
        &mut self,
        sender: Sender<PoolExtMessages<'static>>,
        receiver: &mut Receiver<PoolExtMessages<'static>>,
        timeout: std::time::Duration,
    ) -> Result<(), Error> {
        info!(
            "Starting extension negotiation for extensions: {:?}",
            self.requested_extensions
        );

        self.send_request_extensions(sender).await?;

        self.handle_response(receiver, timeout).await?;

        info!(
            "Extension negotiation completed. Negotiated: {:?}",
            self.negotiated_extensions
        );
        Ok(())
    }

    async fn send_request_extensions(
        &self,
        sender: Sender<PoolExtMessages<'static>>,
    ) -> Result<(), Error> {
        info!(
            "Sending RequestExtensions for: {:?}",
            self.requested_extensions
        );

        let extensions_seq: Seq064K<u16> = self.requested_extensions.clone().into();

        let request_extensions = RequestExtensions {
            request_id: self.request_id,
            requested_extensions: extensions_seq,
        };

        let request_extensions =
            ExtensionNegotiationMessages::RequestExtensions(request_extensions);
        let pool_ext_msg = PoolExtMessages::ExtensionNegotiationMessages(request_extensions);

        if let Err(e) = sender.send(pool_ext_msg).await {
            error!("{e:?}");
            ProxyState::update_share_accounter_state(ShareAccounterState::Down);
        }

        Ok(())
    }

    async fn handle_response(
        &mut self,
        receiver: &mut Receiver<PoolExtMessages<'static>>,
        timeout: std::time::Duration,
    ) -> Result<(), Error> {
        info!("Waiting for extension negotiation response...");

        let response = tokio::time::timeout(timeout, receiver.recv())
            .await
            .map_err(|_| Error::Timeout)?
            .ok_or(Error::Timeout)?;

        match response {
            PoolExtMessages::ExtensionNegotiationMessages(
                ExtensionNegotiationMessages::RequestExtensionsSuccess(success),
            ) => {
                self.handle_success(success)?;
            }
            PoolExtMessages::ExtensionNegotiationMessages(
                ExtensionNegotiationMessages::RequestExtensionsError(error),
            ) => {
                self.handle_error(error)?;
                return Err(Error::NegotationFailed);
            }
            _ => {
                self.handle_success(RequestExtensionsSuccess {
                    request_id: 123,
                    supported_extensions: vec![32].into(),
                })?;
            }
        }

        Ok(())
    }

    fn handle_success(&mut self, success: RequestExtensionsSuccess<'_>) -> Result<(), Error> {
        if success.request_id != self.request_id {
            return Err(Error::RequestIdMismatch);
        }

        self.negotiated_extensions = success.supported_extensions.into_inner().to_vec();

        info!(
            "Extension negotiation successful! Supported: {:?}",
            self.negotiated_extensions
        );

        // Chdck if share accounting was accepted
        if !self.is_share_accounting_enabled() {
            warn!("Share accounting extension was not accepted by the pool");
        }

        Ok(())
    }

    fn handle_error(&mut self, error: RequestExtensionsError<'_>) -> Result<(), Error> {
        // Verify request ID
        if error.request_id != self.request_id {
            return Err(Error::RequestIdMismatch);
        }

        let unsupported: Vec<u16> = error.unsupported_extensions.into_inner().to_vec();
        error!(
            "Extension negotiation failed. Unsupported: {:?}",
            unsupported
        );

        Err(Error::NegotationFailed)
    }

    pub fn is_extension_supported(&self, extension_type: u16) -> bool {
        self.negotiated_extensions.contains(&extension_type)
    }

    pub fn is_share_accounting_enabled(&self) -> bool {
        self.is_extension_supported(SHARE_ACCOUNTING_EXTENSION_TYPE)
    }
}

pub async fn negotiate_extension_after_connection(
    sender: Sender<PoolExtMessages<'static>>,
    receiver: &mut Receiver<PoolExtMessages<'static>>,
    timeout: std::time::Duration,
) -> Result<ExtensionNegotiationHandler, Error> {
    let mut handler = ExtensionNegotiationHandler::new();
    handler
        .negotiate_extensions(sender, receiver, timeout)
        .await?;
    Ok(handler)
}
