use roles_logic_sv2::mining_sv2::SubmitSharesExtended;
use roles_logic_sv2::mining_sv2::Target;
use sv1_api::{client_to_server::Submit, utils::HexU32Be};
use tokio::sync::oneshot;

use crate::monitor::shares::RejectionReason;
pub mod diff_management;
#[allow(clippy::module_inception)]
pub mod downstream;
pub use downstream::Downstream;
mod accept_connection;
mod notify;
mod receive_from_downstream;
mod send_to_downstream;
mod task_manager;

/// This constant is used as a check to ensure clients
/// do not send a mining.subscribe and never a mining.authorize
/// since they will take up a tcp connection but never be allowed to
/// receive jobs. Without the timeout the TProxy can be exploited by incoming
/// `mining.subscribe` messages that init connections and take up compute
const SUBSCRIBE_TIMEOUT_SECS: u64 = 10;

/// enum of messages sent to the Bridge
#[derive(Debug)]
pub enum DownstreamMessages {
    SubmitShares(SubmitShareWithChannelId),
    SetDownstreamTarget(SetDownstreamTarget),
}

pub type SubmitShareResult = Result<(), RejectionReason>;
pub type SubmitShareResultReceiver = oneshot::Receiver<SubmitShareResult>;
pub type SubmitShareResultSender = oneshot::Sender<SubmitShareResult>;

/// wrapper around a `mining.submit` with extra channel informationfor the Bridge to
/// process
#[derive(Debug)]
pub struct SubmitShareWithChannelId {
    pub channel_id: u32,
    pub share: Submit<'static>,
    // TODO why we need allow dead code here???
    #[allow(dead_code)]
    extranonce: Vec<u8>,
    #[allow(dead_code)]
    extranonce2_len: usize,
    pub version_rolling_mask: Option<HexU32Be>,
    pub result_tx: SubmitShareResultSender,
}

#[derive(Debug)]
pub struct UpstreamSubmitShare {
    pub share: SubmitSharesExtended<'static>,
    pub result_tx: SubmitShareResultSender,
}

/// message for notifying the bridge that a downstream target has updated
/// so the Bridge can process the update
#[derive(Debug, Clone)]
pub struct SetDownstreamTarget {
    pub channel_id: u32,
    pub new_target: Target,
}

pub fn new_subscription_id() -> String {
    "ae6812eb4cd7735a302a8a9dd95cf71f".into()
}
