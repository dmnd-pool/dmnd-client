use std::fmt::Display;

use sv1_api::utils::HexU32Be;
use tokio::task::AbortHandle;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct AbortOnDrop {
    abort_handle: AbortHandle,
}

impl AbortOnDrop {
    pub fn new<T: Send + 'static>(handle: JoinHandle<T>) -> Self {
        let abort_handle = handle.abort_handle();
        Self { abort_handle }
    }

    pub fn is_finished(&self) -> bool {
        self.abort_handle.is_finished()
    }
}

impl core::ops::Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.abort_handle.abort()
    }
}

impl<T: Send + 'static> From<JoinHandle<T>> for AbortOnDrop {
    fn from(value: JoinHandle<T>) -> Self {
        Self::new(value)
    }
}

/// Select a version rolling mask and min bit count based on the request from the miner.
/// It copy the behavior from SRI translator
pub fn sv1_rolling(configure: &sv1_api::client_to_server::Configure) -> (HexU32Be, HexU32Be) {
    let pool_mask = std::env::var("POOL_MASK")
        .map(|val| u32::from_str_radix(&val, 16).unwrap_or(0x1FFFE000))
        .unwrap_or(0x1FFFE000);
    let miner_mask = configure
        .version_rolling_mask()
        .unwrap_or(HexU32Be(0xFFFFFFFF));
    let negotiated_mask = HexU32Be(miner_mask.0 & pool_mask);
    let min_bit_count = configure
        .version_rolling_min_bit_count()
        .unwrap_or(HexU32Be(0));
    (negotiated_mask, min_bit_count)
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct UserId(pub i64);
impl Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
