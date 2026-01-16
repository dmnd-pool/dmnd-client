use std::fmt::Display;

use sv1_api::utils::HexU32Be;
use tokio::task::AbortHandle;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct AbortOnDrop {
    abort_handle: Vec<AbortHandle>,
}

impl AbortOnDrop {
    pub fn new<T: Send + 'static>(handle: JoinHandle<T>) -> Self {
        let abort_handle = vec![handle.abort_handle()];
        Self { abort_handle }
    }

    pub fn is_finished(&self) -> bool {
        for task in &self.abort_handle {
            if !task.is_finished() {
                return false;
            }
        }
        true
    }

    pub fn add_task<T: Send + 'static>(&mut self, handle: JoinHandle<T>) {
        self.abort_handle.push(handle.abort_handle());
    }
}

impl core::ops::Drop for AbortOnDrop {
    fn drop(&mut self) {
        for task in &self.abort_handle {
            task.abort();
        }
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

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct UserId(pub i64);

impl Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
