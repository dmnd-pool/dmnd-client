use super::{control::ChannelKey, protocol::OpenExtendedMiningChannelSuccess};

#[derive(Clone, Debug)]
pub struct ChannelState {
    pub key: ChannelKey,
    pub target: [u8; 32],
    pub extranonce_prefix: Vec<u8>,
    pub extranonce_size: u16,
}

impl ChannelState {
    pub fn opened(key: ChannelKey, message: &OpenExtendedMiningChannelSuccess) -> Self {
        Self {
            key,
            target: message.target.as_ref().try_into().expect("U256"),
            extranonce_prefix: message.extranonce_prefix.as_ref().to_vec(),
            extranonce_size: message.extranonce_size,
        }
    }
}

#[derive(Default)]
pub struct MiningSession {
    pub channels: std::collections::HashMap<ChannelKey, ChannelState>,
}
