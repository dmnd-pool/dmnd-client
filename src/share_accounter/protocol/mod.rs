mod messages;
pub mod parser;
mod sequence;

pub use binary_sv2::U256Owned as Hash256;
pub use messages::*;
pub use sequence::SequenceOwned;

pub const EXTENSION_TYPE: u16 = 32;
pub const MESSAGE_TYPE_SHARE_OK: u8 = 0x02;

#[cfg(test)]
mod tests;
