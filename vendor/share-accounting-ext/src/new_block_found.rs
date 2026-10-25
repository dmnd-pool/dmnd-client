#[cfg(not(feature = "with_serde"))]
use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2;
#[cfg(feature = "with_serde")]
use binary_sv2::GetSize;
use binary_sv2::{Deserialize, Serialize, U256};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct NewBlockFound<'decoder> {
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub block_hash: U256<'decoder>,
}

#[cfg(feature = "with_serde")]
impl<'d> GetSize for NewBlockFound<'d> {
    fn get_size(&self) -> usize {
        self.block_hash.get_size()
    }
}
