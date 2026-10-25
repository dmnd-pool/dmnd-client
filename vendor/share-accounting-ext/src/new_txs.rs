#[cfg(not(feature = "with_serde"))]
use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2;
#[cfg(feature = "with_serde")]
use binary_sv2::GetSize;
use binary_sv2::{Deserialize, Seq064K, Serialize, B016M};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct NewTxs<'decoder> {
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub transactions: Seq064K<'decoder, B016M<'decoder>>,
}

#[cfg(feature = "with_serde")]
impl<'d> GetSize for NewTxs<'d> {
    fn get_size(&self) -> usize {
        self.transactions.get_size()
    }
}
