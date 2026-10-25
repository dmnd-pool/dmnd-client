#[cfg(not(feature = "with_serde"))]
use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2;
#[cfg(feature = "with_serde")]
use binary_sv2::GetSize;
use binary_sv2::{Deserialize, Serialize, Str0255};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ErrorMessage<'decoder> {
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub message: Str0255<'decoder>,
}

#[cfg(feature = "with_serde")]
impl<'d> GetSize for ErrorMessage<'d> {
    fn get_size(&self) -> usize {
        self.message.get_size()
    }
}
