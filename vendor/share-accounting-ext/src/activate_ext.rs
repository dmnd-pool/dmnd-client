#[cfg(not(feature = "with_serde"))]
use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2;
#[cfg(feature = "with_serde")]
use binary_sv2::GetSize;
use binary_sv2::{Deserialize, Serialize};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Activate {
    pub request_id: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ActivateSuccess {
    pub request_id: u32,
}

#[cfg(feature = "with_serde")]
impl<'d> GetSize for Activate<'d> {
    fn get_size(&self) -> usize {
        self.request_id.get_size()
    }
}

#[cfg(feature = "with_serde")]
impl<'d> GetSize for ActivateSuccess<'d> {
    fn get_size(&self) -> usize {
        self.request_id.get_size()
    }
}
