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
pub struct ShareOk {
    pub ref_job_id: u64,
    pub share_index: u32,
}

#[cfg(feature = "with_serde")]
impl<'d> GetSize for ShareOk<'d> {
    fn get_size(&self) -> usize {
        self.ref_job_id.get_size() + self.share_index.get_size()
    }
}
