#[cfg(not(feature = "with_serde"))]
use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2;
use binary_sv2::{encodable::EncodableField, Deserialize, GetSize, Seq064K, Serialize, U256};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;

use crate::{PHash, Slice};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetWindow<'decoder> {
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub block_hash: U256<'decoder>,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetWindowSuccess<'decoder> {
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub slices: Seq064K<'decoder, Slice>,
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub phashes: Seq064K<'decoder, PHash>,
}
impl<'decoder> From<GetWindowSuccess<'decoder>> for EncodableField<'decoder> {
    fn from(v: GetWindowSuccess<'decoder>) -> Self {
        let fields = vec![
            crate::Slice::seq_into_encodable_field(v.slices),
            PHash::seq_into_encodable_field(v.phashes),
        ];
        Self::Struct(fields)
    }
}
impl GetSize for GetWindowSuccess<'_> {
    fn get_size(&self) -> usize {
        let mut size = 0;
        size += self.slices.get_size();
        size += self.phashes.get_size();
        size
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetWindowBusy {
    pub retry_in_seconds: u64,
}

#[cfg(feature = "with_serde")]
impl<'d> GetSize for GetWindow<'d> {
    fn get_size(&self) -> usize {
        self.block_hash.get_size()
    }
}
#[cfg(feature = "with_serde")]
impl<'d> GetSize for GetWindowSuccess<'d> {
    fn get_size(&self) -> usize {
        self.slices.get_size() + self.phashes.get_size()
    }
}
#[cfg(feature = "with_serde")]
impl GetSize for GetWindowBusy {
    fn get_size(&self) -> usize {
        self.retry_in_seconds.get_size()
    }
}
