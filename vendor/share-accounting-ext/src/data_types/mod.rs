use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2;
#[cfg(feature = "with_serde")]
use binary_sv2::GetSize;
use binary_sv2::{Deserialize, Serialize};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;
mod phash;
mod share;
mod slice;

pub use phash::PHash;
pub use share::Share;
pub use slice::Slice;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[already_sized]
#[repr(C)]
pub struct Hash256 {
    first: u64,
    second: u64,
    third: u64,
    forth: u64,
}

impl From<[u8; 32]> for Hash256 {
    fn from(data: [u8; 32]) -> Self {
        Self {
            first: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            second: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            third: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            forth: u64::from_le_bytes(data[24..32].try_into().unwrap()),
        }
    }
}

impl From<Hash256> for [u8; 32] {
    fn from(data: Hash256) -> Self {
        let first = data.first.to_le_bytes();
        let second = data.second.to_le_bytes();
        let third = data.third.to_le_bytes();
        let forth = data.forth.to_le_bytes();
        [
            first[0], first[1], first[2], first[3], first[4], first[5], first[6], first[7],
            second[0], second[1], second[2], second[3], second[4], second[5], second[6], second[7],
            third[0], third[1], third[2], third[3], third[4], third[5], third[6], third[7],
            forth[0], forth[1], forth[2], forth[3], forth[4], forth[5], forth[6], forth[7],
        ]
    }
}

#[cfg(feature = "with_serde")]
impl GetSize for Hash256 {
    fn get_size(&self) -> usize {
        32
    }
}
