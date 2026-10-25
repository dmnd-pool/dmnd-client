use super::Hash256;
use binary_sv2::{
    decodable::FieldMarker,
    encodable::{EncodableField, EncodablePrimitive},
    Error, Fixed, GetMarker, Seq064K, Sv2DataType, B032,
};

use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2;
#[cfg(feature = "with_serde")]
use binary_sv2::GetSize;
use binary_sv2::{Deserialize, Serialize};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[already_sized]
#[repr(C)]
pub struct PHash {
    pub phash: Hash256,
    pub index_start: u32,
}

impl Fixed for PHash {
    const SIZE: usize = 32 + 4;
}

impl PHash {
    pub fn seq_into_encodable_field(v: Seq064K<'_, PHash>) -> EncodableField<'_> {
        let inner = v.into_inner();
        let inner_len = inner.len() as u16;
        let mut as_encodable: Vec<EncodableField> = Vec::with_capacity((inner_len + 2) as usize);
        as_encodable.push(EncodableField::Primitive(EncodablePrimitive::OwnedU8(
            inner_len.to_le_bytes()[0],
        )));
        as_encodable.push(EncodableField::Primitive(EncodablePrimitive::OwnedU8(
            inner_len.to_le_bytes()[1],
        )));
        for element in inner {
            as_encodable.push(element.into());
        }
        EncodableField::Struct(as_encodable)
    }
}

impl GetMarker for PHash {
    fn get_marker() -> FieldMarker {
        let markers = vec![B032::get_marker(), u32::get_marker()];
        FieldMarker::Struct(markers)
    }
}

#[cfg(feature = "with_serde")]
impl GetSize for PHash {
    fn get_size(&self) -> usize {
        self.phash.get_size() + self.index_start.get_size()
    }
}

impl TryInto<FieldMarker> for PHash {
    type Error = ();
    fn try_into(self) -> Result<FieldMarker, Self::Error> {
        Ok(PHash::get_marker())
    }
}
impl<'d> Sv2DataType<'d> for PHash {
    fn from_bytes_unchecked(data: &'d mut [u8]) -> Self {
        let phash_1 = u64::from_bytes_unchecked(&mut data[0..8]);
        let phash_2 = u64::from_bytes_unchecked(&mut data[8..16]);
        let phash_3 = u64::from_bytes_unchecked(&mut data[16..24]);
        let phash_4 = u64::from_bytes_unchecked(&mut data[24..32]);
        let phash = Hash256 {
            first: phash_1,
            second: phash_2,
            third: phash_3,
            forth: phash_4,
        };
        let index_start = u32::from_bytes_unchecked(&mut data[32..]);
        Self { phash, index_start }
    }

    fn from_vec_(_data: Vec<u8>) -> Result<Self, Error> {
        unreachable!()
    }

    fn from_vec_unchecked(_data: Vec<u8>) -> Self {
        unreachable!()
    }

    fn to_slice_unchecked(&'d self, dst: &mut [u8]) {
        debug_assert!(dst.len() == 60);
        let phash: [u8; 32] = self.phash.clone().into();
        let dst = &mut dst[0..32];
        dst.copy_from_slice(&phash);
        self.index_start.to_slice_unchecked(&mut dst[32..]);
    }
}
