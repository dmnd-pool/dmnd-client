use binary_sv2::{
    decodable::FieldMarker,
    encodable::{EncodableField, EncodablePrimitive},
    Error, Fixed, GetMarker, Seq064K, Sv2DataType,
};

use super::Hash256;
use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2;
#[cfg(feature = "with_serde")]
use binary_sv2::GetSize;
use binary_sv2::{Deserialize, Serialize, U256};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[already_sized]
#[repr(C)]
pub struct Slice {
    // How many shares are in the slice
    pub number_of_shares: u32,
    // Below 2 values ara cached for performance
    // sum of all the difficulties of the shares that compose the slice
    pub difficulty: u64,
    // fees of the ref job for this slice
    pub fees: u64,
    // merkle root of the tree composed by all the shares in the slice
    pub root: Hash256,
    // id of the ref job for this slice
    pub job_id: u64,
}

impl<'a> Slice {
    pub fn seq_into_encodable_field(v: Seq064K<'a, Slice>) -> EncodableField<'a> {
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

impl TryInto<FieldMarker> for Slice {
    type Error = ();
    fn try_into(self) -> Result<FieldMarker, Self::Error> {
        Ok(Slice::get_marker())
    }
}

impl Fixed for Slice {
    const SIZE: usize = 60;
}

impl GetMarker for Slice {
    fn get_marker() -> FieldMarker {
        let markers = vec![
            u32::get_marker(),
            u64::get_marker(),
            u64::get_marker(),
            U256::get_marker(),
            u64::get_marker(),
        ];
        FieldMarker::Struct(markers)
    }
}

impl<'d> Sv2DataType<'d> for Slice {
    fn from_bytes_unchecked(data: &'d mut [u8]) -> Self {
        debug_assert!(data.len() == 60);
        let number_of_shares = u32::from_bytes_unchecked(&mut data[0..4]);
        let difficulty = u64::from_bytes_unchecked(&mut data[4..12]);
        let fees = u64::from_bytes_unchecked(&mut data[12..20]);
        let root_1 = u64::from_bytes_unchecked(&mut data[20..28]);
        let root_2 = u64::from_bytes_unchecked(&mut data[28..36]);
        let root_3 = u64::from_bytes_unchecked(&mut data[36..44]);
        let root_4 = u64::from_bytes_unchecked(&mut data[44..52]);
        let root = Hash256 {
            first: root_1,
            second: root_2,
            third: root_3,
            forth: root_4,
        };
        let job_id = u64::from_bytes_unchecked(&mut data[52..60]);
        Self {
            number_of_shares,
            difficulty,
            fees,
            root,
            job_id,
        }
    }

    fn from_vec_(mut data: Vec<u8>) -> Result<Self, Error> {
        Self::from_bytes_(&mut data)
    }

    fn from_vec_unchecked(mut data: Vec<u8>) -> Self {
        Self::from_bytes_unchecked(&mut data)
    }

    fn to_slice_unchecked(&'d self, dst: &mut [u8]) {
        debug_assert!(dst.len() == 60);
        self.number_of_shares.to_slice_unchecked(&mut dst[0..4]);
        self.difficulty.to_slice_unchecked(&mut dst[4..12]);
        self.fees.to_slice_unchecked(&mut dst[12..20]);
        self.root.first.to_slice_unchecked(&mut dst[20..52]);
        self.root.second.to_slice_unchecked(&mut dst[20..52]);
        self.root.third.to_slice_unchecked(&mut dst[20..52]);
        self.root.forth.to_slice_unchecked(&mut dst[20..52]);
        self.job_id.to_slice_unchecked(&mut dst[52..60]);
    }
}

#[cfg(feature = "with_serde")]
impl GetSize for Slice {
    fn get_size(&self) -> usize {
        60
    }
}
