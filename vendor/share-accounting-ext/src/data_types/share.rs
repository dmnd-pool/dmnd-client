use binary_sv2::{
    decodable::FieldMarker,
    encodable::{EncodableField, EncodablePrimitive},
    Error, GetMarker, GetSize, Seq064K, Sv2DataType, B032, B064K,
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
pub struct Share<'decoder> {
    pub nonce: u32,
    pub ntime: u32,
    pub version: u32,
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub extranonce: B032<'decoder>,
    pub job_id: u64,
    pub reference_job_id: u64,
    pub share_index: u32,
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub merkle_path: B064K<'decoder>,
}

impl<'a> Share<'a> {
    pub fn seq_into_encodable_field(v: Seq064K<'a, Share<'a>>) -> EncodableField<'a> {
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

impl GetMarker for Share<'_> {
    fn get_marker() -> FieldMarker {
        let markers = vec![
            u32::get_marker(),
            u32::get_marker(),
            u32::get_marker(),
            B032::get_marker(),
            u64::get_marker(),
            u64::get_marker(),
            u32::get_marker(),
            B064K::get_marker(),
        ];
        FieldMarker::Struct(markers)
    }
}

impl GetSize for Share<'_> {
    fn get_size(&self) -> usize {
        self.nonce.get_size()
            + self.ntime.get_size()
            + self.version.get_size()
            + self.extranonce.get_size()
            + self.job_id.get_size()
            + self.reference_job_id.get_size()
            + self.share_index.get_size()
            + self.merkle_path.get_size()
    }
}

impl binary_sv2::SizeHint for Share<'_> {
    // This is not needed
    fn size_hint(_: &[u8], _: usize) -> Result<usize, binary_sv2::Error> {
        todo!()
    }
    fn size_hint_(&self, _: &[u8], _: usize) -> Result<usize, binary_sv2::Error> {
        Ok(self.get_size())
    }
}

impl TryInto<binary_sv2::decodable::FieldMarker> for Share<'_> {
    type Error = ();
    fn try_into(self) -> Result<binary_sv2::decodable::FieldMarker, Self::Error> {
        Ok(Share::get_marker())
    }
}
impl<'d> Sv2DataType<'d> for Share<'d> {
    fn from_bytes_unchecked(data: &'d mut [u8]) -> Self {
        let nonce = u32::from_bytes_unchecked(&mut data[0..4]);
        let ntime = u32::from_bytes_unchecked(&mut data[4..8]);
        let version = u32::from_bytes_unchecked(&mut data[8..12]);
        let extranonce = B032::from_bytes_unchecked(&mut data[12..]);
        let extranonce = extranonce.into_static();
        let index = extranonce.len() + 12;
        let job_id = u64::from_bytes_unchecked(&mut data[index..index + 8]);
        let reference_job_id = u64::from_bytes_unchecked(&mut data[index + 8..index + 16]);
        let share_index = u32::from_bytes_unchecked(&mut data[index + 16..]);
        let merkle_path = B064K::from_bytes_unchecked(&mut data[index + 16..]);
        Self {
            nonce,
            ntime,
            version,
            extranonce,
            job_id,
            reference_job_id,
            share_index,
            merkle_path,
        }
    }

    fn from_vec_(_data: Vec<u8>) -> Result<Self, Error> {
        unreachable!()
    }

    fn from_vec_unchecked(_data: Vec<u8>) -> Self {
        unreachable!()
    }

    fn to_slice_unchecked(&'d self, dst: &mut [u8]) {
        debug_assert!(dst.len() == 60);
        self.nonce.to_slice_unchecked(&mut dst[0..4]);
        self.ntime.to_slice_unchecked(&mut dst[4..12]);
        self.version.to_slice_unchecked(&mut dst[8..12]);
        self.extranonce.to_slice_unchecked(&mut dst[12..]);
        let index = 12 + self.extranonce.get_size();
        self.job_id.to_slice_unchecked(&mut dst[index..index + 8]);
        self.reference_job_id
            .to_slice_unchecked(&mut dst[index + 8..index + 16]);
        self.share_index
            .to_slice_unchecked(&mut dst[index + 16..index + 20]);
        self.merkle_path.to_slice_unchecked(&mut dst[index + 20..]);
    }
}
