#[cfg(not(feature = "with_serde"))]
use alloc::vec::Vec;
#[cfg(not(feature = "with_serde"))]
use binary_sv2::binary_codec_sv2::{
    self, decodable::DecodableField, decodable::FieldMarker, encodable::EncodableField, Decodable,
    Error,
};
use binary_sv2::GetSize;
use binary_sv2::{Deserialize, Seq064K, Serialize};
#[cfg(not(feature = "with_serde"))]
use core::convert::TryInto;

use crate::Share;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetShares<'decoder> {
    pub request_id: u32,
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub shares: Seq064K<'decoder, u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetSharesSuccess<'decoder> {
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    pub shares: Seq064K<'decoder, Share<'decoder>>,
}
impl<'decoder> From<GetSharesSuccess<'decoder>> for EncodableField<'decoder> {
    fn from(v: GetSharesSuccess<'decoder>) -> Self {
        let mut fields: Vec<EncodableField> = Vec::new();
        let val = v.shares;
        fields.push(crate::Share::seq_into_encodable_field(val));
        Self::Struct(fields)
    }
}
impl GetSize for GetSharesSuccess<'_> {
    fn get_size(&self) -> usize {
        let mut size = 0;
        size += self.shares.get_size();
        size
    }
}

impl<'decoder> Decodable<'decoder> for GetSharesSuccess<'decoder> {
    fn get_structure(data: &[u8]) -> Result<Vec<FieldMarker>, Error> {
        let mut fields = Vec::new();
        let offset = 0;
        let shares: Vec<FieldMarker> =
            Seq064K::<'decoder, Share<'decoder>>::get_structure(&data[offset..])?;
        let shares = shares.try_into()?;
        fields.push(shares);
        Ok(fields)
    }
    fn from_decoded_fields(mut data: Vec<DecodableField<'decoder>>) -> Result<Self, Error> {
        Ok(Self {
            shares: Seq064K::<'decoder, Share<'decoder>>::from_decoded_fields(
                data.pop().ok_or(Error::NoDecodableFieldPassed)?.into(),
            )?,
        })
    }
}
impl GetSharesSuccess<'_> {
    pub fn into_static(self) -> GetSharesSuccess<'static> {
        let mut inner = vec![];
        let shares = self.shares.into_inner();
        for share in shares {
            inner.push(share.into_static());
        }
        let seq: Seq064K<'static, crate::Share<'static>> = inner.into();
        GetSharesSuccess { shares: seq }
    }
}
impl GetSharesSuccess<'_> {
    pub fn as_static(&self) -> GetSharesSuccess<'static> {
        self.clone().into_static()
    }
}

#[cfg(feature = "with_serde")]
impl<'d> GetSize for GetShares<'d> {
    fn get_size(&self) -> usize {
        self.shares.get_size() + self.request_id.get_size()
    }
}
#[cfg(feature = "with_serde")]
impl<'d> GetSize for GetSharesSuccess<'d> {
    fn get_size(&self) -> usize {
        self.shares.get_size()
    }
}
