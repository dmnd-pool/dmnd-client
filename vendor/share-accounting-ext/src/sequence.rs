use alloc::vec::Vec;
use binary_sv2::{
    decodable::{DecodableField, FieldMarker},
    encodable::EncodableField,
    Deserialize, Error, GetSize, SizeHint,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceOwned<T>(Vec<T>);

impl<T> SequenceOwned<T> {
    pub fn new(values: Vec<T>) -> Result<Self, Error> {
        if values.len() > u16::MAX as usize {
            return Err(Error::OutOfBound);
        }
        Ok(Self(values))
    }
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}
impl<T> TryFrom<Vec<T>> for SequenceOwned<T> {
    type Error = Error;
    fn try_from(values: Vec<T>) -> Result<Self, Error> {
        Self::new(values)
    }
}
impl<T: GetSize> GetSize for SequenceOwned<T> {
    fn get_size(&self) -> usize {
        2 + self.0.iter().map(GetSize::get_size).sum::<usize>()
    }
}
impl<'a, T: Into<EncodableField<'a>>> From<SequenceOwned<T>> for EncodableField<'a> {
    fn from(value: SequenceOwned<T>) -> Self {
        let mut fields = Vec::with_capacity(value.0.len() + 1);
        fields.push((value.0.len() as u16).into());
        fields.extend(value.0.into_iter().map(Into::into));
        Self::Struct(fields)
    }
}
impl<'a, T: Deserialize<'a>> Deserialize<'a> for SequenceOwned<T> {
    fn get_structure(data: &[u8]) -> Result<Vec<FieldMarker>, Error> {
        let count = u16::from_le_bytes(data.get(..2).ok_or(Error::OutOfBound)?.try_into().unwrap());
        let mut fields = u16::get_structure(data)?;
        let mut offset = 2;
        for _ in 0..count {
            let element = FieldMarker::Struct(T::get_structure(
                data.get(offset..).ok_or(Error::OutOfBound)?,
            )?);
            offset += element.size_hint_(data, offset)?;
            if offset > data.len() {
                return Err(Error::OutOfBound);
            }
            fields.push(element);
        }
        Ok(fields)
    }
    fn from_decoded_fields(data: Vec<DecodableField<'a>>) -> Result<Self, Error> {
        let mut fields = data.into_iter();
        let count =
            u16::from_decoded_fields(vec![fields.next().ok_or(Error::NoDecodableFieldPassed)?])?;
        let mut values = Vec::with_capacity(count as usize);
        for field in fields {
            values.push(T::from_decoded_fields(field.into())?);
        }
        if values.len() != count as usize {
            return Err(Error::DecodableConversionError);
        }
        Self::new(values)
    }
}
