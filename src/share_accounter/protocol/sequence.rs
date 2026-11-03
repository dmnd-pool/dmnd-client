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
        let count = data.get(..2).ok_or(Error::OutOfBound)?;
        let count = u16::from_le_bytes([count[0], count[1]]);
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
        if fields.len() != count as usize {
            return Err(Error::DecodableConversionError);
        }
        let values = fields
            .map(|field| T::from_decoded_fields(field.into()))
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(values)
    }
}
