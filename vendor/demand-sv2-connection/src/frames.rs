use binary_sv2::{GetSize, Serialize};
use codec_sv2::{NoiseDecoder, SerializedFrame};
use framing_sv2::{
    framing::{EncodableFrame, HandshakeMessage, MessageFrame, SizeHint},
    header::Header,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) type DecoderOwner = Arc<Mutex<NoiseDecoder>>;

#[derive(Debug)]
pub enum StandardSv2Frame<T> {
    Message(MessageFrame<T>),
    Serialized {
        frame: SerializedFrame,
        // Drop the frame before its pool. This also permits task cancellation.
        _owner: Option<DecoderOwner>,
    },
}

impl<T: Serialize + GetSize> StandardSv2Frame<T> {
    pub fn from_message(
        message: T,
        message_type: u8,
        extension_type: u16,
        channel_msg: bool,
    ) -> Result<Self, framing_sv2::Error> {
        MessageFrame::from_message(message, message_type, extension_type, channel_msg)
            .map(Self::Message)
    }

    pub fn get_header(&self) -> Option<Header> {
        Some(match self {
            Self::Message(frame) => frame.header(),
            Self::Serialized { frame, .. } => frame.header(),
        })
    }

    pub fn encoded_length(&self) -> usize {
        match self {
            Self::Message(frame) => frame.encoded_length(),
            Self::Serialized { frame, .. } => frame.encoded_length(),
        }
    }

    pub fn serialize(self, destination: &mut [u8]) -> Result<(), framing_sv2::Error> {
        self.encode_into(destination)
    }

    pub fn payload(&mut self) -> &mut [u8] {
        match self {
            Self::Serialized { frame, .. } => frame.payload(),
            Self::Message(_) => panic!("Serialize an outgoing frame before reading its payload"),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, SizeHint> {
        SerializedFrame::from_bytes(bytes.into()).map(|frame| Self::Serialized {
            frame,
            _owner: None,
        })
    }

    pub(crate) fn received(frame: SerializedFrame, owner: DecoderOwner) -> Self {
        Self::Serialized {
            frame,
            _owner: Some(owner),
        }
    }
}

impl<T: Serialize + GetSize> GetSize for StandardSv2Frame<T> {
    fn get_size(&self) -> usize {
        self.encoded_length()
    }
}

impl<T: Serialize + GetSize> EncodableFrame for StandardSv2Frame<T> {
    fn encoded_length(&self) -> usize {
        Self::encoded_length(self)
    }

    fn encode_into(self, destination: &mut [u8]) -> Result<(), framing_sv2::Error> {
        match self {
            Self::Message(frame) => frame.encode_into(destination),
            Self::Serialized { frame, _owner } => {
                let result = frame.encode_into(destination);
                drop(_owner);
                result
            }
        }
    }
}

impl<T> From<MessageFrame<T>> for StandardSv2Frame<T> {
    fn from(frame: MessageFrame<T>) -> Self {
        Self::Message(frame)
    }
}

#[derive(Debug)]
pub enum StandardEitherFrame<T> {
    Sv2(StandardSv2Frame<T>),
    HandShake(HandshakeMessage),
}

impl<T: Serialize + GetSize> StandardEitherFrame<T> {
    pub fn encoded_length(&self) -> usize {
        match self {
            Self::Sv2(frame) => frame.encoded_length(),
            Self::HandShake(message) => message.payload().len(),
        }
    }
}

impl<T> From<StandardSv2Frame<T>> for StandardEitherFrame<T> {
    fn from(frame: StandardSv2Frame<T>) -> Self {
        Self::Sv2(frame)
    }
}

impl<T> From<HandshakeMessage> for StandardEitherFrame<T> {
    fn from(frame: HandshakeMessage) -> Self {
        Self::HandShake(frame)
    }
}

impl<T> TryFrom<StandardEitherFrame<T>> for StandardSv2Frame<T> {
    type Error = framing_sv2::Error;

    fn try_from(frame: StandardEitherFrame<T>) -> Result<Self, Self::Error> {
        match frame {
            StandardEitherFrame::Sv2(frame) => Ok(frame),
            StandardEitherFrame::HandShake(frame) => Err(
                framing_sv2::Error::UnexpectedHeaderLength(frame.payload().len()),
            ),
        }
    }
}

macro_rules! message_frames {
    ($($borrowed:ident => $owned:ident),+ $(,)?) => {$(
        impl<'a> TryFrom<roles_logic_sv2::parsers::$borrowed<'a>>
            for StandardSv2Frame<roles_logic_sv2::parsers::$borrowed<'a>>
        {
            type Error = roles_logic_sv2::Error;

            fn try_from(message: roles_logic_sv2::parsers::$borrowed<'a>)
                -> Result<Self, Self::Error>
            {
                MessageFrame::try_from(message).map(Self::Message)
            }
        }

        impl TryFrom<roles_logic_sv2::parsers::$owned>
            for StandardSv2Frame<roles_logic_sv2::parsers::$owned>
        {
            type Error = roles_logic_sv2::Error;

            fn try_from(message: roles_logic_sv2::parsers::$owned)
                -> Result<Self, Self::Error>
            {
                MessageFrame::try_from(message).map(Self::Message)
            }
        }
    )+};
}

message_frames! {
    CommonMessages => CommonMessagesOwned,
    Mining => MiningOwned,
    JobDeclaration => JobDeclarationOwned,
    TemplateDistribution => TemplateDistributionOwned,
    PoolMessages => PoolMessagesOwned,
    MiningDeviceMessages => MiningDeviceMessagesOwned,
}

#[cfg(test)]
mod tests {
    use super::*;
    use roles_logic_sv2::mining_sv2::SubmitSharesStandardOwned;

    fn frame() -> StandardSv2Frame<SubmitSharesStandardOwned> {
        StandardSv2Frame::from_message(
            SubmitSharesStandardOwned {
                channel_id: 1,
                sequence_number: 2,
                job_id: 3,
                nonce: 4,
                ntime: 5,
                version: 6,
            },
            0x42,
            0,
            true,
        )
        .unwrap()
    }

    #[test]
    fn frame_bytes_preserve_header_and_payload() {
        let frame = frame();
        let mut bytes = [0; 30];
        frame.serialize(&mut bytes).unwrap();
        assert_eq!(
            bytes,
            [
                0, 128, 66, 24, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0,
                6, 0, 0, 0
            ]
        );
        let mut received =
            StandardSv2Frame::<SubmitSharesStandardOwned>::from_bytes(bytes.to_vec()).unwrap();
        assert_eq!(received.payload(), &bytes[6..]);
        assert_eq!(received.get_header().unwrap().msg_type(), 0x42);
        assert!(received.get_header().unwrap().channel_msg());
        let mut forwarded = [0; 30];
        received.serialize(&mut forwarded).unwrap();
        assert_eq!(forwarded, bytes);
    }

    #[test]
    fn frame_size_queries_do_not_allocate() {
        let frame = frame();
        let allocations = crate::test_support::allocations(|| {
            for _ in 0..100 {
                assert_eq!(std::hint::black_box(&frame).get_size(), 30);
                assert_eq!(std::hint::black_box(&frame).encoded_length(), 30);
            }
        });
        assert_eq!(allocations, 0);
        let either: StandardEitherFrame<_> = frame.into();
        assert_eq!(
            crate::test_support::allocations(|| {
                assert_eq!(std::hint::black_box(&either).encoded_length(), 30);
            }),
            0
        );
    }

    #[test]
    fn frame_rejects_short_and_surplus_bytes() {
        assert!(matches!(
            StandardSv2Frame::<SubmitSharesStandardOwned>::from_bytes(vec![0; 5]),
            Err(SizeHint::Missing(1))
        ));
        let mut bytes = vec![0; 30];
        frame().serialize(&mut bytes).unwrap();
        bytes.push(0);
        assert!(matches!(
            StandardSv2Frame::<SubmitSharesStandardOwned>::from_bytes(bytes),
            Err(SizeHint::Surplus(1))
        ));
        assert!(frame().serialize(&mut [0; 31]).is_err());
    }
}
