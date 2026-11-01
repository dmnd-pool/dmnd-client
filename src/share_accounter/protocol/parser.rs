use super::*;
use crate::sv2_connection::StandardSv2Frame;
use binary_sv2::{encodable::EncodableField, GetSize};
use roles_logic_sv2::{
    parsers::{self, IsSv2Message},
    Error,
};

macro_rules! accounting_messages {
    ($($variant:ident = $id:tt),+ $(,)?) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub enum ShareAccountingMessages {
            $($variant($variant)),+
        }

        impl GetSize for ShareAccountingMessages {
            fn get_size(&self) -> usize {
                match self { $(Self::$variant(message) => message.get_size()),+ }
            }
        }

        impl IsSv2Message for ShareAccountingMessages {
            fn message_type(&self) -> u8 {
                match self { $(Self::$variant(_) => $id),+ }
            }

            fn channel_bit(&self) -> bool {
                false
            }
        }

        impl<'a> From<ShareAccountingMessages> for EncodableField<'a> {
            fn from(message: ShareAccountingMessages) -> Self {
                match message {
                    $(ShareAccountingMessages::$variant(message) => message.into()),+
                }
            }
        }

        impl TryFrom<(u8, &mut [u8])> for ShareAccountingMessages {
            type Error = Error;

            fn try_from((id, payload): (u8, &mut [u8])) -> Result<Self, Error> {
                let length = payload.len();
                let message = match id {
                    $($id => Self::$variant(binary_sv2::from_bytes(payload)?)),+,
                    _ => return Err(Error::UnexpectedMessage(id)),
                };
                if message.get_size() != length {
                    return Err(Error::BadPayloadSize);
                }
                Ok(message)
            }
        }
    };
}

accounting_messages! {
    ShareOk = MESSAGE_TYPE_SHARE_OK,
    NewBlockFound = 0x03,
    GetWindow = 0x04,
    GetWindowSuccess = 0x05,
    GetWindowBusy = 0x06,
    GetShares = 0x07,
    GetSharesSuccess = 0x08,
    NewTxs = 0x0f,
    ErrorMessage = 0x10,
}

macro_rules! pool_messages {
    ($name:ident $(<$lifetime:lifetime>)? { $($variant:ident($message:ty)),+ $(,)? }) => {
        #[derive(Clone, Debug)]
        pub enum $name $(<$lifetime>)? {
            $($variant($message)),+
        }

        impl $(<$lifetime>)? GetSize for $name $(<$lifetime>)? {
            fn get_size(&self) -> usize {
                match self { $(Self::$variant(message) => message.get_size()),+ }
            }
        }

        impl $(<$lifetime>)? IsSv2Message for $name $(<$lifetime>)? {
            fn message_type(&self) -> u8 {
                match self { $(Self::$variant(message) => message.message_type()),+ }
            }

            fn channel_bit(&self) -> bool {
                match self { $(Self::$variant(message) => message.channel_bit()),+ }
            }
        }

        impl<'a> From<$name $(<$lifetime>)?> for EncodableField<'a> {
            fn from(message: $name $(<$lifetime>)?) -> Self {
                match message { $($name::$variant(message) => message.into()),+ }
            }
        }

        impl $(<$lifetime>)? TryFrom<$name $(<$lifetime>)?>
            for StandardSv2Frame<$name $(<$lifetime>)?>
        {
            type Error = Error;

            fn try_from(message: $name $(<$lifetime>)?) -> Result<Self, Error> {
                let extension = match message {
                    $name::ShareAccountingMessages(_) => EXTENSION_TYPE,
                    _ => 0,
                };
                let (id, channel) = (message.message_type(), message.channel_bit());
                Self::from_message(message, id, extension, channel)
                    .map_err(|_| Error::BadPayloadSize)
            }
        }
    };
}

pool_messages!(PoolExtMessages<'a> {
    Common(parsers::CommonMessages<'a>),
    Mining(parsers::Mining<'a>),
    JobDeclaration(parsers::JobDeclaration<'a>),
    TemplateDistribution(parsers::TemplateDistribution<'a>),
    ShareAccountingMessages(ShareAccountingMessages),
});

pool_messages!(PoolExtMessagesOwned {
    Common(parsers::CommonMessagesOwned),
    Mining(parsers::MiningOwned),
    JobDeclaration(parsers::JobDeclarationOwned),
    TemplateDistribution(parsers::TemplateDistributionOwned),
    ShareAccountingMessages(ShareAccountingMessages),
});

impl PoolExtMessages<'_> {
    pub fn into_owned(self) -> PoolExtMessagesOwned {
        match self {
            Self::Common(message) => PoolExtMessagesOwned::Common(message.into_owned()),
            Self::Mining(message) => PoolExtMessagesOwned::Mining(message.into_owned()),
            Self::JobDeclaration(message) => {
                PoolExtMessagesOwned::JobDeclaration(message.into_owned())
            }
            Self::TemplateDistribution(message) => {
                PoolExtMessagesOwned::TemplateDistribution(message.into_owned())
            }
            Self::ShareAccountingMessages(message) => {
                PoolExtMessagesOwned::ShareAccountingMessages(message)
            }
        }
    }
}

impl<'a> TryFrom<(u16, u8, &'a mut [u8])> for PoolExtMessages<'a> {
    type Error = Error;

    fn try_from((extension, id, payload): (u16, u8, &'a mut [u8])) -> Result<Self, Error> {
        let message = match extension & 0x7fff {
            0 => match parsers::PoolMessages::try_from((id, payload))? {
                parsers::PoolMessages::Common(message) => Self::Common(message),
                parsers::PoolMessages::Mining(message) => Self::Mining(message),
                parsers::PoolMessages::JobDeclaration(message) => Self::JobDeclaration(message),
                parsers::PoolMessages::TemplateDistribution(message) => {
                    Self::TemplateDistribution(message)
                }
            },
            EXTENSION_TYPE => Self::ShareAccountingMessages((id, payload).try_into()?),
            _ => return Err(Error::UnexpectedMessage(id)),
        };
        if message.channel_bit() != (extension & 0x8000 != 0) {
            return Err(Error::UnexpectedMessage(id));
        }
        Ok(message)
    }
}
