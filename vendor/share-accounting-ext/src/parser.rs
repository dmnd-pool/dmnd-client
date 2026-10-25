use crate::*;
use binary_sv2::{encodable::EncodableField, GetSize};
use demand_sv2_connection::StandardSv2Frame;
use roles_logic_sv2::{
    parsers::{self, IsSv2Message},
    Error,
};

#[derive(Clone, Debug)]
pub enum ShareAccountingMessages {
    ShareOk(ShareOk),
    NewBlockFound(NewBlockFound),
    GetWindow(GetWindow),
    GetWindowSuccess(GetWindowSuccess),
    GetWindowBusy(GetWindowBusy),
    GetShares(GetShares),
    GetSharesSuccess(GetSharesSuccess),
    NewTxs(NewTxs),
    ErrorMessage(ErrorMessage),
}
impl GetSize for ShareAccountingMessages {
    fn get_size(&self) -> usize {
        match self {
            Self::ShareOk(m) => m.get_size(),
            Self::NewBlockFound(m) => m.get_size(),
            Self::GetWindow(m) => m.get_size(),
            Self::GetWindowSuccess(m) => m.get_size(),
            Self::GetWindowBusy(m) => m.get_size(),
            Self::GetShares(m) => m.get_size(),
            Self::GetSharesSuccess(m) => m.get_size(),
            Self::NewTxs(m) => m.get_size(),
            Self::ErrorMessage(m) => m.get_size(),
        }
    }
}
impl IsSv2Message for ShareAccountingMessages {
    fn message_type(&self) -> u8 {
        match self {
            Self::ShareOk(_) => MESSAGE_TYPE_SHARE_OK,
            Self::NewBlockFound(_) => MESSAGE_TYPE_NEW_BLOCK_FOUND,
            Self::GetWindow(_) => MESSAGE_TYPE_GET_WINDOW,
            Self::GetWindowSuccess(_) => MESSAGE_TYPE_GET_WINDOW_SUCCESS,
            Self::GetWindowBusy(_) => MESSAGE_TYPE_GET_WINDOW_BUSY,
            Self::GetShares(_) => MESSAGE_TYPE_GET_SHARES,
            Self::GetSharesSuccess(_) => MESSAGE_TYPE_GET_SHARES_SUCCESS,
            Self::NewTxs(_) => MESSAGE_TYPE_NEW_TXS,
            Self::ErrorMessage(_) => MESSAGE_TYPE_ERROR_MESSAGE,
        }
    }
    fn channel_bit(&self) -> bool {
        match self {
            Self::ShareOk(_) => CHANNEL_BIT_SHARE_OK,
            Self::NewBlockFound(_) => CHANNEL_BIT_NEW_BLOCK_FOUND,
            Self::GetWindow(_) => CHANNEL_BIT_GET_WINDOW,
            Self::GetWindowSuccess(_) => CHANNEL_BIT_GET_WINDOW_SUCCESS,
            Self::GetWindowBusy(_) => CHANNEL_BIT_GET_WINDOW_BUSY,
            Self::GetShares(_) => CHANNEL_BIT_GET_SHARES,
            Self::GetSharesSuccess(_) => CHANNEL_BIT_GET_SHARES_SUCCESS,
            Self::NewTxs(_) => CHANNEL_BIT_NEW_TXS,
            Self::ErrorMessage(_) => CHANNEL_BIT_ERROR_MESSAGE,
        }
    }
}
impl<'a> From<ShareAccountingMessages> for EncodableField<'a> {
    fn from(m: ShareAccountingMessages) -> Self {
        match m {
            ShareAccountingMessages::ShareOk(m) => m.into(),
            ShareAccountingMessages::NewBlockFound(m) => m.into(),
            ShareAccountingMessages::GetWindow(m) => m.into(),
            ShareAccountingMessages::GetWindowSuccess(m) => m.into(),
            ShareAccountingMessages::GetWindowBusy(m) => m.into(),
            ShareAccountingMessages::GetShares(m) => m.into(),
            ShareAccountingMessages::GetSharesSuccess(m) => m.into(),
            ShareAccountingMessages::NewTxs(m) => m.into(),
            ShareAccountingMessages::ErrorMessage(m) => m.into(),
        }
    }
}
impl TryFrom<(u8, &mut [u8])> for ShareAccountingMessages {
    type Error = Error;
    fn try_from((id, payload): (u8, &mut [u8])) -> Result<Self, Error> {
        let len = payload.len();
        let message = match id {
            MESSAGE_TYPE_SHARE_OK => Self::ShareOk(binary_sv2::from_bytes(payload)?),
            MESSAGE_TYPE_NEW_BLOCK_FOUND => Self::NewBlockFound(binary_sv2::from_bytes(payload)?),
            MESSAGE_TYPE_GET_WINDOW => Self::GetWindow(binary_sv2::from_bytes(payload)?),
            MESSAGE_TYPE_GET_WINDOW_SUCCESS => {
                Self::GetWindowSuccess(binary_sv2::from_bytes(payload)?)
            }
            MESSAGE_TYPE_GET_WINDOW_BUSY => Self::GetWindowBusy(binary_sv2::from_bytes(payload)?),
            MESSAGE_TYPE_GET_SHARES => Self::GetShares(binary_sv2::from_bytes(payload)?),
            MESSAGE_TYPE_GET_SHARES_SUCCESS => {
                Self::GetSharesSuccess(binary_sv2::from_bytes(payload)?)
            }
            MESSAGE_TYPE_NEW_TXS => Self::NewTxs(binary_sv2::from_bytes(payload)?),
            MESSAGE_TYPE_ERROR_MESSAGE => Self::ErrorMessage(binary_sv2::from_bytes(payload)?),
            _ => return Err(Error::UnexpectedMessage(id)),
        };
        if message.get_size() != len {
            return Err(Error::BadPayloadSize);
        }
        Ok(message)
    }
}
#[derive(Clone, Debug)]
pub enum PoolExtMessages<'a> {
    Common(parsers::CommonMessages<'a>),
    Mining(parsers::Mining<'a>),
    JobDeclaration(parsers::JobDeclaration<'a>),
    TemplateDistribution(parsers::TemplateDistribution<'a>),
    ShareAccountingMessages(ShareAccountingMessages),
}
#[derive(Clone, Debug)]
pub enum PoolExtMessagesOwned {
    Common(parsers::CommonMessagesOwned),
    Mining(parsers::MiningOwned),
    JobDeclaration(parsers::JobDeclarationOwned),
    TemplateDistribution(parsers::TemplateDistributionOwned),
    ShareAccountingMessages(ShareAccountingMessages),
}
impl GetSize for PoolExtMessages<'_> {
    fn get_size(&self) -> usize {
        match self {
            Self::Common(m) => m.get_size(),
            Self::Mining(m) => m.get_size(),
            Self::JobDeclaration(m) => m.get_size(),
            Self::TemplateDistribution(m) => m.get_size(),
            Self::ShareAccountingMessages(m) => m.get_size(),
        }
    }
}
impl IsSv2Message for PoolExtMessages<'_> {
    fn message_type(&self) -> u8 {
        match self {
            Self::Common(m) => m.message_type(),
            Self::Mining(m) => m.message_type(),
            Self::JobDeclaration(m) => m.message_type(),
            Self::TemplateDistribution(m) => m.message_type(),
            Self::ShareAccountingMessages(m) => m.message_type(),
        }
    }
    fn channel_bit(&self) -> bool {
        match self {
            Self::Common(m) => m.channel_bit(),
            Self::Mining(m) => m.channel_bit(),
            Self::JobDeclaration(m) => m.channel_bit(),
            Self::TemplateDistribution(m) => m.channel_bit(),
            Self::ShareAccountingMessages(m) => m.channel_bit(),
        }
    }
}
impl GetSize for PoolExtMessagesOwned {
    fn get_size(&self) -> usize {
        match self {
            Self::Common(m) => m.get_size(),
            Self::Mining(m) => m.get_size(),
            Self::JobDeclaration(m) => m.get_size(),
            Self::TemplateDistribution(m) => m.get_size(),
            Self::ShareAccountingMessages(m) => m.get_size(),
        }
    }
}
impl IsSv2Message for PoolExtMessagesOwned {
    fn message_type(&self) -> u8 {
        match self {
            Self::Common(m) => m.message_type(),
            Self::Mining(m) => m.message_type(),
            Self::JobDeclaration(m) => m.message_type(),
            Self::TemplateDistribution(m) => m.message_type(),
            Self::ShareAccountingMessages(m) => m.message_type(),
        }
    }
    fn channel_bit(&self) -> bool {
        match self {
            Self::Common(m) => m.channel_bit(),
            Self::Mining(m) => m.channel_bit(),
            Self::JobDeclaration(m) => m.channel_bit(),
            Self::TemplateDistribution(m) => m.channel_bit(),
            Self::ShareAccountingMessages(m) => m.channel_bit(),
        }
    }
}
impl<'a> From<PoolExtMessages<'a>> for EncodableField<'a> {
    fn from(m: PoolExtMessages<'a>) -> Self {
        match m {
            PoolExtMessages::Common(m) => m.into(),
            PoolExtMessages::Mining(m) => m.into(),
            PoolExtMessages::JobDeclaration(m) => m.into(),
            PoolExtMessages::TemplateDistribution(m) => m.into(),
            PoolExtMessages::ShareAccountingMessages(m) => m.into(),
        }
    }
}
impl<'a> From<PoolExtMessagesOwned> for EncodableField<'a> {
    fn from(m: PoolExtMessagesOwned) -> Self {
        match m {
            PoolExtMessagesOwned::Common(m) => m.into(),
            PoolExtMessagesOwned::Mining(m) => m.into(),
            PoolExtMessagesOwned::JobDeclaration(m) => m.into(),
            PoolExtMessagesOwned::TemplateDistribution(m) => m.into(),
            PoolExtMessagesOwned::ShareAccountingMessages(m) => m.into(),
        }
    }
}
impl PoolExtMessages<'_> {
    pub fn into_owned(self) -> PoolExtMessagesOwned {
        match self {
            Self::Common(m) => PoolExtMessagesOwned::Common(m.into_owned()),
            Self::Mining(m) => PoolExtMessagesOwned::Mining(m.into_owned()),
            Self::JobDeclaration(m) => PoolExtMessagesOwned::JobDeclaration(m.into_owned()),
            Self::TemplateDistribution(m) => {
                PoolExtMessagesOwned::TemplateDistribution(m.into_owned())
            }
            Self::ShareAccountingMessages(m) => PoolExtMessagesOwned::ShareAccountingMessages(m),
        }
    }
}
impl<'a> TryFrom<(u16, u8, &'a mut [u8])> for PoolExtMessages<'a> {
    type Error = Error;
    fn try_from((extension, id, payload): (u16, u8, &'a mut [u8])) -> Result<Self, Error> {
        let message = match extension & 0x7fff {
            0 => match parsers::PoolMessages::try_from((id, payload))? {
                parsers::PoolMessages::Common(m) => Ok(Self::Common(m)),
                parsers::PoolMessages::Mining(m) => Ok(Self::Mining(m)),
                parsers::PoolMessages::JobDeclaration(m) => Ok(Self::JobDeclaration(m)),
                parsers::PoolMessages::TemplateDistribution(m) => Ok(Self::TemplateDistribution(m)),
            },
            EXTENSION_TYPE => Ok(Self::ShareAccountingMessages((id, payload).try_into()?)),
            _ => Err(Error::UnexpectedMessage(id)),
        }?;
        if message.channel_bit() != (extension & 0x8000 != 0) {
            return Err(Error::UnexpectedMessage(id));
        }
        Ok(message)
    }
}
impl TryFrom<PoolExtMessagesOwned> for StandardSv2Frame<PoolExtMessagesOwned> {
    type Error = Error;
    fn try_from(message: PoolExtMessagesOwned) -> Result<Self, Error> {
        let extension = match message {
            PoolExtMessagesOwned::ShareAccountingMessages(_) => EXTENSION_TYPE,
            _ => 0,
        };
        let (id, channel) = (message.message_type(), message.channel_bit());
        Self::from_message(message, id, extension, channel).map_err(|_| Error::BadPayloadSize)
    }
}

impl<'a> TryFrom<PoolExtMessages<'a>> for StandardSv2Frame<PoolExtMessages<'a>> {
    type Error = Error;

    fn try_from(message: PoolExtMessages<'a>) -> Result<Self, Error> {
        let extension = match message {
            PoolExtMessages::ShareAccountingMessages(_) => EXTENSION_TYPE,
            _ => 0,
        };
        let (id, channel) = (message.message_type(), message.channel_bit());
        Self::from_message(message, id, extension, channel).map_err(|_| Error::BadPayloadSize)
    }
}
