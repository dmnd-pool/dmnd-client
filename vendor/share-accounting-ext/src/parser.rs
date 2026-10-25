#[cfg(not(feature = "with_serde"))]
use binary_sv2::{decodable::DecodableField, decodable::FieldMarker, encodable::EncodableField};

#[cfg(feature = "with_serde")]
use binary_sv2::Serialize;

use binary_sv2::GetSize;

use binary_sv2::{from_bytes, Deserialize};
use roles_logic_sv2::parsers::{CommonMessageTypes, JobDeclarationTypes, MiningTypes};
use roles_logic_sv2::parsers::{
    CommonMessages, IsSv2Message, JobDeclaration, Mining, MiningDeviceMessages,
    TemplateDistribution,
};
use roles_logic_sv2::Error;

use framing_sv2::framing::Sv2Frame;

use crate::error_message::ErrorMessage;
use crate::get_shares::{GetShares, GetSharesSuccess};
use crate::get_window::{GetWindow, GetWindowBusy, GetWindowSuccess};
use crate::new_block_found::NewBlockFound;
use crate::new_txs::NewTxs;
use crate::share_ok::ShareOk;

use crate::r#const::*;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "with_serde", derive(Serialize, Deserialize))]
pub enum ShareAccountingMessages<'a> {
    ShareOk(ShareOk),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    NewBlockFound(NewBlockFound<'a>),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    GetWindow(GetWindow<'a>),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    GetWindowSuccess(GetWindowSuccess<'a>),
    GetWindowBusy(GetWindowBusy),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    GetShares(GetShares<'a>),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    GetSharesSuccess(GetSharesSuccess<'a>),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    NewTxs(NewTxs<'a>),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    ErrorMessage(ErrorMessage<'a>),
}

impl IsSv2Message for ShareAccountingMessages<'_> {
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

#[cfg(not(feature = "with_serde"))]
impl<'decoder> From<ShareAccountingMessages<'decoder>> for EncodableField<'decoder> {
    fn from(m: ShareAccountingMessages<'decoder>) -> Self {
        match m {
            ShareAccountingMessages::ShareOk(a) => a.into(),
            ShareAccountingMessages::NewBlockFound(a) => a.into(),
            ShareAccountingMessages::GetWindow(a) => a.into(),
            ShareAccountingMessages::GetWindowSuccess(a) => a.into(),
            ShareAccountingMessages::GetWindowBusy(a) => a.into(),
            ShareAccountingMessages::GetShares(a) => a.into(),
            ShareAccountingMessages::GetSharesSuccess(a) => a.into(),
            ShareAccountingMessages::NewTxs(a) => a.into(),
            ShareAccountingMessages::ErrorMessage(a) => a.into(),
        }
    }
}
impl GetSize for ShareAccountingMessages<'_> {
    fn get_size(&self) -> usize {
        match self {
            Self::ShareOk(a) => a.get_size(),
            Self::NewBlockFound(a) => a.get_size(),
            Self::GetWindow(a) => a.get_size(),
            Self::GetWindowSuccess(a) => a.get_size(),
            Self::GetWindowBusy(a) => a.get_size(),
            Self::GetShares(a) => a.get_size(),
            Self::GetSharesSuccess(a) => a.get_size(),
            Self::NewTxs(a) => a.get_size(),
            Self::ErrorMessage(a) => a.get_size(),
        }
    }
}

#[cfg(not(feature = "with_serde"))]
impl<'decoder> Deserialize<'decoder> for ShareAccountingMessages<'decoder> {
    fn get_structure(_v: &[u8]) -> std::result::Result<Vec<FieldMarker>, binary_sv2::Error> {
        unimplemented!()
    }
    fn from_decoded_fields(
        _v: Vec<DecodableField<'decoder>>,
    ) -> std::result::Result<Self, binary_sv2::Error> {
        unimplemented!()
    }
}

#[cfg(not(feature = "with_serde"))]
impl<'decoder> Deserialize<'decoder> for PoolExtMessages<'decoder> {
    fn get_structure(_v: &[u8]) -> std::result::Result<Vec<FieldMarker>, binary_sv2::Error> {
        unimplemented!()
    }
    fn from_decoded_fields(
        _v: Vec<DecodableField<'decoder>>,
    ) -> std::result::Result<Self, binary_sv2::Error> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[allow(clippy::enum_variant_names)]
pub enum ShareAccountingMessagesTypes {
    ShareOk = MESSAGE_TYPE_SHARE_OK,
    NewBlockFound = MESSAGE_TYPE_NEW_BLOCK_FOUND,
    GetWindow = MESSAGE_TYPE_GET_WINDOW,
    GetWindowSuccess = MESSAGE_TYPE_GET_WINDOW_SUCCESS,
    GetWindowBusy = MESSAGE_TYPE_GET_WINDOW_BUSY,
    GetShares = MESSAGE_TYPE_GET_SHARES,
    GetSharesSuccess = MESSAGE_TYPE_GET_SHARES_SUCCESS,
    NewTxs = MESSAGE_TYPE_NEW_TXS,
    ErrorMessage = MESSAGE_TYPE_ERROR_MESSAGE,
}

impl TryFrom<u8> for ShareAccountingMessagesTypes {
    type Error = Error;

    fn try_from(v: u8) -> Result<ShareAccountingMessagesTypes, Error> {
        match v {
            MESSAGE_TYPE_SHARE_OK => Ok(ShareAccountingMessagesTypes::ShareOk),
            MESSAGE_TYPE_NEW_BLOCK_FOUND => Ok(ShareAccountingMessagesTypes::NewBlockFound),
            MESSAGE_TYPE_GET_WINDOW => Ok(ShareAccountingMessagesTypes::GetWindow),
            MESSAGE_TYPE_GET_WINDOW_SUCCESS => Ok(ShareAccountingMessagesTypes::GetWindowSuccess),
            MESSAGE_TYPE_GET_WINDOW_BUSY => Ok(ShareAccountingMessagesTypes::GetWindowBusy),
            MESSAGE_TYPE_GET_SHARES => Ok(ShareAccountingMessagesTypes::GetShares),
            MESSAGE_TYPE_GET_SHARES_SUCCESS => Ok(ShareAccountingMessagesTypes::GetSharesSuccess),
            MESSAGE_TYPE_NEW_TXS => Ok(ShareAccountingMessagesTypes::NewTxs),
            MESSAGE_TYPE_ERROR_MESSAGE => Ok(ShareAccountingMessagesTypes::ErrorMessage),
            _ => Err(Error::UnexpectedMessage(v)),
        }
    }
}

impl<'a> TryFrom<(u8, &'a mut [u8])> for ShareAccountingMessages<'a> {
    type Error = Error;

    fn try_from(v: (u8, &'a mut [u8])) -> Result<Self, Self::Error> {
        let msg_type: ShareAccountingMessagesTypes = v.0.try_into()?;
        match msg_type {
            ShareAccountingMessagesTypes::ShareOk => {
                let message: ShareOk = from_bytes(v.1)?;
                Ok(ShareAccountingMessages::ShareOk(message))
            }
            ShareAccountingMessagesTypes::NewBlockFound => {
                let message: NewBlockFound<'a> = from_bytes(v.1)?;
                Ok(ShareAccountingMessages::NewBlockFound(message))
            }
            ShareAccountingMessagesTypes::GetWindow => {
                let message: GetWindow<'a> = from_bytes(v.1)?;
                Ok(ShareAccountingMessages::GetWindow(message))
            }
            ShareAccountingMessagesTypes::GetWindowSuccess => {
                let message: GetWindowSuccess<'a> = from_bytes(v.1)?;
                Ok(ShareAccountingMessages::GetWindowSuccess(message))
            }
            ShareAccountingMessagesTypes::GetWindowBusy => {
                let message: GetWindowBusy = from_bytes(v.1)?;
                Ok(ShareAccountingMessages::GetWindowBusy(message))
            }
            ShareAccountingMessagesTypes::GetShares => {
                let message: GetShares<'a> = from_bytes(v.1)?;
                Ok(ShareAccountingMessages::GetShares(message))
            }
            ShareAccountingMessagesTypes::GetSharesSuccess => {
                let message: GetSharesSuccess<'a> = from_bytes(v.1)?;
                Ok(ShareAccountingMessages::GetSharesSuccess(message))
            }
            ShareAccountingMessagesTypes::NewTxs => {
                let message: NewTxs<'a> = from_bytes(v.1)?;
                Ok(ShareAccountingMessages::NewTxs(message))
            }
            ShareAccountingMessagesTypes::ErrorMessage => {
                let message: ErrorMessage<'a> = from_bytes(v.1)?;
                Ok(ShareAccountingMessages::ErrorMessage(message))
            }
        }
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "with_serde", derive(Serialize, Deserialize))]
pub enum PoolExtMessages<'a> {
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    Common(CommonMessages<'a>),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    Mining(Mining<'a>),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    JobDeclaration(JobDeclaration<'a>),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    TemplateDistribution(TemplateDistribution<'a>),
    #[cfg_attr(feature = "with_serde", serde(borrow))]
    ShareAccountingMessages(ShareAccountingMessages<'a>),
}

impl<'a> TryFrom<MiningDeviceMessages<'a>> for PoolExtMessages<'a> {
    type Error = Error;

    fn try_from(value: MiningDeviceMessages<'a>) -> Result<Self, Self::Error> {
        match value {
            MiningDeviceMessages::Common(m) => Ok(PoolExtMessages::Common(m)),
            MiningDeviceMessages::Mining(m) => Ok(PoolExtMessages::Mining(m)),
        }
    }
}

#[cfg(not(feature = "with_serde"))]
impl<'decoder> From<PoolExtMessages<'decoder>> for EncodableField<'decoder> {
    fn from(m: PoolExtMessages<'decoder>) -> Self {
        match m {
            PoolExtMessages::Common(a) => a.into(),
            PoolExtMessages::Mining(a) => a.into(),
            PoolExtMessages::JobDeclaration(a) => a.into(),
            PoolExtMessages::TemplateDistribution(a) => a.into(),
            PoolExtMessages::ShareAccountingMessages(a) => a.into(),
        }
    }
}
impl GetSize for PoolExtMessages<'_> {
    fn get_size(&self) -> usize {
        match self {
            PoolExtMessages::Common(a) => a.get_size(),
            PoolExtMessages::Mining(a) => a.get_size(),
            PoolExtMessages::JobDeclaration(a) => a.get_size(),
            PoolExtMessages::TemplateDistribution(a) => a.get_size(),
            PoolExtMessages::ShareAccountingMessages(a) => a.get_size(),
        }
    }
}

impl IsSv2Message for PoolExtMessages<'_> {
    fn message_type(&self) -> u8 {
        match self {
            PoolExtMessages::Common(a) => a.message_type(),
            PoolExtMessages::Mining(a) => a.message_type(),
            PoolExtMessages::JobDeclaration(a) => a.message_type(),
            PoolExtMessages::TemplateDistribution(a) => a.message_type(),
            PoolExtMessages::ShareAccountingMessages(a) => a.message_type(),
        }
    }

    fn channel_bit(&self) -> bool {
        match self {
            PoolExtMessages::Common(a) => a.channel_bit(),
            PoolExtMessages::Mining(a) => a.channel_bit(),
            PoolExtMessages::JobDeclaration(a) => a.channel_bit(),
            PoolExtMessages::TemplateDistribution(a) => a.channel_bit(),
            PoolExtMessages::ShareAccountingMessages(a) => a.channel_bit(),
        }
    }
}

fn ingnore_channel_bit(ext: u16) -> u16 {
    let mask = 0b0111_1111_1111_1111;
    ext & mask
}

impl<'a> TryFrom<(u16, u8, &'a mut [u8])> for PoolExtMessages<'a> {
    type Error = Error;

    // extension, message_type, payload -> PoolExtMessages
    fn try_from(v: (u16, u8, &'a mut [u8])) -> Result<Self, Self::Error> {
        let extension = ingnore_channel_bit(v.0);
        if extension == 0 {
            let is_common: Result<CommonMessageTypes, Error> = v.1.try_into();
            let is_mining: Result<MiningTypes, Error> = v.1.try_into();
            let is_job_declaration: Result<JobDeclarationTypes, Error> = v.1.try_into();
            match (is_common, is_mining, is_job_declaration) {
                (Ok(_), Err(_), Err(_)) => Ok(Self::Common((v.1, v.2).try_into()?)),
                (Err(_), Ok(_), Err(_)) => Ok(Self::Mining((v.1, v.2).try_into()?)),
                (Err(_), Err(_), Ok(_)) => Ok(Self::JobDeclaration((v.1, v.2).try_into()?)),
                (Err(e), Err(_), Err(_)) => Err(e),
                // This is an impossible state is safe to panic here
                _ => panic!(),
            }
        // This is possible since channle bit is never set for this extension
        } else if extension == EXTENSION_TYPE {
            Ok(Self::ShareAccountingMessages((v.1, v.2).try_into()?))
        } else {
            // TODO add UnexpectedExtension message to roles_logic_sv2
            Err(Error::UnexpectedMessage(v.1))
        }
    }
}

impl<'decoder, B: AsMut<[u8]> + AsRef<[u8]>> TryFrom<PoolExtMessages<'decoder>>
    for Sv2Frame<PoolExtMessages<'decoder>, B>
{
    type Error = Error;

    fn try_from(v: PoolExtMessages<'decoder>) -> Result<Self, Error> {
        let extension_type = match v {
            PoolExtMessages::ShareAccountingMessages(_) => EXTENSION_TYPE,
            _ => 0,
        };
        let channel_bit = v.channel_bit();
        let message_type = v.message_type();
        Sv2Frame::from_message(v, message_type, extension_type, channel_bit)
            .ok_or(Error::BadPayloadSize)
    }
}
impl<'a> TryFrom<PoolExtMessages<'a>> for MiningDeviceMessages<'a> {
    type Error = Error;

    fn try_from(value: PoolExtMessages<'a>) -> Result<Self, Error> {
        match value {
            PoolExtMessages::Common(message) => Ok(Self::Common(message)),
            PoolExtMessages::Mining(message) => Ok(Self::Mining(message)),
            PoolExtMessages::JobDeclaration(_) => Err(Error::UnexpectedPoolMessage),
            PoolExtMessages::TemplateDistribution(_) => Err(Error::UnexpectedPoolMessage),
            PoolExtMessages::ShareAccountingMessages(_) => Err(Error::UnexpectedPoolMessage),
        }
    }
}

impl PoolExtMessages<'_> {
    pub fn into_static(self) -> PoolExtMessages<'static> {
        match self {
            PoolExtMessages::Common(a) => match a {
                CommonMessages::ChannelEndpointChanged(m) => {
                    PoolExtMessages::Common(CommonMessages::ChannelEndpointChanged(m.into_static()))
                }
                CommonMessages::SetupConnection(m) => {
                    PoolExtMessages::Common(CommonMessages::SetupConnection(m.into_static()))
                }
                CommonMessages::SetupConnectionError(m) => {
                    PoolExtMessages::Common(CommonMessages::SetupConnectionError(m.into_static()))
                }
                CommonMessages::SetupConnectionSuccess(m) => {
                    PoolExtMessages::Common(CommonMessages::SetupConnectionSuccess(m.into_static()))
                }
            },
            PoolExtMessages::Mining(a) => PoolExtMessages::Mining(a.into_static()),
            PoolExtMessages::JobDeclaration(a) => match a {
                JobDeclaration::AllocateMiningJobToken(m) => PoolExtMessages::JobDeclaration(
                    JobDeclaration::AllocateMiningJobToken(m.into_static()),
                ),
                JobDeclaration::AllocateMiningJobTokenSuccess(m) => {
                    PoolExtMessages::JobDeclaration(JobDeclaration::AllocateMiningJobTokenSuccess(
                        m.into_static(),
                    ))
                }
                JobDeclaration::DeclareMiningJob(m) => PoolExtMessages::JobDeclaration(
                    JobDeclaration::DeclareMiningJob(m.into_static()),
                ),
                JobDeclaration::DeclareMiningJobError(m) => PoolExtMessages::JobDeclaration(
                    JobDeclaration::DeclareMiningJobError(m.into_static()),
                ),
                JobDeclaration::DeclareMiningJobSuccess(m) => PoolExtMessages::JobDeclaration(
                    JobDeclaration::DeclareMiningJobSuccess(m.into_static()),
                ),
                JobDeclaration::ProvideMissingTransactions(m) => PoolExtMessages::JobDeclaration(
                    JobDeclaration::ProvideMissingTransactions(m.into_static()),
                ),
                JobDeclaration::ProvideMissingTransactionsSuccess(m) => {
                    PoolExtMessages::JobDeclaration(
                        JobDeclaration::ProvideMissingTransactionsSuccess(m.into_static()),
                    )
                }
                JobDeclaration::SubmitSolution(m) => {
                    PoolExtMessages::JobDeclaration(JobDeclaration::SubmitSolution(m.into_static()))
                }
            },
            PoolExtMessages::TemplateDistribution(a) => match a {
                TemplateDistribution::CoinbaseOutputDataSize(m) => {
                    PoolExtMessages::TemplateDistribution(
                        TemplateDistribution::CoinbaseOutputDataSize(m.into_static()),
                    )
                }
                TemplateDistribution::NewTemplate(m) => PoolExtMessages::TemplateDistribution(
                    TemplateDistribution::NewTemplate(m.into_static()),
                ),
                TemplateDistribution::RequestTransactionData(m) => {
                    PoolExtMessages::TemplateDistribution(
                        TemplateDistribution::RequestTransactionData(m.into_static()),
                    )
                }
                TemplateDistribution::RequestTransactionDataError(m) => {
                    PoolExtMessages::TemplateDistribution(
                        TemplateDistribution::RequestTransactionDataError(m.into_static()),
                    )
                }
                TemplateDistribution::RequestTransactionDataSuccess(m) => {
                    PoolExtMessages::TemplateDistribution(
                        TemplateDistribution::RequestTransactionDataSuccess(m.into_static()),
                    )
                }
                TemplateDistribution::SetNewPrevHash(m) => PoolExtMessages::TemplateDistribution(
                    TemplateDistribution::SetNewPrevHash(m.into_static()),
                ),
                TemplateDistribution::SubmitSolution(m) => PoolExtMessages::TemplateDistribution(
                    TemplateDistribution::SubmitSolution(m.into_static()),
                ),
            },
            PoolExtMessages::ShareAccountingMessages(a) => match a {
                ShareAccountingMessages::ShareOk(m) => PoolExtMessages::ShareAccountingMessages(
                    ShareAccountingMessages::ShareOk(m.into_static()),
                ),
                ShareAccountingMessages::NewBlockFound(m) => {
                    PoolExtMessages::ShareAccountingMessages(
                        ShareAccountingMessages::NewBlockFound(m.into_static()),
                    )
                }
                ShareAccountingMessages::GetWindow(m) => PoolExtMessages::ShareAccountingMessages(
                    ShareAccountingMessages::GetWindow(m.into_static()),
                ),
                ShareAccountingMessages::GetWindowSuccess(m) => {
                    PoolExtMessages::ShareAccountingMessages(
                        ShareAccountingMessages::GetWindowSuccess(m.into_static()),
                    )
                }
                ShareAccountingMessages::GetWindowBusy(m) => {
                    PoolExtMessages::ShareAccountingMessages(
                        ShareAccountingMessages::GetWindowBusy(m.into_static()),
                    )
                }
                ShareAccountingMessages::GetShares(m) => PoolExtMessages::ShareAccountingMessages(
                    ShareAccountingMessages::GetShares(m.into_static()),
                ),
                ShareAccountingMessages::GetSharesSuccess(m) => {
                    PoolExtMessages::ShareAccountingMessages(
                        ShareAccountingMessages::GetSharesSuccess(m.into_static()),
                    )
                }
                ShareAccountingMessages::NewTxs(m) => PoolExtMessages::ShareAccountingMessages(
                    ShareAccountingMessages::NewTxs(m.into_static()),
                ),
                ShareAccountingMessages::ErrorMessage(m) => {
                    PoolExtMessages::ShareAccountingMessages(ShareAccountingMessages::ErrorMessage(
                        m.into_static(),
                    ))
                }
            },
        }
    }
}
