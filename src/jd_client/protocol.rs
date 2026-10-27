use framing_sv2::header::Header;
use roles_logic_sv2::Error;

pub(super) const VERSION: u16 = 2;

pub(super) fn validate_version(version: u16) -> Result<(), Error> {
    if version != VERSION {
        return Err(Error::UnexpectedMessage(
            roles_logic_sv2::common_messages_sv2::MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        ));
    }
    Ok(())
}

pub(super) fn message_type(
    header: Option<Header>,
    channel_bit: impl FnOnce(u8) -> Result<bool, Error>,
) -> Result<u8, Error> {
    let header = header.ok_or(Error::BadPayloadSize)?;
    let message_type = header.msg_type();
    if header.ext_type_without_channel_msg() != 0
        || header.channel_msg() != channel_bit(message_type)?
    {
        return Err(Error::UnexpectedMessage(message_type));
    }
    Ok(message_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use roles_logic_sv2::parsers::CommonMessageTypes;

    #[test]
    fn setup_headers_reject_extensions_channels_and_unknown_ids() {
        for bytes in [
            [1, 0, 1, 0, 0, 0],
            [0, 128, 1, 0, 0, 0],
            [0, 0, 255, 0, 0, 0],
        ] {
            assert!(
                message_type(Some(Header::from_bytes(&bytes).unwrap()), |id| {
                    CommonMessageTypes::try_from(id).map(CommonMessageTypes::channel_bit)
                })
                .is_err()
            );
        }
        assert_eq!(
            message_type(
                Some(Header::from_bytes(&[0, 0, 1, 0, 0, 0]).unwrap()),
                |id| { CommonMessageTypes::try_from(id).map(CommonMessageTypes::channel_bit) }
            )
            .unwrap(),
            1
        );
    }
}
