mod frames;
pub mod noise_connection_tokio;

pub use frames::{StandardEitherFrame, StandardSv2Frame};
pub use noise_sv2;
pub use noise_sv2::{Initiator, Responder};

#[derive(Debug)]
pub enum HandshakeRole {
    Initiator(Box<Initiator>),
    Responder(Box<Responder>),
}

#[derive(Debug)]
pub enum Error {
    HandshakeRemoteInvalidMessage,
    CodecError(codec_sv2::Error),
    SendError,
    Io(std::io::Error),
}

impl From<codec_sv2::Error> for Error {
    fn from(error: codec_sv2::Error) -> Self {
        Self::CodecError(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::SendError
    }
}
