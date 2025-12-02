use std::fmt;

#[derive(Debug)]
pub enum Error {
    Timeout,
    NegotationFailed,
    RequestIdMismatch,
    ShareAccounterTaskManagerMutexCorrupted,
    ShareAccounterTaskManagerError,
    InvalidShareOk,
    SendError,
    VerificationError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match self {
            ShareAccounterTaskManagerMutexCorrupted => {
                write!(f, "Share Accounter Task Manager Mutex Corrupted")
            }
            ShareAccounterTaskManagerError => {
                write!(f, "Share Accounter TaskManager Failed to add Task")
            }
            Timeout => write!(f, "Timeout"),
            NegotationFailed => write!(f, "Extension Negotiation Failed"),
            RequestIdMismatch => write!(f, "Request Id Mismatch"),
            InvalidShareOk => write!(f, "Invalid ShareOk message from pool"),
            SendError => write!(f, "Failed to send message"),
            VerificationError(msg) => write!(f, "Verification error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}
