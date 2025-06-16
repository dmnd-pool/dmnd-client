use std::fmt;

#[derive(Debug)]
pub enum Error {
    Timeout,
    NegotationFailed,
    RequestIdMismatch,
    ShareAccounterTaskManagerMutexCorrupted,
    ShareAccounterTaskManagerError,
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
            NegotationFailed => write!(f, "Extension Negotation Failed"),
            RequestIdMismatch => write!(f, "Request Id Mismatch"),
        }
    }
}
