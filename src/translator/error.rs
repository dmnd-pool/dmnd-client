use std::{convert::Infallible, fmt, sync::PoisonError};

pub type ProxyResult<'a, T> = core::result::Result<T, Error<'a>>;

#[derive(Debug)]
pub enum Error<'a> {
    /// Errors due to invalid extranonce from upstream
    InvalidExtranonce(String),
    /// Errors from `roles_logic_sv2` crate.
    RolesSv2Logic(roles_logic_sv2::errors::Error),
    V1Protocol(Box<sv1_api::error::Error<'a>>),
    // Locking Errors
    PoisonLock,
    TranslatorUpstreamMutexPoisoned,
    TranslatorDiffConfigMutexPoisoned,
    TranslatorTaskManagerMutexPoisoned,
    BridgeMutexPoisoned,
    BridgeTaskManagerMutexPoisoned,
    // Task Manager Errors
    TranslatorTaskManagerFailed,
    BridgeTaskManagerFailed,
    // Unrecoverable Errors
    Unrecoverable,
    // used to handle SV2 protocol error messages from pool
    #[allow(clippy::enum_variant_names)]
    TargetError(roles_logic_sv2::errors::Error),
    Infallible(Infallible),
    ImpossibleToOpenChannnel,
    #[allow(clippy::enum_variant_names)]
    AsyncChannelError,
}

impl From<Infallible> for Error<'_> {
    fn from(v: Infallible) -> Self {
        Self::Infallible(v)
    }
}

impl fmt::Display for Error<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::InvalidExtranonce(e) => write!(f, "InvalidExtranonce {}", e),
            Error::RolesSv2Logic(e) => write!(f, "RolesSv2Logic {}", e),
            Error::V1Protocol(e) => write!(f, "V1Protocol {}", e),
            Error::PoisonLock => write!(f, "PoisonLock"),
            Error::TargetError(e) => write!(f, "TargetError {}", e),
            Error::Infallible(e) => write!(f, "Infallible {}", e),
            Error::ImpossibleToOpenChannnel => write!(f, "ImpossibleToOpenChannnel"),
            Error::AsyncChannelError => write!(f, "AsyncChannelError"),
            Error::TranslatorUpstreamMutexPoisoned => write!(f, "TranslatorUpstreamMutexPoisoned"),
            Error::TranslatorDiffConfigMutexPoisoned => {
                write!(f, "TranslatorDiffConfigMutexPoisoned")
            }
            Error::TranslatorTaskManagerMutexPoisoned => {
                write!(f, "TranslatorTaskManagerMutexPoisoned")
            }
            Error::BridgeMutexPoisoned => write!(f, "BridgeMutexPoisoned"),
            Error::BridgeTaskManagerMutexPoisoned => write!(f, "BridgeTaskManagerMutexPoisoned"),
            Error::TranslatorTaskManagerFailed => write!(f, "TranslatorTaskManagerFailed"),
            Error::BridgeTaskManagerFailed => write!(f, "BridgeTaskManagerFailed"),
            Error::Unrecoverable => write!(f, "Unrecoverable"),
        }
    }
}

impl<T> From<PoisonError<T>> for Error<'_> {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonLock
    }
}
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error<'_> {
    fn from(_value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::AsyncChannelError
    }
}
impl From<roles_logic_sv2::Error> for Error<'_> {
    fn from(value: roles_logic_sv2::Error) -> Self {
        Self::RolesSv2Logic(value)
    }
}

impl<'a> From<sv1_api::error::Error<'a>> for Error<'a> {
    fn from(value: sv1_api::error::Error<'a>) -> Self {
        Self::V1Protocol(Box::new(value))
    }
}
