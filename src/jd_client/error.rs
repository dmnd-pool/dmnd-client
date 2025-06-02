use std::fmt;

use bitcoin::error::ParseIntError;

pub type ProxyResult<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    VecToSlice32(Vec<u8>),
    /// Errors on bad CLI argument input.
    BadCliArgs,
    /// Errors from `binary_sv2` crate.
    BinarySv2(binary_sv2::Error),
    /// Errors on bad noise handshake.
    CodecNoise(codec_sv2::noise_sv2::Error),
    /// Errors from `framing_sv2` crate.
    FramingSv2(framing_sv2::Error),
    /// Errors on bad `TcpStream` connection.
    Io(std::io::Error),
    /// Errors on bad `String` to `int` conversion.
    ParseInt(std::num::ParseIntError),
    /// Errors from `roles_logic_sv2` crate.
    RolesSv2Logic(roles_logic_sv2::errors::Error),
    UpstreamIncoming(roles_logic_sv2::errors::Error),
    #[allow(dead_code)]
    SubprotocolMining(String),
    // Locking Errors
    PoisonLock,
    TokioChannelErrorRecv(tokio::sync::broadcast::error::RecvError),
    Uint256Conversion(ParseIntError),
    Infallible(std::convert::Infallible),
    Unrecoverable,
    TaskManagerFailed,
    JdClientMutexCorrupted,

    // jd_client/job_declarator specific errors
    JobDeclaratorMutexCorrupted,
    JobDeclaratorTaskManagerFailed,
    // jd_client/mining_downstream specific errors
    JdClientDownstreamMutexCorrupted,
    JdClientDownstreamTaskManagerFailed,
    // jd_client/mining_upstream specific errors
    JdClientUpstreamMutexCorrupted,
    JdClientUpstreamTaskManagerFailed,
    JdMissing,
    // template_receiver specific errors
    TemplateRxMutexCorrupted,
    TemplateRxTaskManagerFailed,
    TpMissing,
    // Payout address & network spepcif errors (solo mining)
    InvalidAddress(String),
    NetworkMismatch(String),
    InvalidNetwork(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match self {
            BadCliArgs => write!(f, "Bad CLI arg input"),
            BinarySv2(ref e) => write!(f, "Binary SV2 error: `{:?}`", e),
            CodecNoise(ref e) => write!(f, "Noise error: `{:?}", e),
            FramingSv2(ref e) => write!(f, "Framing SV2 error: `{:?}`", e),
            Io(ref e) => write!(f, "I/O error: `{:?}", e),
            ParseInt(ref e) => write!(f, "Bad convert from `String` to `int`: `{:?}`", e),
            RolesSv2Logic(ref e) => write!(f, "Roles SV2 Logic Error: `{:?}`", e),
            SubprotocolMining(ref e) => write!(f, "Subprotocol Mining Error: `{:?}`", e),
            UpstreamIncoming(ref e) => write!(f, "Upstream parse incoming error: `{:?}`", e),
            PoisonLock => write!(f, "Poison Lock error"),
            TokioChannelErrorRecv(ref e) => write!(f, "Channel receive error: `{:?}`", e),
            Uint256Conversion(ref e) => write!(f, "U256 Conversion Error: `{:?}`", e),
            VecToSlice32(ref e) => write!(f, "Standard Error: `{:?}`", e),
            Infallible(ref e) => write!(f, "Infallible Error:`{:?}`", e),
            Unrecoverable => write!(f, "Unrecoverable Error"),
            JdClientMutexCorrupted => write!(f, "JdClient mutex Corrupted"),
            TaskManagerFailed => write!(f, "Failed to add Task in JdClient TaskManager"),

            JobDeclaratorMutexCorrupted => write!(f, "Job Declarator mutex Corrupted"),
            JobDeclaratorTaskManagerFailed => {
                write!(f, "Failed to add Task in Job Declarator TaskManager")
            }
            JdMissing => write!(f, "Job declarator is None"),

            JdClientDownstreamMutexCorrupted => {
                write!(f, "JdClient Mining Downstream mutex Corrupted")
            }
            JdClientDownstreamTaskManagerFailed => write!(
                f,
                "Failed to add Task in JdClient Mining Downstream TaskManager"
            ),
            JdClientUpstreamMutexCorrupted => write!(f, "JdClient Mining Upstream mutex Corrupted"),
            JdClientUpstreamTaskManagerFailed => write!(
                f,
                "Failed to add Task in JdClient Mining Upstream TaskManager"
            ),
            TemplateRxMutexCorrupted => write!(f, "TemplateRx mutex Corrupted"),
            TemplateRxTaskManagerFailed => {
                write!(f, "Failed to add Task in TemplateRx TaskManager")
            }
            TpMissing => write!(f, "Failed to connect to TP"),
            InvalidAddress(ref e) => write!(f, "Invalid Bitcoin address: {}", e),
            NetworkMismatch(ref e) => write!(f, "Network mismatch: {}", e),
            InvalidNetwork(ref e) => write!(f, "Invalid network specified: {}", e),
        }
    }
}

impl From<binary_sv2::Error> for Error {
    fn from(e: binary_sv2::Error) -> Self {
        Error::BinarySv2(e)
    }
}

impl From<codec_sv2::noise_sv2::Error> for Error {
    fn from(e: codec_sv2::noise_sv2::Error) -> Self {
        Error::CodecNoise(e)
    }
}

impl From<framing_sv2::Error> for Error {
    fn from(e: framing_sv2::Error) -> Self {
        Error::FramingSv2(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Error::ParseInt(e)
    }
}

impl From<roles_logic_sv2::errors::Error> for Error {
    fn from(e: roles_logic_sv2::errors::Error) -> Self {
        Error::RolesSv2Logic(e)
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for Error {
    fn from(e: tokio::sync::broadcast::error::RecvError) -> Self {
        Error::TokioChannelErrorRecv(e)
    }
}

// *** LOCK ERRORS ***
// impl<'a> From<PoisonError<MutexGuard<'a, proxy::Bridge>>> for Error<'a> {
//     fn from(e: PoisonError<MutexGuard<'a, proxy::Bridge>>) -> Self {
//         Error::PoisonLock(
//             LockError::Bridge(e)
//         )
//     }
// }

// impl<'a> From<PoisonError<MutexGuard<'a, NextMiningNotify>>> for Error<'a> {
//     fn from(e: PoisonError<MutexGuard<'a, NextMiningNotify>>) -> Self {
//         Error::PoisonLock(
//             LockError::NextMiningNotify(e)
//         )
//     }
// }

impl From<Vec<u8>> for Error {
    fn from(e: Vec<u8>) -> Self {
        Error::VecToSlice32(e)
    }
}

impl From<ParseIntError> for Error {
    fn from(e: ParseIntError) -> Self {
        Error::Uint256Conversion(e)
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(e: std::convert::Infallible) -> Self {
        Error::Infallible(e)
    }
}
