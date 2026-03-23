use std::fmt;

#[derive(Debug)]
pub enum Error {
    BinarySv2(binary_sv2::Error),
    /// Errors on bad noise handshake.
    SV2Connection(demand_sv2_connection::Error),
    /// Errors from `framing_sv2` crate.
    FramingSv2(framing_sv2::Error),
    /// Errors on bad `TcpStream` connection.
    Io(std::io::Error),
    /// Errors on bad `String` to `int` conversion.
    RolesSv2Logic(roles_logic_sv2::errors::Error),
    UpstreamIncoming(roles_logic_sv2::errors::Error),
    Timeout,
    Shutdown,
    Unrecoverable,
    UnexpectedMessage,
    MiningPoolMutexCorrupted,
    MiningPoolTaskManagerFailed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match self {
            BinarySv2(ref e) => write!(f, "Binary SV2 error: `{e:?}`"),
            SV2Connection(ref e) => write!(f, "Demand SV2 connectiom  error: `{e:?}"),
            FramingSv2(ref e) => write!(f, "Framing SV2 error: `{e:?}`"),
            Io(ref e) => write!(f, "I/O error: `{e:?}"),
            RolesSv2Logic(ref e) => write!(f, "Roles SV2 Logic Error: `{e:?}`"),
            UpstreamIncoming(ref e) => write!(f, "Upstream parse incoming error: `{e:?}`"),
            Unrecoverable => write!(f, "Unrecoverable error"),
            Shutdown => write!(f, "Shutdown requested"),
            UnexpectedMessage => write!(f, "Unexpected Message Type"),
            Timeout => write!(f, "Timeout Elapsed"),
            MiningPoolMutexCorrupted => write!(f, "Mining Pool Mutex Corrupted"),
            MiningPoolTaskManagerFailed => write!(f, "Mining Pool TaskManager Error"),
        }
    }
}

impl From<binary_sv2::Error> for Error {
    fn from(e: binary_sv2::Error) -> Self {
        Error::BinarySv2(e)
    }
}

impl From<demand_sv2_connection::Error> for Error {
    fn from(e: demand_sv2_connection::Error) -> Self {
        Error::SV2Connection(e)
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

impl From<roles_logic_sv2::errors::Error> for Error {
    fn from(e: roles_logic_sv2::errors::Error) -> Self {
        Error::RolesSv2Logic(e)
    }
}
