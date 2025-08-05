use std::fmt;

pub enum Sv1IngressError {
    TranslatorDropped,
    DownstreamDropped,
    TaskFailed,
}

#[derive(Debug)]
pub enum Error {
    ReqwestError(reqwest::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::ReqwestError(e) => write!(f, "ReqwestError: {}", e),
        }
    }
}
impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::ReqwestError(err)
    }
}
