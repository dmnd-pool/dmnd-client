pub mod artifacts;
pub mod pool;
pub mod processes;
#[path = "../../src/share_accounter/protocol/mod.rs"]
pub mod share_accounting;
pub mod sv1_client;

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message.into()).into()
}
