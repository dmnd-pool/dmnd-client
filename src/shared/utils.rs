use std::fmt::Display;

use sv1_api::utils::HexU32Be;
use tokio::task::AbortHandle;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct AbortOnDrop {
    abort_handle: AbortHandle,
}

impl AbortOnDrop {
    pub fn new<T: Send + 'static>(handle: JoinHandle<T>) -> Self {
        let abort_handle = handle.abort_handle();
        Self { abort_handle }
    }

    pub fn is_finished(&self) -> bool {
        self.abort_handle.is_finished()
    }
}

impl core::ops::Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.abort_handle.abort()
    }
}

impl<T: Send + 'static> From<JoinHandle<T>> for AbortOnDrop {
    fn from(value: JoinHandle<T>) -> Self {
        Self::new(value)
    }
}

/// Select a version rolling mask and min bit count based on the request from the miner.
/// It copy the behavior from SRI translator
pub fn sv1_rolling(configure: &sv1_api::client_to_server::Configure) -> (HexU32Be, HexU32Be) {
    // TODO 0x1FFFE000 should be configured
    // = 11111111111111110000000000000
    // this is a reasonable default as it allows all 16 version bits to be used
    // If the tproxy/pool needs to use some version bits this needs to be configurable
    // so upstreams can negotiate with downstreams. When that happens this should consider
    // the min_bit_count in the mining.configure message
    let version_rollin_mask = configure
        .version_rolling_mask()
        .map(|mask| HexU32Be(mask & 0x1FFFE000))
        .unwrap_or(HexU32Be(0));
    let version_rolling_min_bit = configure
        .version_rolling_min_bit_count()
        .unwrap_or(HexU32Be(0));
    (version_rollin_mask, version_rolling_min_bit)
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct UserId(pub i64);
impl Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Parses a hashrate string (e.g., "10T", "2.5P", "500E") into an f32 value in h/s.
pub fn parse_hashrate(hashrate_str: &str) -> Result<f32, String> {
    let hashrate_str = hashrate_str.trim();
    if hashrate_str.is_empty() {
        return Err("Hashrate cannot be empty. Expected format: '<number><unit>' (e.g., '10T', '2.5P', '5E'".to_string());
    }

    let unit = hashrate_str.chars().last().unwrap_or(' ').to_string();
    let num = &hashrate_str[..hashrate_str.len().saturating_sub(1)];

    let num: f32 = num.parse().map_err(|_| {
        format!(
            "Invalid number '{}'. Expected format: '<number><unit>' (e.g., '10T', '2.5P', '5E')",
            num
        )
    })?;

    let multiplier = HashUnit::from_str(&unit)
        .map(|unit| unit.multiplier())
        .ok_or_else(|| format!(
            "Invalid unit '{}'. Expected 'T' (Terahash), 'P' (Petahash), or 'E' (Exahash). Example: '10T', '2.5P', '5E'",
            unit
        ))?;

    let hashrate = num * multiplier;

    if hashrate.is_infinite() || hashrate.is_nan() {
        return Err("Hashrate too large or invalid".to_string());
    }

    Ok(hashrate)
}

pub enum HashUnit {
    Tera,
    Peta,
    Exa,
}

impl HashUnit {
    /// Returns the multiplier for each unit in h/s
    fn multiplier(&self) -> f32 {
        match self {
            HashUnit::Tera => 1e12,
            HashUnit::Peta => 1e15,
            HashUnit::Exa => 1e18,
        }
    }

    // Converts a unit string (e.g., "T") to a HashUnit variant
    fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "T" => Some(HashUnit::Tera),
            "P" => Some(HashUnit::Peta),
            "E" => Some(HashUnit::Exa),
            _ => None,
        }
    }

    /// Formats a hashrate value (f32) into a string with the appropriate unit
    pub fn format_value(hashrate: f32) -> String {
        if hashrate >= 1e18 {
            format!("{:.2}E", hashrate / 1e18)
        } else if hashrate >= 1e15 {
            format!("{:.2}P", hashrate / 1e15)
        } else if hashrate >= 1e12 {
            format!("{:.2}T", hashrate / 1e12)
        } else {
            format!("{:.2}", hashrate)
        }
    }
}
