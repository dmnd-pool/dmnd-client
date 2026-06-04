use std::{
    env, fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    str,
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, Error as TokioIoError},
    net::TcpStream,
    time,
};

const V1_PREFIX: &[u8] = b"PROXY ";
const V1_MAX_HEADER_LEN: usize = 108;
const V2_SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyProtocolMode {
    Disabled,
    Required,
    Optional,
}

impl ProxyProtocolMode {
    pub fn from_env(var_name: &str) -> Result<Self, ProxyProtocolError> {
        match env::var(var_name) {
            Ok(value) => value.parse(),
            Err(env::VarError::NotPresent) => Ok(Self::Disabled),
            Err(env::VarError::NotUnicode(_)) => Err(ProxyProtocolError::InvalidMode {
                value: format!("{var_name}=<non-unicode>"),
            }),
        }
    }
}

impl str::FromStr for ProxyProtocolMode {
    type Err = ProxyProtocolError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "0" | "false" | "off" | "disabled" => Ok(Self::Disabled),
            "1" | "true" | "on" | "required" => Ok(Self::Required),
            "optional" => Ok(Self::Optional),
            _ => Err(ProxyProtocolError::InvalidMode {
                value: raw.to_string(),
            }),
        }
    }
}

#[derive(Debug)]
pub struct AcceptedStream {
    pub stream: TcpStream,
    pub source_addr: SocketAddr,
    pub destination_addr: Option<SocketAddr>,
    pub used_proxy_protocol: bool,
}

pub async fn accept(
    stream: TcpStream,
    peer_addr: SocketAddr,
    mode: ProxyProtocolMode,
) -> Result<AcceptedStream, ProxyProtocolError> {
    match mode {
        ProxyProtocolMode::Disabled => Ok(AcceptedStream {
            stream,
            source_addr: peer_addr,
            destination_addr: None,
            used_proxy_protocol: false,
        }),
        ProxyProtocolMode::Optional => accept_optional(stream, peer_addr).await,
        ProxyProtocolMode::Required => accept_required(stream, peer_addr).await,
    }
}

async fn accept_optional(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
) -> Result<AcceptedStream, ProxyProtocolError> {
    match detect_proxy_header(&stream).await? {
        ProxyHeaderKind::V2 => {
            let parsed = read_v2_header(&mut stream, peer_addr).await?;
            Ok(parsed.into_accepted(stream))
        }
        ProxyHeaderKind::V1 => {
            let parsed = read_v1_header(&mut stream, peer_addr).await?;
            Ok(parsed.into_accepted(stream))
        }
        ProxyHeaderKind::NotProxy => Ok(AcceptedStream {
            stream,
            source_addr: peer_addr,
            destination_addr: None,
            used_proxy_protocol: false,
        }),
    }
}

async fn accept_required(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
) -> Result<AcceptedStream, ProxyProtocolError> {
    match detect_proxy_header(&stream).await? {
        ProxyHeaderKind::V2 => {
            let parsed = read_v2_header(&mut stream, peer_addr).await?;
            Ok(parsed.into_accepted(stream))
        }
        ProxyHeaderKind::V1 => {
            let parsed = read_v1_header(&mut stream, peer_addr).await?;
            Ok(parsed.into_accepted(stream))
        }
        ProxyHeaderKind::NotProxy => Err(ProxyProtocolError::MissingHeader),
    }
}

async fn detect_proxy_header(stream: &TcpStream) -> Result<ProxyHeaderKind, ProxyProtocolError> {
    let mut prefix = [0_u8; V2_SIGNATURE.len()];

    loop {
        let read_len = stream.peek(&mut prefix).await?;
        if read_len == 0 {
            return Err(ProxyProtocolError::MissingHeader);
        }

        match classify_proxy_prefix(&prefix[..read_len]) {
            PrefixMatch::Complete(kind) => return Ok(kind),
            PrefixMatch::NeedMore => {
                time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            PrefixMatch::NotProxy => return Ok(ProxyHeaderKind::NotProxy),
        }
    }
}

fn classify_proxy_prefix(prefix: &[u8]) -> PrefixMatch {
    if prefix.len() < V2_SIGNATURE.len() && V2_SIGNATURE.starts_with(prefix) {
        return PrefixMatch::NeedMore;
    }

    if prefix == V2_SIGNATURE {
        return PrefixMatch::Complete(ProxyHeaderKind::V2);
    }

    if prefix.len() < V1_PREFIX.len() && V1_PREFIX.starts_with(prefix) {
        return PrefixMatch::NeedMore;
    }

    if prefix.starts_with(V1_PREFIX) {
        return PrefixMatch::Complete(ProxyHeaderKind::V1);
    }

    PrefixMatch::NotProxy
}

async fn read_v1_header(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
) -> Result<ParsedHeader, ProxyProtocolError> {
    let mut header = Vec::with_capacity(V1_MAX_HEADER_LEN);

    loop {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await?;
        header.push(byte[0]);

        if header.len() > V1_MAX_HEADER_LEN {
            return Err(ProxyProtocolError::HeaderTooLong);
        }

        if header.ends_with(b"\r\n") {
            break;
        }
    }

    parse_v1_header(&header, peer_addr)
}

fn parse_v1_header(
    header: &[u8],
    peer_addr: SocketAddr,
) -> Result<ParsedHeader, ProxyProtocolError> {
    let header = str::from_utf8(header).map_err(|_| ProxyProtocolError::InvalidV1Header)?;
    let header = header
        .strip_suffix("\r\n")
        .ok_or(ProxyProtocolError::InvalidV1Header)?;
    let parts: Vec<&str> = header.split_whitespace().collect();

    if parts.len() < 2 || parts[0] != "PROXY" {
        return Err(ProxyProtocolError::InvalidV1Header);
    }

    if parts[1] == "UNKNOWN" {
        return Ok(ParsedHeader {
            source_addr: peer_addr,
            destination_addr: None,
        });
    }

    if parts.len() != 6 {
        return Err(ProxyProtocolError::InvalidV1Header);
    }

    let source_ip: IpAddr = parts[2]
        .parse()
        .map_err(|_| ProxyProtocolError::InvalidV1Header)?;
    let destination_ip: IpAddr = parts[3]
        .parse()
        .map_err(|_| ProxyProtocolError::InvalidV1Header)?;
    let source_port: u16 = parts[4]
        .parse()
        .map_err(|_| ProxyProtocolError::InvalidV1Header)?;
    let destination_port: u16 = parts[5]
        .parse()
        .map_err(|_| ProxyProtocolError::InvalidV1Header)?;

    match (parts[1], source_ip, destination_ip) {
        ("TCP4", IpAddr::V4(_), IpAddr::V4(_)) | ("TCP6", IpAddr::V6(_), IpAddr::V6(_)) => {
            Ok(ParsedHeader {
                source_addr: SocketAddr::new(source_ip, source_port),
                destination_addr: Some(SocketAddr::new(destination_ip, destination_port)),
            })
        }
        _ => Err(ProxyProtocolError::InvalidV1Header),
    }
}

async fn read_v2_header(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
) -> Result<ParsedHeader, ProxyProtocolError> {
    let mut prefix = [0_u8; 12];
    stream.read_exact(&mut prefix).await?;
    if prefix != *V2_SIGNATURE {
        return Err(ProxyProtocolError::InvalidV2Header);
    }

    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await?;
    let len = u16::from_be_bytes([header[2], header[3]]) as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await?;

    parse_v2_header(header[0], header[1], &payload, peer_addr)
}

fn parse_v2_header(
    version_command: u8,
    family_protocol: u8,
    payload: &[u8],
    peer_addr: SocketAddr,
) -> Result<ParsedHeader, ProxyProtocolError> {
    let version = version_command >> 4;
    let command = version_command & 0x0f;

    if version != 0x2 {
        return Err(ProxyProtocolError::InvalidV2Header);
    }

    if command == 0x0 {
        return Ok(ParsedHeader {
            source_addr: peer_addr,
            destination_addr: None,
        });
    }

    if command != 0x1 {
        return Err(ProxyProtocolError::InvalidV2Header);
    }

    let family = family_protocol >> 4;
    let protocol = family_protocol & 0x0f;
    if protocol != 0x1 {
        return Err(ProxyProtocolError::UnsupportedProtocol);
    }

    match family {
        0x1 => {
            if payload.len() < 12 {
                return Err(ProxyProtocolError::InvalidV2Header);
            }
            let source_ip = IpAddr::V4(Ipv4Addr::new(
                payload[0], payload[1], payload[2], payload[3],
            ));
            let destination_ip = IpAddr::V4(Ipv4Addr::new(
                payload[4], payload[5], payload[6], payload[7],
            ));
            let source_port = u16::from_be_bytes([payload[8], payload[9]]);
            let destination_port = u16::from_be_bytes([payload[10], payload[11]]);
            Ok(ParsedHeader {
                source_addr: SocketAddr::new(source_ip, source_port),
                destination_addr: Some(SocketAddr::new(destination_ip, destination_port)),
            })
        }
        0x2 => {
            if payload.len() < 36 {
                return Err(ProxyProtocolError::InvalidV2Header);
            }
            let source_ip = IpAddr::V6(Ipv6Addr::from(
                <[u8; 16]>::try_from(&payload[0..16])
                    .map_err(|_| ProxyProtocolError::InvalidV2Header)?,
            ));
            let destination_ip = IpAddr::V6(Ipv6Addr::from(
                <[u8; 16]>::try_from(&payload[16..32])
                    .map_err(|_| ProxyProtocolError::InvalidV2Header)?,
            ));
            let source_port = u16::from_be_bytes([payload[32], payload[33]]);
            let destination_port = u16::from_be_bytes([payload[34], payload[35]]);
            Ok(ParsedHeader {
                source_addr: SocketAddr::new(source_ip, source_port),
                destination_addr: Some(SocketAddr::new(destination_ip, destination_port)),
            })
        }
        _ => Err(ProxyProtocolError::UnsupportedProtocol),
    }
}

#[derive(Debug)]
struct ParsedHeader {
    source_addr: SocketAddr,
    destination_addr: Option<SocketAddr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyHeaderKind {
    V1,
    V2,
    NotProxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefixMatch {
    Complete(ProxyHeaderKind),
    NeedMore,
    NotProxy,
}

impl ParsedHeader {
    fn into_accepted(self, stream: TcpStream) -> AcceptedStream {
        AcceptedStream {
            stream,
            source_addr: self.source_addr,
            destination_addr: self.destination_addr,
            used_proxy_protocol: true,
        }
    }
}

#[derive(Debug)]
pub enum ProxyProtocolError {
    HeaderTooLong,
    InvalidMode { value: String },
    InvalidV1Header,
    InvalidV2Header,
    MissingHeader,
    UnsupportedProtocol,
    Io(TokioIoError),
}

impl fmt::Display for ProxyProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeaderTooLong => write!(f, "PROXY protocol v1 header is too long"),
            Self::InvalidMode { value } => write!(f, "invalid PROXY protocol mode: {value}"),
            Self::InvalidV1Header => write!(f, "invalid PROXY protocol v1 header"),
            Self::InvalidV2Header => write!(f, "invalid PROXY protocol v2 header"),
            Self::MissingHeader => write!(f, "missing PROXY protocol header"),
            Self::UnsupportedProtocol => write!(f, "unsupported PROXY protocol address family"),
            Self::Io(err) => write!(f, "PROXY protocol I/O error: {err}"),
        }
    }
}

impl std::error::Error for ProxyProtocolError {}

impl From<TokioIoError> for ProxyProtocolError {
    fn from(err: TokioIoError) -> Self {
        Self::Io(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_v1_tcp4_header() {
        let peer = SocketAddr::from(([10, 0, 0, 1], 1234));
        let parsed =
            parse_v1_header(b"PROXY TCP4 203.0.113.10 10.50.30.11 54321 20000\r\n", peer).unwrap();

        assert_eq!(
            parsed.source_addr,
            SocketAddr::from(([203, 0, 113, 10], 54321))
        );
        assert_eq!(
            parsed.destination_addr,
            Some(SocketAddr::from(([10, 50, 30, 11], 20000)))
        );
    }

    #[test]
    fn parses_v1_unknown_as_peer_addr() {
        let peer = SocketAddr::from(([10, 0, 0, 1], 1234));
        let parsed = parse_v1_header(b"PROXY UNKNOWN\r\n", peer).unwrap();

        assert_eq!(parsed.source_addr, peer);
        assert_eq!(parsed.destination_addr, None);
    }

    #[test]
    fn parses_v2_tcp4_header() {
        let peer = SocketAddr::from(([10, 0, 0, 1], 1234));
        let payload = [203, 0, 113, 10, 10, 50, 30, 11, 0xd4, 0x31, 0x4e, 0x20];
        let parsed = parse_v2_header(0x21, 0x11, &payload, peer).unwrap();

        assert_eq!(
            parsed.source_addr,
            SocketAddr::from(([203, 0, 113, 10], 54321))
        );
        assert_eq!(
            parsed.destination_addr,
            Some(SocketAddr::from(([10, 50, 30, 11], 20000)))
        );
    }

    #[test]
    fn parses_mode_values() {
        assert_eq!(
            "required".parse::<ProxyProtocolMode>().unwrap(),
            ProxyProtocolMode::Required
        );
        assert_eq!(
            "optional".parse::<ProxyProtocolMode>().unwrap(),
            ProxyProtocolMode::Optional
        );
        assert_eq!(
            "disabled".parse::<ProxyProtocolMode>().unwrap(),
            ProxyProtocolMode::Disabled
        );
    }

    #[test]
    fn classifies_fragmented_prefixes_without_rejecting_early() {
        assert_eq!(classify_proxy_prefix(b"P"), PrefixMatch::NeedMore);
        assert_eq!(
            classify_proxy_prefix(b"PROXY "),
            PrefixMatch::Complete(ProxyHeaderKind::V1)
        );
        assert_eq!(
            classify_proxy_prefix(b"PROXY TCP4 2"),
            PrefixMatch::Complete(ProxyHeaderKind::V1)
        );
        assert_eq!(
            classify_proxy_prefix(b"\r\n\r\n\0\r\n"),
            PrefixMatch::NeedMore
        );
        assert_eq!(
            classify_proxy_prefix(V2_SIGNATURE),
            PrefixMatch::Complete(ProxyHeaderKind::V2)
        );
        assert_eq!(classify_proxy_prefix(b"{\"id\":1"), PrefixMatch::NotProxy);
    }
}
