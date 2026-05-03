//! Transport abstraction — spec 22 § 3.1 batched INSERT.
//!
//! Spec 22 § 3 calls for native ClickHouse insertion; we keep the wire
//! protocol pluggable so users can plug in `clickhouse-rs`,
//! `clickhouse-client`, or a managed cloud endpoint without forking
//! the sink. The default in-tree transport is the HTTP+JSONEachRow
//! interface speaking to ClickHouse's `/?query=...` endpoint.

use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::Mutex,
    time::Duration,
};

/// One batch's worth of rows, already serialised as one
/// JSON-Each-Row-formatted body (one row per line).
#[derive(Debug, Clone)]
pub struct ClickHouseBatch {
    /// Database name (default `default`).
    pub database: String,
    /// Table name (default `obs_events`).
    pub table: String,
    /// `JSONEachRow` body. Each line is one INSERT row.
    pub body: Vec<u8>,
    /// Number of rows the body represents (used for error metrics).
    pub row_count: usize,
}

/// Pluggable transport.
pub trait ClickHouseTransport: Send + Sync + std::fmt::Debug {
    /// Send the DDL (executed for `CREATE TABLE` migrations).
    ///
    /// # Errors
    ///
    /// Returns transport-specific failure.
    fn execute_ddl(&self, ddl: &str) -> Result<(), TransportError>;

    /// Send one batch of rows.
    ///
    /// # Errors
    ///
    /// Returns transport-specific failure.
    fn insert_batch(&self, batch: &ClickHouseBatch) -> Result<(), TransportError>;
}

/// Transport error — wrapped per the trait contract.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// I/O failure communicating with the server.
    #[error("io: {0}")]
    Io(String),
    /// Server returned non-200 / non-OK.
    #[error("server: status={status}, body={body}")]
    Server {
        /// HTTP / native status code.
        status: u32,
        /// Body payload (truncated to 1024 bytes).
        body: String,
    },
    /// Configuration is incomplete or invalid.
    #[error("config: {0}")]
    Config(String),
}

/// Recording transport — captures DDL + batches in memory. Used by
/// tests and `obs migrate clickhouse --dry-run`.
#[derive(Debug, Default)]
pub struct RecordingTransport {
    inner: Mutex<RecordingState>,
}

#[derive(Debug, Default)]
struct RecordingState {
    pub ddls: Vec<String>,
    pub batches: Vec<ClickHouseBatch>,
}

impl RecordingTransport {
    /// New empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of recorded batches.
    #[must_use]
    pub fn batches(&self) -> Vec<ClickHouseBatch> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .batches
            .clone()
    }

    /// Snapshot of executed DDL statements.
    #[must_use]
    pub fn ddls(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .ddls
            .clone()
    }
}

impl ClickHouseTransport for RecordingTransport {
    fn execute_ddl(&self, ddl: &str) -> Result<(), TransportError> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.ddls.push(ddl.to_string());
        Ok(())
    }

    fn insert_batch(&self, batch: &ClickHouseBatch) -> Result<(), TransportError> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.batches.push(batch.clone());
        Ok(())
    }
}

/// HTTP transport — speaks ClickHouse's HTTP interface using
/// JSONEachRow. The implementation deliberately uses raw `TcpStream`
/// to avoid pulling in `reqwest` / `hyper` — spec 22 § 3 does not
/// constrain the wire protocol, only the table shape.
#[derive(Debug)]
pub struct HttpClickHouseTransport {
    host: String,
    port: u16,
    user: Option<String>,
    password: Option<String>,
    timeout: Duration,
}

impl HttpClickHouseTransport {
    /// Construct from a `clickhouse://` URL.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Config`] when the URL is malformed.
    pub fn from_url(url: &str) -> Result<Self, TransportError> {
        let trimmed = url
            .strip_prefix("clickhouse://")
            .or_else(|| url.strip_prefix("http://"))
            .ok_or_else(|| TransportError::Config(format!("unsupported scheme in `{url}`")))?;
        let (creds_host, _path) = match trimmed.find('/') {
            Some(i) => (&trimmed[..i], &trimmed[i..]),
            None => (trimmed, ""),
        };
        let (creds, host_port) = match creds_host.rfind('@') {
            Some(i) => (Some(&creds_host[..i]), &creds_host[i + 1..]),
            None => (None, creds_host),
        };
        let (host, port) = match host_port.rfind(':') {
            Some(i) => {
                let port: u16 = host_port[i + 1..]
                    .parse()
                    .map_err(|e| TransportError::Config(format!("bad port: {e}")))?;
                (host_port[..i].to_string(), port)
            }
            None => (host_port.to_string(), 8123),
        };
        let (user, password) = match creds {
            None => (None, None),
            Some(c) => match c.find(':') {
                None => (Some(c.to_string()), None),
                Some(i) => (Some(c[..i].to_string()), Some(c[i + 1..].to_string())),
            },
        };
        Ok(Self {
            host,
            port,
            user,
            password,
            timeout: Duration::from_secs(30),
        })
    }

    /// Set request timeout.
    #[must_use]
    pub fn with_timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
        self
    }

    fn send_query(&self, query: &str, body: &[u8]) -> Result<(), TransportError> {
        let addr = format!("{}:{}", self.host, self.port);
        let mut stream = TcpStream::connect_timeout(
            &addr
                .parse()
                .map_err(|e: std::net::AddrParseError| TransportError::Io(e.to_string()))?,
            self.timeout,
        )
        .map_err(|e| TransportError::Io(e.to_string()))?;
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(|e| TransportError::Io(e.to_string()))?;
        stream
            .set_write_timeout(Some(self.timeout))
            .map_err(|e| TransportError::Io(e.to_string()))?;

        let mut req = Vec::new();
        let escaped_query = url_encode(query);
        req.extend_from_slice(b"POST /?query=");
        req.extend_from_slice(escaped_query.as_bytes());
        req.extend_from_slice(b" HTTP/1.1\r\n");
        req.extend_from_slice(b"Host: ");
        req.extend_from_slice(self.host.as_bytes());
        req.extend_from_slice(b"\r\n");
        req.extend_from_slice(b"Content-Type: application/json\r\n");
        if let Some(user) = &self.user {
            req.extend_from_slice(b"X-ClickHouse-User: ");
            req.extend_from_slice(user.as_bytes());
            req.extend_from_slice(b"\r\n");
        }
        if let Some(password) = &self.password {
            req.extend_from_slice(b"X-ClickHouse-Key: ");
            req.extend_from_slice(password.as_bytes());
            req.extend_from_slice(b"\r\n");
        }
        req.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
        req.extend_from_slice(b"Connection: close\r\n\r\n");
        req.extend_from_slice(body);

        stream
            .write_all(&req)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        let mut resp = Vec::with_capacity(1024);
        stream
            .read_to_end(&mut resp)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        // Parse status line.
        let header_end = resp
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| TransportError::Io("missing http header terminator".into()))?;
        let head = std::str::from_utf8(&resp[..header_end]).unwrap_or("");
        let status_line = head.lines().next().unwrap_or("");
        let mut parts = status_line.split_whitespace();
        let _http = parts.next();
        let status: u32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let body_bytes = &resp[header_end + 4..];
        let body_str = String::from_utf8_lossy(body_bytes);
        if !(200..300).contains(&status) {
            return Err(TransportError::Server {
                status,
                body: body_str.chars().take(1024).collect(),
            });
        }
        Ok(())
    }
}

impl ClickHouseTransport for HttpClickHouseTransport {
    fn execute_ddl(&self, ddl: &str) -> Result<(), TransportError> {
        self.send_query(ddl, &[])
    }

    fn insert_batch(&self, batch: &ClickHouseBatch) -> Result<(), TransportError> {
        let q = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            batch.database, batch.table
        );
        self.send_query(&q, &batch.body)
    }
}

fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 16);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recording_transport_should_capture() {
        let t = RecordingTransport::new();
        t.execute_ddl("CREATE TABLE foo()").expect("ddl");
        t.insert_batch(&ClickHouseBatch {
            database: "default".into(),
            table: "obs_events".into(),
            body: b"{}".to_vec(),
            row_count: 1,
        })
        .expect("batch");
        assert_eq!(t.ddls().len(), 1);
        assert_eq!(t.batches().len(), 1);
    }

    #[test]
    fn test_url_should_parse_with_creds_and_port() {
        let t = HttpClickHouseTransport::from_url("clickhouse://user:pass@host:9000/db")
            .expect("parse");
        assert_eq!(t.host, "host");
        assert_eq!(t.port, 9000);
        assert_eq!(t.user.as_deref(), Some("user"));
        assert_eq!(t.password.as_deref(), Some("pass"));
    }

    #[test]
    fn test_url_should_default_port_when_omitted() {
        let t = HttpClickHouseTransport::from_url("clickhouse://host/").expect("parse");
        assert_eq!(t.port, 8123);
        assert!(t.user.is_none());
    }

    #[test]
    fn test_url_should_reject_unsupported_scheme() {
        let err = HttpClickHouseTransport::from_url("https://h").expect_err("err");
        assert!(matches!(err, TransportError::Config(_)));
    }
}
