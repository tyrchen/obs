//! `from_env` plumbing — honour the standard OTel environment variables
//! (spec 20 § 4.1).

use std::{collections::BTreeMap, str::FromStr};

/// OTLP transport variants. Spec 20 § 4.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum OtlpProtocol {
    /// gRPC over TLS — the spec's recommended default.
    #[default]
    Grpc,
    /// HTTP+protobuf — for restricted networks.
    HttpProtobuf,
    /// HTTP+JSON — useful for dev / shells.
    HttpJson,
    /// Stdout/stderr JSON — for debugging the mapping.
    Stdout,
}

impl FromStr for OtlpProtocol {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "grpc" => Ok(Self::Grpc),
            "http/protobuf" | "http_protobuf" => Ok(Self::HttpProtobuf),
            "http/json" | "http_json" => Ok(Self::HttpJson),
            "stdout" | "debug" => Ok(Self::Stdout),
            other => Err(format!("unknown OTLP protocol `{other}`")),
        }
    }
}

/// Resolved OTLP endpoint.
#[derive(Debug, Clone)]
pub struct OtlpEndpoint {
    /// Endpoint URL.
    pub url: String,
    /// Transport protocol.
    pub protocol: OtlpProtocol,
    /// Per-RPC headers (auth, tenant).
    pub headers: BTreeMap<String, String>,
    /// Compression directive (`gzip`, `none`).
    pub compression: String,
    /// Per-request timeout (ms).
    pub timeout_ms: u64,
}

impl Default for OtlpEndpoint {
    fn default() -> Self {
        Self {
            url: "http://localhost:4317".to_string(),
            protocol: OtlpProtocol::default(),
            headers: BTreeMap::new(),
            compression: "gzip".to_string(),
            timeout_ms: 10_000,
        }
    }
}

/// Resolved Resource attributes (spec 20 § 2.1).
#[derive(Debug, Clone, Default)]
pub struct OtlpResourceAttrs {
    /// `service.name`.
    pub service_name: String,
    /// `service.version`.
    pub service_version: String,
    /// Other resource attributes.
    pub extra: BTreeMap<String, String>,
}

/// Read OTel env vars into a resolved endpoint config.
#[must_use]
pub fn endpoint_from_env() -> OtlpEndpoint {
    let mut e = OtlpEndpoint::default();
    if let Ok(url) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        && !url.is_empty()
    {
        e.url = url;
    }
    if let Ok(proto) = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        && let Ok(p) = OtlpProtocol::from_str(&proto)
    {
        e.protocol = p;
    }
    if let Ok(headers) = std::env::var("OTEL_EXPORTER_OTLP_HEADERS") {
        for pair in headers.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((k, v)) = pair.split_once('=') {
                e.headers.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    if let Ok(c) = std::env::var("OTEL_EXPORTER_OTLP_COMPRESSION")
        && !c.is_empty()
    {
        e.compression = c;
    }
    if let Ok(t) = std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT")
        && let Ok(ms) = t.parse::<u64>()
    {
        e.timeout_ms = ms;
    }
    e
}

/// Read OTel env vars into a resolved resource attribute set.
#[must_use]
pub fn resource_from_env() -> OtlpResourceAttrs {
    let mut r = OtlpResourceAttrs::default();
    if let Ok(name) = std::env::var("OTEL_SERVICE_NAME") {
        r.service_name = name;
    }
    if let Ok(extras) = std::env::var("OTEL_RESOURCE_ATTRIBUTES") {
        for pair in extras.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((k, v)) = pair.split_once('=') {
                let key = k.trim().to_string();
                let val = v.trim().to_string();
                if key == "service.name" && r.service_name.is_empty() {
                    r.service_name = val;
                } else if key == "service.version" && r.service_version.is_empty() {
                    r.service_version = val;
                } else {
                    r.extra.insert(key, val);
                }
            }
        }
    }
    r
}

/// Convenience: resolve all three sinks (logs, metrics, traces) from
/// env vars + defaults. Spec 20 § 4.3.
///
/// # Errors
///
/// Returns an error string when env-var parsing finds an invalid
/// protocol identifier.
pub fn otlp_trio_from_env() -> Result<
    (
        super::OtlpLogSink,
        super::OtlpMetricSink,
        super::OtlpTraceSink,
    ),
    String,
> {
    let endpoint = endpoint_from_env();
    let res = resource_from_env();
    let logs = super::OtlpLogSink::builder()
        .endpoint(endpoint.clone())
        .resource(res.clone())
        .build()
        .map_err(|e| format!("{e}"))?;
    let metrics = super::OtlpMetricSink::builder()
        .endpoint(endpoint.clone())
        .resource(res.clone())
        .build()
        .map_err(|e| format!("{e}"))?;
    let traces = super::OtlpTraceSink::builder()
        .endpoint(endpoint)
        .resource(res)
        .build()
        .map_err(|e| format!("{e}"))?;
    Ok((logs, metrics, traces))
}
