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
///
/// Populated from `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES`
/// per the OTel SDK env-var spec, with semconv keys lifted into
/// first-class fields. Spec 93 P1-5.
#[derive(Debug, Clone, Default)]
pub struct OtlpResourceAttrs {
    /// `service.name`.
    pub service_name: String,
    /// `service.version`.
    pub service_version: String,
    /// `service.namespace` — logical service grouping (e.g. `payments`).
    pub service_namespace: String,
    /// `service.instance.id` — unique per process/replica.
    pub service_instance_id: String,
    /// `deployment.environment` — `production`, `staging`, `dev`, …
    pub deployment_environment: String,
    /// `host.name` — typically the OS hostname.
    pub host_name: String,
    /// `host.arch` — `amd64`, `arm64`, …
    pub host_arch: String,
    /// Any other attributes from `OTEL_RESOURCE_ATTRIBUTES` that did
    /// not land in a first-class slot.
    pub extra: BTreeMap<String, String>,
}

impl From<&obs_core::ResourceAttrs> for OtlpResourceAttrs {
    /// Project the workspace-shared `ResourceAttrs` (held on the
    /// observer per spec 94 § 2.7) into the OTLP-flavoured shape this
    /// crate uses on the wire. Spec 94 P1-E.
    fn from(r: &obs_core::ResourceAttrs) -> Self {
        Self {
            service_name: r.service_name.clone(),
            service_version: r.service_version.clone(),
            service_namespace: r.service_namespace.clone(),
            service_instance_id: r.service_instance_id.clone(),
            deployment_environment: r.deployment_environment.clone(),
            host_name: r.host_name.clone(),
            host_arch: r.host_arch.clone(),
            extra: r.extra.clone(),
        }
    }
}

impl OtlpResourceAttrs {
    /// Render this attribute set as a `BTreeMap<String, String>` where
    /// every populated first-class slot becomes the corresponding
    /// semconv key. Used to build the OTLP `Resource` projection.
    #[must_use]
    pub fn to_semconv_map(&self) -> BTreeMap<String, String> {
        let mut m = self.extra.clone();
        if !self.service_name.is_empty() {
            m.insert("service.name".to_string(), self.service_name.clone());
        }
        if !self.service_version.is_empty() {
            m.insert("service.version".to_string(), self.service_version.clone());
        }
        if !self.service_namespace.is_empty() {
            m.insert(
                "service.namespace".to_string(),
                self.service_namespace.clone(),
            );
        }
        if !self.service_instance_id.is_empty() {
            m.insert(
                "service.instance.id".to_string(),
                self.service_instance_id.clone(),
            );
        }
        if !self.deployment_environment.is_empty() {
            m.insert(
                "deployment.environment".to_string(),
                self.deployment_environment.clone(),
            );
        }
        if !self.host_name.is_empty() {
            m.insert("host.name".to_string(), self.host_name.clone());
        }
        if !self.host_arch.is_empty() {
            m.insert("host.arch".to_string(), self.host_arch.clone());
        }
        m
    }
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
///
/// Honours `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` per the
/// OTel SDK env-var spec. Lifts the well-known semconv keys
/// (`service.namespace`, `service.instance.id`, `deployment.environment`,
/// `host.name`, `host.arch`) into first-class fields on
/// [`OtlpResourceAttrs`]. Spec 93 P1-5.
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
                let key = k.trim();
                let val = v.trim().to_string();
                match key {
                    "service.name" if r.service_name.is_empty() => r.service_name = val,
                    "service.version" if r.service_version.is_empty() => r.service_version = val,
                    "service.namespace" if r.service_namespace.is_empty() => {
                        r.service_namespace = val;
                    }
                    "service.instance.id" if r.service_instance_id.is_empty() => {
                        r.service_instance_id = val;
                    }
                    "deployment.environment" if r.deployment_environment.is_empty() => {
                        r.deployment_environment = val;
                    }
                    "host.name" if r.host_name.is_empty() => r.host_name = val,
                    "host.arch" if r.host_arch.is_empty() => r.host_arch = val,
                    _ => {
                        r.extra.insert(key.to_string(), val);
                    }
                }
            }
        }
    }
    // Auto-detect `host.arch` if unset — `std::env::consts::ARCH` is a
    // stable mapping (`x86_64`, `aarch64`, …) that we map to OTel's
    // semconv values.
    if r.host_arch.is_empty() {
        r.host_arch = match std::env::consts::ARCH {
            "x86_64" => "amd64".to_string(),
            "aarch64" => "arm64".to_string(),
            other => other.to_string(),
        };
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
