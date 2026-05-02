//! Per-batch OTLP log payload.

use serde::{Deserialize, Serialize};

use crate::{
    env_config::{OtlpEndpoint, OtlpResourceAttrs},
    mapping::{LogRecord, project_log},
};

/// Resource + per-record list. Mirrors OTLP `LogsData` -> `ResourceLogs`
/// shape; serialised as JSON for the default stdout exporter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpLogPayload {
    /// Resource (`service.name`, etc.).
    pub resource: ResourceMessage,
    /// Endpoint URL the payload would have been sent to.
    pub endpoint: String,
    /// Per-record list.
    pub records: Vec<LogRecord>,
}

/// Embedded resource message — separate from `mapping::ResourceMessage`
/// because we want a per-payload snapshot that does not bind to the
/// observer's live `ArcSwap`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMessage {
    /// `service.name`.
    pub service_name: String,
    /// `service.version`.
    pub service_version: String,
    /// Other attributes.
    pub extra: std::collections::BTreeMap<String, String>,
    /// OTel semconv URL the sink targets.
    pub schema_url: String,
}

impl OtlpLogPayload {
    /// Project a batch of envelopes into the OTLP log payload shape.
    #[must_use]
    pub fn from_envelopes(
        envs: &[obs_proto::obs::v1::ObsEnvelope],
        resource: &OtlpResourceAttrs,
        endpoint: &OtlpEndpoint,
    ) -> Self {
        Self {
            resource: ResourceMessage {
                service_name: resource.service_name.clone(),
                service_version: resource.service_version.clone(),
                extra: resource.extra.clone(),
                schema_url: "https://opentelemetry.io/schemas/1.27.0".to_string(),
            },
            endpoint: endpoint.url.clone(),
            records: envs.iter().map(project_log).collect(),
        }
    }
}
