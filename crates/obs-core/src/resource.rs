//! `ResourceAttrs` — workspace-shared resource attribute set held by
//! the observer. Spec 20 § 2.1 / spec 94 § 2.7 / P1-E.
//!
//! The observer owns one `Arc<ArcSwap<ResourceAttrs>>` so OTLP /
//! Parquet / ClickHouse sinks can read the live snapshot at flush
//! time. Phase-7 work moved this off the per-sink `OtlpResourceAttrs`
//! so a config reload re-projects every sink without redeploying.

use std::collections::BTreeMap;

/// Resource attribute set carrying the OTel semantic-convention keys
/// every sink projects onto its outbound batch (`resource.attributes`
/// for OTLP, partition columns for Parquet, etc.).
#[derive(Debug, Clone, Default)]
pub struct ResourceAttrs {
    /// `service.name` — typically `OTEL_SERVICE_NAME` or the observer's
    /// configured service identity.
    pub service_name: String,
    /// `service.version`.
    pub service_version: String,
    /// `service.namespace` — logical service grouping (e.g. `payments`).
    pub service_namespace: String,
    /// `service.instance.id` — unique per process / replica.
    pub service_instance_id: String,
    /// `deployment.environment` — `production`, `staging`, `dev`, …
    pub deployment_environment: String,
    /// `host.name`.
    pub host_name: String,
    /// `host.arch` — `amd64`, `arm64`, …
    pub host_arch: String,
    /// Any additional `OTEL_RESOURCE_ATTRIBUTES` pairs that did not
    /// land in a first-class slot.
    pub extra: BTreeMap<String, String>,
}

impl ResourceAttrs {
    /// Render the populated semconv keys as a flat `BTreeMap`. Useful
    /// for sinks that project the attributes into a wire-format
    /// `KeyValueList`.
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
