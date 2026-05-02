//! Per-batch OTLP metric payload. Spec 20 § 2.4.

use serde::{Deserialize, Serialize};

use crate::{
    env_config::{OtlpEndpoint, OtlpResourceAttrs},
    logs::ResourceMessage,
    mapping::MetricPoint,
};

/// `MetricsData`-shape payload. The Phase-3 implementation builds a
/// minimal projection — one point per envelope label-set — pending the
/// codegen `project_metrics` impl that lands in Phase 2 task 2.1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpMetricPayload {
    /// Resource attrs.
    pub resource: ResourceMessage,
    /// Endpoint URL the payload would have been sent to.
    pub endpoint: String,
    /// Per-record points.
    pub points: Vec<MetricPoint>,
}

impl OtlpMetricPayload {
    /// Project envelopes; for measurement-bearing schemas, one
    /// counter point is generated with the envelope label-set.
    #[must_use]
    pub fn from_envelopes(
        envs: &[obs_proto::obs::v1::ObsEnvelope],
        resource: &OtlpResourceAttrs,
        endpoint: &OtlpEndpoint,
    ) -> Self {
        let mut points = Vec::with_capacity(envs.len());
        for env in envs {
            let mut attrs = std::collections::BTreeMap::new();
            for (k, v) in env.labels.iter() {
                attrs.insert(k.clone(), v.clone());
            }
            attrs.insert("event.name".to_string(), env.full_name.clone());
            points.push(MetricPoint {
                instrument: format!("{}.count", env.full_name),
                unit: "1".to_string(),
                kind: "counter".to_string(),
                attributes: attrs,
                value_u64: Some(1),
                bounds: Vec::new(),
            });
        }
        Self {
            resource: ResourceMessage {
                service_name: resource.service_name.clone(),
                service_version: resource.service_version.clone(),
                extra: resource.extra.clone(),
                schema_url: "https://opentelemetry.io/schemas/1.27.0".to_string(),
            },
            endpoint: endpoint.url.clone(),
            points,
        }
    }
}
