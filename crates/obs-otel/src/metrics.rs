//! Per-batch OTLP metric payload. Spec 20 § 2.4 / spec 93 P1-6.

use std::collections::BTreeMap;

use obs_core::{MetricEmitter, SchemaRegistry};
use serde::{Deserialize, Serialize};

use crate::{
    env_config::{OtlpEndpoint, OtlpResourceAttrs},
    logs::ResourceMessage,
    mapping::MetricPoint,
};

/// `MetricsData`-shape payload.
///
/// For each envelope, the registered schema's `project_metrics` is
/// invoked with the (post-scrub) payload bytes. Each emitted metric
/// becomes one `MetricPoint`. Envelopes whose schema is not registered
/// (or that have no MEASUREMENT fields) fall back to a single
/// `<full_name>.count = 1` counter so observability still surfaces the
/// volume.
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
    /// Project envelopes through their registered schema's
    /// `project_metrics`. Per-envelope label set is captured into the
    /// emitted `MetricPoint`s. Spec 93 P1-6.
    #[must_use]
    pub fn from_envelopes(
        envs: &[obs_proto::obs::v1::ObsEnvelope],
        resource: &OtlpResourceAttrs,
        endpoint: &OtlpEndpoint,
        registry: &SchemaRegistry,
    ) -> Self {
        let mut points: Vec<MetricPoint> = Vec::with_capacity(envs.len());
        for env in envs {
            let mut attrs: BTreeMap<String, String> = BTreeMap::new();
            for (k, v) in env.labels.iter() {
                attrs.insert(k.clone(), v.clone());
            }
            attrs.insert("event.name".to_string(), env.full_name.clone());

            let mut emitter = CollectingEmitter::new(env.full_name.clone(), attrs.clone());
            let mut emitted_any = false;
            if let Some(schema) = registry.lookup(env)
                && schema.project_metrics(&env.payload, &mut emitter).is_ok()
            {
                emitted_any = !emitter.points.is_empty();
            }
            if emitted_any {
                points.extend(emitter.points);
            } else {
                // Fallback: schema unknown or no MEASUREMENT fields.
                points.push(MetricPoint {
                    instrument: format!("{}.count", env.full_name),
                    unit: "1".to_string(),
                    kind: "counter".to_string(),
                    attributes: attrs,
                    value_u64: Some(1),
                    bounds: Vec::new(),
                });
            }
        }
        Self {
            resource: ResourceMessage::from_attrs(resource),
            endpoint: endpoint.url.clone(),
            points,
        }
    }
}

/// `MetricEmitter` that collects records into a `Vec<MetricPoint>`.
struct CollectingEmitter {
    full_name: String,
    attributes: BTreeMap<String, String>,
    points: Vec<MetricPoint>,
}

impl CollectingEmitter {
    fn new(full_name: String, attributes: BTreeMap<String, String>) -> Self {
        Self {
            full_name,
            attributes,
            points: Vec::new(),
        }
    }

    fn instrument(&self, field: &str) -> String {
        format!("{}.{}", self.full_name, field)
    }
}

impl MetricEmitter for CollectingEmitter {
    fn record_counter(&mut self, instrument: &'static str, value: u64, unit: Option<&'static str>) {
        self.points.push(MetricPoint {
            instrument: self.instrument(instrument),
            unit: unit.unwrap_or("1").to_string(),
            kind: "counter".to_string(),
            attributes: self.attributes.clone(),
            value_u64: Some(value),
            bounds: Vec::new(),
        });
    }

    fn record_gauge_u64(
        &mut self,
        instrument: &'static str,
        value: u64,
        unit: Option<&'static str>,
    ) {
        self.points.push(MetricPoint {
            instrument: self.instrument(instrument),
            unit: unit.unwrap_or("1").to_string(),
            kind: "gauge".to_string(),
            attributes: self.attributes.clone(),
            value_u64: Some(value),
            bounds: Vec::new(),
        });
    }

    fn record_gauge_f64(
        &mut self,
        instrument: &'static str,
        value: f64,
        unit: Option<&'static str>,
    ) {
        // OTLP gauges support double via a separate field; the
        // `MetricPoint` shape carries a u64 slot only — we round.
        self.points.push(MetricPoint {
            instrument: self.instrument(instrument),
            unit: unit.unwrap_or("1").to_string(),
            kind: "gauge".to_string(),
            attributes: self.attributes.clone(),
            value_u64: Some(value.round() as u64),
            bounds: Vec::new(),
        });
    }

    fn record_histogram(
        &mut self,
        instrument: &'static str,
        value: f64,
        unit: Option<&'static str>,
        bounds: &'static [f64],
    ) {
        self.points.push(MetricPoint {
            instrument: self.instrument(instrument),
            unit: unit.unwrap_or("1").to_string(),
            kind: "histogram".to_string(),
            attributes: self.attributes.clone(),
            value_u64: Some(value.round() as u64),
            bounds: bounds.to_vec(),
        });
    }

    fn with_attributes(&mut self, attrs: &[(&'static str, &str)]) {
        for (k, v) in attrs {
            self.attributes.insert((*k).to_string(), (*v).to_string());
        }
    }
}
