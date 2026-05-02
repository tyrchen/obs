//! `EventsConfig` — runtime-tunable configuration loaded from
//! `obs.yaml` and exposed via `ArcSwap` for live reload.
//!
//! Phase-1 surface implements the **shell** (a deserialisable
//! `EventsConfig` type that an observer can hold under `ArcSwap`);
//! file-watcher reload, env-var overlay, and SIGHUP wiring land in
//! Phase 3 sub-tasks. Defining the type now lets every later phase
//! consume the same struct shape rather than improvising. Spec 15.

use std::collections::BTreeMap;

use obs_types::Severity;
use serde::{Deserialize, Serialize};

/// The complete config tree. Every field is optional so a config file
/// can be a single line if the user only cares to override `filter`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct EventsConfig {
    /// EnvFilter-grammar directives. `None` ⇒ defer to `OBS_FILTER`
    /// env var; if both unset, `"info"` applies.
    #[serde(default)]
    pub filter: Option<String>,

    /// Head/tail sampling tunables.
    #[serde(default)]
    pub sampling: SamplingConfig,

    /// Per-event byte caps.
    #[serde(default)]
    pub limits: LimitsConfig,

    /// Per-tier mpsc queue capacities (Phase 3 worker pool).
    #[serde(default)]
    pub queues: QueuesConfig,

    /// Service identity (overrides defaults read from env).
    #[serde(default)]
    pub service: ServiceConfig,
}

impl EventsConfig {
    /// Builder entry. See spec 15 § 5.1.
    #[must_use]
    pub fn builder() -> EventsConfigBuilder {
        EventsConfigBuilder::default()
    }

    /// Validate ranges. Returns the first violation found, or `Ok` if
    /// the config is well-formed. Spec 15 § 6.
    ///
    /// # Errors
    ///
    /// Returns a `ConfigError` describing the first invalid setting.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(0.0..=1.0).contains(&self.sampling.default_rate) {
            return Err(ConfigError::InvalidRange {
                field: "sampling.default_rate",
                detail: "must be in [0.0, 1.0]",
            });
        }
        for (name, rate) in &self.sampling.per_event {
            if !(0.0..=1.0).contains(rate) {
                return Err(ConfigError::InvalidRange {
                    field: "sampling.per_event[..]",
                    detail: Box::leak(
                        format!("{name} = {rate} is outside [0.0, 1.0]").into_boxed_str(),
                    ),
                });
            }
        }
        if self.limits.max_payload_bytes < 1024 {
            return Err(ConfigError::InvalidRange {
                field: "limits.max_payload_bytes",
                detail: "must be ≥ 1 KiB",
            });
        }
        if self.limits.max_payload_bytes > 16 * 1024 * 1024 {
            return Err(ConfigError::InvalidRange {
                field: "limits.max_payload_bytes",
                detail: "must be ≤ 16 MiB",
            });
        }
        if self.queues.log < 64 || self.queues.metric < 64 || self.queues.trace < 64 {
            return Err(ConfigError::InvalidRange {
                field: "queues.{log,metric,trace}",
                detail: "must be ≥ 64",
            });
        }
        Ok(())
    }
}

/// Sampling config (spec 15 § 2 + spec 13 § 6).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct SamplingConfig {
    /// Default head-sample rate `[0.0, 1.0]`. 1.0 = keep everything.
    #[serde(default = "default_one_f64")]
    pub default_rate: f64,
    /// Per-event-name overrides. Key is `full_name`.
    #[serde(default)]
    pub per_event: BTreeMap<String, f64>,
    /// Severity floor that bypasses sampling.
    #[serde(default = "default_warn")]
    pub always_log_at_or_above: Severity,
    /// Tail-on-error buffer capacity per `obs::scope!` frame.
    #[serde(default = "default_64_u16")]
    pub tail_buffer_capacity: u16,
    /// Honour W3C `traceparent.sampled` from inbound HTTP. Default true.
    #[serde(default = "default_true")]
    pub honour_traceparent_sampled: bool,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            default_rate: default_one_f64(),
            per_event: BTreeMap::new(),
            always_log_at_or_above: default_warn(),
            tail_buffer_capacity: default_64_u16(),
            honour_traceparent_sampled: default_true(),
        }
    }
}

/// Per-event byte limits (spec 15 § 2 + spec 11 § 6.2).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct LimitsConfig {
    /// Per-event encoded payload cap. Default 256 KiB.
    #[serde(default = "default_256kib_u32")]
    pub max_payload_bytes: u32,
    /// Per-label-value byte cap. Default 1 KiB.
    #[serde(default = "default_1kib_u16")]
    pub max_label_value_bytes: u16,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: default_256kib_u32(),
            max_label_value_bytes: default_1kib_u16(),
        }
    }
}

/// Per-tier mpsc capacity (spec 15 § 2 + spec 11 § 4).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct QueuesConfig {
    /// LOG-tier mpsc capacity.
    #[serde(default = "default_8192_u32")]
    pub log: u32,
    /// METRIC-tier mpsc capacity.
    #[serde(default = "default_8192_u32")]
    pub metric: u32,
    /// TRACE-tier mpsc capacity.
    #[serde(default = "default_8192_u32")]
    pub trace: u32,
}

impl Default for QueuesConfig {
    fn default() -> Self {
        Self {
            log: default_8192_u32(),
            metric: default_8192_u32(),
            trace: default_8192_u32(),
        }
    }
}

/// Service identity (spec 15 § 2 + spec 11 § 7).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct ServiceConfig {
    /// Service name. Default reads `OTEL_SERVICE_NAME` env then
    /// `CARGO_PKG_NAME`.
    pub name: Option<String>,
    /// Version. Default reads `CARGO_PKG_VERSION`.
    pub version: Option<String>,
    /// Per-pod / per-host instance id. Default empty.
    pub instance: Option<String>,
    /// `service.namespace` for OTel Resource. Default empty.
    pub namespace: Option<String>,
    /// `deployment.environment` for OTel Resource. Default empty.
    pub environment: Option<String>,
    /// Free-form Resource extras.
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

/// Errors returned by [`EventsConfig::validate`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A numeric range constraint was violated.
    #[error("invalid range for `{field}`: {detail}")]
    InvalidRange {
        /// Dotted path to the offending field.
        field: &'static str,
        /// Human-readable detail.
        detail: &'static str,
    },
}

/// Builder for [`EventsConfig`].
#[derive(Debug, Default)]
pub struct EventsConfigBuilder {
    cfg: EventsConfig,
}

impl EventsConfigBuilder {
    /// Set the filter directive string.
    #[must_use]
    pub fn filter(mut self, s: impl Into<String>) -> Self {
        self.cfg.filter = Some(s.into());
        self
    }

    /// Replace the sampling config.
    #[must_use]
    pub fn sampling(mut self, s: SamplingConfig) -> Self {
        self.cfg.sampling = s;
        self
    }

    /// Replace the limits config.
    #[must_use]
    pub fn limits(mut self, l: LimitsConfig) -> Self {
        self.cfg.limits = l;
        self
    }

    /// Replace the queues config.
    #[must_use]
    pub fn queues(mut self, q: QueuesConfig) -> Self {
        self.cfg.queues = q;
        self
    }

    /// Replace the service config.
    #[must_use]
    pub fn service(mut self, s: ServiceConfig) -> Self {
        self.cfg.service = s;
        self
    }

    /// Finalize. Does not validate — call `EventsConfig::validate()`
    /// before installing.
    #[must_use]
    pub fn build(self) -> EventsConfig {
        self.cfg
    }
}

const fn default_one_f64() -> f64 {
    1.0
}
const fn default_true() -> bool {
    true
}
const fn default_warn() -> Severity {
    Severity::Warn
}
const fn default_64_u16() -> u16 {
    64
}
const fn default_256kib_u32() -> u32 {
    256 * 1024
}
const fn default_1kib_u16() -> u16 {
    1024
}
const fn default_8192_u32() -> u32 {
    8192
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_validate_default() {
        EventsConfig::default().validate().unwrap();
    }

    #[test]
    fn test_should_reject_bad_rate() {
        let mut cfg = EventsConfig::default();
        cfg.sampling.default_rate = 1.5;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_should_reject_tiny_payload_cap() {
        let mut cfg = EventsConfig::default();
        cfg.limits.max_payload_bytes = 100;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_should_round_trip_yaml() {
        let cfg = EventsConfig::builder()
            .filter("info")
            .sampling(SamplingConfig {
                default_rate: 0.5,
                ..Default::default()
            })
            .build();
        let s = serde_yaml::to_string(&cfg).unwrap();
        let cfg2: EventsConfig = serde_yaml::from_str(&s).unwrap();
        assert_eq!(cfg.filter, cfg2.filter);
        assert!((cfg.sampling.default_rate - cfg2.sampling.default_rate).abs() < f64::EPSILON);
    }

    #[test]
    fn test_should_reject_unknown_fields() {
        let yaml = "filter: info\nbogus_field: 42\n";
        let result: Result<EventsConfig, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err(), "unknown_fields must reject unknown keys");
    }
}
