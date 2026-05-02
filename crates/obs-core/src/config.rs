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

    /// AUDIT-tier delivery policy (Phase 3 task 3.12 implements the
    /// spool; the config struct lives here so user `obs.yaml` files
    /// already have a stable shape).
    #[serde(default)]
    pub audit: AuditConfig,

    /// Per-tier mpsc queue capacities (Phase 3 worker pool).
    #[serde(default)]
    pub queues: QueuesConfig,

    /// Per-sink configuration (Phase 3+ implements the sinks; the
    /// config struct lives here so user `obs.yaml` files already have
    /// a stable shape).
    #[serde(default)]
    pub sinks: SinksConfig,

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
            return Err(ConfigError::invalid_range(
                "sampling.default_rate",
                "must be in [0.0, 1.0]",
            ));
        }
        for (name, rate) in &self.sampling.per_event {
            if !(0.0..=1.0).contains(rate) {
                return Err(ConfigError::invalid_range(
                    "sampling.per_event[..]",
                    format!("{name} = {rate} is outside [0.0, 1.0]"),
                ));
            }
        }
        if self.limits.max_payload_bytes < 1024 {
            return Err(ConfigError::invalid_range(
                "limits.max_payload_bytes",
                "must be ≥ 1 KiB",
            ));
        }
        if self.limits.max_payload_bytes > 16 * 1024 * 1024 {
            return Err(ConfigError::invalid_range(
                "limits.max_payload_bytes",
                "must be ≤ 16 MiB",
            ));
        }
        if self.queues.log < 64 || self.queues.metric < 64 || self.queues.trace < 64 {
            return Err(ConfigError::invalid_range(
                "queues.{log,metric,trace}",
                "must be ≥ 64",
            ));
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

/// AUDIT-tier delivery policy. Phase-1 ships only the type shape so
/// `obs.yaml` files already validate; the runtime implementation
/// (bounded blocking + binary spool + recovery) lands in Phase 3
/// task 3.12. Spec 11 § 6.4 + spec 15 § 2.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct AuditConfig {
    /// Channel capacity for the AUDIT tier worker. Default 1024.
    #[serde(default = "default_1024_u32")]
    pub channel_capacity: u32,
    /// Bounded blocking on emit when AUDIT channel is full (ms).
    #[serde(default = "default_100_u32")]
    pub block_ms_max: u32,
    /// After this duration of channel-full, switch to disk spool (ms).
    #[serde(default = "default_250_u32")]
    pub spool_after_ms: u32,
    /// Spool directory; created if absent.
    #[serde(default = "default_audit_dir")]
    pub spool_dir: std::path::PathBuf,
    /// Cap total spool size on disk (bytes).
    #[serde(default = "default_1gib")]
    pub spool_max_bytes: u64,
    /// On-failure behaviour when spool is unwritable.
    #[serde(default)]
    pub on_failure: AuditFailureMode,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            channel_capacity: default_1024_u32(),
            block_ms_max: default_100_u32(),
            spool_after_ms: default_250_u32(),
            spool_dir: default_audit_dir(),
            spool_max_bytes: default_1gib(),
            on_failure: AuditFailureMode::default(),
        }
    }
}

/// Behaviour when AUDIT delivery cannot complete (spec 11 § 6.4).
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditFailureMode {
    /// Production default: panic so the supervisor restarts the process.
    #[default]
    Panic,
    /// `process::abort()`; tighter than `panic` for compliance shops.
    Abort,
    /// Dev only: log a warning and drop. Compliance failure.
    WarnOnly,
}

/// Per-sink configuration. Phase-1 ships only the type shape so
/// `obs.yaml` files already validate; the per-sink fields are filled
/// in by their respective Phase-3+ implementations. Spec 15 § 2 + spec
/// 20 / spec 22.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct SinksConfig {
    /// Stdout sink — opaque map until Phase 3 task 3.7 lands the typed
    /// schema. We accept anything (`serde_json::Value`) so users can
    /// already write `sinks.stdout.style: full` without a config-load
    /// error.
    #[serde(default)]
    pub stdout: serde_json::Value,
    /// OTLP sinks (logs/metrics/traces). Phase 3 task 3.8 lands the
    /// typed schema.
    #[serde(default)]
    pub otlp: serde_json::Value,
    /// NDJSON file sink. Phase 3 task 3.7 lands the typed schema.
    #[serde(default)]
    pub ndjson: serde_json::Value,
    /// Parquet sink. Phase 4A task 4A.2 lands the typed schema.
    #[serde(default)]
    pub parquet: serde_json::Value,
    /// ClickHouse sink. Phase 4A task 4A.3 lands the typed schema.
    #[serde(default)]
    pub clickhouse: serde_json::Value,
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
        /// Human-readable detail (heap-owned to support runtime-formatted
        /// messages without leaking via `Box::leak`).
        detail: String,
    },
}

impl ConfigError {
    /// Convenience constructor for [`ConfigError::InvalidRange`]; takes
    /// either a `&'static str` or a `String` for the detail.
    pub(crate) fn invalid_range(field: &'static str, detail: impl Into<String>) -> Self {
        Self::InvalidRange {
            field,
            detail: detail.into(),
        }
    }
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

    /// Replace the AUDIT-tier config.
    #[must_use]
    pub fn audit(mut self, a: AuditConfig) -> Self {
        self.cfg.audit = a;
        self
    }

    /// Replace the per-sink config.
    #[must_use]
    pub fn sinks(mut self, s: SinksConfig) -> Self {
        self.cfg.sinks = s;
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
const fn default_100_u32() -> u32 {
    100
}
const fn default_250_u32() -> u32 {
    250
}
const fn default_1024_u32() -> u32 {
    1024
}
const fn default_1gib() -> u64 {
    1 << 30
}
fn default_audit_dir() -> std::path::PathBuf {
    std::path::PathBuf::from("./obs-audit-spool")
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
