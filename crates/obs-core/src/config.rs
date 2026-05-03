//! `EventsConfig` — runtime-tunable configuration loaded from
//! `obs.yaml` and exposed via `ArcSwap` for live reload. Spec 15 +
//! spec 93 P0-9.
//!
//! The loader uses synchronous `std::fs` because config loading runs
//! once at startup (and on SIGHUP / file-watcher events on cold-ish
//! paths); switching to `tokio::fs` would require either an async
//! constructor or a `block_on` round-trip. The crate's clippy lint
//! against `std::fs` is intentionally allowed here for that reason.
#![allow(clippy::disallowed_methods)]

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

    /// Dev-mode toggle (`OBS_DEV=1` or `dev_mode: true`). Enables
    /// extra diagnostics intended for local iteration: more verbose
    /// scope-field warnings, source-loc capture, and the
    /// `dev_ergonomics` test path. Spec 13 § 2.3 / 60 § 7 / spec 94 §
    /// 3.10 / P3-A.
    #[serde(default)]
    pub dev_mode: bool,
}

impl EventsConfig {
    /// Builder entry. See spec 15 § 5.1.
    #[must_use]
    pub fn builder() -> EventsConfigBuilder {
        EventsConfigBuilder::default()
    }

    /// Parse YAML bytes into an [`EventsConfig`]. Spec 15 § 5.1 / spec
    /// 93 P0-9. `${VAR}` references in scalar string values are
    /// expanded against the process environment before parsing — set
    /// `${VAR:-default}` to provide a fallback.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError::Yaml` when parsing fails (unknown fields,
    /// type mismatch, syntax error). Validation is left to the caller
    /// — call [`Self::validate`] after loading.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, ConfigError> {
        let expanded = expand_env_vars(yaml);
        serde_yaml::from_str(&expanded).map_err(|e| ConfigError::Yaml(e.to_string()))
    }

    /// Read a YAML file from disk.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError::Io` when the file cannot be read or
    /// `ConfigError::Yaml` when parsing fails.
    pub fn from_yaml_path(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let bytes = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(format!("{}: {}", path.display(), e)))?;
        Self::from_yaml_str(&bytes)
    }

    /// Apply environment-variable overrides under `prefix`. The
    /// convention is `<PREFIX>_<DOTTED_PATH_WITH___INSTEAD_OF_DOTS>` —
    /// e.g. `OBS_FILTER` for `filter`, `OBS_AUDIT__SPOOL_DIR` for
    /// `audit.spool_dir`, `OBS_SAMPLING__DEFAULT_RATE` for
    /// `sampling.default_rate`. Values are parsed by re-running
    /// serde_yaml against the resulting flat map. Spec 15 § 5.1 / spec
    /// 93 P0-9.
    #[must_use]
    pub fn merged_with_env(self, prefix: &str) -> Self {
        let mut overlay = serde_yaml::to_value(&self).unwrap_or(serde_yaml::Value::Null);
        let prefix_uc = prefix.to_ascii_uppercase();
        let prefix_with_under = format!("{prefix_uc}_");
        for (key, value) in std::env::vars() {
            if !key.starts_with(&prefix_with_under) {
                continue;
            }
            let stripped = match key.strip_prefix(&prefix_with_under) {
                Some(s) => s,
                None => continue,
            };
            // `__` separator → nested path; `_` keeps the field name as-is.
            let path: Vec<String> = stripped
                .split("__")
                .map(|seg| seg.to_ascii_lowercase())
                .collect();
            apply_yaml_path(&mut overlay, &path, &value);
        }
        serde_yaml::from_value::<EventsConfig>(overlay).unwrap_or(self)
    }
}

fn apply_yaml_path(root: &mut serde_yaml::Value, path: &[String], value: &str) {
    let Some((head, tail)) = path.split_first() else {
        return;
    };
    if !root.is_mapping() {
        *root = serde_yaml::Value::Mapping(Default::default());
    }
    let Some(map) = root.as_mapping_mut() else {
        return;
    };
    let key = serde_yaml::Value::String(head.clone());
    if tail.is_empty() {
        // Try to interpret as YAML scalar so booleans / numbers parse;
        // fall back to a plain string.
        let parsed: serde_yaml::Value = serde_yaml::from_str(value)
            .unwrap_or_else(|_| serde_yaml::Value::String(value.to_string()));
        map.insert(key, parsed);
    } else {
        let entry = map
            .entry(key)
            .or_insert_with(|| serde_yaml::Value::Mapping(Default::default()));
        apply_yaml_path(entry, tail, value);
    }
}

/// Expand `${VAR}` and `${VAR:-default}` references against the
/// process environment. Unknown references with no default are left
/// in place verbatim.
fn expand_env_vars(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let Some(&b) = bytes.get(i) else { break };
        if b == b'$'
            && bytes.get(i + 1) == Some(&b'{')
            && let Some(end) = bytes
                .iter()
                .skip(i + 2)
                .position(|&c| c == b'}')
                .map(|n| n + i + 2)
        {
            let Some(inner) = input.get(i + 2..end) else {
                out.push(b as char);
                i += 1;
                continue;
            };
            let (name, default) = match inner.split_once(":-") {
                Some((n, d)) => (n, Some(d)),
                None => (inner, None),
            };
            let resolved = std::env::var(name)
                .ok()
                .or_else(|| default.map(str::to_string));
            if let Some(v) = resolved {
                out.push_str(&v);
            } else {
                let Some(span) = input.get(i..=end) else {
                    break;
                };
                out.push_str(span);
            }
            i = end + 1;
            continue;
        }
        out.push(b as char);
        i += 1;
    }
    out
}

impl EventsConfig {
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
    /// fsync policy for the on-disk spool. Spec 11 § 6.4 + decision
    /// D6-5: default `per_batch` trades a tiny durability window for
    /// ~64x throughput vs `per_record`. Operators who need strict
    /// durability flip to `per_record`; soak / dev profiles can use
    /// `none`.
    #[serde(default)]
    pub fsync_mode: AuditFsyncMode,
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
            fsync_mode: AuditFsyncMode::default(),
        }
    }
}

/// fsync policy applied to the AUDIT spool after each append. Spec 11
/// § 6.4 + decision D6-5.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditFsyncMode {
    /// No `fsync` after writes. Fastest; lossy under host crash.
    /// Suitable only for non-compliance dev / soak runs.
    None,
    /// Fsync after each batched append (default). Bounds the
    /// durability window to one batch (~64 records) while keeping
    /// steady-state throughput near zero-fsync.
    #[default]
    PerBatch,
    /// Fsync after every single record. Strictest durability, lowest
    /// throughput. Pick this when AUDIT volume is low and
    /// regulatory compliance demands per-record persistence.
    PerRecord,
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

/// Errors returned by [`EventsConfig::validate`] / loaders.
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
    /// I/O failure reading the config file.
    #[error("io: {0}")]
    Io(String),
    /// YAML parsing / shape error from the loader.
    #[error("yaml: {0}")]
    Yaml(String),
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

    #[test]
    fn test_from_yaml_str_should_parse_filter_and_sampling() {
        let yaml = "filter: info\nsampling:\n  default_rate: 0.25\n";
        let cfg = EventsConfig::from_yaml_str(yaml).expect("parse");
        assert_eq!(cfg.filter.as_deref(), Some("info"));
        assert!((cfg.sampling.default_rate - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_from_yaml_str_should_use_default_when_var_unset() {
        // Pure-read env-var test: pick a name nothing in the test env
        // could plausibly set. The expand-on-set / merged_with_env
        // paths are unit-tested by `expand_env_vars` and
        // `apply_yaml_path` below, both safe to call without env
        // mutation (which is `unsafe` under Rust 2024 and disallowed
        // by the crate's `#![forbid(unsafe_code)]`).
        let yaml = "filter: ${OBS_NEVER_SET_VAR_XYZ:-info}\n";
        let cfg = EventsConfig::from_yaml_str(yaml).expect("parse");
        assert_eq!(cfg.filter.as_deref(), Some("info"));
    }

    #[test]
    fn test_expand_env_vars_should_keep_unmatched_reference_verbatim() {
        let out = expand_env_vars("${OBS_NEVER_SET_VAR_AAAA}");
        assert_eq!(out, "${OBS_NEVER_SET_VAR_AAAA}");
    }

    #[test]
    fn test_expand_env_vars_should_drop_to_default_for_unset() {
        let out = expand_env_vars("${OBS_NEVER_SET_VAR_BBBB:-fallback}");
        assert_eq!(out, "fallback");
    }

    #[test]
    fn test_apply_yaml_path_should_walk_nested_keys() {
        let mut root = serde_yaml::Value::Mapping(Default::default());
        apply_yaml_path(
            &mut root,
            &["sampling".to_string(), "default_rate".to_string()],
            "0.5",
        );
        let cfg: EventsConfig = serde_yaml::from_value(root).expect("parse");
        assert!((cfg.sampling.default_rate - 0.5).abs() < f64::EPSILON);
    }
}
