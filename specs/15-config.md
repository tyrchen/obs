# Design — Configuration: `EventsConfig` and `obs.yaml`

Status: draft v1 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [10-data-model.md](./10-data-model.md), [11-runtime-core.md](./11-runtime-core.md), [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md), [20-otel-and-sinks.md](./20-otel-and-sinks.md)

This spec is the canonical definition of the **runtime-tunable
configuration** the SDK loads from `obs.yaml` and exposes as the Rust
type `EventsConfig`. Eight specs reference these — none defines them.
The plan called this gap out
([91-impl-plan.md § 0.2](./91-impl-plan.md#02-two-specs-are-referenced-but-never-authored));
this spec closes it.

> v1 (this draft): `EventsConfig` is the single source of truth for
> runtime tunables. Compile-time knobs (lints, codegen toggles) live in
> `[package.metadata.obs]` per [12-schema-and-codegen.md § 1.2](./12-schema-and-codegen.md#12-rust-first-recommended-for-single-crate-apps);
> they are NOT in `obs.yaml`.

## 1. Where each setting lives

The SDK has two configuration surfaces. Each setting belongs to
exactly one — we do not duplicate.

| Surface          | Lives in                                                             | Reload?            | Examples                                                           |
| ---------------- | -------------------------------------------------------------------- | ------------------ | ------------------------------------------------------------------ |
| **Compile-time** | `Cargo.toml` `[workspace.metadata.obs]` and `[package.metadata.obs]` | No (rebuild)       | `event_prefix`, `forensic_max`, `schema-source`, `proto-root`      |
| **Runtime**      | `obs.yaml`, `OBS_*` env vars, programmatic `EventsConfig::builder()` | Yes (live, atomic) | `filter`, `sampling`, sink endpoints, queue sizes, AUDIT spool dir |

If a setting plausibly belongs to both, default to runtime — the
"change at deploy time without recompile" property is more valuable
than the "compile-time guarantee" alternative for almost every
operational knob.

## 2. The `EventsConfig` Rust type

Lives in `obs-core::config`. Loaded once at observer init; held in
`ArcSwap<EventsConfig>` so reload is a single atomic pointer swap.

```rust
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct EventsConfig {
    /// Filter directives in the EnvFilter grammar (see 13 § 7).
    /// `None` ⇒ the env var `OBS_FILTER` wins; if both unset, "info" applies.
    #[serde(default)]
    pub filter: Option<String>,

    #[serde(default)]
    pub sampling: SamplingConfig,

    #[serde(default)]
    pub limits: LimitsConfig,

    #[serde(default)]
    pub audit: AuditConfig,

    #[serde(default)]
    pub queues: QueuesConfig,

    #[serde(default)]
    pub sinks: SinksConfig,

    #[serde(default)]
    pub service: ServiceConfig,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct SamplingConfig {
    /// Default head-sample rate `[0.0, 1.0]`. 1.0 = keep everything.
    #[serde(default = "default_one")]
    pub default_rate: f64,

    /// Per-event-name overrides. Map key is full_name (e.g. "myapp.v1.ObsXxx"),
    /// or `"*.<EventName>"` for cross-package matches.
    #[serde(default)]
    pub per_event: BTreeMap<String, f64>,

    /// "Always log if at least this severity" — bypasses sampling.
    #[serde(default = "default_warn_floor")]
    pub always_log_at_or_above: Severity,

    /// Tail-on-error buffer capacity per scope.
    #[serde(default = "default_64")]
    pub tail_buffer_capacity: u16,

    /// Honour W3C `traceparent.sampled` from inbound HTTP. Default true.
    #[serde(default = "default_true")]
    pub honour_traceparent_sampled: bool,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct LimitsConfig {
    /// Per-event encoded payload cap in bytes (11 § 6.2). Default 256 KiB.
    #[serde(default = "default_256kib")]
    pub max_payload_bytes: u32,

    /// Per-label-value byte cap. Defaults to 1 KiB; bumped if your domain
    /// legitimately has long but bounded labels (e.g. URL paths).
    #[serde(default = "default_1kib")]
    pub max_label_value_bytes: u16,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct AuditConfig {
    /// Channel capacity for the AUDIT tier worker (11 § 6.4). Default 1024.
    #[serde(default = "default_1024")]
    pub channel_capacity: u32,

    /// Bounded blocking on emit when AUDIT channel is full. Default 100 ms.
    #[serde(default = "default_100ms")]
    pub block_ms_max: u32,

    /// After this duration of channel-full, switch to disk spool. Default 250 ms.
    #[serde(default = "default_250ms")]
    pub spool_after_ms: u32,

    /// Spool directory; created if absent. Default `./obs-audit-spool/`.
    #[serde(default = "default_audit_dir")]
    pub spool_dir: PathBuf,

    /// Cap total spool size on disk; on overflow apply `on_failure`.
    #[serde(default = "default_1gib")]
    pub spool_max_bytes: u64,

    /// Sweep stale spool files older than this. Default 7 days.
    #[serde(default = "default_7d")]
    pub spool_max_age: Duration,

    /// Behaviour when AUDIT cannot be delivered (spool unwritable, disk full).
    #[serde(default)]
    pub on_failure: AuditFailureMode,
}

#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize, Default)]
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

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct QueuesConfig {
    /// Per-tier mpsc capacity (11 § 4). Default 8192 for Log/Metric/Trace.
    #[serde(default = "default_8192")]
    pub log: u32,
    #[serde(default = "default_8192")]
    pub metric: u32,
    #[serde(default = "default_8192")]
    pub trace: u32,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct SinksConfig {
    /// Stdout sink toggle. Default true in dev, false in release.
    #[serde(default)]
    pub stdout: Option<StdoutSinkConfig>,

    /// OTLP sinks (logs/metrics/traces). Per-sink shape — see 20-otel-and-sinks.md.
    #[serde(default)]
    pub otlp: Option<OtlpSinksConfig>,

    /// NDJSON file sink. See 20-otel-and-sinks.md.
    #[serde(default)]
    pub ndjson: Option<NdjsonSinkConfig>,

    /// Parquet sink (single-table). See 22-analytics-storage.md.
    #[serde(default)]
    pub parquet: Option<ParquetSinkConfig>,

    /// ClickHouse sink (single-table). See 22-analytics-storage.md.
    #[serde(default)]
    pub clickhouse: Option<ClickHouseSinkConfig>,
}

/// Identity overrides; defaults read from `OTEL_*` and `CARGO_PKG_VERSION`.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
#[non_exhaustive]
pub struct ServiceConfig {
    pub name: Option<String>,
    pub version: Option<String>,
    pub instance: Option<String>,
    pub namespace: Option<String>,
    pub environment: Option<String>,
    /// Free-form OTel Resource extras (see 11 § 7 ResourceAttrs).
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}
```

The per-sink config shapes (`StdoutSinkConfig`, `OtlpSinksConfig`,
…) are defined in their respective specs and consumed by sink
constructors. They live in this struct because the `obs.yaml` is a
single document; the **schemas** of those nested sub-structs are
owned by the relevant sink specs.

## 3. Defaults helper functions

```rust
fn default_one() -> f64 { 1.0 }
fn default_true() -> bool { true }
fn default_warn_floor() -> Severity { Severity::Warn }
fn default_64() -> u16 { 64 }
fn default_256kib() -> u32 { 256 * 1024 }
fn default_1kib() -> u16 { 1024 }
fn default_1024() -> u32 { 1024 }
fn default_8192() -> u32 { 8192 }
fn default_100ms() -> u32 { 100 }
fn default_250ms() -> u32 { 250 }
fn default_1gib() -> u64 { 1 << 30 }
fn default_7d() -> Duration { Duration::from_secs(7 * 24 * 3600) }
fn default_audit_dir() -> PathBuf { PathBuf::from("./obs-audit-spool") }
```

## 4. The `obs.yaml` schema

This is the *exhaustive* schema. Every field is optional; defaults
match the helper functions in § 3. A real-world `obs.yaml` is
typically 10–30 lines.

```yaml
# obs.yaml — runtime configuration for the obs SDK
# All fields are optional; leave a field out to take the default.

# 1. Filter (EnvFilter grammar; see 13 § 7).
filter: "info,myapp::auth=debug,myapp.v1.ObsRequestCompleted=trace"

# 2. Sampling.
sampling:
  default_rate: 0.5            # head-sample 50% by default
  per_event:
    "myapp.v1.ObsHealthcheck": 0.01    # rare healthchecks
    "myapp.v1.ObsRequestCompleted": 1.0
  always_log_at_or_above: warn  # WARN/ERROR/FATAL bypass sampling
  tail_buffer_capacity: 64
  honour_traceparent_sampled: true

# 3. Per-event byte limits.
limits:
  max_payload_bytes: 262144    # 256 KiB
  max_label_value_bytes: 1024  # 1 KiB

# 4. AUDIT-tier (11 § 6.4).
audit:
  channel_capacity: 1024
  block_ms_max: 100
  spool_after_ms: 250
  spool_dir: "/var/lib/myapp/audit-spool"
  spool_max_bytes: 1073741824   # 1 GiB
  spool_max_age: "7d"           # humantime parse; supports "30s", "5m", "12h", "7d"
  on_failure: panic              # panic | abort | warn_only

# 5. Per-tier mpsc queues.
queues:
  log: 8192
  metric: 8192
  trace: 8192

# 6. Sinks (per-sink shape lives in the sink's spec).
sinks:
  stdout:
    style: full                # noop | compact | full | json
    severity_floor: info
  otlp:
    endpoint: "https://otel-collector.example.com:4317"
    protocol: grpc             # grpc | http_protobuf
    compression: gzip          # none | gzip | zstd
    timeout: "10s"
    headers:
      authorization: "Bearer ${OBS_OTLP_TOKEN}"
  ndjson:
    path: "/var/log/myapp/events.ndjson"
    roll_max_bytes: 134217728  # 128 MiB
    roll_max_age: "1h"
  parquet:
    base_dir: "s3://myapp-events/"
    layout: single             # single | table_per_event
    partition_by: ["service", "date"]
  clickhouse:
    url: "tcp://clickhouse.example.com:9000"
    database: "myapp"
    table: "obs_events"
    batch_size: 4096

# 7. Identity overrides (default reads OTEL_* env + CARGO_PKG_VERSION).
service:
  name: "my-api"
  version: "0.4.2"
  instance: "pod-7f3a"
  namespace: "production"
  environment: "production"
  extra:
    "host.name": "ip-10-0-1-23"
```

### 4.1 Environment-variable expansion

Within a string value, `${VAR}` is expanded to the value of the
environment variable `VAR` at config-load time. Unset variables
produce a config-load error (we do not fall back to empty strings —
that would silently route OTLP to an empty endpoint).

This is the only string transform applied. We do not do `${VAR:-default}`,
nested expansion, or arithmetic. Use a deployment template (helm,
sops, etc.) for anything more complex.

### 4.2 Duration parsing

Duration fields use the [`humantime`](https://docs.rs/humantime) format:
`"30s"`, `"5m"`, `"12h"`, `"7d"`, `"1y"`. Fractional units are not
supported (`"1.5h"` is invalid; use `"90m"`). Bare numbers without a
unit are rejected at parse time — `"100"` is ambiguous; `"100ms"` is
explicit.

### 4.3 Strictness

`#[serde(deny_unknown_fields)]` is set on every struct. A typo like
`filter:` produces a config-load error rather than silently doing
nothing. The error message names the offending field and the line
number from `serde_yaml`'s span info.

## 5. Loading and reload

### 5.1 Programmatic builder

```rust
let config = EventsConfig::builder()
    .filter("info,myapp=debug")
    .sampling(SamplingConfig {
        default_rate: 0.5,
        ..Default::default()
    })
    .build();

// Or load from a path:
let config = EventsConfig::from_yaml_path("/etc/myapp/obs.yaml")?;

// Or merge: file + env override:
let config = EventsConfig::from_yaml_path(path)?.merged_with_env();
```

### 5.2 Env override layer

`OBS_*` environment variables override matching config keys, applied
after file load. Naming convention is **uppercase, double-underscore
between path segments**:

| Env var                                | Maps to                           |
| -------------------------------------- | --------------------------------- |
| `OBS_FILTER`                           | `filter`                          |
| `OBS_SAMPLING__DEFAULT_RATE`           | `sampling.default_rate`           |
| `OBS_SAMPLING__ALWAYS_LOG_AT_OR_ABOVE` | `sampling.always_log_at_or_above` |
| `OBS_AUDIT__SPOOL_DIR`                 | `audit.spool_dir`                 |
| `OBS_QUEUES__LOG`                      | `queues.log`                      |
| `OBS_SINKS__OTLP__ENDPOINT`            | `sinks.otlp.endpoint`             |

Implemented via the `config` crate's `Environment` source, prefix
`OBS_`, separator `__`. Map fields (`per_event`, `headers`, `extra`)
are not env-overridable — env vars don't capture map structure
cleanly.

### 5.3 Reload paths

Triggered by:

- **SIGHUP** (Unix only). `Observer::reload_filter()` is wired to
  the signal in the SDK's optional `signal-hooks` integration.
- **File watcher** (cross-platform). When `reload_on_file_change()`
  is set on the `StandardObserverBuilder`, a `notify` watcher on the
  config file's directory triggers a re-read after a 200 ms debounce
  (per [docs/research/spike-notify.md](../docs/research/spike-notify.md)).
- **Programmatic**: `observer.reload_config(path)` for tests and for
  control planes that push config without restarting the process.

On reload:

1. Re-read the file; parse to `EventsConfig`.
2. If parse fails, **keep the old config** and emit
   `ObsConfigReloadFailed` (LOG/WARN). No partial application.
3. If parse succeeds, build a new `Arc<EventsConfig>`,
   `ArcSwap::store` it, then call `Observer::reload_filter()`
   (which bumps the generation per [11 § 3.2](./11-runtime-core.md#32-filter-cache-invalidation-on-reload)).
4. Emit `ObsConfigReloaded` (LOG/INFO) with the new config's hash.

### 5.4 What changes in place vs. what requires restart

Reload is **best-effort**; some settings can be applied in flight,
others cannot. The runtime documents which is which:

| Setting                              | Reloadable in place?                                                                                                        |
| ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `filter`                             | ✅ via `reload_filter()` + generation bump                                                                                   |
| `sampling.*`                         | ✅ — read on every emit through `ArcSwap`                                                                                    |
| `limits.*`                           | ✅ — same                                                                                                                    |
| `audit.on_failure`                   | ✅ — read on every spool failure                                                                                             |
| `audit.spool_dir`                    | ⚠ partial — new files go to new dir; in-flight in old dir; emit `ObsConfigReloaded` with both paths                         |
| `audit.channel_capacity`, `queues.*` | ❌ — channels are constructed once. Reload validates the value but logs a `ObsConfigInconsistent` warning. Restart to apply. |
| `sinks.otlp.*`                       | ⚠ partial — new requests use new endpoint, in-flight retry uses old. To switch endpoints cleanly, restart.                  |
| `sinks.parquet.base_dir`             | ❌ — restart.                                                                                                                |
| `sinks.clickhouse.url`               | ❌ — restart.                                                                                                                |
| `service.*`                          | ❌ — service identity is set once at observer init.                                                                          |

Operators who need every setting to be live-reloadable must restart
after changing the ❌ ones; the SDK does not pretend otherwise.

## 6. Validation

After deserialisation, `EventsConfig::validate()` runs:

| Check                                                                 | Failure mode                           |
| --------------------------------------------------------------------- | -------------------------------------- |
| `sampling.default_rate ∈ [0.0, 1.0]`                                  | Reject; suggest valid range            |
| Each `sampling.per_event[k]` rate `∈ [0.0, 1.0]`                      | Reject; name the offending key         |
| `limits.max_payload_bytes ≥ 1024`                                     | Reject; <1 KiB cripples even forensic  |
| `limits.max_payload_bytes ≤ 16 MiB`                                   | Reject; over-large is a sign of misuse |
| `audit.spool_dir` parent exists or is creatable                       | Reject (config load fails fast)        |
| `audit.spool_max_bytes ≥ 1 MiB`                                       | Reject                                 |
| `queues.{log,metric,trace} ≥ 64`                                      | Reject                                 |
| `filter` parses as a valid `obs::Filter` directive                    | Reject; quote the parse error          |
| Mutually exclusive sink combos (e.g. two `sinks.otlp.*` sub-sections) | Reject                                 |

Validation runs at every load, including reload. An invalid reload
keeps the old config (§ 5.3 step 2).

## 7. Test harness

For tests that want to assert behaviour under a specific config:

```rust
use obs::config::EventsConfig;

#[obs::test]
async fn drops_below_floor() {
    let cfg = EventsConfig::builder()
        .filter("warn")
        .sampling(SamplingConfig { default_rate: 0.0, ..Default::default() })
        .build();
    let observer = StandardObserver::builder()
        .config(cfg)
        .sink_for(Tier::Log, InMemorySink::default())
        .build()
        .unwrap();
    obs::with_test_observer(observer, || {
        ObsThing::builder().emit_at(Severity::Info);
        ObsThing::builder().emit_at(Severity::Error);
    });
    // assert: only the error was kept
}
```

`EventsConfig::builder()` is the canonical test entry; never load a
fixture YAML in tests (file IO breaks `#[obs::test]` parallelism on
Windows CI).

## 8. CLAUDE.md compliance

- `#[non_exhaustive]` on every public struct so we can add fields
  without a major bump.
- `#[serde(deny_unknown_fields)]` so config typos fail loud.
- All numeric fields are bounded by `validate()` rather than relying
  on the type alone (CLAUDE.md `Numeric ranges` rule).
- Duration fields use `humantime` to avoid the `Duration` ambiguity.
- Path fields go through `PathBuf`; we do not canonicalise eagerly
  (the directory may not exist yet at config load).
- Secrets in config (e.g. `${OBS_OTLP_TOKEN}` after env expansion) are
  wrapped in `secrecy::SecretString` in the in-memory representation
  so they don't leak into `Debug`. The serialised YAML representation
  uses a custom Serialize impl that returns `"<redacted>"` to prevent
  round-tripping a config back to YAML from leaking secrets.

## 9. Build dependencies

| Depends on                                                   | Provides                                                                                        |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| [10-data-model.md](./10-data-model.md)                       | `Severity`, etc.                                                                                |
| [11-runtime-core.md](./11-runtime-core.md)                   | Reload triggers, generation bump, AUDIT semantics                                               |
| [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md) | Filter grammar                                                                                  |
| [20-otel-and-sinks.md](./20-otel-and-sinks.md)               | Sink config shapes                                                                              |
| (provides foundation for)                                    | Phase 1 task 1.8 (`StandardObserver` shell wires `ArcSwap<EventsConfig>`); Phase 3 reload tasks |

This spec ships in `obs-core::config`. The `obs-cli`'s
`obs validate` subcommand ([50-cli.md § 3.2](./50-cli.md#32-obs-validate))
reuses `EventsConfig::from_yaml_path` + `validate()` so users can
catch typos before deploying.
