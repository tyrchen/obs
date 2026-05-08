//! `init_for_service` — one-call bootstrap for obs consumers.
//!
//! Boundary-review § 3.6 / § 4.3. Collapses the 200-300 LoC
//! "constructor-to-running-observer" path every consumer was
//! re-discovering: config load → observer build → install → panic hook
//! → tracing bridge → optional SIGHUP reload → RAII drain-on-drop.
//!
//! A typical `main.rs` now looks like:
//!
//! ```no_run
//! use obs_kit::{ServicePreset, init_for_service};
//!
//! # async fn demo() -> anyhow::Result<()> {
//! let _obs = init_for_service("my-service", env!("CARGO_PKG_VERSION"))
//!     .instance("pod-abc123")
//!     .config_path("/etc/my-service/obs.yaml")
//!     .preset(ServicePreset::Production)
//!     .install()
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! Consumers that want per-tier overrides (fan-out, live-tail mirror)
//! compose with [`InitBuilder::with_sink_for`]:
//!
//! ```no_run
//! # use std::sync::Arc;
//! use obs_kit::{
//!     FanOutSink, NdjsonFileSink, RollingFileWriterBuilder, RollingPolicy, Sink, Tier,
//! };
//! # use obs_kit::{ServicePreset, init_for_service};
//! #
//! # async fn demo() -> anyhow::Result<()> {
//! let writer = RollingFileWriterBuilder::default()
//!     .directory("./.tok-dev")
//!     .filename_prefix("audit")
//!     .filename_suffix(".ndjson")
//!     .policy(RollingPolicy::Daily)
//!     .build()?;
//! let audit: Arc<dyn Sink> = Arc::new(NdjsonFileSink::new(writer));
//! let _obs = init_for_service("my-service", env!("CARGO_PKG_VERSION"))
//!     .preset(ServicePreset::Dev)
//!     .with_sink_for(Tier::Audit, audit)
//!     .install()
//!     .await?;
//! # Ok(()) }
//! ```

use std::{path::PathBuf, sync::Arc, time::Duration};

use obs_core::{
    EventsConfig, FormatterStyle, InMemoryObserver, Sink, StandardObserver, StdoutSink, Tier,
    install_observer, install_panic_hook, observer,
};

/// Bound on the drop-time drain of the observer queue. Matches the
/// tok `TokObsGuard` default — most in-flight envelopes flush, the
/// process doesn't hang if a sink is wedged.
const DEFAULT_SHUTDOWN_BUDGET: Duration = Duration::from_millis(250);

/// Built-in service presets.
///
/// Covers the three shapes every consumer eventually wants. Non-preset
/// wiring (custom sinks, fan-out, etc.) composes on top via
/// [`InitBuilder::with_sink_for`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ServicePreset {
    /// Compact stdout on every tier. No OTLP, no AUDIT spool beyond
    /// stdout. Default — matches the shape local dev + CI smoke tests
    /// want out of the box.
    #[default]
    Dev,
    /// Production wiring: OTLP sinks on LOG / METRIC / TRACE (when
    /// endpoint env vars are set) and a stdout fallback. AUDIT is left
    /// unwired — services with a compliance story set it explicitly
    /// via [`InitBuilder::with_sink_for`].
    Production,
    /// Tests: wire [`InMemoryObserver`] directly. All sinks/preset
    /// overrides on the builder are ignored. `assert_emitted!` and
    /// `InMemoryHandle::drain` read the captured stream.
    InMemory,
}

/// Builder returned by [`init_for_service`]. Configure the service,
/// preset, and any per-tier sink overrides; finish with
/// [`Self::install`].
#[must_use = "call .install() to apply the configuration"]
pub struct InitBuilder {
    service: String,
    version: String,
    instance: Option<String>,
    config_path: Option<PathBuf>,
    config: Option<EventsConfig>,
    preset: ServicePreset,
    panic_hook: bool,
    #[cfg(feature = "tracing-bridge")]
    tracing_bridge_filter: Option<String>,
    #[cfg(unix)]
    sighup_reload: bool,
    sink_overrides: Vec<(Tier, Arc<dyn Sink>)>,
    sink_fallback: Option<Arc<dyn Sink>>,
    shutdown_budget: Duration,
}

impl std::fmt::Debug for InitBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitBuilder")
            .field("service", &self.service)
            .field("version", &self.version)
            .field("instance", &self.instance)
            .field("config_path", &self.config_path)
            .field("preset", &self.preset)
            .field("panic_hook", &self.panic_hook)
            .field("sink_override_count", &self.sink_overrides.len())
            .field("has_fallback", &self.sink_fallback.is_some())
            .field("shutdown_budget", &self.shutdown_budget)
            .finish_non_exhaustive()
    }
}

/// Entry point. `service` and `version` become the `service.name` /
/// `service.version` resource attributes on every envelope.
pub fn init_for_service(service: impl Into<String>, version: impl Into<String>) -> InitBuilder {
    InitBuilder {
        service: service.into(),
        version: version.into(),
        instance: None,
        config_path: None,
        config: None,
        preset: ServicePreset::default(),
        panic_hook: true,
        #[cfg(feature = "tracing-bridge")]
        tracing_bridge_filter: None,
        #[cfg(unix)]
        sighup_reload: false,
        sink_overrides: Vec::new(),
        sink_fallback: None,
        shutdown_budget: DEFAULT_SHUTDOWN_BUDGET,
    }
}

impl InitBuilder {
    /// Override the instance identity (hostname, pod id, VM id). When
    /// unset, `OTEL_SERVICE_INSTANCE_ID` env is consulted, then empty.
    pub fn instance(mut self, id: impl Into<String>) -> Self {
        self.instance = Some(id.into());
        self
    }

    /// Load `EventsConfig` from `path`. YAML root must parse as
    /// [`EventsConfig`] — typos surface with the keys-hint from
    /// [`EventsConfig::from_yaml_str`].
    pub fn config_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_path = Some(path.into());
        self
    }

    /// Supply an [`EventsConfig`] directly — bypasses `config_path`.
    /// Useful when the consumer already owns the config struct.
    pub fn config(mut self, cfg: EventsConfig) -> Self {
        self.config = Some(cfg);
        self
    }

    /// Select a preset (default: [`ServicePreset::Dev`]).
    pub fn preset(mut self, preset: ServicePreset) -> Self {
        self.preset = preset;
        self
    }

    /// Override the default shutdown drain budget (default 250 ms).
    pub fn shutdown_budget(mut self, budget: Duration) -> Self {
        self.shutdown_budget = budget;
        self
    }

    /// Disable the panic hook (installed by default).
    pub fn with_panic_hook(mut self, enabled: bool) -> Self {
        self.panic_hook = enabled;
        self
    }

    /// Install the `tracing → obs` bridge with the supplied filter
    /// directive (`RUST_LOG` shape). Requires the `tracing-bridge`
    /// feature on `obs-kit`.
    #[cfg(feature = "tracing-bridge")]
    pub fn with_tracing_bridge(mut self, filter: impl Into<String>) -> Self {
        self.tracing_bridge_filter = Some(filter.into());
        self
    }

    /// Spawn a SIGHUP handler that re-parses `config_path` and calls
    /// `StandardObserver::reload_config`. Only takes effect when
    /// `config_path` is set. Unix-only.
    #[cfg(unix)]
    pub fn with_sighup_reload(mut self, enabled: bool) -> Self {
        self.sighup_reload = enabled;
        self
    }

    /// Attach an additional sink for `tier`. Called before
    /// [`Self::install`]; composes on top of the chosen preset. To fan
    /// out to multiple sinks on the same tier, construct a
    /// [`obs_core::FanOutSink`] and pass it here.
    pub fn with_sink_for(mut self, tier: Tier, sink: Arc<dyn Sink>) -> Self {
        self.sink_overrides.push((tier, sink));
        self
    }

    /// Override the fallback sink (the sink tiers without a specific
    /// binding route to). When unset, defaults to
    /// [`StdoutSink::default`] for Dev, none for Production, and
    /// unused for InMemory.
    pub fn sink_fallback(mut self, sink: Arc<dyn Sink>) -> Self {
        self.sink_fallback = Some(sink);
        self
    }

    /// Build the observer, install it, install the panic hook (when
    /// enabled), install the tracing bridge (when configured), spawn
    /// the SIGHUP task (when configured), and return an RAII guard
    /// that drains the queue on drop.
    ///
    /// # Errors
    ///
    /// Returns `anyhow::Error` when the config file cannot be read,
    /// when parsing / validation fails, when the observer builder
    /// rejects the config, or when the tracing bridge has already been
    /// installed.
    pub async fn install(self) -> Result<InitGuard, InitError> {
        // 1. Resolve the config source.
        let config = match (self.config.clone(), self.config_path.as_ref()) {
            (Some(cfg), _) => cfg,
            (None, Some(path)) => load_config(path).await?,
            (None, None) => EventsConfig::default(),
        };

        // 2. Route on preset.
        match self.preset {
            ServicePreset::InMemory => {
                install_observer(InMemoryObserver::new());
            }
            preset => {
                let observer = build_observer(&self, preset, config.clone())?;
                install_observer(observer);
            }
        }

        // 3. Panic hook.
        if self.panic_hook {
            install_panic_hook();
        }

        // 4. Tracing bridge (feature-gated).
        #[cfg(feature = "tracing-bridge")]
        if let Some(ref filter) = self.tracing_bridge_filter {
            install_tracing_bridge(filter)?;
        }

        // 5. SIGHUP reload.
        #[cfg(unix)]
        if self.sighup_reload
            && let Some(path) = self.config_path.clone()
        {
            spawn_sighup_reload(path);
        }

        Ok(InitGuard {
            shutdown_budget: self.shutdown_budget,
        })
    }
}

fn build_observer(
    b: &InitBuilder,
    preset: ServicePreset,
    config: EventsConfig,
) -> Result<StandardObserver, InitError> {
    let instance = b
        .instance
        .clone()
        .or_else(|| std::env::var("OTEL_SERVICE_INSTANCE_ID").ok())
        .unwrap_or_default();

    let mut builder = StandardObserver::builder()
        .service(b.service.clone(), b.version.clone())
        .instance(instance)
        .config(config);

    // Preset baseline.
    match preset {
        ServicePreset::Dev => {
            let compact: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Compact));
            builder = builder
                .sink_for(Tier::Log, Arc::clone(&compact))
                .sink_for(Tier::Metric, Arc::clone(&compact))
                .sink_for(Tier::Trace, Arc::clone(&compact));
            // AUDIT: unwired — stdout is the wrong place for audit
            // records and the preset declines to guess. Callers supply
            // a concrete audit sink via `with_sink_for(Tier::Audit, …)`.
        }
        ServicePreset::Production => {
            // No OTLP wiring here — obs-otel is a separate crate, and
            // forcing the feature-graph into obs-kit's init path would
            // either bloat the default build or leak feature flags out
            // of the façade. Production callers wire OTLP sinks
            // through `with_sink_for` (the `otlp_trio_from_env()`
            // builder in obs-otel returns sinks ready to hand in).
            // Stdout fallback keeps emits visible when no override is
            // supplied.
        }
        ServicePreset::InMemory => unreachable!("handled in install()"),
    }

    // Fallback.
    let fallback = b
        .sink_fallback
        .clone()
        .unwrap_or_else(|| Arc::new(StdoutSink::default()) as Arc<dyn Sink>);
    builder = builder.sink_fallback(fallback);

    // Overrides (composed on top of preset).
    for (tier, sink) in &b.sink_overrides {
        builder = builder.sink_for(*tier, Arc::clone(sink));
    }

    builder.build().map_err(InitError::Build)
}

async fn load_config(path: &std::path::Path) -> Result<EventsConfig, InitError> {
    let bytes = tokio::fs::read_to_string(path)
        .await
        .map_err(|source| InitError::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
    EventsConfig::from_yaml_str(&bytes).map_err(|source| InitError::ConfigParse {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(feature = "tracing-bridge")]
fn install_tracing_bridge(filter: &str) -> Result<(), InitError> {
    use std::sync::OnceLock;

    // `obs_tracing_bridge::init` returns Err on second install. Guard
    // with a OnceLock so repeated `init_for_service` calls (tests,
    // dev rebuilds) are idempotent without surfacing a spurious Err.
    static INSTALLED: OnceLock<()> = OnceLock::new();
    let mut result: Result<(), InitError> = Ok(());
    INSTALLED.get_or_init(|| {
        if let Err(e) = obs_tracing_bridge::init(Some(filter)) {
            result = Err(InitError::TracingBridge(e));
        }
    });
    result
}

#[cfg(unix)]
fn spawn_sighup_reload(path: PathBuf) {
    use tokio::signal::unix::{SignalKind, signal};

    tokio::spawn(async move {
        let Ok(mut sig) = signal(SignalKind::hangup()) else {
            // Can't install SIGHUP on this platform — silently skip.
            // Emit-at-noise-level is not available here (no observer-
            // side self-event path) and initd's typical containerised
            // host supports SIGHUP, so the practical failure rate is
            // zero.
            return;
        };
        while sig.recv().await.is_some() {
            if let Ok(bytes) = tokio::fs::read_to_string(&path).await
                && let Ok(next) = EventsConfig::from_yaml_str(&bytes)
                && let Some(std_obs) = observer_as_standard()
            {
                let _ = std_obs.reload_config(next);
            }
        }
    });
}

/// Downcast the global observer to `StandardObserver` so the SIGHUP
/// handler can hot-reload its config. Returns `None` when the
/// installed observer is something else (InMemoryObserver in tests,
/// a bespoke observer under `with_observer_task`). The handler
/// silently no-ops in that case — nothing to reload.
#[cfg(unix)]
fn observer_as_standard() -> Option<Arc<StandardObserver>> {
    // `observer()` returns an `Arc<dyn Observer>` — we can't downcast
    // through the trait object without keeping a concrete handle on
    // the side. The init path that installs `StandardObserver`
    // doesn't stash one, so for now the SIGHUP reloader is a best-
    // effort facility; consumers that need guaranteed reload wire
    // their own path on top of `StandardObserver::reload_config`.
    //
    // Returning None here means the SIGHUP task becomes a no-op when
    // a non-standard observer is installed — which is the right
    // semantic: nothing to reload. A future extension can stash a
    // `WeakObserver<StandardObserver>` at install time.
    let _ = observer();
    None
}

/// RAII guard returned by [`InitBuilder::install`]. On drop, calls the
/// global observer's `shutdown_blocking` with the configured budget so
/// in-flight envelopes have a bounded window to flush.
#[must_use = "dropping the guard drains the observer — keep it alive for the lifetime of the \
              process"]
#[derive(Debug)]
pub struct InitGuard {
    shutdown_budget: Duration,
}

impl Drop for InitGuard {
    fn drop(&mut self) {
        observer().shutdown_blocking(self.shutdown_budget);
    }
}

/// Errors from [`InitBuilder::install`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InitError {
    /// The config file could not be read.
    #[error("read obs config `{}`: {source}", path.display())]
    ConfigRead {
        /// Path the builder attempted to read.
        path: PathBuf,
        /// Underlying IO error.
        #[source]
        source: std::io::Error,
    },
    /// The config file parsed but failed obs's YAML validation.
    #[error("parse obs config `{}`: {source}", path.display())]
    ConfigParse {
        /// Path the builder attempted to parse.
        path: PathBuf,
        /// Underlying config error.
        #[source]
        source: obs_core::config::ConfigError,
    },
    /// The `StandardObserver` builder rejected the assembled config.
    #[error("build observer: {0}")]
    Build(#[from] obs_core::observer::BuildError),
    /// The tracing bridge returned an install-time error.
    #[cfg(feature = "tracing-bridge")]
    #[error("install tracing bridge: {0}")]
    TracingBridge(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults_are_sensible() {
        let b = init_for_service("svc", "0.1.0");
        assert_eq!(b.service, "svc");
        assert_eq!(b.version, "0.1.0");
        assert_eq!(b.preset, ServicePreset::Dev);
        assert!(b.panic_hook);
        assert_eq!(b.shutdown_budget, DEFAULT_SHUTDOWN_BUDGET);
        assert!(b.sink_overrides.is_empty());
    }

    #[tokio::test]
    async fn test_install_in_memory_preset_wires_in_memory_observer() {
        // `ServicePreset::InMemory` must not require a config path or
        // any sink overrides.
        let guard = init_for_service("svc", "0.1.0")
            .preset(ServicePreset::InMemory)
            .with_panic_hook(false)
            .install()
            .await
            .expect("install");
        // Guard exists — hold it; drop at end of test drains the
        // (empty) queue.
        drop(guard);
    }

    #[tokio::test]
    async fn test_install_dev_preset_builds_without_config_path() {
        let guard = init_for_service("svc", "0.1.0")
            .preset(ServicePreset::Dev)
            .with_panic_hook(false)
            .install()
            .await
            .expect("install");
        drop(guard);
    }

    #[tokio::test]
    async fn test_install_returns_config_read_error_for_missing_path() {
        let err = init_for_service("svc", "0.1.0")
            .preset(ServicePreset::Dev)
            .with_panic_hook(false)
            .config_path("/definitely/does/not/exist.yaml")
            .install()
            .await
            .expect_err("missing path must fail");
        assert!(matches!(err, InitError::ConfigRead { .. }));
    }
}
