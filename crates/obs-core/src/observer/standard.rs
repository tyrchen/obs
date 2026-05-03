//! `StandardObserver` — production-ready observer with per-tier
//! workers, AUDIT spool, head sampler, scope auto-fill, and live
//! config reload. Spec 11 §§ 3, 4, 6.4 + spec 13 §§ 2, 6.

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use arc_swap::ArcSwap;
use bytes;
use obs_proto::obs::v1::{ObsEnvelope, SamplingReason as PSamplingReason};
use obs_types::Tier;
use parking_lot::Mutex;

use super::{
    Observer,
    workers::{TierWorker, WorkerCounters, note_channel_full, spawn_tier_worker},
};
use crate::{
    audit_spool::SpoolWriter,
    callsite::ObsCallsite,
    config::{AuditFailureMode, EventsConfig},
    filter::Filter,
    registry::{SchemaRegistry, ScrubbedEnvelope},
    sampling::{SamplingDecision, decide as sample_decide},
    scope::{auto_fill_envelope, inbound_traceparent_sampled, push_tail_buffer},
    sink::{NoopSink, Sink, SinkFut, StdoutSink},
};

/// Tier-matching dispatcher. One sink slot per tier plus a fallback.
#[derive(Default)]
struct SinkRouter {
    log: Option<Arc<dyn Sink>>,
    metric: Option<Arc<dyn Sink>>,
    trace: Option<Arc<dyn Sink>>,
    audit: Option<Arc<dyn Sink>>,
    fallback: Option<Arc<dyn Sink>>,
}

impl std::fmt::Debug for SinkRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkRouter")
            .field("log", &self.log.as_ref().map(|_| "..."))
            .field("metric", &self.metric.as_ref().map(|_| "..."))
            .field("trace", &self.trace.as_ref().map(|_| "..."))
            .field("audit", &self.audit.as_ref().map(|_| "..."))
            .field("fallback", &self.fallback.as_ref().map(|_| "..."))
            .finish()
    }
}

impl SinkRouter {
    fn for_tier(&self, tier: Tier) -> Option<&Arc<dyn Sink>> {
        let primary = match tier {
            Tier::Log => self.log.as_ref(),
            Tier::Metric => self.metric.as_ref(),
            Tier::Trace => self.trace.as_ref(),
            Tier::Audit => self.audit.as_ref(),
            _ => None,
        };
        primary.or(self.fallback.as_ref())
    }
}

/// Worker handles, indexed by tier; AUDIT is special (spool path).
#[derive(Debug, Default)]
struct WorkerPool {
    log: Option<TierWorker>,
    metric: Option<TierWorker>,
    trace: Option<TierWorker>,
    audit: Option<TierWorker>,
}

/// Production-ready observer with reloadable config and per-tier
/// worker pool.
pub struct StandardObserver {
    router: SinkRouter,
    workers: WorkerPool,
    spool: Option<Arc<SpoolWriter>>,
    registry: Arc<SchemaRegistry>,
    config: ArcSwap<EventsConfig>,
    filter: ArcSwap<Filter>,
    counters: Arc<WorkerCounters>,
    generation: AtomicU32,
    service: String,
    instance: String,
    version: String,
    /// Synchronous fallback for environments without a tokio runtime
    /// (tests, CLI tools): protects in-thread sink dispatch.
    sync_dispatch_lock: Mutex<()>,
}

impl std::fmt::Debug for StandardObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandardObserver")
            .field("schemas", &self.registry.len())
            .field("service", &self.service)
            .field("instance", &self.instance)
            .field("version", &self.version)
            .field("generation", &self.generation.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl StandardObserver {
    /// Builder entry.
    #[must_use]
    pub fn builder() -> StandardObserverBuilder {
        StandardObserverBuilder::default()
    }

    /// Convenience: `StandardObserver` with `StdoutSink(Full)` as
    /// fallback.
    ///
    /// # Errors
    ///
    /// Returns `BuildError` if config validation fails.
    pub fn dev() -> Result<Self, BuildError> {
        Self::builder()
            .service("dev", env!("CARGO_PKG_VERSION"))
            .sink_fallback(Arc::new(StdoutSink::default()))
            .build()
    }

    /// Read-only access to the registry (used by sinks).
    #[must_use]
    pub fn registry(&self) -> Arc<SchemaRegistry> {
        Arc::clone(&self.registry)
    }

    /// Read-only access to the live config.
    #[must_use]
    pub fn config(&self) -> arc_swap::Guard<Arc<EventsConfig>> {
        self.config.load()
    }

    /// Atomically swap the config and bump the generation so all
    /// callsites re-probe. Spec 11 § 3.2.
    ///
    /// # Errors
    ///
    /// Returns `BuildError::InvalidConfig` if validation rejects
    /// `new_config`.
    pub fn reload_config(&self, new_config: EventsConfig) -> Result<(), BuildError> {
        new_config.validate().map_err(BuildError::InvalidConfig)?;
        if let Some(spec) = new_config.filter.as_deref() {
            let parsed = Filter::parse(spec).map_err(|e| {
                BuildError::InvalidConfig(crate::config::ConfigError::invalid_range(
                    "filter",
                    format!("{e}"),
                ))
            })?;
            self.filter.store(Arc::new(parsed));
        } else {
            self.filter.store(Arc::new(Filter::new()));
        }
        self.config.store(Arc::new(new_config));
        self.generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Read-only access to the live filter.
    #[must_use]
    pub fn filter(&self) -> Arc<Filter> {
        self.filter.load_full()
    }

    /// Worker counters surface for tests + diagnostics.
    #[must_use]
    pub fn counters(&self) -> Arc<WorkerCounters> {
        Arc::clone(&self.counters)
    }

    fn fill_identity(&self, env: &mut ObsEnvelope) {
        if env.service.is_empty() {
            env.service.clone_from(&self.service);
        }
        if env.instance.is_empty() {
            env.instance.clone_from(&self.instance);
        }
        if env.version.is_empty() {
            env.version.clone_from(&self.version);
        }
    }

    fn dispatch_sync(&self, env: ObsEnvelope, tier: Tier) {
        // No tokio runtime ⇒ deliver in-emit-thread. The spec's
        // "scrubber on the worker thread" rule (spec 11 § 4.1) still
        // applies: sinks must never see an unscrubbed envelope, so we
        // run the scrubber here and then dispatch.
        let _g = self.sync_dispatch_lock.lock();
        let Some(sink) = self.router.for_tier(tier) else {
            return;
        };
        let mut scratch = bytes::BytesMut::with_capacity(env.payload.len());
        let scrubbed = match ScrubbedEnvelope::scrub(&env, &self.registry, &mut scratch) {
            Ok(s) => s,
            Err(_) => return,
        };
        sink.deliver(scrubbed);
    }

    fn dispatch_async(&self, env: ObsEnvelope, tier: Tier) {
        let worker = match tier {
            Tier::Log => self.workers.log.as_ref(),
            Tier::Metric => self.workers.metric.as_ref(),
            Tier::Trace => self.workers.trace.as_ref(),
            Tier::Audit => self.workers.audit.as_ref(),
            _ => None,
        };
        let Some(worker) = worker else {
            // No worker (no tokio runtime) — fall back to sync.
            self.dispatch_sync(env, tier);
            return;
        };
        if tier == Tier::Audit {
            self.dispatch_audit(worker, env);
        } else {
            match worker.try_send(env) {
                Ok(()) => {}
                Err(_dropped) => {
                    note_channel_full(&self.counters, tier);
                }
            }
        }
    }

    fn dispatch_audit(&self, worker: &TierWorker, env: ObsEnvelope) {
        let cfg = self.config.load();
        let block_ms = u64::from(cfg.audit.block_ms_max);
        // First try a non-blocking send; if it succeeds, we're done.
        let mut env_unsent = match worker.try_send(env) {
            Ok(()) => return,
            Err(env) => env,
        };
        // Fall back to bounded busy-wait with the configured timeout.
        // We deliberately do NOT call `Handle::block_on` here — that
        // panics when the caller is already inside a runtime. Instead,
        // poll `try_send` with a short sleep to honour the documented
        // "bounded blocking" semantics (spec 11 § 6.4) without relying
        // on `block_on`. The total wall-clock blocking is bounded by
        // `audit.block_ms_max`.
        let started = std::time::Instant::now();
        let interval = std::time::Duration::from_millis(2);
        while started.elapsed().as_millis() < u128::from(block_ms) {
            match worker.try_send(env_unsent) {
                Ok(()) => return,
                Err(env) => env_unsent = env,
            }
            std::thread::sleep(interval);
        }
        // Channel still full ⇒ spool to disk.
        if let Some(spool) = self.spool.as_ref() {
            match spool.append(&env_unsent) {
                Ok(()) => {
                    note_channel_full(&self.counters, Tier::Audit);
                }
                Err(_) => self.handle_spool_failure(),
            }
        } else {
            self.handle_spool_failure();
        }
    }

    fn handle_spool_failure(&self) {
        // The AUDIT-tier failure path is documented as a deliberate
        // policy escalation (spec 11 § 6.4); the choice between panic /
        // abort / warn_only is taken from `audit.on_failure`. Allow
        // `clippy::panic` here because the panic is the documented
        // escape hatch when the operator picked `Panic` mode.
        #[allow(clippy::panic)]
        {
            let cfg = self.config.load();
            match cfg.audit.on_failure {
                AuditFailureMode::Panic => {
                    panic!("audit spool unwritable; compliance failure")
                }
                AuditFailureMode::Abort => std::process::abort(),
                AuditFailureMode::WarnOnly => {
                    eprintln!("[obs] AUDIT spool unwritable; envelope dropped (warn_only)");
                }
            }
        }
    }

    /// Drain any `*.audit.bin` files left in `audit.spool_dir` by a
    /// prior process. Recovered envelopes are dispatched through the
    /// AUDIT worker (or the sync fallback if no runtime is alive). One
    /// `ObsAuditSpoolRecovered` self-event is emitted at the end with
    /// the total count. Spec 11 § 6.4.
    fn recover_audit_spool(&self) {
        let cfg = self.config.load();
        let dir = cfg.audit.spool_dir.clone();
        if !dir.exists() {
            return;
        }
        let mut total: u64 = 0;
        let report = crate::audit_spool::recover(&dir, |env| {
            total += 1;
            // Re-enqueue: sync if no worker, async otherwise.
            if let Some(worker) = self.workers.audit.as_ref() {
                let _ = worker.try_send(env);
            } else {
                self.dispatch_sync(env, Tier::Audit);
            }
            Ok(())
        });
        if total == 0 {
            let _ = report;
            return;
        }
        let mut env = ObsEnvelope {
            full_name: "obs.runtime.v1.ObsAuditSpoolRecovered".to_string(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
            ..Default::default()
        };
        env.labels
            .insert("record_count".to_string(), total.to_string());
        // Route directly through this observer (the global may not be
        // installed yet at builder-time).
        self.fill_identity(&mut env);
        self.dispatch_sync(env, Tier::Log);
    }

    /// Apply scope auto-fill, head sampling, and tail-buffer push to
    /// `env`. Returns `true` when the envelope should continue down
    /// the per-tier worker; `false` when it was dropped or counted as
    /// a buffer push.
    fn run_emit_pipeline(&self, env: &mut ObsEnvelope, sev: obs_types::Severity) -> bool {
        // Step 3 (post-project): auto-fill from scope frame stack.
        auto_fill_envelope(env);
        // Step 4: head sampler.
        let cfg = self.config.load();
        let inbound = inbound_traceparent_sampled();
        match sample_decide(&cfg.sampling, env.full_name.as_str(), sev, inbound) {
            SamplingDecision::Drop => {
                return false;
            }
            SamplingDecision::Keep => {}
            SamplingDecision::ParentSet { sampled: true } => {
                env.sampling_reason =
                    ::buffa::EnumValue::Known(PSamplingReason::SAMPLING_REASON_OVERRIDE);
            }
            SamplingDecision::ParentSet { sampled: false } => {
                return false;
            }
        }
        // Step 5: tail-on-error push (TRACE/DEBUG only).
        if matches!(sev, obs_types::Severity::Trace | obs_types::Severity::Debug) {
            push_tail_buffer(env);
        } else if sev >= obs_types::Severity::Error {
            crate::scope::mark_error_on_active_scopes();
        }
        true
    }
}

impl Observer for StandardObserver {
    fn emit_envelope(&self, mut env: ObsEnvelope) {
        self.fill_identity(&mut env);
        let sev = match env.sev {
            ::buffa::EnumValue::Known(s) => proto_sev_to_native(s),
            ::buffa::EnumValue::Unknown(_) => obs_types::Severity::Unspecified,
        };
        if !self.run_emit_pipeline(&mut env, sev) {
            return;
        }
        let tier = match env.tier {
            ::buffa::EnumValue::Known(t) => proto_tier_to_native(t),
            ::buffa::EnumValue::Unknown(_) => Tier::Unspecified,
        };
        if let Ok(_h) = tokio::runtime::Handle::try_current() {
            self.dispatch_async(env, tier);
        } else {
            self.dispatch_sync(env, tier);
        }
    }

    fn enabled(&self, callsite: &ObsCallsite) -> bool {
        let filter = self.filter.load();
        // Trust the filter's interest cache; default rejects below
        // its severity floor.
        callsite.default_sev() >= filter.default_level()
            || filter.callsite_interest(callsite) != crate::callsite::Interest::Never
    }

    fn generation(&self) -> u32 {
        self.generation.load(Ordering::Acquire)
    }

    fn reload_filter(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async move {
            for w in [
                self.workers.log.as_ref(),
                self.workers.metric.as_ref(),
                self.workers.trace.as_ref(),
                self.workers.audit.as_ref(),
            ]
            .iter()
            .flatten()
            {
                w.flush().await;
            }
        })
    }

    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async move {
            for w in [
                self.workers.log.as_ref(),
                self.workers.metric.as_ref(),
                self.workers.trace.as_ref(),
                self.workers.audit.as_ref(),
            ]
            .iter()
            .flatten()
            {
                w.shutdown().await;
            }
            if let Some(spool) = self.spool.as_ref() {
                spool.close();
            }
        })
    }

    fn shutdown_blocking(&self, timeout: std::time::Duration) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _ = handle.block_on(tokio::time::timeout(timeout, self.shutdown()));
        }
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_tier_to_native(t: obs_proto::obs::v1::Tier) -> Tier {
    use obs_proto::obs::v1::Tier as P;
    match t {
        P::TIER_UNSPECIFIED => Tier::Unspecified,
        P::TIER_LOG => Tier::Log,
        P::TIER_METRIC => Tier::Metric,
        P::TIER_TRACE => Tier::Trace,
        P::TIER_AUDIT => Tier::Audit,
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_sev_to_native(s: obs_proto::obs::v1::Severity) -> obs_types::Severity {
    use obs_proto::obs::v1::Severity as P;
    match s {
        P::SEVERITY_UNSPECIFIED => obs_types::Severity::Unspecified,
        P::SEVERITY_TRACE => obs_types::Severity::Trace,
        P::SEVERITY_DEBUG => obs_types::Severity::Debug,
        P::SEVERITY_INFO => obs_types::Severity::Info,
        P::SEVERITY_WARN => obs_types::Severity::Warn,
        P::SEVERITY_ERROR => obs_types::Severity::Error,
        P::SEVERITY_FATAL => obs_types::Severity::Fatal,
    }
}

/// Builder for [`StandardObserver`].
pub struct StandardObserverBuilder {
    router: SinkRouter,
    registry: Option<Arc<SchemaRegistry>>,
    config: Option<EventsConfig>,
    filter_spec: Option<String>,
    service: Option<String>,
    instance: Option<String>,
    version: Option<String>,
    spawn_workers: bool,
}

impl Default for StandardObserverBuilder {
    fn default() -> Self {
        Self {
            router: SinkRouter::default(),
            registry: None,
            config: None,
            filter_spec: None,
            service: None,
            instance: None,
            version: None,
            spawn_workers: true,
        }
    }
}

impl std::fmt::Debug for StandardObserverBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandardObserverBuilder")
            .field("service", &self.service)
            .field("version", &self.version)
            .field("spawn_workers", &self.spawn_workers)
            .finish_non_exhaustive()
    }
}

impl StandardObserverBuilder {
    /// Set service identity.
    #[must_use]
    pub fn service(mut self, name: impl Into<String>, version: impl Into<String>) -> Self {
        self.service = Some(name.into());
        self.version = Some(version.into());
        self
    }

    /// Set instance id.
    #[must_use]
    pub fn instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    /// Wire a sink for a specific tier. Calling twice replaces the
    /// prior sink.
    #[must_use]
    pub fn sink_for(mut self, tier: Tier, sink: Arc<dyn Sink>) -> Self {
        match tier {
            Tier::Log => self.router.log = Some(sink),
            Tier::Metric => self.router.metric = Some(sink),
            Tier::Trace => self.router.trace = Some(sink),
            Tier::Audit => self.router.audit = Some(sink),
            _ => {}
        }
        self
    }

    /// Wire a fallback sink.
    #[must_use]
    pub fn sink_fallback(mut self, sink: Arc<dyn Sink>) -> Self {
        self.router.fallback = Some(sink);
        self
    }

    /// Set an explicit config.
    #[must_use]
    pub fn config(mut self, cfg: EventsConfig) -> Self {
        self.config = Some(cfg);
        self
    }

    /// Set the filter spec (overrides anything in `config.filter`).
    #[must_use]
    pub fn filter(mut self, spec: impl Into<String>) -> Self {
        self.filter_spec = Some(spec.into());
        self
    }

    /// Use a specific schema registry.
    #[must_use]
    pub fn registry(mut self, registry: Arc<SchemaRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Spawn per-tier mpsc workers when a tokio runtime is available
    /// (default `true`). Disable for synchronous tests that want
    /// in-emit-thread delivery.
    #[must_use]
    pub fn spawn_workers(mut self, yes: bool) -> Self {
        self.spawn_workers = yes;
        self
    }

    /// Finalise.
    ///
    /// # Errors
    ///
    /// Returns `BuildError` when config validation or filter parsing
    /// fails.
    pub fn build(self) -> Result<StandardObserver, BuildError> {
        let cfg = self.config.unwrap_or_default();
        cfg.validate().map_err(BuildError::InvalidConfig)?;

        let filter_spec = self
            .filter_spec
            .or_else(|| cfg.filter.clone())
            .or_else(|| std::env::var("OBS_FILTER").ok());
        let filter = match filter_spec.as_deref() {
            Some(spec) => Filter::parse(spec).map_err(|e| {
                BuildError::InvalidConfig(crate::config::ConfigError::invalid_range(
                    "filter",
                    format!("{e}"),
                ))
            })?,
            None => Filter::new(),
        };

        let registry = self
            .registry
            .unwrap_or_else(|| Arc::new(SchemaRegistry::from_link_section()));

        // Service defaults from env.
        let service = self
            .service
            .or_else(|| std::env::var("OTEL_SERVICE_NAME").ok())
            .unwrap_or_else(|| "obs".to_string());
        let version = self
            .version
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        let instance = self.instance.unwrap_or_default();

        let counters = Arc::new(WorkerCounters::default());
        let spool = if self.router.audit.is_some() {
            Some(Arc::new(
                SpoolWriter::open(
                    cfg.audit.spool_dir.clone(),
                    cfg.audit.spool_max_bytes,
                    cfg.audit.on_failure,
                )
                .map_err(BuildError::SpoolOpen)?,
            ))
        } else {
            None
        };
        let workers = if self.spawn_workers {
            spawn_pool(&self.router, &registry, &counters, &cfg.queues)
        } else {
            WorkerPool::default()
        };

        let observer = StandardObserver {
            router: self.router,
            workers,
            spool,
            registry,
            config: ArcSwap::from_pointee(cfg),
            filter: ArcSwap::from_pointee(filter),
            counters,
            generation: AtomicU32::new(1),
            service,
            instance,
            version,
            sync_dispatch_lock: Mutex::new(()),
        };
        // Spec 11 § 6.4: at observer init, drain any `*.audit.bin`
        // files left over from a prior process. Each recovered record
        // is enqueued onto the AUDIT worker; one
        // `ObsAuditSpoolRecovered` self-event is emitted with the total
        // count.
        observer.recover_audit_spool();
        Ok(observer)
    }
}

fn spawn_pool(
    router: &SinkRouter,
    registry: &Arc<SchemaRegistry>,
    counters: &Arc<WorkerCounters>,
    queues: &crate::config::QueuesConfig,
) -> WorkerPool {
    let mut pool = WorkerPool::default();
    if let Some(sink) = router.log.as_ref().or(router.fallback.as_ref()) {
        pool.log = spawn_tier_worker(
            Tier::Log,
            queues,
            Arc::clone(sink),
            Arc::clone(registry),
            Arc::clone(counters),
        );
    }
    if let Some(sink) = router.metric.as_ref().or(router.fallback.as_ref()) {
        pool.metric = spawn_tier_worker(
            Tier::Metric,
            queues,
            Arc::clone(sink),
            Arc::clone(registry),
            Arc::clone(counters),
        );
    }
    if let Some(sink) = router.trace.as_ref().or(router.fallback.as_ref()) {
        pool.trace = spawn_tier_worker(
            Tier::Trace,
            queues,
            Arc::clone(sink),
            Arc::clone(registry),
            Arc::clone(counters),
        );
    }
    if let Some(sink) = router.audit.as_ref() {
        pool.audit = spawn_tier_worker(
            Tier::Audit,
            queues,
            Arc::clone(sink),
            Arc::clone(registry),
            Arc::clone(counters),
        );
    }
    pool
}

/// Errors returned by [`StandardObserverBuilder::build`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// Config validation failed.
    #[error("invalid config: {0}")]
    InvalidConfig(#[from] crate::config::ConfigError),
    /// AUDIT spool could not be opened.
    #[error("audit spool open failed: {0}")]
    SpoolOpen(#[source] std::io::Error),
}

#[allow(dead_code)]
fn _ensure_noop_compiles() {
    let _: Arc<dyn Sink> = Arc::new(NoopSink);
}
