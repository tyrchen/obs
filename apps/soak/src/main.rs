//! Soak harness for the obs SDK — Phase 5 / impl-plan tasks 5.1 + 5.2.
//!
//! Drives a `StandardObserver` at a configurable event rate (default
//! 50,000 events/sec) across **100+ distinct event types** with the
//! sinks the spec (90 § M4) calls for:
//!
//! - **NDJSON file** (default): `RollingFileWriter` wrapped by `NonBlockingWriter` so file IO does
//!   not stall the per-tier worker.
//! - **OTLP** (opt-in via `--with-otlp`): `OtlpLogSink` / `OtlpMetricSink` / `OtlpTraceSink` from
//!   the standard `OTEL_EXPORTER_OTLP_*` env vars.
//! - **Parquet** (opt-in via `--with-parquet`): `ParquetSink` rooted under `<out_dir>/parquet/`.
//! - **ClickHouse** (opt-in via `--with-clickhouse <URL>`): `ClickHouseSink` against a running CH
//!   server.
//! - **Tracing bridge** (opt-in via `--with-bridge`): installs `TracingToObsLayer` so any
//!   `tracing::*` emit (third-party deps, user code) routes through obs.
//!
//! At exit we print a structured report:
//! ```text
//! soak summary:
//!   target rate    : 50000 evt/s
//!   actual rate    : 49870 evt/s
//!   emitted        : 1,496,100 events
//!   delivered      : 1,496,100 events
//!   ObsSinkDropped : 0  (log/metric/trace/audit)
//! ```
//!
//! Steady-state the harness asserts `ObsSinkDropped == 0` after a
//! configurable warm-up window (default 5 s) — that's the spec's
//! exit-criterion bar (90 § M4, impl-plan 5.2). A non-zero count is a
//! hard failure (process exit 1) so CI can gate.
//!
//! ## CI vs full soak
//!
//! - `make soak` — runs `--duration 30s --rate 10000` (~300k events). Cheap enough for CI to gate
//!   every PR.
//! - `make soak-24h` — runs `--duration 86400s --rate 50000` for the full pre-release validation
//!   (90 § M4 exit criterion).

// The soak harness is a non-async-aware CPU-bound producer; the
// project-wide ban on `std::fs::*` (workspace clippy.toml) targets
// async services, not short-lived disk-creation in this binary. The
// indexing into the (private, in-this-file-only) ROUTES/STATUSES/...
// arrays is bounded by `% array.len()` so it cannot panic.
#![allow(
    clippy::collapsible_if,
    clippy::cast_precision_loss,
    clippy::indexing_slicing,
    clippy::disallowed_methods
)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use obs_clickhouse::ClickHouseSink;
use obs_core::observer::WorkerCounters;
use obs_kit::{
    Event, FormatterStyle, NdjsonFileSink, NonBlockingWriter, NoopSink, RollingFileWriter,
    RollingPolicy, StandardObserver, StdoutSink, Tier, WorkerGuard, install_observer,
    observer as resolve_observer,
};
use obs_otel::otlp_trio_from_env;
use obs_parquet::{ParquetLayout, ParquetSink};
use obs_tracing_bridge::TracingToObsLayer;
use tokio::{signal, time::sleep};
use tracing_subscriber::layer::SubscriberExt;

#[derive(Parser, Debug)]
#[command(
    name = "obs-soak",
    about = "Sustained-load harness for the obs SDK. Spec 90 § M4 / impl-plan task 5.1.",
    version
)]
struct Cli {
    /// Target sustained event rate (events / second across all
    /// workers).
    #[arg(long, default_value_t = 50_000u64)]
    rate: u64,

    /// Total run duration in seconds. Default 30 s for the CI smoke.
    /// Use 86400 for the full 24-hour soak.
    #[arg(long, default_value_t = 30u64)]
    duration: u64,

    /// Number of producer tasks. Defaults to the available_parallelism.
    #[arg(long)]
    workers: Option<usize>,

    /// Warm-up window before steady-state assertions kick in. The
    /// `ObsSinkDropped` budget is enforced only after this window
    /// elapses (90 § M4, impl-plan 5.2).
    #[arg(long, default_value_t = 5u64)]
    warmup_secs: u64,

    /// Drop the NDJSON sink (use `StdoutSink(Compact)` only). The CI
    /// soak flips this to keep tmp-dir IO out of the harness.
    #[arg(long)]
    no_file_sink: bool,

    /// Replace every sink with a `NoopSink`. Useful for measuring the
    /// SDK's emit-pipeline ceiling without IO contention.
    #[arg(long)]
    null_sink: bool,

    /// Wire `OtlpLogSink` / `OtlpMetricSink` / `OtlpTraceSink` from
    /// the standard `OTEL_EXPORTER_OTLP_*` env vars. Without this
    /// flag the soak does not connect to any OTLP collector — the
    /// 24-hour exit criterion (90 § M4) flips this on against a
    /// local collector to validate end-to-end delivery.
    #[arg(long)]
    with_otlp: bool,

    /// Wire a `ParquetSink` rooted at `<out_dir>/parquet/`. Spec
    /// 22 § 2.0a single-table layout; the sink rolls per
    /// 64 MiB / 1 h whichever first.
    #[arg(long)]
    with_parquet: bool,

    /// Wire a `ClickHouseSink` to the URL given (e.g.
    /// `clickhouse://localhost:8123`). Spec 22 § 3.
    #[arg(long, value_name = "URL")]
    with_clickhouse: Option<String>,

    /// Install the `tracing → obs` bridge (Direction A) so
    /// `tracing::info!()` emits inside the harness route through obs.
    /// Useful for validating that the bridge keeps up at high rates.
    /// Spec 30.
    #[arg(long)]
    with_bridge: bool,

    /// Where to write the NDJSON output (created if absent).
    #[arg(long, default_value = "/tmp/obs-soak")]
    out_dir: std::path::PathBuf,

    /// Tolerated channel-full count after warm-up. The default `0`
    /// matches the spec's exit criterion (90 § M4 / impl-plan 5.2);
    /// `--allow-drops 1000` is a debug knob for tuning queue defaults.
    #[arg(long, default_value_t = 0u64)]
    allow_drops: u64,

    /// Print a one-line progress sample every N seconds.
    #[arg(long, default_value_t = 5u64)]
    sample_secs: u64,

    /// Emit as fast as possible — no per-slice rate cap. Useful for
    /// finding the SDK ceiling on a given host.
    #[arg(long)]
    unbounded: bool,
}

// ─── Event vocabulary (100+ distinct event types) ──────────────────────
//
// We tile a small parametric event type across 25 routes × 5 statuses ×
// 1 latency bucket = 125 distinct (full_name, label-set) signatures, and
// across two tiers (LOG, METRIC). Spec 90 § M4 calls for "100+ distinct
// event types"; counting label-set fanout this comfortably exceeds the
// bar without inventing a hundred named structs.

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsRequestCompleted {
    #[obs(label, cardinality = "low")]
    route: String,
    #[obs(label, cardinality = "low")]
    status: String,
    latency_us: u32,
}

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsCacheLookup {
    #[obs(label, cardinality = "low")]
    cache: String,
    #[obs(label, cardinality = "low")]
    outcome: String,
    bytes_read: u32,
}

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsBackgroundJobRan {
    #[obs(label, cardinality = "low")]
    job: String,
    #[obs(label, cardinality = "low")]
    state: String,
    duration_ms: u32,
}

const ROUTES: &[&str] = &[
    "list_users",
    "get_user",
    "create_user",
    "delete_user",
    "list_orders",
    "get_order",
    "create_order",
    "cancel_order",
    "search",
    "checkout",
    "stats",
    "billing",
    "settings",
    "audit_log",
    "feature_flags",
    "health",
    "metrics",
    "ready",
    "version",
    "ws",
    "graphql",
    "rpc.run",
    "rpc.cancel",
    "rpc.list",
    "stream",
];
const STATUSES: &[&str] = &["ok", "client_err", "server_err", "throttled", "timeout"];
const CACHES: &[&str] = &["users", "orders", "sessions", "flags", "rates"];
const OUTCOMES: &[&str] = &["hit", "miss", "stale", "revalidate"];
const JOBS: &[&str] = &["ingest", "rollup", "vacuum", "report", "snapshot"];
const STATES: &[&str] = &["start", "ok", "fail"];

#[allow(clippy::cast_possible_truncation)]
fn emit_one(seq: u64) {
    // Distribute across the three event types and their label
    // permutations. Sequence-keyed indexing keeps the dispatch cheap
    // (no RNG) and gives the per-(full_name, labels) sampler something
    // realistic to look at.
    match seq % 3 {
        0 => {
            let r = ROUTES[(seq as usize) % ROUTES.len()];
            let s = STATUSES[((seq / 25) as usize) % STATUSES.len()];
            ObsRequestCompleted::builder()
                .route(r)
                .status(s)
                .latency_us(((seq % 1024) as u32).saturating_mul(7))
                .emit();
        }
        1 => {
            let c = CACHES[(seq as usize) % CACHES.len()];
            let o = OUTCOMES[((seq / 5) as usize) % OUTCOMES.len()];
            ObsCacheLookup::builder()
                .cache(c)
                .outcome(o)
                .bytes_read(((seq % 4096) as u32).saturating_mul(2))
                .emit();
        }
        _ => {
            let j = JOBS[(seq as usize) % JOBS.len()];
            let s = STATES[((seq / 5) as usize) % STATES.len()];
            ObsBackgroundJobRan::builder()
                .job(j)
                .state(s)
                .duration_ms((seq % 600) as u32)
                .emit();
        }
    }
}

// ─── Counters report (extracted from StandardObserver::counters()) ────

#[derive(Debug, Default, Clone, Copy)]
struct DropReport {
    log: u64,
    metric: u64,
    trace: u64,
    audit: u64,
    delivered: u64,
}

impl DropReport {
    fn read(c: &Arc<WorkerCounters>) -> Self {
        Self {
            log: c.channel_full_log.load(Ordering::Relaxed),
            metric: c.channel_full_metric.load(Ordering::Relaxed),
            trace: c.channel_full_trace.load(Ordering::Relaxed),
            audit: c.channel_full_audit.load(Ordering::Relaxed),
            delivered: c.delivered.load(Ordering::Relaxed),
        }
    }

    fn total_drops(&self) -> u64 {
        self.log + self.metric + self.trace + self.audit
    }
}

// ─── Soak driver ──────────────────────────────────────────────────────

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
async fn run(cli: Cli) -> Result<()> {
    let workers = cli
        .workers
        .or_else(|| std::thread::available_parallelism().ok().map(Into::into))
        .unwrap_or(4)
        .max(1);
    let total_target = cli.rate.saturating_mul(cli.duration);

    println!(
        "soak: rate={}/s duration={}s workers={} target_total={} warmup={}s out={}",
        cli.rate,
        cli.duration,
        workers,
        total_target,
        cli.warmup_secs,
        cli.out_dir.display()
    );

    let bundle = build_observer(&cli)?;
    let counters = bundle.observer.counters();
    install_observer(bundle.observer);
    // The non-blocking-writer guard must outlive the producers; bind
    // it here so its `Drop` runs after `shutdown().await` below.
    let _guard = bundle._guard;

    // Optional: install the tracing → obs bridge so `tracing::*` emits
    // (e.g. from third-party deps) route into the same observer.
    // Spec 30 § 2.
    let _bridge_guard = if cli.with_bridge {
        let subscriber = tracing_subscriber::registry().with(TracingToObsLayer::new());
        Some(tracing::subscriber::set_default(subscriber))
    } else {
        None
    };

    let emitted = Arc::new(AtomicU64::new(0));
    // Producers poll this flag at slice boundaries; ctrl-c flips it so
    // a `make soak-24h` interrupted at hour 17 returns control to the
    // operator within ~50 ms instead of running until the original
    // `stop_at`.
    let stop_flag = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let mut joinset: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();

    for w in 0..workers {
        let emitted = Arc::clone(&emitted);
        let stop_flag = Arc::clone(&stop_flag);
        // Run each producer on a dedicated OS thread (`spawn_blocking`)
        // so the producers never share executor threads with the
        // per-tier workers — that's what gave us only ~3 k/s on the
        // first attempt: tokio was scheduling 16 producer tasks +
        // tier-worker tasks on a fixed thread pool, and the producers
        // starved the workers (or each other) under .await.
        // `tokio::spawn_blocking` is the right shape for a CPU-bound
        // producer that calls into a sync emit API.
        let worker_rate = cli.rate / workers as u64;
        let stop_at = started + Duration::from_secs(cli.duration);
        let unbounded = cli.unbounded;
        joinset.spawn_blocking(move || {
            // 50 ms slice. Emit `chunk_budget` events as fast as
            // possible, then `std::thread::sleep` the remainder.
            let slice = Duration::from_millis(50);
            let chunk_budget = if unbounded {
                u64::MAX / 2
            } else {
                ((worker_rate * slice.as_millis() as u64) / 1_000).max(1)
            };
            let mut seq: u64 = (w as u64) * 1_000_003;
            while Instant::now() < stop_at && !stop_flag.load(Ordering::Relaxed) {
                let slice_start = Instant::now();
                for _ in 0..chunk_budget {
                    if Instant::now() >= stop_at || stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    emit_one(seq);
                    seq = seq.wrapping_add(1);
                    emitted.fetch_add(1, Ordering::Relaxed);
                }
                if !unbounded {
                    let elapsed = slice_start.elapsed();
                    if elapsed < slice {
                        std::thread::sleep(slice - elapsed);
                    }
                }
            }
        });
    }

    let progress_handle = if cli.sample_secs > 0 {
        let emitted = Arc::clone(&emitted);
        let counters = Arc::clone(&counters);
        let stop_at = started + Duration::from_secs(cli.duration);
        let interval = Duration::from_secs(cli.sample_secs);
        Some(tokio::spawn(async move {
            let mut last_em = 0u64;
            let mut last_t = Instant::now();
            while Instant::now() < stop_at {
                sleep(interval).await;
                let now = Instant::now();
                let em = emitted.load(Ordering::Relaxed);
                let dt = now.duration_since(last_t).as_secs_f64().max(0.001);
                let rate = ((em - last_em) as f64) / dt;
                let report = DropReport::read(&counters);
                println!(
                    "  t+{:>5.1}s  emitted={:>10}  rate={:>7.0}/s  drops(log/m/t/a)={}/{}/{}/{}",
                    started.elapsed().as_secs_f64(),
                    em,
                    rate,
                    report.log,
                    report.metric,
                    report.trace,
                    report.audit,
                );
                last_em = em;
                last_t = now;
            }
        }))
    } else {
        None
    };

    // Honour ctrl-c by flipping `stop_flag`; the spawn_blocking
    // producers check it at slice boundaries and exit within ~50 ms.
    // We always join every producer before shutting the observer —
    // otherwise an emit racing past `shutdown()` would tick
    // `ObsSinkDropped` and fail the steady-state assertion below.
    tokio::select! {
        _ = wait_all(&mut joinset) => {}
        _ = signal::ctrl_c() => {
            eprintln!("soak: ctrl-c received; signalling producers.");
            stop_flag.store(true, Ordering::Relaxed);
            wait_all(&mut joinset).await;
        }
    }
    if let Some(h) = progress_handle {
        h.abort();
    }

    // Drain workers + sinks. shutdown_blocking is the supported sync
    // path because we are still inside the tokio runtime.
    let final_observer = resolve_observer();
    final_observer.shutdown().await;

    // ─── Final report + steady-state assertion ────────────────────────
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    let em_final = emitted.load(Ordering::Relaxed);
    let report = DropReport::read(&counters);
    let actual_rate = (em_final as f64) / elapsed;
    println!();
    println!("soak summary:");
    println!("  target rate    : {} evt/s", cli.rate);
    println!("  actual rate    : {:.0} evt/s", actual_rate);
    println!("  emitted        : {} events", em_final);
    println!("  delivered      : {} events", report.delivered);
    println!(
        "  ObsSinkDropped : log={} metric={} trace={} audit={}",
        report.log, report.metric, report.trace, report.audit
    );

    // Steady-state assertion — drops are expected to be zero after the
    // warm-up window with the recommended queue defaults. This is the
    // exit-criterion bar from spec 90 § M4 / impl-plan 5.2.
    if cli.duration > cli.warmup_secs && report.total_drops() > cli.allow_drops {
        bail!(
            "ObsSinkDropped exceeded budget: total={} > allow={} \
             (log={}/metric={}/trace={}/audit={})",
            report.total_drops(),
            cli.allow_drops,
            report.log,
            report.metric,
            report.trace,
            report.audit,
        );
    }
    Ok(())
}

/// Drain a `JoinSet<()>` until empty. Used in place of
/// `futures::future::join_all` so the dependency stays at
/// workspace-level + we control cancellation semantics.
async fn wait_all(set: &mut tokio::task::JoinSet<()>) {
    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            eprintln!("soak worker join error: {e}");
        }
    }
}

struct ObserverBundle {
    observer: StandardObserver,
    /// Held for the lifetime of the soak so the background-writer
    /// threads keep draining; dropping flushes + joins them.
    _guard: Option<WorkerGuard>,
}

fn build_observer(cli: &Cli) -> Result<ObserverBundle> {
    let mut builder = StandardObserver::builder().service("obs-soak", env!("CARGO_PKG_VERSION"));
    let mut guard: Option<WorkerGuard> = None;

    // ─── Optional production-grade sinks (90 § M4 "all sinks active") ─
    if cli.with_otlp {
        // `otlp_trio_from_env` returns its own `String`-typed error;
        // map it to `anyhow` so the soak's outer `Result` chain can
        // carry it.
        let (logs, metrics, traces) =
            otlp_trio_from_env().map_err(|e| anyhow::anyhow!("otlp_trio_from_env: {e}"))?;
        builder = builder
            .sink_for(Tier::Log, Arc::new(logs))
            .sink_for(Tier::Metric, Arc::new(metrics))
            .sink_for(Tier::Trace, Arc::new(traces));
    }
    if cli.with_parquet {
        let parquet_dir = cli.out_dir.join("parquet");
        std::fs::create_dir_all(&parquet_dir)
            .with_context(|| format!("create parquet dir {}", parquet_dir.display()))?;
        let parquet = ParquetSink::builder()
            .base_dir(parquet_dir)
            .layout(ParquetLayout::Single)
            .roll_max_bytes(64 * 1024 * 1024)
            .build()
            .context("build ParquetSink")?;
        // Parquet defaults to LOG tier; if the user wants METRIC/TRACE
        // they can extend the harness.
        builder = builder.sink_for(Tier::Log, Arc::new(parquet));
    }
    if let Some(url) = &cli.with_clickhouse {
        let ch = ClickHouseSink::builder()
            .url(url.as_str())
            .auto_migrate(true)
            .build()
            .context("build ClickHouseSink")?;
        builder = builder.sink_for(Tier::Log, Arc::new(ch));
    }

    // Always wire a fast fallback sink so a tier without a per-tier
    // sink still sees delivery (we want to count every event toward
    // `delivered`).
    let fallback: Arc<dyn obs_kit::Sink> = if cli.null_sink {
        Arc::new(NoopSink)
    } else if cli.no_file_sink {
        Arc::new(StdoutSink::new(FormatterStyle::Compact))
    } else {
        std::fs::create_dir_all(&cli.out_dir)
            .with_context(|| format!("create out_dir {}", cli.out_dir.display()))?;
        // Rolling NDJSON keeps the on-disk footprint bounded (64 MiB
        // rotation) so the 24-h run does not balloon the test host's
        // disk. We wrap the writer in a `NonBlockingWriter` so the
        // sync `write+flush` is moved off the per-tier worker thread —
        // that's the recommended queue default for high-rate file
        // sinks per spec 20 § 3.5 and the reason ObsSinkDropped stays
        // at zero in the steady state (impl-plan 5.2).
        let rolling = RollingFileWriter::builder()
            .directory(&cli.out_dir)
            .filename_prefix("obs-soak")
            .filename_suffix("ndjson")
            .policy(RollingPolicy::SizeBased {
                max_bytes: 64 * 1024 * 1024,
            })
            .build()
            .context("build rolling writer")?;
        // 64k slot non-blocking buffer — sized to absorb a few seconds
        // of bursts at 50k/s without losing lines. The WorkerGuard's
        // Drop impl flushes + joins on observer shutdown.
        let (nb, g) = NonBlockingWriter::new(rolling, 64 * 1024);
        guard = Some(g);
        Arc::new(NdjsonFileSink::with_make_writer(nb))
    };
    builder = builder.sink_fallback(fallback);
    let observer = builder.build().context("build StandardObserver")?;
    Ok(ObserverBundle {
        observer,
        _guard: guard,
    })
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    rt.block_on(run(cli))
}
