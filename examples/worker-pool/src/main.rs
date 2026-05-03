//! Example: a worker-pool simulator that emits MEASUREMENT-class
//! events feeding the obs metric tier.
//!
//! Demonstrates:
//! - METRIC-tier `ObsWorkerTaskCompleted` events with `latency_ms` and `queue_depth` MEASUREMENT
//!   fields. Once spec 93 P1-6 lands the codegen `project_metrics` impl, these flow as OTLP `Sum` /
//!   `Histogram` data points; today they are stored as the typed payload with the `MEASUREMENT`
//!   field role flag.
//! - LOG-tier `ObsWorkerStarted` / `ObsWorkerStopped` so OTel / ClickHouse have one row per worker
//!   lifecycle.
//! - `OtlpMetricSink` (real gRPC if `OTEL_EXPORTER_OTLP_ENDPOINT` is set, else
//!   `StdoutDebugExporter`) so you can wire a real collector when one is available.
//! - Stdout fallback for ergonomic visibility.
//!
//! Run:
//!   cargo run -p obs-example-worker-pool -- --workers 4 --tasks 200
//!   OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 \
//!     cargo run -p obs-example-worker-pool -- --workers 8 --tasks 1000

#![allow(missing_docs)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Parser;
use obs_otel::{
    GrpcOtlpExporter, OtlpEndpoint, OtlpMetricSink, OtlpProtocol, OtlpResourceAttrs,
    StdoutDebugExporter,
};
use obs_sdk::{
    Event, FormatterStyle, Sink, StandardObserver, StdoutSink, Tier, install_observer,
    observer as resolve_observer,
};
use tokio::{sync::mpsc, time::sleep};

#[derive(Parser, Debug)]
#[command(name = "obs-example-worker-pool", version)]
struct Cli {
    /// Number of worker tasks running in parallel.
    #[arg(long, default_value_t = 4)]
    workers: u32,
    /// Total tasks to feed into the pool.
    #[arg(long, default_value_t = 200)]
    tasks: u32,
    /// Per-task work latency floor (ms). Real latency adds a small
    /// jitter on top.
    #[arg(long, default_value_t = 5)]
    latency_floor_ms: u64,
}

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsWorkerStarted {
    #[obs(label, cardinality = "low")]
    worker_id: String,
}

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsWorkerStopped {
    #[obs(label, cardinality = "low")]
    worker_id: String,
    #[obs(attribute)]
    tasks_processed: u64,
}

/// One per task completion. The MEASUREMENT-flagged fields drive the
/// metric projection (spec 12 § 3.7); `worker_id` and `task_kind`
/// are LABEL-flagged so OTLP attributes group by them.
#[derive(Debug, Default, Event)]
#[event(tier = "metric", default_sev = "info")]
struct ObsWorkerTaskCompleted {
    #[obs(label, cardinality = "low")]
    worker_id: String,
    #[obs(label, cardinality = "low")]
    task_kind: String,
    #[obs(measurement)]
    latency_ms: u64,
    #[obs(measurement)]
    queue_depth: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    install_observer_with_sinks()?;

    eprintln!(
        "starting worker pool: {workers} workers × {tasks} tasks",
        workers = cli.workers,
        tasks = cli.tasks,
    );

    let (tx, rx) = mpsc::channel::<Task>(cli.tasks as usize);
    let queue_depth = Arc::new(AtomicU64::new(0));

    // Producer: enqueue all the tasks up-front. In a real service
    // this would be the inbound HTTP / message-queue path.
    let producer_qd = Arc::clone(&queue_depth);
    let producer = tokio::spawn(async move {
        for i in 0..cli.tasks {
            let kind = if i.is_multiple_of(5) {
                "render"
            } else {
                "fetch"
            };
            producer_qd.fetch_add(1, Ordering::Relaxed);
            if tx
                .send(Task {
                    kind: kind.to_string(),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Workers: pull from the channel, do simulated work, emit a
    // MEASUREMENT event per completion.
    let mut handles = Vec::new();
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    for w in 0..cli.workers {
        let worker_id = format!("w-{w}");
        let rx = Arc::clone(&rx);
        let qd = Arc::clone(&queue_depth);
        let floor = cli.latency_floor_ms;
        handles.push(tokio::spawn(async move {
            ObsWorkerStarted::builder()
                .worker_id(worker_id.clone())
                .emit();
            let mut processed: u64 = 0;
            loop {
                let task = {
                    let mut g = rx.lock().await;
                    g.recv().await
                };
                let Some(task) = task else { break };
                let started = Instant::now();
                qd.fetch_sub(1, Ordering::Relaxed);
                // Simulate work; real workers would do IO / CPU here.
                sleep(Duration::from_millis(floor + (processed % 7))).await;
                let latency_ms: u64 =
                    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                ObsWorkerTaskCompleted::builder()
                    .worker_id(worker_id.clone())
                    .task_kind(task.kind)
                    .latency_ms(latency_ms)
                    .queue_depth(qd.load(Ordering::Relaxed))
                    .emit();
                processed += 1;
            }
            ObsWorkerStopped::builder()
                .worker_id(worker_id)
                .tasks_processed(processed)
                .emit();
            processed
        }));
    }

    let _ = producer.await;
    drop(rx);
    let mut total: u64 = 0;
    for h in handles {
        if let Ok(n) = h.await {
            total += n;
        }
    }
    eprintln!("processed {total} tasks; flushing observer...");

    // Async drain — see batch-pipeline for the equivalent. Calling
    // shutdown_blocking from #[tokio::main] is also safe now (it
    // detects multi-thread runtime + uses block_in_place), but the
    // async path is the cleanest from in-runtime code.
    let _ = tokio::time::timeout(Duration::from_secs(5), resolve_observer().shutdown()).await;
    Ok(())
}

#[derive(Debug)]
struct Task {
    kind: String,
}

fn install_observer_with_sinks() -> Result<()> {
    let stdout: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Compact));

    // OTLP metric sink — real gRPC when an endpoint is configured,
    // else the bundled stdout debug exporter so the example always
    // produces visible output.
    let exporter: Arc<dyn obs_otel::OtlpExporter> =
        match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok() {
            Some(url) => {
                eprintln!("OTLP enabled: {url}");
                let endpoint = OtlpEndpoint {
                    url,
                    protocol: OtlpProtocol::Grpc,
                    headers: Default::default(),
                    compression: String::new(),
                    timeout_ms: 5_000,
                };
                Arc::new(GrpcOtlpExporter::connect(&endpoint)?)
            }
            None => {
                eprintln!("no OTLP endpoint set; using StdoutDebugExporter");
                Arc::new(StdoutDebugExporter)
            }
        };
    let resource = OtlpResourceAttrs {
        service_name: "obs-example-worker-pool".to_string(),
        service_version: env!("CARGO_PKG_VERSION").to_string(),
        extra: Default::default(),
    };
    let endpoint = OtlpEndpoint {
        url: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default(),
        protocol: OtlpProtocol::Grpc,
        headers: Default::default(),
        compression: String::new(),
        timeout_ms: 5_000,
    };
    let metric_sink: Arc<dyn Sink> = Arc::new(
        OtlpMetricSink::builder()
            .exporter(exporter)
            .resource(resource)
            .endpoint(endpoint)
            .build()?,
    );

    let observer = StandardObserver::builder()
        .service("obs-example-worker-pool", env!("CARGO_PKG_VERSION"))
        .instance("local")
        .sink_for(Tier::Metric, metric_sink)
        .sink_fallback(stdout)
        .build()?;
    install_observer(observer);
    Ok(())
}
