//! Example: TodoMVC HTTP backend wired end-to-end with the obs SDK.
//!
//! - Proto-first authoring: `proto/todomvc/v1/events.proto` → `obs-build` in `build.rs` → typed
//!   builders under `todomvc::v1::*`.
//! - Per-request scope frame via `obs::scope!`, plus `ObsHttpLayer` for W3C `traceparent` parsing.
//! - Dual-sink fan-out: pretty stdout for human eyes during dev, daily rolling NDJSON file for `obs
//!   tail` / `obs query`.

#![allow(missing_docs)]

mod handlers;
mod schema;
mod store;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use axum::{
    Router,
    routing::{get, patch, post},
};
use clap::Parser;
use obs_kit::{
    FormatterStyle, NdjsonFileSink, RollingFileWriter, RollingPolicy, Sink, StandardObserver,
    StdoutSink, Tier, install_observer, observer,
};
use obs_tower::ObsHttpLayer;
use tokio::net::TcpListener;
use tower::ServiceBuilder;

use crate::store::TodoStore;

#[derive(Debug, Parser)]
#[command(
    name = "obs-example-todomvc",
    about = "TodoMVC backend with proto-first obs events."
)]
struct Cli {
    /// TCP port to bind.
    #[arg(long, default_value_t = 8090)]
    port: u16,
    /// NDJSON file path. The directory is used by `RollingFileWriter`
    /// and the basename becomes the daily prefix.
    #[arg(long, default_value = "./obs-out/todomvc.ndjson")]
    ndjson: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct AppState {
    pub(crate) todos: TodoStore,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    install_observer_with_sinks(&cli.ndjson)?;

    let state = AppState {
        todos: TodoStore::new(),
    };
    let app = Router::new()
        .route("/healthz", get(handlers::healthz))
        .route(
            "/todos",
            post(handlers::create_todo).get(handlers::list_todos),
        )
        .route(
            "/todos/{id}",
            patch(handlers::patch_todo).delete(handlers::delete_todo),
        )
        .with_state(state)
        .layer(ServiceBuilder::new().layer(ObsHttpLayer::<axum::body::Body>::server()));

    let addr: SocketAddr = format!("127.0.0.1:{}", cli.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    eprintln!("obs-example-todomvc listening on http://{addr}");
    eprintln!("ndjson sink: {}", cli.ndjson.display());

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    observer().shutdown().await;
    Ok(())
}

fn install_observer_with_sinks(ndjson_path: &Path) -> Result<()> {
    let dir = ndjson_path.parent().unwrap_or_else(|| Path::new("."));
    let stem = ndjson_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("todomvc");

    let writer = RollingFileWriter::builder()
        .directory(dir)
        .filename_prefix(stem)
        .policy(RollingPolicy::Daily)
        .build()
        .context("build rolling file writer")?;
    let ndjson: Arc<dyn Sink> = Arc::new(NdjsonFileSink::new(writer));
    let stdout_pretty: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Pretty));

    // Tier-routed sinks: LOG and METRIC events flow into the daily
    // NDJSON file (which `obs tail` / `obs query` consume); anything
    // not explicitly routed (TRACE, AUDIT) falls back to pretty
    // stdout. Spec 11 § 6.
    let observer = StandardObserver::builder()
        .service("obs-example-todomvc", env!("CARGO_PKG_VERSION"))
        .instance(hostname())
        .sink_fallback(stdout_pretty)
        .sink_for(Tier::Log, Arc::clone(&ndjson))
        .sink_for(Tier::Metric, ndjson)
        .build()
        .context("build StandardObserver")?;
    install_observer(observer);
    Ok(())
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "local".to_string())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("shutting down");
}
