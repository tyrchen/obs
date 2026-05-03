//! Migrate a tracing-only service to obs typed events.
//!
//! Spec 60 § 4.3.J / spec 95 § 3.11 / P2-AI. Three steps:
//!
//! 1. Baseline: a service that uses only `tracing::info!` / `tracing::warn!`.
//! 2. Wire `obs_tracing_bridge::init(...)` so every existing `tracing::*` call lands on the obs
//!    runtime.
//! 3. Promote one hot call site to a typed `#[derive(obs::Event)]` schema; the rest of the codebase
//!    keeps working unchanged.
//!
//! Run: `cargo run -p obs-example-tracing-migration`

use std::sync::Arc;

use obs_sdk::{Event, FormatterStyle, Sink, StandardObserver, StdoutSink, install_observer};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsCheckoutAttempted {
    #[obs(label, cardinality = "low")]
    sku: String,
    #[obs(measurement, metric = "counter", unit = "1")]
    qty: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // Step 1+2: install the bridge alongside an obs observer. The
    // bridge's `init()` registers a tracing subscriber that forwards
    // every `tracing::*` event to the active obs observer.
    let stdout: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Pretty));
    let observer = StandardObserver::builder()
        .service("obs-example-tracing-migration", env!("CARGO_PKG_VERSION"))
        .sink_fallback(stdout)
        .build()?;
    install_observer(observer);

    // Bridge: legacy `tracing::*` macros now produce obs envelopes.
    obs_tracing_bridge::init::<&str>(None).map_err(|e| anyhow::anyhow!("install bridge: {e}"))?;

    println!("\n--- step 1: legacy tracing emit (bridged to obs) ---");
    tracing::info!(
        target = "myapp::checkout",
        sku = "ABC-001",
        qty = 1,
        "checkout attempted"
    );
    tracing::warn!(
        target = "myapp::checkout",
        sku = "OOS-001",
        qty = 0,
        "out of stock"
    );

    println!("\n--- step 3: same emit, now typed ---");
    ObsCheckoutAttempted::builder()
        .sku("ABC-001".to_string())
        .qty(1u64)
        .emit();

    Ok(())
}
