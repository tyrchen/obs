//! Per-task observer routing for multi-tenant services.
//!
//! Demonstrates `with_observer_task`: each tenant runs under its own
//! observer so its emits land in a tenant-scoped sink (here an
//! `InMemoryObserver` for visibility; in a real deployment you'd swap
//! in a `StandardObserver` wired to per-tenant OTLP / Parquet).
//!
//! Run: `cargo run -p obs-example-multi-tenant`

#![allow(missing_docs)]

use std::sync::Arc;

use obs_core::{InMemoryObserver, Observer, observer::with_observer_task};
use obs_sdk::{
    Event, FormatterStyle, Severity, Sink, StandardObserver, StdoutSink, install_observer,
};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsTenantWorkCompleted {
    #[obs(label, cardinality = "low")]
    tenant: String,
    #[obs(label, cardinality = "low")]
    operation: String,
    #[obs(attribute)]
    items_processed: u32,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let stdout: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Pretty));
    let global = StandardObserver::builder()
        .service("obs-example-multi-tenant", env!("CARGO_PKG_VERSION"))
        .sink_fallback(stdout)
        .build()?;
    install_observer(global);

    let alpha_obs = InMemoryObserver::new();
    let alpha_handle = alpha_obs.handle();
    let alpha: Arc<dyn Observer> = Arc::new(alpha_obs);

    let beta_obs = InMemoryObserver::new();
    let beta_handle = beta_obs.handle();
    let beta: Arc<dyn Observer> = Arc::new(beta_obs);

    let alpha_task = with_observer_task(Arc::clone(&alpha), async {
        do_tenant_work("alpha", 12);
        do_tenant_work("alpha", 7);
    });
    let beta_task = with_observer_task(Arc::clone(&beta), async {
        do_tenant_work("beta", 3);
    });
    let _ = tokio::join!(alpha_task, beta_task);

    ObsTenantWorkCompleted::builder()
        .tenant("control-plane".to_string())
        .operation("nightly-rollup".to_string())
        .items_processed(1u32)
        .emit_at(Severity::Info);

    println!(
        "\n--- alpha tenant captured {} envelope(s) ---",
        alpha_handle.drain().len()
    );
    println!(
        "--- beta tenant captured {} envelope(s) ---",
        beta_handle.drain().len()
    );
    Ok(())
}

fn do_tenant_work(tenant: &str, items: u32) {
    ObsTenantWorkCompleted::builder()
        .tenant(tenant.to_string())
        .operation("batch-import".to_string())
        .items_processed(items)
        .emit();
}
