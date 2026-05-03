//! Per-task observer routing for multi-tenant services.
//!
//! Spec 60 § 4.3.L / spec 95 § 3.11 / P2-AI. Demonstrates installing
//! a per-task observer override so each tenant's emits ship to a
//! tenant-scoped sink while the global default still catches
//! background work.
//!
//! Run: `cargo run -p obs-example-multi-tenant`

use std::sync::Arc;

use obs_core::{InMemoryObserver, Observer, observer::with_observer_task};
use obs_sdk::{FormatterStyle, Sink, StandardObserver, StdoutSink, install_observer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // Global observer routes anything outside a per-task scope.
    let stdout: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Pretty));
    let global = StandardObserver::builder()
        .service("obs-example-multi-tenant", env!("CARGO_PKG_VERSION"))
        .sink_fallback(stdout)
        .build()?;
    install_observer(global);

    // Two per-tenant observers — these collect to in-memory buffers
    // so the example can print which tenant saw which event. In a
    // real service these would be StandardObservers wired to
    // tenant-scoped OTLP endpoints.
    let alpha_obs = InMemoryObserver::new();
    let alpha_handle = alpha_obs.handle();
    let alpha: Arc<dyn Observer> = Arc::new(alpha_obs);

    let beta_obs = InMemoryObserver::new();
    let beta_handle = beta_obs.handle();
    let beta: Arc<dyn Observer> = Arc::new(beta_obs);

    // Spawn each tenant's work under its own observer.
    let alpha_task = with_observer_task(Arc::clone(&alpha), async {
        emit_some_work("alpha");
    });
    let beta_task = with_observer_task(Arc::clone(&beta), async {
        emit_some_work("beta");
    });
    let _ = tokio::join!(alpha_task, beta_task);

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

fn emit_some_work(tenant: &str) {
    let mut env = obs_proto::obs::v1::ObsEnvelope {
        full_name: "myapp.v1.ObsTenantWorkCompleted".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
        ..Default::default()
    };
    env.labels.insert("tenant".to_string(), tenant.to_string());
    obs_core::observer().emit_envelope(env);
}
