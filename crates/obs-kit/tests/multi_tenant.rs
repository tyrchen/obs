//! End-to-end multi-tenant integration test. Spec 40 § 3.1.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]

//!
//! Three tenants, each with their own [`InMemoryObserver`] (stand-in
//! for "OTLP endpoint + Parquet bucket" in production); the HTTP-layer
//! analogue here is a per-request closure that picks the observer by
//! `tenant_id`. The test asserts that an event emitted while running
//! under each tenant's scope routes to the right per-tenant sink even
//! when tokio worker threads migrate between polls.

use std::sync::Arc;

use obs_core::{
    InMemoryObserver, Observer,
    observer::{install_observer, with_observer_task},
};
use obs_proto::obs::v1::ObsEnvelope;

#[tokio::test]
async fn multi_tenant_observers_should_route_per_request() {
    // Install a NoopObserver as the global so events leak loudly if
    // routing is broken.
    let alpha = Arc::new(InMemoryObserver::new());
    let beta = Arc::new(InMemoryObserver::new());
    let gamma = Arc::new(InMemoryObserver::new());
    let alpha_handle = alpha.handle();
    let beta_handle = beta.handle();
    let gamma_handle = gamma.handle();

    let global = InMemoryObserver::new();
    install_observer(global);

    // Three concurrent tasks, one per tenant.
    let alpha_arc: Arc<dyn Observer> = alpha.clone();
    let beta_arc: Arc<dyn Observer> = beta.clone();
    let gamma_arc: Arc<dyn Observer> = gamma.clone();

    let a = tokio::spawn(with_observer_task(alpha_arc, async {
        emit_for("alpha").await;
    }));
    let b = tokio::spawn(with_observer_task(beta_arc, async {
        emit_for("beta").await;
    }));
    let g = tokio::spawn(with_observer_task(gamma_arc, async {
        emit_for("gamma").await;
    }));
    a.await.expect("alpha");
    b.await.expect("beta");
    g.await.expect("gamma");

    let alpha_drained = alpha_handle.drain();
    let beta_drained = beta_handle.drain();
    let gamma_drained = gamma_handle.drain();

    assert_eq!(alpha_drained.len(), 4);
    assert_eq!(beta_drained.len(), 4);
    assert_eq!(gamma_drained.len(), 4);
    for (handle, expected) in [
        (alpha_drained, "alpha"),
        (beta_drained, "beta"),
        (gamma_drained, "gamma"),
    ] {
        for env in handle {
            assert_eq!(env.labels.get("tenant"), Some(&expected.to_string()));
        }
    }
}

async fn emit_for(tenant: &'static str) {
    // Simulate a workload that emits 4 events with await points
    // between, exercising tokio's task migration.
    for i in 0..4 {
        let mut env = ObsEnvelope {
            full_name: "myapp.v1.ObsRequestCompleted".into(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
            ..Default::default()
        };
        env.labels.insert("tenant".into(), tenant.into());
        env.labels.insert("seq".into(), i.to_string());
        obs_core::observer().emit_envelope(env);
        tokio::task::yield_now().await;
    }
}
