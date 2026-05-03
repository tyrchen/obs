//! Bench harness for bridge overhead — spec 30 § 7 + spec 71 § 3.2.
//!
//! Two budgets:
//! - `bench_tracing_to_obs_overhead` ≤ 2 µs delta over baseline emit.
//! - `bench_obs_to_tracing_overhead` ≤ 1.5 µs delta over baseline emit.

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{
    Observer, SchemaRegistry, ScrubbedEnvelope,
    observer::{InMemoryObserver, with_test_observer},
};
use obs_proto::obs::v1::ObsEnvelope;
use obs_tracing_bridge::{ObsToTracingSink, TracingToObsLayer};
use tracing_subscriber::layer::SubscriberExt;

fn bench_tracing_to_obs_overhead(c: &mut Criterion) {
    let observer = InMemoryObserver::new();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    // `Layered<TracingToObsLayer, Registry>` is not Clone (Registry isn't),
    // so install the subscriber once for the duration of the bench.
    let subscriber = tracing_subscriber::registry().with(TracingToObsLayer::new());
    let _guard = tracing::subscriber::set_default(subscriber);
    c.bench_function("tracing_to_obs_overhead", |b| {
        with_test_observer(observer.clone(), || {
            b.iter(|| {
                tracing::info!(target: "myapp", route = "list_users", "request done");
            });
        });
    });
}

fn bench_obs_to_tracing_overhead(c: &mut Criterion) {
    let sink = ObsToTracingSink::new();
    let reg = Arc::new(SchemaRegistry::empty());
    let env = ObsEnvelope {
        full_name: "myapp.v1.ObsRequestCompleted".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
        ..Default::default()
    };
    c.bench_function("obs_to_tracing_overhead", |b| {
        b.iter(|| {
            obs_core::Sink::deliver(&sink, ScrubbedEnvelope::for_test(&env, &reg));
        });
    });
}

criterion_group!(
    benches,
    bench_tracing_to_obs_overhead,
    bench_obs_to_tracing_overhead
);
criterion_main!(benches);
