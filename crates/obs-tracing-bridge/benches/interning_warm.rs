//! `bench_interning_warm` — measures the cost of an interning lookup
//! after the callsite has been registered (the steady-state hot path).
//! Spec 71 § 4.

#![allow(missing_docs, clippy::expect_used)]

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::ObsCallsiteRegistry;
use obs_proto::obs::v1::Severity;
use obs_tracing_bridge::intern_or_lookup;

fn bench_interning_warm(c: &mut Criterion) {
    let registry = Arc::new(ObsCallsiteRegistry::new());
    // Prime the registry once. Subsequent calls hit the warm path.
    let _ = intern_or_lookup(
        &registry,
        "myapp::auth",
        "login",
        "myapp::auth",
        "src/auth.rs",
        Some(42),
        Severity::Info,
        &["user_id"],
        "user logged in",
    );

    c.bench_function("interning_warm_lookup", |b| {
        b.iter(|| {
            let (id, _new) = intern_or_lookup(
                &registry,
                "myapp::auth",
                "login",
                "myapp::auth",
                "src/auth.rs",
                Some(42),
                Severity::Info,
                &["user_id"],
                "user logged in",
            );
            std::hint::black_box(id);
        });
    });
}

criterion_group!(benches, bench_interning_warm);
criterion_main!(benches);
