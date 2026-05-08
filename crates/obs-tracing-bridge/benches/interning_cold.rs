//! `bench_interning_cold` — measures the cost of the first interning
//! lookup for a callsite (registry insertion + envelope tagging).
//! Spec 71 § 4.

#![allow(missing_docs, clippy::expect_used)]

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::ObsCallsiteRegistry;
use obs_proto::obs::v1::Severity;
use obs_tracing_bridge::{PrewarmEntry, run_prewarm};

fn bench_interning_cold(c: &mut Criterion) {
    let entries: &[PrewarmEntry] = &[PrewarmEntry {
        target: "myapp::auth",
        anchor: "myapp",
        level: Severity::Info,
        field_names: &["user_id"],
    }];

    c.bench_function("interning_cold_register", |b| {
        b.iter(|| {
            // Fresh registry per iter so every call is the cold path.
            let registry = Arc::new(ObsCallsiteRegistry::new());
            let stats = run_prewarm(&registry, entries);
            std::hint::black_box(stats);
        });
    });
}

criterion_group!(benches, bench_interning_cold);
criterion_main!(benches);
