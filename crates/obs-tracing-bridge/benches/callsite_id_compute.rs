//! `bench_callsite_id_compute` — measures the cost of the BLAKE3-based
//! 64-bit callsite id computation in `obs_core::callsite_id`. Spec 71
//! § 4 / spec 31 § 3.1.

#![allow(missing_docs)]

use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{CallsiteSource, callsite_id};
use obs_types::Severity;

fn bench_callsite_id_compute(c: &mut Criterion) {
    let field_names: &[&str] = &["user_id", "tenant", "route"];
    c.bench_function("callsite_id_compute", |b| {
        b.iter(|| {
            let id = callsite_id(
                CallsiteSource::TracingEvent,
                "myapp::auth",
                "src/auth.rs",
                Some(42),
                Severity::Info,
                field_names,
                "user logged in",
            );
            std::hint::black_box(id);
        });
    });
}

criterion_group!(benches, bench_callsite_id_compute);
criterion_main!(benches);
