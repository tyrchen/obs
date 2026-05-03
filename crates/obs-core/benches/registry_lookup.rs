#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used
)]

//! `bench_registry_lookup` — `SchemaRegistry::lookup` hot-path cost.
//!
//! Spec 71 § 4 / spec 93 P2-20.

use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{ObsEnvelope, SchemaRegistry};

fn bench_registry(c: &mut Criterion) {
    let registry = SchemaRegistry::from_link_section();
    c.bench_function("registry_lookup_empty_envelope", |b| {
        let env = ObsEnvelope::default();
        b.iter(|| {
            let r = registry.lookup(&env);
            std::hint::black_box(r);
        });
    });
    c.bench_function("registry_init_from_link_section", |b| {
        b.iter(|| {
            let r = SchemaRegistry::from_link_section();
            std::hint::black_box(r.len());
        });
    });
}

criterion_group!(benches, bench_registry);
criterion_main!(benches);
