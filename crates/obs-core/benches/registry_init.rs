//! `bench_registry_init` — measures the cold cost of building a
//! `SchemaRegistry` from `linkme`'s distributed slice. Spec 71 § 4.

#![allow(missing_docs)]

use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::SchemaRegistry;

fn bench_registry_init(c: &mut Criterion) {
    c.bench_function("registry_init_from_link_section", |b| {
        b.iter(|| {
            let r = SchemaRegistry::from_link_section();
            std::hint::black_box(&r);
        });
    });

    c.bench_function("registry_init_empty", |b| {
        b.iter(|| {
            let r = SchemaRegistry::empty();
            std::hint::black_box(&r);
        });
    });
}

criterion_group!(benches, bench_registry_init);
criterion_main!(benches);
