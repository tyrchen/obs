#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used
)]

//! `bench_scope_overhead` — `obs::scope!` push + auto-fill cost.
//!
//! Spec 71 § 4. Target: ≤ 200 ns per emit-with-scope (P50).

use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{
    ObsEnvelope,
    scope::{ScopeField, ScopeGuard, auto_fill_envelope},
};

fn bench_scope(c: &mut Criterion) {
    c.bench_function("scope_push_pop", |b| {
        b.iter(|| {
            let _g = ScopeGuard::enter(vec![ScopeField::Label("tenant", "alpha".to_string())], 64);
            std::hint::black_box(&_g);
        });
    });

    c.bench_function("auto_fill_envelope_no_scope", |b| {
        b.iter(|| {
            let mut env = ObsEnvelope::default();
            auto_fill_envelope(&mut env);
            std::hint::black_box(&env);
        });
    });

    c.bench_function("auto_fill_envelope_one_scope", |b| {
        let _g = ScopeGuard::enter_context(vec![ScopeField::Label("tenant", "alpha".to_string())]);
        b.iter(|| {
            let mut env = ObsEnvelope::default();
            auto_fill_envelope(&mut env);
            std::hint::black_box(&env);
        });
    });
}

criterion_group!(benches, bench_scope);
criterion_main!(benches);
