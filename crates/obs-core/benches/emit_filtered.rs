//! `bench_emit_filtered` — measures the cost of an emit that the
//! callsite cache short-circuits as `Never`. The hot path is the
//! atomic `Interest` load + branch; the envelope is never built.
//! Spec 71 § 4.

#![allow(missing_docs)]

use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{
    ObsCallsite,
    callsite::{EnabledOutcome, Interest},
    observer,
};
use obs_proto::obs::v1::Severity;

fn bench_emit_filtered(c: &mut Criterion) {
    static CALLSITE: ObsCallsite = ObsCallsite::new(
        "bench.v1.BenchFiltered",
        Severity::Trace,
        module_path!(),
        file!(),
        line!(),
    );
    // Pre-cache `Never` against the observer's current cur_generation so
    // the hot path skips the re-probe.
    let cur_gen = observer().generation();
    CALLSITE.cache(Interest::Never, cur_gen);

    c.bench_function("emit_filtered_short_circuit", |b| {
        b.iter(|| match CALLSITE.enabled(cur_gen) {
            EnabledOutcome::Off => {
                // Filtered: short-circuit; this is the branch the
                // bench measures.
                std::hint::black_box(());
            }
            other => {
                // Defensive: criterion runs the closure many times;
                // if the cache shape changed mid-run we'd rather
                // surface it than measure the wrong branch.
                std::hint::black_box(other);
            }
        });
    });
}

criterion_group!(benches, bench_emit_filtered);
criterion_main!(benches);
