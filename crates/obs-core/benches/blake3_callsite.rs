#![allow(missing_docs)]

//! `bench_blake3_callsite` — measure BLAKE3 hash cost over the
//! canonical descriptor string used to compute `SCHEMA_HASH`. Spec 71
//! § 4 / spec 93 P2-20.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_blake3(c: &mut Criterion) {
    // A representative descriptor string ~ 100 bytes long.
    let descriptor = "myapp.v1.ObsRequestCompleted|log|info|route:label:medium:internal,\
                      latency_ms:measurement:unspecified:internal,";

    c.bench_function("blake3_hash_descriptor", |b| {
        b.iter(|| {
            let h = blake3::hash(descriptor.as_bytes());
            let bytes = h.as_bytes();
            std::hint::black_box(bytes[0]);
        });
    });
}

criterion_group!(benches, bench_blake3);
criterion_main!(benches);
