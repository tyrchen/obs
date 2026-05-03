#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used
)]

//! `bench_scrub_for_log` — runtime payload scrubber (`scrub_payload`)
//! cost on a representative envelope. Spec 71 § 4 / spec 93 P0-1 + P2-20.

use bytes::BytesMut;
use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{Cardinality, Classification, FieldMeta, FieldRole, scrub_payload};

fn bench_scrub(c: &mut Criterion) {
    // Synthetic FieldMeta table: one PII string + one internal int.
    static FIELDS: &[FieldMeta] = &[
        FieldMeta::new(
            "email",
            1,
            FieldRole::Attribute,
            Cardinality::Unspecified,
            Classification::Pii,
        ),
        FieldMeta::new(
            "count",
            2,
            FieldRole::Measurement,
            Cardinality::Unspecified,
            Classification::Internal,
        ),
    ];

    // Pre-built buffa wire payload: tag(1, len), len=5, "alice", tag(2, varint), 42.
    // Hand-encoded to avoid pulling in buffa-build at bench time.
    let mut payload = Vec::new();
    payload.push(0x0a); // field 1, len-delimited
    payload.push(5);
    payload.extend_from_slice(b"alice");
    payload.push(0x10); // field 2, varint
    payload.push(42);

    c.bench_function("scrub_payload_pii_field", |b| {
        let mut scratch = BytesMut::with_capacity(64);
        b.iter(|| {
            scratch.clear();
            let res = scrub_payload(&payload, FIELDS, &mut scratch);
            std::hint::black_box(res.is_ok());
        });
    });
}

criterion_group!(benches, bench_scrub);
criterion_main!(benches);
