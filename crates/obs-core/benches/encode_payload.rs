//! `bench_encode_payload` — measures the buffa-encode hot path on a
//! representative typed event. Spec 71 § 4.

use buffa::Message as _;
use bytes::BytesMut;
use criterion::{Criterion, criterion_group, criterion_main};
use obs_proto::obs::v1::ObsHttpRequestCompleted;

fn bench_encode_payload(c: &mut Criterion) {
    let evt = ObsHttpRequestCompleted {
        method: "GET".to_string(),
        route: "/api/v1/users".to_string(),
        status_class: "2xx".to_string(),
        latency_ms: 42,
        bytes_out: 1024,
        __buffa_unknown_fields: Default::default(),
    };
    c.bench_function("encode_payload_obshttprequestcompleted", |b| {
        let mut cache = ::buffa::SizeCache::default();
        let mut buf = BytesMut::with_capacity(64);
        b.iter(|| {
            buf.clear();
            cache = ::buffa::SizeCache::default();
            let _ = evt.compute_size(&mut cache);
            evt.write_to(&mut cache, &mut buf);
            std::hint::black_box(&buf);
        });
    });
}

criterion_group!(benches, bench_encode_payload);
criterion_main!(benches);
