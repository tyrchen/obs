//! `bench_emit_ndjson` — measures emit through an `NdjsonFileSink`
//! writing to a temporary file. Establishes the per-emit cost when
//! the sink path includes JSON rendering and a buffered file write.
//! Spec 71 § 4.

#![allow(missing_docs, clippy::expect_used, clippy::disallowed_types)]

use std::sync::Arc;

use bytes::BytesMut;
use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{
    Cardinality, Classification, EventSchema, FieldMeta, FieldRole, NdjsonFileSink, ObsCallsite,
    Severity, Tier, envelope,
    observer::{StandardObserver, with_test_observer},
    sink::{RollingFileWriter, RollingPolicy},
};
use obs_types::Tier as TierKind;

#[derive(Default)]
struct BenchEvent {
    who: String,
}

impl EventSchema for BenchEvent {
    const FULL_NAME: &'static str = "bench.v1.BenchNdjson";
    const TIER: Tier = Tier::Log;
    const DEFAULT_SEV: Severity = Severity::Info;
    const FIELDS: &'static [FieldMeta] = &[FieldMeta::new(
        "who",
        1,
        FieldRole::Label,
        Cardinality::Low,
        Classification::Internal,
    )];
    const SCHEMA_HASH: u64 = 0xBEEF_CAFE_0000_0001;

    fn encode_payload(&self, _buf: &mut BytesMut) {}
    fn project(&self, env: &mut obs_core::ObsEnvelope) {
        env.labels.insert("who".to_string(), self.who.clone());
    }
}

fn bench_emit_ndjson(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tmp");
    let writer = RollingFileWriter::builder()
        .directory(dir.path())
        .filename_prefix("bench")
        .filename_suffix("ndjson")
        .policy(RollingPolicy::Never)
        .build()
        .expect("rolling writer");
    let sink = Arc::new(NdjsonFileSink::new(writer));
    let observer = StandardObserver::builder()
        .service("bench", "1.0")
        .sink_for(TierKind::Log, sink)
        .spawn_workers(false)
        .build()
        .expect("observer");
    let observer: Arc<dyn obs_core::Observer> = Arc::new(observer);

    static CALLSITE: ObsCallsite = ObsCallsite::new(
        BenchEvent::FULL_NAME,
        BenchEvent::DEFAULT_SEV,
        module_path!(),
        file!(),
        line!(),
    );

    c.bench_function("emit_ndjson", |b| {
        with_test_observer(observer.clone(), || {
            b.iter(|| {
                let evt = BenchEvent {
                    who: "world".to_string(),
                };
                let mut env = envelope::build_envelope::<BenchEvent>(&CALLSITE, &evt);
                evt.project(&mut env);
                obs_core::observer().emit_envelope(env);
            });
        });
    });
}

criterion_group!(benches, bench_emit_ndjson);
criterion_main!(benches);
