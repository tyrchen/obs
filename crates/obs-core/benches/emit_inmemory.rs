//! `bench_emit_inmemory` — measures emit through an `InMemoryObserver`
//! (sink writes go straight into a ring buffer). Used to bound the
//! cost the dev / test observer adds compared to the noop baseline.
//! Spec 71 § 4.

#![allow(missing_docs, clippy::expect_used)]

use std::sync::Arc;

use bytes::BytesMut;
use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{
    Cardinality, Classification, EventSchema, FieldMeta, FieldRole, ObsCallsite, Severity, Tier,
    envelope,
    observer::{InMemoryObserver, with_test_observer},
};

#[derive(Default)]
struct BenchEvent {
    who: String,
}

impl EventSchema for BenchEvent {
    const FULL_NAME: &'static str = "bench.v1.BenchInMemory";
    const TIER: Tier = Tier::Log;
    const DEFAULT_SEV: Severity = Severity::Info;
    const FIELDS: &'static [FieldMeta] = &[FieldMeta::new(
        "who",
        1,
        FieldRole::Label,
        Cardinality::Low,
        Classification::Internal,
    )];
    const SCHEMA_HASH: u64 = 0xCAFE_F00D_0000_0001;

    fn encode_payload(&self, _buf: &mut BytesMut) {}
    fn project(&self, env: &mut obs_core::ObsEnvelope) {
        env.labels.insert("who".to_string(), self.who.clone());
    }
}

fn bench_emit_inmemory(c: &mut Criterion) {
    let observer: Arc<dyn obs_core::Observer> = Arc::new(InMemoryObserver::new());
    static CALLSITE: ObsCallsite = ObsCallsite::new(
        BenchEvent::FULL_NAME,
        BenchEvent::DEFAULT_SEV,
        module_path!(),
        file!(),
        line!(),
    );

    c.bench_function("emit_inmemory", |b| {
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

criterion_group!(benches, bench_emit_inmemory);
criterion_main!(benches);
