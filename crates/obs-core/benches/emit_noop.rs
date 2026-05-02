#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used
)]

//! `bench_emit_noop` — measures the cost of an emit through a
//! `NoopObserver` (no sinks fire). Spec 71 § 4 / impl-plan task 1.14.
//!
//! Target budget: ≤ 50 ns per emit on 2024-class hardware. The
//! steady-state path is: observer() → enabled() → project() → mpsc
//! try_send → drop. With the `NoopObserver` default the whole chain
//! collapses into a single TLS check + ArcSwap load + virtual call.

use bytes::BytesMut;
use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{
    __private::Sealed, Cardinality, Classification, EventSchema, EventSchemaErased, FieldMeta,
    FieldRole, ObsCallsite, Severity, Tier, envelope, observer,
};

#[derive(Debug, Default)]
struct BenchEvent {
    who: String,
}

impl EventSchema for BenchEvent {
    const FULL_NAME: &'static str = "bench.v1.BenchEvent";
    const TIER: Tier = Tier::Log;
    const DEFAULT_SEV: Severity = Severity::Info;
    const FIELDS: &'static [FieldMeta] = &[FieldMeta::new(
        "who",
        1,
        FieldRole::Label,
        Cardinality::Low,
        Classification::Internal,
    )];
    const SCHEMA_HASH: u64 = 0xDEAD_BEEF_0000_0001;

    fn encode_payload(&self, _buf: &mut BytesMut) {}
    fn project(&self, env: &mut obs_core::ObsEnvelope) {
        env.labels.insert("who".to_string(), self.who.clone());
    }
}

#[allow(dead_code)] // shadow type for the schema-registry contract; bench doesn't register it
struct BenchSchema;
impl Sealed for BenchSchema {}
impl EventSchemaErased for BenchSchema {
    fn full_name(&self) -> &'static str {
        BenchEvent::FULL_NAME
    }
    fn schema_hash(&self) -> u64 {
        BenchEvent::SCHEMA_HASH
    }
    fn tier(&self) -> Tier {
        BenchEvent::TIER
    }
    fn default_sev(&self) -> Severity {
        BenchEvent::DEFAULT_SEV
    }
    fn fields(&self) -> &'static [FieldMeta] {
        BenchEvent::FIELDS
    }
}

fn bench_emit(c: &mut Criterion) {
    static CALLSITE: ObsCallsite = ObsCallsite::new(
        BenchEvent::FULL_NAME,
        BenchEvent::DEFAULT_SEV,
        module_path!(),
        file!(),
        line!(),
    );

    c.bench_function("emit_noop", |b| {
        b.iter(|| {
            let evt = BenchEvent {
                who: "world".to_string(),
            };
            let mut env = envelope::build_envelope::<BenchEvent>(&CALLSITE, &evt);
            evt.project(&mut env);
            let o = observer();
            o.emit_envelope(env);
        });
    });

    c.bench_function("observer_resolution", |b| {
        b.iter(|| {
            let o = observer();
            std::hint::black_box(&o);
        });
    });

    c.bench_function("callsite_enabled_unknown", |b| {
        // Force re-probe each time (Unknown).
        b.iter(|| {
            CALLSITE.reset_cache();
            let outcome = CALLSITE.enabled(0);
            std::hint::black_box(outcome);
        });
    });

    c.bench_function("callsite_enabled_always_cached", |b| {
        CALLSITE.cache(obs_core::callsite::Interest::Always, 1);
        b.iter(|| {
            let outcome = CALLSITE.enabled(1);
            std::hint::black_box(outcome);
        });
    });
}

criterion_group!(benches, bench_emit);
criterion_main!(benches);
