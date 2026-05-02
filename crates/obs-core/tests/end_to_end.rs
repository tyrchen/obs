//! End-to-end integration tests for Phase 1 obs-core.

#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used
)]
//!
//! These tests cover the full hot path *without* the `#[derive(Event)]`
//! macro: a hand-rolled `EventSchema` impl, a callsite, and a
//! StandardObserver wired with `InMemorySink`. The macro path lands in
//! Phase 1 task 1.9; this test suite is independent of that wiring.

use std::sync::Arc;

use bytes::BytesMut;
use obs_core::__private::Sealed;
use obs_core::{
    Cardinality, Classification, EventSchema, EventSchemaErased, FieldMeta, FieldRole,
    InMemoryObserver, InMemorySink, ObsCallsite, Observer, Severity, StandardObserver, Tier,
    envelope, install_observer, observer, with_test_observer,
};

// ─── Hand-rolled event type (mimics what `#[derive(Event)]` will emit) ──

#[derive(Debug, Default)]
struct TestEvent {
    who: String,
}

impl EventSchema for TestEvent {
    const FULL_NAME: &'static str = "test.v1.TestEvent";
    const TIER: Tier = Tier::Log;
    const DEFAULT_SEV: Severity = Severity::Info;
    const FIELDS: &'static [FieldMeta] = &[FieldMeta {
        name: "who",
        number: 1,
        role: FieldRole::Label,
        cardinality: Cardinality::Low,
        classification: Classification::Internal,
    }];
    // First 8 bytes of BLAKE3("test.v1.TestEvent|LOG|INFO|who:LABEL:LOW") —
    // a stable arbitrary u64 for the test.
    const SCHEMA_HASH: u64 = 0xCAFE_BABE_DEAD_BEEF;

    fn encode_payload(&self, buf: &mut BytesMut) {
        // For Phase 1 we just write the bytes of `who`; real codegen
        // would produce a buffa-encoded message.
        buf.extend_from_slice(self.who.as_bytes());
    }

    fn project(&self, env: &mut obs_core::ObsEnvelope) {
        env.labels.insert("who".to_string(), self.who.clone());
    }
}

#[allow(dead_code)] // referenced via the linkme distributed slice in apps; not in this test crate.
struct TestEventSchema;
impl Sealed for TestEventSchema {}
impl EventSchemaErased for TestEventSchema {
    fn full_name(&self) -> &'static str {
        TestEvent::FULL_NAME
    }
    fn schema_hash(&self) -> u64 {
        TestEvent::SCHEMA_HASH
    }
    fn tier(&self) -> Tier {
        TestEvent::TIER
    }
    fn default_sev(&self) -> Severity {
        TestEvent::DEFAULT_SEV
    }
    fn fields(&self) -> &'static [FieldMeta] {
        TestEvent::FIELDS
    }
}

#[test]
fn test_should_emit_through_in_memory_observer() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);

    with_test_observer(observer, || {
        let cs = ObsCallsite::new(
            TestEvent::FULL_NAME,
            TestEvent::DEFAULT_SEV,
            module_path!(),
            file!(),
            line!(),
        );
        let evt = TestEvent {
            who: "world".to_string(),
        };
        let mut env = envelope::build_envelope::<TestEvent>(&cs, &evt);
        evt.project(&mut env);
        let o = obs_core::observer();
        o.emit_envelope(env);
    });

    let drained = handle.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].full_name, "test.v1.TestEvent");
    assert_eq!(drained[0].schema_hash, 0xCAFE_BABE_DEAD_BEEF);
    assert_eq!(drained[0].labels.get("who"), Some(&"world".to_string()));
}

#[test]
fn test_should_route_log_tier_to_sink() {
    let sink = InMemorySink::new();
    let handle = sink.handle();
    let observer = StandardObserver::builder()
        .service("test", "0.0.0")
        .sink_for(Tier::Log, Arc::new(sink))
        .build()
        .unwrap();

    let cs = ObsCallsite::new(
        TestEvent::FULL_NAME,
        TestEvent::DEFAULT_SEV,
        module_path!(),
        file!(),
        line!(),
    );
    let evt = TestEvent {
        who: "router".to_string(),
    };
    let mut env = envelope::build_envelope::<TestEvent>(&cs, &evt);
    evt.project(&mut env);
    observer.emit_envelope(env);

    let drained = handle.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].labels.get("who"), Some(&"router".to_string()));
    assert_eq!(drained[0].service, "test");
}

#[test]
fn test_should_bump_generation_on_reload() {
    let observer = StandardObserver::builder()
        .service("test", "0.0.0")
        .sink_for(Tier::Log, Arc::new(InMemorySink::new()))
        .build()
        .unwrap();
    let g1 = observer.generation();
    observer.reload_filter();
    let g2 = observer.generation();
    assert!(g2 > g1, "reload_filter must bump generation");
}

#[test]
fn test_three_tier_resolution_should_pick_thread_local() {
    // Sanity test: install a thread-local override; observer() returns
    // it instead of the global Noop.
    let captured = InMemoryObserver::new();
    let handle = captured.handle();
    let captured: Arc<dyn Observer> = Arc::new(captured);
    with_test_observer(captured, || {
        let cs = ObsCallsite::new(
            TestEvent::FULL_NAME,
            TestEvent::DEFAULT_SEV,
            module_path!(),
            file!(),
            line!(),
        );
        let evt = TestEvent {
            who: "thread".to_string(),
        };
        let mut env = envelope::build_envelope::<TestEvent>(&cs, &evt);
        evt.project(&mut env);
        observer().emit_envelope(env);
    });
    let drained = handle.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].labels.get("who"), Some(&"thread".to_string()));
}

#[test]
fn test_should_install_global_observer() {
    // We cannot reset the global between tests reliably (cargo test
    // shares the process), so this test runs after the others and
    // installs a captured observer; we just verify install does not
    // panic and observer() returns something usable.
    let captured = InMemoryObserver::new();
    install_observer(captured);
    let _ = observer();
}
