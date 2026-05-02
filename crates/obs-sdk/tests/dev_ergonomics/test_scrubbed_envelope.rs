//! `test_scrubbed_envelope` — verifies that the `ScrubbedEnvelope`
//! handoff is the only path sinks see, and that the worker-side
//! pass-through wrapper preserves the schema lookup. Phase-2 surface
//! ships only `pass_through`; the scrubbing path lives behind
//! `pub(crate) ScrubbedEnvelope::scrub` until Phase 3 wires the
//! worker pool. Spec 14 § 5 + spec 72 § 7.

use std::sync::Arc;

use obs_core::{InMemorySink, ObsEnvelope, SchemaRegistry, ScrubbedEnvelope, Sink};

#[test]
fn test_pass_through_should_preserve_payload_and_resolve_schema() {
    let registry = Arc::new(SchemaRegistry::from_link_section());
    let env = ObsEnvelope {
        full_name: "myapp.v1.ObsScrubProbe".into(),
        payload: b"hello-world".to_vec(),
        ..Default::default()
    };

    let sink = InMemorySink::new();
    let scrubbed = ScrubbedEnvelope::for_test(&env, &registry);
    sink.deliver(scrubbed);

    let drained = sink.handle().drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].payload, b"hello-world");
}

#[test]
fn test_pass_through_schema_should_be_none_for_unknown_full_name() {
    let registry = SchemaRegistry::empty();
    let env = ObsEnvelope {
        full_name: "unknown.v1.NotRegistered".into(),
        ..Default::default()
    };
    let scrubbed = ScrubbedEnvelope::for_test(&env, &registry);
    assert!(scrubbed.schema().is_none());
    assert_eq!(scrubbed.envelope().full_name, "unknown.v1.NotRegistered");
}
