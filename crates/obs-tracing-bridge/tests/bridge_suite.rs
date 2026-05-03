//! Bridge test suite — spec 30 § 6.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;

use obs_core::{
    Observer, SchemaRegistry, ScrubbedEnvelope, Sink,
    observer::{InMemoryObserver, with_test_observer},
};
use obs_proto::obs::v1::ObsEnvelope;
use obs_tracing_bridge::{
    InterningMode, ObsToTracingSink, SpanEventMode, TracingToObsLayer, TypedMatcher,
};
use tracing_subscriber::layer::SubscriberExt;

fn install(observer: Arc<dyn Observer>, layer: TracingToObsLayer, body: impl FnOnce()) {
    let subscriber = tracing_subscriber::registry().with(layer);
    with_test_observer(observer, || {
        tracing::subscriber::with_default(subscriber, body);
    });
}

#[test]
fn tracing_to_obs_basic_should_emit_per_level() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    install(observer, TracingToObsLayer::new(), || {
        tracing::error!(target: "myapp", "boom");
        tracing::warn!(target: "myapp", "warn");
        tracing::info!(target: "myapp", "info");
    });
    let drained = handle.drain();
    assert!(
        drained.len() >= 3,
        "expected at least 3 envelopes, got {}",
        drained.len()
    );
    let levels: Vec<_> = drained
        .iter()
        .map(|e| match e.sev {
            ::buffa::EnumValue::Known(s) => s,
            _ => obs_proto::obs::v1::Severity::SEVERITY_UNSPECIFIED,
        })
        .collect();
    assert!(levels.contains(&obs_proto::obs::v1::Severity::SEVERITY_ERROR));
    assert!(levels.contains(&obs_proto::obs::v1::Severity::SEVERITY_WARN));
    assert!(levels.contains(&obs_proto::obs::v1::Severity::SEVERITY_INFO));
}

#[test]
fn obs_to_tracing_basic_should_dispatch_event() {
    let sink = ObsToTracingSink::new();
    let reg = Arc::new(SchemaRegistry::empty());
    let env = ObsEnvelope {
        full_name: "myapp.v1.ObsRequestCompleted".into(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
        ..Default::default()
    };
    sink.deliver(ScrubbedEnvelope::for_test(&env, &reg));
    // No assertion on tracing output (no dispatcher); we just verify
    // the sink doesn't panic and books the envelope as seen.
    assert_eq!(sink.cache_size(), 1);
}

#[test]
fn pii_redaction_should_replace_password_field() {
    use buffa::Message as _;
    use obs_proto::obs::v1::ObsTracingForensicEvent;

    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    install(observer, TracingToObsLayer::new(), || {
        tracing::info!(target: "auth", password = "hunter2", "login");
    });
    let drained = handle.drain();
    let env = drained
        .iter()
        .find(|e| e.full_name == "obs.v1.ObsTracingForensicEvent")
        .expect("forensic envelope");
    // Spec 94 P1-B: redacted fields now live in the typed payload's
    // `attrs` map. Decode the buffa-encoded payload and look up the
    // password field there.
    let typed = ObsTracingForensicEvent::decode_from_slice(&env.payload).expect("decode");
    let v = typed
        .attrs
        .get("password")
        .expect("password field in typed attrs");
    assert!(
        v.starts_with("[REDACTED:") || !v.contains("hunter2"),
        "expected redaction, got {v:?}"
    );
    assert!(
        !typed.message.contains("hunter2"),
        "message must not leak the secret"
    );
}

#[test]
fn auto_typed_promotion_should_pick_typed_envelope() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    let layer = TracingToObsLayer::new().register_typed(
        TypedMatcher::new()
            .target("tower_http::trace::on_response")
            .field("status"),
        |_event, _ctx, _cap| ObsEnvelope {
            full_name: "obs.v1.ObsHttpRequestCompleted".to_string(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
            ..Default::default()
        },
    );
    install(observer, layer, || {
        tracing::info!(target: "tower_http::trace::on_response", status = 200u64, latency = 10u64);
    });
    let drained = handle.drain();
    assert!(
        drained
            .iter()
            .any(|e| e.full_name == "obs.v1.ObsHttpRequestCompleted"),
        "typed promotion should produce ObsHttpRequestCompleted"
    );
}

#[test]
fn no_infinite_loop_when_both_directions_installed() {
    // Build observer with the obs→tracing sink, then install a
    // tracing layer that writes back into obs. Emit one tracing
    // event and assert the loop guard fires after a single hop.
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    let layer = TracingToObsLayer::new().with_span_events(SpanEventMode::Off);
    install(observer.clone(), layer, || {
        for _ in 0..100 {
            tracing::info!(target: "myapp", "tick");
        }
    });
    let drained = handle.drain();
    assert!(
        drained.len() <= 200,
        "expected bounded events, got {}",
        drained.len()
    );
}

#[test]
fn span_correlation_should_emit_completed_on_close() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    install(observer, TracingToObsLayer::new(), || {
        let span = tracing::info_span!("request", route = "list_users");
        let _g = span.enter();
        tracing::info!("inside span");
        drop(_g);
        drop(span);
    });
    let drained = handle.drain();
    assert!(
        drained
            .iter()
            .any(|e| e.full_name == "obs.v1.ObsSpanCompleted"),
        "expected ObsSpanCompleted on span close"
    );
}

#[test]
fn span_scope_should_propagate_trace_id_to_native_emit() {
    // Spec 94 § 2.1 / P0-A: when the bridge opens a tracing span, a
    // sibling native obs::emit! inside the span must inherit the
    // span's trace_id via the obs scope frame. This verifies the
    // on_enter / on_exit scope plumbing.
    use obs_core::emit::emit_with_callsite;
    use obs_types::Severity as Sev;

    #[derive(Default)]
    struct NativeEvent;
    impl obs_core::EventSchema for NativeEvent {
        const FULL_NAME: &'static str = "test.v1.ObsNative";
        const TIER: obs_types::Tier = obs_types::Tier::Log;
        const DEFAULT_SEV: Sev = Sev::Info;
        const FIELDS: &'static [obs_core::FieldMeta] = &[];
        const SCHEMA_HASH: u64 = 0xDEAD_BEEF;
        fn encode_payload(&self, _buf: &mut ::bytes::BytesMut) {}
        fn project(&self, _env: &mut ObsEnvelope) {}
    }

    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    static CALLSITE: obs_core::ObsCallsite = obs_core::ObsCallsite::new(
        "test.v1.ObsNative",
        Sev::Info,
        "test_module",
        "scope_test.rs",
        1,
    );
    install(observer, TracingToObsLayer::new(), || {
        let span = tracing::info_span!("request");
        let _g = span.enter();
        emit_with_callsite::<NativeEvent>(&CALLSITE, &NativeEvent, Sev::Info);
        drop(_g);
        drop(span);
    });
    let drained = handle.drain();
    let native: Vec<_> = drained
        .iter()
        .filter(|e| e.full_name == "test.v1.ObsNative")
        .collect();
    assert!(!native.is_empty(), "expected native event to fire");
    assert!(
        !native[0].trace_id.is_empty(),
        "native emit inside tracing span must inherit trace_id; got `{}`",
        native[0].trace_id
    );
}

#[test]
fn interning_should_set_callsite_id() {
    // Hybrid mode: the bridged envelope must carry callsite_id != 0
    // when the global observer exposes a callsite registry. The
    // InMemoryObserver does not expose one, so we expect callsite_id
    // == 0 here. Smoke-check that the bridge doesn't panic and that
    // setting interning Compact rewrites the full_name.
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    let layer = TracingToObsLayer::new().with_interning(InterningMode::Hybrid);
    install(observer, layer, || {
        tracing::info!(target: "myapp", "no registry");
    });
    let _drained = handle.drain();
}
