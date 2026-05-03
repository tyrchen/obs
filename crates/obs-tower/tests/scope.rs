//! `obs-tower` scope-frame integration test (spec 94 § 2.1 / P0-A).
//!
//! Verifies that a native `obs::emit!` call inside a tower service
//! handler inherits the request's `(trace_id, span_id, parent_span_id)`
//! via the obs scope frame the layer pushes around `inner.call(req)`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{
    convert::Infallible,
    sync::Arc,
    task::{Context, Poll},
};

use http::{Request, Response};
use obs_core::{
    Observer,
    emit::emit_with_callsite,
    observer::{InMemoryObserver, with_test_observer},
};
use obs_proto::obs::v1::ObsEnvelope;
use obs_tower::ObsHttpLayer;
use obs_types::Severity;
use tower::{Layer, Service, ServiceExt};

#[derive(Default)]
struct NativeEvent;

impl obs_core::EventSchema for NativeEvent {
    const FULL_NAME: &'static str = "test.v1.ObsHandlerNative";
    const TIER: obs_types::Tier = obs_types::Tier::Log;
    const DEFAULT_SEV: Severity = Severity::Info;
    const FIELDS: &'static [obs_core::FieldMeta] = &[];
    const SCHEMA_HASH: u64 = 0xCAFE_BABE;
    fn encode_payload(&self, _buf: &mut bytes::BytesMut) {}
    fn project(&self, _env: &mut ObsEnvelope) {}
}

#[derive(Clone)]
struct EmitOnceService;

impl Service<Request<()>> for EmitOnceService {
    type Response = Response<&'static str>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<()>) -> Self::Future {
        Box::pin(async {
            static CALLSITE: obs_core::ObsCallsite = obs_core::ObsCallsite::new(
                "test.v1.ObsHandlerNative",
                Severity::Info,
                "tower_test",
                "scope.rs",
                1,
            );
            emit_with_callsite::<NativeEvent>(&CALLSITE, &NativeEvent, Severity::Info);
            Ok(Response::new("ok"))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_emit_inherits_trace_context_from_request_scope() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);

    let layer: ObsHttpLayer<()> = ObsHttpLayer::server();
    let mut svc = layer.layer(EmitOnceService);
    let req = Request::builder()
        .method("GET")
        .uri("/test")
        .body(())
        .expect("build request");

    with_test_observer(observer, || {
        // The outer test fn is `#[tokio::test]` so we have a runtime;
        // bridge the synchronous `with_test_observer` body over to the
        // current runtime via `Handle::block_on`.
        let handle = tokio::runtime::Handle::current();
        let _resp = tokio::task::block_in_place(|| {
            handle.block_on(async {
                svc.ready().await.expect("ready");
                svc.call(req).await
            })
        })
        .expect("call");
    });

    let drained = handle.drain();
    let native = drained
        .iter()
        .find(|e| e.full_name == "test.v1.ObsHandlerNative")
        .expect("native event");
    assert!(
        !native.trace_id.is_empty(),
        "handler emit must inherit trace_id from request scope; got `{}`",
        native.trace_id
    );
    assert!(
        !native.span_id.is_empty(),
        "handler emit must inherit span_id from request scope; got `{}`",
        native.span_id
    );

    let completed = drained
        .iter()
        .find(|e| e.full_name == "obs.v1.ObsHttpRequestCompleted")
        .expect("completed event");
    assert_eq!(
        native.trace_id, completed.trace_id,
        "handler trace_id must match request trace_id"
    );

    // Spec 94 § 3.7 / P1-G: MEASUREMENT fields (`latency_ms`,
    // `bytes_out`) must live in the typed buffa payload, not as
    // string labels. Decoding the payload should yield the typed
    // `ObsHttpRequestCompleted` shape.
    use buffa::Message as _;
    use obs_proto::obs::v1::ObsHttpRequestCompleted;
    let typed = ObsHttpRequestCompleted::decode_from_slice(&completed.payload)
        .expect("decode completed payload");
    assert_eq!(typed.method, "GET");
    assert_eq!(typed.route, "/test");
    // Server returned 2xx
    assert_eq!(typed.status_class, "2xx");
    assert!(
        !completed.labels.contains_key("latency_ms"),
        "MEASUREMENT field must not leak into labels (D7-4)"
    );
}
