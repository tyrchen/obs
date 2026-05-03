//! `obs-tower` client-side scope-frame integration test.
//!
//! Spec 95 § 3.1 / P1-AC: the outbound HTTP client middleware reads
//! `obs::scope::active_correlation()` so a chained downstream call
//! preserves trace continuity. Spec 95 § 3.2 / P1-AD: the typed
//! `ObsHttpClientCompleted` payload carries `latency_ms` (drop it from
//! labels per D7-4).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{
    convert::Infallible,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::BytesMut;
use http::{HeaderMap, Request, Response};
use obs_core::{
    Observer, ScopeFrameBuilder,
    observer::{InMemoryObserver, with_test_observer},
};
use obs_tower::{ObsHttpClientLayer, W3cPropagator};
use tower::{Layer, Service, ServiceExt};

#[derive(Clone)]
struct CapturingService {
    captured: Arc<std::sync::Mutex<HeaderMap>>,
}

impl Service<Request<()>> for CapturingService {
    type Response = Response<&'static str>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<()>) -> Self::Future {
        let captured = Arc::clone(&self.captured);
        Box::pin(async move {
            *captured.lock().expect("lock") = req.headers().clone();
            Ok(Response::new("ok"))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outbound_traceparent_inherits_active_scope() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);

    let captured: Arc<std::sync::Mutex<HeaderMap>> =
        Arc::new(std::sync::Mutex::new(HeaderMap::new()));
    let mut svc = ObsHttpClientLayer::<()>::new().layer(CapturingService {
        captured: Arc::clone(&captured),
    });

    // Push a scope frame mirroring what the server middleware would
    // build for an inbound request.
    let scope_trace = "0af7651916cd43dd8448eb211c80319c";
    let scope_span = "b7ad6b7169203331";
    let frame = ScopeFrameBuilder::new()
        .context()
        .trace_id(scope_trace.to_string())
        .span_id(scope_span.to_string())
        .into_frame();

    with_test_observer(observer, || {
        let handle = tokio::runtime::Handle::current();
        let _resp = tokio::task::block_in_place(|| {
            handle.block_on(async {
                obs_core::scope::push_frame_pub(frame);
                let req = Request::builder()
                    .method("GET")
                    .uri("https://upstream.example/api")
                    .body(())
                    .expect("build req");
                svc.ready().await.expect("ready");
                let resp = svc.call(req).await.expect("call");
                let _ = obs_core::scope::pop_frame_pub();
                resp
            })
        });
    });

    // Verify the outbound traceparent header carries the scope's trace_id.
    let headers = captured.lock().expect("lock").clone();
    let traceparent = headers
        .get("traceparent")
        .expect("traceparent header present")
        .to_str()
        .expect("ascii");
    let prop = W3cPropagator::new();
    let ctx = prop.extract(&headers).expect("parse traceparent");
    assert_eq!(
        ctx.trace_id, scope_trace,
        "outbound traceparent trace_id must match the active scope (got header `{traceparent}`)"
    );
    assert_ne!(
        ctx.span_id, scope_span,
        "outbound span_id must be a fresh child of the scope's span"
    );

    // Verify the typed payload encoding (P1-AD).
    let drained = handle.drain();
    let completed = drained
        .iter()
        .find(|e| e.full_name == "obs.v1.ObsHttpClientCompleted")
        .expect("client completed event");
    use buffa::Message as _;
    use obs_proto::obs::v1::ObsHttpClientCompleted;
    let typed =
        ObsHttpClientCompleted::decode_from_slice(&completed.payload).expect("decode payload");
    assert_eq!(typed.method, "GET");
    assert!(typed.host.contains("upstream.example"));
    assert!(
        !completed.labels.contains_key("latency_ms"),
        "MEASUREMENT field must not leak into labels (D7-4)"
    );
    assert_eq!(
        completed.trace_id, scope_trace,
        "client envelope trace_id must match scope"
    );

    // Round-trip the typed payload to confirm encode_into shape.
    let mut cache = ::buffa::SizeCache::default();
    let size = typed.compute_size(&mut cache);
    let mut buf = BytesMut::with_capacity(size as usize);
    typed.write_to(&mut cache, &mut buf);
    assert_eq!(&buf[..], &completed.payload[..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outbound_generates_fresh_trace_when_no_scope() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);

    let captured: Arc<std::sync::Mutex<HeaderMap>> =
        Arc::new(std::sync::Mutex::new(HeaderMap::new()));
    let mut svc = ObsHttpClientLayer::<()>::new().layer(CapturingService {
        captured: Arc::clone(&captured),
    });

    with_test_observer(observer, || {
        let handle = tokio::runtime::Handle::current();
        let _resp = tokio::task::block_in_place(|| {
            handle.block_on(async {
                let req = Request::builder()
                    .method("POST")
                    .uri("https://upstream.example/x")
                    .body(())
                    .expect("build req");
                svc.ready().await.expect("ready");
                svc.call(req).await
            })
        })
        .expect("call");
    });

    let headers = captured.lock().expect("lock").clone();
    assert!(headers.contains_key("traceparent"));
    let drained = handle.drain();
    let completed = drained
        .iter()
        .find(|e| e.full_name == "obs.v1.ObsHttpClientCompleted")
        .expect("client completed");
    assert!(!completed.trace_id.is_empty());
}
