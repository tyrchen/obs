//! DoS hardening test for the obs-tower middleware. Spec 95 § 3.10 /
//! P2-AH.
//!
//! Sends a request whose route extractor would otherwise return a
//! pathological 1 MiB string and asserts that the captured envelope
//! carries the truncated form (`…<truncated:N>` suffix), keeping
//! `env.labels` and the typed payload bounded.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{
    convert::Infallible,
    sync::Arc,
    task::{Context, Poll},
};

use http::{Request, Response};
use obs_core::{
    Observer,
    observer::{InMemoryObserver, with_test_observer},
};
use obs_tower::ObsHttpLayer;
use tower::{Layer, Service, ServiceExt};

#[derive(Clone)]
struct EchoService;

impl Service<Request<()>> for EchoService {
    type Response = Response<&'static str>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _req: Request<()>) -> Self::Future {
        Box::pin(async { Ok(Response::new("ok")) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_label_should_cap_at_external_string_limit() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);

    // Route extractor that maliciously fabricates a 1 MiB route.
    let big_route = "/".repeat(1024 * 1024);
    let layer: ObsHttpLayer<()> =
        ObsHttpLayer::server().with_route_extractor(move |_req| big_route.clone());
    let mut svc = layer.layer(EchoService);

    with_test_observer(observer, || {
        let handle = tokio::runtime::Handle::current();
        let _resp = tokio::task::block_in_place(|| {
            handle.block_on(async {
                let req = Request::builder()
                    .method("GET")
                    .uri("/probe")
                    .body(())
                    .expect("build");
                svc.ready().await.expect("ready");
                svc.call(req).await
            })
        })
        .expect("call");
    });

    let drained = handle.drain();
    let completed = drained
        .iter()
        .find(|e| e.full_name == "obs.v1.ObsHttpRequestCompleted")
        .expect("completed");
    let route_label = completed.labels.get("route").expect("route label present");
    assert!(
        route_label.len() < 1024,
        "route label must be capped (got {} bytes)",
        route_label.len(),
    );
    assert!(
        route_label.contains("…<truncated:"),
        "truncated value must carry the marker suffix; got `{}`",
        &route_label[..route_label.len().min(64)],
    );
}
