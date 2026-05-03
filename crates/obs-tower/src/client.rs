//! Client-side `tower::Layer`. Spec 40 § 1.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use http::Request;
use obs_proto::obs::v1::ObsEnvelope;
use pin_project_lite::pin_project;
use tower::Service;

use crate::propagator::{TraceContext, W3cPropagator, fresh_span_id, fresh_trace_id, status_class};

type StatusFn = Arc<dyn Fn(u16) -> &'static str + Send + Sync>;
type RouteFn<B> = Arc<dyn Fn(&Request<B>) -> String + Send + Sync>;

/// HTTP client-side layer. Spec 40 § 1.
pub struct ObsHttpClientLayer<B = ()> {
    propagator: Arc<W3cPropagator>,
    target_extractor: RouteFn<B>,
    status_classifier: StatusFn,
}

impl<B> std::fmt::Debug for ObsHttpClientLayer<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObsHttpClientLayer").finish_non_exhaustive()
    }
}

impl<B> Clone for ObsHttpClientLayer<B> {
    fn clone(&self) -> Self {
        Self {
            propagator: Arc::clone(&self.propagator),
            target_extractor: Arc::clone(&self.target_extractor),
            status_classifier: Arc::clone(&self.status_classifier),
        }
    }
}

impl<B> ObsHttpClientLayer<B> {
    /// Construct.
    #[must_use]
    pub fn new() -> Self {
        Self {
            propagator: Arc::new(W3cPropagator::new()),
            target_extractor: Arc::new(|req: &Request<B>| {
                req.uri()
                    .host()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| req.uri().to_string())
            }),
            status_classifier: Arc::new(|s| status_class(s)),
        }
    }

    /// Override the target extractor (default: hostname).
    #[must_use]
    pub fn with_target_extractor<F>(mut self, f: F) -> Self
    where
        F: Fn(&Request<B>) -> String + Send + Sync + 'static,
    {
        self.target_extractor = Arc::new(f);
        self
    }
}

impl<B> Default for ObsHttpClientLayer<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, B> tower::Layer<S> for ObsHttpClientLayer<B>
where
    S: Service<Request<B>>,
{
    type Service = ObsHttpClientService<S, B>;
    fn layer(&self, inner: S) -> Self::Service {
        ObsHttpClientService {
            inner,
            layer: self.clone(),
        }
    }
}

/// Wrapped client service.
pub struct ObsHttpClientService<S, B> {
    inner: S,
    layer: ObsHttpClientLayer<B>,
}

impl<S, B> std::fmt::Debug for ObsHttpClientService<S, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObsHttpClientService")
            .field("layer", &self.layer)
            .finish_non_exhaustive()
    }
}

impl<S, B> Clone for ObsHttpClientService<S, B>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            layer: self.layer.clone(),
        }
    }
}

impl<S, B, ResBody> Service<Request<B>> for ObsHttpClientService<S, B>
where
    S: Service<Request<B>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ObsHttpClientFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        let started = Instant::now();
        let target = (self.layer.target_extractor)(&req);
        let method = req.method().as_str().to_string();
        let propagator = Arc::clone(&self.layer.propagator);
        let status_classifier = Arc::clone(&self.layer.status_classifier);

        // Inject the active trace context. We synthesise one if none
        // is set (scope plumbing is the user's responsibility for the
        // tower-http v1 surface; we ensure a header is sent regardless).
        let ctx = TraceContext {
            trace_id: fresh_trace_id(),
            span_id: fresh_span_id(),
            flags: "01".to_string(),
            tracestate: String::new(),
        };
        propagator.inject(req.headers_mut(), &ctx);
        let trace_id = ctx.trace_id.clone();
        let span_id = ctx.span_id.clone();
        emit_client_started(&target, &method, &trace_id);

        ObsHttpClientFuture {
            inner: self.inner.call(req),
            started: Some(started),
            target,
            method,
            trace_id,
            span_id,
            status_classifier,
        }
    }
}

pin_project! {
    /// Future returned by [`ObsHttpClientService::call`].
    pub struct ObsHttpClientFuture<F> {
        #[pin]
        inner: F,
        started: Option<Instant>,
        target: String,
        method: String,
        trace_id: String,
        span_id: String,
        status_classifier: StatusFn,
    }
}

impl<F, ResBody, E> Future for ObsHttpClientFuture<F>
where
    F: Future<Output = Result<http::Response<ResBody>, E>>,
{
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(out) => {
                let started = this.started.take().unwrap_or_else(Instant::now);
                let elapsed_ms = started.elapsed().as_millis() as u64;
                let class = match &out {
                    Ok(resp) => (this.status_classifier)(resp.status().as_u16()),
                    Err(_) => "err",
                };
                emit_client_completed(
                    this.target,
                    this.method,
                    class,
                    elapsed_ms,
                    this.trace_id,
                    this.span_id,
                );
                Poll::Ready(out)
            }
        }
    }
}

fn emit_client_started(target: &str, method: &str, trace_id: &str) {
    let mut env = ObsEnvelope {
        full_name: "obs.v1.ObsHttpClientStarted".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
        trace_id: trace_id.to_string(),
        ..Default::default()
    };
    env.labels.insert("host".to_string(), target.to_string());
    env.labels.insert("method".to_string(), method.to_string());
    obs_core::observer().emit_envelope(env);
}

fn emit_client_completed(
    target: &str,
    method: &str,
    status_class: &str,
    latency_ms: u64,
    trace_id: &str,
    span_id: &str,
) {
    let mut env = ObsEnvelope {
        full_name: "obs.v1.ObsHttpClientCompleted".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        ..Default::default()
    };
    env.labels.insert("host".to_string(), target.to_string());
    env.labels.insert("method".to_string(), method.to_string());
    env.labels
        .insert("status_class".to_string(), status_class.to_string());
    env.labels
        .insert("latency_ms".to_string(), latency_ms.to_string());
    obs_core::observer().emit_envelope(env);
}
