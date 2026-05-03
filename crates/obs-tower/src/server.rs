//! Server-side `tower::Layer`. Spec 40 § 1.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use http::Request;
use obs_core::{Observer, with_observer_task_sync};
use obs_proto::obs::v1::ObsEnvelope;
use pin_project_lite::pin_project;
use tower::Service;

use crate::propagator::{TraceContext, W3cPropagator, fresh_span_id, fresh_trace_id, status_class};

type RouteFn<B> = Arc<dyn Fn(&Request<B>) -> String + Send + Sync>;
type ObserverFn<B> = Arc<dyn Fn(&Request<B>) -> Option<Arc<dyn Observer>> + Send + Sync>;
type StatusFn = Arc<dyn Fn(u16) -> &'static str + Send + Sync>;

/// HTTP server-side layer. Spec 40 § 1.
pub struct ObsHttpLayer<B = ()> {
    route_extractor: RouteFn<B>,
    propagator: Arc<W3cPropagator>,
    emit_started: bool,
    emit_metrics: bool,
    status_classifier: StatusFn,
    per_request_observer: Option<ObserverFn<B>>,
}

impl<B> std::fmt::Debug for ObsHttpLayer<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObsHttpLayer")
            .field("emit_started", &self.emit_started)
            .field("emit_metrics", &self.emit_metrics)
            .finish_non_exhaustive()
    }
}

impl<B> Clone for ObsHttpLayer<B> {
    fn clone(&self) -> Self {
        Self {
            route_extractor: Arc::clone(&self.route_extractor),
            propagator: Arc::clone(&self.propagator),
            emit_started: self.emit_started,
            emit_metrics: self.emit_metrics,
            status_classifier: Arc::clone(&self.status_classifier),
            per_request_observer: self.per_request_observer.clone(),
        }
    }
}

impl<B> ObsHttpLayer<B> {
    /// Construct a server-side layer with sensible defaults.
    /// `emit_started` is off; `emit_metrics` is on.
    #[must_use]
    pub fn server() -> Self {
        Self {
            route_extractor: Arc::new(|req: &Request<B>| req.uri().path().to_string()),
            propagator: Arc::new(W3cPropagator::new()),
            emit_started: false,
            emit_metrics: true,
            status_classifier: Arc::new(|s| status_class(s)),
            per_request_observer: None,
        }
    }

    /// Override the route extractor.
    #[must_use]
    pub fn with_route_extractor<F>(mut self, f: F) -> Self
    where
        F: Fn(&Request<B>) -> String + Send + Sync + 'static,
    {
        self.route_extractor = Arc::new(f);
        self
    }

    /// Toggle emission of `ObsHttpRequestStarted`. Default off.
    #[must_use]
    pub fn with_emit_started(mut self, on: bool) -> Self {
        self.emit_started = on;
        self
    }

    /// Toggle emission of `ObsHttpRequestCompleted` metrics fields.
    /// Default on.
    #[must_use]
    pub fn with_emit_metrics(mut self, on: bool) -> Self {
        self.emit_metrics = on;
        self
    }

    /// Override the W3C propagator.
    #[must_use]
    pub fn with_propagator(mut self, p: W3cPropagator) -> Self {
        self.propagator = Arc::new(p);
        self
    }

    /// Override the status classifier.
    #[must_use]
    pub fn with_status_classifier<F>(mut self, f: F) -> Self
    where
        F: Fn(u16) -> &'static str + Send + Sync + 'static,
    {
        self.status_classifier = Arc::new(f);
        self
    }

    /// Per-request observer hook. Spec 40 § 3.1.
    #[must_use]
    pub fn with_per_request_observer<F>(mut self, f: F) -> Self
    where
        F: Fn(&Request<B>) -> Option<Arc<dyn Observer>> + Send + Sync + 'static,
    {
        self.per_request_observer = Some(Arc::new(f));
        self
    }
}

impl<S, B> tower::Layer<S> for ObsHttpLayer<B>
where
    S: Service<Request<B>>,
    S::Future: Send,
    B: 'static,
{
    type Service = ObsHttpService<S, B>;
    fn layer(&self, inner: S) -> Self::Service {
        ObsHttpService {
            inner,
            layer: self.clone(),
        }
    }
}

/// The wrapped service.
pub struct ObsHttpService<S, B> {
    inner: S,
    layer: ObsHttpLayer<B>,
}

impl<S, B> std::fmt::Debug for ObsHttpService<S, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObsHttpService")
            .field("layer", &self.layer)
            .finish_non_exhaustive()
    }
}

impl<S, B> Clone for ObsHttpService<S, B>
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

impl<S, B, ResBody> Service<Request<B>> for ObsHttpService<S, B>
where
    S: Service<Request<B>, Response = http::Response<ResBody>> + Send,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ObsHttpFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let started = Instant::now();
        let route = (self.layer.route_extractor)(&req);
        let method = req.method().as_str().to_string();
        let propagator = Arc::clone(&self.layer.propagator);
        let status_classifier = Arc::clone(&self.layer.status_classifier);
        let emit_started = self.layer.emit_started;
        let emit_metrics = self.layer.emit_metrics;
        let observer_override = self
            .layer
            .per_request_observer
            .as_ref()
            .and_then(|f| f(&req));

        // Extract or generate trace context.
        let mut ctx = propagator
            .extract(req.headers())
            .unwrap_or_else(|| TraceContext {
                trace_id: fresh_trace_id(),
                span_id: fresh_span_id(),
                flags: "01".to_string(),
                tracestate: String::new(),
            });
        // Always assign a fresh `span_id` at the boundary (the
        // extracted span becomes the parent if present).
        let parent_span = if !ctx.span_id.is_empty() && propagator.extract(req.headers()).is_some()
        {
            ctx.span_id.clone()
        } else {
            String::new()
        };
        ctx.span_id = fresh_span_id();
        let trace_id = ctx.trace_id.clone();
        let span_id = ctx.span_id.clone();

        if emit_started {
            emit_request_started(
                &route,
                &method,
                &trace_id,
                &parent_span,
                observer_override.as_ref(),
            );
        }

        let inner_fut = self.inner.call(req);

        ObsHttpFuture {
            inner: inner_fut,
            started: Some(started),
            route,
            method,
            trace_id,
            span_id,
            parent_span,
            status_classifier,
            emit_metrics,
            observer_override,
        }
    }
}

pin_project! {
    /// Future returned by [`ObsHttpService::call`].
    pub struct ObsHttpFuture<F> {
        #[pin]
        inner: F,
        started: Option<Instant>,
        route: String,
        method: String,
        trace_id: String,
        span_id: String,
        parent_span: String,
        status_classifier: StatusFn,
        emit_metrics: bool,
        observer_override: Option<Arc<dyn Observer>>,
    }
}

impl<F, ResBody, E> Future for ObsHttpFuture<F>
where
    F: Future<Output = Result<http::Response<ResBody>, E>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        // If a per-request observer override is present, install it
        // for this poll. Otherwise just poll directly.
        let result = if let Some(o) = this.observer_override.clone() {
            with_observer_task_sync(o, || this.inner.as_mut().poll(cx))
        } else {
            this.inner.as_mut().poll(cx)
        };
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(out) => {
                let started = this.started.take().unwrap_or_else(Instant::now);
                let elapsed_ms = started.elapsed().as_millis() as u64;
                match &out {
                    Ok(resp) => {
                        if *this.emit_metrics {
                            let status = resp.status().as_u16();
                            let class = (this.status_classifier)(status);
                            emit_request_completed(
                                this.route,
                                this.method,
                                class,
                                elapsed_ms,
                                this.trace_id,
                                this.span_id,
                                this.parent_span,
                                this.observer_override.as_ref(),
                            );
                        }
                    }
                    Err(_) => {
                        if *this.emit_metrics {
                            emit_request_completed(
                                this.route,
                                this.method,
                                "err",
                                elapsed_ms,
                                this.trace_id,
                                this.span_id,
                                this.parent_span,
                                this.observer_override.as_ref(),
                            );
                        }
                    }
                }
                Poll::Ready(out)
            }
        }
    }
}

fn emit_request_started(
    route: &str,
    method: &str,
    trace_id: &str,
    parent_span: &str,
    observer: Option<&Arc<dyn Observer>>,
) {
    let mut env = ObsEnvelope {
        full_name: "obs.v1.ObsHttpRequestStarted".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
        trace_id: trace_id.to_string(),
        parent_span_id: parent_span.to_string(),
        ..Default::default()
    };
    env.labels.insert("route".to_string(), route.to_string());
    env.labels.insert("method".to_string(), method.to_string());
    if let Some(o) = observer {
        o.emit_envelope(env);
    } else {
        obs_core::observer().emit_envelope(env);
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_request_completed(
    route: &str,
    method: &str,
    status_class: &str,
    latency_ms: u64,
    trace_id: &str,
    span_id: &str,
    parent_span: &str,
    observer: Option<&Arc<dyn Observer>>,
) {
    let mut env = ObsEnvelope {
        full_name: "obs.v1.ObsHttpRequestCompleted".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        parent_span_id: parent_span.to_string(),
        ..Default::default()
    };
    env.labels.insert("route".to_string(), route.to_string());
    env.labels.insert("method".to_string(), method.to_string());
    env.labels
        .insert("status_class".to_string(), status_class.to_string());
    env.labels
        .insert("latency_ms".to_string(), latency_ms.to_string());
    if let Some(o) = observer {
        o.emit_envelope(env);
    } else {
        obs_core::observer().emit_envelope(env);
    }
}
