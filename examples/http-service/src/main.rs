//! Example: axum HTTP service that opens an obs scope per request,
//! propagates W3C `traceparent`, and emits typed events.
//!
//! Demonstrates:
//! - `obs::Event` derive for `ObsCheckoutAttempted` / `ObsCheckoutCompleted` (LOG tier).
//! - `obs-tower::ObsHttpLayer::server()` parsing inbound `traceparent` and synthesising one when
//!   absent.
//! - Per-request observer override hook so different `service` identities can be plumbed through
//!   (multi-tenant pattern).
//! - `StdoutSink` for human-readable visibility, plus optional `OtlpLogSink` + `GrpcOtlpExporter`
//!   when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
//! - Proper `Severity::Warn` escalation on 4xx, `Severity::Error` on 5xx so tail-on-error fires.
//!
//! Run:
//!   cargo run -p obs-example-http-service
//!   curl -i http://127.0.0.1:8080/healthz
//!   curl -i -X POST http://127.0.0.1:8080/checkout \
//!     -H 'traceparent: 00-0123456789abcdef0123456789abcdef-fedcba9876543210-01' \
//!     -H 'content-type: application/json' \
//!     -d '{"sku":"OBS-001","qty":2}'

#![allow(missing_docs)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use obs_otel::{GrpcOtlpExporter, OtlpEndpoint, OtlpLogSink, OtlpProtocol, OtlpResourceAttrs};
use obs_sdk::{
    Event, FormatterStyle, Severity, Sink, StandardObserver, StdoutSink, Tier, install_observer,
};
use obs_tower::ObsHttpLayer;
use serde::Deserialize;
use tokio::net::TcpListener;
use tower::ServiceBuilder;

/// Inbound HTTP request → emitted on entry. Includes the route /
/// method labels the obs-tower layer also stamps.
#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsCheckoutAttempted {
    #[obs(label, cardinality = "low")]
    sku: String,
    #[obs(attribute)]
    qty: u32,
}

/// Result of the checkout. `outcome` is a low-cardinality label so
/// downstream metrics can group by it.
#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsCheckoutCompleted {
    #[obs(label, cardinality = "low")]
    sku: String,
    #[obs(label, cardinality = "low")]
    outcome: String,
    #[obs(attribute)]
    latency_ms: u32,
}

#[derive(Deserialize, Debug)]
struct CheckoutBody {
    sku: String,
    qty: u32,
}

#[derive(Clone)]
struct AppState {
    /// Pretend inventory: SKU → units in stock.
    inventory: Arc<Mutex<HashMap<String, u32>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    install_observer_with_sinks()?;

    let state = AppState {
        inventory: Arc::new(Mutex::new(HashMap::from([
            ("OBS-001".to_string(), 5_u32),
            ("OBS-002".to_string(), 0_u32),
        ]))),
    };

    // Spec 40 § 1: open a scope per request, parse W3C traceparent,
    // synthesise one when absent. obs-tower also emits
    // `ObsHttpRequestCompleted` once the response is produced.
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/checkout", post(checkout))
        .with_state(state)
        .layer(ServiceBuilder::new().layer(ObsHttpLayer::<axum::body::Body>::server()));

    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let listener = TcpListener::bind(addr).await?;
    eprintln!("obs-example-http-service listening on http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn checkout(State(state): State<AppState>, Json(body): Json<CheckoutBody>) -> Response {
    let started = std::time::Instant::now();
    ObsCheckoutAttempted::builder()
        .sku(body.sku.clone())
        .qty(body.qty)
        .emit();

    let outcome = process_checkout(&state, &body);
    let latency_ms: u32 = started.elapsed().as_millis().min(u128::from(u32::MAX)) as u32;

    let (status, sev, body_text): (StatusCode, Severity, &str) = match outcome {
        CheckoutResult::Approved => (StatusCode::OK, Severity::Info, "approved"),
        CheckoutResult::OutOfStock => (StatusCode::CONFLICT, Severity::Warn, "out-of-stock"),
        CheckoutResult::UnknownSku => (StatusCode::NOT_FOUND, Severity::Warn, "unknown-sku"),
    };

    // Spec 40 § 2 / spec 93 P1-10: escalate severity on 4xx/5xx so
    // tail-on-error sampling fires.
    ObsCheckoutCompleted::builder()
        .sku(body.sku)
        .outcome(body_text.to_string())
        .latency_ms(latency_ms)
        .emit_at(sev);

    (status, body_text).into_response()
}

#[derive(Debug)]
enum CheckoutResult {
    Approved,
    OutOfStock,
    UnknownSku,
}

fn process_checkout(state: &AppState, body: &CheckoutBody) -> CheckoutResult {
    let Ok(mut g) = state.inventory.lock() else {
        return CheckoutResult::UnknownSku;
    };
    let Some(stock) = g.get_mut(&body.sku) else {
        return CheckoutResult::UnknownSku;
    };
    if *stock < body.qty {
        return CheckoutResult::OutOfStock;
    }
    *stock -= body.qty;
    CheckoutResult::Approved
}

fn install_observer_with_sinks() -> Result<()> {
    let mut builder = StandardObserver::builder()
        .service("obs-example-http-service", env!("CARGO_PKG_VERSION"))
        .instance(hostname());

    let stdout: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Full));

    // OTLP exporter is opt-in via env. Falls back to stdout-only for a
    // dependency-free dev loop.
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();
    if let Some(url) = otlp_endpoint {
        eprintln!("OTLP enabled: {url}");
        let endpoint = OtlpEndpoint {
            url,
            protocol: OtlpProtocol::Grpc,
            headers: Default::default(),
            compression: String::new(),
            timeout_ms: 5_000,
        };
        let exporter = GrpcOtlpExporter::connect(&endpoint)?;
        let resource = OtlpResourceAttrs {
            service_name: "obs-example-http-service".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            ..Default::default()
        };
        let log_sink: Arc<dyn Sink> = Arc::new(
            OtlpLogSink::builder()
                .exporter(Arc::new(exporter))
                .resource(resource)
                .endpoint(endpoint)
                .build()?,
        );
        // Tier-routed sinks: LOG-tier emits go through OTLP; anything
        // not explicitly routed (METRIC, TRACE, AUDIT) falls back to
        // stdout for visibility. Spec 11 § 6.
        builder = builder.sink_for(Tier::Log, log_sink).sink_fallback(stdout);
    } else {
        eprintln!("no OTLP endpoint set; logs go to stdout only");
        builder = builder.sink_fallback(stdout);
    }

    let observer = builder.build()?;
    install_observer(observer);
    Ok(())
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "local".to_string())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("shutting down");
}
