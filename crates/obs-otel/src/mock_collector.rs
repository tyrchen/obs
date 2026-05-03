//! In-process OTLP/gRPC mock collector for tests. Spec 72 § 6 / spec
//! 93 P1-12.
//!
//! Bring up a [`MockOtelCollector`], hand the `endpoint()` URL to a
//! [`crate::GrpcOtlpExporter`], drain the captured requests via
//! `take_logs / take_metrics / take_traces`. The collector listens
//! on `127.0.0.1:0` (kernel-assigned port) so multiple parallel test
//! cases do not collide.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
        logs_service_server::{LogsService, LogsServiceServer},
    },
    metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::{MetricsService, MetricsServiceServer},
    },
    trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
};
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status, transport::Server};

#[derive(Default)]
struct Captured {
    logs: Vec<ExportLogsServiceRequest>,
    metrics: Vec<ExportMetricsServiceRequest>,
    traces: Vec<ExportTraceServiceRequest>,
}

/// Captures every OTLP request the exporter sends. Backing store
/// for the three `take_*` accessors and the only state owned by the
/// collector.
#[derive(Default, Clone)]
pub struct MockCollectorState {
    captured: Arc<Mutex<Captured>>,
}

impl MockCollectorState {
    /// Drain captured log exports.
    #[must_use]
    pub fn take_logs(&self) -> Vec<ExportLogsServiceRequest> {
        std::mem::take(&mut self.captured.lock().logs)
    }
    /// Drain captured metric exports.
    #[must_use]
    pub fn take_metrics(&self) -> Vec<ExportMetricsServiceRequest> {
        std::mem::take(&mut self.captured.lock().metrics)
    }
    /// Drain captured trace exports.
    #[must_use]
    pub fn take_traces(&self) -> Vec<ExportTraceServiceRequest> {
        std::mem::take(&mut self.captured.lock().traces)
    }
}

impl std::fmt::Debug for MockCollectorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.captured.lock();
        f.debug_struct("MockCollectorState")
            .field("logs", &g.logs.len())
            .field("metrics", &g.metrics.len())
            .field("traces", &g.traces.len())
            .finish()
    }
}

#[tonic::async_trait]
impl LogsService for MockCollectorState {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        self.captured.lock().logs.push(request.into_inner());
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for MockCollectorState {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        self.captured.lock().metrics.push(request.into_inner());
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl TraceService for MockCollectorState {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.captured.lock().traces.push(request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

/// Running OTLP/gRPC mock collector. Stops cleanly on `drop`.
pub struct MockOtelCollector {
    endpoint: String,
    state: MockCollectorState,
    shutdown: Option<oneshot::Sender<()>>,
    runtime: Option<tokio::runtime::Runtime>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for MockOtelCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockOtelCollector")
            .field("endpoint", &self.endpoint)
            .field("state", &self.state)
            .finish()
    }
}

impl MockOtelCollector {
    /// Bind to `127.0.0.1:0` and start serving. Returns once the
    /// server is listening.
    ///
    /// # Errors
    ///
    /// Returns the underlying tonic transport error when the listener
    /// cannot bind.
    pub fn start() -> std::io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("obs-otlp-mock")
            .build()?;

        // Bind a stdlib listener first to discover the kernel-assigned
        // port; tonic accepts a `TcpIncoming`/listener via
        // `serve_with_incoming`. Setting nonblocking is required by
        // `tokio::net::TcpListener::from_std`.
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        std_listener.set_nonblocking(true)?;
        let local_addr = std_listener.local_addr()?;
        let endpoint = format!("http://{local_addr}");
        let state = MockCollectorState::default();
        let (tx, rx) = oneshot::channel::<()>();

        let serve_state = state.clone();
        let join = runtime.spawn(async move {
            let listener = match tokio::net::TcpListener::from_std(std_listener) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(error = %e, "mock collector listener bind failed");
                    return;
                }
            };
            let incoming = tonic::service::Routes::default();
            let _ = incoming;
            let server = Server::builder()
                .add_service(LogsServiceServer::new(serve_state.clone()))
                .add_service(MetricsServiceServer::new(serve_state.clone()))
                .add_service(TraceServiceServer::new(serve_state));
            let listener_stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
            if let Err(e) = server
                .serve_with_incoming_shutdown(listener_stream, async move {
                    let _ = rx.await;
                })
                .await
            {
                tracing::error!(error = %e, "mock collector exited with error");
            }
        });

        Ok(Self {
            endpoint,
            state,
            shutdown: Some(tx),
            runtime: Some(runtime),
            join: Some(join),
        })
    }

    /// gRPC endpoint URL — pass to [`crate::GrpcOtlpExporter::connect`].
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Shared state handle for draining captured requests.
    #[must_use]
    pub fn state(&self) -> MockCollectorState {
        self.state.clone()
    }
}

impl Drop for MockOtelCollector {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let (Some(rt), Some(join)) = (self.runtime.take(), self.join.take()) {
            // Best-effort: await the server task to drain.
            rt.block_on(async move {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(2), join).await;
            });
        }
    }
}
