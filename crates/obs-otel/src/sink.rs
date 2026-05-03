//! OTLP sink wrappers around the per-record projection types in
//! `mapping`.

use std::{sync::Arc, time::Duration};

use obs_core::{Sink, registry::ScrubbedEnvelope, sink::SinkFut};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    OtlpError,
    backpressure::RetryQueue,
    batch::Batch,
    env_config::{OtlpEndpoint, OtlpResourceAttrs},
    logs::OtlpLogPayload,
    metrics::OtlpMetricPayload,
    traces::{OtlpTracePayload, SpanPairTracker},
};

const DEFAULT_BATCH_RECORDS: usize = 256;
const DEFAULT_BATCH_AGE_MS: u64 = 1_000;
const DEFAULT_RETRY_QUEUE: usize = 16_384;

/// Retry policy. Spec 20 § 4.1.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OtlpRetry {
    /// Maximum attempts including the first.
    pub max_attempts: u32,
    /// Initial backoff before retry.
    pub initial_backoff_ms: u64,
    /// Cap on the exponential backoff.
    pub max_backoff_ms: u64,
}

impl Default for OtlpRetry {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
        }
    }
}

/// Pluggable transport. Implementors translate the structured payload
/// into a network call (gRPC, HTTP+protobuf, HTTP+JSON, …).
pub trait OtlpExporter: Send + Sync + 'static {
    /// Send one batch of log records.
    fn export_logs(&self, payload: &OtlpLogPayload) -> Result<(), OtlpError>;
    /// Send one batch of metric data points.
    fn export_metrics(&self, payload: &OtlpMetricPayload) -> Result<(), OtlpError>;
    /// Send one batch of span records.
    fn export_traces(&self, payload: &OtlpTracePayload) -> Result<(), OtlpError>;
}

/// Default exporter — writes JSON to stdout. Useful for the
/// `apps/server` end-to-end demo, the unit tests, and as a fallback
/// when no OTLP endpoint is wired in.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdoutDebugExporter;

impl OtlpExporter for StdoutDebugExporter {
    fn export_logs(&self, payload: &OtlpLogPayload) -> Result<(), OtlpError> {
        match serde_json::to_string(payload) {
            Ok(s) => {
                println!("{s}");
                Ok(())
            }
            Err(e) => Err(OtlpError::Transport(e.to_string())),
        }
    }
    fn export_metrics(&self, payload: &OtlpMetricPayload) -> Result<(), OtlpError> {
        match serde_json::to_string(payload) {
            Ok(s) => {
                println!("{s}");
                Ok(())
            }
            Err(e) => Err(OtlpError::Transport(e.to_string())),
        }
    }
    fn export_traces(&self, payload: &OtlpTracePayload) -> Result<(), OtlpError> {
        match serde_json::to_string(payload) {
            Ok(s) => {
                println!("{s}");
                Ok(())
            }
            Err(e) => Err(OtlpError::Transport(e.to_string())),
        }
    }
}

// ─── OtlpLogSink ──────────────────────────────────────────────────────

/// OTLP log-tier sink. Spec 20 § 2.3.
pub struct OtlpLogSink {
    exporter: Arc<dyn OtlpExporter>,
    batch: Arc<Batch<obs_proto::obs::v1::ObsEnvelope>>,
    retry: Arc<RetryQueue<OtlpLogPayload>>,
    resource: Arc<OtlpResourceAttrs>,
    endpoint: Arc<OtlpEndpoint>,
    retry_policy: OtlpRetry,
    last_flush: Mutex<()>,
}

impl std::fmt::Debug for OtlpLogSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpLogSink")
            .field("endpoint", &self.endpoint.url)
            .field("retry", &self.retry_policy)
            .finish_non_exhaustive()
    }
}

/// Builder for [`OtlpLogSink`].
#[derive(Default)]
pub struct OtlpLogSinkBuilder {
    exporter: Option<Arc<dyn OtlpExporter>>,
    endpoint: Option<OtlpEndpoint>,
    resource: Option<OtlpResourceAttrs>,
    retry: Option<OtlpRetry>,
}

impl std::fmt::Debug for OtlpLogSinkBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpLogSinkBuilder").finish_non_exhaustive()
    }
}

impl OtlpLogSink {
    /// Builder entry.
    #[must_use]
    pub fn builder() -> OtlpLogSinkBuilder {
        OtlpLogSinkBuilder::default()
    }

    /// Convenience: construct the sink with defaults from env vars.
    ///
    /// # Errors
    ///
    /// Returns `OtlpError::Config` when env-var parsing finds an
    /// invalid setting.
    pub fn from_env() -> Result<Self, OtlpError> {
        Self::builder()
            .endpoint(crate::env_config::endpoint_from_env())
            .resource(crate::env_config::resource_from_env())
            .build()
    }
}

impl OtlpLogSinkBuilder {
    /// Set the endpoint.
    #[must_use]
    pub fn endpoint(mut self, e: OtlpEndpoint) -> Self {
        self.endpoint = Some(e);
        self
    }

    /// Set the resource attributes.
    #[must_use]
    pub fn resource(mut self, r: OtlpResourceAttrs) -> Self {
        self.resource = Some(r);
        self
    }

    /// Override the exporter; default is [`StdoutDebugExporter`].
    #[must_use]
    pub fn exporter(mut self, e: Arc<dyn OtlpExporter>) -> Self {
        self.exporter = Some(e);
        self
    }

    /// Override the retry policy.
    #[must_use]
    pub fn retry(mut self, r: OtlpRetry) -> Self {
        self.retry = Some(r);
        self
    }

    /// Finalise.
    ///
    /// # Errors
    ///
    /// Returns `OtlpError::Config` when required fields are missing.
    pub fn build(self) -> Result<OtlpLogSink, OtlpError> {
        let endpoint = self.endpoint.unwrap_or_default();
        let resource = self.resource.unwrap_or_default();
        let exporter = self
            .exporter
            .unwrap_or_else(|| Arc::new(StdoutDebugExporter));
        let retry_policy = self.retry.unwrap_or_default();
        Ok(OtlpLogSink {
            exporter,
            batch: Arc::new(Batch::new(
                DEFAULT_BATCH_RECORDS,
                Duration::from_millis(DEFAULT_BATCH_AGE_MS),
            )),
            retry: Arc::new(RetryQueue::new(DEFAULT_RETRY_QUEUE)),
            resource: Arc::new(resource),
            endpoint: Arc::new(endpoint),
            retry_policy,
            last_flush: Mutex::new(()),
        })
    }
}

impl Sink for OtlpLogSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        // Spec 14 § 5 / spec 93 P0-8: ship the *scrubbed* payload.
        let mut owned = env.envelope().clone();
        owned.payload = env.payload().to_vec();
        if let Some(batch) = self.batch.push(owned) {
            self.dispatch(batch);
        }
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async move {
            let leftover = self.batch.drain();
            if !leftover.is_empty() {
                self.dispatch(leftover);
            }
        })
    }

    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async move {
            let leftover = self.batch.drain();
            if !leftover.is_empty() {
                self.dispatch(leftover);
            }
            // Drain retry queue best-effort.
            while let Some(payload) = self.retry.pop() {
                let _ = self.exporter.export_logs(&payload);
            }
        })
    }
}

impl OtlpLogSink {
    fn dispatch(&self, envelopes: Vec<obs_proto::obs::v1::ObsEnvelope>) {
        let payload = OtlpLogPayload::from_envelopes(&envelopes, &self.resource, &self.endpoint);
        let _g = self.last_flush.lock();
        match self.exporter.export_logs(&payload) {
            Ok(()) => {}
            Err(_) => {
                let _ = self.retry.push(payload);
            }
        }
    }

    /// Test helper.
    #[must_use]
    pub fn retry_depth(&self) -> usize {
        self.retry.depth()
    }
}

// ─── OtlpMetricSink ───────────────────────────────────────────────────

/// OTLP metric-tier sink. Spec 20 § 2.4.
pub struct OtlpMetricSink {
    exporter: Arc<dyn OtlpExporter>,
    batch: Arc<Batch<obs_proto::obs::v1::ObsEnvelope>>,
    retry: Arc<RetryQueue<OtlpMetricPayload>>,
    resource: Arc<OtlpResourceAttrs>,
    endpoint: Arc<OtlpEndpoint>,
    retry_policy: OtlpRetry,
}

impl std::fmt::Debug for OtlpMetricSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpMetricSink")
            .field("endpoint", &self.endpoint.url)
            .field("retry", &self.retry_policy)
            .finish_non_exhaustive()
    }
}

/// Builder for [`OtlpMetricSink`].
#[derive(Default)]
pub struct OtlpMetricSinkBuilder {
    exporter: Option<Arc<dyn OtlpExporter>>,
    endpoint: Option<OtlpEndpoint>,
    resource: Option<OtlpResourceAttrs>,
    retry: Option<OtlpRetry>,
}

impl std::fmt::Debug for OtlpMetricSinkBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpMetricSinkBuilder")
            .finish_non_exhaustive()
    }
}

impl OtlpMetricSink {
    /// Builder entry.
    #[must_use]
    pub fn builder() -> OtlpMetricSinkBuilder {
        OtlpMetricSinkBuilder::default()
    }

    /// `from_env` convenience.
    ///
    /// # Errors
    ///
    /// See [`OtlpLogSink::from_env`].
    pub fn from_env() -> Result<Self, OtlpError> {
        Self::builder()
            .endpoint(crate::env_config::endpoint_from_env())
            .resource(crate::env_config::resource_from_env())
            .build()
    }
}

impl OtlpMetricSinkBuilder {
    /// Set the endpoint.
    #[must_use]
    pub fn endpoint(mut self, e: OtlpEndpoint) -> Self {
        self.endpoint = Some(e);
        self
    }

    /// Set the resource attributes.
    #[must_use]
    pub fn resource(mut self, r: OtlpResourceAttrs) -> Self {
        self.resource = Some(r);
        self
    }

    /// Override the exporter.
    #[must_use]
    pub fn exporter(mut self, e: Arc<dyn OtlpExporter>) -> Self {
        self.exporter = Some(e);
        self
    }

    /// Override the retry policy.
    #[must_use]
    pub fn retry(mut self, r: OtlpRetry) -> Self {
        self.retry = Some(r);
        self
    }

    /// Finalise.
    ///
    /// # Errors
    ///
    /// Returns `OtlpError::Config` if required fields are missing.
    pub fn build(self) -> Result<OtlpMetricSink, OtlpError> {
        let endpoint = self.endpoint.unwrap_or_default();
        let resource = self.resource.unwrap_or_default();
        let exporter = self
            .exporter
            .unwrap_or_else(|| Arc::new(StdoutDebugExporter));
        let retry_policy = self.retry.unwrap_or_default();
        Ok(OtlpMetricSink {
            exporter,
            batch: Arc::new(Batch::new(
                DEFAULT_BATCH_RECORDS,
                Duration::from_millis(DEFAULT_BATCH_AGE_MS),
            )),
            retry: Arc::new(RetryQueue::new(DEFAULT_RETRY_QUEUE)),
            resource: Arc::new(resource),
            endpoint: Arc::new(endpoint),
            retry_policy,
        })
    }
}

impl Sink for OtlpMetricSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        // Spec 14 § 5 / spec 93 P0-8: ship the *scrubbed* payload.
        let mut owned = env.envelope().clone();
        owned.payload = env.payload().to_vec();
        if let Some(batch) = self.batch.push(owned) {
            self.dispatch(batch);
        }
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async move {
            let leftover = self.batch.drain();
            if !leftover.is_empty() {
                self.dispatch(leftover);
            }
        })
    }

    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async move {
            let leftover = self.batch.drain();
            if !leftover.is_empty() {
                self.dispatch(leftover);
            }
            while let Some(payload) = self.retry.pop() {
                let _ = self.exporter.export_metrics(&payload);
            }
        })
    }
}

impl OtlpMetricSink {
    fn dispatch(&self, envelopes: Vec<obs_proto::obs::v1::ObsEnvelope>) {
        let payload = OtlpMetricPayload::from_envelopes(&envelopes, &self.resource, &self.endpoint);
        match self.exporter.export_metrics(&payload) {
            Ok(()) => {}
            Err(_) => {
                let _ = self.retry.push(payload);
            }
        }
    }
}

// ─── OtlpTraceSink ────────────────────────────────────────────────────

/// OTLP trace-tier sink. Spec 20 § 2.5.
pub struct OtlpTraceSink {
    exporter: Arc<dyn OtlpExporter>,
    batch: Arc<Batch<obs_proto::obs::v1::ObsEnvelope>>,
    retry: Arc<RetryQueue<OtlpTracePayload>>,
    resource: Arc<OtlpResourceAttrs>,
    endpoint: Arc<OtlpEndpoint>,
    retry_policy: OtlpRetry,
    pair_tracker: Arc<SpanPairTracker>,
}

impl std::fmt::Debug for OtlpTraceSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpTraceSink")
            .field("endpoint", &self.endpoint.url)
            .field("retry", &self.retry_policy)
            .finish_non_exhaustive()
    }
}

/// Builder for [`OtlpTraceSink`].
#[derive(Default)]
pub struct OtlpTraceSinkBuilder {
    exporter: Option<Arc<dyn OtlpExporter>>,
    endpoint: Option<OtlpEndpoint>,
    resource: Option<OtlpResourceAttrs>,
    retry: Option<OtlpRetry>,
}

impl std::fmt::Debug for OtlpTraceSinkBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpTraceSinkBuilder")
            .finish_non_exhaustive()
    }
}

impl OtlpTraceSink {
    /// Builder entry.
    #[must_use]
    pub fn builder() -> OtlpTraceSinkBuilder {
        OtlpTraceSinkBuilder::default()
    }

    /// `from_env` convenience.
    ///
    /// # Errors
    ///
    /// See [`OtlpLogSink::from_env`].
    pub fn from_env() -> Result<Self, OtlpError> {
        Self::builder()
            .endpoint(crate::env_config::endpoint_from_env())
            .resource(crate::env_config::resource_from_env())
            .build()
    }
}

impl OtlpTraceSinkBuilder {
    /// Set endpoint.
    #[must_use]
    pub fn endpoint(mut self, e: OtlpEndpoint) -> Self {
        self.endpoint = Some(e);
        self
    }
    /// Set resource attrs.
    #[must_use]
    pub fn resource(mut self, r: OtlpResourceAttrs) -> Self {
        self.resource = Some(r);
        self
    }
    /// Override exporter.
    #[must_use]
    pub fn exporter(mut self, e: Arc<dyn OtlpExporter>) -> Self {
        self.exporter = Some(e);
        self
    }
    /// Override retry policy.
    #[must_use]
    pub fn retry(mut self, r: OtlpRetry) -> Self {
        self.retry = Some(r);
        self
    }

    /// Finalise.
    ///
    /// # Errors
    ///
    /// Returns `OtlpError::Config` when required fields are missing.
    pub fn build(self) -> Result<OtlpTraceSink, OtlpError> {
        let endpoint = self.endpoint.unwrap_or_default();
        let resource = self.resource.unwrap_or_default();
        let exporter = self
            .exporter
            .unwrap_or_else(|| Arc::new(StdoutDebugExporter));
        let retry_policy = self.retry.unwrap_or_default();
        Ok(OtlpTraceSink {
            exporter,
            batch: Arc::new(Batch::new(
                DEFAULT_BATCH_RECORDS,
                Duration::from_millis(DEFAULT_BATCH_AGE_MS),
            )),
            retry: Arc::new(RetryQueue::new(DEFAULT_RETRY_QUEUE)),
            resource: Arc::new(resource),
            endpoint: Arc::new(endpoint),
            retry_policy,
            pair_tracker: Arc::new(SpanPairTracker::default()),
        })
    }
}

impl Sink for OtlpTraceSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        // Spec 14 § 5 / spec 93 P0-8: ship the *scrubbed* payload.
        let mut owned = env.envelope().clone();
        owned.payload = env.payload().to_vec();
        if let Some(batch) = self.batch.push(owned) {
            self.dispatch(batch);
        }
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async move {
            let leftover = self.batch.drain();
            if !leftover.is_empty() {
                self.dispatch(leftover);
            }
        })
    }

    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async move {
            let leftover = self.batch.drain();
            if !leftover.is_empty() {
                self.dispatch(leftover);
            }
            while let Some(payload) = self.retry.pop() {
                let _ = self.exporter.export_traces(&payload);
            }
        })
    }
}

impl OtlpTraceSink {
    fn dispatch(&self, envelopes: Vec<obs_proto::obs::v1::ObsEnvelope>) {
        let payload = OtlpTracePayload::from_envelopes(
            &envelopes,
            &self.resource,
            &self.endpoint,
            &self.pair_tracker,
        );
        match self.exporter.export_traces(&payload) {
            Ok(()) => {}
            Err(_) => {
                let _ = self.retry.push(payload);
            }
        }
    }
}
