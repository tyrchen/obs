//! [`ClickHouseSink`] — single-table batched INSERT sink. Spec 22 § 3.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use obs_core::{Sink, registry::ScrubbedEnvelope, sink::SinkFut};
use obs_proto::obs::v1::ObsEnvelope;
use parking_lot::Mutex;
use serde_json::{Map, Value};

use crate::{
    ddl::render_create_table_ddl,
    transport::{ClickHouseBatch, ClickHouseTransport, HttpClickHouseTransport, TransportError},
};

const DEFAULT_BATCH: usize = 8_192;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_RETRY_ATTEMPTS: u32 = 5;

/// Errors emitted from the ClickHouse sink.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClickHouseSinkError {
    /// Builder configuration was incomplete.
    #[error("config: {0}")]
    Config(String),
    /// Transport failure during DDL or insert.
    #[error("transport: {0}")]
    Transport(#[source] TransportError),
}

/// Single-table ClickHouse sink. Spec 22 § 3.
pub struct ClickHouseSink {
    transport: Arc<dyn ClickHouseTransport>,
    database: String,
    table: String,
    batch_size: usize,
    flush_interval: Duration,
    retry_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    buffer: Arc<Mutex<BufferState>>,
    failures: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}

impl std::fmt::Debug for ClickHouseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSink")
            .field("database", &self.database)
            .field("table", &self.table)
            .field("batch_size", &self.batch_size)
            .field("flush_interval", &self.flush_interval)
            .field("retry_attempts", &self.retry_attempts)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct BufferState {
    started: Instant,
    rows: Vec<ObsEnvelope>,
}

impl BufferState {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            rows: Vec::new(),
        }
    }
}

impl ClickHouseSink {
    /// Builder entry.
    #[must_use]
    pub fn builder() -> ClickHouseSinkBuilder {
        ClickHouseSinkBuilder::default()
    }

    /// Counter of irrecoverable failures (after retry exhaustion).
    #[must_use]
    pub fn failure_count(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    /// Counter of envelopes that were dropped because the batch was
    /// rejected after all retries.
    #[must_use]
    pub fn drop_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn attempt_flush(&self, rows: Vec<ObsEnvelope>) {
        if rows.is_empty() {
            return;
        }
        let body = serialize_rows(&rows);
        let batch = ClickHouseBatch {
            database: self.database.clone(),
            table: self.table.clone(),
            body,
            row_count: rows.len(),
        };
        let mut backoff = self.initial_backoff;
        let mut last_err: Option<TransportError> = None;
        for attempt in 0..self.retry_attempts {
            match self.transport.insert_batch(&batch) {
                Ok(()) => return,
                Err(e) => {
                    last_err = Some(e);
                    if attempt + 1 == self.retry_attempts {
                        break;
                    }
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(self.max_backoff);
                }
            }
        }
        self.failures.fetch_add(1, Ordering::Relaxed);
        self.dropped.fetch_add(rows.len() as u64, Ordering::Relaxed);
        emit_sink_failed(last_err);
    }

    fn drain_now(&self) {
        let rows = {
            let mut g = self.buffer.lock();
            let taken = std::mem::take(&mut g.rows);
            g.started = Instant::now();
            taken
        };
        self.attempt_flush(rows);
    }
}

impl Sink for ClickHouseSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        // Spec 14 § 5 / spec 93 P0-8: persist the *scrubbed* payload,
        // not `env.envelope().payload`. The worker already ran the
        // schema's scrubber; if we cloned the original envelope we
        // would ship classified bytes to ClickHouse.
        let mut envelope = env.envelope().clone();
        envelope.payload = env.payload().to_vec();
        // Take, append, possibly flush. Release the lock before
        // invoking any I/O.
        let to_flush = {
            let mut g = self.buffer.lock();
            g.rows.push(envelope);
            let len = g.rows.len();
            let aged = g.started.elapsed() >= self.flush_interval;
            if len >= self.batch_size || (aged && len > 0) {
                let taken = std::mem::take(&mut g.rows);
                g.started = Instant::now();
                Some(taken)
            } else {
                None
            }
        };
        if let Some(rows) = to_flush {
            self.attempt_flush(rows);
        }
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async move {
            self.drain_now();
        })
    }

    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async move {
            self.drain_now();
        })
    }
}

/// Builder for [`ClickHouseSink`].
#[derive(Default)]
pub struct ClickHouseSinkBuilder {
    transport: Option<Arc<dyn ClickHouseTransport>>,
    url: Option<String>,
    database: Option<String>,
    table: Option<String>,
    auto_migrate: bool,
    batch_size: Option<usize>,
    flush_interval: Option<Duration>,
    retry_attempts: Option<u32>,
    schema_model: Option<obs_core::ArrowSchemaModel>,
}

impl std::fmt::Debug for ClickHouseSinkBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSinkBuilder")
            .field("database", &self.database)
            .field("table", &self.table)
            .field("auto_migrate", &self.auto_migrate)
            .finish_non_exhaustive()
    }
}

impl ClickHouseSinkBuilder {
    /// `clickhouse://user:pass@host:port/db` URL — the default
    /// transport will be the in-tree HTTP client.
    #[must_use]
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Inject a custom transport (e.g. a `clickhouse-rs` adapter).
    #[must_use]
    pub fn transport(mut self, t: Arc<dyn ClickHouseTransport>) -> Self {
        self.transport = Some(t);
        self
    }

    /// Override the default `default` database.
    #[must_use]
    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.database = Some(db.into());
        self
    }

    /// Override the default `obs_events` table name.
    #[must_use]
    pub fn table(mut self, name: impl Into<String>) -> Self {
        self.table = Some(name.into());
        self
    }

    /// Run `CREATE TABLE IF NOT EXISTS` on construction.
    /// Spec 22 § 3 — opt-in, dev-only.
    #[must_use]
    pub fn auto_migrate(mut self, on: bool) -> Self {
        self.auto_migrate = on;
        self
    }

    /// Bound the in-memory batch size (rows). Default 8192.
    #[must_use]
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = Some(n.max(1));
        self
    }

    /// Maximum batch age. Default 1 s.
    #[must_use]
    pub fn flush_interval(mut self, d: Duration) -> Self {
        self.flush_interval = Some(d);
        self
    }

    /// Maximum retry attempts on a failed INSERT. Default 5.
    #[must_use]
    pub fn retry_attempts(mut self, n: u32) -> Self {
        self.retry_attempts = Some(n.max(1));
        self
    }

    /// Provide the schema model used for `auto_migrate`. The CLI feeds
    /// this from `SchemaRegistry::arrow_schema()`.
    #[must_use]
    pub fn schema_model(mut self, model: obs_core::ArrowSchemaModel) -> Self {
        self.schema_model = Some(model);
        self
    }

    /// Finalise.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseSinkError::Config`] when neither `url` nor
    /// `transport` is set, or [`ClickHouseSinkError::Transport`] when
    /// `auto_migrate` runs and the DDL fails.
    pub fn build(self) -> Result<ClickHouseSink, ClickHouseSinkError> {
        let transport: Arc<dyn ClickHouseTransport> = match (self.transport, self.url) {
            (Some(t), _) => t,
            (None, Some(url)) => Arc::new(
                HttpClickHouseTransport::from_url(&url).map_err(ClickHouseSinkError::Transport)?,
            ),
            (None, None) => {
                return Err(ClickHouseSinkError::Config(
                    "either `transport` or `url` must be set".into(),
                ));
            }
        };
        let database = self.database.unwrap_or_else(|| "default".to_string());
        let table = self.table.unwrap_or_else(|| "obs_events".to_string());
        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH);
        let flush_interval = self.flush_interval.unwrap_or(DEFAULT_FLUSH_INTERVAL);
        let retry_attempts = self.retry_attempts.unwrap_or(DEFAULT_RETRY_ATTEMPTS);

        if self.auto_migrate {
            let model = self.schema_model.unwrap_or_default();
            let ddl = render_create_table_ddl(&model, &table);
            transport
                .execute_ddl(&ddl)
                .map_err(ClickHouseSinkError::Transport)?;
        }

        Ok(ClickHouseSink {
            transport,
            database,
            table,
            batch_size,
            flush_interval,
            retry_attempts,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            buffer: Arc::new(Mutex::new(BufferState::new())),
            failures: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
        })
    }
}

fn serialize_rows(rows: &[ObsEnvelope]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.len() * 256);
    for env in rows {
        let mut row = Map::new();
        row.insert(
            "ts_ns".into(),
            Value::String((env.ts_ns as i64).to_string()),
        );
        row.insert("full_name".into(), Value::String(env.full_name.clone()));
        row.insert(
            "schema_hash".into(),
            Value::Number(serde_json::Number::from(env.schema_hash)),
        );
        row.insert("tier".into(), Value::String(super::ddl_tier(env)));
        row.insert("sev".into(), Value::String(super::ddl_sev(env)));
        row.insert("trace_id".into(), Value::String(env.trace_id.clone()));
        row.insert("span_id".into(), Value::String(env.span_id.clone()));
        row.insert(
            "parent_span_id".into(),
            Value::String(env.parent_span_id.clone()),
        );
        row.insert(
            "callsite_id".into(),
            Value::Number(serde_json::Number::from(env.callsite_id)),
        );
        row.insert("service".into(), Value::String(env.service.clone()));
        row.insert("instance".into(), Value::String(env.instance.clone()));
        row.insert("version".into(), Value::String(env.version.clone()));
        row.insert(
            "sampling_reason".into(),
            Value::String(super::ddl_sampling(env)),
        );
        let mut labels = Map::new();
        for (k, v) in &env.labels {
            labels.insert(k.clone(), Value::String(v.clone()));
        }
        row.insert("labels".into(), Value::Object(labels));
        row.insert("attrs".into(), Value::Object(Map::new()));
        // payload_proto is base64-encoded for transport safety; using
        // the JSONEachRow string representation per ClickHouse docs.
        if !env.payload.is_empty() {
            row.insert(
                "payload_proto".into(),
                Value::String(base64_encode(&env.payload)),
            );
        } else {
            row.insert("payload_proto".into(), Value::String(String::new()));
        }
        let line = serde_json::to_vec(&Value::Object(row)).unwrap_or_else(|_| b"{}".to_vec());
        out.extend_from_slice(&line);
        out.push(b'\n');
    }
    out
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHA: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len() * 4 / 3 + 4);
    let mut i = 0;
    while i + 3 <= bytes.len() {
        let b0 = bytes[i];
        let b1 = bytes[i + 1];
        let b2 = bytes[i + 2];
        out.push(ALPHA[(b0 >> 2) as usize] as char);
        out.push(ALPHA[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(ALPHA[(((b1 & 0x0F) << 2) | (b2 >> 6)) as usize] as char);
        out.push(ALPHA[(b2 & 0x3F) as usize] as char);
        i += 3;
    }
    let rem = bytes.len() - i;
    if rem == 1 {
        let b0 = bytes[i];
        out.push(ALPHA[(b0 >> 2) as usize] as char);
        out.push(ALPHA[((b0 & 0x03) << 4) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rem == 2 {
        let b0 = bytes[i];
        let b1 = bytes[i + 1];
        out.push(ALPHA[(b0 >> 2) as usize] as char);
        out.push(ALPHA[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(ALPHA[((b1 & 0x0F) << 2) as usize] as char);
        out.push('=');
    }
    out
}

fn emit_sink_failed(err: Option<TransportError>) {
    let mut env = ObsEnvelope {
        full_name: "obs.runtime.v1.ObsSinkFailed".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_WARN),
        ..Default::default()
    };
    env.labels
        .insert("sink".to_string(), "clickhouse".to_string());
    if let Some(e) = err {
        env.labels.insert("reason".to_string(), e.to_string());
    }
    obs_core::observer().emit_envelope(env);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use obs_core::{SchemaRegistry, ScrubbedEnvelope};
    use obs_proto::obs::v1::ObsEnvelope;

    use super::*;
    use crate::transport::RecordingTransport;

    fn env(name: &str) -> ObsEnvelope {
        ObsEnvelope {
            full_name: name.to_string(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
            service: "api".into(),
            ts_ns: 1,
            ..Default::default()
        }
    }

    #[test]
    fn test_should_flush_on_size_threshold() {
        let recorder = Arc::new(RecordingTransport::new());
        let recorder_t: Arc<dyn ClickHouseTransport> = recorder.clone();
        let sink = ClickHouseSink::builder()
            .transport(recorder_t)
            .batch_size(2)
            .flush_interval(Duration::from_secs(60))
            .build()
            .expect("build");
        let registry = Arc::new(SchemaRegistry::empty());
        sink.deliver(ScrubbedEnvelope::for_test(&env("myapp.v1.A"), &registry));
        assert_eq!(recorder.batches().len(), 0);
        sink.deliver(ScrubbedEnvelope::for_test(&env("myapp.v1.B"), &registry));
        // Threshold reached → flushed.
        assert_eq!(recorder.batches().len(), 1);
        assert_eq!(recorder.batches()[0].row_count, 2);
    }

    #[test]
    fn test_should_run_auto_migrate() {
        let recorder = Arc::new(RecordingTransport::new());
        let recorder_t: Arc<dyn ClickHouseTransport> = recorder.clone();
        let _sink = ClickHouseSink::builder()
            .transport(recorder_t)
            .auto_migrate(true)
            .build()
            .expect("build");
        let ddls = recorder.ddls();
        assert_eq!(ddls.len(), 1);
        assert!(ddls[0].contains("CREATE TABLE IF NOT EXISTS obs_events"));
    }

    #[test]
    fn test_base64_encoding_round_trip_format() {
        // Spot check: classic test vector "Man" -> "TWFu"
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"M"), "TQ==");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
    }
}
