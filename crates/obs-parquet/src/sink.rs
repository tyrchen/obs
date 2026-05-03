//! [`ParquetSink`] — bounded in-memory batch buffer that flushes to
//! Parquet files on `roll_max_bytes` / `roll_max_age` thresholds.
//!
//! Spec 22 § 2 + spec 61 § 2.7.

use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicU64},
    time::{Duration, Instant},
};

use obs_core::{Sink, registry::ScrubbedEnvelope, sink::SinkFut};
use obs_proto::obs::v1::ObsEnvelope;
use parking_lot::Mutex;

use crate::{
    model::{ParquetCompression, ParquetLayout},
    partition::{PartitionKey, now_seconds},
    writer::{
        build_record_batch, next_batch_id, sweep_tmp_files, unified_schema, write_parquet_atomic,
    },
};

const DEFAULT_ROLL_BYTES: u64 = 256 * 1024 * 1024; // 256 MiB
const DEFAULT_ROLL_AGE: Duration = Duration::from_secs(300); // 5 min

/// Errors emitted by the Parquet sink.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ParquetSinkError {
    /// Filesystem IO failure.
    #[error("io: {0}")]
    Io(#[source] std::io::Error),
    /// Arrow / Parquet encoding failure.
    #[error("encode: {0}")]
    Encode(String),
    /// Configuration is incomplete.
    #[error("config: {0}")]
    Config(String),
}

#[derive(Debug)]
struct PartitionBuf {
    started: Instant,
    bytes_estimate: u64,
    envelopes: Vec<ObsEnvelope>,
}

impl PartitionBuf {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            bytes_estimate: 0,
            envelopes: Vec::new(),
        }
    }
}

/// Single-table Parquet sink. Spec 22 § 2.
pub struct ParquetSink {
    base_dir: PathBuf,
    layout: ParquetLayout,
    roll_max_bytes: u64,
    roll_max_age: Duration,
    compression: ParquetCompression,
    partition_fields: Vec<&'static str>,
    default_service: String,
    schema: Arc<arrow_schema::Schema>,
    /// Batches buffered per partition key.
    batches: Arc<Mutex<std::collections::HashMap<PartitionKey, PartitionBuf>>>,
    batch_counter: Arc<AtomicU64>,
    /// Counter of envelopes that hit Encode failures; emitted as
    /// `ObsSinkFailed` from `flush`.
    encode_failures: Arc<AtomicU64>,
}

impl std::fmt::Debug for ParquetSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetSink")
            .field("base_dir", &self.base_dir)
            .field("layout", &self.layout)
            .field("roll_max_bytes", &self.roll_max_bytes)
            .field("roll_max_age", &self.roll_max_age)
            .field("compression", &self.compression)
            .field("partition_fields", &self.partition_fields)
            .finish_non_exhaustive()
    }
}

impl ParquetSink {
    /// Builder entry.
    #[must_use]
    pub fn builder() -> ParquetSinkBuilder {
        ParquetSinkBuilder::default()
    }

    fn flush_one_locked(
        &self,
        key: &PartitionKey,
        buf: &mut PartitionBuf,
    ) -> Result<u64, ParquetSinkError> {
        if buf.envelopes.is_empty() {
            return Ok(0);
        }
        let dir = key.dir(&self.base_dir, &self.partition_fields);
        let id = next_batch_id(&self.batch_counter);
        let final_path = dir.join(format!("obs_events-{id}.parquet"));
        let tmp_path = dir.join(format!("obs_events-{id}.parquet.tmp"));
        let batch = build_record_batch(&self.schema, &buf.envelopes)?;
        let bytes = write_parquet_atomic(
            &final_path,
            &tmp_path,
            &self.schema,
            &batch,
            self.compression,
        )?;
        buf.envelopes.clear();
        buf.bytes_estimate = 0;
        buf.started = Instant::now();
        Ok(bytes)
    }

    fn maybe_flush_partition(&self, key: &PartitionKey) {
        let mut guard = self.batches.lock();
        if let Some(buf) = guard.get_mut(key) {
            let should_roll = buf.bytes_estimate >= self.roll_max_bytes
                || buf.started.elapsed() >= self.roll_max_age;
            if should_roll {
                let _ = self.flush_one_locked(key, buf);
            }
        }
    }

    /// Drain every partition. Used by [`Sink::flush`] / `shutdown`.
    fn drain_all(&self) {
        let keys: Vec<PartitionKey> = {
            let guard = self.batches.lock();
            guard.keys().cloned().collect()
        };
        for key in &keys {
            let mut guard = self.batches.lock();
            if let Some(buf) = guard.get_mut(key) {
                if let Err(e) = self.flush_one_locked(key, buf) {
                    self.encode_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(error = ?e, partition = ?key, "parquet flush failed");
                }
            }
        }
    }
}

impl Sink for ParquetSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        // Spec 14 § 5 / spec 93 P0-8: persist the *scrubbed* payload
        // bytes — `env.envelope().payload` would be the original
        // pre-scrub bytes and would leak Pii/Secret fields into the
        // Parquet `payload_proto` column.
        let scrubbed_payload = env.payload().to_vec();
        let envelope = env.envelope();
        let mut clone = envelope.clone();
        clone.payload = scrubbed_payload;
        if clone.ts_ns == 0 {
            let secs = now_seconds();
            clone.ts_ns = (secs * 1_000_000_000) as u64;
        }
        let key = PartitionKey::from_envelope(&clone, &self.default_service);
        {
            let mut guard = self.batches.lock();
            let buf = guard.entry(key.clone()).or_insert_with(PartitionBuf::new);
            buf.bytes_estimate += clone.payload.len() as u64 + 256;
            buf.envelopes.push(clone);
        }
        self.maybe_flush_partition(&key);
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async move {
            self.drain_all();
        })
    }

    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async move {
            self.drain_all();
        })
    }
}

/// Builder for [`ParquetSink`].
#[derive(Debug, Default)]
pub struct ParquetSinkBuilder {
    base_dir: Option<PathBuf>,
    layout: Option<ParquetLayout>,
    roll_max_bytes: Option<u64>,
    roll_max_age: Option<Duration>,
    compression: Option<ParquetCompression>,
    partition_by: Vec<String>,
    default_service: Option<String>,
}

impl ParquetSinkBuilder {
    /// Set the base directory all files are written under.
    #[must_use]
    pub fn base_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.base_dir = Some(dir.into());
        self
    }

    /// Choose [`ParquetLayout`]. Default `Single`.
    #[must_use]
    pub fn layout(mut self, l: ParquetLayout) -> Self {
        self.layout = Some(l);
        self
    }

    /// Maximum bytes per Parquet file before rolling. Default 256 MiB.
    #[must_use]
    pub fn roll_max_bytes(mut self, n: u64) -> Self {
        self.roll_max_bytes = Some(n);
        self
    }

    /// Maximum age before rolling. Default 5 min.
    #[must_use]
    pub fn roll_max_age(mut self, d: Duration) -> Self {
        self.roll_max_age = Some(d);
        self
    }

    /// Choose compression. Default Snappy.
    #[must_use]
    pub fn compression(mut self, c: ParquetCompression) -> Self {
        self.compression = Some(c);
        self
    }

    /// Partition fields. Allowed values: `"service"`, `"date"`,
    /// `"hour"`. Default `["service", "date"]`.
    #[must_use]
    pub fn partition_by(mut self, fields: &[&str]) -> Self {
        self.partition_by = fields.iter().map(ToString::to_string).collect();
        self
    }

    /// Service name to fall back to when `env.service` is empty.
    #[must_use]
    pub fn default_service(mut self, s: impl Into<String>) -> Self {
        self.default_service = Some(s.into());
        self
    }

    /// Finalise.
    ///
    /// # Errors
    ///
    /// Returns `ParquetSinkError::Config` if `base_dir` is missing or
    /// the directory cannot be created.
    pub fn build(self) -> Result<ParquetSink, ParquetSinkError> {
        let base_dir = self
            .base_dir
            .ok_or_else(|| ParquetSinkError::Config("base_dir is required".into()))?;
        std::fs::create_dir_all(&base_dir).map_err(ParquetSinkError::Io)?;
        // Spec 22 § 2.0a — sweep stale `.tmp` files, emit one
        // ObsAnalyticsPartialDropped per file. The emit goes through
        // `obs_core::observer()` which is the global; if no observer is
        // installed yet (sink construction may precede install_observer)
        // these become a no-op via `NoopObserver`.
        let _ = sweep_tmp_files(&base_dir, |path| {
            emit_partial_dropped(path);
        });
        let partitions: Vec<&'static str> = if self.partition_by.is_empty() {
            vec!["service", "date"]
        } else {
            self.partition_by
                .iter()
                .filter_map(|s| match s.as_str() {
                    "service" => Some("service"),
                    "date" => Some("date"),
                    "hour" => Some("hour"),
                    _ => None,
                })
                .collect()
        };
        Ok(ParquetSink {
            base_dir,
            layout: self.layout.unwrap_or_default(),
            roll_max_bytes: self.roll_max_bytes.unwrap_or(DEFAULT_ROLL_BYTES),
            roll_max_age: self.roll_max_age.unwrap_or(DEFAULT_ROLL_AGE),
            compression: self.compression.unwrap_or_default(),
            partition_fields: partitions,
            default_service: self.default_service.unwrap_or_else(|| "obs".to_string()),
            schema: unified_schema(),
            batches: Arc::new(Mutex::new(std::collections::HashMap::new())),
            batch_counter: Arc::new(AtomicU64::new(1)),
            encode_failures: Arc::new(AtomicU64::new(0)),
        })
    }
}

fn emit_partial_dropped(path: PathBuf) {
    let mut env = ObsEnvelope {
        full_name: "obs.runtime.v1.ObsAnalyticsPartialDropped".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_WARN),
        ..Default::default()
    };
    env.labels
        .insert("path".to_string(), path.to_string_lossy().into_owned());
    obs_core::observer().emit_envelope(env);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use obs_core::{SchemaRegistry, ScrubbedEnvelope};
    use obs_proto::obs::v1::ObsEnvelope;

    use super::*;

    fn sample_env(name: &str) -> ObsEnvelope {
        ObsEnvelope {
            full_name: name.to_string(),
            schema_hash: 0xdeadbeef,
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
            ts_ns: 1_777_731_600_000_000_000,
            service: "api".into(),
            ..Default::default()
        }
    }

    #[test]
    fn test_should_write_parquet_file_atomic() {
        let dir = tempfile::tempdir().expect("tmp");
        let sink = ParquetSink::builder()
            .base_dir(dir.path())
            .partition_by(&["service", "date"])
            .build()
            .expect("build");

        let registry = Arc::new(SchemaRegistry::empty());
        for i in 0..5 {
            let mut env = sample_env("myapp.v1.ObsRequestCompleted");
            env.labels.insert("seq".to_string(), i.to_string());
            env.payload = format!("payload-{i}").into_bytes();
            sink.deliver(ScrubbedEnvelope::for_test(&env, &registry));
        }

        // Force flush.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            sink.flush().await;
        });

        let mut found = 0usize;
        for entry in walkdir(dir.path()) {
            if entry.extension().and_then(|e| e.to_str()) == Some("parquet") {
                found += 1;
            }
        }
        assert!(found >= 1, "expected at least one parquet file");
    }

    #[test]
    fn test_should_sweep_existing_tmp_files() {
        let dir = tempfile::tempdir().expect("tmp");
        let part = dir.path().join("service=api/date=2026-05-02");
        std::fs::create_dir_all(&part).expect("dirs");
        let stale = part.join("obs_events-aaa.parquet.tmp");
        std::fs::write(&stale, b"junk").expect("write");

        let _sink = ParquetSink::builder()
            .base_dir(dir.path())
            .build()
            .expect("build");
        assert!(!stale.exists(), ".tmp should have been swept");
    }

    fn walkdir(p: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![p.to_path_buf()];
        while let Some(d) = stack.pop() {
            if let Ok(entries) = std::fs::read_dir(&d) {
                for e in entries.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        stack.push(p);
                    } else {
                        out.push(p);
                    }
                }
            }
        }
        out
    }
}
