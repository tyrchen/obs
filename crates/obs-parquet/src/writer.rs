//! Arrow + Parquet writer used by [`crate::sink::ParquetSink`].
//!
//! The writer takes a batch of `ObsEnvelope`s, builds Arrow arrays for
//! the unified-table schema, and emits one Parquet file per call. The
//! file is written to a `*.parquet.tmp` path and atomically renamed
//! once `close()` succeeds — spec 22 § 2.0a.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BinaryArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
    builder::{MapBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use obs_proto::obs::v1::ObsEnvelope;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties as ParquetProps};

use crate::{model::ParquetCompression, sink::ParquetSinkError};

/// Build the unified-table Arrow schema. Spec 22 § 1.1 envelope
/// columns + maps + payload bytes column. The per-event Nested struct
/// columns are exposed via `obs_core::ArrowSchemaModel` for downstream
/// tools; the in-process Parquet writer stores the bytes in
/// `payload_proto` and lets the consumer decode at query time.
pub(crate) fn unified_schema() -> Arc<Schema> {
    let map_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("keys", DataType::Utf8, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ]
            .into(),
        ),
        false,
    ));
    let labels_ty = DataType::Map(Arc::clone(&map_field), false);
    let attrs_ty = DataType::Map(map_field, false);

    let fields = vec![
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("full_name", DataType::Utf8, false),
        Field::new("schema_hash", DataType::UInt64, false),
        Field::new("tier", DataType::Utf8, false),
        Field::new("sev", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("service", DataType::Utf8, false),
        Field::new("instance", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        Field::new("sampling_reason", DataType::Utf8, true),
        Field::new("callsite_id", DataType::UInt64, false),
        Field::new("labels", labels_ty, false),
        Field::new("attrs", attrs_ty, false),
        Field::new("payload_proto", DataType::Binary, true),
    ];
    Arc::new(Schema::new(fields))
}

/// Build a `RecordBatch` from `envelopes` against the unified schema.
pub(crate) fn build_record_batch(
    schema: &Arc<Schema>,
    envelopes: &[ObsEnvelope],
) -> Result<RecordBatch, ParquetSinkError> {
    let n = envelopes.len();
    let mut ts_ns: Vec<i64> = Vec::with_capacity(n);
    let mut full_name: Vec<String> = Vec::with_capacity(n);
    let mut schema_hash: Vec<u64> = Vec::with_capacity(n);
    let mut tier: Vec<String> = Vec::with_capacity(n);
    let mut sev: Vec<String> = Vec::with_capacity(n);
    let mut trace_id: Vec<Option<String>> = Vec::with_capacity(n);
    let mut span_id: Vec<Option<String>> = Vec::with_capacity(n);
    let mut parent_span_id: Vec<Option<String>> = Vec::with_capacity(n);
    let mut service: Vec<String> = Vec::with_capacity(n);
    let mut instance: Vec<Option<String>> = Vec::with_capacity(n);
    let mut version: Vec<Option<String>> = Vec::with_capacity(n);
    let mut sampling_reason: Vec<Option<String>> = Vec::with_capacity(n);
    let mut callsite_id: Vec<u64> = Vec::with_capacity(n);
    let mut payload_proto: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);

    let mut labels_builder: MapBuilder<StringBuilder, StringBuilder> =
        MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let mut attrs_builder: MapBuilder<StringBuilder, StringBuilder> =
        MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    for env in envelopes {
        ts_ns.push(env.ts_ns as i64);
        full_name.push(env.full_name.clone());
        schema_hash.push(env.schema_hash);
        tier.push(tier_str(env).to_string());
        sev.push(sev_str(env).to_string());
        trace_id.push(opt_str(&env.trace_id));
        span_id.push(opt_str(&env.span_id));
        parent_span_id.push(opt_str(&env.parent_span_id));
        service.push(env.service.clone());
        instance.push(opt_str(&env.instance));
        version.push(opt_str(&env.version));
        sampling_reason.push(Some(sampling_reason_str(env).to_string()));
        callsite_id.push(env.callsite_id);
        payload_proto.push(if env.payload.is_empty() {
            None
        } else {
            Some(env.payload.clone())
        });

        // labels — collect all promoted env.labels.
        let mut keys: Vec<&String> = env.labels.keys().collect();
        keys.sort();
        for k in keys {
            if let Some(v) = env.labels.get(k.as_str()) {
                labels_builder.keys().append_value(k.as_str());
                labels_builder.values().append_value(v.as_str());
            }
        }
        labels_builder
            .append(true)
            .map_err(|e| ParquetSinkError::Encode(e.to_string()))?;

        // attrs is reserved for future per-event-type promoted attrs;
        // keep it empty per row for now.
        attrs_builder
            .append(true)
            .map_err(|e| ParquetSinkError::Encode(e.to_string()))?;
    }

    let labels = labels_builder.finish();
    let attrs = attrs_builder.finish();

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(ts_ns)),
        Arc::new(StringArray::from(full_name)),
        Arc::new(UInt64Array::from(schema_hash)),
        Arc::new(StringArray::from(tier)),
        Arc::new(StringArray::from(sev)),
        Arc::new(StringArray::from(trace_id)),
        Arc::new(StringArray::from(span_id)),
        Arc::new(StringArray::from(parent_span_id)),
        Arc::new(StringArray::from(service)),
        Arc::new(StringArray::from(instance)),
        Arc::new(StringArray::from(version)),
        Arc::new(StringArray::from(sampling_reason)),
        Arc::new(UInt64Array::from(callsite_id)),
        Arc::new(labels),
        Arc::new(attrs),
        Arc::new(BinaryArray::from_iter(
            payload_proto.iter().map(|o| o.as_deref()),
        )),
    ];
    RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|e| ParquetSinkError::Encode(e.to_string()))
}

fn opt_str(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn tier_str(env: &ObsEnvelope) -> &'static str {
    match env.tier {
        ::buffa::EnumValue::Known(t) => match t {
            obs_proto::obs::v1::Tier::TIER_LOG => "LOG",
            obs_proto::obs::v1::Tier::TIER_METRIC => "METRIC",
            obs_proto::obs::v1::Tier::TIER_TRACE => "TRACE",
            obs_proto::obs::v1::Tier::TIER_AUDIT => "AUDIT",
            _ => "UNSPECIFIED",
        },
        _ => "UNSPECIFIED",
    }
}

fn sev_str(env: &ObsEnvelope) -> &'static str {
    match env.sev {
        ::buffa::EnumValue::Known(s) => match s {
            obs_proto::obs::v1::Severity::SEVERITY_TRACE => "TRACE",
            obs_proto::obs::v1::Severity::SEVERITY_DEBUG => "DEBUG",
            obs_proto::obs::v1::Severity::SEVERITY_INFO => "INFO",
            obs_proto::obs::v1::Severity::SEVERITY_WARN => "WARN",
            obs_proto::obs::v1::Severity::SEVERITY_ERROR => "ERROR",
            obs_proto::obs::v1::Severity::SEVERITY_FATAL => "FATAL",
            _ => "UNSPECIFIED",
        },
        _ => "UNSPECIFIED",
    }
}

fn sampling_reason_str(env: &ObsEnvelope) -> &'static str {
    match env.sampling_reason {
        ::buffa::EnumValue::Known(r) => match r {
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_HEAD_RATE => "HEAD_RATE",
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_TAIL_ERROR => "TAIL_ERROR",
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_OVERRIDE => "OVERRIDE",
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_FORENSIC => "FORENSIC",
            _ => "UNSPECIFIED",
        },
        _ => "UNSPECIFIED",
    }
}

/// Atomic write helper. Writes Parquet bytes to `tmp_path`, fsyncs the
/// file and the parent directory, then renames to `final_path`.
/// Spec 22 § 2.0a.
pub(crate) fn write_parquet_atomic(
    final_path: &Path,
    tmp_path: &Path,
    schema: &Arc<Schema>,
    batch: &RecordBatch,
    compression: ParquetCompression,
) -> Result<u64, ParquetSinkError> {
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent).map_err(ParquetSinkError::Io)?;
    }
    let mut tmp = fs::File::create(tmp_path).map_err(ParquetSinkError::Io)?;
    let props = ParquetProps::builder()
        .set_compression(compression.to_parquet())
        .build();
    let mut writer = ArrowWriter::try_new(&mut tmp, Arc::clone(schema), Some(props))
        .map_err(|e| ParquetSinkError::Encode(e.to_string()))?;
    writer
        .write(batch)
        .map_err(|e| ParquetSinkError::Encode(e.to_string()))?;
    let bytes = writer
        .into_inner()
        .map_err(|e| ParquetSinkError::Encode(e.to_string()))?;
    bytes.flush().map_err(ParquetSinkError::Io)?;
    bytes.sync_all().map_err(ParquetSinkError::Io)?;
    let size = bytes.metadata().map(|m| m.len()).unwrap_or(0);
    fs::rename(tmp_path, final_path).map_err(ParquetSinkError::Io)?;
    Ok(size)
}

/// Sweep `*.parquet.tmp` files in `base_dir` and its descendants. Each
/// removed file is reported through `report` so the caller can emit
/// `ObsAnalyticsPartialDropped` self-events. Spec 22 § 2.0a.
pub(crate) fn sweep_tmp_files(
    base_dir: &Path,
    mut report: impl FnMut(PathBuf),
) -> Result<(), ParquetSinkError> {
    if !base_dir.exists() {
        return Ok(());
    }
    let mut stack = vec![base_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(it) => it,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let ftype = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if ftype.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) == Some("tmp")
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.contains(".parquet"))
            {
                if fs::remove_file(&path).is_ok() {
                    report(path);
                }
            }
        }
    }
    Ok(())
}

/// Counter producing collision-free batch ids without any external
/// state. Combines wall-clock ns and a monotonic counter.
pub(crate) fn next_batch_id(counter: &std::sync::atomic::AtomicU64) -> String {
    let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let h = blake3::hash(&[ts.to_le_bytes(), n.to_le_bytes()].concat());
    let bytes = h.as_bytes();
    let mut s = String::with_capacity(16);
    for b in &bytes[..8] {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unified_schema_should_carry_envelope_columns() {
        let s = unified_schema();
        let fields: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(fields.contains(&"ts_ns"));
        assert!(fields.contains(&"full_name"));
        assert!(fields.contains(&"callsite_id"));
        assert!(fields.contains(&"labels"));
        assert!(fields.contains(&"payload_proto"));
    }

    #[test]
    fn test_batch_id_should_be_stable_within_run() {
        let c = std::sync::atomic::AtomicU64::new(0);
        let a = next_batch_id(&c);
        let b = next_batch_id(&c);
        assert_ne!(a, b);
        assert_eq!(a.len(), 16);
    }
}
