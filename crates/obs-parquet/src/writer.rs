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
use obs_core::{ResourceAttrs, SchemaRegistry};
use obs_proto::obs::v1::ObsEnvelope;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties as ParquetProps};

use crate::{
    event_columns::{EventColumn, event_columns_from_registry},
    model::ParquetCompression,
    sink::ParquetSinkError,
};

/// Build the unified-table Arrow schema with per-event nested Struct
/// columns sourced from `registry`. Spec 22 § 1.1 / spec 94 § 2.8 /
/// P1-F. When `registry` is `None`, falls back to the legacy schema
/// (envelope + labels/attrs + `payload_proto`).
pub(crate) fn unified_schema_with_registry(registry: Option<&SchemaRegistry>) -> Arc<Schema> {
    let mut fields = base_unified_fields();
    if let Some(reg) = registry {
        let columns = event_columns_from_registry(reg);
        for col in columns {
            let dt = DataType::Struct(col.struct_fields.clone().into());
            fields.push(Field::new(&col.payload_column, dt, true));
        }
    }
    Arc::new(Schema::new(fields))
}

/// Build the unified-table Arrow schema without per-event Struct
/// columns. Retained for the legacy registry-less path used by the
/// existing tests in this module; production sinks build the schema
/// via [`unified_schema_with_registry`].
#[cfg(test)]
fn unified_schema() -> Arc<Schema> {
    Arc::new(Schema::new(base_unified_fields()))
}

fn base_unified_fields() -> Vec<Field> {
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

    // Spec 95 § 3.3 / D8-3 / P1-AE: every analytics row carries the
    // OTel semconv Resource columns alongside `service`, `instance`,
    // `version` so a join on `service=…` between Parquet and OTLP can
    // disambiguate environment / namespace / host.
    vec![
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
        Field::new("service_namespace", DataType::Utf8, true),
        Field::new("environment", DataType::Utf8, true),
        Field::new("host_name", DataType::Utf8, true),
        Field::new("host_arch", DataType::Utf8, true),
        Field::new("sampling_reason", DataType::Utf8, true),
        Field::new("callsite_id", DataType::UInt64, false),
        Field::new("labels", labels_ty, false),
        Field::new("attrs", attrs_ty, false),
        Field::new("payload_proto", DataType::Binary, true),
    ]
}

/// Build a `RecordBatch` from `envelopes` against a unified schema
/// that includes per-event nested Struct columns. Spec 94 § 2.8 / P1-F.
/// `resource` is the active observer's `ResourceAttrs` snapshot,
/// populated once per batch (spec 95 § 3.3 / D8-3).
pub(crate) fn build_record_batch_with_registry(
    schema: &Arc<Schema>,
    envelopes: &[ObsEnvelope],
    registry: &SchemaRegistry,
    resource: &ResourceAttrs,
) -> Result<RecordBatch, ParquetSinkError> {
    let mut event_columns = event_columns_from_registry(registry);
    let mut by_full_name: std::collections::HashMap<String, usize> =
        std::collections::HashMap::with_capacity(event_columns.len());
    for (i, col) in event_columns.iter().enumerate() {
        by_full_name.insert(col.full_name.clone(), i);
    }
    let mut base = build_base_arrays(
        envelopes,
        &mut event_columns,
        Some((registry, &by_full_name)),
        resource,
    )?;
    for col in &mut event_columns {
        base.push(col.finish()?);
    }
    RecordBatch::try_new(Arc::clone(schema), base)
        .map_err(|e| ParquetSinkError::Encode(e.to_string()))
}

/// Build a `RecordBatch` from `envelopes` against the unified schema
/// (no per-event Struct columns).
pub(crate) fn build_record_batch(
    schema: &Arc<Schema>,
    envelopes: &[ObsEnvelope],
    resource: &ResourceAttrs,
) -> Result<RecordBatch, ParquetSinkError> {
    let mut empty: Vec<EventColumn> = Vec::new();
    let arrays = build_base_arrays(envelopes, &mut empty, None, resource)?;
    RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|e| ParquetSinkError::Encode(e.to_string()))
}

/// Build the envelope/labels/attrs/payload columns shared by both
/// the legacy `build_record_batch` and the registry-aware
/// `build_record_batch_with_registry`. When `dispatch` is `Some`,
/// each envelope's typed payload is decoded into the matching
/// `EventColumn` (or null'd across every column for unmatched
/// envelopes). Spec 94 § 2.8 / P1-F.
fn build_base_arrays(
    envelopes: &[ObsEnvelope],
    event_columns: &mut [EventColumn],
    dispatch: Option<(&SchemaRegistry, &std::collections::HashMap<String, usize>)>,
    resource: &ResourceAttrs,
) -> Result<Vec<ArrayRef>, ParquetSinkError> {
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
    let mut service_namespace: Vec<Option<String>> = Vec::with_capacity(n);
    let mut environment: Vec<Option<String>> = Vec::with_capacity(n);
    let mut host_name: Vec<Option<String>> = Vec::with_capacity(n);
    let mut host_arch: Vec<Option<String>> = Vec::with_capacity(n);
    let mut sampling_reason: Vec<Option<String>> = Vec::with_capacity(n);
    let mut callsite_id: Vec<u64> = Vec::with_capacity(n);
    let mut payload_proto: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);

    let mut labels_builder: MapBuilder<StringBuilder, StringBuilder> =
        MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let mut attrs_builder: MapBuilder<StringBuilder, StringBuilder> =
        MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    // Snapshot the resource semconv values once for the entire batch
    // (spec 95 D8-3).
    let resource_namespace = opt_str(&resource.service_namespace);
    let resource_env = opt_str(&resource.deployment_environment);
    let resource_host_name = opt_str(&resource.host_name);
    let resource_host_arch = opt_str(&resource.host_arch);

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
        service_namespace.push(resource_namespace.clone());
        environment.push(resource_env.clone());
        host_name.push(resource_host_name.clone());
        host_arch.push(resource_host_arch.clone());
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

        // attrs carries the ResourceAttrs::extra map (semconv keys
        // that don't map to a first-class slot). Per-batch snapshot.
        let mut extra_keys: Vec<&String> = resource.extra.keys().collect();
        extra_keys.sort();
        for k in &extra_keys {
            if let Some(v) = resource.extra.get(k.as_str()) {
                attrs_builder.keys().append_value(k.as_str());
                attrs_builder.values().append_value(v.as_str());
            }
        }
        attrs_builder
            .append(true)
            .map_err(|e| ParquetSinkError::Encode(e.to_string()))?;

        // Per-event Struct column dispatch. Spec 94 § 2.8 / P1-F.
        if let Some((registry, by_full_name)) = dispatch {
            let matched_idx = by_full_name.get(env.full_name.as_str()).copied();
            for (idx, col) in event_columns.iter_mut().enumerate() {
                if Some(idx) == matched_idx {
                    let schema = match registry.lookup_by_full_name(&env.full_name) {
                        Some(s) => s,
                        None => {
                            // Schema disappeared between dispatch
                            // computation and the lookup — bail to
                            // unmatched.
                            col.append_unmatched();
                            continue;
                        }
                    };
                    if let Err(e) = schema.decode_to_arrow_struct(&env.payload, col) {
                        // Decode failure: log via tracing and treat
                        // this row as unmatched. The legacy
                        // `payload_proto` column still carries the
                        // raw bytes for forensic recovery.
                        tracing::debug!(
                            error = ?e,
                            full_name = %env.full_name,
                            "obs-parquet: decode_to_arrow_struct failed"
                        );
                        col.append_unmatched();
                    } else {
                        col.finish_matched_row();
                    }
                } else {
                    col.append_unmatched();
                }
            }
        }
    }

    let labels = labels_builder.finish();
    let attrs = attrs_builder.finish();

    Ok(vec![
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
        Arc::new(StringArray::from(service_namespace)),
        Arc::new(StringArray::from(environment)),
        Arc::new(StringArray::from(host_name)),
        Arc::new(StringArray::from(host_arch)),
        Arc::new(StringArray::from(sampling_reason)),
        Arc::new(UInt64Array::from(callsite_id)),
        Arc::new(labels),
        Arc::new(attrs),
        Arc::new(BinaryArray::from_iter(
            payload_proto.iter().map(|o| o.as_deref()),
        )),
    ])
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
    // Spec 22 § 2.0a — durability bound. The data file is fsync'd
    // above; without a parent-directory fsync, a crash immediately
    // post-rename can lose the directory entry on ext4/APFS, leaving
    // the .parquet bytes orphaned. Open the parent and `sync_all`.
    if let Some(parent) = final_path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            // Errors on directory fsync are non-fatal — some
            // platforms (notably tmpfs and FAT) reject the operation;
            // the rename's own ordering still gives readers a
            // bounded view.
            let _ = dir.sync_all();
        }
    }
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
