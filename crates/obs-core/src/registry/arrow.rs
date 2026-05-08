//! Lightweight, dependency-free Arrow schema descriptor.
//!
//! Spec 14 § 4 / KD5: the unified Arrow schema for the `obs_events`
//! table is built once at observer init from the registry, not per
//! Parquet file or per ClickHouse INSERT. Spec 22 § 1.1 gives the
//! column groups.
//!
//! `obs-core` does not depend on `arrow-rs` or `arrow-schema`; the full
//! crate set lives in `obs-parquet` / `obs-clickhouse`. We instead
//! expose a tiny logical model here that captures the per-event field
//! shape (name, role, classification, primitive type) so downstream
//! sinks can translate into their respective target representations
//! without having to re-parse descriptors.

use obs_proto::obs::v1::{Cardinality, Classification, FieldKind};

use super::erased::EventSchemaErased;
use crate::envelope::{FieldMeta, FieldRole};

/// Logical Arrow type for a leaf field. Mirrors the subset of
/// `arrow_schema::DataType` we ever produce from `EventSchema::FIELDS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ArrowLeafType {
    /// Variable-length UTF-8.
    Utf8,
    /// Variable-length UTF-8 with dictionary encoding (LowCardinality).
    DictUtf8,
    /// 64-bit signed integer.
    Int64,
    /// 64-bit unsigned integer.
    UInt64,
    /// 64-bit floating point.
    Float64,
    /// Boolean.
    Bool,
    /// Variable-length bytes (used for `payload_proto` raw fallback).
    Binary,
    /// Nanosecond timestamp encoded as `fixed64`.
    TimestampNs,
}

impl ArrowLeafType {
    /// Stable wire-type name used by both Parquet and ClickHouse codegen.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Utf8 => "utf8",
            Self::DictUtf8 => "dict_utf8",
            Self::Int64 => "int64",
            Self::UInt64 => "uint64",
            Self::Float64 => "float64",
            Self::Bool => "bool",
            Self::Binary => "binary",
            Self::TimestampNs => "timestamp_ns",
        }
    }
}

/// One leaf-typed field in the Arrow schema for a per-event payload
/// struct.
#[derive(Debug, Clone)]
pub struct ArrowField {
    /// Snake-case proto field name, e.g. `latency_ms`.
    pub name: String,
    /// The proto tag — provides stable column identity across renames.
    pub tag: u32,
    /// Logical primitive type.
    pub ty: ArrowLeafType,
    /// `kind:` from the proto annotation.
    pub kind: FieldKind,
    /// Cardinality cap (LABEL only; `Unspecified` for non-LABEL).
    pub cardinality: Cardinality,
    /// Classification (drives PII redaction).
    pub classification: Classification,
}

/// One per-event payload struct. Combined into the unified
/// `obs_events` table by [`ArrowSchemaModel`].
#[derive(Debug, Clone)]
pub struct ArrowEventSchema {
    /// Stable identity, matches `EventSchema::FULL_NAME`.
    pub full_name: String,
    /// `payload_<full_name_snake>` — the column name for this event's
    /// per-event Nested struct. Spec 22 § 1.1.
    pub payload_column: String,
    /// One Arrow field per declared schema field.
    pub fields: Vec<ArrowField>,
    /// First 8 bytes of BLAKE3 over the canonical descriptor.
    pub schema_hash: u64,
}

impl ArrowEventSchema {
    /// Build an `ArrowEventSchema` from an [`EventSchemaErased`] view.
    /// The leaf-type inference uses `FieldRole` plus a small heuristic
    /// on the field name suffix (`*_ns`, `*_ms`, `*_count`, `*_id`).
    /// Codegen will eventually own the type table directly; this
    /// keeps Phase-4 sinks operational against the Phase-1/2 codegen.
    #[must_use]
    pub fn from_erased(schema: &dyn EventSchemaErased) -> Self {
        let full = schema.full_name().to_string();
        let payload_column = format!("payload_{}", full.replace('.', "_").to_lowercase());
        let fields = schema
            .fields()
            .iter()
            .map(arrow_field_for)
            .collect::<Vec<_>>();
        Self {
            full_name: full,
            payload_column,
            fields,
            schema_hash: schema.schema_hash(),
        }
    }
}

fn arrow_field_for(meta: &FieldMeta) -> ArrowField {
    let kind = match meta.role {
        FieldRole::Label => FieldKind::Label,
        FieldRole::Attribute => FieldKind::Attribute,
        FieldRole::Measurement => FieldKind::Measurement,
        FieldRole::TraceId => FieldKind::TraceId,
        FieldRole::SpanId => FieldKind::SpanId,
        FieldRole::ParentSpanId => FieldKind::ParentSpanId,
        FieldRole::TimestampNs => FieldKind::TimestampNs,
        FieldRole::DurationNs => FieldKind::DurationNs,
        FieldRole::Forensic => FieldKind::Forensic,
    };
    let ty = infer_leaf_type(meta);
    ArrowField {
        name: meta.name.to_string(),
        tag: meta.number,
        ty,
        kind,
        cardinality: meta.cardinality,
        classification: meta.classification,
    }
}

fn infer_leaf_type(meta: &FieldMeta) -> ArrowLeafType {
    match meta.role {
        FieldRole::Label => match meta.cardinality {
            Cardinality::Low | Cardinality::Medium => ArrowLeafType::DictUtf8,
            _ => ArrowLeafType::Utf8,
        },
        FieldRole::TraceId | FieldRole::SpanId | FieldRole::ParentSpanId => ArrowLeafType::Utf8,
        FieldRole::TimestampNs => ArrowLeafType::TimestampNs,
        FieldRole::DurationNs => ArrowLeafType::UInt64,
        FieldRole::Measurement => {
            // Heuristic: name suffix decides numeric width.
            let n = meta.name;
            let is_int = n.ends_with("_count")
                || n.ends_with("_total")
                || n.ends_with("_n")
                || n.ends_with("_ms")
                || n.ends_with("_us")
                || n.ends_with("_ns")
                || n.ends_with("_bytes")
                || n.ends_with("_size");
            if is_int {
                ArrowLeafType::UInt64
            } else {
                ArrowLeafType::Float64
            }
        }
        FieldRole::Attribute | FieldRole::Forensic => ArrowLeafType::Utf8,
    }
}

/// Unified `obs_events` table schema. Used by `ParquetSink` to pre-
/// compute the file-level Arrow schema, by `ClickHouseSink` to emit the
/// `CREATE TABLE` DDL, and by `obs migrate {parquet,clickhouse}` for
/// CI-driven migrations. Spec 14 § 4 / KD5 + spec 22 § 1.
#[derive(Debug, Clone, Default)]
pub struct ArrowSchemaModel {
    /// Per-event payload structs, sorted by `full_name`.
    pub events: Vec<ArrowEventSchema>,
}

impl ArrowSchemaModel {
    /// Build the model by walking a sequence of schemas. Sorted
    /// output makes downstream codegen byte-identical across runs
    /// (spec 12 § 1.2).
    #[must_use]
    pub fn from_schemas<'a, I>(iter: I) -> Self
    where
        I: IntoIterator<Item = &'a (dyn EventSchemaErased + 'static)>,
    {
        let mut events: Vec<_> = iter
            .into_iter()
            .map(ArrowEventSchema::from_erased)
            .collect();
        events.sort_by(|a, b| a.full_name.cmp(&b.full_name));
        Self { events }
    }

    /// Number of registered event types.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// True if no events are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Look up one event's struct by `full_name`.
    #[must_use]
    pub fn lookup(&self, full_name: &str) -> Option<&ArrowEventSchema> {
        self.events.iter().find(|e| e.full_name == full_name)
    }

    /// Emit the model as a stable JSON string. Used by
    /// `obs migrate parquet` and as the snapshot format for CI diffing.
    /// Output is deterministic.
    ///
    /// # Errors
    ///
    /// Returns `serde_json::Error` if serialization fails (cannot
    /// happen in practice for our owned structures).
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        let v = self.to_serializable();
        serde_json::to_string_pretty(&v)
    }

    fn to_serializable(&self) -> serde_json::Value {
        use serde_json::{Value, json};
        let envelope_columns: Vec<Value> = ENVELOPE_COLUMNS
            .iter()
            .map(|(name, ty)| json!({"name": name, "type": ty.as_str()}))
            .collect();
        let labels = json!({
            "name": "labels",
            "type": "map<dict_utf8, utf8>",
        });
        let attrs = json!({
            "name": "attrs",
            "type": "map<dict_utf8, utf8>",
        });
        let payload_proto = json!({
            "name": "payload_proto",
            "type": "binary",
        });
        let mut payloads = Vec::with_capacity(self.events.len());
        for evt in &self.events {
            let columns: Vec<Value> = evt
                .fields
                .iter()
                .map(|f| {
                    json!({
                        "name": f.name,
                        "tag": f.tag,
                        "type": f.ty.as_str(),
                        "kind": kind_str(f.kind),
                        "cardinality": format!("{:?}", f.cardinality),
                        "classification": format!("{:?}", f.classification),
                    })
                })
                .collect();
            payloads.push(json!({
                "full_name": evt.full_name,
                "payload_column": evt.payload_column,
                "schema_hash": format!("{:#018x}", evt.schema_hash),
                "fields": columns,
            }));
        }
        json!({
            "table": "obs_events",
            "envelope": envelope_columns,
            "labels": labels,
            "attrs": attrs,
            "payload_proto": payload_proto,
            "events": payloads,
        })
    }
}

const fn kind_str(k: FieldKind) -> &'static str {
    match k {
        FieldKind::Label => "LABEL",
        FieldKind::Attribute => "ATTRIBUTE",
        FieldKind::Measurement => "MEASUREMENT",
        FieldKind::TraceId => "TRACE_ID",
        FieldKind::SpanId => "SPAN_ID",
        FieldKind::ParentSpanId => "PARENT_SPAN_ID",
        FieldKind::TimestampNs => "TIMESTAMP_NS",
        FieldKind::DurationNs => "DURATION_NS",
        FieldKind::Forensic => "FORENSIC",
        _ => "ATTRIBUTE",
    }
}

/// Envelope columns emitted by every analytical sink. Spec 22 § 1.1
/// "Envelope" + "Resource" rows, intentionally short names matching
/// the ClickHouse template in spec 22 § 3.
pub const ENVELOPE_COLUMNS: &[(&str, ArrowLeafType)] = &[
    ("ts_ns", ArrowLeafType::TimestampNs),
    ("full_name", ArrowLeafType::DictUtf8),
    ("schema_hash", ArrowLeafType::UInt64),
    ("tier", ArrowLeafType::DictUtf8),
    ("sev", ArrowLeafType::DictUtf8),
    ("trace_id", ArrowLeafType::Utf8),
    ("span_id", ArrowLeafType::Utf8),
    ("parent_span_id", ArrowLeafType::Utf8),
    ("service", ArrowLeafType::DictUtf8),
    ("instance", ArrowLeafType::DictUtf8),
    ("version", ArrowLeafType::DictUtf8),
    ("sampling_reason", ArrowLeafType::DictUtf8),
    ("callsite_id", ArrowLeafType::UInt64),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_emit_deterministic_json() {
        let model = ArrowSchemaModel::default();
        let s = model.to_json().expect("json renders");
        assert!(s.contains("obs_events"));
        assert!(s.contains("ts_ns"));
    }

    #[test]
    fn test_envelope_columns_should_include_callsite_id() {
        assert!(ENVELOPE_COLUMNS.iter().any(|(n, _)| *n == "callsite_id"));
    }
}
