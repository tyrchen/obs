//! Per-event Nested-struct column accumulator for [`crate::ParquetSink`].
//!
//! Spec 22 § 1.1 / spec 94 § 2.8 / P1-F: the unified `obs_events`
//! table carries one `payload_<full_name_snake>: Struct<…>` column
//! per registered event. Each column is sparse — at most one is
//! non-null per row, the one whose full_name matches `env.full_name`.
//!
//! Implementation: rather than fight Arrow's `StructBuilder` type
//! erasure for every leaf type combination, this module accumulates
//! one [`ChildBuilder`] per declared field, tracks per-row validity,
//! and assembles a `StructArray` at flush time. The codegen-emitted
//! `EventSchemaErased::decode_to_arrow_struct` (spec 94 P1-C) walks
//! the buffa wire format and dispatches to the typed `append_*`
//! methods this module exposes via [`ArrowStructBuilder`].

use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    ArrayRef, StructArray,
    builder::{
        BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
        TimestampNanosecondBuilder, UInt64Builder,
    },
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, FieldRef, TimeUnit};
use obs_core::{
    ArrowEventSchema, ArrowField, ArrowLeafType, ArrowStructBuilder, EventSchemaErased,
    SchemaRegistry,
};

use crate::sink::ParquetSinkError;

/// One per-event Nested-struct column, accumulating values across
/// rows and finalising into a `StructArray` at flush time.
#[derive(Debug)]
pub(crate) struct EventColumn {
    /// Stable identity of the event this column belongs to. Matches
    /// the envelope's `full_name`.
    pub(crate) full_name: String,
    /// The Parquet column name for this event's nested struct, e.g.
    /// `payload_myapp_v1_obs_request_completed`.
    pub(crate) payload_column: String,
    /// Field layout for the StructArray's Arrow type.
    pub(crate) struct_fields: Vec<FieldRef>,
    /// Per-field child builders, indexed by the same position as
    /// `struct_fields`.
    children: Vec<ChildBuilder>,
    /// Lookup from declared field name to child index + leaf type
    /// (used by the dispatch impl of `ArrowStructBuilder`).
    field_index: HashMap<&'static str, (usize, ArrowLeafType)>,
    /// Per-row validity bit: `true` when this row carried a typed
    /// payload that decoded into this event.
    validity: Vec<bool>,
    /// Tracks whether the current in-progress row has appended a
    /// value for each child. Reset between rows so missing fields can
    /// be back-filled with null without leaving the children's row
    /// counts out of sync with the parent struct.
    row_seen: Vec<bool>,
}

impl EventColumn {
    /// Build an empty column from the registry's
    /// [`ArrowEventSchema`]. The column has zero rows; `append_*`
    /// methods grow it.
    pub(crate) fn new(schema: &ArrowEventSchema) -> Self {
        let mut struct_fields = Vec::with_capacity(schema.fields.len());
        let mut children = Vec::with_capacity(schema.fields.len());
        let mut field_index = HashMap::with_capacity(schema.fields.len());
        for (idx, f) in schema.fields.iter().enumerate() {
            struct_fields.push(Arc::new(field_to_arrow(f)));
            children.push(ChildBuilder::for_leaf(f.ty));
            // The field name on the registry side is `String`; we need
            // a `&'static str` to satisfy the `ArrowStructBuilder`
            // trait. Leak the string — the registry is a long-lived
            // Arc and the column lifetime ties to the sink's. Users
            // build the registry once at startup so this is bounded.
            let leaked: &'static str = Box::leak(f.name.clone().into_boxed_str());
            field_index.insert(leaked, (idx, f.ty));
        }
        Self {
            full_name: schema.full_name.clone(),
            payload_column: schema.payload_column.clone(),
            struct_fields,
            children,
            field_index,
            validity: Vec::new(),
            row_seen: vec![false; schema.fields.len()],
        }
    }

    /// Append a row that this column does NOT match — the parent
    /// struct is null for this row, and every child gets a null so
    /// the row counts stay aligned. Spec 94 § 2.8.
    pub(crate) fn append_unmatched(&mut self) {
        for child in &mut self.children {
            child.append_null();
        }
        self.validity.push(false);
        self.reset_row_seen();
    }

    /// Append a row that matches this column. The caller is expected
    /// to have already populated child builders via the
    /// [`ArrowStructBuilder`] trait methods. Any field that wasn't
    /// touched gets a null so children stay aligned with the parent.
    pub(crate) fn finish_matched_row(&mut self) {
        for (idx, &seen) in self.row_seen.iter().enumerate() {
            if !seen {
                self.children[idx].append_null();
            }
        }
        self.validity.push(true);
        self.reset_row_seen();
    }

    /// Build the final `StructArray` for this column. Consumes the
    /// builders.
    pub(crate) fn finish(&mut self) -> Result<ArrayRef, ParquetSinkError> {
        let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(self.children.len());
        for child in &mut self.children {
            child_arrays.push(child.finish()?);
        }
        let validity = NullBuffer::from(std::mem::take(&mut self.validity));
        let arr = StructArray::try_new(
            self.struct_fields.clone().into(),
            child_arrays,
            Some(validity),
        )
        .map_err(|e| ParquetSinkError::Encode(e.to_string()))?;
        Ok(Arc::new(arr))
    }

    fn reset_row_seen(&mut self) {
        for slot in &mut self.row_seen {
            *slot = false;
        }
    }
}

impl ArrowStructBuilder for EventColumn {
    fn append_null(&mut self) {
        // The trait's "append null across every field" semantics are
        // the unmatched-row case from the column's perspective.
        self.append_unmatched();
    }

    fn append_str(&mut self, name: &'static str, value: &str) {
        if let Some(&(idx, ty)) = self.field_index.get(name) {
            self.children[idx].append_str(value, ty);
            self.row_seen[idx] = true;
        }
    }

    fn append_i64(&mut self, name: &'static str, value: i64) {
        if let Some(&(idx, ty)) = self.field_index.get(name) {
            self.children[idx].append_i64(value, ty);
            self.row_seen[idx] = true;
        }
    }

    fn append_u64(&mut self, name: &'static str, value: u64) {
        if let Some(&(idx, ty)) = self.field_index.get(name) {
            self.children[idx].append_u64(value, ty);
            self.row_seen[idx] = true;
        }
    }

    fn append_f64(&mut self, name: &'static str, value: f64) {
        if let Some(&(idx, ty)) = self.field_index.get(name) {
            self.children[idx].append_f64(value, ty);
            self.row_seen[idx] = true;
        }
    }

    fn append_bool(&mut self, name: &'static str, value: bool) {
        if let Some(&(idx, ty)) = self.field_index.get(name) {
            self.children[idx].append_bool(value, ty);
            self.row_seen[idx] = true;
        }
    }

    fn append_bytes(&mut self, name: &'static str, value: &[u8]) {
        if let Some(&(idx, ty)) = self.field_index.get(name) {
            self.children[idx].append_bytes(value, ty);
            self.row_seen[idx] = true;
        }
    }

    fn append_field_null(&mut self, name: &'static str) {
        if let Some(&(idx, _)) = self.field_index.get(name) {
            self.children[idx].append_null();
            self.row_seen[idx] = true;
        }
    }
}

#[derive(Debug)]
enum ChildBuilder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Binary(BinaryBuilder),
    TimestampNs(TimestampNanosecondBuilder),
}

impl ChildBuilder {
    fn for_leaf(ty: ArrowLeafType) -> Self {
        match ty {
            ArrowLeafType::Utf8 | ArrowLeafType::DictUtf8 => Self::Utf8(StringBuilder::new()),
            ArrowLeafType::Int64 => Self::Int64(Int64Builder::new()),
            ArrowLeafType::UInt64 => Self::UInt64(UInt64Builder::new()),
            ArrowLeafType::Float64 => Self::Float64(Float64Builder::new()),
            ArrowLeafType::Bool => Self::Boolean(BooleanBuilder::new()),
            ArrowLeafType::Binary => Self::Binary(BinaryBuilder::new()),
            ArrowLeafType::TimestampNs => Self::TimestampNs(TimestampNanosecondBuilder::new()),
            // ArrowLeafType is `#[non_exhaustive]`. Default any
            // future-added leaf type to a String column so the build
            // doesn't break — the codegen will emit nulls until the
            // dispatch layer is updated.
            _ => Self::Utf8(StringBuilder::new()),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Utf8(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::UInt64(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Boolean(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::TimestampNs(b) => b.append_null(),
        }
    }

    fn append_str(&mut self, value: &str, ty: ArrowLeafType) {
        match self {
            Self::Utf8(b) => b.append_value(value),
            Self::Binary(b) => b.append_value(value.as_bytes()),
            // Number columns: the wire format produced bytes for a
            // string (e.g. a Pii-redacted "<redacted>" marker placed
            // on a numeric field). Best-effort: drop into null.
            _ => {
                let _ = ty;
                self.append_null();
            }
        }
    }

    fn append_i64(&mut self, value: i64, _ty: ArrowLeafType) {
        match self {
            Self::Int64(b) => b.append_value(value),
            Self::UInt64(b) => b.append_value(value as u64),
            Self::Float64(b) => b.append_value(value as f64),
            Self::TimestampNs(b) => b.append_value(value),
            _ => self.append_null(),
        }
    }

    fn append_u64(&mut self, value: u64, _ty: ArrowLeafType) {
        match self {
            Self::UInt64(b) => b.append_value(value),
            Self::Int64(b) => b.append_value(value as i64),
            Self::Float64(b) => b.append_value(value as f64),
            Self::TimestampNs(b) => b.append_value(value as i64),
            _ => self.append_null(),
        }
    }

    fn append_f64(&mut self, value: f64, _ty: ArrowLeafType) {
        match self {
            Self::Float64(b) => b.append_value(value),
            Self::Int64(b) => b.append_value(value as i64),
            Self::UInt64(b) => b.append_value(value as u64),
            _ => self.append_null(),
        }
    }

    fn append_bool(&mut self, value: bool, _ty: ArrowLeafType) {
        match self {
            Self::Boolean(b) => b.append_value(value),
            _ => self.append_null(),
        }
    }

    fn append_bytes(&mut self, value: &[u8], _ty: ArrowLeafType) {
        match self {
            Self::Binary(b) => b.append_value(value),
            Self::Utf8(b) => match std::str::from_utf8(value) {
                Ok(s) => b.append_value(s),
                Err(_) => b.append_null(),
            },
            _ => self.append_null(),
        }
    }

    fn finish(&mut self) -> Result<ArrayRef, ParquetSinkError> {
        Ok(match self {
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::UInt64(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::TimestampNs(b) => Arc::new(b.finish()),
        })
    }
}

fn field_to_arrow(f: &ArrowField) -> Field {
    let dt = match f.ty {
        ArrowLeafType::Utf8 | ArrowLeafType::DictUtf8 => DataType::Utf8,
        ArrowLeafType::Int64 => DataType::Int64,
        ArrowLeafType::UInt64 => DataType::UInt64,
        ArrowLeafType::Float64 => DataType::Float64,
        ArrowLeafType::Bool => DataType::Boolean,
        ArrowLeafType::Binary => DataType::Binary,
        ArrowLeafType::TimestampNs => DataType::Timestamp(TimeUnit::Nanosecond, None),
        // ArrowLeafType is `#[non_exhaustive]` — default to Utf8.
        _ => DataType::Utf8,
    };
    // Children are nullable so a row that doesn't write every field
    // can back-fill nulls without errors.
    Field::new(&f.name, dt, true)
}

/// Build one [`EventColumn`] per registered schema, sorted by
/// `full_name` for deterministic column order.
pub(crate) fn event_columns_from_registry(registry: &SchemaRegistry) -> Vec<EventColumn> {
    let schemas: Vec<&'static dyn EventSchemaErased> = registry.iter().collect();
    let model = obs_core::ArrowSchemaModel::from_schemas(schemas);
    model.events.iter().map(EventColumn::new).collect()
}
