//! ClickHouse DDL emitter — spec 22 § 3.
//!
//! Walks an [`obs_core::ArrowSchemaModel`] and produces a `CREATE TABLE`
//! statement for the unified `obs_events` table. Used both by the
//! sink's `auto_migrate` path and by the CLI's
//! `obs migrate clickhouse`.

use obs_core::{ArrowEventSchema, ArrowField, ArrowLeafType, ArrowSchemaModel};
use obs_types::FieldKind;

/// Render the full `CREATE TABLE IF NOT EXISTS obs_events ...` for the
/// supplied schema model. Spec 22 § 3.
#[must_use]
pub fn render_create_table_ddl(model: &ArrowSchemaModel, table: &str) -> String {
    let mut s = String::new();
    s.push_str(&format!("CREATE TABLE IF NOT EXISTS {table} (\n"));
    push_envelope_columns(&mut s);
    s.push_str(",\n");
    push_label_and_attr_columns(&mut s);
    for evt in &model.events {
        s.push_str(",\n");
        push_payload_column(&mut s, evt);
    }
    s.push_str(",\n    payload_proto                    String CODEC(ZSTD(3))\n");
    s.push_str(")\n");
    s.push_str("ENGINE = MergeTree\n");
    s.push_str("PARTITION BY toDate(ts_ns)\n");
    s.push_str("ORDER BY (ts_ns, full_name, trace_id);\n");
    s
}

fn push_envelope_columns(s: &mut String) {
    // Spec 95 § 3.3 / D8-3 / P1-AE: ResourceAttrs columns alongside the
    // envelope identity so analytics joins on `service=…` can
    // disambiguate environment / namespace / host. These columns are
    // populated by `obs-clickhouse::sink` from
    // `observer().resource_attrs()`.
    let lines = [
        "    ts_ns                            DateTime64(9)",
        "    full_name                        LowCardinality(String)",
        "    schema_hash                      UInt64",
        "    tier                             LowCardinality(String)",
        "    sev                              LowCardinality(String)",
        "    trace_id                         String",
        "    span_id                          String",
        "    parent_span_id                   String",
        "    callsite_id                      UInt64",
        "    service                          LowCardinality(String)",
        "    instance                         LowCardinality(String)",
        "    version                          LowCardinality(String)",
        "    service_namespace                LowCardinality(String)",
        "    environment                      LowCardinality(String)",
        "    host_name                        LowCardinality(String)",
        "    host_arch                        LowCardinality(String)",
        "    sampling_reason                  LowCardinality(String)",
    ];
    for (i, l) in lines.iter().enumerate() {
        if i > 0 {
            s.push_str(",\n");
        }
        s.push_str(l);
    }
}

fn push_label_and_attr_columns(s: &mut String) {
    s.push_str("    labels                           Map(LowCardinality(String), String)");
    s.push_str(",\n");
    s.push_str("    attrs                            Map(LowCardinality(String), String)");
}

fn push_payload_column(s: &mut String, evt: &ArrowEventSchema) {
    s.push_str("    ");
    s.push_str(&evt.payload_column);
    s.push_str(" Nested(\n");
    for (i, f) in evt.fields.iter().enumerate() {
        if i > 0 {
            s.push_str(",\n");
        }
        s.push_str("        ");
        s.push_str(&f.name);
        s.push(' ');
        s.push_str(&clickhouse_type_for(f));
    }
    s.push_str("\n    )");
}

fn clickhouse_type_for(f: &ArrowField) -> String {
    match (f.kind, f.ty) {
        (FieldKind::Label, ArrowLeafType::DictUtf8) => "LowCardinality(String)".to_string(),
        (FieldKind::Label, _) => "String".to_string(),
        (FieldKind::Attribute, _) => "String".to_string(),
        (FieldKind::Measurement, ArrowLeafType::UInt64) => "UInt64".to_string(),
        (FieldKind::Measurement, ArrowLeafType::Int64) => "Int64".to_string(),
        (FieldKind::Measurement, _) => "Float64".to_string(),
        (FieldKind::TraceId | FieldKind::SpanId | FieldKind::ParentSpanId, _) => {
            "String".to_string()
        }
        (FieldKind::TimestampNs, _) => "DateTime64(9)".to_string(),
        (FieldKind::DurationNs, _) => "UInt64".to_string(),
        (FieldKind::Forensic, _) => "String".to_string(),
        _ => "String".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_render_envelope_columns() {
        let model = ArrowSchemaModel::default();
        let ddl = render_create_table_ddl(&model, "obs_events");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS obs_events"));
        assert!(ddl.contains("ts_ns"));
        assert!(ddl.contains("callsite_id"));
        assert!(ddl.contains("MergeTree"));
        assert!(ddl.contains("PARTITION BY toDate(ts_ns)"));
        assert!(ddl.contains("ORDER BY (ts_ns, full_name, trace_id)"));
    }

    #[test]
    fn test_should_render_resource_columns() {
        let model = ArrowSchemaModel::default();
        let ddl = render_create_table_ddl(&model, "obs_events");
        // Spec 95 § 3.3 / P1-AE.
        assert!(ddl.contains("service_namespace"));
        assert!(ddl.contains("environment"));
        assert!(ddl.contains("host_name"));
        assert!(ddl.contains("host_arch"));
    }
}
