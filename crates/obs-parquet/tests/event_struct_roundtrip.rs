//! Roundtrip the per-event Nested-struct columns through the
//! `ParquetSink` and read them back via `arrow::reader`. Spec 22 § 1.1
//! / spec 94 § 2.8 / P1-F.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StructArray, UInt64Array};
use obs_core::{EventSchema as _, SchemaRegistry, ScrubbedEnvelope, Sink};
use obs_macros::Event;
use obs_parquet::ParquetSink;
use obs_proto::obs::v1::ObsEnvelope;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsRoundtripCheckout {
    #[obs(label, cardinality = "medium")]
    sku: String,
    #[obs(measurement, metric = "histogram", unit = "ms")]
    latency_ms: u64,
}

#[test]
fn parquet_sink_emits_typed_struct_column_for_registered_event() {
    let dir = tempfile::tempdir().expect("tmp");

    // The registry is global to the test binary via linkme — every
    // schema registered through `#[derive(Event)]` is visible here.
    let registry = Arc::new(SchemaRegistry::from_link_section());
    assert!(
        registry
            .iter()
            .any(|s| s.full_name() == ObsRoundtripCheckout::FULL_NAME),
        "registry must include the test-local event"
    );

    let sink = ParquetSink::builder()
        .base_dir(dir.path())
        .registry(Arc::clone(&registry))
        .partition_by(&["service", "date"])
        .build()
        .expect("build");

    // Build the typed event, encode its payload, and ship through
    // the sink. Mirrors what `Event::emit()` would do at the runtime
    // pipeline's "build envelope" step.
    let evt = ObsRoundtripCheckout {
        sku: "ABC-001".into(),
        latency_ms: 42,
    };
    let mut payload = bytes::BytesMut::new();
    evt.encode_payload(&mut payload);

    let env = ObsEnvelope {
        full_name: ObsRoundtripCheckout::FULL_NAME.to_string(),
        schema_hash: ObsRoundtripCheckout::SCHEMA_HASH,
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
        ts_ns: 1_777_731_600_000_000_000,
        service: "api".into(),
        payload: payload.to_vec(),
        ..Default::default()
    };
    sink.deliver(ScrubbedEnvelope::for_test(&env, &registry));

    // Force flush.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    rt.block_on(async {
        sink.flush().await;
    });

    // Read the parquet file back and inspect the per-event struct
    // column.
    let parquet_path = walk_parquet(dir.path()).expect("at least one parquet file");
    let file = std::fs::File::open(&parquet_path).expect("open parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("reader");
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().expect("collect");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    let payload_column = format!(
        "payload_{}",
        ObsRoundtripCheckout::FULL_NAME
            .replace('.', "_")
            .to_lowercase()
    );
    let col_idx = batch
        .schema()
        .index_of(&payload_column)
        .expect("per-event struct column present");
    let struct_col = batch.column(col_idx);
    let struct_arr = struct_col
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("struct array");
    assert_eq!(struct_arr.len(), 1);
    assert!(!struct_arr.is_null(0), "matching row must be non-null");

    // Inspect the typed `latency_ms` MEASUREMENT field.
    let latency_idx = struct_arr
        .column_names()
        .iter()
        .position(|n| *n == "latency_ms")
        .expect("latency_ms field");
    let latency = struct_arr
        .column(latency_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("latency_ms is uint64");
    assert_eq!(
        latency.value(0),
        42,
        "MEASUREMENT field must round-trip with the typed value"
    );
}

fn walk_parquet(root: &std::path::Path) -> Option<std::path::PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).ok()?.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                return Some(p);
            }
        }
    }
    None
}
