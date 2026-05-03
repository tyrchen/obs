//! Roundtrip the ResourceAttrs columns through the `ParquetSink` and
//! read them back via `arrow::reader`. Spec 95 § 3.3 / D8-3 / P1-AE.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]

use std::sync::Arc;

use arrow_array::{Array, MapArray, RecordBatch, StringArray};
use obs_core::{
    ResourceAttrs, SchemaRegistry, ScrubbedEnvelope, Sink, StandardObserver,
    observer::with_test_observer,
};
use obs_parquet::ParquetSink;
use obs_proto::obs::v1::ObsEnvelope;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[test]
fn parquet_sink_emits_resource_columns_from_observer_snapshot() {
    let dir = tempfile::tempdir().expect("tmp");

    // Install an observer with non-default ResourceAttrs.
    let observer = StandardObserver::builder()
        .service("api", "1.2.3")
        .spawn_workers(false)
        .build()
        .expect("observer");
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("k8s.cluster.name".to_string(), "alpha".to_string());
    observer.set_resource_attrs(ResourceAttrs {
        service_name: "api".into(),
        service_version: "1.2.3".into(),
        service_namespace: "payments".into(),
        service_instance_id: "pod-123".into(),
        deployment_environment: "staging".into(),
        host_name: "host-7".into(),
        host_arch: "arm64".into(),
        extra,
    });
    let observer: Arc<dyn obs_core::Observer> = Arc::new(observer);

    let registry = Arc::new(SchemaRegistry::empty());

    let env = ObsEnvelope {
        full_name: "myapp.v1.ResourceProbe".to_string(),
        schema_hash: 0xDEAD_BEEF,
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
        ts_ns: 1_777_731_600_000_000_000,
        service: "api".into(),
        instance: "pod-123".into(),
        version: "1.2.3".into(),
        ..Default::default()
    };

    with_test_observer(observer, || {
        let sink = ParquetSink::builder()
            .base_dir(dir.path())
            .registry(Arc::clone(&registry))
            .partition_by(&["service", "date"])
            .build()
            .expect("build");
        sink.deliver(ScrubbedEnvelope::for_test(&env, &registry));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            sink.flush().await;
        });
    });

    let parquet_path = walk_parquet(dir.path()).expect("parquet file");
    let file = std::fs::File::open(&parquet_path).expect("open");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("reader");
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().expect("collect");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    let utf8_col = |name: &str| -> Option<String> {
        let idx = batch.schema().index_of(name).ok()?;
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 col");
        if col.is_null(0) {
            None
        } else {
            Some(col.value(0).to_string())
        }
    };

    assert_eq!(utf8_col("service_namespace").as_deref(), Some("payments"));
    assert_eq!(utf8_col("environment").as_deref(), Some("staging"));
    assert_eq!(utf8_col("host_name").as_deref(), Some("host-7"));
    assert_eq!(utf8_col("host_arch").as_deref(), Some("arm64"));

    // attrs map carries the extras.
    let attrs_idx = batch.schema().index_of("attrs").expect("attrs col");
    let attrs = batch
        .column(attrs_idx)
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("map array");
    let entries = attrs.value(0);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("keys utf8");
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("values utf8");
    let mut found = false;
    for i in 0..keys.len() {
        if keys.value(i) == "k8s.cluster.name" {
            assert_eq!(values.value(i), "alpha");
            found = true;
        }
    }
    assert!(found, "ResourceAttrs::extra must round-trip into attrs map");
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
