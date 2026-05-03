//! End-to-end test: `StandardObserver` with the per-tier worker pool
//! delivers an envelope to a sink. Spec 11 §§ 4 + 4.1 (pipeline order).

use std::sync::Arc;

use obs_sdk::{InMemorySink, Observer, StandardObserver, Tier};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_worker_pool_should_deliver_to_log_sink() {
    let sink = InMemorySink::new();
    let handle = sink.handle();
    let observer = StandardObserver::builder()
        .service("worker-pool", "0.0.0")
        .filter("info")
        .sink_for(Tier::Log, Arc::new(sink))
        .build()
        .unwrap();

    let env = obs_sdk::ObsEnvelope {
        full_name: "test.v1.WorkerPool".to_string(),
        tier: ::obs_sdk::__private::EnumValue::Known(::obs_sdk::__private::ProtoTier::TIER_LOG),
        sev: ::obs_sdk::__private::EnumValue::Known(
            ::obs_sdk::__private::ProtoSeverity::SEVERITY_INFO,
        ),
        ..Default::default()
    };
    observer.emit_envelope(env);
    observer.flush().await;
    // Yield once more so the spawned worker drains anything still in
    // its mpsc.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let drained = handle.snapshot();
    assert_eq!(drained.len(), 1, "worker pool should deliver one envelope");
    assert_eq!(drained[0].full_name, "test.v1.WorkerPool");
    observer.shutdown().await;
}
