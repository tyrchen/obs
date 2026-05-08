//! `test_tracing_bridge` (spec 72 § 7) — Direction A lifts a
//! `tracing::Event` into an `ObsTracingForensicEvent`.

use std::sync::Arc;

use obs_kit::{InMemoryObserver, Observer, with_test_observer};
use obs_tracing_bridge::TracingToObsLayer;
use tracing_subscriber::layer::SubscriberExt;

#[test]
fn test_should_bridge_tracing_event_into_envelope() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    let subscriber = tracing_subscriber::registry().with(TracingToObsLayer::new());
    with_test_observer(observer, || {
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "myapp::handlers", route = "list_users", "request done");
        });
    });
    let drained = handle.drain();
    assert!(!drained.is_empty(), "expected at least one envelope");
    let env = &drained[0];
    assert_eq!(env.full_name, "obs.v1.ObsTracingForensicEvent");
    assert_eq!(
        env.labels.get("target"),
        Some(&"myapp::handlers".to_string())
    );
}
