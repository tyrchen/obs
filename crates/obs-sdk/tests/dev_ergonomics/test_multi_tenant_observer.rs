//! `test_multi_tenant_observer` — verifies that two threads each
//! installing their own `InMemoryObserver` see only their own
//! envelopes, even when both emit the same event type concurrently.
//!
//! Phase 4 task 4B.9 lands the full per-task `Future::with_observer`
//! end-to-end test (with three concurrent tenant tasks each mapped to
//! a distinct OTLP endpoint); Phase 2 covers the per-thread tier here
//! to back the propagation matrix from spec 11 § 3.1. Spec 72 § 7.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use obs_sdk::{Emit, Event, InMemoryObserver, Observer, with_test_observer};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsTenantProbe {
    #[obs(label, cardinality = "low")]
    pub tenant: String,
}

#[test]
fn test_per_thread_observers_should_not_cross_contaminate() {
    static MISMATCHES: AtomicUsize = AtomicUsize::new(0);

    let mut handles = Vec::new();
    for tenant in ["alpha", "beta", "gamma", "delta"] {
        handles.push(std::thread::spawn(move || {
            let observer = InMemoryObserver::new();
            let in_handle = observer.handle();
            let observer: Arc<dyn Observer> = Arc::new(observer);
            with_test_observer(observer, || {
                for _ in 0..32 {
                    ObsTenantProbe {
                        tenant: tenant.to_string(),
                    }
                    .emit();
                }
            });
            for env in in_handle.snapshot() {
                if env.labels.get("tenant").map(String::as_str) != Some(tenant) {
                    MISMATCHES.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("tenant thread joined cleanly");
    }
    assert_eq!(
        MISMATCHES.load(Ordering::Relaxed),
        0,
        "per-thread observer slot should isolate tenants completely"
    );
}
