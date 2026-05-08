//! `test_in_memory_observer` — verifies `assert_emitted!` matches on
//! partial fields and `wait_for` times out. Spec 60 § 13.

use obs_kit::{Emit, Event, Severity, with_test_observer};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsRequestCompleted {
    #[obs(label, cardinality = "low")]
    pub route: String,
    #[obs(label, cardinality = "low")]
    pub status: String,
}

#[test]
fn test_assert_emitted_should_match_partial_fields() {
    let (observer, _handle, _g) = obs_kit::test::install_thread_handle();
    with_test_observer(observer, || {
        ObsRequestCompleted {
            route: "list_users".into(),
            status: "ok".into(),
        }
        .emit();

        // Partial match — only `route` named.
        obs_kit::test::assert_emitted!(ObsRequestCompleted {
            route: "list_users",
            ..
        });

        // Empty body — match by type name only.
        obs_kit::test::assert_emitted!(ObsRequestCompleted { .. });
    });
}

#[test]
fn test_in_memory_handle_wait_for_should_timeout() {
    let (_observer, handle, _g) = obs_kit::test::install_thread_handle();
    // Nothing emitted; `wait_for` should time out.
    let res = handle.wait_for(1, std::time::Duration::from_millis(20));
    assert!(res.is_none(), "expected wait_for to time out");
}

#[test]
fn test_emit_at_should_be_callable() {
    let (observer, handle, _g) = obs_kit::test::install_thread_handle();
    with_test_observer(observer, || {
        ObsRequestCompleted {
            route: "x".into(),
            status: "ok".into(),
        }
        .emit_at(Severity::Warn);
    });
    let drained = handle.drain();
    assert_eq!(drained.len(), 1, "emit_at should still produce one event");
}
