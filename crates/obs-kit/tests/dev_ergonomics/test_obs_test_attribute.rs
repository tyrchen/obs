//! `test_obs_test_attribute` — verifies `#[obs::test]` (sync + async)
//! installs an observer, captures emits, and supports `Result<T, E>`
//! returns so tests use `?`. Spec 60 § 8 + spec 72 § 3.

use obs_kit::{Emit, Event};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsAttrProbe {
    #[obs(label, cardinality = "low")]
    pub kind: String,
}

#[obs_kit::test::test]
fn test_obs_test_sync_should_capture_via_assert_emitted() -> anyhow::Result<()> {
    ObsAttrProbe {
        kind: "sync".into(),
    }
    .emit();
    obs_kit::test::assert_emitted!(ObsAttrProbe { kind: "sync", .. });
    Ok(())
}

#[obs_kit::test::test]
async fn test_obs_test_async_should_capture_via_assert_emitted() -> anyhow::Result<()> {
    ObsAttrProbe {
        kind: "async".into(),
    }
    .emit();
    obs_kit::test::assert_emitted!(ObsAttrProbe { kind: "async", .. });
    Ok(())
}

#[obs_kit::test::test]
fn test_obs_test_should_support_no_return() {
    ObsAttrProbe {
        kind: "no_return".into(),
    }
    .emit();
    obs_kit::test::assert_emitted!(ObsAttrProbe { .. });
}
