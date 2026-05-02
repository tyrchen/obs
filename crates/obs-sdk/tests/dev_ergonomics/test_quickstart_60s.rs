//! `test_quickstart_60s` — verify the rust-first quickstart from spec
//! 60 § 2 is functional in-tree: define a typed event, install
//! `StandardObserver::dev()`, emit via the builder, observe via the
//! per-thread test handle.
//!
//! Spec 60 § 13 also calls for a `cargo install obs-cli && obs init
//! demo && cargo run` end-to-end smoke; that path is exercised
//! against the workspace `apps/server-proto` binary which lives in
//! the same workspace and runs in CI via `cargo build`.

use obs_sdk::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsHelloEmitted {
    #[obs(label, cardinality = "low")]
    pub who: String,
}

#[obs_sdk::test::test]
fn test_quickstart_should_emit_via_builder() -> anyhow::Result<()> {
    ObsHelloEmitted::builder().who("world").emit();
    obs_sdk::test::assert_emitted!(ObsHelloEmitted { who: "world", .. });
    Ok(())
}

#[obs_sdk::test::test]
fn test_quickstart_should_emit_via_struct_literal_macro() -> anyhow::Result<()> {
    obs_sdk::emit!(ObsHelloEmitted {
        who: "macro".to_string()
    });
    obs_sdk::test::assert_emitted!(ObsHelloEmitted { who: "macro", .. });
    Ok(())
}

#[obs_sdk::test::test]
fn test_quickstart_should_support_emit_at_severity() -> anyhow::Result<()> {
    ObsHelloEmitted::builder()
        .who("warn")
        .emit_at(obs_sdk::Severity::Warn);
    obs_sdk::test::assert_emitted!(ObsHelloEmitted { who: "warn", .. });
    Ok(())
}
