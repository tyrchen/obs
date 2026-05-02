//! `test_compile_errors` — assert that the trybuild compile-error
//! snapshots in `obs-macros/tests/trybuild/` exist and pin the lint
//! IDs that spec 60 § 6 documents. Spec 72 § 4 + spec 72 § 7.
//!
//! The actual `cargo test --test trybuild` lives under `obs-macros`
//! (where the proc-macro is); duplicating it here would compile every
//! fixture twice. Instead we read the `.stderr` snapshots and assert
//! the lint IDs surface, so a regression in either crate (silently
//! dropping a lint) fails this test.

use std::{fs, path::PathBuf};

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("obs-macros")
        .join("tests")
        .join("trybuild")
        .join("fail")
}

fn read_stderr(name: &str) -> String {
    let path = fixtures_dir().join(format!("{name}.stderr"));
    fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "snapshot {} missing — regenerate via TRYBUILD=overwrite cargo test",
            path.display()
        )
    })
}

#[test]
fn test_l001_should_pin_label_high_cardinality_message() {
    let s = read_stderr("L001_label_high_cardinality");
    assert!(s.contains("L001"), "missing lint ID:\n{s}");
    assert!(s.contains("LABEL"), "missing rule kind:\n{s}");
    assert!(
        s.contains("note:") || s.contains("help:"),
        "missing note/help line per spec 60 § 6:\n{s}"
    );
}

#[test]
fn test_l002_should_pin_pii_label_message() {
    let s = read_stderr("L002_pii_label");
    assert!(s.contains("L002"), "missing lint ID:\n{s}");
    assert!(s.contains("PII"), "missing classification:\n{s}");
    assert!(
        s.contains("note:") || s.contains("help:"),
        "missing note/help line per spec 60 § 6:\n{s}"
    );
}

#[test]
fn test_l003_should_pin_secret_on_log_tier_message() {
    let s = read_stderr("L003_secret_on_log");
    assert!(s.contains("L003"), "missing lint ID:\n{s}");
    assert!(s.contains("SECRET"), "missing classification:\n{s}");
}

#[test]
fn test_l011_should_pin_obs_prefix_message() {
    let s = read_stderr("L011_missing_obs_prefix");
    assert!(s.contains("L011"), "missing lint ID:\n{s}");
    assert!(s.contains("Obs"), "missing prefix mention:\n{s}");
    assert!(
        s.contains("rename to"),
        "missing fix suggestion per spec 60 § 6:\n{s}"
    );
}
