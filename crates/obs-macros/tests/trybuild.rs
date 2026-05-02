//! trybuild fixtures for `#[derive(Event)]` lints (impl-plan task 1.14).
//!
//! Snapshots: each `*.rs` under `tests/trybuild/fail/` is expected to
//! produce a compile error matching the sibling `*.stderr`. Snapshots
//! are regenerated with `TRYBUILD=overwrite cargo test --test trybuild`.

#[test]
fn lint_failures_should_match_snapshots() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/trybuild/fail/*.rs");
}
