//! Smoke tests for the `obs` CLI subcommands. Runs the compiled binary
//! via `Command::new(env!("CARGO_BIN_EXE_obs"))` so we test the same
//! artefact CI ships.

#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]

use std::{
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

fn obs_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_obs"))
}

fn workspace_proto_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("crates")
        .join("obs-proto")
        .join("proto")
}

#[test]
fn test_obs_version_should_print_envelope_formats() {
    let out = Command::new(obs_bin())
        .arg("version")
        .output()
        .expect("run obs version");
    assert!(out.status.success(), "exit: {}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("envelope formats: 2"), "stdout: {text}");
}

#[test]
fn test_obs_completions_bash_should_emit_a_script() {
    let out = Command::new(obs_bin())
        .args(["completions", "bash"])
        .output()
        .expect("run obs completions bash");
    assert!(out.status.success(), "exit: {}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("complete -F"),
        "expected bash completion stanza, got: {text}"
    );
}

#[test]
fn test_obs_init_should_scaffold_rust_mode() {
    let tmp = tempdir();
    let target = tmp.path().join("myapp");
    let out = Command::new(obs_bin())
        .args([
            "init",
            "--mode",
            "rust",
            "--name",
            "myapp",
            target.to_str().unwrap(),
        ])
        .output()
        .expect("run obs init");
    assert!(
        out.status.success(),
        "exit: {} · stderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(target.join("Cargo.toml").exists());
    assert!(target.join("src").join("main.rs").exists());
    assert!(target.join("src").join("events.rs").exists());
    assert!(target.join("obs.yaml").exists());

    // Re-run should be idempotent (no error, says skipped).
    let out2 = Command::new(obs_bin())
        .args([
            "init",
            "--mode",
            "rust",
            "--name",
            "myapp",
            target.to_str().unwrap(),
        ])
        .output()
        .expect("re-run obs init");
    assert!(out2.status.success());
    let stdout = String::from_utf8_lossy(&out2.stdout);
    assert!(stdout.contains("skipped"), "stdout: {stdout}");
}

#[test]
fn test_obs_validate_should_pass_on_a_well_formed_proto() {
    let tmp = tempdir();
    let proto = tmp.path().join("evt.proto");
    std::fs::write(
        &proto,
        r#"syntax = "proto3";
package myapp.v1;
import "obs/v1/options.proto";
message ObsHelloEmitted {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };
  string who = 1 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
}
"#,
    )
    .unwrap();
    let out = Command::new(obs_bin())
        .args([
            "validate",
            "--include",
            workspace_proto_dir().to_str().unwrap(),
            proto.to_str().unwrap(),
        ])
        .output()
        .expect("run obs validate");
    assert!(
        out.status.success(),
        "exit: {} · stderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("OK"), "stdout: {text}");
    assert!(text.contains("ObsHelloEmitted"), "stdout: {text}");
}

#[test]
fn test_obs_lint_should_flag_l011_on_missing_prefix() {
    let tmp = tempdir();
    let proto_dir = tmp.path().join("proto").join("myapp").join("v1");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("evt.proto"),
        r#"syntax = "proto3";
package myapp.v1;
import "obs/v1/options.proto";
message Bad {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };
  string who = 1 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
}
"#,
    )
    .unwrap();
    let out = Command::new(obs_bin())
        .args([
            "lint",
            "--schemas",
            tmp.path().join("proto").to_str().unwrap(),
        ])
        .output()
        .expect("run obs lint");
    assert!(!out.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("[L011]"), "stderr: {stderr}");
}

#[test]
fn test_obs_schema_show_should_print_event_details() {
    let tmp = tempdir();
    let proto_dir = tmp.path().join("proto").join("myapp").join("v1");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("evt.proto"),
        r#"syntax = "proto3";
package myapp.v1;
import "obs/v1/options.proto";
message ObsLoggedIn {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };
  string method = 1 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
  string user_id = 2 [(obs.v1.field) = { kind: ATTRIBUTE, classification: PII }];
}
"#,
    )
    .unwrap();
    let out = Command::new(obs_bin())
        .args([
            "schema",
            "show",
            "myapp.v1.ObsLoggedIn",
            "--schemas",
            tmp.path().join("proto").to_str().unwrap(),
        ])
        .output()
        .expect("run obs schema show");
    assert!(
        out.status.success(),
        "exit: {} · stderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("Event:"), "stdout: {text}");
    assert!(text.contains("ObsLoggedIn"), "stdout: {text}");
    assert!(text.contains("method"), "stdout: {text}");
    assert!(text.contains("user_id"), "stdout: {text}");
}

#[test]
fn test_obs_lint_should_downgrade_to_warning_with_allow() {
    let tmp = tempdir();
    let proto_dir = tmp.path().join("proto").join("myapp").join("v1");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("evt.proto"),
        r#"syntax = "proto3";
package myapp.v1;
import "obs/v1/options.proto";
message Bad {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };
  string who = 1 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
}
"#,
    )
    .unwrap();
    let out = Command::new(obs_bin())
        .args([
            "lint",
            "--allow",
            "L011",
            "--schemas",
            tmp.path().join("proto").to_str().unwrap(),
        ])
        .output()
        .expect("run obs lint --allow L011");
    assert!(out.status.success(), "expected zero exit with --allow L011");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("warning[L011]"), "stderr: {stderr}");
}

#[test]
fn test_obs_lint_should_filter_events_by_pattern() {
    let tmp = tempdir();
    let proto_dir = tmp.path().join("proto").join("myapp").join("v1");
    std::fs::create_dir_all(&proto_dir).unwrap();
    std::fs::write(
        proto_dir.join("evt.proto"),
        r#"syntax = "proto3";
package myapp.v1;
import "obs/v1/options.proto";
message Bad {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };
  string who = 1 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
}
message ObsGood {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };
  string who = 1 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
}
"#,
    )
    .unwrap();
    // --filter "ObsGood" should skip the Bad message → exit 0.
    let out = Command::new(obs_bin())
        .args([
            "lint",
            "--filter",
            "ObsGood",
            "--schemas",
            tmp.path().join("proto").to_str().unwrap(),
        ])
        .output()
        .expect("run obs lint --filter");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Spec 50 § 4 / spec 93 P1-9: summary line lives on stderr.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("1 event(s) scanned"), "stderr: {stderr}");
}

// ─── tiny tempdir helper ──────────────────────────────────────────────

struct TempDir(PathBuf);
impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
static TEMPDIR_SEQ: AtomicU64 = AtomicU64::new(0);

fn tempdir() -> TempDir {
    let mut path = std::env::temp_dir();
    let seq = TEMPDIR_SEQ.fetch_add(1, Ordering::Relaxed);
    path.push(format!("obs_cli_smoke_{}_{}", std::process::id(), seq));
    std::fs::create_dir_all(&path).unwrap();
    TempDir(path)
}
