// Build scripts run at build time only and are exempt from the
// workspace's tokio-fs / no-expect lints.
#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing
)]

//! Build script for `obs-proto`.
//!
//! Generates Rust types into `src/pb/` (checked-in style, per
//! `buffa-build`'s `out_dir` + `include_file = "mod.rs"` pattern).
//! The `FileDescriptorSet` is written to `$OUT_DIR/obs_proto.fds`
//! and embedded via `include_bytes!` so `obs-core::registry` can
//! load the built-in schemas at runtime regardless of how the
//! binary was linked.

use std::path::PathBuf;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let pb_dir = manifest.join("src").join("pb");
    let fds_path = out_dir.join("obs_proto.fds");

    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=build.rs");

    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());

    let proto_files = [
        "proto/obs/v1/enums.proto",
        "proto/obs/v1/options.proto",
        "proto/obs/v1/envelope.proto",
        "proto/obs/v1/builtin.proto",
        "proto/obs/runtime/v1/self_events.proto",
    ];

    let status = Command::new(&protoc)
        .arg("--proto_path=proto")
        .arg("--include_imports")
        .arg(format!("--descriptor_set_out={}", fds_path.display()))
        .args(proto_files)
        .status()?;
    if !status.success() {
        return Err(format!("protoc failed (status {status})").into());
    }

    // Run buffa-build with `out_dir = src/pb` and `include_file = "mod.rs"`
    // so the checked-in module tree lives under src/pb/. The user does
    // `mod pb;` at lib.rs to wire it in.
    std::fs::create_dir_all(&pb_dir)?;
    buffa_build::Config::new()
        .files(
            proto_files
                .iter()
                .map(|p| p.trim_start_matches("proto/"))
                .collect::<Vec<_>>()
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<_>>()
                .as_slice(),
        )
        .out_dir(&pb_dir)
        .descriptor_set(&fds_path)
        .include_file("mod.rs")
        .compile()?;

    // Embed the FDS path so the crate can include it as bytes at compile
    // time — see lib.rs `BUILTIN_FDS`.
    println!("cargo:rustc-env=OBS_PROTO_FDS={}", fds_path.display());
    Ok(())
}
