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
//! Generates Rust types into `$OUT_DIR` (the idiomatic Cargo pattern)
//! and wires them into the crate via `include!(concat!(env!("OUT_DIR"),
//! "/mod.rs"))` in `lib.rs`. Keeping generated code out of the source
//! tree is what lets Cargo's fingerprint logic skip the build script
//! when nothing under `proto/` has changed — emitting into `src/pb/`
//! would re-touch source files on every build and force perpetual
//! rebuilds of every downstream crate.
//!
//! The `FileDescriptorSet` is written to `$OUT_DIR/obs_proto.fds` and
//! embedded via `include_bytes!` so `obs-core::registry` can load the
//! built-in schemas at runtime regardless of how the binary was linked.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let fds_path = out_dir.join("obs_proto.fds");
    let fds_tmp = out_dir.join("obs_proto.fds.tmp");

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

    // Write the FDS to a temp path, then move it over the real file only
    // when bytes differ. buffa-build emits
    // `cargo:rerun-if-changed=<fds_path>`, and if we unconditionally
    // rewrote the file its mtime would advance on every build — cargo
    // would then see the FDS as "newer than the build-script output
    // marker" and rerun the script on the next invocation, looping
    // forever. Preserving mtime on no-op runs breaks that cycle.
    let status = Command::new(&protoc)
        .arg("--proto_path=proto")
        .arg("--include_imports")
        .arg(format!("--descriptor_set_out={}", fds_tmp.display()))
        .args(proto_files)
        .status()?;
    if !status.success() {
        return Err(format!("protoc failed (status {status})").into());
    }
    write_if_changed(&fds_tmp, &fds_path)?;

    // Leave `out_dir` unset so buffa-build reads `OUT_DIR` from the
    // environment and emits the entry file with
    // `include!(concat!(env!("OUT_DIR"), "/foo.rs"))` paths — the form
    // that resolves from any `include!` site in `src/`.
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
        .descriptor_set(&fds_path)
        .include_file("mod.rs")
        .compile()?;

    println!("cargo:rustc-env=OBS_PROTO_FDS={}", fds_path.display());
    Ok(())
}

fn write_if_changed(src: &Path, dst: &Path) -> std::io::Result<()> {
    let new = std::fs::read(src)?;
    let changed = match std::fs::read(dst) {
        Ok(existing) => existing != new,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => return Err(e),
    };
    if changed {
        std::fs::rename(src, dst)?;
    } else {
        std::fs::remove_file(src)?;
    }
    Ok(())
}
