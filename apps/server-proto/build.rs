//! Two-stage codegen for the proto-first demo:
//!
//! 1. `buffa-build` produces wire types under `$OUT_DIR/obs_buffa.rs`.
//! 2. `obs-build` produces `$OUT_DIR/obs/{schemas,builders,lints,arrow_schema}.rs`.
//!
//! `obs::include_schemas!("myapp.v1")` in `src/main.rs` wires the
//! generated files into the binary. Spec 12 § 3.

#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::expect_used,
    clippy::indexing_slicing
)]

fn main() -> anyhow::Result<()> {
    obs_build::Config::new()
        .files(&["proto/myapp/v1/events.proto"])
        .include("proto")
        .include_obs_options()
        .compile()?;
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}
