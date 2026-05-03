//! Two-stage codegen for the proto-first sinks-showcase example:
//!
//! 1. `buffa-build` produces wire types under `$OUT_DIR`.
//! 2. `obs-build` produces builders, lints, and the schema registry plumbing for every
//!    `obs.v1.event`-annotated message.
//!
//! `obs_sdk::include_schemas!("showcase.v1")` in `src/main.rs` wires the
//! generated files into the binary. Spec 12 § 3.

#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::expect_used,
    clippy::indexing_slicing
)]

fn main() -> anyhow::Result<()> {
    obs_build::Config::new()
        .files(&["proto/showcase/v1/events.proto"])
        .include("proto")
        .include_obs_options()
        .compile()?;
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}
