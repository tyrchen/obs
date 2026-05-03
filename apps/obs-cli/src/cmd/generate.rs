//! `obs generate` — run codegen for the proto-first authoring path.
//! Spec 50 § 3.2 / spec 95 § 3.14 / P2-AK.
//!
//! Drives `obs_build::Config::compile` with the user-supplied
//! `.proto` files, emitting `schemas.rs`, `builders.rs`, `lints.rs`,
//! `arrow_schema.rs` to the target directory. The canonical path is
//! still the `build.rs` integration (spec 12 § 4); `obs generate`
//! exists as an off-band option for CI smoke checks and one-shot
//! scaffolding.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use obs_build::Config;

#[derive(Debug, Args)]
pub struct GenerateArgs {
    /// `.proto` files to compile. Repeatable.
    #[arg(long, value_name = "FILE")]
    pub proto: Vec<PathBuf>,

    /// Include directories searched by `protoc`. Repeatable.
    #[arg(long, value_name = "DIR")]
    pub include: Vec<PathBuf>,

    /// Output directory. The codegen writes
    /// `schemas.rs / builders.rs / lints.rs / arrow_schema.rs` into
    /// `<out>/obs/`. Defaults to `target/obs-generate/`.
    #[arg(long, value_name = "DIR")]
    pub out: Option<PathBuf>,

    /// Skip `obs/v1/options.proto` import injection. Useful when the
    /// caller vendors `options.proto` themselves.
    #[arg(long)]
    pub no_embedded_options: bool,

    /// Workspace event prefix for the L011 lint (default `Obs`).
    #[arg(long, value_name = "PREFIX")]
    pub event_prefix: Option<String>,
}

pub fn run(args: GenerateArgs) -> Result<()> {
    if args.proto.is_empty() {
        anyhow::bail!(
            "obs generate: at least one --proto FILE is required. The canonical path is \
             `obs_build::Config::compile()` from a build.rs (spec 12 § 4); this command exists \
             for CI smoke checks."
        );
    }

    let out = args
        .out
        .unwrap_or_else(|| PathBuf::from("target").join("obs-generate"));
    std::fs::create_dir_all(&out).context("creating output directory")?;

    let mut cfg = Config::new().files(&args.proto).out_dir(&out);
    for inc in &args.include {
        cfg = cfg.include(inc);
    }
    if !args.no_embedded_options {
        cfg = cfg.include_obs_options();
    }
    if let Some(prefix) = args.event_prefix {
        cfg = cfg.event_prefix(prefix);
    }

    cfg.compile().context("obs-build codegen failed")?;

    eprintln!(
        "wrote schemas.rs / builders.rs / lints.rs / arrow_schema.rs into {}",
        out.display()
    );
    Ok(())
}
