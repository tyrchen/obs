//! `obs validate` — parse `.proto` files and confirm every event /
//! field carries valid `obs.v1.event` / `obs.v1.field` annotations.
//! Spec 50 § 3.5.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use buffa::Message;
use buffa_descriptor::generated::descriptor::FileDescriptorSet;
use buffa_reflect::DescriptorPool;
use obs_build::reflect::scan_pool;

use super::schema_source::{tempdir, write_bundled_options};

#[derive(clap::Args, Debug)]
pub struct ValidateArgs {
    /// One or more `.proto` files. `--include` directories are searched
    /// for imports.
    pub files: Vec<PathBuf>,

    /// Additional include directories passed to `protoc`.
    #[arg(long, value_name = "DIR")]
    pub include: Vec<PathBuf>,
}

pub fn run(args: ValidateArgs) -> Result<()> {
    if args.files.is_empty() {
        return Err(anyhow!("provide one or more .proto files to validate"));
    }
    let tmp = tempdir()?;
    let bundled = tmp.path().join("bundled");
    write_bundled_options(&bundled)?;
    let fds_path = tmp.path().join("fds.bin");
    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());

    let mut cmd = std::process::Command::new(&protoc);
    cmd.arg("--include_imports");
    cmd.arg(format!("--descriptor_set_out={}", fds_path.display()));
    cmd.arg(format!("--proto_path={}", bundled.display()));
    for inc in &args.include {
        cmd.arg(format!("--proto_path={}", inc.display()));
    }
    // Add each .proto file's parent dir as a proto_path so absolute
    // paths resolve, and pass the file's basename so protoc records a
    // short relative path in the FDS (and avoids the "Could not make
    // proto path relative" error).
    for f in &args.files {
        if let Some(parent) = f.parent()
            && !parent.as_os_str().is_empty()
        {
            cmd.arg(format!("--proto_path={}", parent.display()));
        }
        if let Some(name) = f.file_name() {
            cmd.arg(name);
        } else {
            cmd.arg(f);
        }
    }
    let status = cmd.status().context("invoke protoc")?;
    if !status.success() {
        return Err(anyhow!("protoc exit status {status}"));
    }
    let bytes = std::fs::read(&fds_path)?;
    let fds =
        FileDescriptorSet::decode_from_slice(&bytes).map_err(|e| anyhow!("decode FDS: {e}"))?;
    let pool = DescriptorPool::from_file_descriptor_set(fds)
        .map_err(|e| anyhow!("descriptor pool: {e}"))?;
    let events = scan_pool(&pool)?;
    println!("OK · {} annotated event(s)", events.len());
    for e in events {
        println!(
            "  - {} (tier={:?}, sev={:?}, fields={})",
            e.full_name,
            e.tier(),
            e.default_sev(),
            e.fields.len()
        );
    }
    Ok(())
}
