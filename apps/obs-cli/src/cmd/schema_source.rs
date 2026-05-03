//! Builds a `buffa_reflect::DescriptorPool` from one of three sources:
//!
//! 1. A directory of `.proto` files (default), invoked via `protoc` with a workspace-bundled
//!    `obs/v1/options.proto` include.
//! 2. A precompiled `FileDescriptorSet` file (`--schemas-fds`).
//! 3. The built-in obs schemas only (no user input — used by `obs schema show` for the runtime
//!    self-events).
//!
//! Spec 14 § 10.1 (CLI-only descriptor walk) + spec 50 § 3.4.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, anyhow};
use buffa::Message;
use buffa_descriptor::generated::descriptor::FileDescriptorSet;
use buffa_reflect::DescriptorPool;
use clap::Args;

#[derive(Args, Debug, Clone)]
pub struct SchemaSourceArgs {
    /// Path to a directory of `.proto` files to scan.
    #[arg(long, value_name = "DIR")]
    pub schemas: Option<PathBuf>,

    /// Path to a pre-compiled FileDescriptorSet (skips `protoc`).
    #[arg(long, value_name = "FILE")]
    pub schemas_fds: Option<PathBuf>,

    /// Override the workspace event prefix (default `Obs`).
    #[arg(
        long,
        value_name = "PREFIX",
        default_value = "Obs",
        env = "OBS_EVENT_PREFIX"
    )]
    pub event_prefix: String,
}

impl SchemaSourceArgs {
    /// Resolve to a [`DescriptorPool`] populated with the schemas
    /// requested by the user.
    ///
    /// # Errors
    ///
    /// Returns an error when neither `--schemas` nor `--schemas-fds` is
    /// set and the built-in schemas are insufficient, or when protoc
    /// fails / FDS bytes cannot be decoded.
    pub fn build_pool(&self) -> Result<DescriptorPool> {
        if let Some(fds_path) = &self.schemas_fds {
            return load_fds_file(fds_path);
        }
        if let Some(dir) = &self.schemas {
            let fds = compile_dir_to_fds(dir)?;
            return decode_fds(&fds);
        }
        Err(anyhow!(
            "no schema source provided. Pass --schemas <dir> or --schemas-fds <file>."
        ))
    }
}

fn load_fds_file(path: &Path) -> Result<DescriptorPool> {
    let bytes =
        std::fs::read(path).with_context(|| format!("reading FDS file `{}`", path.display()))?;
    decode_fds(&bytes)
}

fn decode_fds(bytes: &[u8]) -> Result<DescriptorPool> {
    let fds = FileDescriptorSet::decode_from_slice(bytes)
        .map_err(|e| anyhow!("decode FileDescriptorSet: {e}"))?;
    DescriptorPool::from_file_descriptor_set(fds).map_err(|e| anyhow!("build descriptor pool: {e}"))
}

fn compile_dir_to_fds(dir: &Path) -> Result<Vec<u8>> {
    let proto_files = collect_proto_files(dir)?;
    if proto_files.is_empty() {
        return Err(anyhow!("no `.proto` files found under `{}`", dir.display()));
    }
    let tmp = tempdir()?;
    let fds_path = tmp.path().join("fds.bin");
    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());

    // Materialise the bundled obs/v1/options.proto + enums.proto so the
    // user's protos can `import "obs/v1/options.proto"` without
    // vendoring it.
    let bundled = tmp.path().join("bundled");
    write_bundled_options(&bundled)?;

    let mut cmd = std::process::Command::new(&protoc);
    // protoc resolves the file paths relative to the current working
    // directory before checking the --proto_path roots, so we set cwd
    // to `dir` and then pass each .proto file as its `dir`-relative
    // path. The relative form is also what protoc records in the FDS
    // (so error messages say `myapp/v1/evt.proto`, not the host's
    // tmp-dir path).
    cmd.current_dir(dir);
    cmd.arg("--include_imports");
    cmd.arg(format!("--descriptor_set_out={}", fds_path.display()));
    cmd.arg(format!("--proto_path={}", dir.display()));
    cmd.arg(format!("--proto_path={}", bundled.display()));
    for f in &proto_files {
        cmd.arg(f);
    }
    let status = cmd.status().with_context(|| format!("spawn `{protoc}`"))?;
    if !status.success() {
        return Err(anyhow!("protoc exit status {status}"));
    }
    Ok(std::fs::read(&fds_path)?)
}

fn collect_proto_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    walk(dir, dir, &mut out)?;
    out.sort();
    Ok(out)
}

fn walk(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir `{}`", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk(root, &path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("proto") {
            // Path stored relative to `root` so protoc finds the file
            // under the matching --proto_path entry.
            if let Ok(stripped) = path.strip_prefix(root) {
                out.push(stripped.to_path_buf());
            } else {
                out.push(path);
            }
        }
    }
    Ok(())
}

const EMBEDDED_OPTIONS_PROTO: &str =
    include_str!("../../../../crates/obs-proto/proto/obs/v1/options.proto");
const EMBEDDED_ENUMS_PROTO: &str =
    include_str!("../../../../crates/obs-proto/proto/obs/v1/enums.proto");

pub(crate) fn write_bundled_options(target: &Path) -> Result<()> {
    let dir = target.join("obs").join("v1");
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("options.proto"), EMBEDDED_OPTIONS_PROTO)?;
    std::fs::write(dir.join("enums.proto"), EMBEDDED_ENUMS_PROTO)?;
    Ok(())
}

// ─── tiny tempdir helper ──────────────────────────────────────────────

pub(crate) struct TempDir(PathBuf);
impl TempDir {
    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
static TEMPDIR_SEQ: AtomicU64 = AtomicU64::new(0);

pub(crate) fn tempdir() -> Result<TempDir> {
    let mut path = std::env::temp_dir();
    let seq = TEMPDIR_SEQ.fetch_add(1, Ordering::Relaxed);
    path.push(format!("obs_cli_{}_{}", std::process::id(), seq));
    std::fs::create_dir_all(&path)?;
    Ok(TempDir(path))
}
