//! `obs_build::Config` — the build-script entry point.
//!
//! Walks the FDS via `buffa-reflect`, reads `(obs.v1.event)` /
//! `(obs.v1.field)` custom options out of the
//! `__buffa_unknown_fields` byte stream (per
//! `docs/research/spike-buffa-reflect.md`), and emits four files into
//! `$OUT_DIR/obs/`:
//!
//! - `schemas.rs`  — `EventSchemaErased` impls + `linkme` registrations
//! - `builders.rs` — fluent setter + `.emit()` per event
//! - `lints.rs`    — const-eval lint asserts (L001/L002/L003/L011)
//! - `arrow_schema.rs` — Arrow fragment dispatch table (Phase-2 stub)
//!
//! The user wires every file in via:
//!
//! ```ignore
//! obs::include_schemas!("myapp.v1");   // one macro, four `include!`s
//! ```
//!
//! Spec 12 § 3.1 + § 4.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use buffa::Message;
use buffa_descriptor::generated::descriptor::FileDescriptorSet;
use buffa_reflect::{DescriptorPool, Kind};

use crate::{
    codegen::{
        EventDecl, FieldDecl, render_arrow_schema, render_builders, render_lints, render_schemas,
    },
    lints::LintProtoType,
    options::{CodegenError, read_event_options, read_field_options},
};

/// Source of the `FileDescriptorSet`. Spec 12 § 4.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum DescriptorSource {
    /// Invoke `protoc` (default). Requires it on `PATH` or via
    /// `PROTOC` env.
    #[default]
    Protoc,
    /// Use a pre-built FDS file (skips protoc invocation).
    Precompiled(PathBuf),
}

/// Build-script entry point.
#[derive(Debug, Default)]
pub struct Config {
    files: Vec<PathBuf>,
    includes: Vec<PathBuf>,
    out_dir: Option<PathBuf>,
    descriptor_source: DescriptorSource,
    event_prefix: Option<String>,
    /// Codegen feature toggles per spec 12 § 4. The defaults are
    /// conservative: lints + schemas + builders + arrow on; render and
    /// scrub off (deferred to later phases).
    arrow_schema: bool,
    json_render: bool,
    payload_scrub: bool,
    otel_attribute_view: bool,
}

impl Config {
    /// New config with sane defaults (lints + schemas + builders + arrow on).
    #[must_use]
    pub fn new() -> Self {
        Self {
            arrow_schema: true,
            json_render: false,
            payload_scrub: true,
            otel_attribute_view: true,
            ..Self::default()
        }
    }

    /// Add proto file paths.
    #[must_use]
    pub fn files(mut self, files: &[impl AsRef<Path>]) -> Self {
        self.files
            .extend(files.iter().map(|p| p.as_ref().to_owned()));
        self
    }

    /// Add include directories searched by `protoc`.
    #[must_use]
    pub fn include(mut self, dir: impl AsRef<Path>) -> Self {
        self.includes.push(dir.as_ref().to_owned());
        self
    }

    /// Override the output directory. Defaults to `$OUT_DIR`.
    #[must_use]
    pub fn out_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.out_dir = Some(dir.as_ref().to_owned());
        self
    }

    /// Pull `obs/v1/options.proto` from the embedded `obs-build`
    /// package so the user does not need to vendor it. The proto file
    /// is bundled at compile time and extracted to `$OUT_DIR/obs/include/`.
    #[must_use]
    pub fn include_obs_options(mut self) -> Self {
        self.includes
            .push(PathBuf::from("__obs_build_embedded_options__"));
        self
    }

    /// Use a pre-compiled FDS file (skips protoc).
    #[must_use]
    pub fn descriptor_source(mut self, src: DescriptorSource) -> Self {
        self.descriptor_source = src;
        self
    }

    /// Override the workspace event prefix used by lint L011. Defaults
    /// to reading `OBS_EVENT_PREFIX` env var, then falling back to
    /// `"Obs"`. Spec 12 § 3.4.
    #[must_use]
    pub fn event_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.event_prefix = Some(prefix.into());
        self
    }

    /// Toggle Arrow schema fragment emission. On by default. Spec 12 § 4.
    #[must_use]
    pub fn with_arrow_schema(mut self, on: bool) -> Self {
        self.arrow_schema = on;
        self
    }

    /// Toggle JSON-render dispatcher. Off by default; lands in Phase 4. Spec 12 § 4.
    #[must_use]
    pub fn with_json_render(mut self, on: bool) -> Self {
        self.json_render = on;
        self
    }

    /// Toggle payload scrub dispatcher. On by default. Spec 12 § 4.
    #[must_use]
    pub fn with_payload_scrub(mut self, on: bool) -> Self {
        self.payload_scrub = on;
        self
    }

    /// Toggle OTel attribute view emission. On by default. Spec 12 § 4.
    #[must_use]
    pub fn with_otel_attribute_view(mut self, on: bool) -> Self {
        self.otel_attribute_view = on;
        self
    }

    /// Run codegen.
    ///
    /// Two-stage pipeline per spec 12 § 3:
    ///
    /// 1. **Stage 1 — `buffa-build`**: compile the `.proto` files into Rust wire types under
    ///    `$OUT_DIR/obs_buffa.rs` (one entry-point file via `include_file`). Skipped when the user
    ///    pre-built the FDS via `descriptor_source(Precompiled(_))` and there are no `.proto` files
    ///    set on `Config` (the test path).
    /// 2. **Stage 2 — obs codegen**: read `(obs.v1.event)` / `(obs.v1.field)` annotations from the
    ///    descriptor pool and emit `$OUT_DIR/obs/{schemas,builders,lints,arrow_schema}.rs`.
    ///
    /// # Errors
    ///
    /// Returns `CodegenError` for any step that fails: `protoc`
    /// invocation, `buffa-build` invocation, FDS decode, option scan,
    /// generated-file IO.
    pub fn compile(self) -> Result<(), CodegenError> {
        let out_dir = self
            .out_dir
            .clone()
            .or_else(|| std::env::var("OUT_DIR").ok().map(PathBuf::from))
            .ok_or_else(|| CodegenError::Protoc("OUT_DIR not set".into()))?;
        std::fs::create_dir_all(out_dir.join("obs")).map_err(CodegenError::OutputIo)?;

        // Materialise embedded include dir (obs/v1/options.proto) once
        // so both the buffa-build and protoc invocations share it.
        let mut effective_includes = self.includes.clone();
        if effective_includes
            .iter()
            .any(|p| p.as_os_str() == "__obs_build_embedded_options__")
        {
            let embed_dir = out_dir.join("obs").join("include");
            materialise_embedded_options(&embed_dir).map_err(CodegenError::OutputIo)?;
            effective_includes.retain(|p| p.as_os_str() != "__obs_build_embedded_options__");
            effective_includes.push(embed_dir);
        }

        // Stage 1: buffa-build for wire types. Skip when the user has
        // no .proto files (the test path uses a precompiled FDS only).
        if !self.files.is_empty() {
            self.invoke_buffa_build(&out_dir, &effective_includes)?;
        }

        // Stage 2: obs codegen.
        let fds_bytes = self.produce_fds(&out_dir, &effective_includes)?;
        let fds = FileDescriptorSet::decode_from_slice(&fds_bytes)
            .map_err(|e| CodegenError::DescriptorDecode(e.to_string()))?;
        let pool = DescriptorPool::from_file_descriptor_set(fds)
            .map_err(|e| CodegenError::DescriptorDecode(e.to_string()))?;

        let events = collect_event_decls(&pool)?;

        let event_prefix = self
            .event_prefix
            .clone()
            .or_else(|| std::env::var("OBS_EVENT_PREFIX").ok())
            .unwrap_or_else(|| "Obs".to_string());

        // Always emit schemas + builders + lints. Arrow stub gated by toggle.
        std::fs::write(
            out_dir.join("obs").join("schemas.rs"),
            render_schemas(&events),
        )
        .map_err(CodegenError::OutputIo)?;
        std::fs::write(
            out_dir.join("obs").join("builders.rs"),
            render_builders(&events),
        )
        .map_err(CodegenError::OutputIo)?;
        std::fs::write(
            out_dir.join("obs").join("lints.rs"),
            render_lints(&events, &event_prefix),
        )
        .map_err(CodegenError::OutputIo)?;
        if self.arrow_schema {
            std::fs::write(
                out_dir.join("obs").join("arrow_schema.rs"),
                render_arrow_schema(&events),
            )
            .map_err(CodegenError::OutputIo)?;
        } else {
            // Always create the file with an empty stub so
            // `include_schemas!` references resolve regardless of
            // toggle. Spec 12 § 3.1.
            std::fs::write(
                out_dir.join("obs").join("arrow_schema.rs"),
                "// arrow_schema disabled by `with_arrow_schema(false)`\n",
            )
            .map_err(CodegenError::OutputIo)?;
        }
        Ok(())
    }

    fn invoke_buffa_build(
        &self,
        _out_dir: &Path,
        effective_includes: &[PathBuf],
    ) -> Result<(), CodegenError> {
        // buffa-build's `include_file` mode is path-aware:
        //
        // - When `out_dir` is **unset** (default), buffa reads `OUT_DIR` from the env and emits the
        //   entry file with `include!(concat!(env!("OUT_DIR"), "/foo.rs"))` paths. The user can
        //   therefore `include!` the entry from any source file in their crate.
        // - When `out_dir` is **explicitly set**, buffa emits sibling-relative `include!("foo.rs")`
        //   paths, which only resolve correctly when the user `mod foo;`'s the parent.
        //
        // We want the first form (env!("OUT_DIR")-rooted) so
        // `obs::include_schemas!` can drop the entry into any
        // `src/*.rs`. Therefore: only pass `.out_dir(...)` to
        // buffa-build when the user explicitly overrode `obs-build`'s
        // `Config::out_dir`. The OS env `OUT_DIR` cargo supplies
        // covers the production path; tests that override `out_dir`
        // are responsible for `include!`'ing via `mod`s instead.
        let mut cfg = buffa_build::Config::new()
            .files(&self.files)
            .includes(effective_includes)
            .include_file("obs_buffa.rs")
            .generate_views(true);
        if let Some(explicit_out) = &self.out_dir {
            cfg = cfg.out_dir(explicit_out);
        }
        if let DescriptorSource::Precompiled(path) = &self.descriptor_source {
            cfg = cfg.descriptor_set(path);
        }
        cfg.compile()
            .map_err(|e| CodegenError::Buffa(e.to_string()))?;
        Ok(())
    }

    fn produce_fds(
        &self,
        out_dir: &Path,
        effective_includes: &[PathBuf],
    ) -> Result<Vec<u8>, CodegenError> {
        match &self.descriptor_source {
            DescriptorSource::Protoc => self.invoke_protoc(out_dir, effective_includes),
            DescriptorSource::Precompiled(path) => {
                std::fs::read(path).map_err(CodegenError::DescriptorIo)
            }
        }
    }

    fn invoke_protoc(
        &self,
        out_dir: &Path,
        effective_includes: &[PathBuf],
    ) -> Result<Vec<u8>, CodegenError> {
        let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());
        let fds_path = out_dir.join("obs").join("fds.bin");
        let mut cmd = Command::new(&protoc);
        cmd.arg("--include_imports");
        cmd.arg(format!("--descriptor_set_out={}", fds_path.display()));
        for inc in effective_includes {
            cmd.arg(format!("--proto_path={}", inc.display()));
        }
        for f in &self.files {
            cmd.arg(f);
        }
        let status = cmd
            .status()
            .map_err(|e| CodegenError::Protoc(format!("failed to spawn protoc: {e}")))?;
        if !status.success() {
            return Err(CodegenError::Protoc(format!("protoc exit status {status}")));
        }
        std::fs::read(&fds_path).map_err(CodegenError::DescriptorIo)
    }
}

fn collect_event_decls(pool: &DescriptorPool) -> Result<Vec<EventDecl>, CodegenError> {
    let mut events: Vec<EventDecl> = Vec::new();
    for msg in pool.all_messages() {
        let dp = msg.descriptor_proto();
        if !dp.options.is_set() {
            continue;
        }
        let mut bytes = Vec::new();
        dp.options.__buffa_unknown_fields.write_to(&mut bytes);
        let Some(event_opts) = read_event_options(&bytes, msg.full_name())? else {
            continue;
        };
        let mut decl = EventDecl {
            full_name: msg.full_name().to_string(),
            event: event_opts,
            fields: Vec::new(),
        };
        for f in msg.fields() {
            let fdp = f.descriptor_proto();
            let mut fbytes = Vec::new();
            if fdp.options.is_set() {
                fdp.options.__buffa_unknown_fields.write_to(&mut fbytes);
            }
            let opts = read_field_options(&fbytes, &format!("{}/{}", msg.full_name(), f.name()))?
                .unwrap_or_default();
            let proto_type = Some(map_kind_to_lint_type(&f.kind()));
            let wire_rust_type = map_kind_to_rust_type(&f.kind());
            decl.fields.push(FieldDecl {
                name: f.name().to_string(),
                number: f.number(),
                options: opts,
                proto_type,
                wire_rust_type,
            });
        }
        events.push(decl);
    }
    // Stable order so generated bytes are deterministic across runs.
    events.sort_by(|a, b| a.full_name.cmp(&b.full_name));
    Ok(events)
}

fn map_kind_to_rust_type(k: &Kind) -> Option<&'static str> {
    match k {
        Kind::Bool => Some("bool"),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Some("i32"),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Some("i64"),
        Kind::Uint32 | Kind::Fixed32 => Some("u32"),
        Kind::Uint64 | Kind::Fixed64 => Some("u64"),
        Kind::Float => Some("f32"),
        Kind::Double => Some("f64"),
        _ => None,
    }
}

fn map_kind_to_lint_type(k: &Kind) -> LintProtoType {
    match k {
        Kind::String => LintProtoType::String,
        Kind::Bytes => LintProtoType::Bytes,
        Kind::Bool => LintProtoType::Bool,
        Kind::Double | Kind::Float => LintProtoType::Numeric,
        Kind::Int32
        | Kind::Int64
        | Kind::Uint32
        | Kind::Uint64
        | Kind::Sint32
        | Kind::Sint64
        | Kind::Fixed32
        | Kind::Fixed64
        | Kind::Sfixed32
        | Kind::Sfixed64 => LintProtoType::Numeric,
        Kind::Enum(_) => LintProtoType::Numeric,
        Kind::Message(_) => LintProtoType::Other("message".to_string()),
        _ => LintProtoType::Other("unknown".to_string()),
    }
}

const EMBEDDED_OPTIONS_PROTO: &str = include_str!("../../obs-proto/proto/obs/v1/options.proto");
const EMBEDDED_ENUMS_PROTO: &str = include_str!("../../obs-proto/proto/obs/v1/enums.proto");

fn materialise_embedded_options(dir: &Path) -> std::io::Result<()> {
    let target = dir.join("obs").join("v1");
    std::fs::create_dir_all(&target)?;
    std::fs::write(target.join("options.proto"), EMBEDDED_OPTIONS_PROTO)?;
    std::fs::write(target.join("enums.proto"), EMBEDDED_ENUMS_PROTO)?;
    Ok(())
}
