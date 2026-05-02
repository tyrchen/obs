//! `obs_build::Config` — the build-script entry point.
//!
//! Walks the FDS via `buffa-reflect`, reads `(obs.v1.event)` /
//! `(obs.v1.field)` custom options out of the
//! `__buffa_unknown_fields` byte stream (per
//! `docs/research/spike-buffa-reflect.md`), and emits per-event
//! `EventSchema` impls + `linkme` registrations into
//! `$OUT_DIR/obs/schemas.rs`.
//!
//! The user wires the generated file in via:
//!
//! ```ignore
//! obs::include_schemas!("myapp.v1");   // expands to include!(...)
//! ```
//!
//! That macro lands in Phase 2; for Phase 1 the user can `include!`
//! the generated file directly.

use std::path::{Path, PathBuf};
use std::process::Command;

use buffa::Message;
use buffa_descriptor::generated::descriptor::FileDescriptorSet;
use buffa_reflect::DescriptorPool;
use heck::ToShoutySnakeCase;
use obs_types::{Cardinality, Classification, FieldKind, Severity, Tier};

use crate::options::{CodegenError, EventOptions, FieldOptions, read_event_options, read_field_options};

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
}

impl Config {
    /// New config with sane defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add proto file paths.
    #[must_use]
    pub fn files(mut self, files: &[impl AsRef<Path>]) -> Self {
        self.files.extend(files.iter().map(|p| p.as_ref().to_owned()));
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
    /// package so the user does not need to vendor it.
    #[must_use]
    pub fn include_obs_options(self) -> Self {
        // Phase-1: a no-op convenience — we recommend the user add
        // their own `import "obs/v1/options.proto"` and include the
        // path. Phase-2 ships the embedded copy.
        self
    }

    /// Use a pre-compiled FDS file (skips protoc).
    #[must_use]
    pub fn descriptor_source(mut self, src: DescriptorSource) -> Self {
        self.descriptor_source = src;
        self
    }

    /// Run codegen.
    ///
    /// # Errors
    ///
    /// Returns `CodegenError` for any step that fails: `protoc`
    /// invocation, FDS decode, option scan, generated-file IO.
    pub fn compile(self) -> Result<(), CodegenError> {
        let out_dir = self
            .out_dir
            .clone()
            .or_else(|| std::env::var("OUT_DIR").ok().map(PathBuf::from))
            .ok_or_else(|| CodegenError::Protoc("OUT_DIR not set".into()))?;
        std::fs::create_dir_all(out_dir.join("obs")).map_err(CodegenError::OutputIo)?;

        let fds_bytes = self.produce_fds(&out_dir)?;
        let fds = FileDescriptorSet::decode_from_slice(&fds_bytes)
            .map_err(|e| CodegenError::DescriptorDecode(e.to_string()))?;
        let pool = DescriptorPool::from_file_descriptor_set(fds)
            .map_err(|e| CodegenError::DescriptorDecode(e.to_string()))?;

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
                if !fdp.options.is_set() {
                    continue;
                }
                let mut fbytes = Vec::new();
                fdp.options.__buffa_unknown_fields.write_to(&mut fbytes);
                let Some(field_opts) = read_field_options(
                    &fbytes,
                    &format!("{}/{}", msg.full_name(), f.name()),
                )?
                else {
                    continue;
                };
                decl.fields.push(FieldDecl {
                    name: f.name().to_string(),
                    number: f.number(),
                    options: field_opts,
                });
            }
            events.push(decl);
        }

        let mut schemas_rs = String::new();
        schemas_rs.push_str("// @generated by obs-build. DO NOT EDIT.\n");
        for evt in &events {
            schemas_rs.push_str(&codegen_event(evt));
            schemas_rs.push('\n');
        }
        std::fs::write(out_dir.join("obs").join("schemas.rs"), schemas_rs)
            .map_err(CodegenError::OutputIo)?;
        Ok(())
    }

    fn produce_fds(&self, out_dir: &Path) -> Result<Vec<u8>, CodegenError> {
        match &self.descriptor_source {
            DescriptorSource::Protoc => self.invoke_protoc(out_dir),
            DescriptorSource::Precompiled(path) => {
                std::fs::read(path).map_err(CodegenError::DescriptorIo)
            }
        }
    }

    fn invoke_protoc(&self, out_dir: &Path) -> Result<Vec<u8>, CodegenError> {
        let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());
        let fds_path = out_dir.join("obs").join("fds.bin");
        let mut cmd = Command::new(&protoc);
        cmd.arg("--include_imports");
        cmd.arg(format!("--descriptor_set_out={}", fds_path.display()));
        for inc in &self.includes {
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

#[derive(Debug)]
struct EventDecl {
    full_name: String,
    event: EventOptions,
    fields: Vec<FieldDecl>,
}

#[derive(Debug)]
struct FieldDecl {
    name: String,
    number: u32,
    options: FieldOptions,
}

fn codegen_event(decl: &EventDecl) -> String {
    let rust_name = decl
        .full_name
        .rsplit('.')
        .next()
        .unwrap_or(&decl.full_name);
    let static_ident = format!("__OBS_SCHEMA_{}", rust_name.to_shouty_snake_case());
    let erased_struct = format!("{}Schema", rust_name);

    let tier = decl.event.tier.unwrap_or(Tier::Log);
    let sev = decl.event.default_sev.unwrap_or(Severity::Info);
    let schema_hash = compute_schema_hash(decl);

    let mut fields_lit = String::from("&[");
    for f in &decl.fields {
        let role_str = field_role_path(f.options.kind.unwrap_or(FieldKind::Attribute));
        let card = cardinality_path(f.options.cardinality.unwrap_or(Cardinality::Unspecified));
        let classn = classification_path(
            f.options.classification.unwrap_or(Classification::Internal),
        );
        fields_lit.push_str(&format!(
            "::obs_core::FieldMeta::new(\"{name}\", {num}, {role}, {card}, {classn}),",
            name = f.name,
            num = f.number,
            role = role_str,
            card = card,
            classn = classn,
        ));
    }
    fields_lit.push(']');

    format!(
        r#"#[doc(hidden)]
#[allow(non_camel_case_types)]
pub struct {erased_struct};

impl ::obs_core::__private::Sealed for {erased_struct} {{}}

impl ::obs_core::__private::EventSchemaErased for {erased_struct} {{
    fn full_name(&self) -> &'static str {{ "{full_name}" }}
    fn schema_hash(&self) -> u64 {{ {hash}u64 }}
    fn tier(&self) -> ::obs_core::__private::Tier {{ {tier_path} }}
    fn default_sev(&self) -> ::obs_core::__private::Severity {{ {sev_path} }}
    fn fields(&self) -> &'static [::obs_core::FieldMeta] {{ {fields} }}
}}

#[used]
#[::obs_core::__private::linkme::distributed_slice(::obs_core::__private::EVENT_SCHEMAS)]
#[linkme(crate = ::obs_core::__private::linkme)]
#[doc(hidden)]
static {static_ident}: &'static dyn ::obs_core::__private::EventSchemaErased = &{erased_struct};
"#,
        erased_struct = erased_struct,
        full_name = decl.full_name,
        hash = schema_hash,
        tier_path = tier_path(tier),
        sev_path = sev_path(sev),
        fields = fields_lit,
        static_ident = static_ident,
    )
}

fn tier_path(t: Tier) -> &'static str {
    match t {
        Tier::Log => "::obs_core::__private::Tier::Log",
        Tier::Metric => "::obs_core::__private::Tier::Metric",
        Tier::Trace => "::obs_core::__private::Tier::Trace",
        Tier::Audit => "::obs_core::__private::Tier::Audit",
        _ => "::obs_core::__private::Tier::Unspecified",
    }
}
fn sev_path(s: Severity) -> &'static str {
    match s {
        Severity::Trace => "::obs_core::__private::Severity::Trace",
        Severity::Debug => "::obs_core::__private::Severity::Debug",
        Severity::Info => "::obs_core::__private::Severity::Info",
        Severity::Warn => "::obs_core::__private::Severity::Warn",
        Severity::Error => "::obs_core::__private::Severity::Error",
        Severity::Fatal => "::obs_core::__private::Severity::Fatal",
        _ => "::obs_core::__private::Severity::Unspecified",
    }
}
fn field_role_path(k: FieldKind) -> &'static str {
    match k {
        FieldKind::Label => "::obs_core::FieldRole::Label",
        FieldKind::Attribute => "::obs_core::FieldRole::Attribute",
        FieldKind::Measurement => "::obs_core::FieldRole::Measurement",
        FieldKind::TraceId => "::obs_core::FieldRole::TraceId",
        FieldKind::SpanId => "::obs_core::FieldRole::SpanId",
        FieldKind::ParentSpanId => "::obs_core::FieldRole::ParentSpanId",
        FieldKind::TimestampNs => "::obs_core::FieldRole::TimestampNs",
        FieldKind::DurationNs => "::obs_core::FieldRole::DurationNs",
        FieldKind::Forensic => "::obs_core::FieldRole::Forensic",
        _ => "::obs_core::FieldRole::Attribute",
    }
}
fn cardinality_path(c: Cardinality) -> &'static str {
    match c {
        Cardinality::Low => "::obs_core::__private::Cardinality::Low",
        Cardinality::Medium => "::obs_core::__private::Cardinality::Medium",
        Cardinality::High => "::obs_core::__private::Cardinality::High",
        Cardinality::Unbounded => "::obs_core::__private::Cardinality::Unbounded",
        _ => "::obs_core::__private::Cardinality::Unspecified",
    }
}
fn classification_path(c: Classification) -> &'static str {
    match c {
        Classification::Pii => "::obs_core::__private::Classification::Pii",
        Classification::Secret => "::obs_core::__private::Classification::Secret",
        Classification::Internal => "::obs_core::__private::Classification::Internal",
        _ => "::obs_core::__private::Classification::Unspecified",
    }
}

fn compute_schema_hash(decl: &EventDecl) -> u64 {
    // Same algorithm as `obs-macros::derive(Event)` so the codegen
    // produces identical hashes when the schemas are equivalent. Spec
    // 12 § 1.2 byte-identical-output property.
    let mut s = String::new();
    s.push_str(&decl.full_name);
    s.push('|');
    s.push_str(decl.event.tier.unwrap_or(Tier::Log).as_str());
    s.push('|');
    s.push_str(decl.event.default_sev.unwrap_or(Severity::Info).as_str());
    s.push('|');
    for f in &decl.fields {
        s.push_str(&f.name);
        s.push(':');
        s.push_str(
            f.options
                .kind
                .unwrap_or(FieldKind::Attribute)
                .as_str(),
        );
        s.push(':');
        s.push_str(
            f.options
                .cardinality
                .unwrap_or(Cardinality::Unspecified)
                .as_str(),
        );
        s.push(':');
        s.push_str(
            f.options
                .classification
                .unwrap_or(Classification::Internal)
                .as_str(),
        );
        s.push(',');
    }
    let h = blake3::hash(s.as_bytes());
    let bytes = h.as_bytes();
    let arr = <[u8; 8]>::try_from(&bytes[..8]).expect("blake3 always produces 32 bytes");
    u64::from_le_bytes(arr)
}
