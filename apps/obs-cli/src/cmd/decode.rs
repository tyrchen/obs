//! `obs decode` — decode binary `ObsBatch` (or AUDIT spool) → NDJSON.
//!
//! Spec 50 § 3.8.

use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};
use buffa::Message;
use clap::Args;
use obs_core::audit_spool::recover;
use obs_proto::{
    ENVELOPE_FORMAT_VER,
    obs::v1::{ObsBatch, ObsEnvelope},
};

#[derive(Debug, Args)]
pub struct DecodeArgs {
    /// Source file (or `-` for stdin).
    pub file: Option<PathBuf>,
    /// Treat the source as an AUDIT spool (binary length-prefixed
    /// envelopes + sibling `.crc` file).
    #[arg(long)]
    pub audit_spool: bool,
    /// Skip payload decode; emit raw bytes as base64.
    #[arg(long)]
    pub raw: bool,
}

pub fn run(args: DecodeArgs) -> Result<()> {
    if args.audit_spool {
        return run_audit_spool(args);
    }
    let bytes = read_source(args.file.as_ref())?;
    let batch = ObsBatch::decode_from_slice(&bytes).context("failed to decode ObsBatch")?;
    if batch.format_ver != 0 && batch.format_ver != ENVELOPE_FORMAT_VER {
        return Err(anyhow::anyhow!(
            "incompatible envelope format_ver: batch reports {} but this CLI supports {}",
            batch.format_ver,
            ENVELOPE_FORMAT_VER,
        ));
    }
    let mut stdout = std::io::stdout().lock();
    for env in batch.events.iter() {
        let line = render_envelope_json(env);
        writeln!(&mut stdout, "{line}")?;
    }
    Ok(())
}

fn run_audit_spool(args: DecodeArgs) -> Result<()> {
    let path = args
        .file
        .clone()
        .ok_or_else(|| anyhow::anyhow!("--audit-spool requires a path"))?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("audit spool path has no parent dir"))?;
    let mut stdout = std::io::stdout().lock();
    let _ = recover(parent, |env: ObsEnvelope| {
        let line = render_envelope_json(&env);
        let _ = writeln!(&mut stdout, "{line}");
        Ok(())
    })
    .context("audit spool recovery failed")?;
    Ok(())
}

fn read_source(path: Option<&PathBuf>) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    match path.map(PathBuf::as_path) {
        Some(p) if p == std::path::Path::new("-") => {
            std::io::stdin().read_to_end(&mut buf)?;
        }
        Some(p) => {
            File::open(p)
                .with_context(|| format!("opening {}", p.display()))?
                .read_to_end(&mut buf)?;
        }
        None => {
            std::io::stdin().read_to_end(&mut buf)?;
        }
    }
    Ok(buf)
}

pub fn render_envelope_json(env: &ObsEnvelope) -> String {
    use serde_json::{Map, Value};
    let mut root = Map::new();
    root.insert("ts_ns".into(), Value::from(env.ts_ns));
    root.insert("full_name".into(), Value::from(env.full_name.clone()));
    if env.schema_hash != 0 {
        root.insert("schema_hash".into(), Value::from(env.schema_hash));
    }
    root.insert("service".into(), Value::from(env.service.clone()));
    root.insert("instance".into(), Value::from(env.instance.clone()));
    root.insert("version".into(), Value::from(env.version.clone()));
    if !env.trace_id.is_empty() {
        root.insert("trace_id".into(), Value::from(env.trace_id.clone()));
    }
    if !env.span_id.is_empty() {
        root.insert("span_id".into(), Value::from(env.span_id.clone()));
    }
    let mut labels = Map::new();
    for (k, v) in env.labels.iter() {
        labels.insert(k.clone(), Value::from(v.clone()));
    }
    if !labels.is_empty() {
        root.insert("labels".into(), Value::Object(labels));
    }
    Value::Object(root).to_string()
}

/// Helper used by `obs tail --file` to read NDJSON line-by-line.
#[allow(dead_code)]
pub fn read_ndjson_lines(path: &std::path::Path) -> Result<Vec<String>> {
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            out.push(line);
        }
    }
    Ok(out)
}
