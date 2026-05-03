//! `obs audit` — workspace governance summary. Spec 50 § 3.7.

use std::path::PathBuf;

use anyhow::{Context, Result};

use super::schema_source::SchemaSourceArgs;

#[derive(clap::Args, Debug)]
pub struct AuditArgs {
    #[command(flatten)]
    pub source: SchemaSourceArgs,

    /// Path to a Rust workspace root for forensic-callsite scan.
    #[arg(long, value_name = "DIR", default_value = ".")]
    pub root: PathBuf,

    /// Per-crate forensic-event budget. Default 5.
    #[arg(long, value_name = "N", default_value_t = 5)]
    pub forensic_max: u32,
}

pub fn run(args: AuditArgs) -> Result<()> {
    let pool = args.source.build_pool().context("schema source")?;
    let events = obs_build::reflect::scan_pool(&pool)?;
    let crates = enumerate_crates(&args.root)?;
    println!(
        "Workspace: {} events across {} crates",
        events.len(),
        crates.len()
    );
    println!();
    println!("Forensic events used:");
    let mut over_budget = false;
    for c in &crates {
        let count = scan_forensic_count(&c.src_dir).unwrap_or(0);
        let label = if count > args.forensic_max as usize {
            over_budget = true;
            format!("{count}/{} (BUDGET EXCEEDED)", args.forensic_max)
        } else {
            format!("{count}/{}", args.forensic_max)
        };
        println!("  {:<32} {label}", c.name);
    }
    println!();
    println!("Audit-tier coverage:");
    let audit_events = events
        .iter()
        .filter(|e| matches!(e.tier(), obs_types::Tier::Audit))
        .count();
    println!("  {} AUDIT-tier events declared", audit_events);
    if over_budget {
        std::process::exit(1);
    }
    Ok(())
}

#[derive(Debug)]
struct CrateEntry {
    name: String,
    src_dir: PathBuf,
}

fn enumerate_crates(root: &std::path::Path) -> Result<Vec<CrateEntry>> {
    let crates_dir = root.join("crates");
    let mut out = Vec::new();
    if !crates_dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(&crates_dir)? {
        let entry = entry?;
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        let name = p
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let src = p.join("src");
        if src.exists() {
            out.push(CrateEntry { name, src_dir: src });
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

fn scan_forensic_count(root: &std::path::Path) -> Option<usize> {
    let mut count = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).ok()?;
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let Ok(content) = std::fs::read_to_string(&p) else {
                continue;
            };
            count += content.matches("forensic!(").count();
        }
    }
    Some(count)
}
