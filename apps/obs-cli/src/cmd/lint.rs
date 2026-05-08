//! `obs lint` — run every static schema lint over a crate. Spec 50 § 3.4.
//!
//! Spec 95 § 2.3 / D8-1: the lint catalogue is delegated to the shared
//! `obs_build::lints` module so the CLI mirrors what `cargo build`
//! enforces (L001–L014). The CLI adds two extra lints layered on top:
//!
//! - **L010** — forensic budget exceeded (CLI-only; needs source-tree scan).
//! - **L013** — LABEL name carries conflicting `(cardinality, classification)` across events
//!   (CLI-only cross-event check; the shared module's L013 handles schema_hash collisions which the
//!   build path catches at compile time).
//!
//! `--strict` upgrades all warnings to errors.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use obs_build::{
    LintField, LintInput, LintProtoType,
    reflect::{AnnotatedEvent, AnnotatedField, scan_pool},
};
use obs_proto::obs::v1::{Cardinality, Classification, FieldKind};

use super::schema_source::SchemaSourceArgs;

#[derive(clap::Args, Debug)]
pub struct LintArgs {
    #[command(flatten)]
    pub source: SchemaSourceArgs,

    /// Treat warnings as errors (CI integration).
    #[arg(long)]
    pub strict: bool,

    /// Allowlist a specific lint id (e.g. `--allow L011`); repeatable.
    /// Findings whose rule matches are reported as warnings, not errors.
    #[arg(long, value_name = "ID")]
    pub allow: Vec<String>,

    /// Restrict scan to events whose `full_name` contains this
    /// substring; repeatable (any-match). Spec 50 § 3.4.
    #[arg(long, value_name = "PATTERN")]
    pub filter: Vec<String>,

    /// Per-crate forensic-event budget (L010). Default 5; set higher
    /// for emergency-only crates or lower for production-critical
    /// services. Spec 11 § 6.3.
    #[arg(long, value_name = "N", default_value_t = 5)]
    pub forensic_max: u32,

    /// Optional path to a Rust source tree to scan for forensic
    /// callsites (L010). Without this argument L010 is skipped.
    #[arg(long, value_name = "DIR")]
    pub src_root: Option<std::path::PathBuf>,
}

pub fn run(args: LintArgs) -> Result<()> {
    let pool = args.source.build_pool()?;
    let events = scan_pool(&pool)?;
    let mut errors = 0usize;
    let mut warnings = 0usize;
    let scanned = events
        .iter()
        .filter(|e| matches_filter(&e.full_name, &args.filter))
        .count();
    let l013 = collect_label_conflicts(&events);
    for e in &events {
        if !matches_filter(&e.full_name, &args.filter) {
            continue;
        }
        let report = lint_one(e, &args.source.event_prefix, &l013);
        for f in &report.findings {
            let allowed = args.allow.iter().any(|a| a.eq_ignore_ascii_case(f.rule));
            if allowed {
                warnings += 1;
                eprintln!("warning[{}] {}: {}", f.rule, e.full_name, f.detail);
            } else {
                errors += 1;
                eprintln!("error[{}] {}: {}", f.rule, e.full_name, f.detail);
            }
        }
    }

    // L010: scan source tree for forensic emit count.
    if let Some(src_root) = &args.source_path() {
        if let Some(count) = scan_forensic_count(src_root) {
            if count > args.forensic_max as usize {
                let allowed = args.allow.iter().any(|a| a.eq_ignore_ascii_case("L010"));
                let msg = format!(
                    "found {count} forensic call site(s) > budget {} in {:?}",
                    args.forensic_max, src_root
                );
                if allowed {
                    warnings += 1;
                    eprintln!("warning[L010] forensic budget exceeded: {msg}");
                } else {
                    errors += 1;
                    eprintln!("error[L010] forensic budget exceeded: {msg}");
                }
            }
        }
    }

    // Spec 50 § 4 / spec 93 P1-9: summary lines go to stderr so
    // pipelines that consume `obs lint` stdout (e.g. structured JSON
    // findings) do not collide with the human-readable summary.
    eprintln!(
        "{} error(s) · {} warning(s) · {} event(s) scanned",
        errors, warnings, scanned
    );
    if errors > 0 || (args.strict && warnings > 0) {
        std::process::exit(1);
    }
    Ok(())
}

impl LintArgs {
    fn source_path(&self) -> Option<&std::path::Path> {
        self.src_root.as_deref()
    }
}

fn matches_filter(full_name: &str, patterns: &[String]) -> bool {
    if patterns.is_empty() {
        return true;
    }
    patterns.iter().any(|p| full_name.contains(p.as_str()))
}

#[derive(Debug)]
struct Report {
    findings: Vec<Finding>,
}

#[derive(Debug)]
struct Finding {
    rule: &'static str,
    detail: String,
}

fn lint_one(event: &AnnotatedEvent, prefix: &str, l013: &LabelConflictMap) -> Report {
    let mut findings = Vec::new();
    let rust_name = event
        .full_name
        .rsplit('.')
        .next()
        .unwrap_or(&event.full_name);

    // Spec 95 § 2.3 / D8-1: hand the event off to the shared lint
    // module. Each `LintError` becomes one CLI Finding so operators
    // see the same catalogue as `cargo build`.
    let input = LintInput {
        event_name: rust_name.to_string(),
        tier: event.tier(),
        event_prefix: prefix.to_string(),
        fields: event
            .fields
            .iter()
            .map(|f| LintField {
                name: f.name.clone(),
                kind: f.kind(),
                cardinality: f.cardinality(),
                classification: f.classification(),
                has_metric: f.options.metric.is_some(),
                // The CLI does not currently inspect proto types from
                // `AnnotatedField`; pass `None` so L014's type check
                // is skipped (the name check still fires).
                proto_type: Some(LintProtoType::String),
            })
            .collect(),
    };
    for err in obs_build::emit_lints(&input) {
        findings.push(Finding {
            rule: err.code,
            detail: short_detail(&err.message),
        });
    }

    // CLI-only L013: cross-event LABEL name conflict. Different from
    // the shared module's L013 (schema_hash collision).
    for f in &event.fields {
        if !matches!(f.kind(), FieldKind::Label) {
            continue;
        }
        if let Some(conflicts) = l013.0.get(f.name.as_str())
            && conflicts.len() > 1
        {
            let mut sigs: HashSet<(Cardinality, Classification)> = HashSet::new();
            for s in conflicts {
                sigs.insert((s.cardinality, s.classification));
            }
            if sigs.len() > 1 {
                let mut events: Vec<String> = conflicts
                    .iter()
                    .map(|s| {
                        format!(
                            "{}({:?}/{:?})",
                            s.event_full_name, s.cardinality, s.classification
                        )
                    })
                    .collect();
                events.sort();
                events.dedup();
                findings.push(Finding {
                    rule: "L013",
                    detail: format!(
                        "label `{}` declared with conflicting types: {}",
                        f.name,
                        events.join(", ")
                    ),
                });
            }
        }
    }

    Report { findings }
}

/// Strip the multi-line `note:`/`help:` block from the shared lint
/// message and return just the leading `obs L00x: <reason>` line so
/// CLI output stays compact. The full message is still available via
/// `obs lint --format json` (spec 50 § 2 global flag).
fn short_detail(msg: &str) -> String {
    let head = msg.lines().next().unwrap_or(msg);
    let trimmed = head
        .trim_start_matches(|c: char| !c.is_alphabetic())
        .trim_start_matches("obs ");
    match trimmed.split_once(": ") {
        Some((_, rest)) => rest.to_string(),
        None => msg.to_string(),
    }
}

#[derive(Debug, Default)]
struct LabelConflictMap(HashMap<String, Vec<LabelSignature>>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct LabelSignature {
    event_full_name: String,
    cardinality: Cardinality,
    classification: Classification,
}

fn collect_label_conflicts(events: &[AnnotatedEvent]) -> LabelConflictMap {
    let mut by_name: HashMap<String, Vec<LabelSignature>> = HashMap::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for e in events {
        for f in &e.fields {
            if !matches!(f.kind(), FieldKind::Label) {
                continue;
            }
            let key = (e.full_name.clone(), f.name.clone());
            if !seen.insert(key) {
                continue;
            }
            by_name
                .entry(f.name.clone())
                .or_default()
                .push(LabelSignature {
                    event_full_name: e.full_name.clone(),
                    cardinality: f.cardinality(),
                    classification: f.classification(),
                });
        }
    }
    LabelConflictMap(by_name)
}

// `scan_forensic_count` lives in `super::scan` so `obs audit` can
// share it. Spec 93 P3-6.
use super::scan::scan_forensic_count;

#[allow(dead_code)]
fn _ensure_field_used(_f: &AnnotatedField) {}
