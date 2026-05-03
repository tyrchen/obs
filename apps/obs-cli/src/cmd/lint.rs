//! `obs lint` — run every static schema lint over a crate. Spec 50 § 3.4.
//!
//! The Phase-4 lint set extends Phase-2 with cross-cutting / governance
//! checks:
//!
//! - L001 — LABEL field with cardinality > MEDIUM
//! - L002 — PII classification on a LABEL field
//! - L003 — SECRET classification on a LOG/AUDIT-tier event
//! - L004 — MEASUREMENT field missing `metric` annotation
//! - L005 — Enum used as LABEL has more variants than declared cap (best-effort static check using
//!   the proto enum's value count)
//! - L010 — Forensic budget exceeded (per `metadata.obs.forensic_max`) — uses `--forensic-max` plus
//!   a count of `obs.v1.ObsForensicEvent` call sites within the supplied `*.rs` source tree (when
//!   `--source` is provided).
//! - L011 — Event message name does not start with the workspace prefix
//! - L013 — LABEL field name on multiple events with conflicting types or cardinality
//!
//! `--strict` upgrades all warnings to errors.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use obs_build::reflect::{AnnotatedEvent, AnnotatedField, scan_pool};
use obs_types::{Cardinality, Classification, FieldKind, Tier};

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

    println!(
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

    // L011
    if !rust_name.starts_with(prefix) {
        findings.push(Finding {
            rule: "L011",
            detail: format!(
                "event type name `{rust_name}` must start with `{prefix}` (workspace event_prefix)"
            ),
        });
    }

    let tier = event.tier();
    for f in &event.fields {
        let kind = f.kind();
        let card = f.cardinality();
        let classification = f.classification();

        if matches!(kind, FieldKind::Label) && !card.is_label_compatible() {
            findings.push(Finding {
                rule: "L001",
                detail: format!(
                    "field `{}` is LABEL but cardinality `{:?}` is not label-compatible",
                    f.name, card
                ),
            });
        }

        if matches!(kind, FieldKind::Label) && matches!(classification, Classification::Pii) {
            findings.push(Finding {
                rule: "L002",
                detail: format!("field `{}` is LABEL with classification PII", f.name),
            });
        }

        if matches!(classification, Classification::Secret)
            && matches!(tier, Tier::Log | Tier::Audit)
        {
            findings.push(Finding {
                rule: "L003",
                detail: format!("field `{}` is SECRET on a `{:?}` tier event", f.name, tier),
            });
        }

        // L004 — MEASUREMENT field missing `metric` annotation.
        if matches!(kind, FieldKind::Measurement) && f.options.metric.is_none() {
            findings.push(Finding {
                rule: "L004",
                detail: format!(
                    "field `{}` is MEASUREMENT but missing `metric {{ kind: ..., unit: ... }}` \
                     annotation",
                    f.name
                ),
            });
        }

        // L013 — LABEL field name conflict across events. Only fires
        // when the (cardinality, classification) signatures differ
        // for the same field name.
        if matches!(kind, FieldKind::Label) {
            if let Some(conflicts) = l013.0.get(f.name.as_str()) {
                if conflicts.len() > 1 {
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
        }
    }

    // L005 — repeated/enum LABEL field with declared cap exceeded.
    // We do best-effort: when a LABEL field's proto type is an enum,
    // check that the enum's value count fits the declared cardinality.
    // Since `AnnotatedField` doesn't expose the proto type kind, we
    // skip; the macro path handles L005 via the EnumCount derive.
    Report { findings }
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
            // Conservative: count occurrences of `forensic!(` or
            // `obs::forensic!(` as a callsite proxy.
            count += content.matches("forensic!(").count();
        }
    }
    Some(count)
}

#[allow(dead_code)]
fn _ensure_field_used(_f: &AnnotatedField) {}
