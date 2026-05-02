//! `obs lint` — run every static schema lint over a crate. Spec 50 § 3.4.
//!
//! The Phase-2 lint set is the subset that the proc-macro and obs-build
//! codegen also emit at compile time:
//!
//! - L001 — LABEL field with cardinality > MEDIUM
//! - L002 — PII classification on a LABEL field
//! - L003 — SECRET classification on a LOG/AUDIT-tier event
//! - L011 — event message name does not start with the workspace prefix
//!
//! L004/L005/L007/L008/L010/L012/L013 land in Phase 4 task 4A.5 / 4A.6.

use anyhow::Result;
use obs_build::reflect::{AnnotatedEvent, scan_pool};
use obs_types::{Classification, FieldKind, Tier};

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
    for e in &events {
        if !matches_filter(&e.full_name, &args.filter) {
            continue;
        }
        let report = lint_one(e, &args.source.event_prefix);
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
    println!(
        "{} error(s) · {} warning(s) · {} event(s) scanned",
        errors, warnings, scanned
    );
    if errors > 0 || (args.strict && warnings > 0) {
        std::process::exit(1);
    }
    Ok(())
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

fn lint_one(event: &AnnotatedEvent, prefix: &str) -> Report {
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
    }
    Report { findings }
}
