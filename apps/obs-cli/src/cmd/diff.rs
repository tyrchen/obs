//! `obs diff` — compare two schema directories and emit a
//! breaking-change report. Spec 50 § 3.6.

use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use obs_build::reflect::{AnnotatedEvent, scan_pool};

use super::schema_source::SchemaSourceArgs;

#[derive(clap::Args, Debug)]
pub struct DiffArgs {
    /// Baseline schema directory or `--schemas-fds` file.
    #[arg(value_name = "BASELINE")]
    pub baseline: String,
    /// HEAD schema directory or `--schemas-fds` file.
    #[arg(value_name = "HEAD")]
    pub head: String,
    /// Optional event prefix override; defaults to workspace `Obs`.
    #[arg(long, value_name = "PREFIX", default_value = "Obs")]
    pub event_prefix: String,
}

pub fn run(args: DiffArgs) -> Result<()> {
    let base_pool = SchemaSourceArgs {
        schemas: Some(args.baseline.clone().into()),
        schemas_fds: None,
        event_prefix: args.event_prefix.clone(),
    }
    .build_pool()
    .context("baseline pool")?;
    let head_pool = SchemaSourceArgs {
        schemas: Some(args.head.clone().into()),
        schemas_fds: None,
        event_prefix: args.event_prefix.clone(),
    }
    .build_pool()
    .context("head pool")?;
    let base = scan_pool(&base_pool)?;
    let head = scan_pool(&head_pool)?;

    let base_map: HashMap<&str, &AnnotatedEvent> =
        base.iter().map(|e| (e.full_name.as_str(), e)).collect();
    let head_map: HashMap<&str, &AnnotatedEvent> =
        head.iter().map(|e| (e.full_name.as_str(), e)).collect();

    let mut new = 0usize;
    let mut breaking = 0usize;
    let mut ok = 0usize;

    // New events.
    for (name, h) in &head_map {
        if !base_map.contains_key(name) {
            println!("event {name}    NEW");
            let _ = h;
            new += 1;
        }
    }

    // Modified events.
    for (name, b) in &base_map {
        let Some(h) = head_map.get(name) else {
            println!("event {name}    REMOVED (BREAKING)");
            breaking += 1;
            continue;
        };
        let mut shown_header = false;
        let bf: HashMap<u32, &obs_build::reflect::AnnotatedField> =
            b.fields.iter().map(|f| (f.number, f)).collect();
        let hf: HashMap<u32, &obs_build::reflect::AnnotatedField> =
            h.fields.iter().map(|f| (f.number, f)).collect();
        for (tag, base_field) in &bf {
            match hf.get(tag) {
                None => {
                    if !shown_header {
                        println!("event {name}");
                        shown_header = true;
                    }
                    println!(
                        "  - field {} (#{tag})                            BREAKING (tag reuse \
                         risk)",
                        base_field.name
                    );
                    breaking += 1;
                }
                Some(head_field) => {
                    let bk = base_field.kind();
                    let hk = head_field.kind();
                    let bc = base_field.cardinality();
                    let hc = head_field.cardinality();
                    let bcl = base_field.classification();
                    let hcl = head_field.classification();
                    if bk != hk {
                        if !shown_header {
                            println!("event {name}");
                            shown_header = true;
                        }
                        println!(
                            "  ~ field {} kind {bk:?} → {hk:?}    BREAKING",
                            head_field.name
                        );
                        breaking += 1;
                    } else if bc != hc {
                        if !shown_header {
                            println!("event {name}");
                            shown_header = true;
                        }
                        let is_breaking = is_breaking_card(bc, hc);
                        let tag = if is_breaking { "BREAKING" } else { "OK" };
                        println!(
                            "  ~ field {} cardinality {bc:?} → {hc:?}    {tag}",
                            head_field.name
                        );
                        if is_breaking {
                            breaking += 1;
                        } else {
                            ok += 1;
                        }
                    } else if bcl != hcl {
                        if !shown_header {
                            println!("event {name}");
                            shown_header = true;
                        }
                        let tag = if is_breaking_class(bcl, hcl) {
                            "BREAKING"
                        } else {
                            "OK"
                        };
                        println!(
                            "  ~ field {} classification {bcl:?} → {hcl:?}    {tag}",
                            head_field.name
                        );
                        if tag == "BREAKING" {
                            breaking += 1;
                        } else {
                            ok += 1;
                        }
                    }
                }
            }
        }
        // New fields.
        for (tag, head_field) in &hf {
            if !bf.contains_key(tag) {
                if !shown_header {
                    println!("event {name}");
                    shown_header = true;
                }
                println!("  + field {} (#{tag})    OK", head_field.name);
                ok += 1;
            }
        }
    }

    println!();
    println!("{new} NEW · {breaking} BREAKING · {ok} OK");
    if breaking > 0 {
        std::process::exit(2);
    }
    Ok(())
}

fn is_breaking_card(b: obs_types::Cardinality, h: obs_types::Cardinality) -> bool {
    use obs_types::Cardinality::*;
    matches!(
        (b, h),
        (Low, Medium | High | Unbounded) | (Medium, High | Unbounded) | (High, Unbounded)
    )
}

fn is_breaking_class(b: obs_types::Classification, h: obs_types::Classification) -> bool {
    // Demoting PII → Internal or removing Secret protection is
    // breaking because it changes the redaction contract.
    use obs_types::Classification::*;
    matches!((b, h), (Pii, Internal | Unspecified) | (Secret, _))
}

#[allow(dead_code)]
fn _ensure_anyhow_used() -> Result<()> {
    Err(anyhow!("placeholder"))
}
