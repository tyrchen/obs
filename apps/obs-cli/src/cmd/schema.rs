//! `obs schema show <full_name>` — print everything we know about an
//! event type. Spec 50 § 3.11.

use anyhow::{Result, anyhow};
use obs_build::reflect::scan_pool;

use super::schema_source::SchemaSourceArgs;

#[derive(clap::Subcommand, Debug)]
pub enum Sub {
    /// Print everything known about one event type.
    Show(ShowArgs),
}

#[derive(clap::Args, Debug)]
pub struct ShowArgs {
    /// Fully qualified event name (e.g. `myapp.v1.ObsRequestCompleted`).
    pub full_name: String,
    #[command(flatten)]
    pub source: SchemaSourceArgs,
}

pub fn run(sub: Sub) -> Result<()> {
    match sub {
        Sub::Show(args) => show(args),
    }
}

fn show(args: ShowArgs) -> Result<()> {
    let pool = args.source.build_pool()?;
    let events = scan_pool(&pool)?;
    let event = events
        .iter()
        .find(|e| e.full_name == args.full_name)
        .ok_or_else(|| {
            anyhow!(
                "event `{}` not found in supplied schemas. Found:\n  {}",
                args.full_name,
                events
                    .iter()
                    .map(|e| e.full_name.as_str())
                    .collect::<Vec<_>>()
                    .join("\n  ")
            )
        })?;

    println!("Event:        {}", event.full_name);
    println!("Tier:         {:?}", event.tier());
    println!("Default sev:  {:?}", event.default_sev());
    println!();
    println!("Fields:");
    println!(
        "  {:<3} {:<14} {:<14} {:<7} {:<8}",
        "#", "NAME", "KIND", "CARD", "CLASS"
    );
    for f in &event.fields {
        println!(
            "  {:<3} {:<14} {:?} {:?} {:?}",
            f.number,
            f.name,
            f.kind(),
            f.cardinality(),
            f.classification()
        );
    }
    Ok(())
}
