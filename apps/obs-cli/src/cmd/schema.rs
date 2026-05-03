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
    /// Render output as JSON instead of the human table. Useful for
    /// AI consumers / scripts. Spec 93 P3-9.
    #[arg(long)]
    pub json: bool,
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

    if args.json {
        let payload = serde_json::json!({
            "full_name": event.full_name,
            "tier": format!("{:?}", event.tier()),
            "default_sev": format!("{:?}", event.default_sev()),
            "schema_hash": format!("{:#018x}", event.schema_hash()),
            "fields": event.fields.iter().map(|f| serde_json::json!({
                "name": f.name,
                "number": f.number,
                "kind": format!("{:?}", f.kind()),
                "cardinality": format!("{:?}", f.cardinality()),
                "classification": format!("{:?}", f.classification()),
            })).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    println!("Event:        {}", event.full_name);
    println!("Tier:         {:?}", event.tier());
    println!("Default sev:  {:?}", event.default_sev());
    println!("Schema hash:  {:#018x}", event.schema_hash());
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
