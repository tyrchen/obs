//! `obs version` — print version + supported envelope formats. Spec 50 § 3.14.

use anyhow::Result;
use obs_proto::ENVELOPE_FORMAT_VER;

#[derive(clap::Args, Debug)]
pub struct VersionArgs {
    /// Print only the schema/envelope-format compatibility line; useful
    /// for healthcheck scripts.
    #[arg(long)]
    pub schema: bool,
}

pub fn run(args: VersionArgs) -> Result<()> {
    if args.schema {
        println!("envelope formats: {ENVELOPE_FORMAT_VER}");
        return Ok(());
    }
    let pkg = env!("CARGO_PKG_VERSION");
    println!("obs {pkg}");
    println!("envelope formats: {ENVELOPE_FORMAT_VER}");
    println!("codegen targets: rust-buffa(0.4)");
    Ok(())
}
