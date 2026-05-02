//! `obs version` — print version + supported envelope formats. Spec 50 § 3.14.

use anyhow::Result;

#[derive(clap::Args, Debug)]
pub struct VersionArgs {
    /// Print only the schema/envelope-format compatibility line; useful
    /// for healthcheck scripts.
    #[arg(long)]
    pub schema: bool,
}

pub fn run(args: VersionArgs) -> Result<()> {
    if args.schema {
        println!("envelope formats: 1");
        return Ok(());
    }
    let pkg = env!("CARGO_PKG_VERSION");
    println!("obs {pkg}");
    println!("envelope formats: 1");
    println!("codegen targets: rust-buffa(0.4)");
    Ok(())
}
