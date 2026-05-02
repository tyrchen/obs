//! `obs completions <shell>` — emit shell completion script. Spec 50 § 3.15.

use anyhow::Result;
use clap::CommandFactory;
use clap_complete::{Shell, generate};

#[derive(clap::Args, Debug)]
pub struct CompletionArgs {
    /// Target shell (bash, zsh, fish, elvish, powershell).
    pub shell: Shell,
}

pub fn run(args: CompletionArgs) -> Result<()> {
    let mut cmd = crate::Cli::command();
    let bin = cmd.get_name().to_string();
    generate(args.shell, &mut cmd, bin, &mut std::io::stdout());
    Ok(())
}
