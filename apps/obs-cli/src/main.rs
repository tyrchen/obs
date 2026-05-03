//! `obs` — the developer-facing CLI for the obs SDK.
//!
//! Phase-2 surface (impl-plan task 2.4):
//!
//! - `obs init` — scaffold a new schema crate (proto-first or rust-first)
//! - `obs validate` — validate `.proto` files against `obs/v1/options.proto`
//! - `obs lint` — run every static schema lint (L001/L002/L003/L011)
//! - `obs schema show <full_name>` — print one event's schema
//! - `obs version` — print version + supported envelope formats
//! - `obs completions <shell>` — emit shell completion script
//!
//! Plus the `--schemas`/`--schemas-fds` runtime descriptor-pool path
//! per spec 14 § 10.1 — used by future `decode`/`tail`/`query`
//! subcommands; the foundation lives in `cmd::schema_source` and is
//! consumed by `lint` / `validate` / `schema show` today.

// The CLI is a synchronous process (clap-driven, short-lived); the
// workspace-wide tokio::fs / tokio::process bans don't apply here.
#![allow(
    missing_docs,
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::collapsible_if,
    clippy::if_same_then_else,
    clippy::indexing_slicing
)]

mod cmd;

use anyhow::Result;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "obs",
    version,
    about = "Developer CLI for the obs wide-events SDK.",
    long_about = "Authoring, schema governance, and inspection commands for crates that use the \
                  obs SDK. The CLI manages Rust crates only (spec 12 § 9)."
)]
pub(crate) struct Cli {
    /// Project root for the workspace under inspection. Defaults to
    /// the current directory. Spec 50 § 2 / spec 95 § 3.14 / P2-AK.
    #[arg(long, global = true, value_name = "DIR")]
    pub root: Option<std::path::PathBuf>,

    /// Path to an `obs.yaml` to use for runtime config when commands
    /// need it (e.g. `obs tail` filter directive). Defaults to
    /// `${root}/obs.yaml`.
    #[arg(long, global = true, value_name = "FILE")]
    pub config: Option<std::path::PathBuf>,

    /// Output format. `human` (default) or `json` for pipelines.
    #[arg(long, global = true, value_name = "FORMAT", default_value = "human")]
    pub format: OutputFormat,

    /// Suppress ANSI colour escape sequences regardless of TTY
    /// detection.
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Suppress non-essential output. Errors still print.
    #[arg(long, global = true, short = 'q')]
    pub quiet: bool,

    /// Increase log verbosity. `-v` adds debug; `-vv` adds trace.
    #[arg(short = 'v', long = "verbose", global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    #[command(subcommand)]
    command: Command,
}

/// Output format selector for the `--format` global flag. Spec 50 § 2.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum OutputFormat {
    /// Human-readable terminal output (default).
    Human,
    /// Machine-consumable JSON for pipeline integration.
    Json,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Scaffold a new schema crate (proto-first or rust-first).
    Init(cmd::init::InitArgs),
    /// Validate one or more `.proto` files against `obs/v1/options.proto`.
    Validate(cmd::validate::ValidateArgs),
    /// Run every static schema lint over a crate.
    Lint(cmd::lint::LintArgs),
    /// Run codegen for the proto-first authoring path. Spec 50 § 3.2 /
    /// spec 95 § 3.14 / P2-AK.
    Generate(cmd::generate::GenerateArgs),
    /// Schema inspection commands.
    Schema {
        #[command(subcommand)]
        sub: cmd::schema::Sub,
    },
    /// Decode a binary `ObsBatch` (or AUDIT spool) to NDJSON.
    Decode(cmd::decode::DecodeArgs),
    /// Live-tail an NDJSON / OTLP source.
    Tail(cmd::tail::TailArgs),
    /// Filter + project events from a local NDJSON source.
    Query(cmd::query::QueryArgs),
    /// Diagnose a crate's obs setup.
    Doctor(cmd::doctor::DoctorArgs),
    /// Compare two schema versions and emit a breaking-change report.
    Diff(cmd::diff::DiffArgs),
    /// Roll up forensic-event budget across crates.
    Audit(cmd::audit::AuditArgs),
    /// Emit migration artefacts (DDL or unified Arrow schema).
    Migrate(cmd::migrate::MigrateArgs),
    /// Print version + supported envelope formats.
    Version(cmd::version::VersionArgs),
    /// Emit shell completion script.
    Completions(cmd::completions::CompletionArgs),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init(args) => cmd::init::run(args),
        Command::Validate(args) => cmd::validate::run(args),
        Command::Lint(args) => cmd::lint::run(args),
        Command::Generate(args) => cmd::generate::run(args),
        Command::Schema { sub } => cmd::schema::run(sub),
        Command::Decode(args) => cmd::decode::run(args),
        Command::Tail(args) => cmd::tail::run(args),
        Command::Query(args) => cmd::query::run(args),
        Command::Doctor(args) => cmd::doctor::run(args),
        Command::Diff(args) => cmd::diff::run(args),
        Command::Audit(args) => cmd::audit::run(args),
        Command::Migrate(args) => cmd::migrate::run(args),
        Command::Version(args) => cmd::version::run(args),
        Command::Completions(args) => cmd::completions::run(args),
    }
}
