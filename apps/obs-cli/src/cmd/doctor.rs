//! `obs doctor` — diagnose a crate's obs setup.
//!
//! Spec 50 § 3.3.

use std::path::PathBuf;

use anyhow::Result;
use clap::Args;

#[derive(Debug, Args)]
pub struct DoctorArgs {
    /// Crate root to inspect (default `.`).
    #[arg(long, default_value = ".")]
    pub root: PathBuf,
}

pub fn run(args: DoctorArgs) -> Result<()> {
    let mut report = Report::default();
    let cargo_toml = args.root.join("Cargo.toml");
    let manifest = match std::fs::read_to_string(&cargo_toml) {
        Ok(s) => s,
        Err(e) => {
            report.error(format!(
                "could not read {} ({e}); is `--root` correct?",
                cargo_toml.display()
            ));
            report.print();
            std::process::exit(1);
        }
    };

    if manifest.contains("obs-kit =") {
        report.ok("obs-kit in [dependencies]");
    } else {
        report.error("obs-kit missing from [dependencies]");
    }
    if manifest.contains("obs-build =") {
        report.ok("obs-build in [build-dependencies]");
    } else {
        report.info("obs-build absent — proto-first authoring not configured (rust-first OK)");
    }

    let build_rs = args.root.join("build.rs");
    if build_rs.exists() {
        let body = std::fs::read_to_string(&build_rs).unwrap_or_default();
        if body.contains("obs_build::Config") {
            report.ok("build.rs invokes obs_build::Config::compile()");
        } else {
            report.info("build.rs present but does not invoke obs_build::Config");
        }
    } else {
        report.info("no build.rs found (rust-first authoring is fine)");
    }

    let proto_dir = args.root.join("proto");
    if proto_dir.is_dir() {
        let count = walk_proto(&proto_dir);
        report.ok(format!(
            "schema-source = proto; proto/ exists with {count} .proto files"
        ));
    } else {
        report.info("proto/ not present (rust-first authoring)");
    }

    let yaml = args.root.join("obs.yaml");
    if yaml.exists() {
        report.ok("obs.yaml present — config will be loaded at runtime");
    } else {
        report.info("obs.yaml not found; observer will run with defaults");
    }

    report.print();
    if report.errors > 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[derive(Default)]
struct Report {
    lines: Vec<(char, String)>,
    errors: usize,
    oks: usize,
    infos: usize,
}

impl Report {
    fn ok(&mut self, msg: impl Into<String>) {
        self.lines.push(('✔', msg.into()));
        self.oks += 1;
    }

    fn error(&mut self, msg: impl Into<String>) {
        self.lines.push(('✘', msg.into()));
        self.errors += 1;
    }

    fn info(&mut self, msg: impl Into<String>) {
        self.lines.push(('ℹ', msg.into()));
        self.infos += 1;
    }

    fn print(&self) {
        for (c, msg) in &self.lines {
            println!("{c} {msg}");
        }
        println!();
        println!(
            "{} OK · {} ERROR · {} INFO",
            self.oks, self.errors, self.infos
        );
    }
}

fn walk_proto(dir: &std::path::Path) -> usize {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                n += walk_proto(&path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("proto") {
                n += 1;
            }
        }
    }
    n
}
