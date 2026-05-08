//! `obs tail` — pretty-print envelopes from a sink (file / stdin /
//! OTLP). Spec 50 § 3.10.
//!
//! Phase-3 implements `--file` and `--stdin` (NDJSON sources). The
//! `--otlp` server-side receiver is deferred behind an `obs-otel`
//! integration — `obs tail --otlp 0.0.0.0:4317` is documented in spec
//! 50 § 3.10 but lands when the OTLP exporter trio gains a server
//! complement.

use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    thread,
    time::Duration,
};

use anyhow::Result;
use clap::Args;

#[derive(Debug, Args)]
pub struct TailArgs {
    /// NDJSON file to follow (like `tail -f`).
    #[arg(long, conflicts_with = "stdin", conflicts_with = "otlp")]
    pub file: Option<PathBuf>,
    /// Read NDJSON from stdin.
    #[arg(long, conflicts_with = "file", conflicts_with = "otlp")]
    pub stdin: bool,
    /// (deferred) Spin up an OTLP receiver on the given socket.
    #[arg(long, conflicts_with = "file", conflicts_with = "stdin")]
    pub otlp: Option<String>,
    /// Render output as NDJSON (default: pretty single-line).
    #[arg(long)]
    pub format_ndjson: bool,

    /// `obs::Filter`-shaped directive for client-side filtering. Same
    /// grammar as `OBS_FILTER` / `RUST_LOG` (spec 13 § 7). Lines that
    /// don't match the directive are skipped before render. Spec 50 §
    /// 3.10 / spec 95 § 3.14.
    #[arg(long, value_name = "DIRECTIVE")]
    pub filter: Option<String>,
}

pub fn run(args: TailArgs) -> Result<()> {
    if let Some(addr) = &args.otlp {
        anyhow::bail!(
            "--otlp {addr} receiver is deferred to a future milestone (see spec 50 § 3.10); use \
             --file or --stdin for now"
        );
    }
    let filter = match &args.filter {
        Some(spec) => Some(
            obs_core::Filter::parse(spec)
                .map_err(|e| anyhow::anyhow!("invalid --filter directive `{spec}`: {e}"))?,
        ),
        None => None,
    };
    if args.stdin {
        let stdin = std::io::stdin();
        let stdout = std::io::stdout();
        for line in stdin.lock().lines() {
            let line = line?;
            if !filter_allows(filter.as_ref(), &line) {
                continue;
            }
            render_line(&mut stdout.lock(), &line, args.format_ndjson)?;
        }
        return Ok(());
    }
    if let Some(path) = &args.file {
        return tail_file(path, args.format_ndjson, filter.as_ref());
    }
    anyhow::bail!("--file or --stdin is required (see spec 50 § 3.10)");
}

/// Best-effort client-side filter. NDJSON rows that fail to parse
/// pass through (the user's pipeline may be downstream of a non-obs
/// source).
fn filter_allows(filter: Option<&obs_core::Filter>, line: &str) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(line) else {
        return true;
    };
    let Some(full_name) = parsed.get("full_name").and_then(|v| v.as_str()) else {
        return true;
    };
    let mut labels = std::collections::HashMap::new();
    if let Some(map) = parsed.get("labels").and_then(|v| v.as_object()) {
        for (k, v) in map {
            if let Some(vs) = v.as_str() {
                labels.insert(k.clone(), vs.to_string());
            }
        }
    }
    let env = obs_proto::obs::v1::ObsEnvelope {
        full_name: full_name.to_string(),
        labels,
        ..Default::default()
    };
    let sev_str = parsed.get("sev").and_then(|v| v.as_str()).unwrap_or("INFO");
    let sev = match sev_str {
        "TRACE" => obs_proto::obs::v1::Severity::Trace,
        "DEBUG" => obs_proto::obs::v1::Severity::Debug,
        "WARN" => obs_proto::obs::v1::Severity::Warn,
        "ERROR" => obs_proto::obs::v1::Severity::Error,
        "FATAL" => obs_proto::obs::v1::Severity::Fatal,
        _ => obs_proto::obs::v1::Severity::Info,
    };
    filter.event_allowed(&env, sev)
}

fn tail_file(path: &PathBuf, ndjson: bool, filter: Option<&obs_core::Filter>) -> Result<()> {
    let mut last_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    // Print whatever is already there first.
    let stdout = std::io::stdout();
    {
        let mut out = stdout.lock();
        for line in (&mut reader).lines() {
            let line = line?;
            if !filter_allows(filter, &line) {
                continue;
            }
            render_line(&mut out, &line, ndjson)?;
        }
    }
    loop {
        thread::sleep(Duration::from_millis(250));
        let size = std::fs::metadata(path)
            .map(|m| m.len())
            .unwrap_or(last_size);
        if size > last_size {
            // Re-open and seek to last_size.
            let f = std::fs::File::open(path)?;
            use std::io::Seek;
            let mut f = f;
            let _ = f.seek(std::io::SeekFrom::Start(last_size));
            let mut reader = BufReader::new(f);
            let mut out = stdout.lock();
            for line in (&mut reader).lines() {
                let line = line?;
                if !filter_allows(filter, &line) {
                    continue;
                }
                render_line(&mut out, &line, ndjson)?;
            }
            last_size = size;
        } else if size < last_size {
            // File rotated — re-open from the start.
            last_size = 0;
        }
    }
}

fn render_line<W: Write>(w: &mut W, line: &str, ndjson: bool) -> Result<()> {
    if ndjson {
        writeln!(w, "{line}")?;
        return Ok(());
    }
    let parsed: serde_json::Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => {
            writeln!(w, "{line}")?;
            return Ok(());
        }
    };
    let ts = parsed.get("ts_ns").and_then(|v| v.as_u64()).unwrap_or(0);
    let secs = ts / 1_000_000_000;
    let ns = ts % 1_000_000_000;
    let name = parsed
        .get("full_name")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let labels = parsed
        .get("labels")
        .and_then(|v| v.as_object())
        .map(|o| {
            let mut s = String::new();
            for (k, v) in o {
                s.push(' ');
                s.push_str(k);
                s.push('=');
                s.push_str(&v.to_string());
            }
            s
        })
        .unwrap_or_default();
    writeln!(w, "{secs}.{ns:09} {name}{labels}")?;
    Ok(())
}
