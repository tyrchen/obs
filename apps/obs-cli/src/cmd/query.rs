//! `obs query` — minimal query CLI over local NDJSON.
//!
//! Spec 50 § 3.9. Phase-3 supports `--from path/file.ndjson` plus a
//! subset of filters (`--event`, `--severity`, `--label k=v`,
//! `--since`, `--limit`); ClickHouse / S3 sources land in Phase 4A.

use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::Args;
use serde_json::Value;

#[derive(Debug, Args)]
pub struct QueryArgs {
    /// Source: a local NDJSON file (other URI schemes deferred).
    #[arg(long)]
    pub from: PathBuf,
    /// Filter on `full_name`.
    #[arg(long, action = clap::ArgAction::Append)]
    pub event: Vec<String>,
    /// Filter on minimum severity (`info` / `warn` / `error` / `fatal`).
    #[arg(long)]
    pub severity: Option<String>,
    /// Repeat `--label key=value` to AND filter on labels.
    #[arg(long, action = clap::ArgAction::Append)]
    pub label: Vec<String>,
    /// `--since 1h` / `30m` / RFC3339.
    #[arg(long)]
    pub since: Option<String>,
    /// Limit rows.
    #[arg(long)]
    pub limit: Option<usize>,
}

pub fn run(args: QueryArgs) -> Result<()> {
    let path = args.from.clone();
    let f = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
    let reader = BufReader::new(f);

    let since_ns = args.since.as_deref().and_then(parse_since).unwrap_or(0);
    let label_filters: Vec<(String, String)> = args
        .label
        .iter()
        .filter_map(|s| {
            s.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect();
    let mut count = 0usize;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !args.event.is_empty()
            && !args
                .event
                .iter()
                .any(|e| v.get("full_name").and_then(|x| x.as_str()) == Some(e.as_str()))
        {
            continue;
        }
        if let Some(min) = args.severity.as_deref() {
            let sev = v.get("sev").and_then(|x| x.as_str()).unwrap_or("");
            if !severity_at_least(sev, min) {
                continue;
            }
        }
        if !label_filters.is_empty() {
            let labels = v.get("labels").and_then(|x| x.as_object());
            let ok = label_filters.iter().all(|(k, expected)| {
                labels
                    .map(|o| o.get(k).and_then(|x| x.as_str()) == Some(expected.as_str()))
                    .unwrap_or(false)
            });
            if !ok {
                continue;
            }
        }
        let ts_ns = v.get("ts_ns").and_then(|x| x.as_u64()).unwrap_or(0);
        if ts_ns < since_ns {
            continue;
        }
        writeln!(out, "{line}")?;
        count += 1;
        if let Some(limit) = args.limit
            && count >= limit
        {
            break;
        }
    }
    Ok(())
}

fn parse_since(s: &str) -> Option<u64> {
    if let Ok(ts) = humantime_strict(s) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        return Some(now.saturating_sub(ts.as_nanos() as u64));
    }
    None
}

fn humantime_strict(s: &str) -> Result<Duration, String> {
    if let Some(rest) = s.strip_suffix("ms") {
        return rest
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| e.to_string());
    }
    if let Some(rest) = s.strip_suffix('s') {
        return rest
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| e.to_string());
    }
    if let Some(rest) = s.strip_suffix('m') {
        return rest
            .parse::<u64>()
            .map(|n| Duration::from_secs(n * 60))
            .map_err(|e| e.to_string());
    }
    if let Some(rest) = s.strip_suffix('h') {
        return rest
            .parse::<u64>()
            .map(|n| Duration::from_secs(n * 3600))
            .map_err(|e| e.to_string());
    }
    if let Some(rest) = s.strip_suffix('d') {
        return rest
            .parse::<u64>()
            .map(|n| Duration::from_secs(n * 86_400))
            .map_err(|e| e.to_string());
    }
    Err(format!("unparsable duration `{s}`"))
}

fn severity_at_least(actual: &str, min: &str) -> bool {
    severity_rank(actual) >= severity_rank(min)
}

fn severity_rank(s: &str) -> u32 {
    match s.to_ascii_uppercase().as_str() {
        "TRACE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARN" => 13,
        "ERROR" => 17,
        "FATAL" => 21,
        _ => 0,
    }
}
