//! `obs migrate clickhouse` / `obs migrate parquet` — emit DDL / Arrow
//! schema JSON for the unified `obs_events` table. Spec 50 §§ 3.12–3.13.

use std::path::PathBuf;

use anyhow::{Context, Result};
use obs_build::reflect::scan_pool;
use obs_clickhouse::render_create_table_ddl;
use obs_core::{ArrowField, ArrowLeafType, ArrowSchemaModel};
use obs_types::FieldKind;

use super::schema_source::SchemaSourceArgs;

#[derive(clap::Args, Debug)]
pub struct MigrateArgs {
    #[command(subcommand)]
    pub backend: Backend,
}

#[derive(clap::Subcommand, Debug)]
pub enum Backend {
    /// Emit `CREATE TABLE` DDL for ClickHouse.
    Clickhouse(ClickhouseArgs),
    /// Emit unified Arrow schema as JSON for downstream Parquet writers.
    Parquet(ParquetArgs),
}

#[derive(clap::Args, Debug)]
pub struct ClickhouseArgs {
    #[command(flatten)]
    pub source: SchemaSourceArgs,
    /// Output file (default stdout).
    #[arg(long, value_name = "FILE")]
    pub out: Option<PathBuf>,
    /// Override the table name. Default `obs_events`.
    #[arg(long, default_value = "obs_events")]
    pub table: String,
}

#[derive(clap::Args, Debug)]
pub struct ParquetArgs {
    #[command(flatten)]
    pub source: SchemaSourceArgs,
    /// Output file (default stdout).
    #[arg(long, value_name = "FILE")]
    pub out: Option<PathBuf>,
}

pub fn run(args: MigrateArgs) -> Result<()> {
    match args.backend {
        Backend::Clickhouse(a) => run_clickhouse(a),
        Backend::Parquet(a) => run_parquet(a),
    }
}

fn run_clickhouse(args: ClickhouseArgs) -> Result<()> {
    let pool = args.source.build_pool().context("schema source")?;
    let events = scan_pool(&pool)?;
    let model = build_arrow_model(&events);
    let ddl = render_create_table_ddl(&model, &args.table);
    write_out(&args.out, ddl.as_bytes())
}

fn run_parquet(args: ParquetArgs) -> Result<()> {
    let pool = args.source.build_pool().context("schema source")?;
    let events = scan_pool(&pool)?;
    let model = build_arrow_model(&events);
    let json = model.to_json().context("render arrow json")?;
    write_out(&args.out, json.as_bytes())
}

fn build_arrow_model(events: &[obs_build::reflect::AnnotatedEvent]) -> ArrowSchemaModel {
    let mut entries = Vec::with_capacity(events.len());
    for e in events {
        let payload_column = format!("payload_{}", e.full_name.replace('.', "_").to_lowercase());
        let mut fields = Vec::with_capacity(e.fields.len());
        for f in &e.fields {
            let kind = f.kind();
            let card = f.cardinality();
            let cls = f.classification();
            let ty = leaf_for(kind, &f.name, card);
            fields.push(ArrowField {
                name: f.name.clone(),
                tag: f.number,
                ty,
                kind,
                cardinality: card,
                classification: cls,
            });
        }
        entries.push(obs_core::ArrowEventSchema {
            full_name: e.full_name.clone(),
            payload_column,
            fields,
            schema_hash: 0, // hash is not available without codegen path here
        });
    }
    entries.sort_by(|a, b| a.full_name.cmp(&b.full_name));
    ArrowSchemaModel { events: entries }
}

fn leaf_for(kind: FieldKind, name: &str, card: obs_types::Cardinality) -> ArrowLeafType {
    match kind {
        FieldKind::Label => match card {
            obs_types::Cardinality::Low | obs_types::Cardinality::Medium => ArrowLeafType::DictUtf8,
            _ => ArrowLeafType::Utf8,
        },
        FieldKind::Measurement => {
            if name.ends_with("_ms")
                || name.ends_with("_ns")
                || name.ends_with("_us")
                || name.ends_with("_bytes")
                || name.ends_with("_size")
                || name.ends_with("_count")
                || name.ends_with("_total")
            {
                ArrowLeafType::UInt64
            } else if name.ends_with("_ratio") || name.ends_with("_rate") {
                ArrowLeafType::Float64
            } else {
                ArrowLeafType::Float64
            }
        }
        FieldKind::TimestampNs => ArrowLeafType::TimestampNs,
        FieldKind::DurationNs => ArrowLeafType::UInt64,
        FieldKind::TraceId | FieldKind::SpanId | FieldKind::ParentSpanId => ArrowLeafType::Utf8,
        FieldKind::Attribute | FieldKind::Forensic => ArrowLeafType::Utf8,
        _ => ArrowLeafType::Utf8,
    }
}

fn write_out(path: &Option<PathBuf>, bytes: &[u8]) -> Result<()> {
    match path {
        Some(p) => {
            std::fs::write(p, bytes).with_context(|| format!("writing {}", p.display()))?;
            Ok(())
        }
        None => {
            use std::io::Write;
            std::io::stdout().write_all(bytes)?;
            Ok(())
        }
    }
}
