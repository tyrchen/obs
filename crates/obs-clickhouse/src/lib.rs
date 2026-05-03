#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
// `obs-clickhouse` speaks the ClickHouse HTTP protocol directly with a
// raw `TcpStream` rather than pull in a heavy HTTP client. The
// resulting code uses `std::io` and a few index slices that clippy
// flags by default; allow them at the crate root.
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::indexing_slicing
)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! Single-table ClickHouse sink. Spec 22 § 3 + spec 61 § 2.8.
//!
//! The sink batches `ObsEnvelope`s in memory and flushes to a single
//! `obs_events` table per service. Transport is pluggable via
//! [`ClickHouseTransport`]; the in-tree default writes JSONEachRow over
//! HTTP using only `std::net::TcpStream` so we avoid pulling in a heavy
//! ClickHouse-specific client. Production deployments can plug in
//! `clickhouse-rs` or another driver via the same trait.
//!
//! `auto_migrate` is opt-in (dev-only); production CI runs the SQL DDL
//! emitted by [`render_create_table_ddl`] / `obs migrate clickhouse`.

mod ddl;
mod sink;
mod transport;

pub use ddl::render_create_table_ddl;
pub use sink::{ClickHouseSink, ClickHouseSinkBuilder, ClickHouseSinkError};
pub use transport::{
    ClickHouseBatch, ClickHouseTransport, HttpClickHouseTransport, RecordingTransport,
};

/// Internal tier→string used by the JSONEachRow row writer; kept here
/// (not in `sink.rs`) so the format is shared with the recording-
/// transport tests.
pub(crate) fn ddl_tier(env: &obs_proto::obs::v1::ObsEnvelope) -> String {
    match env.tier {
        ::buffa::EnumValue::Known(t) => match t {
            obs_proto::obs::v1::Tier::TIER_LOG => "LOG",
            obs_proto::obs::v1::Tier::TIER_METRIC => "METRIC",
            obs_proto::obs::v1::Tier::TIER_TRACE => "TRACE",
            obs_proto::obs::v1::Tier::TIER_AUDIT => "AUDIT",
            _ => "UNSPECIFIED",
        },
        _ => "UNSPECIFIED",
    }
    .to_string()
}

/// Severity → string.
pub(crate) fn ddl_sev(env: &obs_proto::obs::v1::ObsEnvelope) -> String {
    match env.sev {
        ::buffa::EnumValue::Known(s) => match s {
            obs_proto::obs::v1::Severity::SEVERITY_TRACE => "TRACE",
            obs_proto::obs::v1::Severity::SEVERITY_DEBUG => "DEBUG",
            obs_proto::obs::v1::Severity::SEVERITY_INFO => "INFO",
            obs_proto::obs::v1::Severity::SEVERITY_WARN => "WARN",
            obs_proto::obs::v1::Severity::SEVERITY_ERROR => "ERROR",
            obs_proto::obs::v1::Severity::SEVERITY_FATAL => "FATAL",
            _ => "UNSPECIFIED",
        },
        _ => "UNSPECIFIED",
    }
    .to_string()
}

/// Sampling reason → string.
pub(crate) fn ddl_sampling(env: &obs_proto::obs::v1::ObsEnvelope) -> String {
    match env.sampling_reason {
        ::buffa::EnumValue::Known(r) => match r {
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_HEAD_RATE => "HEAD_RATE",
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_TAIL_ERROR => "TAIL_ERROR",
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_OVERRIDE => "OVERRIDE",
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_FORENSIC => "FORENSIC",
            _ => "UNSPECIFIED",
        },
        _ => "UNSPECIFIED",
    }
    .to_string()
}
