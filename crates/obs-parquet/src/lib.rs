#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
// `obs-parquet` writes Parquet files synchronously from the per-tier
// worker (which is already on a tokio task). The Apache parquet writer
// is sync-only; using `tokio::fs` would require spawn_blocking on every
// row group which doubles the cost. Document and allow the std::fs
// path here, matching how `obs-otel`'s on-disk retry path works.
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::indexing_slicing,
    clippy::collapsible_if
)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! Single-table Parquet analytics sink. Spec 22 § 2 + spec 61 § 2.7.
//!
//! Phase 4A: writes batches as **Parquet files** using a sparse
//! single-table Arrow schema. The envelope columns are first-class;
//! per-event payload bytes are preserved in the `payload_proto`
//! column. Per-event Nested struct columns (one per registered schema)
//! are exposed via [`obs_core::ArrowSchemaModel`] for downstream
//! tools; the in-process Parquet writer stores the bytes in
//! `payload_proto` and lets the consumer decode at query time.
//!
//! Crash semantics per spec 22 § 2.0a: every batch lands in
//! `obs_events-{batch_id}.parquet.tmp`; on clean close the file is
//! atomically renamed (POSIX `rename`). At sink construction the base
//! directory is swept for stale `.tmp` files (deleted; one
//! `ObsAnalyticsPartialDropped` self-event per file).

mod model;
mod partition;
mod sink;
mod writer;

pub use model::{ParquetCompression, ParquetLayout};
pub use sink::{ParquetSink, ParquetSinkBuilder, ParquetSinkError};
