//! `obs-sink-batch` — generic batching sink framework.
//!
//! Ships a [`BatchingSink<B>`](crate::BatchingSink) generic over a pluggable
//! [`BatchBackend`] trait. The framework owns the queue, triggers
//! (count / bytes / age), retry loop with exponential backoff and jitter,
//! per-partition ring-buffer overflow protection, and the envelope-framed
//! disk spool + escalation. Backend implementations own key derivation,
//! body encoding, and the actual I/O (S3 PUT, ClickHouse INSERT, Kafka
//! batch, …).
//!
//! The shape is deliberately aligned with Vector's
//! `(Partitioner, Encoder, RequestBuilder)` decomposition: one small
//! object-safe trait, composition above it. See the Phase 2 design doc
//! in the obs-migration spec tree of the `tok` workspace for the
//! motivating research.
//!
//! # Spool format
//!
//! The spool holds **pre-encoding** envelopes (length-prefixed via
//! [`obs_core::wire::envelope_codec`]) — not post-encoding backend
//! bodies. This costs CPU on recovery but keeps codec evolution
//! tractable: a backend that switches from `zstd(proto)` to Parquet can
//! still replay a spool written under the old shape. Per-partition
//! files live at
//! `{spool_root}/{backend_id}/{partition_hash_hex}/{ts_ms}-{uuid}.spool`.
//!
//! # Self-events
//!
//! The framework emits runtime self-events (see [`self_events`]) through
//! the process-global observer: upload / retry / failed / spooled /
//! recovered / escalated / overflow. Each event carries a
//! `backend: &'static str` label so fan-out consumers can filter by
//! backend identity.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod backend;
mod config;
pub mod self_events;
mod sink;
mod spool;

pub use backend::{BatchBackend, UploadError};
pub use config::{BatchConfig, BatchTriggers, FsyncMode, JitterMode, RetryPolicy, SpoolConfig};
pub use sink::{BatchingSink, WorkerCounters};
pub use spool::SpoolRecord;
