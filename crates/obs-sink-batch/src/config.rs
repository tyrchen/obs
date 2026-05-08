//! Typed configuration for [`BatchingSink`](crate::BatchingSink).

use std::{path::PathBuf, time::Duration};

/// Top-level sink configuration.
///
/// Matches the three-axis model the design doc pins: **triggers**
/// decide when a batch closes, **retry** governs upload failures, and
/// **spool** governs what happens when retries exhaust.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Ingress channel capacity. Overflow is handled inside the worker
    /// (per-partition ring eviction), not on the channel — so
    /// `Sink::deliver` stays non-blocking even when the worker is
    /// catching up.
    pub ingress_capacity: usize,
    /// Batch close triggers.
    pub triggers: BatchTriggers,
    /// Upload retry policy.
    pub retry: RetryPolicy,
    /// Local spool configuration.
    pub spool: SpoolConfig,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            ingress_capacity: 16_384,
            triggers: BatchTriggers::default(),
            retry: RetryPolicy::default(),
            spool: SpoolConfig::default(),
        }
    }
}

/// Batch close triggers. Whichever fires first closes the partition.
#[derive(Debug, Clone, Copy)]
pub struct BatchTriggers {
    /// Maximum envelopes per batch.
    pub max_events: u32,
    /// Maximum total encoded bytes before flushing. Sum of each
    /// envelope's `encoded_len` at admit time.
    pub max_bytes: u64,
    /// Maximum age (time since first envelope entered the partition)
    /// before flushing.
    pub max_age: Duration,
}

impl Default for BatchTriggers {
    fn default() -> Self {
        Self {
            max_events: 1_000,
            max_bytes: 256 * 1024,
            max_age: Duration::from_secs(10),
        }
    }
}

/// Retry policy for transient upload failures.
///
/// Backoff is `initial_backoff * multiplier^(attempt - 1)`, clamped to
/// `max_backoff`. [`JitterMode::FullJitter`] randomises the actual
/// sleep uniformly in `[0, computed_backoff]` — the AWS pattern and a
/// well-studied shape for retry-storm avoidance.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts (including the first). Attempts exhausted without
    /// success fall through to the spool.
    pub max_attempts: u32,
    /// Initial backoff before the first retry.
    pub initial_backoff: Duration,
    /// Multiplier applied each failed attempt.
    pub multiplier: f64,
    /// Upper bound on the computed backoff.
    pub max_backoff: Duration,
    /// Jitter shape.
    pub jitter: JitterMode,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            multiplier: 5.0,
            max_backoff: Duration::from_secs(10),
            jitter: JitterMode::FullJitter,
        }
    }
}

/// Jitter shape applied to retry backoffs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JitterMode {
    /// No jitter — backoff is exactly the computed value.
    None,
    /// Uniform in `[0, computed_backoff]`.
    FullJitter,
    /// Uniform in `[computed_backoff / 2, computed_backoff]` — keeps
    /// retries loosely synchronised, useful when debugging.
    HalfJitter,
}

/// Spool configuration.
#[derive(Debug, Clone)]
pub struct SpoolConfig {
    /// Directory root. Per-backend subtrees live at `{root}/{backend_id}/`.
    pub root: PathBuf,
    /// Total on-disk byte cap. Exceeding the cap evicts oldest-first.
    pub max_bytes: u64,
    /// Background retry cadence — how often the worker re-walks the
    /// spool and re-ships stuck records.
    pub retry_interval: Duration,
    /// After this duration past `first_failed_at`, a stuck record
    /// moves to `{root}/failed/{backend_id}/…` and emits
    /// `ObsBatchSinkEscalatedToFailed`.
    pub escalate_after: Duration,
    /// When `Fsync`, the spool file is `sync_all`'d after each write —
    /// bounds the durability window but halves steady-state throughput
    /// on a loaded node.
    pub fsync_mode: FsyncMode,
}

impl Default for SpoolConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from("./.obs-spool"),
            max_bytes: 1 << 30,
            retry_interval: Duration::from_secs(30),
            escalate_after: Duration::from_secs(60 * 60),
            fsync_mode: FsyncMode::None,
        }
    }
}

/// Durability mode for spool writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// No fsync — relies on the OS page cache. Default; matches
    /// Vector's `disk_v2`.
    None,
    /// `sync_all` after each spool write.
    Fsync,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults_match_design_doc() {
        let cfg = BatchConfig::default();
        assert_eq!(cfg.ingress_capacity, 16_384);
        assert_eq!(cfg.triggers.max_events, 1_000);
        assert_eq!(cfg.triggers.max_bytes, 256 * 1024);
        assert_eq!(cfg.triggers.max_age, Duration::from_secs(10));
        assert_eq!(cfg.retry.max_attempts, 3);
        assert_eq!(cfg.retry.multiplier, 5.0);
        assert_eq!(cfg.spool.max_bytes, 1 << 30);
        assert_eq!(cfg.spool.retry_interval, Duration::from_secs(30));
        assert_eq!(cfg.spool.escalate_after, Duration::from_secs(3_600));
    }
}
