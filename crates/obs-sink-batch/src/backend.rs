//! [`BatchBackend`] trait — the pluggable destination surface.
//!
//! Backends implement three hooks:
//!
//! - [`partition_key`](BatchBackend::partition_key) — derive the grouping key from an envelope.
//! - [`encode_batch`](BatchBackend::encode_batch) — turn a homogeneous batch into a body.
//! - [`upload`](BatchBackend::upload) — ship the body. Returns a [`RetryDecision`] so the framework
//!   knows whether to retry-with-backoff or spool immediately.
//!
//! The framework runs all three on the backend's per-sink worker task,
//! so implementations can hold `&self` state (S3 client, KMS handle, …)
//! without synchronisation. `encode_batch` is intentionally synchronous:
//! encode is CPU-bound (zstd, Parquet, Arrow) and keeping the trait
//! sync-by-default avoids forcing every consumer through an async
//! runtime step that buys nothing. Async work belongs in `upload`.

use std::{fmt, future::Future};

use obs_proto::obs::v1::ObsEnvelope;

/// A pluggable destination for batched obs envelopes.
///
/// See the module docs for the contract. The trait uses native async
/// functions (`upload` returns `impl Future`) — backend types keep full
/// control over their future type and don't pay the dyn-dispatch cost
/// the `async-trait` crate would add.
pub trait BatchBackend: Send + Sync + 'static {
    /// Opaque partition key used to group envelopes inside the worker.
    ///
    /// Must be `Hash + Eq + Clone + Send + Sync + 'static` so the
    /// worker can shard pending batches by key in an in-memory map.
    type PartitionKey: std::hash::Hash + Eq + Clone + Send + Sync + fmt::Debug + 'static;

    /// Encoded body ready for upload. The backend chooses the shape:
    /// `Bytes` for byte-stream sinks (S3, GCS), a typed struct for RPC
    /// sinks (ClickHouse `INSERT`, Kafka batch). Constrained to
    /// `Send + 'static` so the worker can hand it to the retry loop.
    type Body: Send + 'static;

    /// Error type surfaced from [`Self::upload`] and
    /// [`Self::encode_batch`]. `Display` + `Debug` so self-events can
    /// render it in label values.
    type Error: fmt::Debug + fmt::Display + Send + 'static;

    /// Short, stable backend identifier stamped into self-event
    /// labels. Required so fan-out consumers can tell the tok S3
    /// backend apart from a ClickHouse backend sharing the same
    /// process. No default — forcing the backend author to pick a name
    /// keeps the label space readable.
    fn backend_id(&self) -> &'static str;

    /// Derive the partition key from an envelope. Called on the worker
    /// task for every admitted envelope. `None` means "route to the
    /// default catch-all partition" — a sensible shape for AUDIT-tier
    /// backends where no partition axis applies.
    fn partition_key(&self, env: &ObsEnvelope) -> Option<Self::PartitionKey>;

    /// Encode a batch of envelopes (sharing one partition key) into
    /// the backend's body type. Called on the worker task when a
    /// trigger fires. Backend owns compression, framing, schema
    /// projection, KMS wrapping, etc.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` when encoding fails. Encode failures are
    /// treated as fatal by the framework — the batch is spooled and
    /// an `ObsBatchSinkFailed` self-event is emitted. The backend
    /// should not retry internally.
    fn encode_batch(
        &self,
        key: &Self::PartitionKey,
        envs: &[ObsEnvelope],
    ) -> Result<Self::Body, Self::Error>;

    /// Ship one encoded batch.
    ///
    /// Returns `Ok(())` on success or an [`UploadError`] describing
    /// whether the failure is retryable. `attempt` is 1-indexed;
    /// backends can use it for jitter / circuit-breaker decisions.
    ///
    /// # Errors
    ///
    /// Returns [`UploadError::Retry`] for transient failures the
    /// framework should retry, or [`UploadError::Fatal`] to skip
    /// further retries and spool immediately.
    fn upload(
        &self,
        key: &Self::PartitionKey,
        body: &Self::Body,
        attempt: u32,
    ) -> impl Future<Output = Result<(), UploadError<Self::Error>>> + Send;

    /// Optional: render the partition key into `backend_key` +
    /// `backend_partition` label values attached to self-events.
    /// Defaults to a truncated `Debug` representation of the key as
    /// the `partition` label, with an empty `backend_key`.
    fn describe_key(&self, key: &Self::PartitionKey) -> (String, String) {
        (String::new(), truncate_debug(key))
    }
}

/// How the framework should treat an `upload` failure.
#[derive(Debug)]
pub enum UploadError<E> {
    /// Transient failure — framework retries per the configured
    /// [`RetryPolicy`](crate::RetryPolicy).
    Retry(E),
    /// Permanent failure — skip further retries on this batch and
    /// spool immediately. Example: S3 `AccessDenied` on a bucket
    /// whose policy forbids the IAM role.
    Fatal(E),
}

impl<E: fmt::Display> fmt::Display for UploadError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retry(e) => write!(f, "retry: {e}"),
            Self::Fatal(e) => write!(f, "fatal: {e}"),
        }
    }
}

impl<E> UploadError<E> {
    /// Returns the inner error.
    pub fn into_inner(self) -> E {
        match self {
            Self::Retry(e) | Self::Fatal(e) => e,
        }
    }
}

/// Truncate a `Debug` rendering of `key` so self-event labels cannot
/// blow the envelope size budget. Keeps the first 128 bytes.
fn truncate_debug<T: fmt::Debug>(value: &T) -> String {
    let s = format!("{value:?}");
    if s.len() <= 128 {
        s
    } else {
        let mut end = 128;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        let mut out = String::with_capacity(end + 1);
        out.push_str(&s[..end]);
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_debug_clips_long_strings() {
        let got = truncate_debug(&"x".repeat(200));
        assert!(got.len() <= 131, "len={}", got.len());
        assert!(got.ends_with('…'));
    }

    #[test]
    fn test_truncate_debug_preserves_short_strings() {
        #[derive(Debug)]
        struct Small;
        let got = truncate_debug(&Small);
        assert_eq!(got, "Small");
    }

    #[test]
    fn test_upload_error_into_inner() {
        let e = UploadError::<&str>::Retry("oops");
        assert_eq!(e.into_inner(), "oops");
    }
}
