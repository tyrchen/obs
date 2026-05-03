//! `ScrubbedEnvelope<'a>` — the worker→sink handoff.
//!
//! The per-tier worker runs the payload scrubber and then hands a
//! `ScrubbedEnvelope` to each `Sink::deliver`. The `'a` lifetime ties
//! the scrubbed payload to the worker's scratch buffer, so a sink
//! cannot escape a reference past the per-event call boundary.
//!
//! Spec 14 § 5.

use bytes::BytesMut;
use obs_proto::obs::v1::ObsEnvelope;

use super::{
    SchemaRegistry,
    erased::{EventSchemaErased, ScrubError},
};

/// Read-only view of an envelope whose payload has already been run
/// through the per-tier scrubber. Constructed by the worker; consumed
/// by sinks.
pub struct ScrubbedEnvelope<'a> {
    inner: &'a ObsEnvelope,
    payload: &'a [u8],
    schema: Option<&'static dyn EventSchemaErased>,
}

impl std::fmt::Debug for ScrubbedEnvelope<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScrubbedEnvelope")
            .field("full_name", &self.inner.full_name)
            .field("payload_len", &self.payload.len())
            .field("schema", &self.schema.map(|s| s.full_name()))
            .finish()
    }
}

impl<'a> ScrubbedEnvelope<'a> {
    /// Worker-side: run the scrubber, build the wrapper.
    ///
    /// **`pub(crate)`** by design — only the per-tier worker may
    /// construct a `ScrubbedEnvelope`. Sinks receive it through
    /// `Sink::deliver`. Spec 14 § 5.
    ///
    /// # Errors
    ///
    /// Returns `ScrubError` when the schema's scrubber fails to
    /// re-encode the payload. The unscrubbed envelope is **never**
    /// passed to a sink (spec 14 § 8 last row).
    #[allow(dead_code)] // wired by Phase-3 task 3.1 worker pool
    pub(crate) fn scrub(
        env: &'a ObsEnvelope,
        registry: &SchemaRegistry,
        scratch: &'a mut BytesMut,
    ) -> Result<Self, ScrubError> {
        let schema = registry.lookup(env);
        let payload = match schema {
            Some(s) => s.scrub_for_log(&env.payload, scratch)?,
            None => env.payload.as_slice(),
        };
        Ok(Self {
            inner: env,
            payload,
            schema,
        })
    }

    /// Build a wrapper that hands a sink the *raw* payload bytes
    /// without running the scrubber. Used by paths that have already
    /// scrubbed (the test `InMemorySink`) or for which scrubbing is
    /// not applicable (Phase-1 stdout pretty-printer).
    ///
    /// `pub(crate)` since only the runtime constructs this; the per-tier
    /// worker switches between `scrub` and `pass_through` based on the
    /// schema's classification annotations. Spec 14 § 5.
    #[must_use]
    pub(crate) fn pass_through(env: &'a ObsEnvelope, registry: &SchemaRegistry) -> Self {
        Self {
            inner: env,
            payload: &env.payload,
            schema: registry.lookup(env),
        }
    }

    /// Test-only constructor that mirrors `Self::pass_through` (the
    /// internal worker-thread fast-path that wraps an already-scrubbed
    /// envelope without re-running the scrubber).
    ///
    /// Gated behind the `test` feature so production sinks cannot
    /// fabricate envelopes — only test code that opts into the `test`
    /// feature gets this constructor. Spec 14 § 5 / KD3.
    ///
    /// # Panics
    ///
    /// Never panics. Returns a `ScrubbedEnvelope` whose payload borrows
    /// directly from `env.payload`.
    #[cfg(feature = "test")]
    #[must_use]
    pub fn for_test(env: &'a ObsEnvelope, registry: &SchemaRegistry) -> Self {
        Self::pass_through(env, registry)
    }

    /// Borrow the underlying envelope (without payload mutation).
    #[must_use]
    pub fn envelope(&self) -> &ObsEnvelope {
        self.inner
    }

    /// The (possibly scrubbed) payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload
    }

    /// Resolved schema, when this envelope's `full_name`/`schema_hash`
    /// is registered. `None` for foreign-producer envelopes.
    #[must_use]
    pub fn schema(&self) -> Option<&'static dyn EventSchemaErased> {
        self.schema
    }
}
