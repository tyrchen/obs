//! `InMemoryObserver` — captures every envelope into a bounded ring
//! buffer; for tests and live debug capture (spec 11 § 3.1).

use std::sync::Arc;

use obs_proto::obs::v1::ObsEnvelope;

use crate::sink::{InMemorySink, Sink};
pub use crate::sink::InMemoryHandle;
use crate::registry::{ScrubbedEnvelope, SchemaRegistry};

use super::Observer;

/// Test-grade observer: every envelope is delivered to an
/// [`InMemorySink`]. Spec 61 § 2.4 example.
#[derive(Debug, Clone)]
pub struct InMemoryObserver {
    sink: InMemorySink,
    registry: Arc<SchemaRegistry>,
}

impl InMemoryObserver {
    /// Construct with an empty schema registry. The registry is only
    /// consulted by `ScrubbedEnvelope::pass_through` to populate
    /// `schema()` — for in-memory tests we don't need decoding so an
    /// empty registry is fine.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sink: InMemorySink::new(),
            registry: Arc::new(SchemaRegistry::empty()),
        }
    }

    /// Construct with an existing sink. Used when several observers
    /// should aggregate into the same buffer.
    #[must_use]
    pub fn with_sink(sink: InMemorySink) -> Self {
        Self {
            sink,
            registry: Arc::new(SchemaRegistry::empty()),
        }
    }

    /// Stable handle to the buffer (`drain` / `count` / `wait_for`).
    #[must_use]
    pub fn handle(&self) -> InMemoryHandle {
        self.sink.handle()
    }
}

impl Default for InMemoryObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl Observer for InMemoryObserver {
    fn emit_envelope(&self, env: ObsEnvelope) {
        // Skip the worker pool: in-memory observers are synchronous
        // for testing determinism. The pass-through wrapper feeds the
        // sink without running the scrubber (spec 14 § 5: scrubber
        // belongs in the worker; here we are the worker).
        let envref: &ObsEnvelope = &env;
        let scrubbed = ScrubbedEnvelope::pass_through(envref, &self.registry);
        self.sink.deliver(scrubbed);
    }

    fn enabled(&self, _callsite: &crate::ObsCallsite) -> bool {
        true
    }
}
