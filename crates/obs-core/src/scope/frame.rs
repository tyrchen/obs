//! `ScopeFrame` — one entry on the per-task / per-thread scope stack.

use std::collections::VecDeque;

use obs_proto::obs::v1::ObsEnvelope;

use crate::codegen_helpers::SpanFrame;

/// A field declared on `obs::scope!` / `obs::context!`. Field types
/// match the envelope projection contract: trace ids land on the typed
/// envelope slots, labels go into `env.labels`. Spec 13 § 2.1.
#[derive(Debug, Clone)]
pub enum ScopeField {
    /// Pushes onto `env.trace_id` when missing.
    TraceId(String),
    /// Pushes onto `env.span_id` when missing.
    SpanId(String),
    /// Pushes onto `env.parent_span_id` when missing.
    ParentSpanId(String),
    /// Pushes onto `env.labels[name]` when the key is absent.
    Label(&'static str, String),
}

impl ScopeField {
    /// Field name as it would appear on a schema (for diagnostics).
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::TraceId(_) => "trace_id",
            Self::SpanId(_) => "span_id",
            Self::ParentSpanId(_) => "parent_span_id",
            Self::Label(k, _) => k,
        }
    }

    /// Borrow the value's bytes for diagnostics or `SpanCtx` rendering.
    #[must_use]
    pub fn value(&self) -> &str {
        match self {
            Self::TraceId(v) | Self::SpanId(v) | Self::ParentSpanId(v) | Self::Label(_, v) => v,
        }
    }
}

/// Whether a frame carries a tail-on-error buffer (`Scope`) or is a
/// pure broadcast frame (`Context`). Spec 13 § 2.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScopeKind {
    /// `obs::scope!` — fields + 64-deep tail-on-error buffer.
    Scope,
    /// `obs::context!` — fields only; no buffer cost.
    Context,
}

/// One frame on the per-task / per-thread scope stack.
#[derive(Debug, Clone)]
pub struct ScopeFrame {
    fields: Vec<ScopeField>,
    kind: ScopeKind,
    tail_capacity: u16,
    tail_buffer: VecDeque<ObsEnvelope>,
    seen_error: bool,
    traceparent_sampled: Option<bool>,
    span_name: Option<&'static str>,
    span_target: Option<&'static str>,
}

impl ScopeFrame {
    /// Construct a frame with the supplied declared fields and tail
    /// buffer capacity.
    #[must_use]
    pub fn new(fields: Vec<ScopeField>, kind: ScopeKind, tail_capacity: u16) -> Self {
        let cap = match kind {
            ScopeKind::Scope => tail_capacity as usize,
            ScopeKind::Context => 0,
        };
        Self {
            fields,
            kind,
            tail_capacity,
            tail_buffer: VecDeque::with_capacity(cap),
            seen_error: false,
            traceparent_sampled: None,
            span_name: None,
            span_target: None,
        }
    }

    /// Update the inbound `traceparent.sampled` decision (set by the
    /// HTTP middleware at request entry). Spec 13 § 6.
    pub fn set_traceparent_sampled(&mut self, sampled: bool) {
        self.traceparent_sampled = Some(sampled);
    }

    /// Set the bridged tracing span identity for `obs::SpanTrace`
    /// rendering. Spec 13 § 9.
    pub fn set_span_identity(&mut self, name: &'static str, target: &'static str) {
        self.span_name = Some(name);
        self.span_target = Some(target);
    }

    /// Inbound `traceparent.sampled` decision, when set.
    #[must_use]
    pub fn traceparent_sampled(&self) -> Option<bool> {
        self.traceparent_sampled
    }

    /// Read-only view of the declared fields.
    #[must_use]
    pub fn fields(&self) -> &[ScopeField] {
        &self.fields
    }

    /// Frame kind.
    #[must_use]
    pub fn kind(&self) -> ScopeKind {
        self.kind
    }

    /// `true` if the scope has observed an `>= ERROR` envelope.
    #[must_use]
    pub fn seen_error(&self) -> bool {
        self.seen_error
    }

    /// Mark this frame's tail buffer as needing flush.
    pub fn mark_error(&mut self) {
        self.seen_error = true;
    }

    /// Capacity of the tail buffer (in envelopes).
    #[must_use]
    pub fn tail_capacity(&self) -> u16 {
        self.tail_capacity
    }

    /// Push an envelope onto the ring buffer. No-op for `Context`.
    pub fn push_tail(&mut self, env: ObsEnvelope) {
        if self.kind == ScopeKind::Context {
            return;
        }
        if self.tail_capacity == 0 {
            return;
        }
        if self.tail_buffer.len() >= self.tail_capacity as usize {
            self.tail_buffer.pop_front();
        }
        self.tail_buffer.push_back(env);
    }

    /// Drain and return the buffered envelopes (callers flush on
    /// scope-end-with-error).
    #[must_use]
    pub fn drain_tail(&mut self) -> Vec<ObsEnvelope> {
        self.tail_buffer.drain(..).collect()
    }

    /// Snapshot of currently-buffered envelopes (test-only).
    #[must_use]
    pub fn tail_snapshot(&self) -> Vec<ObsEnvelope> {
        self.tail_buffer.iter().cloned().collect()
    }

    /// Render the frame as a `SpanFrame` for `SpanTrace`.
    #[must_use]
    pub fn as_span_frame(&self) -> Option<SpanFrame<'_>> {
        Some(SpanFrame {
            name: self.span_name?,
            target: self.span_target?,
        })
    }
}
