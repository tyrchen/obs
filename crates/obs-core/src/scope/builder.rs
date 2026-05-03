//! `ScopeFrameBuilder` — public, programmatic API for pushing a scope
//! frame from outside `obs-core`.
//!
//! `obs::scope!` is the canonical user-facing API and resolves to a
//! macro that captures the call site at compile time. External crates
//! that need to push a frame from a generic context (the tracing
//! bridge, `obs-tower`, user-defined middleware) cannot use the macro
//! because the field set is computed at runtime. This builder fills
//! that gap. Spec 13 § 4 (D7-3 in spec 94).

use super::{ScopeField, ScopeFrame, ScopeGuard, ScopeKind};

/// Programmatic builder for pushing an `obs::scope!`-shaped frame.
///
/// External crates use this when they need to push a frame whose
/// fields are decided at runtime (e.g. the tracing bridge stamps
/// `(trace_id, span_id, parent_span_id)` from a span extension; the
/// HTTP middleware stamps the same fields from extracted W3C
/// `traceparent` headers).
///
/// The builder is consumed by [`Self::push`], which returns a
/// [`ScopeGuard`] that pops the frame on drop. To carry the frame
/// across an async boundary, use [`Self::into_frame`] and feed the
/// resulting [`ScopeFrame`] to
/// [`crate::instrumented::Instrument::instrument`].
#[derive(Debug)]
pub struct ScopeFrameBuilder {
    fields: Vec<ScopeField>,
    kind: ScopeKind,
    tail_capacity: u16,
    traceparent_sampled: Option<bool>,
    span_identity: Option<(&'static str, &'static str)>,
}

impl Default for ScopeFrameBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ScopeFrameBuilder {
    /// New builder defaulting to a `Scope` frame with a 64-deep
    /// tail-on-error buffer (matches the `obs::scope!` defaults).
    #[must_use]
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            kind: ScopeKind::Scope,
            tail_capacity: 64,
            traceparent_sampled: None,
            span_identity: None,
        }
    }

    /// Switch to a `Context` frame (no tail-on-error buffer cost).
    /// Equivalent to `obs::context!`.
    #[must_use]
    pub fn context(mut self) -> Self {
        self.kind = ScopeKind::Context;
        self.tail_capacity = 0;
        self
    }

    /// Override the tail-on-error capacity (only meaningful for
    /// `Scope` kind; ignored for `Context`).
    #[must_use]
    pub fn tail_capacity(mut self, capacity: u16) -> Self {
        self.tail_capacity = capacity;
        self
    }

    /// Set `trace_id` on the frame so emitted envelopes inherit it
    /// via [`super::auto_fill_envelope`].
    #[must_use]
    pub fn trace_id(mut self, value: impl Into<String>) -> Self {
        self.fields.push(ScopeField::TraceId(value.into()));
        self
    }

    /// Set `span_id` on the frame.
    #[must_use]
    pub fn span_id(mut self, value: impl Into<String>) -> Self {
        self.fields.push(ScopeField::SpanId(value.into()));
        self
    }

    /// Set `parent_span_id` on the frame.
    #[must_use]
    pub fn parent_span_id(mut self, value: impl Into<String>) -> Self {
        self.fields.push(ScopeField::ParentSpanId(value.into()));
        self
    }

    /// Add a `(name, value)` label pair. The `name` must be a static
    /// `&'static str` so it can round-trip through the envelope's
    /// `labels` map without an allocation. Spec 13 § 2.1.
    #[must_use]
    pub fn label(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.fields.push(ScopeField::Label(name, value.into()));
        self
    }

    /// Inbound `traceparent.sampled` decision. Spec 13 § 6.
    #[must_use]
    pub fn traceparent_sampled(mut self, sampled: bool) -> Self {
        self.traceparent_sampled = Some(sampled);
        self
    }

    /// Bridged tracing-span identity for `obs::SpanTrace` rendering.
    /// Spec 13 § 9.
    #[must_use]
    pub fn span_identity(mut self, name: &'static str, target: &'static str) -> Self {
        self.span_identity = Some((name, target));
        self
    }

    /// Push the frame onto the active task's scope stack and return
    /// the RAII guard. Drop the guard to pop the frame.
    pub fn push(self) -> ScopeGuard {
        let frame = self.into_frame();
        ScopeGuard::enter_with_frame(frame)
    }

    /// Build the frame without pushing it. Useful when the caller
    /// wants to attach it to a future via
    /// [`crate::instrumented::Instrument::instrument`] so the frame
    /// is re-entered on every poll.
    #[must_use]
    pub fn into_frame(self) -> ScopeFrame {
        let mut frame = ScopeFrame::new(self.fields, self.kind, self.tail_capacity);
        if let Some(sampled) = self.traceparent_sampled {
            frame.set_traceparent_sampled(sampled);
        }
        if let Some((name, target)) = self.span_identity {
            frame.set_span_identity(name, target);
        }
        frame
    }
}
