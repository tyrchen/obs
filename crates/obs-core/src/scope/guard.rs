//! `ScopeGuard` — RAII guard returned by `obs::scope!` /
//! `obs::context!`. Pops the frame on drop and, for `Scope` kind,
//! flushes the tail buffer when `seen_error == true`.
//!
//! Spec 13 §§ 2 + 6.

use std::sync::Arc;

use obs_proto::obs::v1::{ObsEnvelope, SamplingReason as PSamplingReason};

use super::{ScopeField, ScopeFrame, ScopeKind, pop_frame, push_frame};
use crate::observer::Observer;

/// RAII guard returned by `obs::scope!` and `obs::context!`. Dropping
/// pops the frame; for `Scope` kind frames where any `>= ERROR`
/// envelope was observed, the tail buffer is flushed back through the
/// active observer with `sampling_reason = TAIL_ERROR`.
#[must_use = "the scope guard is popped on Drop; bind to a name like `_scope`"]
pub struct ScopeGuard {
    /// `None` after `into_inner` so the destructor knows not to pop.
    armed: bool,
}

impl std::fmt::Debug for ScopeGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeGuard")
            .field("armed", &self.armed)
            .finish()
    }
}

impl ScopeGuard {
    /// Push a `Scope` frame.
    pub fn enter(fields: Vec<ScopeField>, tail_capacity: u16) -> Self {
        let _ = push_frame(ScopeFrame::new(fields, ScopeKind::Scope, tail_capacity));
        Self { armed: true }
    }

    /// Push a `Context` frame (no tail buffer).
    pub fn enter_context(fields: Vec<ScopeField>) -> Self {
        let _ = push_frame(ScopeFrame::new(fields, ScopeKind::Context, 0));
        Self { armed: true }
    }

    /// Push a frame with explicit identity, used by the bridge to
    /// stamp `(name, target)` for `obs::SpanTrace`.
    pub fn enter_with_identity(
        fields: Vec<ScopeField>,
        kind: ScopeKind,
        tail_capacity: u16,
        name: &'static str,
        target: &'static str,
    ) -> Self {
        let mut frame = ScopeFrame::new(fields, kind, tail_capacity);
        frame.set_span_identity(name, target);
        let _ = push_frame(frame);
        Self { armed: true }
    }

    /// Detach the guard so the caller can layer it onto a
    /// `Future::instrument(...)` (the future then re-applies the frame
    /// on every poll). The frame is popped immediately so the caller
    /// doesn't hold two copies.
    #[must_use]
    pub fn into_inner(mut self) -> ScopeFrame {
        self.armed = false;
        // Pop the live frame back out so the caller can transplant it
        // onto an `Instrumented<F>`; if pop_frame returns None (because
        // a child task already cleared its own stack), synthesise an
        // empty frame so callers don't hit `Option::unwrap`.
        pop_frame().unwrap_or_else(|| ScopeFrame::new(Vec::new(), ScopeKind::Scope, 64))
    }
}

impl Drop for ScopeGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(mut frame) = pop_frame() else {
            return;
        };
        if frame.kind() != ScopeKind::Scope {
            return;
        }
        if !frame.seen_error() {
            return;
        }
        flush_tail_buffer(&mut frame);
    }
}

fn flush_tail_buffer(frame: &mut ScopeFrame) {
    let observer = crate::observer::observer();
    flush_through(&observer, frame);
}

fn flush_through(observer: &Arc<dyn Observer>, frame: &mut ScopeFrame) {
    for mut env in frame.drain_tail() {
        env.sampling_reason =
            ::buffa::EnumValue::Known(PSamplingReason::SAMPLING_REASON_TAIL_ERROR);
        // Cannot recurse through enter_emit_envelope because the
        // outer emit already cleared CAN_ENTER. Instead, dispatch
        // directly: tail flush happens *after* the original error's
        // emit completes so re-entry is safe.
        flush_one(observer, env);
    }
}

fn flush_one(observer: &Arc<dyn Observer>, env: ObsEnvelope) {
    observer.emit_envelope(env);
}
