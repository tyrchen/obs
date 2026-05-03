//! `obs::scope!` and `obs::context!` runtime support — task-local /
//! thread-local stacks of `ScopeFrame`s, the tail-on-error ring buffer,
//! and the auto-fill machinery used by `EventSchema::project`.
//!
//! Spec 13 §§ 2 + 3, spec 11 § 4.1 (pipeline order steps 3 + 5).

mod builder;
mod frame;
mod guard;

use std::cell::RefCell;

use obs_proto::obs::v1::ObsEnvelope;

pub use self::{
    builder::ScopeFrameBuilder,
    frame::{ScopeField, ScopeFrame, ScopeKind},
    guard::ScopeGuard,
};

thread_local! {
    static THREAD_STACK: RefCell<Vec<ScopeFrame>> = const { RefCell::new(Vec::new()) };
}

tokio::task_local! {
    static TASK_STACK: RefCell<Vec<ScopeFrame>>;
}

/// Push a frame onto the active scope stack. Returns a numerical depth
/// hint the RAII guard uses to validate LIFO order at drop.
pub(crate) fn push_frame(frame: ScopeFrame) -> usize {
    if let Ok(depth) = TASK_STACK.try_with(|cell| {
        let mut v = cell.borrow_mut();
        v.push(frame.clone());
        v.len()
    }) {
        return depth;
    }
    THREAD_STACK.with(|cell| {
        let mut v = cell.borrow_mut();
        v.push(frame);
        v.len()
    })
}

/// Pop the active scope's top frame, returning it for the RAII guard
/// to inspect (`seen_error` decides whether the tail buffer flushes).
pub(crate) fn pop_frame() -> Option<ScopeFrame> {
    if let Ok(frame) = TASK_STACK.try_with(|cell| cell.borrow_mut().pop()) {
        return frame;
    }
    THREAD_STACK.with(|cell| cell.borrow_mut().pop())
}

/// Public push helper for external integrations that own their own
/// pop (e.g. the tracing bridge pushes on `on_enter` and pops on
/// `on_exit`). External callers MUST pair every call to
/// [`push_frame_pub`] with exactly one call to [`pop_frame_pub`] in
/// LIFO order — using [`ScopeFrameBuilder`] + the returned
/// [`ScopeGuard`] is preferred when the lifetime is RAII-shaped.
/// Spec 94 D7-3.
pub fn push_frame_pub(frame: ScopeFrame) {
    let _ = push_frame(frame);
}

/// Public pop helper. See [`push_frame_pub`] for ownership rules.
pub fn pop_frame_pub() -> Option<ScopeFrame> {
    pop_frame()
}

/// Visit every active scope frame innermost-first. Used by
/// `auto_fill_envelope` and by `obs::SpanTrace`.
pub fn with_frames_innermost_first<F, R>(f: F) -> R
where
    F: FnOnce(&[ScopeFrame]) -> R,
{
    // The closure is FnOnce so we cannot reuse it across both
    // task-local and thread-local probes. Snapshot the active stack
    // into a single Vec and hand it to the user.
    let snapshot = collect_active_stack();
    f(snapshot.as_slice())
}

fn collect_active_stack() -> Vec<ScopeFrame> {
    if let Ok(v) = TASK_STACK.try_with(|cell| cell.borrow().clone()) {
        return v;
    }
    THREAD_STACK.with(|cell| cell.borrow().clone())
}

/// Walk active scopes innermost-first and inject any declared fields
/// the envelope is missing. Mirrors spec 13 § 2.1: only `None`-equivalent
/// envelope slots inherit; explicit values pass through untouched.
pub fn auto_fill_envelope(env: &mut ObsEnvelope) {
    let frames = collect_active_stack();
    for frame in frames.iter().rev() {
        for field in frame.fields() {
            match field {
                ScopeField::TraceId(v) if env.trace_id.is_empty() => {
                    env.trace_id.clone_from(v);
                }
                ScopeField::SpanId(v) if env.span_id.is_empty() => {
                    env.span_id.clone_from(v);
                }
                ScopeField::ParentSpanId(v) if env.parent_span_id.is_empty() => {
                    env.parent_span_id.clone_from(v);
                }
                ScopeField::Label(k, v) if !env.labels.contains_key(*k) => {
                    env.labels.insert((*k).to_string(), v.clone());
                }
                _ => {}
            }
        }
    }
}

/// Inbound `traceparent.sampled` decision from the outermost (oldest)
/// scope frame, when set. Spec 13 § 6.
#[must_use]
pub fn inbound_traceparent_sampled() -> Option<bool> {
    let frames = collect_active_stack();
    frames.iter().find_map(|f| f.traceparent_sampled())
}

/// Read the active scope's correlation pair as
/// `Some((trace_id, span_id))` when both have been pushed, otherwise
/// `None`. Walks the stack innermost-first so the *deepest* set value
/// wins — matches the auto-fill ordering in `auto_fill_envelope`.
///
/// This is the symmetrical *read* surface to `ScopeFrameBuilder` (D7-3,
/// which writes frames). Spec 95 D8-2: outbound HTTP middleware,
/// OTLP exporters, and any other generic code that needs to inherit
/// the active trace context calls this function.
#[must_use]
pub fn active_correlation() -> Option<(String, String)> {
    let frames = collect_active_stack();
    let mut trace_id: Option<String> = None;
    let mut span_id: Option<String> = None;
    for frame in frames.iter().rev() {
        for field in frame.fields() {
            match field {
                ScopeField::TraceId(v) if trace_id.is_none() => trace_id = Some(v.clone()),
                ScopeField::SpanId(v) if span_id.is_none() => span_id = Some(v.clone()),
                _ => {}
            }
        }
        if trace_id.is_some() && span_id.is_some() {
            break;
        }
    }
    match (trace_id, span_id) {
        (Some(t), Some(s)) => Some((t, s)),
        _ => None,
    }
}

/// Inbound sampled decision exposed under the same naming as
/// [`active_correlation`] so callers can grab both with one symmetrical
/// import. Equivalent to [`inbound_traceparent_sampled`]. Spec 95 D8-2.
#[must_use]
pub fn active_sampled() -> Option<bool> {
    inbound_traceparent_sampled()
}

/// Push an envelope onto the innermost active scope's tail buffer (if
/// the scope is a `Scope`, not a `Context`). No-op when no frame is
/// active or the active frame is `Context`. Spec 13 § 6.
pub fn push_tail_buffer(env: &ObsEnvelope) {
    if let Ok(()) = TASK_STACK.try_with(|cell| push_to_top(cell.borrow_mut().last_mut(), env)) {
        return;
    }
    THREAD_STACK.with(|cell| push_to_top(cell.borrow_mut().last_mut(), env));
}

/// Mark every active scope frame as having seen an error so the
/// outermost scope's drop will trigger the tail-on-error flush.
/// Spec 13 § 6.
pub fn mark_error_on_active_scopes() {
    if let Ok(()) = TASK_STACK.try_with(|cell| {
        for f in cell.borrow_mut().iter_mut() {
            f.mark_error();
        }
    }) {
        return;
    }
    THREAD_STACK.with(|cell| {
        for f in cell.borrow_mut().iter_mut() {
            f.mark_error();
        }
    });
}

/// Run `fut` under a fresh task-local scope stack so spawned tasks do
/// not see the parent's stack. Used by callers that want a clean
/// child task with no inherited frames.
#[allow(dead_code)]
pub(crate) async fn scope_task<F, R>(fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    TASK_STACK.scope(RefCell::new(Vec::new()), fut).await
}

fn push_to_top(top: Option<&mut ScopeFrame>, env: &ObsEnvelope) {
    if let Some(frame) = top {
        frame.push_tail(env.clone());
    }
}

#[cfg(test)]
mod tests {
    use obs_types::Severity;

    use super::*;

    fn make_frame(fields: Vec<ScopeField>, kind: ScopeKind) -> ScopeFrame {
        ScopeFrame::new(fields, kind, 64)
    }

    #[test]
    fn test_should_inject_label_when_envelope_missing() {
        let frame = make_frame(
            vec![ScopeField::Label("tenant", "alpha".to_string())],
            ScopeKind::Scope,
        );
        let _depth = push_frame(frame);
        let mut env = ObsEnvelope::default();
        auto_fill_envelope(&mut env);
        assert_eq!(env.labels.get("tenant"), Some(&"alpha".to_string()));
        let _ = pop_frame();
    }

    #[test]
    fn test_should_not_overwrite_explicit_label() {
        let frame = make_frame(
            vec![ScopeField::Label("tenant", "alpha".to_string())],
            ScopeKind::Scope,
        );
        let _depth = push_frame(frame);
        let mut env = ObsEnvelope::default();
        env.labels.insert("tenant".to_string(), "beta".to_string());
        auto_fill_envelope(&mut env);
        assert_eq!(env.labels.get("tenant"), Some(&"beta".to_string()));
        let _ = pop_frame();
    }

    #[test]
    fn test_should_inject_trace_id() {
        let frame = make_frame(
            vec![ScopeField::TraceId("abc".to_string())],
            ScopeKind::Scope,
        );
        let _depth = push_frame(frame);
        let mut env = ObsEnvelope::default();
        auto_fill_envelope(&mut env);
        assert_eq!(env.trace_id, "abc");
        let _ = pop_frame();
    }

    #[test]
    fn test_should_return_active_correlation_when_present() {
        let frame = make_frame(
            vec![
                ScopeField::TraceId("trc".to_string()),
                ScopeField::SpanId("spn".to_string()),
            ],
            ScopeKind::Scope,
        );
        let _ = push_frame(frame);
        assert_eq!(
            active_correlation(),
            Some(("trc".to_string(), "spn".to_string())),
        );
        let _ = pop_frame();
    }

    #[test]
    fn test_should_return_none_when_no_correlation() {
        // Each test runs on its own thread; no scope active.
        assert_eq!(active_correlation(), None);
    }

    #[test]
    fn test_should_walk_stack_for_correlation() {
        let outer = make_frame(
            vec![ScopeField::TraceId("outer-trc".to_string())],
            ScopeKind::Scope,
        );
        let inner = make_frame(
            vec![ScopeField::SpanId("inner-spn".to_string())],
            ScopeKind::Scope,
        );
        let _ = push_frame(outer);
        let _ = push_frame(inner);
        // Walking innermost-first: span_id from inner, trace_id from outer.
        assert_eq!(
            active_correlation(),
            Some(("outer-trc".to_string(), "inner-spn".to_string())),
        );
        let _ = pop_frame();
        let _ = pop_frame();
    }

    #[test]
    fn test_should_push_tail_buffer_only_for_scope_kind() {
        let frame = make_frame(vec![], ScopeKind::Context);
        let _ = push_frame(frame);
        let env = ObsEnvelope {
            full_name: "test.v1.X".to_string(),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_DEBUG),
            ..Default::default()
        };
        push_tail_buffer(&env);
        let frame = pop_frame().unwrap();
        // Context kind should not buffer.
        assert!(frame.tail_snapshot().is_empty());
        let _ = Severity::Debug;
    }
}
