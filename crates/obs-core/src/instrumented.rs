//! `Instrumented<F>` — async scope + observer adapter.
//!
//! `obs::scope!` returns a `ScopeGuard` that pushes a frame onto the
//! per-task stack on construction and pops it on `Drop`. For futures
//! that cross `tokio::spawn` we cannot rely on a single-poll RAII
//! guard, so we wrap the future in `Instrumented<F>` which re-enters
//! the scope on every `poll`.
//!
//! Spec 13 § 3.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

use crate::{
    observer::{Observer, with_observer_task_sync},
    scope::{ScopeField, ScopeFrame, ScopeKind, pop_frame, push_frame},
};

pin_project! {
    /// Future adapter that re-enters an `obs::scope!` frame and an
    /// `Arc<dyn Observer>` override on every poll. Constructed via
    /// [`Instrument::instrument`] / [`WithObserver::with_observer`].
    #[must_use = "Instrumented<F> is a future; await it to drive the inner future"]
    pub struct Instrumented<F> {
        #[pin]
        inner: F,
        scope_seed: Option<ScopeSeed>,
        observer: Option<Arc<dyn Observer>>,
    }
}

impl<F> std::fmt::Debug for Instrumented<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Instrumented")
            .field("has_scope", &self.scope_seed.is_some())
            .field("has_observer", &self.observer.is_some())
            .finish()
    }
}

/// Frame-shaped seed cloned on every poll. Holding the frame as a seed
/// (rather than the live `ScopeFrame`) means each poll gets a fresh
/// tail buffer, which is what the spec requires for safe re-entry.
#[derive(Debug, Clone)]
struct ScopeSeed {
    fields: Vec<ScopeField>,
    kind: ScopeKind,
    tail_capacity: u16,
}

impl ScopeSeed {
    fn into_frame(self) -> ScopeFrame {
        ScopeFrame::new(self.fields, self.kind, self.tail_capacity)
    }

    fn from_frame(f: &ScopeFrame) -> Self {
        Self {
            fields: f.fields().to_vec(),
            kind: f.kind(),
            tail_capacity: f.tail_capacity(),
        }
    }
}

impl<F: Future> Future for Instrumented<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let _scope_guard = this
            .scope_seed
            .as_ref()
            .map(|seed| PollScopeGuard::push(seed.clone().into_frame()));
        match this.observer.as_ref() {
            Some(o) => with_observer_task_sync(o.clone(), || this.inner.poll(cx)),
            None => this.inner.poll(cx),
        }
    }
}

/// Per-poll scope guard: pushes the frame at poll-start, pops at
/// poll-end. Kept private to this module so callers cannot bypass the
/// guarantee that `Instrumented::poll` is the only path that mutates
/// the per-task scope stack.
struct PollScopeGuard;

impl PollScopeGuard {
    fn push(frame: ScopeFrame) -> Self {
        let _ = push_frame(frame);
        Self
    }
}

impl Drop for PollScopeGuard {
    fn drop(&mut self) {
        let _ = pop_frame();
    }
}

/// Public extension trait — `.instrument(scope!(...))` on every future
/// owned by user code.
pub trait Instrument: Future + Sized {
    /// Attach an `obs::scope!`-built frame to the future. The frame
    /// is re-entered on every poll, so suspended futures keep their
    /// scope across `.await` and `tokio::spawn` boundaries.
    fn instrument(self, scope: ScopeFrame) -> Instrumented<Self> {
        Instrumented {
            inner: self,
            scope_seed: Some(ScopeSeed::from_frame(&scope)),
            observer: None,
        }
    }
}

impl<F: Future> Instrument for F {}

/// Public extension trait — `.with_observer(o)` on a future binds an
/// observer override that follows the future across thread migration
/// (per-task tier; spec 11 § 3.1).
pub trait WithObserver: Future + Sized {
    /// Bind a per-task observer override to the future.
    fn with_observer(self, observer: Arc<dyn Observer>) -> Instrumented<Self> {
        Instrumented {
            inner: self,
            scope_seed: None,
            observer: Some(observer),
        }
    }
}

impl<F: Future> WithObserver for F {}

impl<F: Future> Instrumented<F> {
    /// Layer a scope on top of an `Instrumented` that already carries
    /// an observer — supports both call orders described in spec 13.
    pub fn instrument(mut self, scope: ScopeFrame) -> Self {
        self.scope_seed = Some(ScopeSeed::from_frame(&scope));
        self
    }

    /// Layer an observer on top of an `Instrumented` that already
    /// carries a scope.
    pub fn with_observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observer = Some(observer);
        self
    }
}
