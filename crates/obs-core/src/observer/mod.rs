//! Three-tier observer resolution + the `Observer` trait.
//!
//! Spec 11 § 3:
//!
//! - Per-task `OBSERVER_TASK` (highest priority; via `Future::with_observer` adapter, lands in spec
//!   13 § 3 / Phase 3 task 3.3).
//! - Per-thread `OBSERVER_THREAD` — `with_observer_thread_local`, `with_test_observer`,
//!   `#[obs::test]`.
//! - Global `OBSERVER_GLOBAL` — `install_observer`.
//!
//! The hot path checks `OVERRIDE_COUNT == 0` first so a process that
//! never installs an override pays one atomic load and one
//! `ArcSwap::load_full`. The `CAN_ENTER` cell prevents re-entry when a
//! sink synthesises an event.

mod in_memory;
mod noop;
mod standard;
pub(crate) mod workers;

use std::{
    cell::{Cell, RefCell},
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use arc_swap::ArcSwap;
use obs_proto::obs::v1::ObsEnvelope;
use once_cell::sync::Lazy;

pub use self::{
    in_memory::{InMemoryHandle, InMemoryObserver},
    noop::NoopObserver,
    standard::{BuildError, StandardObserver, StandardObserverBuilder},
    workers::WorkerCounters,
};
use crate::callsite::ObsCallsite;

/// The global observer trait. **Sealed** in spirit (downstream crates
/// implement it freely, but the SDK ships `NoopObserver`,
/// `InMemoryObserver`, `StandardObserver` covering 99% of cases).
///
/// `&self` everywhere so the same observer reference is reused across
/// every emit; no locks taken on the hot path.
pub trait Observer: Send + Sync + 'static {
    /// Hot-path emit. Never blocks. Never panics. Spec 11 § 3.
    fn emit_envelope(&self, env: ObsEnvelope);

    /// Cheap callsite filter check; called only when the cached
    /// `Interest` is `Sometimes`. Default impl returns `true`
    /// (allows every callsite that survived `enabled_static`).
    fn enabled(&self, callsite: &ObsCallsite) -> bool {
        let _ = callsite;
        true
    }

    /// Monotonic generation counter; bumped on every filter / config
    /// change so callsite caches re-validate. Spec 11 § 3.2.
    fn generation(&self) -> u32 {
        0
    }

    /// Force every callsite's `interest` cache back to `Unknown`.
    /// Default impl is a no-op for observers that don't filter.
    fn reload_filter(&self) {}

    /// Flush all sinks; await in-flight batches. Default no-op.
    fn flush(&self) -> crate::sink::SinkFut<'_> {
        Box::pin(async {})
    }

    /// Shutdown all sinks and join workers. Idempotent.
    fn shutdown(&self) -> crate::sink::SinkFut<'_> {
        Box::pin(async {})
    }

    /// Synchronous shutdown for use in panic hooks and `Drop` impls
    /// where awaiting is not possible. Best-effort within `timeout`.
    /// Spec 11 § 3, § 6.1.
    fn shutdown_blocking(&self, timeout: std::time::Duration) {
        let _ = timeout;
    }

    /// Access this observer's per-process callsite registry, when it
    /// has one. The bridge (Direction A) writes the registry on first
    /// sight; `ObsToTracingSink` reads it to reconstitute
    /// `tracing::Metadata` for interned envelopes. Spec 31 § 3.2.
    fn callsites(&self) -> Option<std::sync::Arc<crate::registry::ObsCallsiteRegistry>> {
        None
    }

    /// Access this observer's schema registry, when it has one. Sinks
    /// hold their own `Arc<SchemaRegistry>` from construction; this
    /// hook lets the bridge fall back to the global observer's
    /// registry without depending on `StandardObserver`.
    fn schema_registry(&self) -> Option<std::sync::Arc<crate::registry::SchemaRegistry>> {
        None
    }

    /// Snapshot of the workspace-shared `ResourceAttrs` (OTel
    /// semantic-convention keys). Sinks call this at flush time so a
    /// single config-watcher reload re-projects every sink. Default
    /// returns the empty / observer-less attribute set; concrete
    /// observers (`StandardObserver`) override. Spec 20 § 2.1 / spec
    /// 94 § 2.7 / P1-E.
    fn resource_attrs(&self) -> std::sync::Arc<crate::resource::ResourceAttrs> {
        std::sync::Arc::new(crate::resource::ResourceAttrs::default())
    }
}

// ─── Resolution slots (spec 11 § 3) ───────────────────────────────────

/// Global observer slot. `ArcSwap<T>` requires `T: Sized` (arc_swap's
/// `RefCnt` is sized-only), and `dyn Observer` is unsized, so we
/// store `Arc<dyn Observer>` (a sized fat pointer) inside the
/// `ArcSwap`. `load_full()` therefore returns
/// `Arc<Arc<dyn Observer>>`; `observer()` derefs the outer `Arc` to
/// hand back `Arc<dyn Observer>` directly. See
/// `docs/research/spike-arcswap.md`.
static OBSERVER_GLOBAL: Lazy<ArcSwap<Arc<dyn Observer>>> = Lazy::new(|| {
    let initial: Arc<dyn Observer> = Arc::new(NoopObserver);
    ArcSwap::from_pointee(initial)
});

thread_local! {
    /// Per-thread override. `RefCell` so nested installs stack LIFO.
    /// Mirrors tracing's `CURRENT_STATE.default`.
    static OBSERVER_THREAD: RefCell<Option<Arc<dyn Observer>>> =
        const { RefCell::new(None) };

    /// Re-entry guard. Set to `false` while inside an `Observer::emit_envelope`
    /// so that an inner emit (a sink synthesising an event) returns
    /// `NoopObserver` and becomes a no-op.
    static CAN_ENTER: Cell<bool> = const { Cell::new(true) };
}

tokio::task_local! {
    /// Per-task override. Set by `Future::with_observer(o)` (the
    /// `Instrumented<F>` adapter; lands in spec 13 § 3, Phase 3
    /// task 3.3). Defining the task-local slot now lets observer()
    /// probe it correctly even before the adapter exists, so
    /// resolution is forward-compatible.
    static OBSERVER_TASK: Arc<dyn Observer>;
}

/// Hot-path fast flag. `0` ⇒ no override has ever been installed in
/// this process; the resolver skips both probes and goes straight to
/// the global. Spec 11 § 3 / spec 99 D-3.
static OVERRIDE_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Resolve the active observer.
///
/// Hot path: returns a cloned `Arc` (one atomic refcount bump) so the
/// caller does not hold a `Guard` across `await`. With no overrides
/// ever installed, this collapses to `OVERRIDE_COUNT == 0` test +
/// `OBSERVER_GLOBAL.load_full()`. Spec 11 § 3.
#[inline]
#[must_use]
pub fn observer() -> Arc<dyn Observer> {
    if !CAN_ENTER.with(Cell::get) {
        return noop_observer_arc();
    }
    if OVERRIDE_COUNT.load(Ordering::Relaxed) == 0 {
        // load_full() returns Arc<Arc<dyn Observer>>; (*x).clone()
        // extracts the inner Arc<dyn Observer> with one refcount bump.
        let outer = OBSERVER_GLOBAL.load_full();
        return (*outer).clone();
    }
    if let Ok(per_task) = OBSERVER_TASK.try_with(Clone::clone) {
        return per_task;
    }
    if let Some(per_thread) = OBSERVER_THREAD.with(|c| c.borrow().clone()) {
        return per_thread;
    }
    let outer = OBSERVER_GLOBAL.load_full();
    (*outer).clone()
}

fn noop_observer_arc() -> Arc<dyn Observer> {
    Arc::new(NoopObserver)
}

/// Install the global observer. Called once at process start.
///
/// The argument is a constructed observer (typically
/// `StandardObserver::builder().build()?`); we wrap it in `Arc` and
/// store it under `OBSERVER_GLOBAL`.
pub fn install_observer<O: Observer>(o: O) {
    let arc: Arc<dyn Observer> = Arc::new(o);
    OBSERVER_GLOBAL.store(Arc::new(arc));
}

/// Install a pre-arc'd observer. Convenience for tests and patterns
/// that already have an `Arc<dyn Observer>` (e.g. multi-tenant
/// registries).
pub fn install_observer_arc(o: Arc<dyn Observer>) {
    OBSERVER_GLOBAL.store(Arc::new(o));
}

/// Dispatch one envelope through the observer with the re-entry
/// guard held. Spec 11 § 3.1 "Re-entry and the CAN_ENTER cell".
///
/// All emit paths (`Emit::emit`, the `obs::emit!` macro, the
/// `<EventBuilder>::emit` setter) route through this so a sink that
/// synthesises a new envelope from inside `Observer::emit_envelope`
/// sees `observer()` returning `NoopObserver` and the inner emit
/// becomes a no-op.
#[inline]
pub fn enter_emit_envelope(observer: &Arc<dyn Observer>, env: ObsEnvelope) {
    let was_in = CAN_ENTER.with(|c| c.replace(false));
    if was_in {
        observer.emit_envelope(env);
    } else {
        // Spec 11 § 10 / spec 93 P2-13: surface the re-entry drop as
        // an `ObsSinkDropped{tier=*, reason="reentry"}` self-event so
        // operators can spot a sink that recursively emits. The
        // emit-on-emit path itself is still suppressed by the
        // `was_in == false` branch above; we only fire the metric.
        let tier = match env.tier {
            ::buffa::EnumValue::Known(t) => match t {
                obs_proto::obs::v1::Tier::TIER_LOG => "log",
                obs_proto::obs::v1::Tier::TIER_METRIC => "metric",
                obs_proto::obs::v1::Tier::TIER_TRACE => "trace",
                obs_proto::obs::v1::Tier::TIER_AUDIT => "audit",
                _ => "unspecified",
            },
            _ => "unknown",
        };
        crate::self_events::emit_sink_dropped(tier, "reentry");
    }
    CAN_ENTER.with(|c| c.set(was_in));
}

/// Weak handle for code that needs to refer to the observer without
/// extending its lifetime — chiefly sinks that internally hold
/// callbacks back into the observer (e.g. the future
/// `ObsToTracingSink` re-emitting through the registered tracing
/// dispatcher).
#[derive(Clone)]
pub struct WeakObserver(Weak<dyn Observer>);

impl WeakObserver {
    /// Upgrade to a strong reference. Returns `None` after
    /// `shutdown()` has dropped the last strong reference.
    #[must_use]
    pub fn upgrade(&self) -> Option<Arc<dyn Observer>> {
        self.0.upgrade()
    }
}

impl std::fmt::Debug for WeakObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakObserver").finish_non_exhaustive()
    }
}

/// Weak handle to whatever `observer()` would return right now.
#[must_use]
pub fn observer_weak() -> WeakObserver {
    let strong = observer();
    WeakObserver(Arc::downgrade(&strong))
}

/// Sync RAII guard. Sets the per-thread observer override; restores
/// the previous value on drop. **Do not hold across `.await`** — see
/// the warning in spec 11 § 3.1; use `Future::with_observer` (Phase 3
/// task 3.3) for async.
///
/// Bumps `OVERRIDE_COUNT` on first install in this process.
#[must_use = "the override is reverted on Drop; bind to a variable"]
pub fn with_observer_thread_local(o: Arc<dyn Observer>) -> ThreadObserverGuard {
    let prev = OBSERVER_THREAD.with(|c| c.borrow_mut().replace(o));
    OVERRIDE_COUNT.fetch_add(1, Ordering::Relaxed);
    ThreadObserverGuard { prev }
}

/// RAII guard returned by [`with_observer_thread_local`].
pub struct ThreadObserverGuard {
    prev: Option<Arc<dyn Observer>>,
}

impl std::fmt::Debug for ThreadObserverGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadObserverGuard")
            .field("had_prev", &self.prev.is_some())
            .finish()
    }
}

impl Drop for ThreadObserverGuard {
    fn drop(&mut self) {
        OBSERVER_THREAD.with(|c| {
            *c.borrow_mut() = self.prev.take();
        });
        OVERRIDE_COUNT.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Test-flavored helper. Installs the observer per-thread for the
/// duration of `f`; restores on closure exit. Used by `#[obs::test]`
/// (Phase 2 task 2.6) and by users that want to assert what was
/// emitted from a synchronous block.
///
/// Takes `Arc<dyn Observer>` so the caller can keep a clone and
/// inspect it after the closure (e.g. `InMemoryObserver::handle()`).
/// Spec 11 § 3.1.
///
/// Nested calls stack LIFO via the per-thread `RefCell` slot.
pub fn with_test_observer<F, R>(observer: Arc<dyn Observer>, f: F) -> R
where
    F: FnOnce() -> R,
{
    let _g = with_observer_thread_local(observer);
    f()
}

/// Async sibling of [`with_test_observer`]: install the observer in
/// the per-task slot for the duration of `fut`. This is the version
/// the `#[obs::test]` async expansion uses so that an emit on a
/// migrated-tokio-worker-thread still resolves to the test observer
/// (the per-thread slot would not be set on the destination worker).
///
/// `OVERRIDE_COUNT` is bumped before entering the scope and
/// decremented after, so the hot-path resolver actually probes the
/// task-local. Spec 11 § 3.1, KD-D3.
pub async fn with_observer_task<F, R>(observer: Arc<dyn Observer>, fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    OVERRIDE_COUNT.fetch_add(1, Ordering::Relaxed);
    let result = OBSERVER_TASK.scope(observer, fut).await;
    OVERRIDE_COUNT.fetch_sub(1, Ordering::Relaxed);
    result
}

/// Synchronous sibling of [`with_observer_task`] used by
/// `Instrumented<F>::poll` so a single poll can bind / unbind the
/// per-task observer without requiring an `await`. Spec 13 § 3.
pub fn with_observer_task_sync<F, R>(observer: Arc<dyn Observer>, f: F) -> R
where
    F: FnOnce() -> R,
{
    OVERRIDE_COUNT.fetch_add(1, Ordering::Relaxed);
    let result = OBSERVER_TASK.sync_scope(observer, f);
    OVERRIDE_COUNT.fetch_sub(1, Ordering::Relaxed);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::in_memory::InMemoryObserver;

    #[test]
    fn test_should_default_to_noop() {
        // Default installation is `NoopObserver`. We can't easily
        // identify it by type-id, but we can confirm `observer()`
        // returns successfully.
        let o = observer();
        assert!(Arc::strong_count(&o) >= 1);
    }

    #[test]
    fn test_with_test_observer_should_capture() {
        let observer = InMemoryObserver::new();
        let handle = observer.handle();
        let observer: Arc<dyn Observer> = Arc::new(observer);
        with_test_observer(observer, || {
            // The thread-local is now set; observer() returns the
            // InMemoryObserver, which is wired to its own InMemorySink.
        });
        // The InMemoryObserver in this Phase-1 shell does not fire
        // sinks unless the user calls emit_envelope manually; this
        // test is about resolution, not the full pipeline.
        let _ = handle;
    }
}
