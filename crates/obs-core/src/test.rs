//! Test ergonomics — gated behind the `test` feature so release binaries
//! don't carry the surface. Spec 60 § 8 + spec 72 § 2.
//!
//! - [`assert_emitted!`] partial-match macro that scans the thread-local
//!   [`crate::observer::InMemoryObserver`] handle for an envelope matching a struct-literal
//!   pattern.
//! - [`take_emitted`] helper that drains every captured envelope on the current thread/task — used
//!   in `Result`-returning tests.
//!
//! The `#[obs::test]` attribute (spec 72 § 3, lives in `obs-macros`)
//! installs an [`crate::InMemoryObserver`] under
//! [`crate::with_test_observer`] (sync) or [`crate::WithObserver`]
//! (async) and stashes the handle in a task-local / thread-local cell
//! that `assert_emitted!` reads.

use std::{cell::RefCell, sync::Arc};

use obs_proto::obs::v1::ObsEnvelope;

use crate::observer::{InMemoryHandle, InMemoryObserver, Observer};

thread_local! {
    static TEST_HANDLE: RefCell<Option<InMemoryHandle>> = const { RefCell::new(None) };
}

tokio::task_local! {
    static TEST_HANDLE_TASK: InMemoryHandle;
}

/// Construct an observer + handle pair, install the handle in the
/// current thread's slot, and return both. Callers should hold the
/// observer for the lifetime of the test (e.g. via
/// [`crate::with_test_observer`]) and drop the [`TestHandleGuard`] when
/// the test exits.
#[must_use]
pub fn install_thread_handle() -> (Arc<dyn Observer>, InMemoryHandle, TestHandleGuard) {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let prev = TEST_HANDLE.with(|c| c.borrow_mut().replace(handle.clone()));
    (Arc::new(observer), handle, TestHandleGuard { prev })
}

/// RAII guard that restores the previous thread-local handle on drop.
#[derive(Debug)]
pub struct TestHandleGuard {
    prev: Option<InMemoryHandle>,
}

impl Drop for TestHandleGuard {
    fn drop(&mut self) {
        TEST_HANDLE.with(|c| {
            *c.borrow_mut() = self.prev.take();
        });
    }
}

/// Construct an observer + handle pair for use under the per-task
/// observer override (`Future::with_observer`). The `#[obs::test]`
/// attribute, when applied to an `async fn`, wraps the body in
/// [`scoped_task_handle`] so `assert_emitted!` sees the right handle
/// even when tokio migrates the task between worker threads.
#[must_use]
pub fn new_async_pair() -> (Arc<dyn Observer>, InMemoryHandle) {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    (Arc::new(observer), handle)
}

/// Run an async future with `handle` installed in the per-task slot.
/// Used by `#[obs::test]` to expand to:
///
/// ```ignore
/// let (observer, handle) = obs::test::new_async_pair();
/// obs::test::scoped_task_handle(handle.clone(), async move {
///     /* user body */
/// }.with_observer(observer)).await
/// ```
pub async fn scoped_task_handle<F, R>(handle: InMemoryHandle, fut: F) -> R
where
    F: std::future::Future<Output = R>,
{
    TEST_HANDLE_TASK.scope(handle, fut).await
}

/// Render an envelope's payload as a `serde_json::Map` using the
/// registered schema's `render_json` projection. Used by
/// `assert_emitted!` so payload-class fields (`ATTRIBUTE`,
/// `MEASUREMENT`) participate in matching, not just `LABEL` fields.
/// Spec 60 § 8 / spec 93 P2-16.
#[must_use]
pub fn render_envelope_payload_json(
    env: &ObsEnvelope,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    if env.payload.is_empty() {
        return None;
    }
    let registry = crate::registry::SchemaRegistry::from_link_section();
    let schema = registry.lookup(env)?;
    let mut map = serde_json::Map::new();
    schema.render_json(&env.payload, &mut map).ok()?;
    Some(map)
}

/// Snapshot the current task / thread's captured envelopes without
/// draining. Returns an empty vec when no `#[obs::test]` handle is
/// installed. Spec 60 § 8.
#[must_use]
pub fn snapshot_emitted() -> Vec<ObsEnvelope> {
    if let Ok(h) = TEST_HANDLE_TASK.try_with(InMemoryHandle::clone) {
        return h.snapshot();
    }
    TEST_HANDLE.with(|c| {
        c.borrow()
            .as_ref()
            .map(InMemoryHandle::snapshot)
            .unwrap_or_default()
    })
}

/// Drain the current task / thread's captured envelopes. Returns an
/// empty vec when no `#[obs::test]` handle is installed.
#[must_use]
pub fn take_emitted() -> Vec<ObsEnvelope> {
    if let Ok(h) = TEST_HANDLE_TASK.try_with(InMemoryHandle::clone) {
        return h.drain();
    }
    TEST_HANDLE.with(|c| {
        c.borrow()
            .as_ref()
            .map(InMemoryHandle::drain)
            .unwrap_or_default()
    })
}

/// Internal hook used by `assert_emitted!` so it doesn't need to depend
/// on the inner `Vec<ObsEnvelope>` type directly. Returns true when at
/// least one envelope satisfies the predicate.
#[must_use]
#[doc(hidden)]
pub fn any_emitted_matches(predicate: impl Fn(&ObsEnvelope) -> bool) -> bool {
    snapshot_emitted().iter().any(predicate)
}

/// Pretty-print the captured envelopes for `assert_emitted!` failure
/// diagnostics. Limits output to the first `cap` events to keep the
/// failure noise survivable in CI logs.
#[must_use]
#[doc(hidden)]
pub fn render_emitted(cap: usize) -> String {
    let mut s = String::new();
    for (i, env) in snapshot_emitted().iter().take(cap).enumerate() {
        s.push_str(&format!(
            "  [{i}] {full} labels=",
            i = i,
            full = env.full_name
        ));
        let mut keys: Vec<_> = env.labels.keys().collect();
        keys.sort();
        s.push('{');
        for (j, k) in keys.iter().enumerate() {
            if j > 0 {
                s.push_str(", ");
            }
            if let Some(v) = env.labels.get(*k) {
                s.push_str(&format!("{k}={v}"));
            }
        }
        s.push_str("}\n");
    }
    s
}

/// Re-export of [`crate::assert_emitted`] under the canonical
/// `obs::test::assert_emitted!` path. The `#[macro_export]` attribute
/// places the macro at the crate root by default; callers typing
/// `obs::test::assert_emitted!(...)` expect it under the `test` module
/// alongside the rest of the test ergonomics surface.
#[doc(inline)]
pub use crate::assert_emitted;

/// Partial-match assertion macro. Matches an envelope on its full_name
/// and a subset of its labels. Spec 60 § 8 + spec 72 § 3.
///
/// ```ignore
/// obs::test::assert_emitted!(ObsRequestCompleted {
///     route: "list_users",
///     status: "ok",
///     ..
/// });
/// ```
///
/// The `..` is mandatory and means "ignore every field not named here".
/// Field values may be any expression that evaluates to a value with a
/// `Display` impl — the macro converts them to strings and compares
/// against the envelope's `labels` map (matching the runtime's label
/// projection contract).
///
/// Failure prints the captured envelopes for diagnostics.
#[macro_export]
macro_rules! assert_emitted {
    // Form: `ObsX { f1: v1, f2: v2, .. }` — requires the trailing comma
    // before the `..` rest pattern. The comma is mandatory because
    // `expr` fragments are not allowed to be followed by `..` directly
    // (Rust's macro fragment follow-set rules).
    ($ty:ident { $($field:ident : $value:expr ,)* .. }) => {{
        let __full_name_suffix = ::std::concat!(".", ::std::stringify!($ty));
        let __pairs: &[(&'static str, ::std::string::String)] = &[
            $((::std::stringify!($field), ::std::string::ToString::to_string(&$value))),*
        ];
        let __ok = $crate::test::any_emitted_matches(|env| {
            if !env.full_name.ends_with(__full_name_suffix) {
                return false;
            }
            // Spec 60 § 8 / spec 93 P2-16: match against env.labels
            // (cheap fast path) AND payload fields (decoded via
            // `EventSchemaErased::render_json`) so tests that assert on
            // `ATTRIBUTE`-class fields not promoted to labels still work.
            let __payload_json: ::std::option::Option<
                $crate::__macro_deps::serde_json::Map<
                    String,
                    $crate::__macro_deps::serde_json::Value,
                >,
            > = $crate::test::render_envelope_payload_json(env);
            for (k, v) in __pairs {
                if let ::std::option::Option::Some(actual) = env.labels.get(*k)
                    && actual == v
                {
                    continue;
                }
                if let ::std::option::Option::Some(map) = __payload_json.as_ref()
                    && let ::std::option::Option::Some(json_val) = map.get(*k)
                {
                    let s = match json_val {
                        $crate::__macro_deps::serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    if s == *v {
                        continue;
                    }
                }
                return false;
            }
            true
        });
        if !__ok {
            let __captured = $crate::test::render_emitted(16);
            ::std::panic!(
                "assert_emitted!: no envelope matched `{}` with the supplied label fields.\n\
                 Captured envelopes:\n{}",
                ::std::stringify!($ty),
                __captured,
            );
        }
    }};
    // Empty body form: `ObsX { .. }` — only the type name is matched.
    ($ty:ident { .. }) => {{
        let __full_name_suffix = ::std::concat!(".", ::std::stringify!($ty));
        let __ok = $crate::test::any_emitted_matches(|env| {
            env.full_name.ends_with(__full_name_suffix)
        });
        if !__ok {
            let __captured = $crate::test::render_emitted(16);
            ::std::panic!(
                "assert_emitted!: no envelope matched `{}`.\n\
                 Captured envelopes:\n{}",
                ::std::stringify!($ty),
                __captured,
            );
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        callsite::{EnabledOutcome, ObsCallsite},
        observer::with_test_observer,
    };

    fn synthesize_envelope(full_name: &'static str, labels: &[(&str, &str)]) -> ObsEnvelope {
        let mut env = ObsEnvelope {
            full_name: full_name.to_string(),
            ..Default::default()
        };
        for (k, v) in labels {
            env.labels.insert((*k).to_string(), (*v).to_string());
        }
        env
    }

    #[test]
    fn test_assert_emitted_should_match_partial() {
        let (observer, _handle, _g) = install_thread_handle();
        with_test_observer(observer, || {
            crate::observer::observer().emit_envelope(synthesize_envelope(
                "myapp.v1.ObsRequestCompleted",
                &[("route", "list_users"), ("status", "ok")],
            ));
            assert_emitted!(ObsRequestCompleted {
                route: "list_users",
                ..
            });
        });
    }

    #[test]
    #[should_panic(expected = "no envelope matched")]
    fn test_assert_emitted_should_panic_on_miss() {
        let (observer, _handle, _g) = install_thread_handle();
        with_test_observer(observer, || {
            crate::observer::observer().emit_envelope(synthesize_envelope(
                "myapp.v1.ObsRequestCompleted",
                &[("route", "list_users")],
            ));
            assert_emitted!(ObsRequestCompleted {
                route: "different_route",
                ..
            });
        });
    }

    #[test]
    fn test_assert_emitted_empty_body_should_match_by_type_only() {
        let (observer, _handle, _g) = install_thread_handle();
        with_test_observer(observer, || {
            crate::observer::observer()
                .emit_envelope(synthesize_envelope("myapp.v1.ObsHelloEmitted", &[]));
            assert_emitted!(ObsHelloEmitted { .. });
        });
    }

    #[test]
    fn test_take_emitted_should_drain_thread_handle() {
        let (observer, _handle, _g) = install_thread_handle();
        with_test_observer(observer, || {
            crate::observer::observer()
                .emit_envelope(synthesize_envelope("a.v1.ObsX", &[("k", "v")]));
            let drained = take_emitted();
            assert_eq!(drained.len(), 1);
            // Second drain should be empty.
            assert!(take_emitted().is_empty());
        });
    }

    #[test]
    fn test_callsite_helpers_compile_for_test_module() {
        // Smoke: compile reference to interest cache so that the test
        // build keeps the symbol referenced from the obs::test path.
        let cs = ObsCallsite::new("a.v1.ObsX", crate::Severity::Info, "m", "f", 1);
        let _: EnabledOutcome = cs.enabled(0);
    }
}
