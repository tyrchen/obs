#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
// Proc-macro crate: runs at build time only, so the runtime hot-path
// lints are not relevant here.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic
)]

//! Procedural macros for the obs SDK.
//!
//! Phase-1 surface (impl-plan task 1.9):
//!
//! - [`Event`] (`#[derive(Event)]`) — emits `EventSchema` impl, `EventSchemaErased` impl,
//!   `linkme::distributed_slice` registration, typed builder, and the const-eval lint block
//!   (L001/L002/L003/L011).
//! - [`emit`] — terse `obs::emit!(MyEvent { … })` shorthand.
//! - [`scope`] — placeholder (full impl in Phase 3 task 3.3).
//!
//! See spec 12 § 1.2 (Rust-first authoring) and spec 13 § 2 (`obs::scope!`).

use proc_macro::TokenStream;

mod derive_event;
mod emit_macro;
mod forensic_macro;
mod include_schemas;
mod instrument_attr;
mod scope_macro;
mod test_attr;

/// Derive macro for the Rust-first authoring path.
///
/// Container attributes:
///
/// - `#[event(tier = "log" | "metric" | "trace" | "audit")]`
/// - `#[event(default_sev = "trace" | "debug" | "info" | "warn" | "error" | "fatal")]`
/// - `#[event(full_name = "myapp.v1.ObsXxx")]` (defaults to `<crate>.v1.<TypeName>` derived from
///   `module_path!`).
///
/// Field attributes:
///
/// - `#[obs(label, cardinality = "low" | "medium" | "high" | "unbounded")]`
/// - `#[obs(attribute, classification = "internal" | "pii" | "secret")]`
/// - `#[obs(measurement)]`
/// - `#[obs(trace_id)]`, `#[obs(span_id)]`, `#[obs(parent_span_id)]`
/// - `#[obs(forensic)]`
///
/// Lints (compile-time `const _: () = { assert!(...) }` blocks):
///
/// - **L001** — every `LABEL` field must declare a `Low` or `Medium` cardinality.
/// - **L002** — `PII`-classified fields must not be `LABEL`.
/// - **L003** — `SECRET`-classified fields must not exist on `LOG` or `AUDIT` tier events.
/// - **L011** — the type name must start with the workspace event prefix (default `Obs`).
///
/// See spec 12 § 3.4.
#[proc_macro_derive(Event, attributes(event, obs))]
pub fn derive_event(item: TokenStream) -> TokenStream {
    derive_event::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Function-like emit macro: `obs::emit!(MyEvent { field: value })`
/// or `obs::emit!(WARN, MyEvent { field: value })` to escalate.
///
/// Spec 13 § 1.
#[proc_macro]
pub fn emit(item: TokenStream) -> TokenStream {
    emit_macro::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `obs::include_schemas!("myapp.v1")` — wire up every file
/// `obs-build` emits into the user's crate. Expands to four
/// `include!` calls under `OUT_DIR/obs/`. Spec 12 § 3.1.
#[proc_macro]
pub fn include_schemas(item: TokenStream) -> TokenStream {
    include_schemas::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `obs::scope!(name = value, ...)` — push an `obs::scope!` frame
/// onto the active task's scope stack. Returns a `ScopeGuard` that
/// pops the frame on drop and flushes the tail-on-error buffer when
/// `>= ERROR` was observed inside.
///
/// Spec 13 § 2.
#[proc_macro]
pub fn scope(item: TokenStream) -> TokenStream {
    scope_macro::expand_scope(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `obs::context!(name = value, ...)` — like `obs::scope!` but without
/// the per-scope tail buffer. Spec 13 § 2.2.
#[proc_macro]
pub fn context(item: TokenStream) -> TokenStream {
    scope_macro::expand_context(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `obs::forensic!(site = "...", message = "...", { "k" => v, ... })`
/// — emergency escape hatch. Always emits, regardless of sampling.
/// Spec 13 § 8.
#[proc_macro]
pub fn forensic(item: TokenStream) -> TokenStream {
    forensic_macro::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `#[obs::instrument]` — wraps a function body in an `obs::scope!`
/// and emits one `ObsFnExecuted` event on exit (default) or two
/// (`ObsFnEntered` + `ObsFnExecuted`) when `enter = true`.
///
/// Spec 13 § 5.
#[proc_macro_attribute]
pub fn instrument(attr: TokenStream, item: TokenStream) -> TokenStream {
    instrument_attr::expand(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `#[obs::test]` — drop-in replacement for `#[test]` /
/// `#[tokio::test]` that installs an [`InMemoryObserver`] on the
/// current thread (sync) or current task (async) for the duration of
/// the test. The body's emits land in a thread-local /
/// task-local handle that [`obs::test::assert_emitted!`] reads.
///
/// Sync example:
///
/// ```ignore
/// #[obs::test]
/// fn login_emits_event() -> anyhow::Result<()> {
///     login("alice")?;
///     obs::test::assert_emitted!(ObsLoggedIn { user: "alice", .. });
///     Ok(())
/// }
/// ```
///
/// Async example:
///
/// ```ignore
/// #[obs::test]
/// async fn billing_emits_charge_event() -> anyhow::Result<()> {
///     charge_card("4242…").await?;
///     obs::test::assert_emitted!(ObsChargeAttempted { outcome: "approved", .. });
///     Ok(())
/// }
/// ```
///
/// Spec 60 § 8 + spec 72 § 3.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test_attr::expand(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
