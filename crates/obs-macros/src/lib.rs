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
