//! `#[obs::test]` — drop-in replacement for `#[test]` /
//! `#[tokio::test]` that installs an `InMemoryObserver` on the current
//! thread (sync) or task (async). Spec 72 § 3.
//!
//! Expansion strategy:
//!
//! - **Sync test (`fn`)**: `install_thread_handle()` returns `(observer, handle, guard)`. The body
//!   runs under `with_test_observer(observer, || #body)`, which routes `obs::observer()` to the
//!   per-thread slot for this thread only.
//!
//! - **Async test (`async fn`)**: same handle install, but the observer is placed in the
//!   **per-task** slot via `with_observer_task(observer, async move { #body }).await`. The per-task
//!   slot follows tokio task migrations across worker threads, which the per-thread slot does not.
//!   Phase 3 task 3.3 lands `WithObserver::with_observer` and `Instrumented<F>` —
//!   `with_observer_task` is the Phase-2 surface that becomes the primitive both compile down to.
//!
//! Note: `install_thread_handle` stores the observer's handle in a
//! thread-local cell that `assert_emitted!` reads. The expansion
//! threads the SAME observer through `with_observer_task` so
//! `obs::observer()` routes emits into the `InMemorySink` that the
//! test handle reads. The thread-local handle is read from whichever
//! tokio worker thread the test's `await` is on at the moment of the
//! `assert_emitted!` macro expansion — under the multi-thread tokio
//! runtime that's the same thread that ran `install_thread_handle`,
//! because `with_observer_task` routes the per-task observer (which
//! delivers events synchronously into the `InMemorySink` shared by
//! observer + handle) regardless of worker.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemFn, parse2};

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "#[obs::test] does not accept arguments",
        ));
    }
    let func: ItemFn = parse2(item)?;
    let attrs = &func.attrs;
    let vis = &func.vis;
    let sig = &func.sig;
    let body = &func.block;
    let is_async = sig.asyncness.is_some();

    // Strip the user's existing #[test] / #[tokio::test] (if any) so we
    // don't double-annotate.
    let user_attrs: Vec<_> = attrs.iter().filter(|a| !attr_is_test(a)).cloned().collect();

    let expanded = if is_async {
        quote! {
            #[::tokio::test]
            #(#user_attrs)*
            #vis #sig {
                let (__obs_observer, __obs_handle, __obs_guard) =
                    ::obs_core::test::install_thread_handle();
                let __obs_result = ::obs_core::with_observer_task(
                    __obs_observer,
                    async move #body,
                ).await;
                drop(__obs_guard);
                let _ = __obs_handle;
                __obs_result
            }
        }
    } else {
        quote! {
            #[::core::prelude::v1::test]
            #(#user_attrs)*
            #vis #sig {
                let (__obs_observer, __obs_handle, __obs_guard) =
                    ::obs_core::test::install_thread_handle();
                let __obs_result = ::obs_core::with_test_observer(
                    __obs_observer,
                    || #body,
                );
                drop(__obs_guard);
                let _ = __obs_handle;
                __obs_result
            }
        }
    };
    Ok(expanded)
}

fn attr_is_test(attr: &syn::Attribute) -> bool {
    let path = attr.path();
    path.is_ident("test")
        || path
            .segments
            .last()
            .is_some_and(|s| s.ident == "test" && path.segments.len() <= 2)
}
