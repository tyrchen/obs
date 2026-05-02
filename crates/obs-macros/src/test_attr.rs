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
//! - **Async test (`async fn`)**: same shape, but wrapped in `#[tokio::test(flavor =
//!   "current_thread")]` so the future runs on the same thread that called `install_thread_handle`.
//!   The per-thread slot is sufficient under the current_thread runtime; the multi-thread path will
//!   switch to the per-task observer slot in Phase 3 task 3.3 (`Future::with_observer`) without
//!   changing user-visible API.
//!
//! Note: install_thread_handle uses one InMemoryObserver and stores
//! that observer's handle in TEST_HANDLE. The expansion threads the
//! SAME observer through `with_test_observer` so `obs::observer()`
//! routes emits into the InMemorySink that the test handle reads.

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
            #[::tokio::test(flavor = "current_thread")]
            #(#user_attrs)*
            #vis #sig {
                let (__obs_observer, __obs_handle, __obs_guard) =
                    ::obs_core::test::install_thread_handle();
                let _g = ::obs_core::with_observer_thread_local(__obs_observer);
                let __obs_result = async move #body.await;
                drop(_g);
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
