//! `obs::include_schemas!("myapp.v1")` — wires up every codegen output
//! file produced by `obs_build::Config::compile()` into the user's
//! crate. Spec 12 § 3.1.
//!
//! The macro expands to four `include!` calls that pull in the four
//! files `obs-build` emits under `$OUT_DIR/obs/`:
//!
//! ```ignore
//! include!(concat!(env!("OUT_DIR"), "/obs/schemas.rs"));
//! include!(concat!(env!("OUT_DIR"), "/obs/builders.rs"));
//! include!(concat!(env!("OUT_DIR"), "/obs/lints.rs"));
//! include!(concat!(env!("OUT_DIR"), "/obs/arrow_schema.rs"));
//! ```
//!
//! The `<package>` argument (e.g. `"myapp.v1"`) is currently informational
//! — `obs-build` emits a single file per kind, not one-per-package.
//! Future revisions of the codegen may emit per-package directories
//! (`$OUT_DIR/obs/myapp.v1/schemas.rs`); this macro would gain a `match`
//! arm at that point. The argument is required so user crates that
//! adopt the per-package layout in a future SDK version do not have to
//! rewrite every call site.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{LitStr, parse2};

pub(crate) fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let _package: LitStr = parse2(input).map_err(|_| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "obs::include_schemas! requires a single string-literal package name, e.g. \
             `obs::include_schemas!(\"myapp.v1\")`",
        )
    })?;

    Ok(quote! {
        ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/obs/schemas.rs"));
        ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/obs/builders.rs"));
        ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/obs/lints.rs"));
        ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/obs/arrow_schema.rs"));
    })
}
