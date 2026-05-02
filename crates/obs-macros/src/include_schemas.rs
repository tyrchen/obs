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

    // We wrap the includes in a private module so that the workspace's
    // strict clippy bans (`clippy::indexing_slicing`,
    // `clippy::disallowed_*`, etc.) do not fire on the buffa-generated
    // wire-type code. The user re-exports the generated `myapp::v1::*`
    // module via `pub use __obs_generated::*` so call-site paths read
    // identically (`myapp::v1::ObsXxx`). Spec 12 § 3.1.
    Ok(quote! {
        #[allow(
            clippy::all,
            clippy::pedantic,
            clippy::restriction,
            clippy::indexing_slicing,
            clippy::expect_used,
            clippy::unwrap_used,
            clippy::panic,
            clippy::disallowed_methods,
            clippy::disallowed_types,
            non_camel_case_types,
            non_snake_case,
            dead_code,
            unused_imports,
            missing_docs,
        )]
        mod __obs_generated {
            // Stage 1: buffa-build wire types. `obs_buffa.rs` is the
            // entry file emitted via `include_file("obs_buffa.rs")`;
            // it brings every proto package's nested `pub mod` into
            // scope.
            ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/obs_buffa.rs"));
            // Stage 2: obs codegen, including `impl EventSchema for ObsXxx`
            // which references the wire types loaded just above.
            ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/obs/schemas.rs"));
            ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/obs/builders.rs"));
            ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/obs/lints.rs"));
            ::std::include!(::std::concat!(::std::env!("OUT_DIR"), "/obs/arrow_schema.rs"));
        }
        #[allow(unused_imports)]
        pub use __obs_generated::*;
    })
}
