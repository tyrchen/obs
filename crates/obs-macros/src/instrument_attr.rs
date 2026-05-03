//! `#[obs::instrument]` attribute macro. Spec 13 § 5.
//!
//! Default expansion (one event, on exit):
//!
//! ```ignore
//! async fn handle(req: Request) -> Response {
//!     let _scope = obs::scope!(/* declared fields */);
//!     let __started = std::time::Instant::now();
//!     let __res = async move { /* original body */ }.await;
//!     obs::emit!(ObsFnExecuted {
//!         fn_name: "handle",
//!         latency_ns: __started.elapsed().as_nanos() as u64,
//!     });
//!     __res
//! }
//! ```

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Expr, Ident, ItemFn, LitStr, Meta, Token,
    parse::{Parse, ParseStream},
    parse2,
    punctuated::Punctuated,
};

pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let cfg: InstrumentArgs = if attr.is_empty() {
        InstrumentArgs::default()
    } else {
        parse2(attr)?
    };
    let func: ItemFn = parse2(item)?;
    let attrs = &func.attrs;
    let vis = &func.vis;
    let sig = &func.sig;
    let body = &func.block;
    let is_async = sig.asyncness.is_some();
    let fn_name = sig.ident.to_string();

    let scope_fields = cfg
        .fields
        .iter()
        .map(|field| {
            quote! { #field = #field }
        })
        .collect::<Vec<_>>();
    let scope_setup = if scope_fields.is_empty() {
        quote!()
    } else {
        quote! { let __obs_scope = ::obs_macros::scope!(#(#scope_fields,)*); }
    };

    let emit_entered = if cfg.enter {
        quote! {
            ::obs_macros::emit!(::obs_sdk::ObsFnEntered {
                fn_name: #fn_name,
            });
        }
    } else {
        quote!()
    };
    let executed_emit = quote! {
        ::obs_macros::emit!(::obs_sdk::ObsFnExecuted {
            fn_name: #fn_name,
            latency_ns: __obs_started.elapsed().as_nanos() as u64,
        });
    };

    let body_expr = if is_async {
        // Spec 13 § 5 / spec 93 P2-8: capture the scope inside the
        // async block so the RAII guard travels with the future
        // across `.await` boundaries. The scope! macro pushes onto a
        // tokio `task_local!` stack, so the frame is task-scoped
        // (not thread-scoped) and survives executor migration. A
        // proper `Instrumented<F>` adapter that also handles
        // `tokio::spawn` is tracked in spec 93 P2-8 follow-up.
        quote! {
            let __obs_started = ::std::time::Instant::now();
            let __obs_res = async move {
                #scope_setup
                #emit_entered
                #body
            }.await;
            #executed_emit
            __obs_res
        }
    } else {
        quote! {
            let __obs_started = ::std::time::Instant::now();
            #scope_setup
            #emit_entered
            let __obs_res = (move || #body)();
            #executed_emit
            __obs_res
        }
    };

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #body_expr
        }
    };
    Ok(expanded)
}

#[derive(Default)]
struct InstrumentArgs {
    fields: Vec<Ident>,
    enter: bool,
    #[allow(dead_code)]
    skip: Vec<Ident>,
}

impl Parse for InstrumentArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut out = InstrumentArgs::default();
        let pairs: Punctuated<Meta, Token![,]> = Punctuated::parse_terminated(input)?;
        for meta in pairs {
            match meta {
                Meta::List(list) if list.path.is_ident("fields") => {
                    let idents: Punctuated<Ident, Token![,]> =
                        list.parse_args_with(Punctuated::parse_terminated)?;
                    out.fields.extend(idents);
                }
                Meta::List(list) if list.path.is_ident("skip") => {
                    let idents: Punctuated<Ident, Token![,]> =
                        list.parse_args_with(Punctuated::parse_terminated)?;
                    out.skip.extend(idents);
                }
                Meta::NameValue(nv) if nv.path.is_ident("enter") => match &nv.value {
                    Expr::Lit(lit) => match &lit.lit {
                        syn::Lit::Bool(b) => out.enter = b.value,
                        _ => {
                            return Err(syn::Error::new_spanned(
                                lit,
                                "`enter` expects a bool literal",
                            ));
                        }
                    },
                    other => {
                        return Err(syn::Error::new_spanned(
                            other,
                            "`enter` expects a bool literal",
                        ));
                    }
                },
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "expected `fields(...)`, `skip(...)`, or `enter = true`",
                    ));
                }
            }
        }
        Ok(out)
    }
}

#[allow(dead_code)]
fn ensure_lit(_l: LitStr) {}
