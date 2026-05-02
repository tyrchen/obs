//! `obs::emit!(MyEvent { … })` — terse shorthand for the builder
//! pattern. Spec 13 § 1.
//!
//! The macro extracts the event-type path from the struct-literal
//! expression so it can inline a `static __CALLSITE: ObsCallsite`
//! whose `FULL_NAME` / `DEFAULT_SEV` come from the schema's associated
//! constants. Without that, the atomic-Interest cache (spec 11 § 2.1)
//! cannot short-circuit per-call site.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Expr, ExprStruct, Path, Token, parse2};

pub(crate) fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let parsed: EmitInput = parse2(input)?;
    let (sev_override, expr) = match parsed {
        EmitInput::Default(expr) => (None, expr),
        EmitInput::WithSev(sev, expr) => (Some(sev), expr),
    };

    let path = extract_struct_path(&expr).ok_or_else(|| {
        syn::Error::new_spanned(
            &expr,
            "obs::emit! requires a struct-literal expression like `MyEvent { ... }`",
        )
    })?;

    let sev_expr = sev_override.map_or_else(
        || quote!(<#path as ::obs_core::EventSchema>::DEFAULT_SEV),
        |s| quote!(#s),
    );

    Ok(quote! {{
        let __evt = #expr;
        static __CALLSITE: ::obs_core::__private::ObsCallsite =
            ::obs_core::__private::ObsCallsite::new(
                <#path as ::obs_core::EventSchema>::FULL_NAME,
                <#path as ::obs_core::EventSchema>::DEFAULT_SEV,
                module_path!(),
                file!(),
                line!(),
            );
        ::obs_core::emit::emit_with_callsite::<#path>(&__CALLSITE, &__evt, #sev_expr);
    }})
}

fn extract_struct_path(expr: &Expr) -> Option<Path> {
    if let Expr::Struct(ExprStruct { path, .. }) = expr {
        return Some(path.clone());
    }
    None
}

enum EmitInput {
    Default(Expr),
    WithSev(Expr, Expr),
}

impl syn::parse::Parse for EmitInput {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let first: Expr = input.parse()?;
        if input.peek(Token![,]) {
            let _comma: Token![,] = input.parse()?;
            let second: Expr = input.parse()?;
            return Ok(Self::WithSev(first, second));
        }
        Ok(Self::Default(first))
    }
}
