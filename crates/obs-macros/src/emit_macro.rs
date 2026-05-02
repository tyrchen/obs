//! `obs::emit!(MyEvent { … })` — terse shorthand for the builder
//! pattern. Spec 13 § 1.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Expr, Token, parse2};

pub(crate) fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    // Two forms supported:
    //   1. `MyEvent { … }`            — emit at default severity.
    //   2. `Severity::Warn, MyEvent { … }` — emit at the supplied severity.
    let parsed: EmitInput = parse2(input)?;
    let out = match parsed {
        EmitInput::Default(expr) => quote! {
            ::obs_core::Emit::emit(#expr)
        },
        EmitInput::WithSev(sev, expr) => quote! {
            ::obs_core::Emit::emit_at(#expr, #sev)
        },
    };
    Ok(out)
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
