//! `obs::scope!` and `obs::context!` macros. Spec 13 § 2.
//!
//! Both expand to a let-bound `ScopeGuard`. Each named field becomes
//! one [`ScopeField`] entry on the constructed frame.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Expr, Ident, Token,
    parse::{Parse, ParseStream},
    parse2,
    punctuated::Punctuated,
};

pub(crate) fn expand_scope(input: TokenStream) -> syn::Result<TokenStream> {
    let parsed: ScopeInput = parse2(input)?;
    let fields = parsed.field_tokens();
    Ok(quote! {{
        let __obs_fields: ::std::vec::Vec<::obs_core::__private::ScopeField> = ::std::vec![
            #(#fields),*
        ];
        ::obs_core::__private::ScopeGuard::enter(__obs_fields, 64u16)
    }})
}

pub(crate) fn expand_context(input: TokenStream) -> syn::Result<TokenStream> {
    let parsed: ScopeInput = parse2(input)?;
    let fields = parsed.field_tokens();
    Ok(quote! {{
        let __obs_fields: ::std::vec::Vec<::obs_core::__private::ScopeField> = ::std::vec![
            #(#fields),*
        ];
        ::obs_core::__private::ScopeGuard::enter_context(__obs_fields)
    }})
}

struct ScopeInput {
    fields: Vec<(Ident, Expr)>,
}

impl ScopeInput {
    fn field_tokens(&self) -> Vec<TokenStream> {
        self.fields
            .iter()
            .map(|(ident, expr)| {
                let name = ident.to_string();
                match name.as_str() {
                    "trace_id" => quote! {
                        ::obs_core::__private::ScopeField::TraceId(
                            ::std::string::ToString::to_string(&(#expr))
                        )
                    },
                    "span_id" => quote! {
                        ::obs_core::__private::ScopeField::SpanId(
                            ::std::string::ToString::to_string(&(#expr))
                        )
                    },
                    "parent_span_id" => quote! {
                        ::obs_core::__private::ScopeField::ParentSpanId(
                            ::std::string::ToString::to_string(&(#expr))
                        )
                    },
                    _ => {
                        let label_name = name.clone();
                        quote! {
                            ::obs_core::__private::ScopeField::Label(
                                #label_name,
                                ::std::string::ToString::to_string(&(#expr))
                            )
                        }
                    }
                }
            })
            .collect()
    }
}

impl Parse for ScopeInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let pairs: Punctuated<FieldAssign, Token![,]> = Punctuated::parse_terminated(input)?;
        Ok(Self {
            fields: pairs.into_iter().map(|f| (f.name, f.value)).collect(),
        })
    }
}

struct FieldAssign {
    name: Ident,
    value: Expr,
}

impl Parse for FieldAssign {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        let _: Token![=] = input.parse()?;
        let value: Expr = input.parse()?;
        Ok(Self { name, value })
    }
}
