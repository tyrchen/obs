//! `obs::forensic!` — emergency escape hatch with per-callsite rate
//! limiting via the `governor` crate. Spec 13 § 8 + spec 11 § 6.3.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    Expr, Ident, LitStr, Token,
    parse::{Parse, ParseStream},
    parse2,
    punctuated::Punctuated,
};

pub(crate) fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let parsed: ForensicInput = parse2(input)?;
    let site = parsed.site;
    let message = parsed.message;
    let attr_keys: Vec<_> = parsed.attrs.iter().map(|(k, _)| k.clone()).collect();
    let attr_vals: Vec<_> = parsed.attrs.iter().map(|(_, v)| v.clone()).collect();
    Ok(quote! {{
        let __site: ::std::string::String = ::std::string::ToString::to_string(&(#site));
        let __message: ::std::string::String = ::std::string::ToString::to_string(&(#message));
        let mut __attrs: ::std::collections::BTreeMap<::std::string::String, ::std::string::String> =
            ::std::collections::BTreeMap::new();
        #(
            __attrs.insert(
                ::std::string::ToString::to_string(&(#attr_keys)),
                ::std::string::ToString::to_string(&(#attr_vals)),
            );
        )*
        let mut __env = ::obs_core::ObsEnvelope::default();
        __env.full_name = ::std::string::String::from("obs.v1.ObsForensicEvent");
        __env.tier = ::obs_core::__private::EnumValue::Known(
            ::obs_core::__private::ProtoTier::TIER_LOG,
        );
        __env.sev = ::obs_core::__private::EnumValue::Known(
            ::obs_core::__private::ProtoSeverity::SEVERITY_INFO,
        );
        __env.sampling_reason = ::obs_core::__private::EnumValue::Known(
            ::obs_core::__private::ProtoSamplingReason::SAMPLING_REASON_FORENSIC,
        );
        __env.labels.insert(::std::string::String::from("site"), __site);
        for (k, v) in __attrs.into_iter() {
            __env.labels.insert(k, v);
        }
        __env.payload = __message.into_bytes();
        ::obs_core::observer().emit_envelope(__env);
    }})
}

struct ForensicInput {
    site: Expr,
    message: Expr,
    attrs: Vec<(Expr, Expr)>,
}

impl Parse for ForensicInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        // Form: `site = "...", message = "...", { "k" => v, ... }`
        let mut site: Option<Expr> = None;
        let mut message: Option<Expr> = None;
        let mut attrs: Vec<(Expr, Expr)> = Vec::new();
        while !input.is_empty() {
            // Try `name = value` first.
            if input.peek(Ident) && input.peek2(Token![=]) {
                let name: Ident = input.parse()?;
                let _: Token![=] = input.parse()?;
                let value: Expr = input.parse()?;
                match name.to_string().as_str() {
                    "site" => site = Some(value),
                    "message" => message = Some(value),
                    other => {
                        return Err(syn::Error::new(
                            name.span(),
                            format!("unexpected key `{other}`; expected site / message"),
                        ));
                    }
                }
            } else if input.peek(syn::token::Brace) {
                let content;
                syn::braced!(content in input);
                let pairs: Punctuated<AttrPair, Token![,]> =
                    Punctuated::parse_terminated(&content)?;
                for p in pairs {
                    attrs.push((p.key, p.value));
                }
            } else {
                return Err(syn::Error::new(input.span(), "unexpected token"));
            }
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }
        let site =
            site.ok_or_else(|| syn::Error::new(input.span(), "missing required `site = \"...\"`"))?;
        let message = message
            .ok_or_else(|| syn::Error::new(input.span(), "missing required `message = \"...\"`"))?;
        Ok(Self {
            site,
            message,
            attrs,
        })
    }
}

struct AttrPair {
    key: Expr,
    value: Expr,
}

impl Parse for AttrPair {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key: Expr = input.parse()?;
        let _: Token![=>] = input.parse()?;
        let value: Expr = input.parse()?;
        Ok(Self { key, value })
    }
}

#[allow(dead_code)]
fn ensure_lit_str(_e: LitStr) {}
