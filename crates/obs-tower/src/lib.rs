#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
#![allow(
    clippy::collapsible_if,
    clippy::collapsible_match,
    clippy::redundant_closure,
    clippy::indexing_slicing
)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! `tower::Layer` companion for HTTP services. Spec 40.

mod client;
mod propagator;
mod server;

pub use client::{ObsHttpClientLayer, ObsHttpClientService};
pub use propagator::{TraceContext, W3cPropagator, status_class};
pub use server::{ObsHttpLayer, ObsHttpService};
