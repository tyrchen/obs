//! W3C Trace Context propagation. Spec 20 § 2.6 / spec 40 § 1.
//!
//! Thin re-export of [`obs_core::propagator`]. Spec 93 P1-5 moved the
//! single source of truth for the W3C parser into `obs-core` so that
//! every middleware, sink, and example consumes the same code path.

pub use obs_core::propagator::{
    ObsTraceCtx as TraceContext, W3cPropagator, fresh_span_id, fresh_trace_id, status_class,
};
#[allow(unused_imports)] // re-exported for downstream consumers
pub use obs_core::propagator::{extract_w3c, inject_w3c};
