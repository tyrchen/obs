//! Envelope construction and projection helpers.
//!
//! Spec 10 § 6 defines the wire envelope (`obs.v1.ObsEnvelope`); this
//! module provides the typed `EventSchema` trait that codegen targets,
//! the `Envelope` newtype around the wire envelope (so tests can
//! manipulate it without going through buffa), and the
//! `build_envelope` / project helpers used by the emit hot path
//! (spec 11 § 5).

mod builder;
mod projection;

pub use builder::{Envelope, build_envelope, build_envelope_at};
pub use projection::{EventSchema, FieldMeta, FieldRole};
