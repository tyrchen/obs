//! Wire-format helpers for obs.
//!
//! - [`BuffaEncodeField`] / field-level encoding helpers for `#[derive(Event)]` codegen
//!   ([`fields`]). Spec 12 § 1.2.
//! - [`envelope_codec`] — length-prefixed envelope framing for stream transports (vsock, unix
//!   socket, TCP). Boundary-review § 3.5.

pub mod envelope_codec;
pub mod fields;

pub use fields::BuffaEncodeField;
