//! Canonical obs/v1 protobuf schemas.
//!
//! This crate ships the wire-format types every other crate consumes:
//!
//! - `obs::v1::ObsEnvelope`, `ObsBatch` — the envelope & batch shape (spec 10).
//! - `obs::v1::*` enums — vocabulary mirrors of the [`obs_types`] enums.
//! - `obs::v1::*` events — user-facing built-ins (spec 61 § 2.2).
//! - `obs::runtime::v1::*` events — SDK self-events (spec 11 § 10).
//! - [`BUILTIN_FDS`] — the `FileDescriptorSet` bytes for everything in this crate, embedded at
//!   compile time.
//!
//! The generated buffa code lives under `src/pb/` (checked in, regenerated
//! by `build.rs` on every build).

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility)]
// The generated `pb/` module is large and machine-emitted; we accept that
// it does not satisfy our usual lint set. Restrict the relaxation to that
// module (see the inner `#[allow(...)] mod pb` below).
#![allow(missing_docs, missing_debug_implementations)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

#[allow(
    clippy::all,
    clippy::pedantic,
    clippy::restriction,
    clippy::indexing_slicing,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    missing_docs
)]
mod pb;

pub use pb::*;

/// Bytes of the `FileDescriptorSet` covering every `.proto` file in this
/// crate, captured at build time.
///
/// Used by `obs-core::registry` to discover the built-in schemas without
/// depending on linkme registrations from this crate (which would
/// require user binaries to `use obs_proto as _;`).
pub static BUILTIN_FDS: &[u8] = include_bytes!(env!("OBS_PROTO_FDS"));

/// Re-export buffa traits user code rarely touches but generated code
/// needs in scope.
pub mod __private {
    pub use buffa::{EnumValue, Enumeration, Message, MessageField, UnknownFields};
}

#[cfg(test)]
mod tests {
    use buffa::Message as _;
    use buffa_descriptor::generated::descriptor::FileDescriptorSet;

    use super::*;

    #[test]
    fn test_should_decode_builtin_fds() {
        let fds = FileDescriptorSet::decode_from_slice(BUILTIN_FDS).unwrap();
        let names: Vec<_> = fds.file.iter().filter_map(|f| f.name.as_deref()).collect();
        assert!(names.iter().any(|n| n.ends_with("envelope.proto")));
        assert!(names.iter().any(|n| n.ends_with("builtin.proto")));
        assert!(names.iter().any(|n| n.ends_with("self_events.proto")));
    }

    #[test]
    fn test_should_round_trip_envelope() {
        let env = obs::v1::ObsEnvelope {
            full_name: "obs.v1.ObsHelloEmitted".to_string(),
            schema_hash: 0x1234_5678_9ABC_DEF0,
            ts_ns: 1_700_000_000_000_000_000,
            service: "test".to_string(),
            ..Default::default()
        };
        let mut buf = Vec::new();
        env.encode(&mut buf);
        let decoded = obs::v1::ObsEnvelope::decode_from_slice(&buf).unwrap();
        assert_eq!(decoded.full_name, env.full_name);
        assert_eq!(decoded.schema_hash, env.schema_hash);
        assert_eq!(decoded.service, env.service);
    }
}
