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
//! The generated buffa code lives under `$OUT_DIR` (idiomatic Cargo) and is
//! wired in via `include!(concat!(env!("OUT_DIR"), "/mod.rs"))` below.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility)]
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
mod pb {
    include!(concat!(env!("OUT_DIR"), "/mod.rs"));
}

pub use pb::*;

/// Bytes of the `FileDescriptorSet` covering every `.proto` file in this
/// crate, captured at build time.
///
/// Used by `obs-core::registry` to discover the built-in schemas without
/// depending on linkme registrations from this crate (which would
/// require user binaries to `use obs_proto as _;`).
pub static BUILTIN_FDS: &[u8] = include_bytes!(env!("OBS_PROTO_FDS"));

/// Wire-format version of the `ObsEnvelope` / `ObsBatch` shape.
///
/// Bumped to `2` alongside the move from JSON-payload (Phase-1
/// `#[derive(Event)]`) to buffa-encoded payload bytes for both
/// authoring paths (decision D6-1 in spec 93).
///
/// Bumped to `3` in Phase 7 alongside spec 94 § P0-A:
/// `ObsSpanCompleted` / `ObsSpanEntered` gained typed
/// `trace_id`/`span_id`/`parent_span_id` fields and the bridge
/// switched from raw-byte payloads to buffa-encoded typed payloads
/// (spec 94 § P1-B). Both producers and consumers therefore must
/// agree on the new wire shape. Any further change to the field
/// layout of `obs/v1/envelope.proto` (adding, removing, renumbering,
/// or repurposing fields) requires bumping this constant **and** the
/// corresponding `format_ver` field on every encoder/decoder. The CI
/// guard at `.github/workflows/format-ver-guard.yml` fails any commit
/// that touches `envelope.proto` without also bumping this value.
///
/// Spec 90 § 3.3 / spec 93 § 1 P0-2 + decision D6-1 / spec 94 D7-1.
pub const ENVELOPE_FORMAT_VER: u32 = 3;

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
    fn test_envelope_format_ver_locked_at_three() {
        // Spec 90 § 3.3 / spec 94 D7-1: bumped from 2 to 3 alongside
        // P0-A (typed trace fields on ObsSpanCompleted / ObsSpanEntered)
        // and P1-B (bridge typed-payload encoding). Both producers and
        // consumers must agree on the new wire shape. The CI guard at
        // `.github/workflows/format-ver-guard.yml` forces a bump on any
        // `envelope.proto` edit; this assertion is the second line of
        // defence so a forced merge cannot quietly desync the const
        // from the proto.
        assert_eq!(ENVELOPE_FORMAT_VER, 3);
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
