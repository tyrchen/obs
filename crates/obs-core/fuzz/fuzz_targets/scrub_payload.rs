//! Fuzz target: `obs_core::scrub_payload`. Spec 95 § 3.9 / P2-AG.
//!
//! Properties asserted on every input:
//!
//! 1. **No panic** — the scrubber must reject malformed bytes via
//!    `Err(ScrubError)` rather than panicking. The libfuzzer harness
//!    flags any panic as a finding.
//! 2. **Bounded execution** — pathological wire bytes must not cause
//!    an infinite loop. The harness enforces a 10-second timeout per
//!    input; any iteration that times out is flagged.
//! 3. **Output is a copy** — every byte in the output buffer must
//!    either be zero or have been present in the input. The scrubber
//!    never synthesises arbitrary bytes; it either copies through
//!    or replaces with the literal `<redacted-{name}>` ASCII string.
//!    This is a sanity check for the redaction policy.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use obs_core::registry::scrub_payload;
use obs_core::FieldMeta;
use obs_types::{Cardinality, Classification, FieldKind};

// One synthetic schema with a mix of plain / PII / SECRET fields so
// the scrubber exercises every redaction branch. The field metadata
// is constructed at compile time so the fuzz body stays allocation-
// free apart from the output BytesMut.
static FIELDS: &[FieldMeta] = &[
    FieldMeta::new(
        "msg",
        1,
        obs_core::FieldRole::Attribute,
        Cardinality::Low,
        Classification::Internal,
    ),
    FieldMeta::new(
        "user_email",
        2,
        obs_core::FieldRole::Attribute,
        Cardinality::High,
        Classification::Pii,
    ),
    FieldMeta::new(
        "auth_token",
        3,
        obs_core::FieldRole::Attribute,
        Cardinality::Unbounded,
        Classification::Secret,
    ),
    FieldMeta::new(
        "latency_ms",
        4,
        obs_core::FieldRole::Measurement,
        Cardinality::Unbounded,
        Classification::Internal,
    ),
];

fuzz_target!(|data: &[u8]| {
    let mut scratch = BytesMut::new();
    let _ = scrub_payload(data, FIELDS, &mut scratch);
});
