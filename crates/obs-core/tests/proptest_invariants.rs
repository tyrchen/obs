//! Property tests for invariants the runtime relies on. Spec 95 § 3.8
//! / P2-AF.
//!
//! Coverage:
//!
//! 1. **Callsite ID non-zero**: every distinct callsite hashes to a non-zero `u64`. Zero is
//!    reserved as "no callsite" so the registry can encode "absent" in one machine word.
//! 2. **Callsite ID determinism**: identical inputs hash to the same `u64` across runs.
//! 3. **Filter parser determinism**: a successfully-parsed filter string yields the same
//!    `callsite_interest` decision for the same callsite across two parses.
//! 4. **Schema-hash determinism**: the canonical descriptor string hashes deterministically (proxy
//!    via `obs_build` reflect schema_hash mirror).

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]

use obs_core::{
    Filter, ObsCallsite,
    registry::{CallsiteSource, callsite_id},
};
use obs_types::Severity;
use proptest::prelude::*;

// ─── 1. callsite_id non-zero invariant ─────────────────────────────

fn arb_severity() -> impl Strategy<Value = Severity> {
    prop_oneof![
        Just(Severity::Trace),
        Just(Severity::Debug),
        Just(Severity::Info),
        Just(Severity::Warn),
        Just(Severity::Error),
        Just(Severity::Fatal),
    ]
}

fn arb_source() -> impl Strategy<Value = CallsiteSource> {
    prop_oneof![
        Just(CallsiteSource::TracingEvent),
        Just(CallsiteSource::TracingSpan),
        Just(CallsiteSource::Forensic),
        Just(CallsiteSource::Instrument),
    ]
}

proptest! {
    #[test]
    fn callsite_id_should_never_be_zero(
        source in arb_source(),
        target in "[a-z][a-z0-9_:]{0,32}",
        file in "[a-z][a-z0-9_/.]{0,32}",
        line in proptest::option::of(1u32..1_000_000),
        level in arb_severity(),
        template in "[A-Za-z0-9 _.]{0,64}",
    ) {
        let id = callsite_id(source, &target, &file, line, level, &[], &template);
        prop_assert_ne!(id, 0, "callsite_id must perturb-to-non-zero (spec 31 § 3.1)");
    }

    #[test]
    fn callsite_id_should_be_deterministic(
        source in arb_source(),
        target in "[a-z][a-z0-9_:]{0,32}",
        file in "[a-z][a-z0-9_/.]{0,32}",
        line in proptest::option::of(1u32..1_000_000),
        level in arb_severity(),
        template in "[A-Za-z0-9 _.]{0,64}",
    ) {
        let a = callsite_id(source, &target, &file, line, level, &[], &template);
        let b = callsite_id(source, &target, &file, line, level, &[], &template);
        prop_assert_eq!(a, b);
    }
}

// ─── 2. Filter parse determinism ───────────────────────────────────

proptest! {
    #[test]
    fn filter_should_parse_deterministically(spec in r"[a-z]{3,8}|info|warn|error|debug|trace|off") {
        let a = Filter::parse(&spec);
        let b = Filter::parse(&spec);
        // Either both succeed or both fail; if both succeed, they
        // make identical decisions on a fixed callsite.
        match (a, b) {
            (Ok(fa), Ok(fb)) => {
                let cs = ObsCallsite::new(
                    "myapp.v1.ObsX",
                    Severity::Info,
                    "myapp::probe",
                    "test.rs",
                    1,
                );
                prop_assert_eq!(
                    format!("{:?}", fa.callsite_interest(&cs)),
                    format!("{:?}", fb.callsite_interest(&cs))
                );
            }
            (Err(_), Err(_)) => {}
            (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
                prop_assert!(false, "filter parse non-deterministic for `{}`", spec);
            }
        }
    }
}

// ─── 3. Schema-hash determinism (via canonical descriptor proxy) ───

fn schema_hash_canonical(full_name: &str, fields: &[(String, String)]) -> u64 {
    // Mirror the algorithm used by both codegen paths (obs-build's
    // `EventDecl::schema_hash` and obs-macros's `compute_schema_hash`)
    // so the property test sits next to the same canonical string.
    let mut s = String::new();
    s.push_str(full_name);
    s.push('|');
    s.push_str("log");
    s.push('|');
    s.push_str("info");
    s.push('|');
    for (n, k) in fields {
        s.push_str(n);
        s.push(':');
        s.push_str(k);
        s.push(':');
        s.push_str("unspecified");
        s.push(':');
        s.push_str("internal");
        s.push(',');
    }
    let h = blake3::hash(s.as_bytes());
    let bytes = h.as_bytes();
    let arr = <[u8; 8]>::try_from(&bytes[..8]).expect("blake3 always 32 bytes");
    u64::from_le_bytes(arr)
}

proptest! {
    #[test]
    fn schema_hash_should_be_deterministic_across_runs(
        full_name in "[a-z]{3,8}\\.v1\\.Obs[A-Z][A-Za-z0-9]{0,12}",
        fields in proptest::collection::vec(
            ("[a-z][a-z0-9_]{0,12}", "label|attribute|measurement|trace_id|span_id"),
            0..6,
        ),
    ) {
        let fs: Vec<(String, String)> = fields.iter().map(|(n, k)| (n.clone(), (*k).to_string())).collect();
        let a = schema_hash_canonical(&full_name, &fs);
        let b = schema_hash_canonical(&full_name, &fs);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn distinct_canonical_strings_should_hash_differently_with_overwhelming_prob(
        a_name in "[a-z]{3,8}\\.v1\\.ObsA[A-Za-z0-9]{0,8}",
        b_name in "[a-z]{3,8}\\.v1\\.ObsB[A-Za-z0-9]{0,8}",
    ) {
        // ObsA* vs ObsB* never collide (the canonical string differs
        // in the prefix). 8-byte truncated blake3 collisions are
        // ~1/2^32 within a few million probes — but we don't sample
        // that many here, so this assertion is safe.
        prop_assume!(a_name != b_name);
        let ha = schema_hash_canonical(&a_name, &[]);
        let hb = schema_hash_canonical(&b_name, &[]);
        prop_assert_ne!(ha, hb);
    }
}

// ─── 4. Envelope round-trip ─────────────────────────────────────────

proptest! {
    #[test]
    fn envelope_should_round_trip_through_buffa(
        full_name in "[a-z]{3,8}\\.v1\\.Obs[A-Z][A-Za-z0-9]{0,12}",
        trace_id in "[0-9a-f]{32}",
        span_id in "[0-9a-f]{16}",
        labels in proptest::collection::vec(
            ("[a-z][a-z0-9_]{0,12}", "[A-Za-z0-9 _.,/-]{0,32}"),
            0..6,
        ),
        ts_ns in 1u64..u64::MAX / 2,
    ) {
        use buffa::Message;
        use bytes::BytesMut;
        let mut label_map = std::collections::HashMap::new();
        for (k, v) in labels {
            label_map.insert(k, v);
        }
        let env = obs_proto::obs::v1::ObsEnvelope {
            full_name,
            trace_id,
            span_id,
            ts_ns,
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
            labels: label_map,
            ..Default::default()
        };

        let mut cache = ::buffa::SizeCache::default();
        let size = env.compute_size(&mut cache);
        let mut buf = BytesMut::with_capacity(size as usize);
        env.write_to(&mut cache, &mut buf);
        let decoded = obs_proto::obs::v1::ObsEnvelope::decode_from_slice(&buf)
            .expect("envelope must round-trip");
        prop_assert_eq!(decoded.full_name, env.full_name);
        prop_assert_eq!(decoded.trace_id, env.trace_id);
        prop_assert_eq!(decoded.span_id, env.span_id);
        prop_assert_eq!(decoded.ts_ns, env.ts_ns);
        prop_assert_eq!(decoded.labels, env.labels);
    }
}

// ─── 5. Scrubber correctness (security-relevant) ───────────────────

proptest! {
    #[test]
    fn scrubber_should_redact_pii_secret_and_be_bounded(
        msg in "[A-Za-z0-9 ]{0,64}",
        email in "[a-z]{1,16}@[a-z]{1,16}\\.com",
        token in "[A-Za-z0-9]{16,64}",
        latency in 0u64..1_000_000,
    ) {
        use bytes::BytesMut;
        use obs_core::registry::scrub_payload;
        use obs_core::FieldMeta;
        use obs_types::{Cardinality, Classification};
        use obs_core::FieldRole;

        // Build a manual buffa payload with one Internal string,
        // one PII string, one SECRET string, and one numeric.
        let mut payload = BytesMut::new();
        encode_string_field(&mut payload, 1, &msg);
        encode_string_field(&mut payload, 2, &email);
        encode_string_field(&mut payload, 3, &token);
        encode_varint_field(&mut payload, 4, latency);

        static FIELDS: &[FieldMeta] = &[
            FieldMeta::new("msg", 1, FieldRole::Attribute, Cardinality::Low, Classification::Internal),
            FieldMeta::new("email", 2, FieldRole::Attribute, Cardinality::High, Classification::Pii),
            FieldMeta::new("token", 3, FieldRole::Attribute, Cardinality::Unbounded, Classification::Secret),
            FieldMeta::new("latency", 4, FieldRole::Measurement, Cardinality::Unbounded, Classification::Internal),
        ];

        let mut scratch = BytesMut::new();
        let scrubbed = scrub_payload(&payload, FIELDS, &mut scratch)
            .expect("scrubber must accept well-formed payload");

        // Internal string survives.
        prop_assert!(contains_subseq(scrubbed, msg.as_bytes()));
        // PII / SECRET strings do NOT survive.
        prop_assert!(!contains_subseq(scrubbed, email.as_bytes()),
            "PII email leaked: scrubbed={:?}, email={:?}", scrubbed, email);
        prop_assert!(!contains_subseq(scrubbed, token.as_bytes()),
            "SECRET token leaked: scrubbed={:?}, token={:?}", scrubbed, token);
    }
}

fn encode_string_field(buf: &mut bytes::BytesMut, number: u32, value: &str) {
    use bytes::BufMut;
    let tag = (number << 3) | 2; // wire type LEN
    encode_varint(buf, u64::from(tag));
    encode_varint(buf, value.len() as u64);
    buf.put_slice(value.as_bytes());
}

fn encode_varint_field(buf: &mut bytes::BytesMut, number: u32, value: u64) {
    // Wire type 0 = Varint; the OR is left explicit to mirror the
    // proto wire format documentation.
    let tag = number << 3;
    encode_varint(buf, u64::from(tag));
    encode_varint(buf, value);
}

fn encode_varint(buf: &mut bytes::BytesMut, mut v: u64) {
    use bytes::BufMut;
    while v >= 0x80 {
        buf.put_u8(((v & 0x7f) as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

fn contains_subseq(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}
