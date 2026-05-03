//! Shared lint emission for the proto-first (`obs-build::codegen`) and
//! Rust-first (`obs-macros::derive_event`) authoring paths.
//!
//! Decision D8-1 (spec 95 § 5): both paths build a [`LintInput`] and
//! call [`emit_lints`]. Each [`LintError`] carries a stable code
//! (`L001`..`L014`) and the same human-readable message regardless of
//! which path produced it; the consumer (codegen.rs / derive_event.rs)
//! formats the errors into either Rust source text or `proc_macro2`
//! tokens.
//!
//! This module has no `proc_macro2`/`quote`/`syn` dep so it can be
//! linked from both an ordinary library crate (obs-build's codegen
//! path) and a proc-macro crate (obs-macros's derive path) without
//! pulling each other's heavy transitive deps.

use obs_types::{Cardinality, Classification, FieldKind, Severity, Tier};

/// Proto wire type a field declares. The proto-first path fills this
/// from `FieldDescriptorProto::r#type`; the derive path fills it from
/// the Rust syntactic type via [`LintProtoType::from_rust_token`]. When
/// the type cannot be inferred it is `Other(_)` and the type-checking
/// portion of L014 simply does not fire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LintProtoType {
    /// `string` / `&str` / `String`.
    String,
    /// `bytes` / `Vec<u8>` / `Bytes`.
    Bytes,
    /// Any integer or float scalar.
    Numeric,
    /// `bool`.
    Bool,
    /// Unknown; type-aware lints skip.
    Other(String),
}

impl LintProtoType {
    /// Map a Rust type's `ToTokens` form to a `LintProtoType`. Heuristic
    /// only — covers the common cases (`String`, `Vec<u8>`, integers,
    /// `bool`); anything else lands in `Other(_)` and L014's
    /// type-validation skips.
    #[must_use]
    pub fn from_rust_token(s: &str) -> Self {
        let normalised: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        if normalised == "String"
            || normalised.ends_with("::String")
            || normalised == "&str"
            || normalised.ends_with("::str")
        {
            return Self::String;
        }
        if normalised == "Vec<u8>"
            || normalised.ends_with("::Vec<u8>")
            || normalised == "Bytes"
            || normalised.ends_with("::Bytes")
        {
            return Self::Bytes;
        }
        if matches!(
            normalised.as_str(),
            "bool"
                | "i8"
                | "i16"
                | "i32"
                | "i64"
                | "u8"
                | "u16"
                | "u32"
                | "u64"
                | "f32"
                | "f64"
                | "usize"
                | "isize"
        ) {
            return if normalised == "bool" {
                Self::Bool
            } else {
                Self::Numeric
            };
        }
        Self::Other(normalised)
    }
}

/// One field's worth of lint input.
#[derive(Debug, Clone)]
pub struct LintField {
    /// Field name as authored (`String` for proto, the Rust ident for
    /// the derive path).
    pub name: String,
    /// Effective field kind (defaulted to `Attribute` when absent).
    pub kind: FieldKind,
    /// Effective cardinality.
    pub cardinality: Cardinality,
    /// Effective classification.
    pub classification: Classification,
    /// `true` when a `Measurement`-kind field declares a metric kind
    /// (counter / gauge / histogram). The lint pass only needs the
    /// presence bit to fire L004; the actual kind/unit/bounds are
    /// consumed downstream by the codegen.
    pub has_metric: bool,
    /// Proto/Rust type the field declares. `None` for callers that
    /// cannot infer it; type-aware lints skip in that case.
    pub proto_type: Option<LintProtoType>,
}

/// Input for the shared lint pass — one event's worth.
#[derive(Debug, Clone)]
pub struct LintInput {
    /// Display name used in lint messages (e.g. `"ObsRequestStarted"`).
    pub event_name: String,
    /// Effective tier.
    pub tier: Tier,
    /// Workspace event prefix for L011 (default `"Obs"`).
    pub event_prefix: String,
    /// Fields in proto declaration order.
    pub fields: Vec<LintField>,
}

/// One lint failure.
#[derive(Debug, Clone)]
pub struct LintError {
    /// Stable lint code (`"L001"`..`"L014"`).
    pub code: &'static str,
    /// Human-readable message, multi-line, ready to embed verbatim into
    /// a `panic!("…")` call. The `\n` is preserved.
    pub message: String,
}

impl LintError {
    fn new(code: &'static str, message: String) -> Self {
        Self { code, message }
    }
}

/// Run every lint in the catalogue against one event. Returns one
/// `LintError` per violation, in stable code order so generated output
/// is deterministic. Spec 95 § 2.1 (D8-1).
#[must_use]
pub fn emit_lints(input: &LintInput) -> Vec<LintError> {
    let mut out: Vec<LintError> = Vec::new();
    check_l011(input, &mut out);
    check_l009(input, &mut out);
    for f in &input.fields {
        check_per_field(input, f, &mut out);
    }
    out
}

/// Cross-event lints — currently L013 (schema_hash uniqueness within
/// a codegen unit). The codegen path passes every event's `(full_name,
/// schema_hash)`; the derive path runs once per event so it cannot
/// detect collisions and skips L013.
#[must_use]
pub fn emit_cross_event_lints(events: &[(String, u64)]) -> Vec<LintError> {
    let mut out = Vec::new();
    for (i, a) in events.iter().enumerate() {
        for b in events.iter().skip(i + 1) {
            if a.1 == b.1 {
                let msg = format!(
                    "obs L013: schema_hash collision: `{a}` and `{b}` both hash to \
                     {hash:#018x}.\nhelp: rename one event so the canonical descriptor differs \
                     (any field rename / reorder will do).",
                    a = a.0,
                    b = b.0,
                    hash = a.1,
                );
                out.push(LintError::new("L013", msg));
            }
        }
    }
    out
}

fn check_l011(input: &LintInput, out: &mut Vec<LintError>) {
    if !input.event_name.starts_with(&input.event_prefix) {
        let msg = format!(
            "obs L011: event type name `{name}` must start with `{prefix}`\nnote: the `{prefix}` \
             prefix gives every event type a unique visual identity at call sites.\nhelp: rename \
             to `{prefix}{name}`.",
            name = input.event_name,
            prefix = input.event_prefix,
        );
        out.push(LintError::new("L011", msg));
    }
}

fn check_l009(input: &LintInput, out: &mut Vec<LintError>) {
    if input.fields.is_empty() {
        let msg = format!(
            "obs L009: event `{name}` has no fields\nnote: empty events make analytics joins \
             meaningless and indicate an unfinished schema.\nhelp: declare at least one field or \
             rethink whether the event should exist.",
            name = input.event_name,
        );
        out.push(LintError::new("L009", msg));
    }
}

fn check_per_field(input: &LintInput, f: &LintField, out: &mut Vec<LintError>) {
    // L001: LABEL must be Low or Medium cardinality.
    if matches!(f.kind, FieldKind::Label) && !f.cardinality.is_label_compatible() {
        let msg = format!(
            "obs L001: field `{name}` is LABEL but cardinality is not label-compatible\nnote: \
             LABEL fields must be Low or Medium cardinality. High and Unbounded are illegal \
             because they would explode the metric attribute set.\nhelp: change `kind: LABEL` to \
             `kind: ATTRIBUTE` if the value is high-cardinality (an ATTRIBUTE is logged but never \
             becomes a metric dim).",
            name = f.name,
        );
        out.push(LintError::new("L001", msg));
    }

    // L002: PII fields must not be LABEL.
    if matches!(f.kind, FieldKind::Label) && matches!(f.classification, Classification::Pii) {
        let msg = format!(
            "obs L002: field `{name}` is LABEL with classification PII\nnote: PII fields cannot \
             be LABEL because labels become metric attributes that are kept indefinitely and leak \
             into vendor backends.\nhelp: change kind to ATTRIBUTE so the value is logged + \
             analytics-only, and the redactor can scrub it on the durable path.",
            name = f.name,
        );
        out.push(LintError::new("L002", msg));
    }

    // L003: SECRET on a LOG/AUDIT tier event.
    if matches!(f.classification, Classification::Secret)
        && matches!(input.tier, Tier::Log | Tier::Audit)
    {
        let msg = format!(
            "obs L003: field `{name}` is SECRET on a `{tier}` tier event\nnote: SECRET fields are \
             forbidden on LOG/AUDIT tiers because those tiers persist payloads to long-retained \
             sinks.\nhelp: move the field to a non-secret column, or move the event to \
             TRACE/METRIC tier (which do not persist payload bytes).",
            name = f.name,
            tier = input.tier.as_str(),
        );
        out.push(LintError::new("L003", msg));
    }

    // L004: MEASUREMENT requires a metric kind.
    if matches!(f.kind, FieldKind::Measurement) && !f.has_metric {
        let msg = format!(
            "obs L004: field `{name}` is MEASUREMENT without a metric kind\nnote: MEASUREMENT \
             fields must declare a metric kind (counter / gauge / histogram) so the OTLP metric \
             sink can dispatch correctly.\nhelp: annotate the proto field with a metric option \
             such as kind=METRIC_KIND_COUNTER and a unit string.",
            name = f.name,
        );
        out.push(LintError::new("L004", msg));
    }

    // L006: AUDIT tier forbids any PII / SECRET on any field.
    if matches!(input.tier, Tier::Audit)
        && matches!(
            f.classification,
            Classification::Pii | Classification::Secret
        )
    {
        let cls = match f.classification {
            Classification::Pii => "PII",
            Classification::Secret => "SECRET",
            _ => "classified",
        };
        let msg = format!(
            "obs L006: AUDIT-tier event must not carry `{cls}` field `{name}`\nnote: AUDIT events \
             ship to long-retained immutable sinks; classified data must be redacted at the \
             source.\nhelp: drop the field or move the event to a non-AUDIT tier.",
            name = f.name,
        );
        out.push(LintError::new("L006", msg));
    }

    // L007: snake_case field names.
    if !is_snake_case(&f.name) {
        let msg = format!(
            "obs L007: field `{name}` is not snake_case\nnote: every obs field name maps 1:1 to a \
             proto field, OTLP attribute, and analytics column; snake_case is required so the \
             projection round-trips deterministically.\nhelp: rename to `{suggest}`.",
            name = f.name,
            suggest = to_snake_case(&f.name),
        );
        out.push(LintError::new("L007", msg));
    }

    // L012: field name must not shadow envelope-reserved name. Skip
    // TRACE_ID / SPAN_ID / PARENT_SPAN_ID — those are *meant* to
    // project onto envelope slots of the same name.
    const RESERVED: &[&str] = &[
        "ts_ns",
        "service",
        "instance",
        "schema_hash",
        "callsite_id",
        "sev",
        "tier",
        "labels",
        "payload",
        "sampling_reason",
    ];
    if !matches!(
        f.kind,
        FieldKind::TraceId | FieldKind::SpanId | FieldKind::ParentSpanId
    ) && RESERVED.contains(&f.name.as_str())
    {
        let msg = format!(
            "obs L012: field `{name}` shadows envelope-reserved name\nnote: `{name}` is one of \
             the obs envelope's first-class fields. A payload field by the same name would clash \
             on the analytics surface.\nhelp: rename the field; if the intent was to project onto \
             the envelope slot, set the appropriate kind (e.g. `kind: TRACE_ID`).",
            name = f.name,
        );
        out.push(LintError::new("L012", msg));
    }

    // L014: TRACE_ID / SPAN_ID / PARENT_SPAN_ID kind fields must be
    // named with the matching envelope slot AND have proto type
    // `string`. Spec 95 § 2.2.
    if let Some(expected) = expected_correlation_name(f.kind) {
        if f.name != expected {
            let msg = format!(
                "obs L014: field `{name}` declares `kind` as a correlation slot but is not named \
                 `{expected}`\nnote: codegen projects fields whose kind is TRACE_ID / SPAN_ID / \
                 PARENT_SPAN_ID into the envelope slot of the same name; renaming keeps the \
                 analytics column predictable.\nhelp: rename the field to `{expected}` or change \
                 the `kind` to ATTRIBUTE.",
                name = f.name,
            );
            out.push(LintError::new("L014", msg));
        }
        if let Some(t) = &f.proto_type
            && !matches!(t, LintProtoType::String | LintProtoType::Other(_))
        {
            let actual = match t {
                LintProtoType::Bytes => "bytes",
                LintProtoType::Numeric => "numeric",
                LintProtoType::Bool => "bool",
                _ => "unknown",
            };
            let msg = format!(
                "obs L014: field `{name}` has kind {kind} but proto type is {actual}; expected \
                 string\nnote: correlation slots are projected into \
                 `env.trace_id`/`env.span_id`/`env.parent_span_id` which are typed `string`; a \
                 non-string proto type would require a runtime cast.\nhelp: change the field's \
                 proto type to `string`.",
                name = f.name,
                kind = correlation_kind_label(f.kind),
            );
            out.push(LintError::new("L014", msg));
        }
    }
}

fn expected_correlation_name(k: FieldKind) -> Option<&'static str> {
    match k {
        FieldKind::TraceId => Some("trace_id"),
        FieldKind::SpanId => Some("span_id"),
        FieldKind::ParentSpanId => Some("parent_span_id"),
        _ => None,
    }
}

fn correlation_kind_label(k: FieldKind) -> &'static str {
    match k {
        FieldKind::TraceId => "TRACE_ID",
        FieldKind::SpanId => "SPAN_ID",
        FieldKind::ParentSpanId => "PARENT_SPAN_ID",
        _ => "",
    }
}

fn is_snake_case(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !s.starts_with('_')
        && !s.ends_with('_')
        && !s.contains("__")
}

fn to_snake_case(s: &str) -> String {
    use heck::ToSnakeCase;
    s.to_snake_case()
}

/// Severity helper: an enum's `as_str` impl (`Severity::as_str` etc.)
/// already exists for `Tier`. The lint module re-exports a small marker
/// trait here so callers don't need to depend on `obs_types`'s `as_str`
/// directly. (Implementation note — both `Tier` and `Severity` already
/// expose `as_str` in `obs_types`, so the explicit `_` parameter
/// silences the unused-import lint.)
#[doc(hidden)]
pub fn _ensure_severity_link(_: Severity) {}

#[cfg(test)]
mod tests {
    use obs_types::{Cardinality, Classification, FieldKind, Tier};

    use super::*;

    fn input(prefix: &str, name: &str, tier: Tier, fields: Vec<LintField>) -> LintInput {
        LintInput {
            event_name: name.to_string(),
            tier,
            event_prefix: prefix.to_string(),
            fields,
        }
    }

    fn field(name: &str, kind: FieldKind) -> LintField {
        LintField {
            name: name.to_string(),
            kind,
            cardinality: Cardinality::Low,
            classification: Classification::Internal,
            has_metric: false,
            proto_type: Some(LintProtoType::String),
        }
    }

    #[test]
    fn test_should_flag_l011_when_prefix_missing() {
        let i = input(
            "Obs",
            "RequestStarted",
            Tier::Log,
            vec![field("a", FieldKind::Attribute)],
        );
        let errs = emit_lints(&i);
        assert!(errs.iter().any(|e| e.code == "L011"));
    }

    #[test]
    fn test_should_flag_l009_when_no_fields() {
        let i = input("Obs", "ObsX", Tier::Log, vec![]);
        let errs = emit_lints(&i);
        assert!(errs.iter().any(|e| e.code == "L009"));
    }

    #[test]
    fn test_should_flag_l001_when_label_high_cardinality() {
        let mut f = field("user_id", FieldKind::Label);
        f.cardinality = Cardinality::High;
        let i = input("Obs", "ObsX", Tier::Log, vec![f]);
        let errs = emit_lints(&i);
        assert!(errs.iter().any(|e| e.code == "L001"));
    }

    #[test]
    fn test_should_flag_l003_secret_on_log() {
        let mut f = field("token", FieldKind::Attribute);
        f.classification = Classification::Secret;
        let i = input("Obs", "ObsX", Tier::Log, vec![f]);
        let errs = emit_lints(&i);
        assert!(errs.iter().any(|e| e.code == "L003"));
    }

    #[test]
    fn test_should_flag_l014_when_wrong_name() {
        let f = field("trc_id", FieldKind::TraceId);
        let i = input("Obs", "ObsX", Tier::Log, vec![f]);
        let errs = emit_lints(&i);
        assert!(errs.iter().any(|e| e.code == "L014"));
    }

    #[test]
    fn test_should_flag_l014_when_wrong_proto_type() {
        let mut f = field("trace_id", FieldKind::TraceId);
        f.proto_type = Some(LintProtoType::Bytes);
        let i = input("Obs", "ObsX", Tier::Log, vec![f]);
        let errs = emit_lints(&i);
        assert!(errs.iter().any(|e| e.code == "L014"));
    }

    #[test]
    fn test_should_pass_when_correlation_field_correct() {
        let f = field("trace_id", FieldKind::TraceId);
        let i = input("Obs", "ObsX", Tier::Log, vec![f]);
        let errs = emit_lints(&i);
        assert!(errs.iter().all(|e| e.code != "L014"));
    }

    #[test]
    fn test_should_detect_l013_collision() {
        let pairs = vec![("a.v1.X".to_string(), 1u64), ("a.v1.Y".to_string(), 1u64)];
        let errs = emit_cross_event_lints(&pairs);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].code, "L013");
    }

    #[test]
    fn test_should_skip_l013_when_unique() {
        let pairs = vec![("a.v1.X".to_string(), 1u64), ("a.v1.Y".to_string(), 2u64)];
        assert!(emit_cross_event_lints(&pairs).is_empty());
    }

    #[test]
    fn test_should_recognize_string_rust_token() {
        assert_eq!(
            LintProtoType::from_rust_token("String"),
            LintProtoType::String
        );
        assert_eq!(
            LintProtoType::from_rust_token("::std::string::String"),
            LintProtoType::String
        );
        assert!(matches!(
            LintProtoType::from_rust_token("Vec < u8 >"),
            LintProtoType::Bytes
        ));
    }
}
