# Design — Data Model

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [00-prd.md](./00-prd.md)

This spec defines the wire-level data shapes the SDK is built around: the
typed event payload, the `ObsEnvelope` that wraps it, the `ObsBatch` that
ships envelopes, and the foundational enums (`Tier`, `Severity`,
`FieldKind`, `Cardinality`, `Classification`, `SamplingReason`).

> v3 changes: split out from the v2 monolithic architecture spec; fixed
> `ObsBatch.schemas` to use `fixed64` keys (matches `schema_hash` width);
> clarified that `callsite_id == 0` is reserved and the hashing path
> perturbs to avoid that value; defined the `Obs*` naming convention
> as binding (lint L011).

## 1. Wide Events

The unit of observation is a **Wide Event**: one strongly-typed protobuf
message, emitted exactly once per logical operation, carrying every
dimension a downstream system might want to query.

A wide event is *operation-shaped*, not *signal-shaped*: a single
emission projects into one log record, N metric data points, optionally
one trace span, and one row of the analytical store. See
[20-otel-and-sinks.md](./20-otel-and-sinks.md) for the projection
contract and [22-analytics-storage.md](./22-analytics-storage.md) for
the storage contract.

## 2. Tier

A wide event declares one *tier* that selects its primary durable
destination. Tier is a routing hint — the same event may also fan out to
metric/trace sinks regardless of tier.

```proto
enum Tier {
  TIER_UNSPECIFIED = 0;
  TIER_LOG    = 1;  // Durable, queryable; default for most events
  TIER_METRIC = 2;  // Aggregated; payload may be discarded after counter inc
  TIER_TRACE  = 3;  // Spans; envelope.trace_id / span_id are required
  TIER_AUDIT  = 4;  // Compliance: separate retention, encryption, immutability
}
```

AUDIT tier has stricter delivery semantics than the others — see
[11-runtime-core.md § 6.4](./11-runtime-core.md#64-audit-tier-delivery-policy).

## 3. Severity

Six levels aligned with OTel `SeverityNumber` buckets:

```proto
enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_TRACE = 1;
  SEVERITY_DEBUG = 2;
  SEVERITY_INFO  = 3;
  SEVERITY_WARN  = 4;
  SEVERITY_ERROR = 5;
  SEVERITY_FATAL = 6;
}
```

A schema declares `default_sev`. Call sites may **escalate or demote**
through `emit_at(sev)`; the value passed wins. Earlier drafts clamped
upward only — that was unnecessarily restrictive (legitimate cases like
"during graceful shutdown a normally-INFO event should be DEBUG"
exist), and the clamp is removed.

OTLP `SeverityNumber` mapping lives in
[20-otel-and-sinks.md § 2.2](./20-otel-and-sinks.md#22-severity--otlp-severitynumber).

## 4. Field roles

Every field on a wide event carries a `FieldKind` and a `Cardinality`.
Together they drive code generation, OTel mapping, and compile-time
lints.

```proto
enum FieldKind {
  FIELD_KIND_UNSPECIFIED   = 0;
  FIELD_KIND_LABEL         = 1;  // Bounded dimension; safe as metric/span attribute
  FIELD_KIND_ATTRIBUTE     = 2;  // Free-form; never a metric dim; in log/span body
  FIELD_KIND_MEASUREMENT   = 3;  // Numeric; emitted as a metric data point
  FIELD_KIND_TRACE_ID      = 4;  // Lifted to envelope.trace_id
  FIELD_KIND_SPAN_ID       = 5;  // Lifted to envelope.span_id
  FIELD_KIND_PARENT_SPAN_ID = 6; // Lifted to envelope.parent_span_id
  FIELD_KIND_TIMESTAMP_NS  = 7;  // Overrides envelope.ts_ns
  FIELD_KIND_DURATION_NS   = 8;  // Drives span start/end derivation
  FIELD_KIND_FORENSIC      = 9;  // Opaque blob; never indexed; size-capped
}

enum Cardinality {
  CARDINALITY_UNSPECIFIED = 0;
  CARDINALITY_LOW       = 1;  // <  10  (status, boolean)
  CARDINALITY_MEDIUM    = 2;  // < 10k  (route, tenant)
  CARDINALITY_HIGH      = 3;  // <  1M  (user_id) — illegal for LABEL
  CARDINALITY_UNBOUNDED = 4;  // open    — illegal for LABEL/MEASUREMENT
}

enum Classification {
  CLASSIFICATION_UNSPECIFIED = 0;
  CLASSIFICATION_INTERNAL = 1;
  CLASSIFICATION_PII      = 2;  // Redactable; never on LABEL
  CLASSIFICATION_SECRET   = 3;  // Stripped before durable write; never on LOG/AUDIT tier
}
```

Detailed treatment (redaction, secret-strip, lint mapping) lives in
[70-security-and-classification.md](./70-security-and-classification.md).

## 5. Sampling provenance

```proto
enum SamplingReason {
  SAMPLING_REASON_UNSPECIFIED = 0;
  SAMPLING_REASON_HEAD_RATE   = 1;  // Selected by head-rate roll
  SAMPLING_REASON_TAIL_ERROR  = 2;  // Flushed because a sibling event hit ERROR/FATAL
  SAMPLING_REASON_SLOW        = 3;  // `always_log_slower_than_ms` triggered
  SAMPLING_REASON_FORENSIC    = 4;  // Emitted by obs::forensic! (always retained)
  SAMPLING_REASON_AUDIT       = 5;  // AUDIT-tier event (always retained)
  SAMPLING_REASON_RUNTIME     = 6;  // SDK self-event (obs.runtime.v1.*)
  SAMPLING_REASON_OVERRIDE    = 7;  // Per-event head_rate=1.0 forces always-on
}
```

`HEAD_RATE` is "head-sampler kept this event"; head-sampler-dropped
events never produce an envelope so they need no enum value.

## 6. Envelope

Every emitted event is wrapped in a transport-neutral envelope. The
envelope is the contract between the SDK and any sink — sinks may
consume only the envelope (cheap, no descriptor needed) or descend into
the typed payload (expensive, schema-aware).

Envelope field names are deliberately short — these go on every event,
multiplied by request rate × service count, so character count matters
in `protoc --decode` output, dashboards, and ad-hoc CLI views.

```proto
message ObsEnvelope {
  string  full_name   = 1;   // "myapp.v1.ObsRequestCompleted"
  fixed64 schema_hash = 2;   // first 8 bytes of BLAKE3 over (full_name, tier, default_sev, FIELDS[])
  Tier    tier        = 3;
  Severity sev        = 4;
  fixed64 ts_ns       = 5;   // Unix epoch nanoseconds

  // Correlation (lifted from FIELD_KIND_*_ID fields by codegen, OR auto-filled
  // from the active obs::scope! task-local; see 13-emit-scope-and-filter.md).
  string  trace_id        = 6;
  string  span_id         = 7;
  string  parent_span_id  = 8;

  // Service identity (set once at observer init; cheap atomic load on emit).
  string  service  = 9;
  string  instance = 10;
  string  version  = 11;

  // Buffa-encoded payload bytes.
  bytes   payload  = 12;

  // Flat label projection: extracted at emit time so cheap sinks
  // (metric counter, OTel attribute writer) never decode the payload.
  map<string, string> labels = 13;

  // Sampling provenance (head-rate / tail-on-error / forensic / always).
  SamplingReason sampling_reason = 14;

  // Stable BLAKE3-derived id for the originating callsite (bridge / forensic /
  // span / instrument). 0 = no interning. When non-zero, downstream resolves
  // (target, file, line, template, field_names) via `ObsCallsiteRegistered`.
  // See 31-callsite-interning.md.
  fixed64 callsite_id = 15;
}

message ObsBatch {
  uint32  format_ver = 1;
  string  batch_id   = 2;
  fixed64 started_ns = 3;
  fixed64 closed_ns  = 4;

  // schema_hash → fully qualified name lookup, deduplicated per batch.
  // Key is fixed64 to match envelope.schema_hash width — earlier drafts
  // used `string` here which forced redundant stringification.
  map<fixed64, string> schemas = 5;
  repeated ObsEnvelope events = 6;
}
```

`schema_hash` is the first 8 bytes of BLAKE3 over
`(full_name, tier, default_sev, FIELDS[])`, computed at build time and
stored as a `u64` constant. It lets a downstream consumer detect schema
evolution without registry lookup, and lets the batch dedupe schema
names.

64 bits is sized for the role this id plays — distinguishing schemas
across services and time, **not** authenticating payloads. Birthday
bound on accidental collision at 64 bits is ~4 × 10⁹ distinct schemas;
realistic workspaces have ≤ 10⁴, an entire industry's lifetime maybe
10⁸. The schema_hash is not a tamper-detection primitive (buffa
payloads are not signed); the only failure mode of a contrived
collision is "downstream picks the wrong typed view and produces
nonsense fields", which is contained by the existing classification
machinery. Saves 24 bytes per envelope vs the 32-byte BLAKE3-256 we
considered, and lines up uniformly with the 64-bit `callsite_id`
(see [31-callsite-interning.md](./31-callsite-interning.md))
so the runtime treats the two id namespaces the same way.

`callsite_id == 0` is reserved to mean "this envelope is not interned".
The hash construction in [31-callsite-interning.md § 3.1](./31-callsite-interning.md#31-id-generation-blake3-truncated-to-64-bits)
perturbs to a non-zero value if the truncation accidentally yields zero
(probability < 2⁻⁶³ but easy to handle).

## 7. Naming convention: `Obs*` event types

User-defined event types **must** be prefixed with a workspace-
configurable string; the default is `Obs`. Examples with the
default prefix:

```
ObsRequestStarted   ObsRequestCompleted   ObsUpstreamFailed
ObsCheckoutStarted  ObsUserSignedUp       ObsForensicEvent
```

Why a prefix at all:

- **Visual distinction** — at a call site, `ObsCheckoutCompleted::builder()`
  is unambiguously an observability emission, never confused with a
  domain type called `CheckoutCompleted`.
- **Greppability** — `rg '\b<prefix>[A-Z]'` finds every emit site in
  the repo in one shot.
- **Codegen pattern matching** — the codegen and CLI lints use the
  prefix as a sanity check; an event without it is a likely typo.

### 7.1 Configuring the prefix

A team can override the prefix workspace-wide via the workspace root
`Cargo.toml`:

```toml
# Cargo.toml at the workspace root (NOT a member crate)
[workspace.metadata.obs]
event_prefix = "Evt"     # default "Obs"
```

Lint L011 reads the configured prefix and enforces it everywhere.
Built-in events shipped in `obs-proto` (`ObsForensicEvent`,
`ObsTracingForensicEvent`, the `obs.runtime.v1.*` self-events,
`ObsHttpRequest*`, etc.) keep the literal `Obs` prefix regardless of
workspace config — they are part of the SDK's namespace and renaming
them per-workspace would break cross-service queries.

**Recommendation**: keep `Obs`. The greppability and visual-
distinction win compounds at scale, and changing it forks your team's
docs and prompts from the SDK's. The configurable knob exists so
domain-rich codebases that already have an `Event` prefix on every
domain type aren't blocked from adopting obs over a name fight.

This convention is enforced by lint L011 (warning by default, error
under `--strict`). The proto namespace prefix is not required (events
can live in any package), only the message name.

Rendering rules for `EnumLabel`-derived enums (`AuthMethod::OAuthGoogle
→ "oauth_google"`) are specified in [12-schema-and-codegen.md § 3.5](./12-schema-and-codegen.md).

## 8. Glossary references

For terminology disambiguation (envelope vs event vs scope vs span,
sink vs observer vs subscriber, etc.) see [80-glossary.md](./80-glossary.md).

## 9. Build dependencies

This spec is the foundation for every other spec. It depends only on
the PRD. The Rust artifacts described here ship in the `obs-types` and
`obs-proto` crates (see [61-crates-and-features.md § 2.1, § 2.2](./61-crates-and-features.md)).
