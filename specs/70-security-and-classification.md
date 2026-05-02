# Design — Security & Classification

Status: draft v1 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [10-data-model.md](./10-data-model.md), [12-schema-and-codegen.md](./12-schema-and-codegen.md)

This spec consolidates the security surface that earlier drafts
scattered across the codegen, runtime, and bridge specs. The
underlying primitives — `Classification`, the payload scrubber, the
PII redactor, the `secrecy::Secret*` integration — are already
described elsewhere in fragments; this document is the cross-cutting
contract.

## 1. Threat model

The SDK is a *boundary library*: events originate from trusted
in-process code but are emitted into a chain of sinks that may
include network egress (OTLP), durable storage (Parquet, ClickHouse,
audit), and downstream consumers we don't control. The threat model:

- A user-defined event type *might* declare a field that the durable
  store should never see (a session token, a password, a credit-card
  number). Compile-time annotations + runtime scrubbing must keep it
  off durable disk.
- A *bridged* `tracing::Event` from a third-party library has no
  declared classification. The bridge defaults to `Internal` but
  applies a name-pattern PII redactor for safety (see
  [30-tracing-bridge.md § 2.6](./30-tracing-bridge.md#26-pii--classification)).
- Operators of the SDK must be able to audit *which* fields were
  redacted and *why*, so a self-event signals every bridge-level
  redaction.

The SDK does **not** attempt to defend against:

- Malicious in-process code that decides to emit a SECRET-classified
  field as `Internal` (we trust the schema author).
- Compromised collectors (TLS to OTLP backends is the user's
  responsibility per [20-otel-and-sinks.md § 4.1](./20-otel-and-sinks.md#41-transport-configuration)).
- Side-channel attacks on the redactor itself (timing, etc.) — the
  redactor is type-driven, not value-driven.

## 2. The three classification levels

```proto
enum Classification {
  CLASSIFICATION_UNSPECIFIED = 0;
  CLASSIFICATION_INTERNAL = 1;
  CLASSIFICATION_PII      = 2;  // Redactable; never on LABEL
  CLASSIFICATION_SECRET   = 3;  // Stripped before durable write; never on LOG/AUDIT tier
}
```

Defined in [10-data-model.md § 4](./10-data-model.md#4-field-roles).
Lints (in [12-schema-and-codegen.md § 3.4](./12-schema-and-codegen.md)):

| Lint | Rule |
| --- | --- |
| `L002` | `Classification::Pii` on a `LABEL` field is illegal |
| `L003` | `Classification::Secret` on a LOG / AUDIT tier event is illegal |

Promotion (Internal → PII) is a non-breaking schema change; the
redactor activates without a code change. Demotion (PII → Internal)
is a breaking change banned by `obs schema diff`.

## 3. The Rust type for SECRET fields

`#[obs(classification = "secret")]` codegen emits the field as
`secrecy::SecretString` (or `secrecy::SecretBox<T>` for non-string
types) at the boundary between user code and the SDK:

```rust
#[derive(Event)]
pub struct ObsTokenIssued {
    #[obs(label, cardinality = "low")]
    pub token_kind: TokenKind,

    #[obs(attribute, classification = "secret")]
    pub token: secrecy::SecretString,
}
```

The `secrecy` crate guarantees:

- `SecretString::expose_secret()` is required to read the value, so
  accidental `Debug`/`Display` printing is a type error.
- `Debug` redacts to `[REDACTED]`.
- `Drop` zeroes the underlying `String`'s buffer (`zeroize`).

The user constructs the event by wrapping their token in
`SecretString::new(...)`; the SDK never extracts it on the durable
path (see § 4).

## 4. The payload scrubber

The scrubber is a per-event method on the object-safe schema view
defined in [14-schema-registry.md § 2](./14-schema-registry.md#2-the-eventschemaerased-trait):

```rust
trait EventSchemaErased {
    /// Strip SECRET fields and redact PII fields in the payload bytes.
    /// Returns a slice into `scratch` (worker-owned reused buffer)
    /// containing the cleaned re-encoded payload. The original
    /// `env.payload` is left untouched so non-durable sinks (metric
    /// counters, audit-filter sinks that need the raw bytes) see the
    /// original.
    fn scrub_for_log(
        &self,
        payload: &[u8],
        scratch: &mut bytes::BytesMut,
    ) -> Result<&[u8], ScrubError>;
}
```

`obs-build` emits the per-event implementation: a generated walk
over the schema's fields that drops or rewrites the bytes for each
SECRET / PII field per its declared classification. PII-classified
ATTRIBUTE fields are *redacted*, not stripped — they remain in the
payload but their values are replaced with `"[REDACTED:pii]"`,
keeping row counts and column structure stable for analytical
queries.

The per-tier worker invokes `scrub_for_log` between the sampler and
the sink chain (see [11-runtime-core.md § 4.1](./11-runtime-core.md#41-pipeline-order-per-envelope)),
then constructs a `ScrubbedEnvelope<'_>` ([14-schema-registry.md § 5](./14-schema-registry.md#5-the-scrubbedenvelope-worker-handoff))
whose payload slot points at the cleaned bytes. Sinks see only
`ScrubbedEnvelope`; they cannot reach the unscrubbed payload by the
type system. Metric sinks that read `env.labels` only never trigger
the scrubber's per-event work because PII is forbidden on LABEL
fields by lint L002.

Scrubber failure (re-encode error) drops the envelope at the worker
and emits `obs.runtime.v1.ObsSinkFailed{reason=scrub_error}`; the
unscrubbed payload is **never** delivered to a sink.

## 5. Bridge-side pattern redaction

Bridged tracing events carry no declared classification. The bridge
applies a name-pattern redactor by default:

- Matches `(?i)password|secret|token|api[_-]?key|authorization|cookie|ssn|credit[_-]?card|bearer`
  on field names.
- Redacted values become the literal `"[REDACTED:bridge_pattern]"`.
- One `obs.runtime.v1.ObsBridgePiiSuspected` self-event per unique
  field name (rate-limited) so operators can audit.

A user-supplied `Redactor` trait gives full control:

```rust
pub trait Redactor: Send + Sync {
    fn redact(&self, target: &str, field: &str, value: &mut String) -> RedactAction;
}
pub enum RedactAction { Keep, Replaced, Drop }
```

Full bridge-side details: [30-tracing-bridge.md § 2.6](./30-tracing-bridge.md#26-pii--classification).

## 6. Logging and error message hygiene

- The SDK never `Debug`-prints raw envelopes through `tracing::error!`
  on its own diagnostic path; bridge-internal traces redact the
  `payload` field.
- `Observer::emit_envelope` is `&self -> ()`; it cannot be made to
  return an error containing the envelope, so the envelope cannot
  leak into a `Result::unwrap` panic message.
- Per CLAUDE.md § Cryptography & Secrets, custom `Debug` impls on
  request types redact `Authorization` and similar headers; `obs-tower`
  honours this in `ObsHttpRequestStarted` (no header data is ever
  fielded).

## 7. AUDIT tier semantics

AUDIT events:

- Are durable (never silent-dropped; see [11-runtime-core.md § 6.4](./11-runtime-core.md#64-audit-tier-delivery-policy)).
- Cannot carry `Classification::Secret` fields (lint L003 — secrets
  are stripped at *every* durable boundary).
- Have separate retention policy and storage encryption — that lives
  outside the SDK; we just emit; operators wire encryption-at-rest on
  the audit sink.

## 8. CLI surface

| Command | Purpose |
| --- | --- |
| `obs lint` | Runs L002 / L003 / L011 + classification rules |
| `obs schema show <full_name>` | Prints classification per field |
| `obs audit` | Reports schemas with PII / SECRET fields, redaction coverage |

## 9. Runtime self-events

| Event | Purpose |
| --- | --- |
| `obs.runtime.v1.ObsBridgePiiSuspected` | bridge name-pattern redactor fired |

Future additions (not in v1):

- `ObsScrubFailed` — payload scrubber raised an internal error.
- `ObsClassificationDriftDetected` — `obs schema diff` saw a PII
  → Internal demotion attempt.

## 10. Build dependencies

| Depends on | Provides |
| --- | --- |
| [10-data-model.md](./10-data-model.md) | `Classification` enum |
| [12-schema-and-codegen.md](./12-schema-and-codegen.md) | scrub dispatcher codegen, lints |

Lives across `obs-types` (enum), `obs-build` (codegen), `obs-core`
(scrubber middleware), and `obs-tracing-bridge` (pattern redactor).
