# Design — Callsite Interning

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [10-data-model.md](./10-data-model.md), [11-runtime-core.md](./11-runtime-core.md), [30-tracing-bridge.md](./30-tracing-bridge.md), [12-schema-and-codegen.md](./12-schema-and-codegen.md)

> v3 changes: callsite_id == 0 collision case explicitly handled in
> § 3.1; cross-references retargeted to the post-split spec
> structure.

This spec applies the lesson of `defmt`'s tokenized-logging
optimisation to `obs` server-side: emit only `(callsite_id, args)`
on the wire and resolve `callsite_id → (target, template, file, line)`
out-of-band via a registry. The biggest wins are on the
**bridged tracing path** and the **forensic path**, where today's
envelopes carry a lot of repeated literal strings. For native
`obs::emit!` the same optimisation already exists in spirit
(`schema_hash` is the analogue of defmt's interned format-string id);
this spec extends it.

## 1. Motivation

### 1.1 What defmt does (so we can copy what's good)

`defmt` (knurling-rs, embedded Rust) emits `[u16 ID][typed args]` on
the wire instead of the format string. The format string lives in
the firmware ELF as the **symbol name** of a one-byte static placed
in a custom `.defmt` section; the linker assigns each unique callsite
a unique address, and that address is the wire ID. Decoders parse
the ELF once at startup and resolve IDs to templates.

(See `vendors/defmt/macros/src/construct.rs:78` for the
`#[link_section]` machinery and `vendors/defmt/decoder/src/elf2table/mod.rs:108`
for the ELF→table extraction. The wire frame for
`info!("user registered: {}", 1)` is 2 bytes ID + timestamp + 4
bytes for the `u32` arg — under 16 bytes total in the typical case.)

The trade-offs (per the user-supplied analysis):

- **Pros**: collapses repeated string fragments; forces structured
  emit shape; massively reduces wire bytes; speeds downstream
  parsers.
- **Cons**: dictionary-version hell; loses `tail -f` readability;
  cross-process dictionary sync is non-trivial.

### 1.2 Where the savings are in `obs`

We do NOT have format strings in the public `obs::emit!` API — the
SDK is schema-first, not format-string-first. But we have three
high-volume paths today that DO carry repeated literal strings:

| Path | What's repeated | Today's per-event bytes | Interned per-event bytes |
| --- | --- | --- | --- |
| Bridged `tracing::*!` (`ObsTracingForensicEvent`) | `target` (~30 B), `module_path` (~40 B), `file:line` (~50 B), message template (~60 B) | ~280 B | ~30 B |
| `obs::forensic!` (`ObsForensicEvent`) | `site` (~20 B), `message` template (~50 B) | ~120 B | ~25 B |
| Bridged `tracing::span!` (`ObsSpanCompleted`) | span `name` (~20 B), `target` (~30 B), field name list (~40 B) | ~150 B | ~30 B |
| Native `obs::emit!` (typed events) | already covered by `schema_hash` | ~80 B | ~80 B (no change) |

For a service running 50 k tracing events/sec, the bridged path
alone is the difference between ~14 MB/s and ~1.5 MB/s of OTLP
egress. At cloud-egress prices and Honeycomb/Datadog ingest tariffs
(typically per-GB), the saving is operationally meaningful.

### 1.3 The brainstorm question, answered

> *Could we treat log fmt as a high-cardinal value?*

Yes — and **that is the queryable representation we want**. Once
each callsite has a stable id (BLAKE3 of `(target, file, line, level,
field_names, template)`), it becomes:

- a clustering key for log-pattern analytics
  (`SELECT callsite_id, count(*) FROM obs_events GROUP BY 1 ORDER BY 2 DESC`)
- a stable identity across deploys, processes, and machines (unlike
  defmt's per-binary linker addresses)
- a join key against the registry table for human rendering

This is the same primitive that vendor tools call "log patterns"
(Datadog), "log clusters" (Logz), or "logreduce" (Honeycomb). We
get it for free as a side effect of interning.

## 2. Scope

### 2.1 What gets interned

| Source | Interned content | Hashed inputs |
| --- | --- | --- |
| `tracing::Event` callsites (Direction A bridge) | `target`, `name`, `module_path`, `file`, `line`, `level`, `field_names` | all of those |
| `tracing::Span` callsites (Direction A bridge) | span `name`, `target`, `level`, `field_names` | all of those |
| `obs::forensic!(site = "X", message = "Y", ...)` | `site`, `file`, `line`, `message` template | all of those |
| `#[obs::instrument]`-derived `ObsFnEntered`/`ObsFnExited` | function name, module path, file, line | all of those |
| Native `obs::emit!(ObsX { ... })` | already interned via `schema_hash`; envelope `full_name` is the "human" rendering | (no change in default mode) |

### 2.2 What does NOT get interned

- **Argument values** — these are the dynamic part; never interned.
  They go on the wire as typed values inside the payload.
- **Per-request strings** like `trace_id`, `tenant_id`, `user_id` —
  these are not callsite-static; they live in `env.labels` /
  `env.trace_id` as today.
- **PII / SECRET fields** — even when the envelope is interned, the
  payload scrubber still runs over arg values per the existing
  classification machinery. Interning is orthogonal to redaction.

## 3. Mechanism

### 3.1 ID generation: BLAKE3 truncated to 64 bits

```rust
pub type CallsiteId = u64;

fn callsite_id(
    source: CallsiteSource,            // Tracing | Forensic | Span | Instrument
    target: &str,
    file: Option<&str>,
    line: Option<u32>,
    level: Severity,
    field_names: &[&str],
    template: Option<&str>,
) -> CallsiteId {
    let mut h = blake3::Hasher::new();
    h.update(&[source as u8]);
    h.update(target.as_bytes());
    h.update(file.unwrap_or("").as_bytes());
    h.update(&line.unwrap_or(0).to_le_bytes());
    h.update(&[level as u8]);
    for f in field_names { h.update(f.as_bytes()); h.update(b"\x00"); }
    if let Some(t) = template { h.update(t.as_bytes()); }
    // `blake3::Hash::as_bytes()` returns `&[u8; 32]` by type. Take the
    // first 8 bytes via `try_into` so `clippy::indexing_slicing` is
    // satisfied (no [..8] subslice indexing, no unwrap).
    let bytes = h.finalize();
    let arr: &[u8; 32] = bytes.as_bytes();
    let head: [u8; 8] = <[u8; 8]>::try_from(&arr[0..8])
        .expect("blake3 hash is always 32 bytes");
    let id = u64::from_le_bytes(head);
    // `0` is the reserved "no interning" sentinel. The truncation has
    // a 1-in-2⁶⁴ chance of producing zero; perturb to a non-zero value
    // by taking bytes 8..16 instead. Both BLAKE3 outputs are uniform
    // and independent under the threat model, so the post-perturbation
    // id is still uniformly distributed.
    if id != 0 { id } else {
        let head2: [u8; 8] = <[u8; 8]>::try_from(&arr[8..16])
            .expect("blake3 hash is always 32 bytes");
        u64::from_le_bytes(head2) | 1   // last-resort: force the LSB on
    }
}
```

Properties:

- **Deterministic** across processes, restarts, machines, and
  release builds.
- **Collision-resistant**: for ≤ 1 M distinct callsites in a
  workspace, birthday-bound collision probability is < 2⁻⁴⁴.
- **Cheap**: BLAKE3 over ~200 bytes runs in tens of ns. Computed
  **once per callsite**, cached by `tracing_core::callsite::Identifier`
  pointer (Direction A) or by source-location hash (forensic).
- **Reserved id `0`** means "no interning" and is what default-mode
  envelopes carry. The hashing path above guarantees a real callsite
  never produces `0` (see KD32 in [99-key-decisions.md](./99-key-decisions.md)).

We deliberately do **not** copy defmt's "linker-address as id" trick:
it cannot be made stable across processes. (And we don't need to —
BLAKE3 is fast enough that the runtime cost is irrelevant compared
to the wire savings.)

### 3.2 The `ObsCallsiteRegistry`

A process-local in-memory map:

```rust
pub struct ObsCallsiteRegistry {
    by_id: DashMap<CallsiteId, Arc<CallsiteRecord>>,
    by_tracing_callsite: DashMap<tracing_core::callsite::Identifier, CallsiteId>,
    by_source_loc: DashMap<(String, u32), CallsiteId>,  // (file, line) for forensic
}

// Per CLAUDE.md § Type Design: do not use Option<T> when T has a default.
// `module_path`/`file`/`template` are encoded as empty `String` when absent
// (no analytical query needs to distinguish "missing" from "empty"); `line`
// uses the `NonZeroU32` newtype since 0 is not a valid source line.
pub struct CallsiteRecord {
    pub id: CallsiteId,
    pub source: CallsiteSource,
    pub target: String,
    pub name: String,
    pub module_path: String,
    pub file: String,
    pub line: Option<NonZeroU32>,    // truly optional: 0 != "no line"
    pub sev: Severity,
    pub field_names: Vec<String>,
    pub template: String,
    pub registered_ns: u64,          // for re-emit cadence
}
```

Lifetime: lives on the global `Observer`, accessed by the bridge
(Direction A) for registration and by `ObsToTracingSink` (Direction B)
for rendering. Reset only on observer reinstall.

### 3.3 Registration flow

On first sight of a callsite:

1. The bridge (or forensic macro) computes `callsite_id` per § 3.1.
2. Inserts into `ObsCallsiteRegistry` (DashMap insert is O(1) and
   contention-tolerant).
3. **Synchronously** emits an `obs.runtime.v1.ObsCallsiteRegistered`
   envelope through the normal observer pipeline. This event is
   `Tier::Log`, `sampling_reason = SAMPLING_REASON_OVERRIDE`
   (always retained), and goes to every sink including
   `ObsToTracingSink`.
4. Then emits the actual data envelope (e.g., the bridged tracing
   event) with `callsite_id = computed_value` and a stripped payload
   (no target/file/line/template — those live on the registry).

On subsequent emissions: skip steps 1–3; jump to 4.

The synchronous registration emit ensures that when the data
envelope arrives at any sink, the registration envelope has already
been delivered to that sink's worker (FIFO per-tier mpsc).

### 3.4 The `ObsCallsiteRegistered` event

```proto
message ObsCallsiteRegistered {
  option (obs.v1.event) = {
    tier: TIER_LOG,
    default_sev: SEVERITY_DEBUG,
  };

  fixed64 callsite_id = 1
    [(obs.v1.field) = { kind: ATTRIBUTE }];        // not a metric dim

  CallsiteSource source = 2
    [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];

  string  target = 3
    [(obs.v1.field) = { kind: LABEL, cardinality: MEDIUM }];

  string  name = 4
    [(obs.v1.field) = { kind: ATTRIBUTE }];        // tracing's metadata.name()

  string  module_path = 5
    [(obs.v1.field) = { kind: LABEL, cardinality: MEDIUM }];

  string  file = 6
    [(obs.v1.field) = { kind: ATTRIBUTE }];

  uint32  line = 7
    [(obs.v1.field) = { kind: ATTRIBUTE }];

  Severity sev = 8
    [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];

  repeated string field_names = 9
    [(obs.v1.field) = { kind: ATTRIBUTE }];

  string  template = 10
    [(obs.v1.field) = { kind: ATTRIBUTE }];        // empty for non-templated paths
}

enum CallsiteSource {
  CALLSITE_SOURCE_UNSPECIFIED = 0;
  CALLSITE_SOURCE_TRACING_EVENT = 1;
  CALLSITE_SOURCE_TRACING_SPAN  = 2;
  CALLSITE_SOURCE_FORENSIC      = 3;
  CALLSITE_SOURCE_INSTRUMENT    = 4;
}
```

`callsite_id` is `ATTRIBUTE` (not LABEL) on this event because it's
high-cardinality and we don't want it as a metric dimension. It
**is** carried on every interned envelope's envelope field
`callsite_id` (see § 4) so downstream queries can group by it
without joining to the registry.

### 3.5 Wire format change

Add one optional field to the envelope:

```proto
message ObsEnvelope {
  // ... existing fields 1..14 unchanged ...
  fixed64 callsite_id = 15;     // 0 = no interning; otherwise lookup via ObsCallsiteRegistered
}
```

When `callsite_id != 0`:

- The envelope's `full_name` still identifies the *event type*
  (`obs.v1.ObsTracingInternedEvent`, `obs.v1.ObsForensicInternedEvent`,
  etc.). This stays so existing tier/sev/sink routing works unchanged.
- The payload carries only the dynamic args. Static metadata is
  resolved out-of-band via the registry.

When `callsite_id == 0` (default for native obs emits and for
interning-disabled bridge):

- Envelope is exactly today's shape; no behaviour change.

This is **proto-additive** — old decoders ignore the unknown field;
new decoders that don't recognise interned event types fall back to
the proto-encoded payload bytes (which still carry the args, just
without the human-readable target/template).

### 3.6 The interned data event types

```proto
message ObsTracingInternedEvent {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };

  // callsite_id is on the envelope, not duplicated here.

  string  message = 1
    [(obs.v1.field) = { kind: ATTRIBUTE }];        // the rendered message (with args interpolated)

  map<string, string> args = 2
    [(obs.v1.field) = { kind: ATTRIBUTE }];        // dynamic field values
}

message ObsForensicInternedEvent {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };
  string  message = 1
    [(obs.v1.field) = { kind: ATTRIBUTE }];
  map<string, string> args = 2
    [(obs.v1.field) = { kind: ATTRIBUTE }];
}
```

The non-interned cousins (`ObsTracingForensicEvent`,
`ObsForensicEvent`) remain in the schema and are emitted when
`callsite_id == 0`. Both shapes coexist; interning is opt-in
per-bridge / per-config.

## 4. Modes

Three modes selectable in `obs.yaml`:

```yaml
interning:
  mode: hybrid                       # off | hybrid | compact
  refresh_interval_secs: 600         # re-emit registrations every 10 min
  refresh_event_count: 10000         # also re-emit every 10k matching events
```

| Mode | Envelope `callsite_id` | Bridge data event payload | Wire size | Decoder needs registry? |
| --- | --- | --- | --- | --- |
| `Off` (default in v1) | 0 | full target/file/line/message strings | 100 % baseline | no |
| `Hybrid` | non-zero | rendered message + dynamic args (strings) | ~50 % | only for human-friendly target/file/line |
| `Compact` | non-zero | dynamic args only as a length-prefixed buffa-encoded values list; no rendered message | ~25 % | yes (else only `callsite_id` + arg values are decodable) |

`Hybrid` is the recommended on-ramp: even without the registry, a
downstream consumer sees the rendered message and the dynamic args,
so `tail -f` is still useful. `Compact` is for the highest-volume
deployments where wire savings matter more than fallback readability.

`Off` remains the v1 default. We do not flip the default until the
SDK has shipped the registry-snapshot tooling (§ 5.3) and the CLI
decoder (§ 5.4) for at least one minor release.

## 5. Lifecycle and consistency

### 5.1 In-process consistency

Within a single process, registration is synchronous: the
`ObsCallsiteRegistered` envelope is enqueued before the first data
envelope referencing that `callsite_id`. Per-tier FIFO mpsc channels
preserve order to each sink worker. There is no in-process race.

### 5.2 Cross-process / downstream consistency

Downstream consumers (OTLP collector, Parquet/ClickHouse decoder,
CLI tools) may receive the registration and the data envelopes via
different channels, get rebatched, or join the stream late. We
mitigate three ways:

#### Re-emit cadence

The registry tracks `(callsite_id, registered_ns, last_seen_ns,
event_count_since_last_register)`. On each envelope emission, if
either `now - registered_ns > refresh_interval_secs` or
`event_count_since_last_register > refresh_event_count`, the bridge
re-emits `ObsCallsiteRegistered` and resets the counters. Defaults
re-emit every 10 min or 10 k events, whichever comes first.

#### Startup snapshot

On `StandardObserver::shutdown` and on any `ObsConfigReloaded`
event, the registry dumps every entry as `ObsCallsiteRegistered`
events. On startup, *first emission per callsite always emits a
fresh registration* (§ 3.3 step 3), so a freshly-started process
populates downstream within the first few seconds.

#### Snapshot file (CLI)

`obs callsites dump --out callsites.ndjson` exports the running
process's registry. `obs callsites load callsites.ndjson | obs decode`
lets a human decoder hydrate even if the wire stream missed
registrations.

### 5.3 Late registry on the consumer

Consumers that join late (e.g., a new ClickHouse importer) and
encounter a `callsite_id` they don't recognise should:

1. Tag the row `callsite_status = "unresolved"` and store
   `callsite_id` as-is.
2. After the next `ObsCallsiteRegistered` for that id, run a
   reconciliation pass that joins prior unresolved rows.

For the OLAP store this is just an UPDATE; for the OTLP path it's
the consumer's problem (we ship the data; downstream owns
materialised views). The CLI `obs query` shows unresolved callsites
as `<unresolved:0xdeadbeef>` until reconciliation.

### 5.4 CLI decoding

```
$ obs callsites show 0x8b7c29d041f2a0fe
callsite_id:  0x8b7c29d041f2a0fe
source:       TRACING_EVENT
target:       sqlx::query
file:         /tmp/.cargo/registry/.../sqlx-0.8.2/src/query.rs
line:         128
sev:          DEBUG
field_names:  [rows_affected, elapsed_secs, summary]
template:     "executed query: {summary}"

$ obs query --callsite 0x8b7c29d041f2a0fe --since 1h --limit 5
ts                          message
2026-05-02T18:01:23.456Z    executed query: SELECT * FROM users WHERE id = $1
2026-05-02T18:01:23.512Z    executed query: SELECT * FROM users WHERE id = $1
...
```

The CLI's `query` and `tail` subcommands learn one new flag,
`--callsite <id>`, plus implicit awareness of the registry for
human rendering of interned envelopes. Without `--callsite`, output
already shows the rendered message.

## 6. Bridge integration

### 6.1 Direction A: `tracing` → `obs`

The bridge gains one config knob:

```rust
TracingToObsLayer::new()
    .with_interning(InterningMode::Hybrid)        // off | hybrid | compact
    .with_field_promotions(...)
    .register_typed::<ObsHttpRequestCompleted>(...);
```

When interning is on:

1. `Layer::on_event` looks up `tracing_core::callsite::Identifier`
   in `ObsCallsiteRegistry::by_tracing_callsite`. Cache hit →
   `callsite_id` is known.
2. Cache miss → compute callsite hash (§ 3.1) from
   `event.metadata()`, insert both maps, emit
   `ObsCallsiteRegistered`, then proceed.
3. Build the data envelope with `env.callsite_id = id`,
   `env.full_name = "obs.v1.ObsTracingInternedEvent"` (or the
   typed promoter's schema if the matcher fired), and a stripped
   payload.
4. PII redactor still runs on `args` values per the existing
   classification rules.

Auto-typed events (per [30-tracing-bridge § 2.5](./30-tracing-bridge.md#25-auto-typing--promoting-tracing-events-to-typed-obs-events))
already carry their own `schema_hash` so they DO NOT need a
`callsite_id`. The interning kicks in only for the forensic /
non-typed path.

### 6.2 Direction B: `obs` → `tracing`

`ObsToTracingSink` receives an envelope. If `env.callsite_id != 0`:

1. Look up `ObsCallsiteRegistry` by id.
2. If found: synthesise the tracing `Metadata` using the registered
   `target`, `name`, `module_path`, `file`, `line`, `field_names`.
   This produces a tracing event indistinguishable to downstream
   layers (`fmt::layer`, `tracing-opentelemetry`, etc.) from the
   original.
3. If not found (impossible in single-process; defensive only):
   emit a degraded tracing event with `target = "obs.bridge.unresolved"`
   and a single field `callsite_id = 0xdeadbeef`. Increment
   `obs.runtime.v1.ObsBridgeCallsiteUnresolved` self-event,
   rate-limited.

The metadata cache is the same `DashMap<MetadataKey, &'static Metadata>`
introduced in [30-tracing-bridge § 3.2](./30-tracing-bridge.md#32-the-obstotracingsink),
keyed on `MetadataKey::ByCallsiteId(id)` for interned envelopes and
`MetadataKey::ByFullName(name)` otherwise. Same `Box::leak` strategy
for the `'static` lifetime.

This makes the round-trip bullet-proof: a bridged tracing event
that gets interned by Direction A is reconstituted by Direction B
into a tracing event with the **same** `Metadata` shape (same
`target`, same `name`, same fields). External tools that key off
`metadata().target()` continue to work transparently.

### 6.3 Loop avoidance still holds

The thread-local guards from
[30-tracing-bridge § 4.1](./30-tracing-bridge.md#41-loop-avoidance)
are unaffected. The `ObsCallsiteRegistered` envelope flows through
the standard sink chain; `ObsToTracingSink` sees it like any other
envelope. A naive concern: "will the bridge's own
`ObsCallsiteRegistered` cause re-entry?" No — the registration
event has its own native schema (`obs.runtime.v1.ObsCallsiteRegistered`)
which is a normal `obs::emit!` callsite, not interned, and goes
through the standard re-entry guard.

## 7. Native `obs::emit!` interaction

Native obs events are unaffected by interning by default — they
already enjoy the equivalent: `schema_hash` is a `u64` (first 8
bytes of BLAKE3 over the schema descriptor), baked at build time.
The buffa-encoded payload uses tag-numbered fields (no field names
on the wire).

One small extension is available behind a config flag:

- `interning.elide_full_name: true` — when `callsite_id` is
  populated from a `(schema_hash → callsite_id)` synthetic
  mapping, the `full_name` string becomes redundant on every
  envelope. Saves 30–50 bytes per event. Default off because the
  CLI decoder relies on `full_name` for several queries.

This is off because the marginal saving is smaller than the
bridged-tracing wins, and it tightens the consumer→registry
coupling further. The bridge case is where interning earns its
keep.

## 8. Wire-size analysis

Concrete byte counts for representative payloads, using buffa's
proto3 encoding (varints + length-prefixed strings) and ignoring
proto framing overhead common to both modes.

### 8.1 Bridged `tracing::info!(target: "sqlx::query", rows_affected = 1, elapsed_secs = 0.012, summary = "SELECT ...", "executed query")`

| Field | Off mode | Hybrid mode | Compact mode |
| --- | --- | --- | --- |
| envelope.full_name | "obs.v1.ObsTracingForensicEvent" (35 B) | "obs.v1.ObsTracingInternedEvent" (35 B) | same (35 B) |
| envelope.callsite_id | absent | 9 B (tag + 8) | 9 B |
| envelope.schema_hash | 9 B (fixed64) | 9 B | 9 B |
| envelope.tier/sev/ts_ns | 18 B | 18 B | 18 B |
| envelope.labels | (empty) | (empty) | (empty) |
| payload.target | "sqlx::query" (12 B) | absent | absent |
| payload.module_path | "..." (40 B) | absent | absent |
| payload.file + line | (60 B) | absent | absent |
| payload.message | "executed query" (15 B) | "executed query: SELECT …" (rendered, 60 B) | absent |
| payload.attrs (rows_affected, elapsed_secs, summary) | (140 B) | (140 B) | typed args (40 B) |
| **total** | **~329 B** | **~171 B** | **~111 B** |

For 50 k events/sec sustained, the savings are 16.5 MB/s → 8.6 MB/s
(Hybrid) → 5.6 MB/s (Compact). Per-month at 24×7 that's ~14 TB
saved (Compact). At AWS inter-region rates this is ~$280/month per
service; cloud-vendor analytics ingest is typically priced per GB
and the savings are similarly meaningful.

### 8.2 Native `obs::emit!` (schema_hash already u64; no truncation flag)

| Field | Default | With elide_full_name |
| --- | --- | --- |
| envelope.full_name | 35 B | absent |
| envelope.schema_hash | 9 B (fixed64) | 9 B |
| envelope.callsite_id | absent | 9 B |
| envelope.tier/sev/ts_ns | 18 B | 18 B |
| envelope.labels (3 LABELs, ~15 B each) | 60 B | 60 B |
| payload (typed, 5 fields) | 40 B | 40 B |
| **total** | **~162 B** | **~136 B** |

Native emits are already ~50 % smaller than bridged-Off without
any opt-in. `elide_full_name` trims another ~16 % at the cost of
forcing the consumer to maintain a `schema_hash → full_name`
mapping (already broadcast via `ObsBatch.schemas` and via
`ObsCallsiteRegistered`); we ship it as opt-in for the
high-throughput case.

## 9. Performance budget

Beyond the budgets in [71-performance-budgets.md § 3.3](./71-performance-budgets.md#33-callsite-interning-when-enabled):

| Path | Budget | Notes |
| --- | --- | --- |
| Direction A bridged emit, interning Hybrid (cold) | ≤ 4 µs P50 | one BLAKE3 hash + DashMap insert + ObsCallsiteRegistered emit + interned emit |
| Direction A bridged emit, interning Hybrid (warm) | ≤ 2.5 µs P50 | one DashMap lookup + interned emit |
| Direction B sink rendering, interning Hybrid (cold) | ≤ 3 µs P50 | DashMap lookup + Metadata leak + tracing dispatch |
| Direction B sink rendering, interning Hybrid (warm) | ≤ 1.5 µs P50 | OnceLock cache hit + dispatch |
| ObsCallsiteRegistry insertion | ≤ 200 ns | DashMap insert is amortised O(1) |
| BLAKE3 hash for callsite (~200 input bytes) | ≤ 80 ns | benched on M2/2024-class hardware |

Cold/warm distinction is per-process and per-callsite; the cold
path runs at most once per (callsite_id, refresh interval) pair.

## 10. Failure modes

| Failure | Behaviour |
| --- | --- |
| BLAKE3 collision (different callsites, same id) | Detected at insert time: if `by_tracing_callsite[id_b]` already mapped to a different `tracing::Identifier`, log `ObsCallsiteHashCollision` self-event, skip interning for callsite B (emit verbose), continue. Probability < 2⁻⁴⁴ for ≤ 1 M callsites. |
| Channel full when emitting `ObsCallsiteRegistered` | Same as native emit overflow: drop with metric increment. The data event still emits with `callsite_id` populated; downstream sees an unresolved id and reconciles when re-emit fires (§ 5.2). |
| Process crash before re-emit | The cold path re-registers on fresh start; downstream sees a flurry of registrations as the new process catches up. |
| Hot-reload changing `interning.mode` from Compact → Off | Bridge stops setting `callsite_id`. Already-emitted envelopes remain interned downstream until the registry's TTL expires (10 min default); during that window queries see a mix of interned and verbose events. Documented; users should treat mode switches as schema migrations. |
| Two processes on different versions emit the same callsite_id with different metadata | First registration wins downstream; the second is logged as `ObsCallsiteRegistryConflict` self-event. This is the dictionary-version-hell concern from the user's analysis; mitigated by deterministic hashing (file path + line) so genuinely same callsites get same id, and by the registry's `registered_ns` for ordering. |
| Decoder receives interned envelope without registry | Tags rows `callsite_status = "unresolved"`, stores raw id, runs reconciliation when the next `ObsCallsiteRegistered` arrives. CLI shows `<unresolved:0xdeadbeef>`. |
| Re-emit storm at startup | Registry dump emits N events serialised by the per-tier mpsc; rate-limited by the channel itself. For large workspaces (10⁴ callsites), startup adds ~1 sec of self-emit traffic — acceptable. |

## 11. Key Design Decisions

### KD1 — BLAKE3 hash, not linker addresses

Defmt's address-as-id trick depends on a single ELF binary. We're
distributed; we need stable ids across processes. BLAKE3 over the
canonical callsite tuple gives us deterministic ids for free.

### KD2 — `Off` is the v1 default, opt-in by config

Interning changes the consumer contract. Shipping it on by default
in v1 would force every downstream consumer (Parquet readers,
ClickHouse importers, CLI users) to learn about the registry on
day one. Off is safe; users opt in when ready.

### KD3 — Three modes, not a binary

`Hybrid` keeps the rendered message in the payload so `tail -f` and
ad-hoc consumers still work without the registry. `Compact` is the
final form for the highest-volume deployments. The split lets
operators trade readability for bytes consciously.

### KD4 — Re-emit cadence, not "once-only"

A produce-once-consume-many model is fragile to consumer churn. We
re-emit registrations periodically so a late or restarted consumer
catches up within minutes. Cost is one extra event per callsite per
10 min — negligible.

### KD5 — Native `obs::emit!` is unchanged by default

`schema_hash` is already a `u64` ([10-data-model.md § 6](./10-data-model.md#6-envelope) / [99-key-decisions.md § D2](./99-key-decisions.md)) so
native emits already have the wire-size benefit defmt-style interning
would otherwise provide. The optional `elide_full_name` flag exists
for the throughput-obsessed but is not on by default; the marginal
wire saving does not justify tightening the consumer coupling for
the typical case.

### KD6 — Registration is synchronous on first sight

Asynchronous registration would introduce ordering races between
the data event and its registration. Synchronous emit (one extra
envelope per first-sight callsite) keeps the in-process FIFO
ordering and the consumer side simple.

### KD7 — Callsite id is on the envelope, not a label

LABEL fields are metric dimensions; a high-cardinality
`callsite_id` would explode the metric attribute set. Putting
`callsite_id` on the envelope as `fixed64 callsite_id = 15` keeps
it queryable in OLAP and skippable for metric sinks.

### KD8 — `ObsCallsiteRegistered` is itself a normal `obs::emit!`

It rides the same observer pipeline, gets the same retention, can
be filtered by `OBS_FILTER`, etc. We do NOT introduce a
side-channel for it — too easy to drift out of sync with the data
events.

### KD9 — Direction B reconstitutes full `Metadata` on the way out

If we emitted degraded `target = "obs.bridge"` events to tracing,
existing tooling that targets `"sqlx::query"` would break. The
sink looks up the original metadata and reconstitutes a faithful
tracing event so downstream layers see no difference between
"never interned" and "round-tripped through interning".

### KD10 — Per-callsite refresh, not per-batch

Defmt re-extracts the entire dictionary by parsing the ELF. We
re-emit per-callsite based on cadence so churn matches actual
event traffic. A callsite that fires once and never again gets
registered once and not maintained.

## 12. CLI surface additions

[50-cli.md](./50-cli.md) gains:

```
$ obs callsites dump --out callsites.ndjson
   exports the live process's registry to NDJSON

$ obs callsites load callsites.ndjson | obs query --from -
   feeds a registry snapshot into the CLI for offline decoding

$ obs callsites show <id>
   prints the full record for one callsite

$ obs query --callsite <id> [other filters]
   filters events by callsite_id; auto-resolves to template

$ obs lint --check-callsites
   warns when proto schemas conflict with registered callsites
   (e.g., schema_hash recycled across versions)
```

The existing `obs decode` and `obs tail` learn to consume
`ObsCallsiteRegistered` events as a side-stream, building an
in-memory map and rendering interned envelopes against it.

## 13. Test strategy

Tests live in `crates/obs-tracing-bridge/tests/interning/` and
`crates/obs-core/tests/callsite_registry/`:

- **`registry_basic.rs`** — emit one tracing event under each mode,
  assert the registry holds one record and the data envelope's
  `callsite_id` matches.
- **`re_emit_cadence.rs`** — set `refresh_event_count = 100`, emit
  150 events from one callsite, assert exactly 2
  `ObsCallsiteRegistered` events landed (initial + cadence).
- **`startup_snapshot.rs`** — install observer, emit, shutdown,
  install fresh observer, emit; assert the second installation
  re-registers on first sight.
- **`compact_mode_roundtrip.rs`** — emit in Compact mode, decode
  using a separately-built registry snapshot, assert the rendered
  output equals the original verbose version byte-for-byte (modulo
  whitespace).
- **`direction_b_reconstitution.rs`** — install both directions
  with interning Hybrid; emit a tracing event; capture what
  `ObsToTracingSink` dispatches; assert the synthesised
  `tracing::Metadata` matches the original (target, name, fields).
- **`unresolved_decoding.rs`** — feed `obs decode` an interned
  envelope without the matching `ObsCallsiteRegistered`; assert
  output contains `<unresolved:0x...>`.
- **`hash_collision_simulated.rs`** — force two distinct callsites
  to hash-collide via a test-only override; assert the second
  registration logs `ObsCallsiteHashCollision` and falls back to
  verbose.
- **`bench_interning_overhead.rs`** — criterion benches matching
  the budget table in § 9; CI gates ≥ 10 % regression.

## 14. Open questions / risks

- **Cross-process registry sharing.** A future revision could
  expose the registry as a Unix-socket service, so a sidecar
  collector can subscribe once and serve all processes on a host.
  Out of scope for v1; would reduce per-process registration
  storms.
- **OTLP semantic-conventions mapping.** `callsite_id` doesn't
  map to any OTel-standard attribute. We propose `obs.callsite_id`
  but `event.id` (semconv) might be repurposable; leaving as a
  custom attribute for now.
- **Compact mode arg encoding.** We propose buffa-encoded values
  list; alternative is CBOR or msgpack for broader cross-language
  support. Buffa is consistent with the rest of the SDK; revisit
  if a non-Rust consumer arrives.
- **`schema_hash` and `callsite_id` width.** Both are `u64` (the
  first 8 bytes of BLAKE3 over their respective canonical input).
  Birthday bound: 64 bits is safe to ~10⁹ distinct ids; realistic
  workspaces have ≤ 10⁴ schemas + ≤ 10⁴ callsites. If a workspace
  genuinely exceeds 10⁵ in either, `obs lint` warns; the SDK could
  add a feature flag to widen to 96 or 128 bits without wire-format
  break (the field would simply use a longer encoding). Not a
  near-term concern.
- **Registry persistence across restart.** Today the registry is
  in-memory. If a fast-restart scenario (rolling deploy, hot
  reload) churns the registry rapidly, downstream sees a flood of
  re-registrations. Optional registry persist-to-disk (open file
  + memory-map) is deferred to v1.1.
- **Does interning interact with sampling?** When tail-on-error
  flushes a 64-deep ring buffer of TRACE/DEBUG events from a
  scope, all of them are interned the same way. The
  `ObsCallsiteRegistered` for any TRACE-level callsite must have
  been emitted at sample time, not skipped because the head
  sampler dropped it. Resolution: registration emits use
  `SamplingReason::OVERRIDE` and bypass the sampler. This is
  documented in § 3.3.
