# Spike: `buffa-reflect` custom-option ergonomics on the FDS

Status: Done · Owner: obs-core · Date: 2026-05-02 · Outcome: **PASS, with implementation rules**

## Question

Spec [12-schema-and-codegen.md § 2 / § 4](../../specs/12-schema-and-codegen.md)
declares that `obs-build` uses `buffa-reflect`'s `DescriptorPool` to walk
the FileDescriptorSet emitted by `buffa-build`, reads the
`(obs.v1.event)` and `(obs.v1.field)` custom options on each message
and field, and generates `EventSchema` impls. The spike validates that
this is feasible *with the current buffa-reflect surface*, and
describes the read pattern for `obs-build`.

The spec's fallback ("text parsing") would mean shipping a custom
proto-text parser and dropping the buffa-reflect dependency — much
worse ergonomics. The spike confirms the fallback is unnecessary.

## Method

A standalone crate at `/tmp/obs-spikes/buffa-reflect-spike/`:

- proto/obs/v1/options.proto — defines `EventMeta`/`FieldMeta` plus the
  two extension declarations against `MessageOptions`/`FieldOptions`
  (field numbers 80001 / 80002).
- proto/myapp/v1/events.proto — declares `ObsRequestCompleted` with
  `option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO }`
  and three fields each carrying `(obs.v1.field) = { ... }`.
- build.rs invokes `protoc` with `--include_imports
  --descriptor_set_out=fds.bin`, then hands the FDS to `buffa-build`
  for Rust-type generation.
- The spike binary loads the FDS, builds a `DescriptorPool`, walks to
  `myapp.v1.ObsRequestCompleted` and dumps the raw bytes of
  `MessageOptions.__buffa_unknown_fields` and each field's
  `FieldOptions.__buffa_unknown_fields`.

## Findings

### ✅ buffa-reflect parses the FDS cleanly

```
FDS files: 3
  google.protobuf/google/protobuf/descriptor.proto -> ...
  obs.v1/obs/v1/options.proto -> messages: ["EventMeta", "FieldMeta"]
  myapp.v1/myapp/v1/events.proto -> messages: ["ObsRequestCompleted"]
pool messages: 37
```

`DescriptorPool::from_file_descriptor_set` succeeds; `all_messages()`
walks every message including descriptor.proto's own. `MessageDescriptor::full_name`,
`fields()`, `descriptor_proto()` all work as expected.

### ✅ Custom-option bytes are accessible (via `__buffa_unknown_fields`)

buffa's generated `MessageOptions` has fields 1, 2, 3, 7, 11, 12, 999,
and our extensions live at field 80001 / 80002. buffa does not
generate Rust fields for extensions; instead the bytes land in the
generated `__buffa_unknown_fields: UnknownFields`. The spike confirms:

```
== myapp.v1.ObsRequestCompleted ==
MessageOptions unknown_fields bytes: 8 hex: 8a 88 27 04 08 01 10 03
- field route   unknown options:  8 bytes  hex: 92 88 27 04 08 01 10 02
- field user_id unknown options: 10 bytes  hex: 92 88 27 06 08 02 10 03 18 02
- field latency_ms unknown options:  8 bytes  hex: 92 88 27 04 08 03 10 04
```

Manual decode of `8a 88 27 04 08 01 10 03`:

- `8a 88 27` is the varint tag for field-number 80001, wire-type 2
  (LEN-delimited): `(80001 << 3) | 2 = 640010 = 0x9C40A`.
- `04` is the inner LEN.
- `08 01 10 03` is `tier=1 (LOG), default_sev=3 (INFO)` encoded as
  EventMeta — matches the source.

For `(obs.v1.field)` (field 80002), the tag prefix is `92 88 27`
(same first two bytes; last byte differs by ((80001-80002) << 3) =
+8 = 0x27 → 0x27 (no change actually, since 80002 << 3 | 2 = 640018,
let me recompute: 640018 / 128 = 5000 r 18 → first byte 0x92, then
5000 → 0x88 0x27). All confirmed.

### ✅ The implementation pattern is straightforward

`obs-build` will:

1. `protoc --descriptor_set_out=$OUT_DIR/fds.bin --include_imports proto/...`
   (or `buffa-build`'s precompiled FDS path; identical bytes).
2. `let fds = FileDescriptorSet::decode_from_slice(&bytes)?;`
3. `let pool = DescriptorPool::from_file_descriptor_set(fds)?;`
4. For each `msg in pool.all_messages()`:
   - `let opts_bytes = msg.descriptor_proto().options.unknown_bytes_for_tag(80001)?;`
     (helper we'll add — manual scan of the unknown-fields encoded
     stream looking for varint tag 80001 with wire-type 2).
   - `let event_meta = obs_proto::EventMeta::decode_from_slice(&opts_bytes)?;`
     (buffa-decoded as our own EventMeta type — that lives in obs-proto).
5. For each `f in msg.fields()`: same pattern with tag 80002 and FieldMeta.

All steps use buffa-decoded types directly. No string/text parsing.

## Decision

**GO**. We use `buffa-reflect` exactly as the spec proposes. We will
add a small helper in `obs-build` that scans `UnknownFields` for a
specific tag — that helper is ≤ 30 lines and is the only "raw byte
work" needed.

Spec amendments:

- 12-schema-and-codegen.md § 4 mentions a "descriptor pool's extension
  API" — buffa-reflect does not surface a typed extension API in the
  current 0.1 version, so we read the bytes ourselves. This is **not**
  a regression: the bytes are protobuf-encoded EventMeta/FieldMeta and
  we have the buffa-generated types to decode them. Update the spec
  language from "descriptor pool's extension API" to "descriptor pool
  + scan `UnknownFields` for the option tag, then decode the inner
  LEN as `EventMeta`/`FieldMeta`".

- The custom-options proto must use **proto2** syntax (`extend` is
  proto2). The user-facing event protos can remain proto3. This is
  the standard pattern (Google's own `descriptor.proto` is proto2);
  documented in 12 § 2 already.

## Risks identified

- **buffa-reflect 0.1 is pre-1.0 and may add a typed extension API
  in a later release.** When it does, we'd want to migrate from the
  `UnknownFields` byte scan to the typed API. Encapsulate the byte
  scan in `obs_build::option_reader::*` so the migration is one
  module's worth of work.
- **buffa stores `UnknownFields` only when generated with
  `preserve_unknown_fields`** (default for the FDS-read path).
  Confirmed from generated code in `buffa-descriptor` — `MessageOptions`
  has `__buffa_unknown_fields` unconditionally. No risk in practice.
- **Tag-encoding bytes for extensions differ by extension number.**
  Our two extensions are stable at 80001 / 80002 forever, so the
  encoded prefixes (`8a 88 27` and `92 88 27`) are constants that we
  can match against — no runtime tag computation needed.
