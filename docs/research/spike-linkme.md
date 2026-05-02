# Spike: `linkme::distributed_slice` reliability

Status: Done · Owner: obs-core · Date: 2026-05-02 · Outcome: **PASS, with caveat**

## Question

Spec [14-schema-registry.md § 3](../../specs/14-schema-registry.md#3-link-time-registration-linkme-over-inventory)
mandates `linkme` for the schema registry. We need to confirm:

1. Cross-crate distributed-slice entries actually collect into one slice
   in the final binary on macOS arm64 (the dev platform) under
   release + LTO + strip.
2. The "compile-time error on duplicates" property advertised in the
   spec actually holds.
3. There is no surprise stripping under aggressive optimisation.

## Method

Workspace at `/tmp/obs-spikes/linkme-spike/`:

- `registry/` — declares `pub trait Schema` + `#[distributed_slice] pub static SCHEMAS`
- `leaf-a/`, `leaf-b/` — depend on `registry`, register one schema each
- `binary/` — depends on registry + both leaves + registers its own

Built with `release` profile: `strip = true`, `lto = "fat"`,
`codegen-units = 1` (production-class settings).

Tested on darwin-arm64 (Apple M-class) with rustc 1.95.0.

## Findings

### ✅ Cross-crate collection works under strip + fat-LTO

When the binary references each leaf crate, the linker pulls in the
distributed-slice entries:

```
registered (3): ["binary.Hello", "leaf_a.Foo", "leaf_b.Bar"]
OK
```

312 KB stripped binary, no symbols remaining (`nm` returns nothing for
`.*schema.*`), and the slice is intact at runtime. Mach-O preserves the
custom section that linkme uses (`__DATA,__linkme`).

### ⚠ CAVEAT: leaf rlibs must be referenced by the binary

If the binary does **not** reference each leaf crate, cargo+rustc do not
link the rlib at all and the distributed-slice entries silently
disappear. Reproduced — without `use leaf_a as _;`, `SCHEMAS.len()`
returns only 1 (the binary's own entry).

`#[used]` on the static **does not help** because cargo does not link
the rlib in the first place. The fix is to ensure each schema-bearing
crate is referenced from the binary. Two reliable patterns:

1. **Implicit reference** — the binary `use`s a public item (function,
   type, schema struct) from each schema crate. The normal case for
   user code: a binary that emits schemas defined in `mycrate` already
   `use`s items from `mycrate`.
2. **Empty re-export** — `use foo as _;` in the binary's `main.rs`
   forces the rlib to be linked even if no other items are touched.
   Useful for crates that *only* register schemas (rare).

This is **not** unique to linkme — it is fundamental to how Rust /
cargo / linker handle rlibs. `inventory` would have the same property
plus the additional issue of constructor stripping under LTO.

### ✅ Spec note: "link-time error on duplicates"

The spec sentence ([14-schema-registry.md § 3](../../specs/14-schema-registry.md#3-link-time-registration-linkme-over-inventory))
needs a small precision — it is correct only when the codegen derives
the registering `static` symbol from the schema's full_name. Two
crates emitting

```rust
#[linkme::distributed_slice(SCHEMAS)]
static __SCHEMA_MYAPP_V1_OBS_X: &dyn Schema = &MyAppV1ObsX;
```

collide at link with "duplicate symbol `__SCHEMA_MYAPP_V1_OBS_X`",
which is the failure mode the spec wants. Two crates emitting
**different** static names registering schemas with the same full_name
would not collide at link — the runtime `SchemaRegistry::from_link_section`
would detect the by_name conflict and emit
`obs.runtime.v1.ObsCallsiteRegistryConflict`.

The codegen MUST derive the static name from the schema full_name.
This is an implementation rule for `obs-build` and `obs-macros`.

## Decision

**GO**, with these spec-implementation amendments:

1. `obs-build` / `obs-macros` derive each registration static's symbol
   name deterministically from the event's full_name (e.g.
   `__SCHEMA_<UPPER_SNAKE>`). Documented as required for the link-time
   collision property.

2. The `obs::include_schemas!("crate.v1")` macro must emit references
   that prevent the leaf rlib from being skipped. The simplest fix is
   to expand `include_schemas!` to `include!` of files in the **same**
   crate's OUT_DIR; user crates that *register schemas in their own
   build.rs* are then naturally linked because the binary uses items
   from those crates anyway.

3. For the rare "schema-only" crate pattern (a crate whose sole purpose
   is to ship schemas for use across services), document the
   `use foo as _;` pattern in `60-dev-ergonomics.md` § 5 (will add).

4. CI gate: an integration test in Phase 1 that registers a schema in a
   leaf crate and asserts the binary's `SchemaRegistry` sees it. This
   pins the property regression-tested.

## Risks identified

- Future Rust / cargo could change rlib linking semantics. `linkme`'s
  documentation tracks the kernel/linker support; we should add
  `linkme = "0.3"` as a workspace dep with a `^0.3` cap that lets us
  pick up bug fixes but freezes major-bump compatibility.
- musl-static and Linux x86_64 not validated locally. The spec phrases
  this as a CI requirement; we add a `linkme-static-musl` job once CI
  is wired (Phase 1 task 1.15 follow-up).
