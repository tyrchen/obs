# Spike: `ArcSwap<Arc<dyn Trait>>` shape with `Lazy::from_pointee`

Status: Done · Owner: obs-core · Date: 2026-05-02 · Outcome: **PASS**

## Question

Spec [11-runtime-core.md § 3](../../specs/11-runtime-core.md#3-the-observer-trait)
declares the global observer slot as

```rust
static OBSERVER_GLOBAL: Lazy<ArcSwap<Arc<dyn Observer>>> = Lazy::new(|| {
    ArcSwap::from_pointee(Arc::new(NoopObserver) as Arc<dyn Observer>)
});
```

Two things we need to confirm:

1. Does the type compose: does `ArcSwap<Arc<dyn Observer>>` actually
   compile under `Lazy::from_pointee` and `ArcSwap::from_pointee`?
2. Is `OBSERVER_GLOBAL.load_full()` cheap enough on the hot path
   (target ≤ 5 ns)?

## Method

Standalone crate at `/tmp/obs-spikes/arcswap-spike/`:

- `arc-swap = "1.7"` (resolved 1.9.1)
- `once_cell = "1.20"` (resolved 1.21.4)
- `criterion = "0.5"`

Construction shape used:

```rust
static OBSERVER_GLOBAL: Lazy<ArcSwap<Arc<dyn Observer>>> = Lazy::new(|| {
    let initial: Arc<dyn Observer> = Arc::new(NoopObserver);
    ArcSwap::from_pointee(initial)
});
```

Fixed-`black_box` Criterion bench, `--quick`, on Apple M-class hardware.

## Findings

1. **Compiles cleanly** with `arc-swap` 1.9.x. No `unsafe`, no
   workarounds. `ArcSwap<Arc<dyn Trait>>` works because `Arc<dyn Trait>`
   is `Sized` (the dyn lives behind one `Arc` indirection), which is
   what `ArcSwap` requires.

2. **Subtle**: `ArcSwap::from_pointee(arc)` produces `ArcSwap<Arc<dyn>>`
   (it wraps the `Arc<dyn>` in an outer `Arc`). `load_full()` therefore
   returns `Arc<Arc<dyn Observer>>`. Two solutions:

   - Accept the extra layer; methods on `Observer` are still reached via
     auto-deref. This is what the spike used.
   - Or call `ArcSwap::new(initial)` and accept that it stores a literal
     `Arc<dyn>` (no extra layer). Both compile.

   The spec's example uses `from_pointee`; we'll keep it for minimum
   diff, document the double-`Arc` shape, and call sites use
   `observer.method()` which auto-derefs through both layers.

3. **Performance**: `load_full()` benches at **4.3 ns** on the spike
   hardware; `load_full().method()` at **4.3 ns** (LLVM optimises the
   second deref away because the trait method is virtual via vtable
   anyway). Well inside the 50 ns total emit budget.

4. **Memory ordering**: `ArcSwap` documents `load_full` as `Acquire`
   semantics on the inner pointer plus a `compare_exchange`-driven
   reclamation cycle. No torn reads possible.

## Decision

**GO**. The spec's typing works as stated. Performance budget validated.

Implementation note: per [99-key-decisions.md D5](../../specs/99-key-decisions.md)
we keep `from_pointee` for the construction (it composes naturally
with `Lazy::new`), and `observer()` returns the cloned `Arc<Arc<dyn>>`
directly. The double-`Arc` layer is a 16-byte pointer pair — negligible
versus the simpler call-site shape.

## Risks identified

None blocking. One minor: `ArcSwap::store(Arc::new(observer))` for
runtime swaps must match the stored type exactly (`Arc<Arc<dyn>>`),
which is what `install_observer` should construct. This is internal,
caught at the type level if we ever get it wrong.
