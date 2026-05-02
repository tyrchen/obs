# Spike: `tokio::task_local!` cancellation behaviour

Status: Done · Owner: obs-core · Date: 2026-05-02 · Outcome: **PASS**

## Question

Spec [11-runtime-core.md § 8.1](../../specs/11-runtime-core.md#81-async-cancellation)
states the scope frame `Drop` runs the flush-or-discard logic and that
tokio guarantees `Drop` runs even on cancellation. Spec
[13-emit-scope-and-filter.md § 3](../../specs/13-emit-scope-and-filter.md#3-obsinstrumentedf--async-scope-adapter)
relies on `tokio::task_local!` semantics for `OBSERVER_TASK`. Three
properties to confirm:

1. `Drop` of a guard living inside `task_local::scope()` runs when a
   `select!` cancels the future on the timeout branch.
2. The task-local value is still accessible from inside `Drop` (so
   the scope frame's flush logic can read its own data).
3. `tokio::spawn` does NOT inherit the parent's task-local value
   (matches the cross-task propagation table in spec 11 § 3.1).

## Method

Standalone binary at `/tmp/obs-spikes/task-local-spike/`, multi-thread
runtime with 4 worker threads, three scenarios validated:

1. `select!` between a busy loop holding a `DropGuard` and a 20 ms
   `sleep`. Sleep wins; assert the guard's `fired` and `saw_value`
   atomics are both set.
2. Spawn a task inside a `task_local::scope()`. Outer and inner each
   record whether `OBSERVER.try_with(|_| ())` succeeded.
3. Nested `OBSERVER.scope("outer", OBSERVER.scope("inner", ...))`.
   Assert outer sees `"outer"`, inner sees `"inner"`, and outer is
   restored after inner returns.

## Findings

All three scenarios pass:

```
== test 1: drop guard runs on select! cancellation ==
  drop guard saw OBSERVER = "tenant-A"
  timeout fires, cancellable_work cancelled
  OK
== test 2: task_local does NOT propagate across spawn ==
  outer saw OBSERVER, spawned task did not — confirmed
  OK
== test 3: nested scopes stack LIFO ==
  outer→inner→outer LIFO stacking confirmed
  OK
```

Detailed observations:

1. **Cancellation runs `Drop`**: tokio's `select!` drops the losing
   branch's future, which runs every guard inside in stack order. This
   is documented in `tokio::select!` and confirmed end-to-end.

2. **Task-local visible during `Drop`**: the `task_local::scope` future
   is still active while its inner future is being dropped, so
   `OBSERVER.try_with(|o| ...)` succeeds inside guards owned by the
   inner future. This is what spec 11 § 8.1 requires (the scope frame's
   `Drop` reads its own `fields` to flush the tail buffer).

3. **No cross-spawn propagation**: `tokio::spawn(...)` inside a
   `task_local::scope` produces a brand-new task that does not see
   the parent's task-local. This matches the propagation table.
   `OBSERVER.try_with` returns `Err(AccessError)` in the spawned task.
   This is the property that necessitates `Future::with_observer`
   wrapping every `tokio::spawn` that should keep the per-task observer.

## Decision

**GO**. The semantics the spec relies on are real and stable.

Implementation notes for Phase 1 task 1.6:

- The `OBSERVER_TASK` task-local can carry the per-task observer
  reliably across `.await` and through cancellation; the scope guard's
  `Drop` impl can synchronously push tail-buffer envelopes onto the
  per-tier `mpsc` channel (which is non-blocking; channel-full just
  drops, per spec 11 § 8.1).
- `tokio::spawn` callers that need per-tenant routing must wrap the
  child future with `Future::with_observer`. Tests for this in
  Phase 2 task 2.6 (`#[obs::test]`) and Phase 4B task 4B.9
  (multi-tenant integration).

## Risks identified

None blocking. Minor: the spec's claim that `Drop` is *guaranteed* to
run is correct only as long as the future is **dropped** (not leaked).
A `tokio::task::JoinHandle` that is never awaited and whose task is
never aborted will not drop its inner future. This is normal Rust
behaviour, not specific to obs; document in 13 § 3 if not already.
