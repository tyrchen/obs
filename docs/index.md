# Docs Index

Internal engineering notes that aren't user-facing specs.

## Research

Spike memos and design research. Each spike addresses a single
load-bearing assumption that needs validating before relying on it.

| File | Purpose |
| --- | --- |
| [research/spike-arcswap.md](./research/spike-arcswap.md) | `ArcSwap<Arc<dyn Trait>>` shape with `Lazy::from_pointee` (Phase 0 risk retirement) |
| [research/spike-linkme.md](./research/spike-linkme.md) | `linkme::distributed_slice` cross-crate reliability under strip + LTO (Phase 0) |
| [research/spike-task-local.md](./research/spike-task-local.md) | `tokio::task_local!` cancellation behaviour with `select!` + `Drop` (Phase 0) |
| [research/spike-notify.md](./research/spike-notify.md) | `notify` file-watcher reliability on macOS APFS (Phase 0) |
| [research/spike-buffa-reflect.md](./research/spike-buffa-reflect.md) | `buffa-reflect` custom-option ergonomics on the FDS (Phase 0) |
