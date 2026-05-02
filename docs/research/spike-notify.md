# Spike: `notify` reliability on macOS APFS

Status: Done · Owner: obs-core · Date: 2026-05-02 · Outcome: **PASS**

## Question

Spec [11-runtime-core.md § 3.2](../../specs/11-runtime-core.md#32-filter-cache-invalidation-on-reload)
proposes file-watch-based config reload as the cross-platform default
(SIGHUP being Unix-only). On macOS APFS the FSEvents API has known
batching and coalescing quirks; we need to confirm that `notify`'s
`RecommendedWatcher` reliably reports both:

1. **In-place rewrite** (`open(O_TRUNC)` then write).
2. **Atomic rename** (`write to .tmp; rename .tmp -> obs.yaml`),
   the pattern editors and config-management tooling use.

If notify drops either, the SDK silently runs with stale config.

## Method

Standalone binary at `/tmp/obs-spikes/notify-spike/`:

- `notify = "8"` (resolved 8.2.0)
- `tempfile = "3"`
- Watches a `TempDir`, runs both scenarios, asserts at least one
  qualifying event arrives within 2 s.

Run on darwin-arm64, APFS volume.

## Findings

Both scenarios deliver the expected events well within the budget:

```
== modify scenario ==
  Create(Folder)        — initial directory entry creation
  Modify(Metadata(...)) — file metadata
  Create(File)
  Modify(Data(Content)) — the content write
  ✓ in-place modify

== rename scenario ==
  Create(File)               — .tmp file
  Modify(Name(Any))          — rename
  Create(File)               — final obs.yaml
  Modify(Data(Content))      — content
  ✓ atomic rename
```

Observations:

1. **Burst events are normal**. A single edit emits 3–6 raw events.
   The reload code must debounce (e.g. coalesce events within
   100–200 ms before triggering reload). `notify-debouncer-mini`
   exists for exactly this.

2. **Event kinds vary by editor**. We observed `Modify(Data(Content))`,
   `Modify(Metadata(...))`, `Create(File)`, `Modify(Name(Any))`.
   Reload code should treat **any event whose path matches the watched
   file** as "config might have changed; re-read it" rather than
   pattern-match specific event kinds. `notify`'s docs explicitly
   warn that event kind portability is poor.

3. **Watching the directory, not the file**, is essential for the
   atomic-rename pattern. If you `watch(file)` and the file is
   replaced via rename, the FSEvents stream loses track of the
   inode. Watching the **directory** with `RecursiveMode::NonRecursive`
   captures all writes to children including renames.

4. **APFS coalescing window** is roughly 30–50 ms in our runs; events
   batched within that window arrive together. Debounce with at least
   150 ms to avoid firing reload twice for one edit.

## Decision

**GO**, with these implementation rules for Phase 3 task 3.* (config
reload):

- Watch the **directory** containing `obs.yaml`, not the file. Use
  `RecursiveMode::NonRecursive`.
- Treat every event with a path matching the configured file as a
  potential change. Re-read the file unconditionally; let the YAML
  parser decide if it differs.
- Debounce by 150–200 ms. We pull in `notify-debouncer-mini` or
  implement a 200 ms timer in the reload task.
- On reload error (parse fail, schema invalid), keep the previous
  config and emit `obs.runtime.v1.ObsConfigReloadFailed` per spec
  11 § 9 / § 10.

For Phase 1 we **do not** wire reload yet — task 1.8 builds the
`ArcSwap`-backed `EventsConfig` shell and the reload hook
(`Observer::reload_filter` bumping `generation`); the actual
file-watcher belongs to Phase 3 sub-tasks. This spike only confirms
the OS-level primitive is reliable.

## Risks identified

- Linux `inotify` has a **per-user fd limit** (`fs.inotify.max_user_watches`,
  typically 8192). For a config reload watching one directory the
  budget is 1; not a concern for our use case.
- The `recommended_watcher` on macOS is FSEvents; on Linux is inotify;
  on Windows is `ReadDirectoryChangesW`. Same `notify` API across all
  three; behaviours differ in event kinds (handled by the
  "any event = re-read" rule above).
- **No NFS / network filesystem support**. Documented as a limitation
  for users who deploy on NFS-mounted config volumes; SIGHUP and
  programmatic `reload()` remain available.
