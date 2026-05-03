# obs SDK examples

Four runnable apps, all **proto-first**, all matching the shape that
`obs init --mode proto` scaffolds. Each one is a real service pattern,
not a feature smoke test — copy the one closest to what you're building.

| Example | Surface | What it shows |
| --- | --- | --- |
| [`todomvc`](./todomvc/) | full app | TodoMVC HTTP backend (axum + obs-tower) with 6 proto-defined events end-to-end; uses `obs validate` / `obs lint` / `obs schema show` / `obs doctor` / `obs tail` / `obs query` against its own NDJSON output. |
| [`interop-obs-host`](./interop-obs-host/) | tracing → obs | obs-first service whose 3rd-party deps emit via `tracing::*`; `obs_tracing_bridge::init(...)` funnels everything into one observer. |
| [`interop-tracing-host`](./interop-tracing-host/) | obs → tracing | Existing `tracing-subscriber::fmt` host adopts an obs-typed library; `ObsToTracingSink` re-emits typed obs events as `tracing::event!()` so the host's pipeline stays the same. |
| [`sinks-showcase`](./sinks-showcase/) | sinks fan-out | Same `.emit()` calls land in console (Pretty), Parquet (always), and OTLP (when `OTEL_EXPORTER_OTLP_ENDPOINT` is set). LOG/METRIC/AUDIT routed per-tier. |

## Shared layout

```
examples/<name>/
├── Cargo.toml             # deps: anyhow, buffa, obs-core, obs-sdk, ...; build-deps: anyhow, obs-build
├── build.rs               # obs_build::Config::new().files(...).include("proto").include_obs_options().compile()
├── proto/<pkg>/v1/events.proto   # messages with obs.v1.event + obs.v1.field annotations
├── README.md
└── src/
    ├── main.rs            # obs_sdk::include_schemas!("<pkg>.v1");  +  StandardObserver::builder()...
    └── ...
```

Every example's README walks through the CLI dogfooding loop:

```bash
obs validate examples/<name>/proto/<pkg>/v1/events.proto --include examples/<name>/proto
obs lint --schemas $(pwd)/examples/<name>/proto
obs schema show <pkg>.v1.<EventName> --schemas $(pwd)/examples/<name>/proto
obs doctor --root examples/<name>
cargo run -p obs-example-<name>
```

Pair with `obs tail --file <ndjson>` and `obs query --from <ndjson> --event <full_name>`
once an example writes NDJSON output.

## Run

```bash
cargo run -p obs-example-todomvc -- --port 8090
cargo run -p obs-example-interop-obs-host
RUST_LOG=info cargo run -p obs-example-interop-tracing-host
cargo run -p obs-example-sinks-showcase -- --requests 50
```

## Patterns not covered by a standalone example

A few SDK surfaces don't get their own crate because the API hook is
small enough that a 5-line snippet in the relevant README (or in the
SDK's own doc-comments) is more useful than a dedicated example:

- **Per-task observer routing** (`with_observer_task`) — see the
  *"Per-tenant routing"* section in [`todomvc/README.md`](./todomvc/README.md).
- **`obs::forensic!`** — emergency unstructured emit; see the macro's
  doc-comment in `obs-macros`. It's deliberately rate-limited and
  budget-capped; reach for a typed `#[derive(Event)]` first.
- **`obs::SpanTrace`** — snapshot of the active scope ancestry into an
  error type; see the type's doc-comment in `obs-core`.

Each example's README explains its inputs, expected output, and any
known gaps tracked in [`specs/`](../specs/).
