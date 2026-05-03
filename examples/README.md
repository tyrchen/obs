# obs SDK examples

Six runnable apps. The first three are **canonical, proto-first** end-to-end
examples that mirror what `obs init --mode proto` scaffolds — they are the
reference shape for new services. The bottom three cover narrower
ergonomics surfaces (`derive(Event)`, per-task observer routing, forensic
events) and use the rust-first authoring path.

| Example | Authoring | Surface | What it shows |
| --- | --- | --- | --- |
| [`todomvc`](./todomvc/) | proto-first | full app | TodoMVC HTTP backend with proto-defined events end-to-end; uses `obs validate` / `obs lint` / `obs schema show` / `obs doctor` / `obs tail` / `obs query` against its own NDJSON output. |
| [`interop-obs-host`](./interop-obs-host/) | proto-first | tracing → obs | obs-first service whose 3rd-party deps emit via `tracing::*`; `obs_tracing_bridge::init(...)` funnels everything into one observer. |
| [`interop-tracing-host`](./interop-tracing-host/) | proto-first | obs → tracing | Existing `tracing-subscriber::fmt` host adopts an obs-typed library; `ObsToTracingSink` re-emits typed obs events as `tracing::event!()` so the host's pipeline stays the same. |
| [`sinks-showcase`](./sinks-showcase/) | proto-first | sinks fan-out | Same `.emit()` calls land in console (Pretty), Parquet (always), and OTLP (when `OTEL_EXPORTER_OTLP_ENDPOINT` is set). LOG/METRIC/AUDIT routed per-tier. |
| [`multi-tenant`](./multi-tenant/) | rust-first | per-task routing | `with_observer_task` — each tenant runs under its own observer so emits land in tenant-scoped sinks. |
| [`forensic-and-spantrace`](./forensic-and-spantrace/) | rust-first | escape hatches | `obs::forensic!` (rate-limited unstructured emit) + `obs::SpanTrace` (snapshot of the active scope ancestry). |

## The proto-first canon

`todomvc`, `interop-obs-host`, `interop-tracing-host`, and `sinks-showcase`
all follow the same shape:

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
cargo run -p obs-example-multi-tenant
cargo run -p obs-example-forensic-and-spantrace
```

Each example's README explains the inputs, expected output, and any
known gaps tracked in [`specs/`](../specs/).
