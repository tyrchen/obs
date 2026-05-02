# Design — `obs` CLI

Status: draft v2 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [crates-design.md](./crates-design.md), [schema-codegen-design.md](./schema-codegen-design.md)

> v2 changes: scope is **Rust only** in v1 — no Go/Python/TypeScript
> codegen here; query model is the single sparse `obs_events` table
> from the analytics design; `Obs*` event-name lint added; envelope
> field names shortened; ClickHouse migration emits one DDL.

## 1. Purpose

A single `obs` binary covers the developer-facing toolchain for the
wide-event SDK in a Rust workspace. It is the answer to "I have proto
files; now what?" and "what's in this batch?". It is **not** an agent
or a daemon; everything is short-lived, predictable, and CI-friendly.

The CLI **only** manages Rust crates. It does not generate code for
other languages. That is a deliberate scope choice for v1; see
[schema-codegen-design.md § 9](./schema-codegen-design.md#9-v1-scope-rust-only).

## 2. Top-level layout

```
$ obs --help
obs <command> [flags]

Authoring (Rust only)
  init           Scaffold a new schema crate (proto-first or rust-first)
  generate       One-shot codegen outside of build.rs (for IDE / CI inspection)
  doctor         Diagnose a crate's obs setup (deps, config, schema-source)

Schema governance
  lint           Run all schema lints over a crate
  validate       Validate one or more .proto files against obs/v1/options.proto
  diff           Compare two schema versions; emit breaking-change report
  audit          Roll up forensic-event budget, tracing-bridge usage

Data inspection
  decode         Decode a binary ObsBatch (file / stdin) to NDJSON
  query          Filter + project events from local NDJSON / Parquet / ClickHouse
  tail           Pretty-print events from a sink (file / OTLP receiver / stdin)
  schema show    Print the schema for one full_name (FIELDS, hash, etc.)

Backends
  migrate clickhouse   Emit DDL for the unified obs_events table
  migrate parquet      Emit unified Arrow schema JSON

Meta
  version        Print version + supported envelope format_ver values
  completions    Emit shell completion script (bash, zsh, fish, nu)
```

Global flags:

- `--root <dir>` (default `.`) — point to a workspace root
- `--config <file>` (default `obs.yaml`) — config file used by query/tail
- `--format <text|json|ndjson>` (default `text` on TTY, `ndjson` otherwise)
- `--no-color`, `--quiet`, `-v / -vv`

## 3. Commands

### 3.1 `obs init`

Scaffolds a new crate or adds wide-event support to an existing one.

```
obs init [--mode proto|rust] [--name myapp] [--package myapp.v1] <path>
```

Generates:

```
<path>/
├── Cargo.toml                # adds obs-sdk, obs-build deps; metadata.obs section
├── build.rs                  # obs_build::Config…compile()
├── proto/myapp/v1/events.proto    # one example event ObsHelloEmitted (only if mode=proto)
├── src/lib.rs                # include_schemas!("myapp.v1");
└── obs.yaml                  # default observer config
```

The example event is named `ObsHelloEmitted` to demonstrate the
naming convention (L011) and to give the user a concrete first emit
to copy from.

Idempotent: re-running on an existing crate only adds missing pieces
and prints a diff of any conflicts.

### 3.2 `obs generate`

One-shot codegen, identical to what `build.rs` runs, but writes
outputs to a specified directory for inspection / commit-into-repo
workflows.

```
obs generate \
    --files proto/myapp/v1/events.proto \
    --include proto \
    --out generated/ \
    [--no-arrow] [--no-render] [--no-scrub]
```

Useful for IDE plugins (display generated API) and CI checks (diff
`generated/` against the committed copy when projects choose to
vendor codegen output).

### 3.3 `obs doctor`

Diagnoses common configuration mistakes without touching the network.

```
$ obs doctor --root crates/myapp
✔ obs-sdk 0.1.4 in [dependencies]
✔ obs-build 0.1.4 in [build-dependencies]
✔ build.rs invokes obs_build::Config::compile()
✔ schema-source = "proto"; proto-root = "proto" exists with 3 .proto files
✔ All .proto files import obs/v1/options.proto
✔ All event names start with `Obs`
✘ Field `myapp.v1.ObsRequestCompleted.user_id` is LABEL with classification=PII
  → fix in proto/myapp/v1/events.proto:18
ℹ obs.yaml not found; observer will run with defaults

3 OK · 1 ERROR · 1 INFO
```

Exit code: `0` on all OK or warnings; `1` on any error.

### 3.4 `obs lint`

Runs every static schema check against a crate. Uses the same parser
that the proc-macro uses (`buffa-reflect` over the FDS), so results
match `cargo build` exactly.

```
obs lint [--root <dir>] [--strict] [--filter <pattern>]
```

Lints (each can be turned into a warning with `--allow <id>`):

| ID | Description |
| --- | --- |
| `L001` | LABEL field has cardinality > MEDIUM |
| `L002` | PII classification on a LABEL field |
| `L003` | SECRET classification on a LOG/AUDIT-tier event |
| `L004` | MEASUREMENT field missing `metric` annotation |
| `L005` | Enum used as LABEL has more variants than its declared cardinality cap |
| `L006` | TIER_AUDIT event has no PII/SECRET fields (suspicious) |
| `L007` | Field name not snake_case |
| `L008` | Field number reused after deletion (against `obs schema diff` history) |
| `L009` | Event has no fields (likely placeholder) |
| `L010` | Forensic budget exceeded for crate (per `metadata.obs.forensic_max`) |
| `L011` | Event message name does not start with `Obs` |
| `L012` | Envelope-shadowing field name (e.g. `ts_ns`, `service`, `instance`) on a payload |
| `L013` | LABEL field name on multiple events with conflicting types/cardinality |

```
$ obs lint --root crates/myapp
proto/myapp/v1/events.proto:18:3: error[L002] PII on LABEL field `user_id`
proto/myapp/v1/events.proto:42:1: error[L011] event name `RequestDone` must start with `Obs`

2 errors · 0 warnings · 12 events scanned
```

CI integrates this as `obs lint --strict` (warnings → errors).

### 3.5 `obs validate`

Lower-level validator for arbitrary `.proto` files. Used by IDE
language servers / `pre-commit` hooks where the lint context is just
a file path.

```
obs validate proto/myapp/v1/events.proto [more.proto ...]
```

Exit code: `0` if all parse and pass annotations, `1` otherwise.
Output is `<file>:<line>:<col>: <severity>: <msg>`.

### 3.6 `obs diff`

Compares two versions of a schema set and emits a breaking-change
report.

```
obs diff <baseline> <head>
```

Each argument is a directory containing `.proto` files (or a git ref
like `HEAD~1`).

```
$ obs diff origin/main HEAD
event myapp.v1.ObsRequestCompleted
  + field bytes_in (#8) ATTRIBUTE                       OK
  ~ field user_id classification INTERNAL → PII         OK (auto-redaction)
  ~ field tenant_id cardinality MEDIUM → HIGH           BREAKING (was a LABEL)
  - field deprecated_field (#5)                         BREAKING (tag reuse risk)
event myapp.v1.ObsCheckoutCompleted                     NEW

1 NEW · 2 BREAKING · 2 OK
```

Exit code: `0` if no breaking changes; `2` if breaking changes
detected (distinct from `1` which is "tool error").

### 3.7 `obs audit`

Aggregates governance metrics across a workspace.

```
obs audit [--root <dir>] [--budget budget.yaml]
```

Outputs:

```
$ obs audit --root .
Workspace: 42 events across 7 crates

Forensic events used:
  crates/billing       4/5 (budget OK)
  crates/checkout      6/5 (BUDGET EXCEEDED — see src/refund.rs:31)
  crates/inventory     0/5

Tracing-bridge events emitted last 7 days (from query --type tracing-forensic):
  highest:
    api::handlers   1.2M  ← suggests these should be typed events
    db::pool        430K

Audit-tier coverage:
  19 admin RPCs declared in proto, 16 emit AUDIT events.
  Missing: ObsAdminUserDelete, ObsAdminUserSuspend, ObsAdminFeatureFlagSet
```

`budget.yaml` is optional and overrides per-crate
`metadata.obs.forensic_max`.

### 3.8 `obs decode`

Read a binary `ObsBatch` and emit NDJSON. Works on files or stdin.

```
obs decode <file.bin>          # or:  cat file.bin | obs decode
obs decode --schemas proto/    # supply schemas dir for decoding payloads
obs decode --raw               # don't decode payload, just envelope + base64
```

Each output line is one event:

```json
{"ts":"2026-05-02T10:23:45.123Z","sev":"INFO","name":"myapp.v1.ObsRequestCompleted",
 "trace_id":"a1b2…","labels":{"route":"list_users","status":"ok","tenant_id":"acme"},
 "payload":{"user_id":"u-42","latency_ms":48,"bytes_out":2048}}
```

### 3.9 `obs query`

A small, batteries-included query CLI for the most common "show me
events" needs. Reads from local files (NDJSON or Parquet) or from
ClickHouse / object storage URIs (via `opendal`).

The query model is **the single sparse `obs_events` table**: every
filter, projection, and ordering is expressed against the
unified-table column set defined in
[crates-design.md § 2.8](./crates-design.md#28-obs-clickhouse).

```
obs query [SOURCE] [filters...]

Source:
  --from path/file.ndjson | path/dir/ | s3://bucket/prefix/ | clickhouse://...

Filters (composable):
  --since 1h | 30m | 2026-05-02T00:00:00Z
  --until <ts>
  --event myapp.v1.ObsRequestCompleted   (repeatable; full_name match)
  --severity warn|error|fatal
  --label route=list_users               (repeatable; AND of label matches)
  --trace <id>                            (single-trace pinpoint)
  --grep <substring>                      (substring on JSON-rendered payload)

Projection:
  --select ts_ns,labels.route,payload.latency_ms
  --limit 1000
  --order-by labels.route,payload.latency_ms desc
```

Output is NDJSON unless `--format text` and stdout is a TTY (then a
compact table is rendered).

The `query` command is not a replacement for ClickHouse / DuckDB —
it covers the on-the-fly debugging case ("grep the last hour's
batches for trace id X").

### 3.10 `obs tail`

Live tail. Three sources:

```
obs tail --file events.ndjson           # follow a file (like `tail -f`)
obs tail --otlp 0.0.0.0:4317            # spin up an OTLP gRPC receiver
obs tail --stdin                        # consume ObsBatch from stdin
```

In TTY mode, renders one event per line in the same format as the
`dev` sink (`StdoutSink`):

```
10:23:45.123 INFO  myapp.v1.ObsRequestCompleted route=list_users status=ok tenant=acme latency_ms=48
10:23:45.124 ERROR myapp.v1.ObsUpstreamFailed   route=list_users tenant=acme err=timeout
```

In `--format ndjson`, prints raw JSON suitable for piping into `jq`.

`--filter` accepts the same flags as `obs query` for narrowing.

### 3.11 `obs schema show`

Print everything we know about an event type.

```
$ obs schema show myapp.v1.ObsRequestCompleted
Event:        myapp.v1.ObsRequestCompleted
Tier:         LOG
Default sev:  INFO
Schema hash:  blake3:51e8f9...
Source:       proto/myapp/v1/events.proto:12

Fields:
  # NAME         KIND          CARD    CLASS     METRIC
  1 route        LABEL         MEDIUM  INTERNAL  -
  2 status       LABEL         LOW     INTERNAL  -
  3 tenant_id    LABEL         MEDIUM  INTERNAL  -
  4 user_id      ATTRIBUTE     HIGH    PII       -
  5 trace_id     TRACE_ID      UNB     INTERNAL  -
  6 latency_ms   MEASUREMENT   UNB     INTERNAL  histogram(ms,[1,5,10,25,50,100,250,500,1000,5000])
  7 bytes_out    MEASUREMENT   UNB     INTERNAL  counter(By)

Sinks projection (default config):
  LogSink:    1 record (all fields)
  MetricSink: 2 data points (latency_ms histogram, bytes_out counter)
              attributes: {route, status, tenant_id}
  TraceSink:  1 span if trace_id non-empty
  ParquetSink (Single layout): one row in obs_events.parquet
              populated columns: payload_myapp_v1_obs_request_completed.{route,status,...}
              all other payload_* columns NULL
```

### 3.12 `obs migrate clickhouse`

Emits **one** DDL for the unified `obs_events` table. The output is a
`.sql` file plus a metadata table that records which schema hashes
have been applied.

```
obs migrate clickhouse --root . --out migrations/0001_initial.sql
obs migrate clickhouse --root . --diff baseline=v1.4.0 --out migrations/0002.sql
```

DDL strategy (single table with sparse Nested columns, see
[crates-design.md § 2.8](./crates-design.md#28-obs-clickhouse) for the
template):

- Envelope columns are first-class
- Each event type contributes one `Nested(...)` column named
  `payload_<full_name_snake>`
- `LowCardinality` for LABEL fields, `String` for ATTRIBUTE, native
  numerics for MEASUREMENT
- Partition `BY toDate(ts_ns)`
- Order `BY (ts_ns, full_name, trace_id)`

Auto-migration in production is intentionally **not** wired into the
SDK (`obs-clickhouse::auto_migrate` is opt-in for dev only).
Production runs this CLI in CI/CD.

### 3.13 `obs migrate parquet`

Prints the unified Arrow schema JSON for the `obs_events` table —
used by external analytics writers (e.g. a dedicated Iceberg
compactor) to declare the target table layout without depending on
`obs-parquet` directly.

```
obs migrate parquet --root .
```

### 3.14 `obs version`

```
$ obs version
obs 0.1.4 (git: a1b2c3d, built: 2026-05-02)
envelope formats: 1
codegen targets: rust-buffa(0.4), arrow(53), clickhouse(0.13), otlp(1.x)
```

`--schema` prints just the format-version compatibility line; useful
in healthchecks.

### 3.15 `obs completions`

Emits a shell completion script.

```
obs completions bash > /etc/bash_completion.d/obs
obs completions nu   > ~/.config/nushell/completions/obs.nu
```

## 4. Implementation notes

- `obs-cli` is a single binary built from `apps/obs-cli`.
- Uses `clap` v4 with the derive API.
- Subcommands are grouped using `clap`'s `subcommand_required(true)`.
- All commands are idempotent and print structured output (NDJSON)
  when stdout is not a TTY, suitable for CI pipelines.
- Heavy commands (`query`, `decode`) stream — they never load a
  whole batch into memory.
- The CLI shares parsers with `obs-build` (both use
  `buffa-reflect::DescriptorPool`) so behaviour is identical.
- For `query --from clickhouse://`, depend on `obs-clickhouse`
  behind a `clickhouse` cargo feature.
- The CLI prints to stderr only operational noise; events / data go
  to stdout. This keeps `obs query | jq` etc. clean.

## 5. CI workflow integration

A typical `.github/workflows/obs.yml`:

```yaml
- name: obs lint
  run: obs lint --strict --root .

- name: obs diff (vs main)
  run: obs diff origin/main HEAD
  # exits 2 on breaking changes

- name: obs migrate (preview)
  run: obs migrate clickhouse --diff origin/main..HEAD --out preview.sql
  # PR comment posts the migration diff
```

The `obs lint --strict` step gates merge; `obs diff` gates
breaking-change PRs. `obs audit` runs nightly and posts a Slack
summary.

## 6. Out of scope

- Codegen for non-Rust languages (deferred to post-1.0).
- A dashboarding UI (use Grafana/Honeycomb against the same backends).
- Multi-tenant authorization (the CLI runs locally against local
  data / user-scoped credentials).
- Cross-cluster replay tooling — that lives in the OTel Collector
  ecosystem.
