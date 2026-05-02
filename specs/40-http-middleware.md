# Design — HTTP Middleware (`obs-tower`)

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md), [20-otel-and-sinks.md § 2.6](./20-otel-and-sinks.md#26-trace-context-propagation)

This spec defines the `obs-tower` companion crate — a `tower::Layer`
that opens an `obs::scope!` per request, extracts/injects W3C trace
context, and emits typed HTTP request events.

> v3 changes: extracted from the v2 monolithic crates spec into a
> standalone surface so the contract is read alongside the trace-
> propagation spec, not buried in the workspace layout.

## 1. Public API

```rust
use obs_tower::{ObsHttpLayer, ObsHttpClientLayer};

let app = axum::Router::new()
    .route("/api/users", get(list_users))
    .layer(
        ObsHttpLayer::server()
            .with_route_extractor(|req| req.uri().path().to_string())
    );

let client = reqwest::Client::builder()
    .build()?
    .with_layer(ObsHttpClientLayer::new());
```

`ObsHttpLayer::server()` is for inbound request handling; it:

1. Extracts `traceparent` / `tracestate` headers (via
   `obs::propagator()::extract_w3c`); falls back to a freshly
   generated `ObsTraceCtx` when absent.
2. Opens an `obs::scope!(trace_id = …, span_id = …, sampled = …)`
   for the duration of the request.
3. Emits `ObsHttpRequestStarted` at request start.
4. Emits `ObsHttpRequestCompleted` at response end.
5. Drops the scope; tail-on-error flush triggers if any handler
   emitted ≥ ERROR.

`ObsHttpClientLayer::new()` is for outbound calls; it:

1. Reads the active `obs::scope!`'s `trace_id`/`span_id` (or
   generates fresh if no scope is active).
2. Calls `obs::propagator()::inject_w3c` to write `traceparent` /
   `tracestate` headers on the outgoing request.
3. Emits `ObsHttpClientCompleted` after the response (or error).

Both layers are framework-agnostic — work with `axum`, `hyper`,
`tonic`, anything tower-compatible.

## 2. Built-in events

`obs-tower` ships these schemas (in `obs-proto/builtin.proto`):

```proto
message ObsHttpRequestStarted {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_DEBUG };
  string  route       = 1 [(obs.v1.field) = { kind: LABEL,    cardinality: MEDIUM }];
  string  method      = 2 [(obs.v1.field) = { kind: LABEL,    cardinality: LOW    }];
  string  trace_id    = 3 [(obs.v1.field) = { kind: TRACE_ID                      }];
  string  parent_span = 4 [(obs.v1.field) = { kind: PARENT_SPAN_ID                }];
}

message ObsHttpRequestCompleted {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };
  string  route        = 1 [(obs.v1.field) = { kind: LABEL,    cardinality: MEDIUM }];
  string  method       = 2 [(obs.v1.field) = { kind: LABEL,    cardinality: LOW    }];
  string  status_class = 3 [(obs.v1.field) = { kind: LABEL,    cardinality: LOW    }];
  uint64  latency_ms   = 4 [(obs.v1.field) = { kind: MEASUREMENT,
                            metric: { kind: HISTOGRAM, unit: "ms",
                                      bounds: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000] } }];
  uint64  bytes_out    = 5 [(obs.v1.field) = { kind: MEASUREMENT,
                            metric: { kind: COUNTER, unit: "By" } }];
  string  trace_id     = 6 [(obs.v1.field) = { kind: TRACE_ID                       }];
  string  span_id      = 7 [(obs.v1.field) = { kind: SPAN_ID                        }];
  string  parent_span  = 8 [(obs.v1.field) = { kind: PARENT_SPAN_ID                 }];
}

message ObsHttpClientStarted   { /* mirrors RequestStarted with target_host */ }
message ObsHttpClientCompleted { /* mirrors RequestCompleted with target_host */ }
```

`status_class` is one of `2xx | 3xx | 4xx | 5xx | err` (transport
error). Keeps cardinality LOW.

The route extractor is user-supplied because frameworks differ on how
they expose the matched route (axum has `MatchedPath`; raw hyper
exposes nothing). Default extractor returns the request path verbatim.

## 3. Configuration

```rust
ObsHttpLayer::server()
    .with_route_extractor(|req| { /* return Cow<'static, str> */ })
    .with_propagator(obs::propagator::w3c())   // default; tracecontext
    .with_emit_started(false)                  // default; flip on for dev
    .with_emit_metrics(true)                   // default; emit MEASUREMENT histograms
    .with_status_classifier(|status| { /* custom status_class string */ })
    .with_per_request_observer(|req| { /* Option<Arc<dyn Observer>> */ })
```

The `with_emit_started` toggle defaults to OFF for the same reason
`#[obs::instrument]` defaults to one event ([13-emit-scope-and-filter.md § 5](./13-emit-scope-and-filter.md#5-the-obsinstrument-attribute)) —
double traffic for marginal value. Dev mode flips it on.

### 3.1 Multi-tenant: per-request observer override

`with_per_request_observer(closure)` is the production hook for
multi-tenant SaaS (per-tenant sinks, per-tenant retention) and for
on-demand live debug capture. The closure is invoked at request
entry; if it returns `Some(observer)`, the layer wraps the inner
service's response future in `Future::with_observer(observer)` per
[13-emit-scope-and-filter.md § 3](./13-emit-scope-and-filter.md#3-obsinstrumentedf--async-scope-and-observer-adapter).
Every event emitted while serving the request — including events
from spawned children that explicitly carry the override forward —
goes to the per-request observer.

```rust
let tenant_observers = Arc::new(TenantObserverRegistry::new(...));

let app = axum::Router::new()
    .route("/api/...", ...)
    .layer(ObsHttpLayer::server()
        .with_per_request_observer({
            let registry = tenant_observers.clone();
            move |req| req.headers()
                .get("x-tenant-id")
                .and_then(|h| h.to_str().ok())
                .and_then(|t| registry.observer_for(t))
        }));
```

When the closure returns `None`, no override is installed and events
flow to the global observer as usual. Returning `Some` for every
request is supported but trades the fast path described in
[11-runtime-core.md § 3](./11-runtime-core.md#3-the-observer-trait) —
each request pays one `task_local::sync_scope` per poll (~30 ns).

Tenant-observer lifetimes are the application's responsibility. A
typical pattern keeps observers in an `Arc<DashMap<TenantId,
Arc<dyn Observer>>>` and calls `observer.shutdown()` when a tenant
is decommissioned. The layer never drops or shuts down observers
itself.

## 4. Build dependencies

| Depends on | Provides |
| --- | --- |
| [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md) | `obs::scope!`, `Instrumented<F>` |
| [20-otel-and-sinks.md § 2.6](./20-otel-and-sinks.md#26-trace-context-propagation) | W3C propagator |

Ships in `obs-tower`. See [61-crates-and-features.md § 2.10](./61-crates-and-features.md#210-obs-tower).
