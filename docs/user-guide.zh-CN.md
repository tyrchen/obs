# obs — 用户指南

> **目标读者：** 在 Rust 服务里要为业务埋点 / 接入可观测性的应用工程师与 SRE。
> **目标：** 安装 SDK、撰写第一个事件、把它接到 logs / metrics / traces / 分析，
> 并在生产环境里运维它。

其他指南：
[开发者指南](./dev-guide.zh-CN.md) ·
[从 `tracing` 迁移](./migration-from-tracing.md)（English） ·
[English User Guide](./user-guide.md) ·
[Specs 索引](../specs/index.md)

---

## 目录

1. [一句话介绍 `obs`](#1-一句话介绍-obs)
2. [安装](#2-安装)
3. [60 秒快速开始](#3-60-秒快速开始)
4. [心智模型](#4-心智模型)
5. [撰写事件](#5-撰写事件)
6. [发射事件](#6-发射事件)
7. [Scope、context 与 trace 关联](#7-scopecontext-与-trace-关联)
8. [过滤与采样](#8-过滤与采样)
9. [Sinks](#9-sinks)
10. [HTTP 中间件（`obs-tower`）](#10-http-中间件obs-tower)
11. [桥接 `tracing`](#11-桥接-tracing)
12. [配置（`obs.yaml`）](#12-配置obsyaml)
13. [多租户 / 每任务 observer](#13-多租户--每任务-observer)
14. [CLI](#14-cli)
15. [测试你的代码](#15-测试你的代码)
16. [运维](#16-运维)
17. [常见问题](#17-常见问题)

---

## 1. 一句话介绍 `obs`

`obs` 是一个面向 Rust 服务的、schema-first 的宽事件 SDK。你为每个**逻辑操作**
撰写**一个有类型的事件**；同一次 `.emit()` 调用会以**一条日志记录、一组 metric 数据点、
（可选地）一个 trace span，以及单个稀疏 Parquet/ClickHouse 表里的一行**这种形式落地 —
全部出自同一个定义。SDK 会在编译期强制 label 基数、分类（PII / SECRET）和命名约定，
所以那种会半夜 oncall 的错误（label 爆炸、密钥进入日志、跨服务字段名漂移）会变成
编译错误。

如果你读过 PRD，本指南是它的运维侧对偶。如果没读过，[`specs/00-prd.md`](../specs/00-prd.md)
解释了**为什么**这么设计。

---

## 2. 安装

### 2.1 作为库（在你的服务里）

在 `Cargo.toml` 中加入门面与构建辅助：

```toml
[dependencies]
obs-sdk = { version = "0.1", features = ["otel", "parquet"] }

[build-dependencies]
obs-build = "0.1"      # 仅 proto-first 撰写时需要

[package.metadata.obs]
schema-source = "proto"     # 或 "rust"（Rust-first）；二者不能并存
proto-root    = "proto"     # 仅 proto-first 时需要
forensic_max  = 5           # 每个 crate 的 `obs::forensic!` 预算
```

`obs-sdk` 的默认 features：`dev`（StdoutSink）、`otel`（OTLP gRPC + HTTP）、
`panic-hook`（panic 时发 FATAL）。可选 features 包括 `parquet`、`clickhouse`、
`tracing-bridge`、`tower`、`test`。要剥到只剩核心 API，加 `default-features = false`。

### 2.2 作为 CLI（一个二进制，可选）

```bash
cargo install --path apps/obs-cli           # 从 workspace checkout 安装
# 或者，发布之后：
cargo install obs-cli
obs --version                               # 健康检查
```

你**不需要**靠 CLI 才能使用 SDK — 服务直接链接 `obs-sdk` 即可。CLI 用于撰写
（`init`、`validate`、`lint`、`diff`、`schema show`）、检查（`tail`、`query`、`decode`），
以及后端管线（`migrate`）。

---

## 3. 60 秒快速开始

```bash
cargo new myapi --bin && cd myapi
obs init --mode rust .          # 生成 src/events.rs + obs.yaml + main.rs
cargo run
# → 1730000000.000000000 INFO  myapi.v1.ObsHelloEmitted who=world
```

`obs init` 写出三个文件：

- `src/events.rs` — 一个示例 `ObsHelloEmitted`，带一个 `LABEL` 字段。
- `obs.yaml` — 运行时配置（filter、sampling、limits、sinks）。
- `src/main.rs` — 安装 `StandardObserver::dev()`。

这就是全部设置。`obs init --mode proto .` 做同样的事，但用 `.proto` schema +
`build.rs`，而不是 `#[derive(Event)]`。

---

## 4. 心智模型

> **一个事件 = 一条日志记录 + N 个 metric 数据点 +（可选）一个 span + 一行分析行，
> 全部来自一次 `.emit()` 调用。**

| 概念 | 含义 |
| --- | --- |
| **事件（Event）** | 一个有类型的 Rust struct（或 `.proto` 消息），完整描述一件发生过的逻辑事项。名字以 `Obs` 开头（如 `ObsRequestCompleted`）。 |
| **Tier** | 路由提示：`LOG`、`METRIC`、`TRACE` 或 `AUDIT`。决定哪条 sink 链收到 envelope。AUDIT 是持久化的（永不静默丢弃）。 |
| **Severity** | 与 OTel 对齐的 `Trace..Fatal`。Schema 声明 `default_sev`；`.emit_at(Severity::Warn)` 可向上**或向下**覆盖。 |
| **字段角色（FieldKind）** | 每个字段都有一个角色：`LABEL`、`ATTRIBUTE`、`MEASUREMENT`、`TRACE_ID`、`SPAN_ID`、`PARENT_SPAN_ID`、`TIMESTAMP_NS`、`DURATION_NS`、`FORENSIC`。 |
| **Cardinality（基数）** | `LOW < 10 · MEDIUM < 10k · HIGH < 1M · UNBOUNDED`。LABEL 字段**必须**是 Low 或 Medium — 在 label 上设高基数是编译错误。 |
| **Classification（分类）** | `INTERNAL` / `PII` / `SECRET`。驱动运行期清洗。 |
| **Observer** | 拥有 sinks 与各 tier worker pool 的唯一调度者。三级解析：每任务 → 每线程 → 全局。 |
| **Sink** | 一个有类型的 `ScrubbedEnvelope<'_>` 消费者（`StdoutSink`、`OtlpLogSink`、`ParquetSink`、…），绑定到某个 tier。Sink 看到的是 `ScrubbedEnvelope<'_>`，而非原始 payload。 |
| **Scope** | 一个 RAII guard，持有 label 白名单 + 64 项 tail-on-error 环缓。**不是** `tracing::Span` 的对应物。 |
| **Schema registry** | 启动期通过 `linkme` 装配的进程级目录；sinks 用它来 decode payload。 |

用户的工作是定义事件**形状**一次。运行时负责 fan-out。

---

## 5. 撰写事件

### 5.1 Rust-first（`#[derive(Event)]`）

```rust
use obs_sdk::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsRequestCompleted {
    #[obs(label, cardinality = "medium")]
    pub route: Route,

    #[obs(label, cardinality = "low")]
    pub status_class: StatusClass,

    #[obs(attribute, cardinality = "high", classification = "pii")]
    pub user_id: UserId,

    #[obs(measurement, metric(histogram, unit = "ms",
        bounds = [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 5_000]))]
    pub latency_ms: u64,

    #[obs(trace_id)]
    pub trace_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, obs_sdk::EnumLabel)]
pub enum StatusClass { TwoXx, ThreeXx, FourXx, FiveXx }
```

宏会生成 `EventSchema` 实现、`linkme` 收集的 registry 注册项、有类型 builder，
以及编译期 lint 断言。完整的属性语法见
[spec 12 § 4](../specs/12-schema-and-codegen.md)。

### 5.2 Proto-first（`obs-build`）

```proto
// proto/myapi/v1/events.proto
syntax = "proto3";
package myapi.v1;
import "obs/v1/options.proto";

message ObsRequestCompleted {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };

  Route        route        = 1 [(obs.v1.field) = { kind: LABEL, cardinality: MEDIUM }];
  StatusClass  status_class = 2 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
  string       user_id      = 3 [(obs.v1.field) = { kind: ATTRIBUTE,
                                                    cardinality: HIGH,
                                                    classification: PII }];
  uint64       latency_ms   = 4 [(obs.v1.field) = { kind: MEASUREMENT,
                                                    metric: { kind: HISTOGRAM, unit: "ms",
                                                              bounds: [1,5,10,25,50,100,250,500,1000,5000] } }];
  string       trace_id     = 5 [(obs.v1.field) = { kind: TRACE_ID }];
}
```

```rust
// build.rs
fn main() -> anyhow::Result<()> {
    obs_build::Config::new()
        .files(&["proto/myapi/v1/events.proto"])
        .include("proto")
        .include_obs_options()
        .out_dir(std::env::var("OUT_DIR")?)
        .compile()?;
    Ok(())
}

// src/lib.rs
obs_sdk::include_schemas!("myapi.v1");
```

两种模式生成**字节级一致**的 `EventSchema` 实现。**多服务**或希望 schema
存在于 Rust 之外时选 proto；**单 binary 拥有 schema** 时选 Rust-first。
**单 crate 内混用是编译错误。**

### 5.3 命名约定

- **事件名：** `Obs<概念>` + 过去式（`ObsRequestCompleted`、`ObsUserSignedUp`、
  `ObsCheckoutAbandoned`）。长操作的两端用 `Obs<概念>Started` 配对
  `Obs<概念>Completed` — OTel sink 会把这对合并成一个 span。
- **字段名：** `snake_case`、有描述性、独立时带单位（`latency_ms`、`bytes_out`）。
- **enum 变体：** `PascalCase`、不带前缀。
- **workspace 全局覆盖：** `Obs` 前缀由 lint L011 强制。要改前缀，在顶层
  `Cargo.toml` 设 `[workspace.metadata.obs] event_prefix = "Evt"`（SDK 自带的
  `obs.runtime.v1.*` 事件不受影响）。

### 5.4 编译期 Lints（L001–L013）

| ID | 捕获的问题 | 为什么重要 |
| --- | --- | --- |
| **L001** | `LABEL` 字段的 `cardinality = High`/`Unbounded`。 | Label 会变成 metric attribute；高基数会撑爆 TSDB 索引。 |
| **L002** | `LABEL` 字段被分类为 `PII`。 | PII label 会无限期地泄漏到所有 vendor 后端。 |
| **L003** | `LOG` 或 `AUDIT` tier 事件包含 `SECRET` 字段。 | 这两个 tier 都是持久化的，密钥永远不该落地。 |
| **L004** | `MEASUREMENT` 字段缺 `MetricSpec`。 | metric sink 没有元信息无法投影。 |
| **L005** | 用作 `LABEL` 的 enum 超过基数上限。 | 在编译期、而非看板上发现 enum 变体爆炸。 |
| **L006** | `AUDIT` 事件没有 PII/SECRET 字段。 | 可疑 — AUDIT 就是为敏感事项而存在；warn。 |
| **L007** | 字段名不是 `snake_case`。 | 跨服务一致性。 |
| **L008** | 复用先前删除的 proto tag。 | 会破坏 Parquet/ClickHouse 历史 NULL 语义。 |
| **L009** | 空事件（无字段）。 | 几乎一定是 bug。 |
| **L010** | 该 crate 的 `obs::forensic!` 预算超额。 | Forensic 是逃生口；预算约束让团队保持自律。 |
| **L011** | 事件名不以 `Obs`（或 workspace 前缀）开头。 | 调用点的视觉识别。 |
| **L012** | 字段名遮蔽 envelope 名（`ts_ns`、`service`、…）。 | 会在 sink 时静默覆盖 envelope 列。 |
| **L013** | 跨事件同名 `LABEL` 字段，但类型/基数/分类冲突。 | 跨事件同名 label 必须代表同一维度。 |

每条 lint 都给出指向文件、行号和修复建议的可操作错误。示例消息见
[spec 60 § 6](../specs/60-dev-ergonomics.md#6-compile-error-quality)。

---

## 6. 发射事件

`obs` 提供**两种等价的发射形式**。Builder 是规范形式（文档与 AI 提示词都默认用它）；
宏是简写。

### 6.1 Builder（规范）

```rust
ObsRequestCompleted::builder()
    .route(Route::ListUsers)
    .status_class(StatusClass::TwoXx)
    .user_id(uid)
    .latency_ms(elapsed.as_millis() as u64)
    .emit();
```

- `rust-analyzer` 的链式补全在 `::builder().` 之后立即可用。
- 缺必填字段的错误精确指向 `.emit()`（typed-builder 的标记类型在缺 setter 时拒绝编译）。
- `.emit_at(Severity::Warn)` 可向上或向下覆盖严重度。

### 6.2 宏（简写）

```rust
obs_sdk::emit!(ObsHelloEmitted { who: Audience::World });
obs_sdk::emit!(WARN, ObsUpstreamFailed { route, error_kind });
```

只在 struct 字面量真的更可读时（一两个字段的事件、或简短的 severity 升级）用宏。
裸 severity 标识符（`TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR`/`FATAL`）从 `obs_sdk`
re-export 出来，方便符合人体工学地用。

### 6.3 `.emit()` 之后发生了什么

1. **静态 `ObsCallsite::enabled` 检查** — 一次原子 load。如果 filter 说 "never"，立即返回（~25 ns）。
2. **`Observer::enabled`** — 仅当缓存说 "sometimes" 时才调用（generation 可能变了）。
3. **`EventSchema::project`** — 填充 envelope label 并从活跃 scope 帧自动填补缺失的 setter。
4. **Head 采样器** — 速率或每事件覆盖。
5. **Tail-on-error 缓冲** — 推入活跃 scope 的 64 项环缓。
6. **`mpsc::try_send`** 至每 tier worker。**热路径在此返回**（在 2024 笔记本上端到端约 1 µs，
   `StandardObserver` + 空 sink）。
7. （worker 线程）scrubber → `ScrubbedEnvelope<'_>` → `Sink::deliver`。

发射线程除 `AUDIT`（见 § 16.3）外永远不为 sink 阻塞。

---

## 7. Scope、context 与 trace 关联

```rust
async fn handle(req: Request) -> Response {
    let _scope = obs_sdk::scope!(
        trace_id  = req.id.clone(),
        tenant_id = req.tenant.clone(),
    );

    ObsRequestStarted::builder()
        .route(req.route())
        .emit();      // trace_id 自动从 scope 填入

    let started = std::time::Instant::now();
    let resp    = serve(req).await;

    ObsRequestCompleted::builder()
        .route(resp.route())
        .status_class(resp.status_class())
        .latency_ms(started.elapsed().as_millis() as u64)
        .emit();      // trace_id + tenant_id 自动填入

    resp
    // _scope 在此 drop。如果 serve() 期间发了 ERROR，tail buffer 会 flush —
    // 这个 scope 下之前被采样掉的 TRACE/DEBUG 事件全都会发出。
    // 否则，它们被丢弃。
}
```

### 7.1 `scope!` vs `context!`

- **`obs_sdk::scope!`** — 同时绑定字段白名单**和** 64 项 tail-on-error 环缓。
  在请求边界使用。
- **`obs_sdk::context!`** — 同样的字段白名单，但**没有** tail buffer。
  在深层嵌套辅助函数里用，避免重复开新环缓。

### 7.2 `scope!` **不是** `tracing::Span`

- 没有起止时间。没有 enter/exit 周期。
- 没有事后 `Span::record`。
- 想要 "进入函数 / 函数返回 + 时长"，用 `#[obs::instrument]` 或 Started/Completed 事件对。

### 7.3 `tokio::spawn` 默认会丢失 scope

```rust
tokio::spawn(
    background_audit(req_id)
        .instrument(scope_clone)              // 跨 .await 携带 scope
        .with_observer(obs_sdk::observer()),  // 也携带 observer
);
```

`instrument(...)` 与 `.with_observer(...)` 都填入同一 `Instrumented<F>` 包装器
的两个正交槽位。不显式传递的话，spawn 出去的任务看到的是 *全局* observer 且
*没有* scope。

### 7.4 `#[obs::instrument]`

```rust
#[obs::instrument(fields(route, tenant_id), skip(raw_body))]
async fn handle_list_users(req: Request, raw_body: Bytes) -> Response {
    // 函数体里发的事件会自动带上 trace_id、route、tenant_id
}
```

默认在返回时发**一个** `ObsFnExecuted` 事件，含 `latency_ns`。加上 `enter = true`
切回老的两事件（`ObsFnEntered` + `ObsFnExecuted`）形态。

---

## 8. 过滤与采样

### 8.1 过滤 DSL

`obs::Filter` 把 `tracing-subscriber::EnvFilter` 的语法**逐字移植** —
`RUST_LOG` 风格的字符串可以直接用：

```bash
OBS_FILTER="info,myapi::auth=debug,myapi.v1.ObsRequestCompleted=trace"
OBS_FILTER='info,myapi.v1.ObsRequestCompleted[route=admin]=trace'
```

`[field=value]` 子句**只**匹配 envelope 的 `labels`（即 LABEL kind）。在 `ATTRIBUTE`
字段上的过滤子句会静默匹不到 — `obs lint` 在这种写法上会 warn。

### 8.2 采样顺序

每次 emit 按以下顺序：

1. **入站 W3C `traceparent.sampled`** — 如果请求带了已传播的决定，遵从它（设为开则
   始终 emit、清掉则始终 drop）。可在 `obs.yaml` 设 `sampling.honour_traceparent_sampled = false` 关闭。
2. **Head 采样器** — 每 `(full_name, severity)` 一次 `f64` 比较。固定 seed，可重现。
3. **Tail-on-error** — 每 scope 环缓；只要同 scope 内任何事件触发 ERROR 或 FATAL，
   缓冲就 flush。

### 8.3 每事件覆盖

```yaml
# obs.yaml
filter: "info,myapi::cache=debug"
sampling:
  default_rate: 1.0
  per_event:
    "myapi.v1.ObsCacheLookup": 0.01      # 取样 1% — 高频事件
    "myapi.v1.ObsHealthcheck": 0.001
  always_log_at_or_above: warn          # 重要事件绕过采样
```

### 8.4 热重载

编辑 `obs.yaml` 然后：

- 发 `SIGHUP`（Unix），或者
- 在构建 observer 时启用 `reload_on_sighup()` / `notify` watcher（推荐，跨平台）。

`StandardObserver::reload_config` 会原子地换掉 filter，并把每个 callsite 的 generation
加一；下一次 emit 会重新探测 `Interest` 状态。解析失败保留旧配置，并发出
`ObsConfigReloadFailed`。

哪些可以热重载（✅）、哪些不行（❌）：

| 设置 | 热重载？ |
| --- | --- |
| `filter`、`sampling.*`、`limits.*`、`audit.on_failure` | ✅ |
| Sink 端点（部分） | ⚠ 部分 |
| `audit.channel_capacity`、队列大小、`service.*`、parquet/CH URL | ❌ 重启 |

---

## 9. Sinks

Sink 是有类型的 `ScrubbedEnvelope<'_>` 消费者。在构建 observer 时把 sink 绑到 tier 上：

```rust
use obs_sdk::*;

let observer = StandardObserver::builder()
    .service("myapi", env!("CARGO_PKG_VERSION"))
    .instance(hostname::get()?.to_string_lossy().into_owned())
    .sink_for(Tier::Log,    NdjsonFileSink::new("./events.ndjson")?)
    .sink_for(Tier::Metric, otel::OtlpMetricSink::from_env()?)
    .sink_for(Tier::Trace,  otel::OtlpTraceSink::from_env()?)
    .sink_for(Tier::Audit,  ParquetSink::builder()
        .base_dir("/var/log/audit-parquet")
        .build()?)
    .config_from_yaml_path("./obs.yaml")?
    .reload_on_sighup()
    .build()?;

install_observer(observer);
```

### 9.1 内置 sink

| Sink | 用途 | Crate |
| --- | --- | --- |
| `StdoutSink` | Pretty / Compact / Full / JSON 格式化到 stdout | `obs-core` |
| `NdjsonFileSink` | NDJSON 写文件（可选 rolling） | `obs-core` |
| `RollingFileWriter` | 按日 / 按时 / 按大小切分 | `obs-core` |
| `NonBlockingWriter` | 受限 mpsc + 后台线程；溢出时丢弃 | `obs-core` |
| `OtlpLogSink` / `OtlpMetricSink` / `OtlpTraceSink` | OTLP gRPC + HTTP | `obs-otel` |
| `ParquetSink` | 单稀疏 `obs_events` 表，支持本地 / S3 / GCS / Azure | `obs-parquet` |
| `ClickHouseSink` | 原生批量 INSERT 到单一 `obs_events` 表 | `obs-clickhouse` |
| `InMemoryObserver` 自带的 sink | 仅测试用；环缓 + `drain()` / `wait_for(...)` | `obs-core`（test feature） |

### 9.2 Stdout 格式化样式

```rust
StdoutSink::builder()
    .formatter(FormatterStyle::Json)        // Full | Compact | Pretty | Json
    .make_writer(LevelSplitWriter::new(StdoutWriter, StderrWriter))  // INFO→stdout, WARN+→stderr
    .build()?;
```

### 9.3 OTLP

```rust
// 12-factor: 读取 OTEL_EXPORTER_OTLP_ENDPOINT 等环境变量。
let (logs, metrics, traces) = obs_otel::otlp_trio_from_env()?;

let observer = StandardObserver::builder()
    .service("myapi", env!("CARGO_PKG_VERSION"))
    .sink_for(Tier::Log,    logs)
    .sink_for(Tier::Metric, metrics)
    .sink_for(Tier::Trace,  traces)
    .build()?;
```

OTLP sink 的语义：

- **`OtlpLogSink`** — 1:1 `LogRecord` 映射。`event.name = full_name`、
  `obs.schema_hash` attribute、原始 payload 字节、severity 按 bucket-floor 映射到
  `SeverityNumber`。
- **`OtlpMetricSink`** — 每个 `MEASUREMENT` 字段一个数据点；instrument 名为
  `<full_name>.<field>`。按 schema 的 `MetricSpec` 选择 Counter / Gauge / Histogram。
- **`OtlpTraceSink`** — `*Started`/`*Completed` 事件对会合并成一个 span；`*Started`
  变成 span event。带 duration 字段的单事件变成完整 span；一次性事件变成零时长 span。

服务身份（`service`/`instance`/`version` + 任何 `resource_attr(...)` 调用）只放在
OTel `Resource` 上一次。每事件 attribute 永不重复它。

### 9.4 分析：Parquet + ClickHouse

```rust
// Parquet — 单稀疏表，S3 兼容
ParquetSink::builder()
    .base_dir("s3://obs-events/myapi/")
    .layout(ParquetLayout::Single)              // 默认
    .roll_max_bytes(256 * 1024 * 1024)
    .roll_max_age(Duration::from_secs(300))
    .compression(ParquetCompression::Zstd)
    .partition_by(&["service", "date"])
    .build()?;

// ClickHouse — 同样的 schema 形态、原生 MergeTree
ClickHouseSink::builder()
    .url("http://clickhouse:8123")
    .database("obs")
    .table("obs_events")
    .batch_size(8192)
    .build()?;
```

两者都写入单一稀疏 `obs_events` 表，每个事件类型对应一个可空 struct 列。
跨事件 join 是一次查询；新事件追加一列、旧行 NULL。从 CLI 拿到 ClickHouse 的 DDL：

```bash
obs migrate clickhouse --schemas $(pwd)/proto > obs_events.sql
```

`auto_migrate` 默认为 **false**；生产环境通过常规 CI/CD 路径走 DDL。

---

## 10. HTTP 中间件（`obs-tower`）

```rust
use axum::{Router, routing::get};
use obs_tower::server::ObsHttpLayer;

let app = Router::new()
    .route("/api/users",    get(list_users))
    .route("/api/users/:id", get(get_user))
    .layer(
        ObsHttpLayer::server()
            .with_route_extractor(|req| req.uri().path().to_string())
    );
```

每请求都会做：

- 从 header 解析 `traceparent`/`tracestate`（W3C）。如果没有，生成新的 `ObsTraceCtx`。
- 为请求生命周期开一个 `scope!(trace_id, span_id, sampled)`。在 `list_users` /
  `get_user` 内部，每次 `.emit()` 都会继承 trace 上下文。
- 入口处发 `ObsHttpRequestStarted`（默认关闭；用 `with_emit_started(true)` 打开）。
- 出口处发 `ObsHttpRequestCompleted`，含 `route`、`method`、`status_class`
  （LABEL：`2xx|3xx|4xx|5xx|err`）、`latency_ms`（histogram）、`bytes_out`（counter）。

出站调用经过包装后会自动注入 `traceparent`：

```rust
use obs_tower::client::ObsHttpClientLayer;

let svc = tower::ServiceBuilder::new()
    .layer(ObsHttpClientLayer::new())
    .service(hyper_util::client::legacy::Client::builder(
        hyper_util::rt::TokioExecutor::new()).build_http());

let resp = svc.call(http::Request::get("https://upstream/...").body(())?).await?;
// traceparent + tracestate 已注入；ObsHttpClientCompleted 已发出。
```

---

## 11. 桥接 `tracing`

你几乎从不需要改一行 `tracing::info!()` 就能从 `obs` 受益。安装一次桥接：

```rust
use obs_sdk::{StandardObserver, install_observer, install_panic_hook};
use obs_tracing_bridge::TracingToObsLayer;
use tracing_subscriber::{layer::SubscriberExt, Registry};

let observer = StandardObserver::dev()?;
install_observer(observer);
install_panic_hook();

tracing_subscriber::registry()
    .with(TracingToObsLayer::new())
    .init();

// 现有的 tracing! 调用会同时通过 obs sink 路由。
tracing::info!(target: "myapi", route = "list_users", "request done");
```

桥接默认把每个 tracing 事件映射成内置的 `ObsTracingForensicEvent`（携带 `target`、
`level`、`message`、字段 map）。对高量目标，注册 typed promoter，桥接会产生
有类型的 `Obs*` 事件 — 见
[spec 30 § 2.5](../specs/30-tracing-bridge.md) 与
[`docs/migration-from-tracing.md`](./migration-from-tracing.md)（English）。

反向桥接（`ObsToTracingSink`）让 `obs::emit!` 事件能在
`tracing-subscriber::fmt` host 的 `cargo run` stdout 里出现 — 在已有的 tracing
栈里增量采用时很有用。

---

## 12. 配置（`obs.yaml`）

```yaml
# obs.yaml — 运行时配置（编译期设置在 Cargo.toml）

filter: "info,myapi::auth=debug,myapi.v1.ObsRequestCompleted=trace"

sampling:
  default_rate: 1.0
  per_event:
    "myapi.v1.ObsHealthcheck": 0.001
    "myapi.v1.ObsCacheLookup": 0.01
  always_log_at_or_above: warn
  honour_traceparent_sampled: true
  tail_buffer_capacity: 64

limits:
  max_payload_bytes: 262144         # 256 KiB；范围 1 KiB .. 16 MiB
  max_label_value_bytes: 1024

audit:
  channel_capacity: 1024
  block_ms_max: 100
  spool_after_ms: 250
  spool_dir: "/var/lib/myapi/audit-spool"
  spool_max_bytes: 1073741824       # 1 GiB
  spool_max_age: "7d"
  on_failure: panic                  # panic | abort | warn_only

queues:
  log_capacity: 8192
  metric_capacity: 8192
  trace_capacity: 8192

sinks:
  otlp:
    endpoint: "https://otel-collector.example.com:4317"
    protocol: grpc
    headers:
      authorization: "Bearer ${OBS_OTLP_TOKEN}"

service:
  name: "myapi"
  version: "1.4.2"
```

### 12.1 约定

- 每个 struct 都用 `serde(deny_unknown_fields)` — 拼写错误会带行号大声失败。
- 时长用 `humantime` 解析：`"30s"`、`"5m"`、`"7d"`。裸数字与小数单位（`"1.5h"`）会被拒。
- 字符串里支持 `${VAR}` 环境变量插值；变量未设是 config-load 错误（不静默回退）。
- 配置中的密钥会被包成 `secrecy::SecretString`；序列化回去时显示 `<redacted>` 防止意外泄漏。

### 12.2 环境变量覆盖

每个字段都可经 `OBS_*`（用 `__` 作分隔符）覆盖：

```bash
OBS_SAMPLING__DEFAULT_RATE=0.1
OBS_SINKS__OTLP__ENDPOINT="https://otel.staging.local:4317"
OBS_AUDIT__SPOOL_DIR=/mnt/spool
```

Map 字段（如 `sampling.per_event`）不能用环境变量覆盖。

### 12.3 `obs validate`

```bash
obs validate ./obs.yaml
```

发布前：捕获拼写错、错的 rate、非法 sink 组合、缺失的环境变量。

---

## 13. 多租户 / 每任务 observer

Observer 有**三级解析**：

```
每任务（tokio task-local）→ 每线程 → 全局
```

多租户服务建议为每个租户构建一个 observer，并在 `obs-tower` 里挂 selector：

```rust
fn observer_for(tenant_id: &str) -> Arc<dyn Observer> {
    StandardObserver::builder()
        .service("myapi", env!("CARGO_PKG_VERSION"))
        .resource_attr("tenant_id", tenant_id)
        .sink_for(Tier::Log,
            obs_otel::OtlpLogSink::builder()
                .endpoint(format!("https://otlp.{tenant_id}.example.com"))
                .build()?)
        .build()
        .map(Arc::new)
        .expect("tenant observer build")
}

let registry = Arc::new(TenantObserverRegistry::new(observer_for));

let app = axum::Router::new()
    .route("/api/...", get(handle))
    .layer(ObsHttpLayer::server()
        .with_per_request_observer({
            let registry = registry.clone();
            move |req| req.headers()
                .get("x-tenant-id")
                .and_then(|h| h.to_str().ok())
                .and_then(|t| registry.get(t))
        }));
```

`handle` 内部，每次 `.emit()` 都会落到该租户对应的 sink。

### 13.1 异步陷阱

- **`tokio::spawn` 不会传播 observer 或 scope。** 包装 future：

  ```rust
  tokio::spawn(
      background_work(req_id)
          .instrument(scope_clone)
          .with_observer(obs_sdk::observer()),
  );
  ```

- **`with_observer_thread_local` 仅同步可用。** 跨 `.await` 持有 guard 是错的 —
  另一个任务可能在同一线程恢复并继承 observer。函数被刻意命名得很长，
  以让误用在调用点就显得不对。异步请用 `Future::with_observer`。

- **每任务 observer 的 drop 是同步的。** 把租户 observer 放在长寿 registry 里；
  不要每请求构造一个新的。

---

## 14. CLI

```
$ obs --help
撰写         init validate lint generate doctor
Schema 治理  schema show / lint / diff / audit
数据检查     decode tail query
后端         migrate clickhouse / parquet
元           version completions
```

### 14.1 撰写

```bash
obs init --mode rust .                        # 脚手架 Rust-first crate
obs init --mode proto .                       # 脚手架 proto-first crate
obs validate proto/myapi/v1/events.proto \
            --include proto                   # 检查 .proto round-trip
obs lint --strict --root .                    # 跑全部 L001..L013，任意失败即 fail
obs generate --root .                         # 一次性 codegen 用于检查
obs doctor --root .                           # 诊断 deps / config / schema-source
```

### 14.2 Schema 治理

```bash
obs schema show myapi.v1.ObsRequestCompleted \
   --schemas ./proto                          # 完整字段表 + sink 投影预览
obs diff origin/main HEAD                     # 有破坏性变更时退出码 2
obs audit --root .                            # workspace 级 forensic + AUDIT 报告
```

### 14.3 数据检查

```bash
obs tail --file ./events.ndjson | jq 'select(.sev=="ERROR")'
obs tail --stdin                              # 从 cargo run 管道接入
obs query --from ./events.ndjson \
          --since 5m \
          --event myapi.v1.ObsRequestCompleted \
          --label route=list_users
obs decode batch.bin > events.ndjson          # 二进制 ObsBatch → NDJSON
obs decode --audit-spool /var/lib/myapi/audit-spool
```

### 14.4 后端

```bash
obs migrate clickhouse --schemas ./proto > obs_events.sql
obs migrate parquet    --schemas ./proto > obs_events.arrow.json
```

### 14.5 退出码

| 码 | 含义 |
| --- | --- |
| `0` | 成功 |
| `1` | 工具错误（调用、IO、解析） |
| `2` | 破坏性 schema diff（`obs diff`） |

CI 集成：

```yaml
- run: obs lint --strict --root .
- run: obs diff origin/main HEAD               # 退出码 2 时阻断
- run: obs migrate clickhouse --diff origin/main..HEAD --out preview.sql
```

---

## 15. 测试你的代码

```toml
# Cargo.toml
[dev-dependencies]
obs-sdk = { version = "0.1", features = ["test"] }
```

### 15.1 `#[obs::test]`

```rust
#[obs::test]
async fn billing_emits_charge_event() -> anyhow::Result<()> {
    charge_card("4242…").await?;

    obs::test::assert_emitted!(ObsChargeAttempted {
        outcome: ChargeOutcome::Approved,
        ..
    });
    Ok(())
}
```

`#[obs::test]` 会在测试期间安装一个每线程（同步）或每任务（异步）的
`InMemoryObserver`。Cargo 的默认并行测试 runner 仍然安全 — 不需要
`serial_test`。

`assert_emitted!` 是部分匹配宏；`..` 忽略你不关心的字段。

### 15.2 直接用 `InMemoryObserver`

```rust
let (observer, handle) = obs_sdk::InMemoryObserver::new();
obs_sdk::install_observer(observer);

signup_flow().await;

let events = handle.drain();
assert!(events.iter().any(|e|
    e.full_name == "myapi.v1.ObsUserSignedUp"
    && e.labels.get("channel") == Some(&"web".into())
));
```

`handle` 暴露 `drain()`、`wait_for(predicate, timeout)`、`count(filter)`。

### 15.3 Mock OTLP collector

```rust
let (mock, addr) = obs_otel::test::MockOtelCollector::start().await?;
let sink = OtlpLogSink::builder().endpoint(format!("http://{addr}")).build()?;
obs_sdk::install_observer(StandardObserver::builder().sink_for(Tier::Log, sink).build()?);

ObsRequestCompleted::builder().route(Route::ListUsers).status(Status::Ok).emit();
obs_sdk::observer().flush().await;

let logs = mock.received_logs().await;
assert_eq!(logs[0].attributes.get("event.name"),
           Some("myapi.v1.ObsRequestCompleted"));
```

### 15.4 测试陷阱

- `tokio::spawn` **不**继承测试安装的每任务 observer — 在 spawn 出去的 future
  上用 `.with_observer(obs_sdk::observer())`。
- `InMemoryObserver` 的容量要大于测试产生的事件数量（默认 1024）；溢出会静默丢。

---

## 16. 运维

### 16.1 Self-events

SDK 自身的可观测性也走同一个 observer。在你的看板上盯着这些 `obs.runtime.v1.*` 事件：

| 事件 | 含义 |
| --- | --- |
| `ObsSinkDropped {tier, reason}` | 每 tier 的 mpsc 满，或 sink 溢出。reason：`channel_full`、`retry_queue_full`、`writer_overflow`。 |
| `ObsConfigReloaded` / `ObsConfigReloadFailed` | 热重载结果。 |
| `ObsAuditSpooled` / `ObsAuditSpoolFailed` / `ObsAuditSpoolRecovered` | AUDIT 持久化路径。 |
| `ObsPanicked` | Panic hook flush + 重新 panic。 |
| `ObsForensicBudgetExceeded` | 某 crate 的 forensic 预算超额。 |
| `ObsLabelCardinalityHigh` | 某 LABEL 字段的去重计数越过其声明的上限。 |
| `ObsOversizedDropped` | payload 超出 `limits.max_payload_bytes`。 |
| `ObsSchemaUnknown` | 外部 producer 的 envelope 命中 registry 的 lookup miss 路径。 |

### 16.2 热路径性能

2024 级笔记本上每 emit 的预算（criterion 在 >10 % 回归时阻断）：

| 路径 | 预算 |
| --- | --- |
| Noop emit（无 observer） | ≤ 110 ns |
| 被过滤掉的 emit | ≤ 25 ns |
| Observer 解析（无 override） | ≤ 15 ns |
| 每线程 / 每任务 override | ≤ 30 ns |
| `Future::with_observer().poll` | ≤ 30 ns / poll |
| 完整 emit、sinks 空操作 | ≤ 1 µs P50 |
| NDJSON sink | ≤ 1.5 µs P50 |
| Scope enter + exit | ≤ 100 ns |

### 16.3 AUDIT 等级

AUDIT 是唯一可能阻塞发射线程、也是唯一永不静默丢弃的 tier：

1. `try_send` 到 AUDIT mpsc（容量 `audit.channel_capacity`）。
2. 满了就**最多阻塞 `audit.block_ms_max`**（默认 100 ms）。
3. 还满，就**spool 到磁盘**：`audit.spool_dir` 中的、含每记录 CRC32C 的、长度前缀的二进制 buffa。
4. Observer 初始化时会 drain spool（FIFO）。恢复时发 `ObsAuditSpoolRecovered`，含计数。
5. 如果 spool 不可写，触发 `audit.on_failure`（`panic` / `abort` / `warn_only`）。

跑 `obs decode --audit-spool /var/lib/myapi/audit-spool` 检查上次未 drain 的记录。

### 16.4 优雅关闭

```rust
async fn run() -> anyhow::Result<()> {
    // ... 你的服务 ...
    obs_sdk::observer().shutdown().await;   // flush 各 tier worker
    Ok(())
}
```

`StandardObserver::Drop` 会以 250 ms 上限调用 `shutdown_blocking`，所以即使
忘了调显式 shutdown，留在飞行中的也很少 — 但要干净的数据，请始终
`await observer().shutdown()`。

### 16.5 Panic 安全

```rust
obs_sdk::install_panic_hook();
```

Panic 时：发 `ObsPanicked`（FATAL）携带消息和位置，调用 `shutdown_blocking`，
然后链到先前安装的 hook，让你已有的 crash reporter 还能跑。

---

## 17. 常见问题

**Q. 为了让 `obs::emit!` 安全无副作用，我需要装 observer 吗？**
不需要。没装 observer 时，emit 是一次原子 load 成本的 noop。这让库 crate 可以
无条件地埋点。

**Q. 我能在同一 binary 里同时用 `tracing::info!` 和 `obs::emit!` 吗？**
可以 — 这正是规范的迁移路径（`obs-tracing-bridge`）。没有 flag day。
见 [migration-from-tracing.md](./migration-from-tracing.md)（English）。

**Q. `obs` 在没有 `tokio` 的环境下能用吗？**
Sink 与 observer 基于 tokio。仅 std 的库 crate 可以安全 emit（noop 路径不用 tokio）。
生产 binary 需要 tokio。

**Q. 为什么要 `Obs` 前缀？**
调用点的视觉识别，加上机械可 grep（`grep -r 'Obs[A-Z]' src/` 可以找到所有事件调用）。
在 `[workspace.metadata.obs] event_prefix` 处可以全 workspace 覆盖。

**Q. 我想发一次性事件、不写 schema 怎么办？**
`obs::forensic!(site, message, { "k" => "v" })`。它每 crate 有预算限制
（`Cargo.toml` 的 `forensic_max`），且会被 audit。当 TODO 标记用，不要常态化。

**Q. 为什么不用一个超大 proto 文件？**
鼓励：按界限上下文（bounded context）分文件（如 `proto/billing/v1/`、
`proto/auth/v1/`）。CLI 的 `--schemas` 标记会递归扫目录。

**Q. Schema 在运行期住在哪？**
每个 schema 在 link 期把自己注册进 `linkme::distributed_slice`。
`StandardObserver::build()` 把它们收集进 `SchemaRegistry`。如果激进 LTO 把
slice 剥掉了，build 会返回错误，建议你调用 `obs::include_schemas!`。

**Q. LABEL、ATTRIBUTE、MEASUREMENT 有啥区别？**
- **LABEL** — 受限维度；变成 metric attribute、OTLP log/span attribute、字典编码的分析列。基数受强制。
- **ATTRIBUTE** — 高基数值；落日志和分析，**绝不**做 metric 维度。可与 PII 兼容（被 redact）。
- **MEASUREMENT** — 数值，会变成 metric 数据点（counter / gauge / histogram）。

**Q. 怎么 redact 桥接过来的 tracing 事件里的 PII？**
桥接默认跑一个名字模式 redactor（`password|secret|token|api[_-]?key|authorization|cookie|ssn|credit[_-]?card|bearer`）。
为你的领域插一个自定义 `Redactor` — 见 spec 70 § 6。

---

下一步：[开发者指南](./dev-guide.zh-CN.md) — 内部实现、sink 契约、schema registry、
性能工作、贡献流程。
