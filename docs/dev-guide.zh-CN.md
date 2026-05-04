# obs — 开发者指南

> **目标读者：** `obs` workspace 的贡献者、自定义 sink/bridge 的实现者，以及
> 与 SDK 深度集成的库作者。
> **目标：** 解释运行时**为什么**长这样、每层必须遵守的契约，以及触及公开 API 的
> 改动应当遵循的工作流。

其他指南：
[用户指南](./user-guide.zh-CN.md) ·
[从 `tracing` 迁移](./migration-from-tracing.md)（English） ·
[English Developer Guide](./dev-guide.md) ·
[Specs 索引](../specs/index.md)

---

## 目录

1. [Workspace 地图](#1-workspace-地图)
2. [一图看懂架构](#2-一图看懂架构)
3. [`Observer` trait 与三级解析](#3-observer-trait-与三级解析)
4. [热路径 callsite 缓存](#4-热路径-callsite-缓存)
5. [每 tier worker 与 pipeline 顺序](#5-每-tier-worker-与-pipeline-顺序)
6. [Schema registry](#6-schema-registry)
7. [代码生成 pipeline](#7-代码生成-pipeline)
8. [Scope、scope frame 与自动填充](#8-scopescope-frame-与自动填充)
9. [Filter DSL](#9-filter-dsl)
10. [采样顺序与 `traceparent.sampled`](#10-采样顺序与-traceparentsampled)
11. [Sink：`ScrubbedEnvelope` 契约](#11-sinkscrubbedenvelope-契约)
12. [AUDIT spool](#12-audit-spool)
13. [`tracing` 桥接](#13-tracing-桥接)
14. [Callsite interning](#14-callsite-interning)
15. [安全：scrubber、分类、secrecy](#15-安全scrubber分类secrecy)
16. [性能预算与 bench harness](#16-性能预算与-bench-harness)
17. [测试策略](#17-测试策略)
18. [贡献清单](#18-贡献清单)
19. [发布工作流](#19-发布工作流)
20. [参考：承重设计决策](#20-参考承重设计决策)

---

## 1. Workspace 地图

```
crates/
  obs-types          # 7 个词汇 enum（Tier、Severity、FieldKind、…）；叶子
  obs-proto          # envelope.proto / builtin.proto + buffa 代码生成
  obs-core           # Observer、sinks、registry、scope、采样、scrubber、config
  obs-macros         # #[derive(Event)]、obs::emit!、#[obs::test]、…
  obs-build          # proto-first 撰写路径的 build.rs 代码生成
  obs-sdk            # 门面 re-export — 用户通常只接触它
  obs-otel           # OTLP log / metric / trace sink
  obs-parquet        # ParquetSink（单稀疏 obs_events 表）
  obs-clickhouse     # ClickHouseSink + DDL emitter
  obs-tower          # HTTP server + client 中间件
  obs-tracing-bridge # tracing ↔ obs 双向桥接
apps/
  obs-cli            # `obs` 开发者 CLI（clap v4）
  server             # 演示：hello-world 发射
  server-proto       # 演示：proto-first 撰写
  soak               # 50k events/sec soak harness（spec 90 § M4）
examples/            # 四个可运行示例服务（todomvc、interop 对、sinks-showcase）
specs/               # 设计 spec — 按 00-prd.md → 99-key-decisions.md 顺序读
docs/                # 本指南、用户指南、迁移指南、研究备忘
```

### 依赖图（必须保持无环）

```
                ┌──────────────┐
                │  obs-types   │
                └──────┬───────┘
                       │
                ┌──────▼───────┐
                │  obs-proto   │
                └──────┬───────┘
                       │
       ┌───────────────┼───────────────┐
       │               │               │
┌──────▼───┐    ┌──────▼───────┐  ┌────▼────────┐
│ obs-core │◄───│  obs-macros  │  │  obs-build  │
└──────┬───┘    └──────────────┘  └─────────────┘
       │
   sinks │ （obs-core 不知道任何 sink crate；
       │   sinks 总是引入 obs-core，反之不允许）
       ▼
┌────────────┐ ┌────────────┐ ┌────────────────┐
│  obs-otel  │ │ obs-parquet│ │ obs-clickhouse │
└────────────┘ └────────────┘ └────────────────┘

┌────────────────────┐  ┌────────────┐
│ obs-tracing-bridge │  │ obs-tower  │
└────────────────────┘  └────────────┘

                 obs-sdk re-export
                  常用子集
```

`obs-core` **禁止**依赖任何 sink crate；sink 永远引入 `obs-core`。
只有 `apps/obs-cli` 可以依赖所有 crate。

---

## 2. 一图看懂架构

```
                                              用户代码
                                               │
                                               │  ObsXxx::builder().…().emit()
                                               │  obs::emit!(WARN, ObsXxx { … })
                                               ▼
                                       ┌──────────────────┐
                                       │  ObsCallsite     │  （静态；每个 callsite）
                                       │  enabled() 检查  │   AtomicU8 + AtomicU32 generation
                                       └────────┬─────────┘
                       (Never) ◄───────────────┐│
                                               │▼
                                  ┌──────────────────────┐
                                  │ Observer::resolve()  │  每任务 → 每线程 → 全局
                                  └──────────┬───────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │ EventSchema.project │  payload bytes + label projection
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │  Head 采样器        │  rate(full_name, sev) → keep/drop
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │ Tail-on-error 推入  │  每 scope 环缓
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │  mpsc::try_send     │  每 Tier 一条 channel
                                  └──────────┬──────────┘
                                             │ — 发射线程在此返回 —
        ────────────────────────────────────────────────────────
                                             │ （worker 任务）
                                  ┌──────────▼──────────┐
                                  │  Scrubber           │  PII redact、SECRET strip
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │ ScrubbedEnvelope    │  类型系统的交接
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │  SinkRouter         │  每 tier 的 sink 链
                                  └──────────┬──────────┘
                                             │
                  ┌──────────────────────────┼──────────────────────────┐
                  ▼                          ▼                          ▼
         StdoutSink / NDJSON       OtlpLog / Metric / Trace      Parquet / ClickHouse
```

整张图遵循两条规则：

1. **发射线程从不为 sink 阻塞**（AUDIT 是唯一刻意例外，且会在受限等待后落到磁盘 spool）。
2. **发射线程从不接触 PII 或 SECRET 形式的 payload** — scrubber 在 worker 里跑，
   所有 sink 看到的都是已清洗后的 envelope。

---

## 3. `Observer` trait 与三级解析

```rust
pub trait Observer: Send + Sync + 'static {
    fn emit_envelope(&self, env: ObsEnvelope);
    fn enabled(&self, callsite: &ObsCallsite) -> Interest;
    fn generation(&self) -> u32;
    fn reload_filter(&self, filter: Filter);
    fn flush<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn shutdown<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn shutdown_blocking(&self);
}
```

trait-async 会让 `dyn Observer` 失去 object-safety；这里刻意用
`Pin<Box<dyn Future>>`（CLAUDE.md 的 async-trait 例外条款）。`Sink` 同理。

### 3.1 解析顺序

```rust
pub fn observer() -> Arc<dyn Observer> {
    if OVERRIDE_COUNT.load(Ordering::Relaxed) == 0 {
        return OBSERVER_GLOBAL.load_full();
    }
    if let Ok(per_task) = OBSERVER_TASK.try_with(|o| o.clone()) { return per_task; }
    if let Some(per_thread) = OBSERVER_THREAD.with(|c| c.borrow().clone()) { return per_thread; }
    OBSERVER_GLOBAL.load_full()
}
```

- **`OVERRIDE_COUNT` short-circuit** 让单租户生产保持一次原子 load + `ArcSwap::load_full`（~15 ns）。
- **每任务** 用 `tokio::task_local!` — 跨 `.await` 存活。
- **每线程** 用带 `RefCell` 的 `thread_local!` — 仅同步用，不能跨 `.await` 持有。
- `with_observer_thread_local` 这个 API 名字刻意起得很长 — 在异步代码里抓它就该看着不对（决策 D47）。

### 3.2 跨 `.await` 携带 override

```rust
tokio::spawn(
    handle_request(req)
        .instrument(scope)                    // scope 帧
        .with_observer(observer_for_tenant),  // observer override
);
```

两个适配器叠加在同一个 `Instrumented<F>` 类型上，使用两个正交槽位。
不显式传播的话，`tokio::spawn` 会同时丢失两者 — spawn 出去的任务看到全局 observer
且没有 scope。

### 3.3 重入护栏

线程局部的 `CAN_ENTER: Cell<bool>` 防止 sink 自己 emit 事件时（如 bridge sink 合成
`tracing::Event` 又回到 layer）发生递归。`CAN_ENTER == false` 时，observer 空操作，
sink 的 emit 被静默丢弃。镜像 `tracing-core` 的 `State::can_enter` 模式。

---

## 4. 热路径 callsite 缓存

每个 emit 处都有一个 `static ObsCallsite`：

```rust
pub struct ObsCallsite {
    pub full_name:  &'static str,
    pub default_sev: Severity,
    interest:    AtomicU8,    // 0 unknown / 1 Never / 2 Sometimes / 3 Always
    generation:  AtomicU32,   // 与 Observer.generation() 匹配则有效
}
```

### 4.1 热路径检查

```rust
fn enabled(&self, cur_gen: u32) -> Option<bool> {
    if self.generation.load(Relaxed) == cur_gen {
        match self.interest.load(Relaxed) {
            1 => return Some(false),    // never
            3 => return Some(true),     // always
            _ => {}
        }
    }
    None  // 重查 observer
}
```

- **`Never`** 在 ~25 ns 内 short-circuit 到 false。
- **`Always`** short-circuit 到 true；即便没装 observer，也能在 ~110 ns 内解析 observer + 投影 payload（noop 情形）。
- **`Sometimes`** 落到 observer 调用（更贵，但只在 filter 关心时）。

### 4.2 缓存失效

`Observer::reload_filter()` 原子地把 `generation` +1。下一次 `enabled()` 看到不匹配，
重查并 CAS 写入新状态。**永不在 observer 一侧阻塞** — 重载期间缓存可能短暂过期；
这是刻意且可接受的。

### 4.3 为什么是 static 而非 DashMap

早期草稿把缓存放在按 callsite 键的进程级 `DashMap`。把 `Atomic*` 对内联到 static
`ObsCallsite` 上，从热路径里彻底移除了一次哈希 + 查表（决策 D11）。与
`tracing::Interest` 形态对齐是刻意的 — 读过 `tracing-core` 的人立刻能认出这套模式。

---

## 5. 每 tier worker 与 pipeline 顺序

### 5.1 每 tier 一个 worker

`StandardObserver::build()` 为每个 `Tier::{Log, Metric, Trace, Audit}` spawn 一个
受限的 mpsc，每个由一个 tokio 任务驱动该 tier 的 sink 链。默认通道容量 8192；
可在 `obs.yaml` 的 `queues.*` 配置。失败隔离：阻塞 LOG tier 的 OTLP 端点
不会影响 METRIC。

### 5.2 完整 pipeline

每次 emit：

| 步骤 | 何处 | 典型耗时 |
| --- | --- | --- |
| `ObsCallsite::enabled` | static | 25 ns |
| `Observer::enabled`（仅 Sometimes 时） | 全局 / 每任务 / 每线程 | 50 ns |
| `EventSchema::project`（构 payload + label map；从 scope 自动填补） | 发射线程 | 200–500 ns |
| Head 采样决定 | 发射线程 | < 50 ns |
| Tail-on-error push | 发射线程 | < 30 ns |
| `mpsc::try_send` | 发射线程 | ~100 ns |
| **emit 返回** | | |
| Scrubber（每事件 redaction） | worker | 100 ns – 1.5 µs |
| 构 `ScrubbedEnvelope<'_>` | worker | 0（生命周期转换） |
| 每 sink 的 `SinkRouter::deliver` | worker | sink 各异 |

### 5.3 背压

- LOG / METRIC / TRACE：通道满时 `try_send` 丢弃 envelope，发出
  `ObsSinkDropped { tier, reason: "channel_full" }`。
- AUDIT：永不静默丢。见 § 12。
- OTLP retry queue：单独容量（默认 16384），便于运维区分 "应用太快"（`channel_full`）
  与 "网络太慢"（`retry_queue_full`）。

---

## 6. Schema registry

### 6.1 `EventSchema`（typed） vs `EventSchemaErased`（object-safe）

用户代码 derive `EventSchema`：

```rust
pub trait EventSchema {
    const FULL_NAME:   &'static str;
    const TIER:        Tier;
    const DEFAULT_SEV: Severity;
    const FIELDS:      &'static [FieldMeta];
    const SCHEMA_HASH: u64;       // 编译期常量
    fn encode_payload(&self, out: &mut Vec<u8>);
    fn project(&self, env: &mut ObsEnvelope, scope: Option<&ScopeFrame>);
    fn project_metrics(&self, mp: &mut MetricEmitter);
}
```

Sink 看到的是 object-safe 的 `EventSchemaErased`：

```rust
#[non_exhaustive]
pub trait EventSchemaErased: Send + Sync + 'static {
    fn full_name(&self) -> &'static str;
    fn schema_hash(&self) -> u64;
    fn tier(&self) -> Tier;
    fn default_sev(&self) -> Severity;
    fn fields(&self) -> &'static [FieldMeta];
    fn project_metrics(&self, env: &ObsEnvelope, mp: &mut MetricEmitter);
    fn decode_to_arrow_struct(&self, env: &ObsEnvelope) -> Option<arrow_array::StructArray>;
    fn decode_to_otlp_kv(&self, env: &ObsEnvelope) -> SmallVec<[KeyValue; 8]>;
    fn render_json(&self, env: &ObsEnvelope, out: &mut String);
    fn scrub_for_log(&self, env: &mut ObsEnvelope, scratch: &mut Vec<u8>) -> Result<()>;
    fn otel_attribute_view<'a>(&'a self, env: &'a ObsEnvelope) -> AttributeView<'a>;
}
```

trait 是 sealed 且 `#[non_exhaustive]` — 只允许 codegen 实现（决策 D49）。
这让我们能在不破坏手写实现的前提下添加方法。

### 6.2 基于 `linkme` 的注册

每个生成的 schema impl 在 link 期注册自己：

```rust
#[linkme::distributed_slice(obs_core::registry::EVENT_SCHEMAS)]
#[linkme(crate = obs_core::__private::linkme)]
static __SCHEMA_OBS_REQUEST_COMPLETED: &dyn EventSchemaErased = &ObsRequestCompletedSchema;
```

为什么选 `linkme` 而非 `inventory`：

- 重复 `full_name` 在编译期错（最佳失败模式）。
- 在 musl-static、WASM、stripped 二进制上可靠 — `inventory` 走 ctor 表，可能被 GC。
- 启动期零遍历；slice 由 linker 排好。

### 6.3 `SchemaRegistry`

Observer 初始化时从 `EVENT_SCHEMAS` 一次构建：

```rust
pub struct SchemaRegistry {
    by_name: HashMap<&'static str, &'static dyn EventSchemaErased>,
    by_hash: HashMap<u64, &'static dyn EventSchemaErased>,
    arrow:   Arc<arrow_schema::Schema>,
}
```

Sink 先查 `schema_hash`（8 字节 u64），再退到 `full_name`（决策 D40）。退路覆盖
*外部 producer envelope* — CLI 在 decode 来自其他服务（链入了不同 schema）的批次时使用。

### 6.4 LTO 剥离

激进 LTO 在没人引用 `EVENT_SCHEMAS` slice 时可能把它丢掉。`obs::include_schemas!("myapi.v1")`
存在的目的就是从你的 crate root 锚定该 slice；如果 slice 为空 *且* 有事件被 emit，
`StandardObserver::build()` 会返回错误，提示你调用这个宏。

### 6.5 Sink lookup miss 路径

`lookup` miss（外部 producer envelope、本 binary 没链入 schema）时：

- OTLP body 变成 `bytes(payload)`。
- Arrow 行写 `payload_proto: bytes`。
- JSON renderer 输出 `{ "_unknown_schema": true, "schema_hash": ..., "payload_b64": "..." }`。
- 受限速率的 `ObsSchemaUnknown` self-event 表面化。

这条路径仅供检查（`obs decode`）— 生产 sink 链假定 schema 都已链入。

---

## 7. 代码生成 pipeline

### 7.1 两个阶段

```
.proto → buffa-build → 线类型 + FileDescriptorSet
                   ↓
                   ↓ (FDS)
                   ↓
         obs-build（通过 buffa-reflect 走 DescriptorPool）
                   ↓
EventSchema 实现 + builder + Arrow 片段
+ JSON renderer + scrub dispatch + lint 断言
```

不需要 `protoc`；两阶段都是纯 Rust。`obs-build` 是字段注解
（`obs.v1.field`、`obs.v1.event`）变成 Rust 代码的*唯一*位置。

### 7.2 Lint 作为 `const _: () = assert!(...)`

每条 L001..L013 lint 都以 `const _: () = { ... assert!(...) }` 块的形式写入生成文件，
违规会让 `cargo build` 失败。`obs lint` CLI 命令在 build 之外对 `.proto` 跑同一逻辑，
让 CI 早期得到信号。

### 7.3 生成的 builder

`typed-builder` 提供 marker-state 机制。Codegen 用一个策划过的 `#[builder(...)]`
配置调入它，让 missing-required-field 错误指向 `.emit()` 而非生成模块内部。

### 7.4 Schema 演进

由 `obs schema diff` 强制：

| 改动 | 允许？ |
| --- | --- |
| 加字段 | ✅（增量 — 旧行 NULL） |
| 删字段 | ⚠ 弃用；tag 变 reserved |
| 复用先前用过的 tag | ❌ 破坏（L008） |
| 改字段类型 | ❌ 破坏 |
| 改 `FieldKind` | ❌ 破坏 |
| 降级 `Classification`（PII → INTERNAL） | ❌ 禁止 |
| 升级 `Classification`（INTERNAL → PII） | ✅ |
| 改 `Tier` | ❌ 破坏 |

`obs diff base..HEAD` 在任何破坏性变更上退出 2。在 CI 里挂上。

### 7.5 辅助 trait

| Trait | 为谁生成 | 用途 |
| --- | --- | --- |
| `BuildableTo<Args>` | 每个事件 | 让 typed-builder marker 链上能 `.emit()` |
| `MetricEmitter` | 每个事件 | 每 `MEASUREMENT` 字段一个闭包 |
| `FieldCapture` | bridge promoter | tracing visitor 适配器 |
| `SpanCtx` | trace-context 字段 | 从 scope 自动填 `trace_id`/`span_id`/`parent_span_id` |
| `EnumCount` | 每个 `EnumLabel` enum | L005 的编译期变体计数 |

---

## 8. Scope、scope frame 与自动填充

### 8.1 `ScopeFrame`

```rust
pub struct ScopeFrame {
    fields:        SmallVec<[ScopeField; 8]>,
    tail_buffer:   Option<RingBuffer<ObsEnvelope>>,   // 64 项，仅 scope! 才有
    parent_id:     Option<NonZeroU64>,
    span_id:       NonZeroU64,
    trace_id:      [u8; 16],
    sampled:       bool,
    started_ns:    u64,
}
```

- **`obs::scope!`** 同时绑定字段 *和* tail buffer。
- **`obs::context!`** 只绑字段（无缓冲）。

两者都是 RAII guard — `Drop` 把 frame 从每任务 scope 栈弹出。`Drop` 时，如果
tail buffer 中任何 envelope 标了 `Severity::Error` 或更高，缓冲就 flush 到 observer。

### 8.2 自动填充机制

自动填充**不是** builder 默认值。Codegen 把可默认填充的字段路由经内部 `Option<T>`：

```rust
// generated
fn project(&self, env: &mut ObsEnvelope, scope: Option<&ScopeFrame>) {
    let trace_id = self.trace_id_opt
        .clone()
        .unwrap_or_else(|| scope.and_then(|s| s.trace_id_str()).unwrap_or_default());
    ...
}
```

用户面 setter 是 `impl Into<T>`，所以显式 `.trace_id("")` 产生 `Some("")` 并
**绕过** scope。省略 setter 产生 `None` 并继承 scope。这精确锁定开发者选择
覆盖 scope 的瞬间。

### 8.3 Scope **不是** `tracing::Span`

- frame 自身没有起止时间戳（如果想要，可从 Started/Completed 事件对推导）。
- 没有 `enter`/`exit` 周期。
- 没有事后 `Span::record`。
- `obs` 不维护 span 树 — scope 之间的关系是**栈位置上的父子**，不是 ID 链接。

要带时长的 span 语义，用 `#[obs::instrument]`（发 `ObsFnExecuted`）或
Started/Completed 事件对（OTLP trace sink 会合成一个 OTel span）。

---

## 9. Filter DSL

`obs::Filter` 把 `tracing-subscriber::EnvFilter` 语法逐字移植：

```
target_or_event[span_field=val]?[=level]?
```

例：

```
info
info,myapi::auth=debug
myapi.v1.ObsRequestCompleted=trace
myapi.v1.ObsRequestCompleted[route=admin]=trace
info,myapi::cache=trace,myapi.v1.ObsHealthcheck=off
```

### 9.1 Statics vs Dynamics

跟 `EnvFilter` 一样，parser 把子句分为：

- **Statics** — parse 时编成扁平查表；每次 callsite enabled-check 都查。
- **Dynamics** — 带字段-值 matcher 的子句。对投影后的 envelope label 求值。

### 9.2 字段子句

`[field=value]` **只**对 envelope 的 `labels` 匹配（即 LABEL kind）。ATTRIBUTE
字段永远匹不到 — 你写了命中 ATTRIBUTE 的 filter，`obs lint` 会 warn。强制这一点
能避免一类令人困惑的"filter 看着对、但匹不到"bug。

### 9.3 优先级

```
tracing::EnvFilter（RUST_LOG，仅当 bridge 安装时）
    ↓
obs::Filter        （OBS_FILTER / obs.yaml `filter:`）
    ↓
每 sink filter     （罕见；如 STDOUT 只 WARN+，OTLP 全收）
```

---

## 10. 采样顺序与 `traceparent.sampled`

每次 emit 按以下顺序：

1. **入站 W3C `traceparent.sampled`** — 如果活跃 scope 已带传播过来的 sampled bit，
   遵从它（设为开则始终 emit、清掉则始终 drop）。镜像 OTel `ParentBasedSampler`。
   可在 `sampling.honour_traceparent_sampled = false` 关闭。
2. **Head 采样器** — 每 `(full_name, severity)` 一次 `f64` 比较。固定 seed，可重现。
3. **Tail-on-error** — 每 scope 环缓；只要任何同 scope 事件触发 ERROR 或 FATAL 就 flush。

顺序很重要：tail-on-error 只看到 head 采样后存活的事件。它是 head 采样之上的
质量增益层，而非替代。

---

## 11. Sink：`ScrubbedEnvelope` 契约

```rust
#[non_exhaustive]
pub trait Sink: Send + Sync + 'static {
    fn deliver<'a>(
        &'a self,
        env: ScrubbedEnvelope<'a>,
        registry: &'a SchemaRegistry,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    fn flush<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }

    fn shutdown<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
}
```

### 11.1 `ScrubbedEnvelope<'_>`

```rust
pub struct ScrubbedEnvelope<'a> {
    inner:  &'a ObsEnvelope,
    scratch: &'a [u8],          // 可选的、redacted payload 视图
}
```

生命周期把 redacted payload 绑到 worker 拥有的 scratch buffer 上。
**Sink 不能延长其生命周期**。如果某 sink 需要 envelope 跨 `deliver` 调用
（如批量 ClickHouse INSERT），它必须 clone 相关字段。

### 11.2 sink 可以与不可以做什么

✅ **可以：**
- 通过 `schema_hash` 或 `full_name` 查 `EventSchemaErased`。
- 通过 registry 把 payload decode 成 Arrow / OTLP key-values / JSON。
- 读所有 envelope 字段（labels、severity、trace context、…）。
- 自己缓冲 / 批量 / 重试；sink 拥有自己的背压。

❌ **不能：**
- 跨 `deliver` 调用持有 `ScrubbedEnvelope<'_>`。
- 在 worker 线程里同步 IO 阻塞；用 tokio。
- 直接调 `Observer::emit_envelope`（用 `obs::emit!`；重入护栏让合成事件安全）。
- 假设 schema 已注册 — lookup-miss 路径存在自有原因。

### 11.3 写自定义 sink

```rust
use obs_core::{ScrubbedEnvelope, SchemaRegistry, Sink};
use std::pin::Pin;
use std::future::Future;

pub struct MyVendorSink { client: reqwest::Client, endpoint: String }

impl Sink for MyVendorSink {
    fn deliver<'a>(
        &'a self,
        env: ScrubbedEnvelope<'a>,
        registry: &'a SchemaRegistry,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let body = registry
            .by_hash(env.schema_hash())
            .map(|schema| {
                let mut s = String::new();
                schema.render_json(env.inner(), &mut s);
                s
            })
            .unwrap_or_else(|| serde_json::to_string(env.inner()).unwrap_or_default());

        Box::pin(async move {
            if let Err(e) = self.client.post(&self.endpoint).body(body).send().await {
                obs_sdk::emit!(WARN, ObsSinkFailed {
                    sink: "my_vendor".into(),
                    reason: e.to_string(),
                });
            }
        })
    }
}
```

测试：写一个 `RecordingTransport` 风格的 fake（参考
`obs-clickhouse::transport::RecordingTransport`）。

---

## 12. AUDIT spool

AUDIT 事件绝不能静默丢。流程：

1. **In-channel 快路径。** `try_send` 到 AUDIT mpsc（容量 `audit.channel_capacity`，默认 1024）。
2. **受限阻塞。** 满了则**最多阻塞 `audit.block_ms_max`**（默认 100 ms）等容量。
3. **Spool 到磁盘。** 阻塞后还满，就把 envelope 写入
   `${audit.spool_dir}/${YYYYMMDD-HHMMSS}-${pid}.audit.bin`。格式：长度前缀的 buffa，
   每记录 CRC32C。发 `ObsAuditSpooled`。
4. **Observer init 时**，扫 `spool_dir` 中的 `*.audit.bin`，按 mtime FIFO 通过 worker 重放，
   再删除。发 `ObsAuditSpoolRecovered { count }`。
5. **Spool 自身不可写**（盘满、权限）时，触发 `audit.on_failure`：
   - `panic`（默认）— `panic!()` 让被 supervise 的进程重启。
   - `abort` — `std::process::abort()`（不展开；最快）。
   - `warn_only` — 发 `ObsAuditSpoolFailed`、**丢弃 AUDIT 事件**、继续。
     仅在外层管道保证持久性时使用。

Spool 格式（二进制，每记录）：

```
| 4 字节 LE 长度 | 4 字节 LE payload CRC32C | <长度> 字节 buffa 编码的 ObsEnvelope |
```

CLI 检查：

```bash
obs decode --audit-spool /var/lib/myapi/audit-spool > audit.ndjson
```

### 12.1 实现备注

- spool writer 是 `obs-audit-spool`（顶层 crate）。文件以 `O_APPEND` 打开，每次写
  `fsync`；正确性优先于吞吐。AUDIT 本就稀少，所以吞吐可接受。
- 恢复时跳过早于 `audit.spool_max_age`（默认 7d）的文件，并发 `ObsAuditSpoolDropped`；
  需要运维介入。

---

## 13. `tracing` 桥接

`obs-tracing-bridge` 提供两半；可同时安装。

### 13.1 方向 A：`tracing → obs`

`TracingToObsLayer`（默认，layered）和
`TracingToObsSubscriber`（在你不能用 `Registry` 时的逃生口）。

Layer 用 `tracing-subscriber::registry()` 的扩展存每 span 状态 — 没有平行 DashMap。
每个 `tracing::Event` 变成：

- `ObsTracingForensicEvent`（默认）— 含 `target`、`module`、`source_loc`、`message`、字段 map。
- 一个有类型的 `Obs*` 事件 — 当为该 callsite identifier 注册了
  `register_typed::<E>(matcher, promoter)` 时。

```rust
TracingToObsLayer::new()
    .with_field_promotions(
        FieldPromotions::new()
            .promote("tenant_id", Cardinality::Medium)
            .promote("route", Cardinality::Medium))
    .register_typed::<ObsHttpRequestCompleted>(
        TypedMatcher::new()
            .target("tower_http::trace::on_response")
            .field("status").field("latency"),
        |event, ctx, cap| {
            ObsHttpRequestCompleted::builder()
                .route(cap.string("route").unwrap_or_default())
                .status_class(parse_status(cap.u64("status").unwrap_or(0)))
                .latency_ms(cap.u64("latency").unwrap_or(0))
                .build()
        });
```

按 `tracing_core::callsite::Identifier` 索引：缓存 O(1)；冲突时第一个注册者赢
（并发 `ObsBridgeMatcherConflict`）。

### 13.2 方向 B：`obs → tracing`

`ObsToTracingSink` 是个普通 `Sink`，每 envelope 合成一个 `tracing::Event`。
适用于：

- 在已有的 `tracing-subscriber::fmt` host 中采用 `obs`，让有类型事件流过同一 pretty printer。
- 让 `tracing-opentelemetry` 或 `console-subscriber` 也看到 obs 发的事件。

`PayloadDecodeMode`：

- `Off` — 仅合成 envelope 字段。
- `DecodeKnown` — 对 registry 中已知 schema 的事件 decode payload。
- `DecodeKnownAttributesOnly` — 同上但跳过 MEASUREMENT 字段。

`SpanEmissionMode::OnScope` 为每个 `obs::scope!` 帧合成一个 span。
**`OnScope` + `OtlpTraceSink` 同时使用会产生重复 OTel span** — 桥接在 init 时
日志 `ObsConfigInconsistent`。

### 13.3 防回环

两个 layer 都给合成事件打 target `obs.bridge`，并设线程局部 `IN_BRIDGE: Cell<bool>`。
另一半在重新 emit 前同时检查这两点。再加上 `CAN_ENTER` 重入护栏，回环不能自我放大。

---

## 14. Callsite interning

`obs.yaml`：

```yaml
interning:
  mode: hybrid              # off | hybrid | compact
  refresh_interval_secs: 600
  refresh_event_count: 10000
```

把桥接路径的 `target`/`module_path`/`file:line`/template 字符串 token 化成
`callsite_id: u64`（`ObsEnvelope` 上的 `fixed64` 字段 15）。

| 模式 | 相对 Off 的线尺寸 | 解码要求 |
| --- | --- | --- |
| `Off` | 100 %（全字符串） | 无 |
| `Hybrid` | ~50 % | 可选 registry；保留渲染后的 message |
| `Compact` | ~25 % | 必须 registry 才能解码 |

### 14.1 `callsite_id` 生成

`BLAKE3(source, target, file, line, level, field_names, template)` 的前 8 字节。

- 跨进程、跨重启确定。
- 100 万 callsite 时碰撞概率 < 2⁻⁴⁴。
- ~80 ns 计算。
- **`callsite_id == 0` 是保留的**（"未 intern"）；BLAKE3 在截断命中 0 时（2⁻⁶⁴ 概率）
  从字节 8..16 重 roll。

### 14.2 `ObsCallsiteRegistry`

按 `callsite_id` 索引的进程级 DashMap。注册是**首次见到时同步进行**的：在数据
envelope 之前先发 `ObsCallsiteRegistered`（`SamplingReason::OVERRIDE`、绕过采样）。
重发节奏（每 10 分钟或每 callsite 1 万事件）让下游消费者在重启后保持同步。

### 14.3 方向 B 重建

`ObsToTracingSink` 读到 intern 过的 envelope 时，在 registry 里查 `callsite_id`，
合成 `tracing::Metadata` 时使用**原始** `target`（非 `obs.bridge`），让外部工具
分不出 "从未 intern" 与 "round-trip 回来" 的区别。

---

## 15. 安全：scrubber、分类、secrecy

### 15.1 威胁模型

`obs` 是**边界库**：进程内 producer 受信任，但下游 sink（网络 OTLP、持久 Parquet/ClickHouse）
不受信任。编译期注解 + 运行期清洗把 SECRET / PII 拦在持久磁盘外；运维必须能
audit 什么被 redact 了、为什么。

### 15.2 分类等级

| 等级 | 行为 |
| --- | --- |
| `INTERNAL`（默认） | 无 redact。 |
| `PII` | redact 为 `"[REDACTED:pii]"`，保持 schema/行结构稳定。**不能**在 `LABEL` 字段（L002）。 |
| `SECRET` | 完全剥离。**不能**出现在 `LOG` 或 `AUDIT` tier 事件（L003）。字段类型**必须**是 `secrecy::SecretString` / `SecretBox<T>`，使值不会通过 `Debug` 泄漏。 |

### 15.3 Scrubber

`EventSchemaErased::scrub_for_log(env, scratch) -> Result<()>` 在**worker** 中、
采样器与 sink 链之间运行：

- 把 redacted payload bytes 写入 worker 拥有的 `scratch`（稳态无额外分配）。
- 原始 `env.payload` 保留不变，给非持久 sink（如只读 label 的 metric counter）使用。
- 失败时在 worker 丢弃 envelope；未清洗的 payload 永不到 sink。

### 15.4 桥接侧模式 redactor

对没有声明分类的 tracing 事件，桥接默认跑名字模式 redactor：

```
(?i)password|secret|token|api[_-]?key|authorization|cookie|ssn|credit[_-]?card|bearer
```

匹配到的字段值变成 `"[REDACTED:bridge_pattern]"`。每字段名一次性 `ObsBridgePiiSuspected`。

### 15.5 自定义 redactor

```rust
pub trait Redactor: Send + Sync + 'static {
    fn redact(&self, target: &str, field: &str, value: &str) -> RedactAction;
}

pub enum RedactAction {
    Keep,
    Replaced(Cow<'static, str>),
    Drop,
}
```

通过 `TracingToObsLayer::with_redactor(...)` 接入。用于领域专属模式
（如 `phone_number`、内部账号 ID）。

### 15.6 `obs` **不**防御什么

- 谎报分类的进程内恶意代码 — 假定 schema 作者可信。
- 到 OTLP 后端的 TLS — 这是用户的责任。
- 后端侧的静态加密 — 由运维按后端决定。

---

## 16. 性能预算与 bench harness

### 16.1 预算

2024 级笔记本上每 emit 的预算（criterion 在 >10 % 回归时阻断）：

| Bench | 路径 | 预算 |
| --- | --- | --- |
| `bench_emit_noop` | 无 observer 安装 | ≤ 110 ns |
| `bench_emit_filtered` | filter 说丢 | ≤ 25 ns |
| `bench_observer_resolution` | 三级、无 override | ≤ 15 ns |
| `bench_with_observer_poll` | 每 poll 都有每任务 override | ≤ 30 ns |
| `bench_emit_inmemory` | 完整路径、空 sink | ≤ 1 µs P50 |
| `bench_emit_ndjson` | 完整路径、NDJSON sink | ≤ 1.5 µs P50 |
| `bench_scope_enter_exit` | scope! ↔ Drop | ≤ 100 ns |
| `bench_encode_payload` | buffa、10 字段 | ≤ 5 µs |
| `bench_registry_lookup` | 按 `schema_hash` | ≤ 15 ns |
| `bench_registry_lookup_by_name` | 退路 | ≤ 60 ns |
| `bench_registry_lookup_miss` | 外部 producer | ≤ 80 ns |
| `bench_registry_init` | 1000 schema | ≤ 10 ms |
| `bench_scrub_for_log` | 干净 | ≤ 100 ns |
| `bench_scrub_for_log` | 5 个 redact 字段 | ≤ 1.5 µs |
| `bench_bridge_to_obs` | tracing → obs | ≤ 3 µs |
| `bench_bridge_from_obs` | obs → tracing | ≤ 2.5 µs |
| `bench_intern_cold` | 首次见到 | ≤ 4 µs |
| `bench_intern_warm` | 缓存命中 | ≤ 2.5 µs |

Bench 在 `crates/obs-core/benches/` 与 `crates/obs-tracing-bridge/benches/`。
Baseline 提交为 `benches/baseline.json`。在发布准备时用 `make bench-update` 更新。

### 16.2 Profiling 工具链

```bash
cargo flamegraph --bench bench_emit_inmemory
samply record cargo bench --bench bench_emit_inmemory
cargo asm --rust 'obs_core::observer::ObsCallsite::enabled' > crates/obs-core/asm/enabled.s
```

触及宏展开路径的 PR **必须**更新 `crates/obs-core/asm/` 中的 asm 快照，让
reviewer 能看到寄存器/指令的变化。

### 16.3 Soak harness

`apps/soak` 以 50 k events/sec 跑 30 s（`make soak`）或 24 h（`make soak-24h`）。断言：

- `ObsSinkDropped` 计数 == 0
- AUDIT spool 计数 == 0（默认 sink 配置下）
- 常驻内存保持有界（暖机后无单调增长）

---

## 17. 测试策略

### 17.1 测试金字塔

- **单元** 在 `#[cfg(test)] mod tests` 与代码同文件。
- **集成** 在每 crate 的 `tests/`。
- **属性测试** 用 `proptest` — proto 编解码 round-trip、scope trace_id 传播、
  codegen 确定性、bridge round-trip 保留 target/level/string 字段。
- **Trybuild 编译错误 fixture** — 每条 L001..L013 lint 都有配对的 `.rs` + `.stderr`
  快照。CI 在漂移时 fail；`TRYBUILD=overwrite` 重新生成。
- **Fuzz harness** 在 `crates/<crate>/fuzz/`（cargo-fuzz、nightly，从 workspace 的
  `cargo build` 中排除）。
- **Bench harness** 在 `crates/<crate>/benches/`（criterion）。
- **Mock OTLP collector** 在 `obs_otel::test::MockOtelCollector` — 真正的 `tonic`
  server，断言 OTLP 线格而非 SDK 内部。

### 17.2 dev-ergonomics 套件

`crates/obs-sdk/tests/dev_ergonomics/`：

- `test_quickstart_60s.rs` — 脚手架 + build + run 一个 fixture crate。
- `test_compile_errors.rs` — 每条 lint 的 trybuild 快照。
- `test_no_observer_noop.rs` — 不 panic、除一次原子 load 外不分配。
- `test_hot_reload.rs` — 写 `obs.yaml`、发 `SIGHUP`、验证下次 emit 用新采样率。
- `test_in_memory_observer.rs` — `assert_emitted!` 部分匹配；`wait_for` 干净超时。
- `test_tracing_bridge.rs` — 安装桥接；emit `tracing::info!`；断言结果是
  `ObsTracingForensicEvent`。
- `test_parallel_obs_test.rs` — 32 个并发 `#[obs::test]`；每线程 / 每任务槽位
  防止污染。

CI 每 PR 都跑这套；这里失败被视为与 clippy 回归同等严重。

### 17.3 约定

- 测试必须用 `EventsConfig::builder()`，不要 load fixture YAML（文件 IO 在
  Windows CI 上会破坏 `#[obs::test]` 并行性）。
- 测试不要直接调 `obs::install_observer` — `#[obs::test]` 自动安装 `InMemoryObserver`。
- 多租户测试构建每租户 observer 并用 `Future::with_observer` —
  绝不跨 `.await` 用 `with_observer_thread_local`。

---

## 18. 贡献清单

开 PR 之前：

```bash
cargo build --workspace --all-features
cargo test  --workspace --all-features
cargo +nightly fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
make lint-strict       # cargo clippy -W clippy::pedantic 配合策划过的 allow
make audit             # cargo deny check advisories
make deny              # cargo deny check（advisories + bans + licenses + sources）
make check-format-ver  # envelope 线格 lock（spec 90 § 3.3）
```

触及热路径的改动：

```bash
cargo bench --bench bench_emit_inmemory -- --save-baseline pr-NN
# 与 benches/baseline.json 对比；> 10% 回归阻断合并
```

触及生成代码或宏的改动：

- 更新 `crates/obs-core/asm/enabled.s`（`cargo asm`）。
- 更新 trybuild fixture（`TRYBUILD=overwrite cargo test --test compile`）。
- 端到端重跑 dev-ergonomics 套件。

Schema 改动（envelope、内置事件）：

- 线格变了就更新 `obs-proto` 的 `format_ver`。
- 跑 `make check-format-ver`。
- 如果影响 reload 语义，把变更加入 `specs/15-config.md` 的迁移矩阵。

新增 public API：

- 加带示例的 `///` 文档注释。
- 用户面就从 `obs-sdk` re-export。
- 在 `docs/user-guide.md` 与 `docs/user-guide.zh-CN.md` 加一节。
- 更新 `docs/index.md`。

项目政策摘要（完整文本见 `/Users/tchen/projects/mycode/rust/obs/CLAUDE.md`）：

- 全 workspace `#![forbid(unsafe_code)]`。
- 用户输入路径上不要 `unwrap()` / `expect()` / `panic!()`（emit 模块 clippy 全 workspace deny）。
- 不要 `tokio::fs` / `tokio::process`（CLI 在本地豁免自己）。
- 库错误用 `thiserror`，应用错误用 `anyhow`。
- v1 仅 tokio 异步运行时。

---

## 19. 发布工作流

1. **开发布 PR** 提升 `[workspace.package].version`。
2. **跑全套验证：**
   ```bash
   make ci-full      # build + test + fmt + clippy + audit + deny + soak (30s)
   make soak-24h     # 仅在打主版本前
   ```
3. **更新 `CHANGELOG.md`** 用 `git-cliff`（`cliff.toml` 已配好）。
4. **按依赖顺序 tag 与发布：** `obs-types` → `obs-proto` → `obs-macros`
   → `obs-core` → `obs-build` → sinks → bridge → tower → `obs-sdk` → `obs-cli`。
   Makefile 的 `make publish-dry-run` target 会在任何 `cargo publish` 之前
   验证每个 `Cargo.toml` 的缺失字段。
5. **给 repo 打 tag** — `git tag v0.X.Y && git push --tags`。
6. **更新 `docs/migration-from-tracing.md`**，列入任何用户面变更。

---

## 20. 参考：承重设计决策

49 条编号设计决策住在 [`specs/99-key-decisions.md`](../specs/99-key-decisions.md)。
新人最容易意外的几条：

| # | 决策 | 为什么 |
| --- | --- | --- |
| **D1** | `buffa` 而非 `prost` | 一等公民的自定义 option 支持；不需要 `protoc`；反射遍历更快。 |
| **D5** | 单稀疏 `obs_events` 表 | 跨事件 join 一次查询；新事件只追加一列、旧行 NULL。 |
| **D7** | 三级 observer（每任务 → 每线程 → 全局） | 异步多租户正确、每 emit 不分配。 |
| **D9** | AUDIT 可阻塞 ≤100ms 后 spool、永不静默丢 | AUDIT 存在的理由就是持久性；静默丢会让它失去意义。 |
| **D11** | static callsite 上的 atomic Interest 缓存 | 与 tracing 对齐；从热路径里移除 DashMap。 |
| **D14** | `Observer`/`Sink` 异步用 `Pin<Box<dyn Future>>` | object-safety 必需；记入 async-trait 例外。 |
| **D15** | Builder 规范、宏简写 | builder 适配 rust-analyzer 链式补全；宏适配 1–2 字段事件。 |
| **D16** | `obs::scope!` **不是** `tracing::Span` | scope 是字段白名单 + tail buffer，不是 span 树节点。 |
| **D22** | tail buffer 绑定在 scope! 的 Drop 上、而非 request_id 字符串 | 避免 string-key 不被清理这一类泄漏。 |
| **D38** | `linkme` 而非 `inventory` | 编译期重复检测；在 musl/WASM 上可靠。 |
| **D39** | `ScrubbedEnvelope<'_>` 作为 worker→sink 交接 | 类型系统保证 sink 看不到未清洗 payload。 |
| **D42** | 入站 `traceparent.sampled` 在本地采样器之前优先 | 与 OTel `ParentBasedSampler` 对齐。 |
| **D44** | Filter DSL 逐字移植 EnvFilter 语法 | `RUST_LOG` 风格字符串可直接用。 |
| **D47** | `with_observer_thread_local` 名字刻意冗长 | 异步陷阱在调用点就该看着不对。 |
| **D49** | `EventSchemaErased` sealed 且 `#[non_exhaustive]` | 让方法添加不破坏手写实现。 |

合并任何改变面向公开契约的代码之前，把全部 49 条都读一遍。
