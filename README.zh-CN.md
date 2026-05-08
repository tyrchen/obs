# obs — Rust 的 Schema-first 宽事件 SDK

状态：1.0 之前（第 5 阶段加固，[spec 91](./specs/91-impl-plan.md)）

> English: [README.md](./README.md)

`obs` 是一个面向 **宽结构化事件** 的 Rust SDK：发射、采样、过滤、scope 自动填充、
OTLP / Parquet / ClickHouse sink，以及与 `tracing` crate 的双向桥接 — 全部由你
以 Rust 结构体（`#[derive(Event)]`）或 `.proto` 消息（`obs-build`）撰写的 schema 驱动。

完整设计存放于 [`./specs/`](./specs)；从 [00-prd.md](./specs/00-prd.md)（章程）和
[91-impl-plan.md](./specs/91-impl-plan.md)（按依赖顺序的构建计划）开始读。
**日常使用直接看 [用户指南](./docs/user-guide.zh-CN.md)。**
内部实现与贡献请参考 [开发者指南](./docs/dev-guide.zh-CN.md)。

## 60 秒上手

```bash
# 1. 安装 CLI（在 2024 级笔记本冷缓存下 ≤ 30 秒；热缓存约 10 秒）。
cargo install --git https://github.com/TODO obs-cli

# 2. 脚手架生成一个新 schema crate。
obs init demo
cd demo

# 3. 构建 + 运行。脚手架会安装一个带有 StdoutSink(Full) 的 StandardObserver，
#    所以第一次 emit 就会打印到 stdout。
cargo run
# → 1730000000.000000000 info demo.v1.ObsHelloEmitted {who=world}
```

精确的命令行表面在 [`specs/50-cli.md`](./specs/50-cli.md) 中有文档。

## 你将获得什么

| 关注点 | `obs` 提供 |
| --- | --- |
| **Schema 撰写** | `#[derive(Event)]`（Rust-first）或 `obs-build`（proto-first）。两者产生字节级一致的代码生成产物（spec 12 § 1.2）。 |
| **热路径发射** | 每个 callsite 上有 atomic-`Interest` 缓存；过滤掉时 ~25 ns，无 observer 时 ~110 ns，投递时约 1 µs。Spec 11 § 2 / 71 § 4。 |
| **三级 Observer 解析** | 每任务 → 每线程 → 全局。多租户服务安装每任务 observer；测试安装每线程；生产挂全局。Spec 11 § 3。 |
| **Sinks** | `StdoutSink`、`NdjsonFileSink`（带 `RollingFileWriter` + `NonBlockingWriter`）、`OtlpLogSink` / `OtlpMetricSink` / `OtlpTraceSink`、`ParquetSink`、`ClickHouseSink`。Spec 20 / 22。 |
| **采样 + 过滤** | `obs::Filter`（EnvFilter 形态）、head 采样器、每 `obs::scope!` 帧 64 项 tail-on-error 环缓。Spec 13。 |
| **tracing 桥接** | `TracingToObsLayer`（每条 `tracing::info!` 变成一个有类型的 `ObsTracingForensicEvent`），以及 `ObsToTracingSink`（通过 `tracing_log` 把事件回放到现有 `tracing-subscriber` 消费者）。Spec 30。 |
| **AUDIT 等级** | 受限阻塞 + 磁盘 spool；observer 初始化时进行 CRC 校验恢复。Spec 11 § 6.4。 |
| **HTTP 中间件** | `obs-tower` — 给 axum/tower 栈用的 `ObsHttpLayer`，可按 header 多租户分发 observer。Spec 40。 |
| **CLI** | `obs init`、`obs validate`、`obs lint`、`obs schema show`、`obs decode`、`obs tail`、`obs query`、`obs diff`、`obs audit`、`obs migrate`。Spec 50。 |

## Workspace 结构

```
crates/
  obs-types          # 7 个词汇 enum（Tier、Severity、FieldKind、…）
  obs-proto          # envelope.proto / builtin.proto + buffa 代码生成
  obs-core           # Observer、sinks、registry、采样、scope、过滤
  obs-macros         # #[derive(Event)]、obs::emit!、#[obs::test]、…
  obs-build          # 用于 proto-first 撰写路径的 build.rs 代码生成
  obs-kit            # 门面 re-export
  obs-otel           # OTLP log / metric / trace sink
  obs-parquet        # Parquet sink（单表布局）
  obs-clickhouse     # ClickHouse sink
  obs-tower          # HTTP server + client 中间件
  obs-tracing-bridge # tracing ↔ obs 双向桥接
apps/
  obs-cli            # `obs` 开发者 CLI
  server             # 演示：hello-world 发射
  server-proto       # 演示：proto-first 撰写路径
  soak               # 50k events/sec 的 soak 测试 harness（spec 90 § M4）
examples/            # 四个可运行示例服务（todomvc、interop 对、sinks-showcase）
specs/               # 设计 specs（按 00-prd.md → 99-key-decisions.md 顺序读）
docs/                # 用户/开发者指南、迁移指南、研究笔记
```

## 构建、测试、Lint

```bash
# CLAUDE.md / spec 90 § 0 规定的"始终可发"目标。
cargo build
cargo test --workspace --all-features
cargo +nightly fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings

# 第 5 阶段加固目标（spec 90 § M4 / impl-plan 5.x）。
make lint-strict       # cargo clippy -W clippy::pedantic 配合策划过的 allow 清单
make audit             # cargo deny check advisories
make deny              # cargo deny check（advisories + bans + licenses + sources）
make soak              # 30 秒、50k events/sec、ObsSinkDropped == 0 断言
make soak-24h          # 完整 24 小时 soak（v1.0 打 tag 之前跑）
make check-format-ver  # spec 90 § 3.3 envelope 线格 lock
```

## 文档

### 指南（从这里开始）

| 指南 | 目标读者 | 中文 | English |
| --- | --- | --- | --- |
| **用户指南** | 在服务里采用 obs 的应用工程师与 SRE。安装、schema 撰写、emit/scope/filter、sinks、OTLP、多租户、CLI、运维。 | [user-guide.zh-CN.md](./docs/user-guide.zh-CN.md) | [user-guide.md](./docs/user-guide.md) |
| **开发者指南** | 贡献者、自定义 sink/bridge 的实现者。架构、observer 模型、callsite 缓存、sink 契约、registry、代码生成、性能、测试、贡献。 | [dev-guide.zh-CN.md](./docs/dev-guide.zh-CN.md) | [dev-guide.md](./docs/dev-guide.md) |
| **从 `tracing` 迁移** | 当前在 `tracing-subscriber` 上、想要类型化事件 / 采样 / OTel 映射的 crate。 | — | [migration-from-tracing.md](./docs/migration-from-tracing.md) |

### 权威参考

| 位置 | 内容 |
| --- | --- |
| [`./specs`](./specs) | 权威设计文档 — 按数字顺序读，或参考 `index.md`。 |
| [`./examples`](./examples) | 四个可运行示例服务，覆盖 todomvc、两种 interop 模式、sinks 扇出展示。 |
| [`./docs/research/`](./docs/research/) | 第 0 阶段的 spike 备忘。 |
| `cargo doc --workspace --no-deps --open` | 所有 public 项的 rustdoc。 |

## 许可

MIT — 见 [LICENSE.md](./LICENSE.md)。
