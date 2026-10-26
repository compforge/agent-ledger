# AGENTS.md

## 项目定位与边界

Agent Ledger 是框架无关的 Agent 执行账本与 Checkpoint 规范，以及遵守该规范的多语言 SDK 和
Harness Adapter。它记录不可变执行事实和 Harness 原生恢复基线，用于安全恢复、审计、轨迹提取
和评测。

- 稳定产品是 `spec/` 的协议；各语言 Core 保持同构，框架差异由 Adapter 承担。
- Ledger 拥有“发生过什么”，不拥有 Agent Loop、编排控制状态、调度、Memory、Eval 或策略激活。
- Harness 原生状态及其 dump/restore 语义由 Harness 和 Adapter 共同拥有；Checkpoint Core 只保存
  不透明状态和可选 Ledger 锚点，不构造通用 `RunContext`。
- V1 不要求 Collector 或网络服务。应用直接注入 Memory、Redis、Bolt 或关系型 `EventStore` /
  `CheckpointStore`。

## 代码地图与核心模块

```text
.
├── spec/                         # 协议的唯一规范来源
│   ├── rfcs/                     # 对象、追加、Adapter 与恢复边界
│   └── schemas/                  # Event、Adapter descriptor 与参考 SQL
├── docs/                         # Checkpoint、共享事实与 Observation 的设计说明
├── conformance/                  # 跨语言 canonical encoding 与 digest 向量
├── python/                       # Python Core；Memory、Redis、SQLAlchemy Stores
├── typescript/
│   └── packages/
│       ├── common/               # 共享调用事实；无存储或渲染依赖
│       ├── observation/          # SessionView / JobView，校验与渲染
│       ├── core/                 # TypeScript Core 与 Memory Store
│       └── pi/                   # ledger / observation 两个独立入口
└── go/                           # Go Core 与公共 LaneRecorder
    ├── common/                  # 共享模型身份、请求/响应、Usage 与 Error
    ├── observation/             # SessionView / JobView，无 Store 依赖
    ├── ledger/observation/       # Ledger 内可选观察入口
    ├── agentgo/                 # ledger / observation 按接入库组织
    └── stores/
        ├── bolt/                 # 单文件 KV Store
        └── gorm/                 # 可注入 MySQL/SQLite 等 driver 的关系型 Store
```

## 核心模型与关键约定

1. 层级固定为 `Session → Run → Lane → Turn → Action → Attempt`。Session 和 Run ID 由上游提供；
   Ledger 不创建权威 Session/Run 行，也不规定 Run 的业务来源和生命周期。
2. Run 是 Session 与 Lane 之间的稳定聚合边界；Lane 是 Run 内的串行链路，也是 OCC 与 `seq` 的
   边界。一个 Run 可有 main、分支和 framework-native Lanes；跨 Lane 顺序只用于观察。
3. Turn 是稳定交互边界；Action 是 `model_call`、`tool_call`、`compact` 等逻辑动作，并在执行前固化
   `Effect(kind, idempotency)`；Attempt
   是一次物理尝试。重试沿用 Action，并递增 `attempt_no`。Core vocabulary 提供跨语言常量，但
   type 字段保持开放；扩展使用 namespaced value。
4. Actor、Lane、Turn、Action、Attempt、Event 和 append ID 使用 UUIDv7。业务表只表达不可变身份
   和从属关系；生命周期、输入、输出和失败均表达为 Event。
5. Event 的 `event_type` 前缀决定 `subject_id` 的类型；`causation_id` 表达因果。时间戳、UUIDv7
   和 Session 投影顺序都不能替代因果关系。
6. 严格 Adapter 必须在模型或工具调用前持久化 `attempt.requested`，在 Loop 前进前写入 Core
   terminal outcome。`cancelled` 只表示已确认取消，无法确认外部结果时使用 `outcome_unknown`；未决
   副作用工具不能静默重试。
7. normalized Events 服务跨框架审计和分析；Checkpoint 保存不透明的 framework-native State。
   Pi entry tree 等私有语义不能提升为 Core 模型。
8. Store 保证原子批次、canonical-content 幂等、Lane OCC、全局 Event/append ID 唯一以及不可变
   归属校验。SQL 不声明外键，关系由 Store 维护。
9. Reader 必须保留未知 Event type、payload 与 extensions；同一 major schema 内只做兼容扩展。
10. `RunView` 是按上游 `(session_id, run_id)` 查询的执行读模型，不创建权威 Run 行；Run inspection
    只汇总终态事实、Checkpoint link 和未决 Attempt，不选择控制状态或恢复策略。

11. `common` 表达真正共享的身份与调用事实；Ledger 侧重 Session 的可恢复性，`observation`
    提供 SessionView 和 JobView 两种可评估的观察视角，共用 Item 模型。一个 SessionView 只表达一个
    Agent Session；JobView 围绕任务组织多个 Session 的相关执行片段。followup/resume 不自动新建 Session。
12. 同名但职责不同的类型可以留在各包，例如 Ledger 查询用的 SessionView 与 observation.SessionView；
    同时使用时 alias，只有语义与契约一致的类型才进入 common。
13. Pi、AgentGo 的适配各归一个库目录，分别提供 ledger 与 observation 入口。原生 Session 可直接
    生成观察模型。通用 Ledger 转换归 Ledger 可选读侧入口；不再发布独立 projection 包。
    Ledger 核心不导入 observation，observation 不导入 Store。缺失结果与 outcome_unknown 保持区别，
    展示不作恢复决策、不重建模型未实际收到的请求、不将其他 Session 的执行体合并进当前 Session。

## 开发约定

- 修改对象模型或 append 契约时，同步检查 JSON Schema、RFC、参考 SQL、跨语言类型、digest
  向量和 Store contract tests。
- 新 Harness 能力优先放入独立 Adapter；只有跨 Harness 语义和约束一致的事实才进入 Core。
- Adapter capability 必须描述实际保证，不能把 telemetry-only hook 标记为 strict。
- Python 关系型 Store 使用 SQLAlchemy；Go 关系型 Store 使用 GORM。连接、driver、连接池和超时由
  应用显式注入；测试可用 SQLite，协议不得绑定具体数据库方言。
- 大输入输出先注册为带 caller-owned `key` / `version` 的不可变 `Artifact`；Event 和
  Checkpoint 只保存 `artifact_id`，实际内容由外部 Content Store 保存。
- 仓库公开发布，禁止提交内部链接、标识、凭据、个人机器路径或仅适用于内部环境的约定。
- 开发和验证入口统一使用根 `Makefile`。

## References

- `README.md` — 使用者视角的项目定位、模型和最短开发入口
- `spec/rfcs/0001-agent-ledger.md` — Ledger 核心契约、因果模型与读侧投影
- `spec/rfcs/0002-polyglot-adapters.md` — 多语言 Adapter、能力声明与恢复边界
- `docs/observation.md` — 共享调用契约、Ledger/Observation 边界与使用方式
- `docs/checkpoint.md` — Checkpoint 对象、保存契约、Ledger 锚点与组合恢复
- `spec/schemas/event.schema.json` — Event envelope
- `spec/schemas/artifact.schema.json` — Artifact identity and immutable version metadata
- `spec/schemas/common.schema.json` — Ledger/Observation 共享调用事实
- `spec/schemas/session.schema.json` — 执行读模型与跨语言展示契约
- `spec/schemas/call-payload.schema.json` — Core model/tool Attempt payload profiles
- `spec/schemas/checkpoint.schema.json` — Checkpoint envelope
- `spec/schemas/mysql.sql` — 无外键的参考关系型 Schema
- `spec/schemas/adapter.schema.json` — Adapter descriptor
- `spec/vocabulary.json` — 跨语言 Core Action/Event type vocabulary
- `conformance/README.md` — 跨语言一致性要求
