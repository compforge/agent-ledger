# AGENTS.md

## 项目定位与边界

Agent Ledger 是一套框架无关的 Agent 执行事实规范，以及围绕该规范实现的多语言 SDK 与框架
适配器。Agent Loop 和 Orchestrator 可以向同一个 Session 写入不可变事件，供恢复、全局
Timeline、轨迹分析和评测等下游用途消费。

- 稳定产品是 `spec/` 中的协议；语言 SDK 保持轻量，框架差异主要由 Adapter 承担。
- Ledger 只拥有“发生过什么”的事实，不拥有具体 Agent Loop、编排状态机、调度、Memory、
  Eval 或能力更新策略。
- 恢复由理解框架原生上下文、checkpoint 和 resume API 的 Adapter 完成；Core 不构造通用
  `RunContext`。
- V1 不要求 Collector 或网络服务。应用直接注入内存、Redis 或数据库 `EventStore`。

## 代码地图与核心模块

| 目录 | 职责 |
| --- | --- |
| `spec/` | Event envelope、追加语义、Adapter 能力和恢复边界的唯一规范来源 |
| `conformance/` | 跨语言 canonical encoding、append digest 和契约向量 |
| `python/` | Python Core、Memory/Redis/SQL Store、投影与 plain-loop 参考实现 |
| `typescript/packages/core/` | TypeScript Core 与 Memory Store |
| `typescript/packages/pi/` | Pi hooks 和 lossless native `SessionStorage` 适配 |
| `go/` | Go Core、Memory Store 与公共 Recorder API |
| `go/adapters/agentgo/` | AgentGo model/tool/message hooks 与原生恢复适配 |

## 核心模型与关键约定

1. `Session` 是端到端任务及全局 Timeline 的边界，不等同于任一框架的 chat/session 对象。
2. `Run` 是 Agent 或 Orchestrator 的一次语义执行；`EventStream` 是独立的物理 OCC 追加分区。
   两者不能合并，否则框架原生状态无法跨进程替换或多个 runtime run 延续。
3. `Step` 表示可跨重试的逻辑工作，`Attempt` 表示一次真实模型或工具调用。重试沿用
   `step_id`，但必须生成新的 `attempt_id`。
4. 分布式执行关系由 `parent_run_id` 与 `caused_by_event_id` 表达。时间戳和
   `commit_cursor` 只用于观察与分页，不能建立因果或全局执行顺序。
5. 严格 Adapter 必须在外部模型或工具调用前等待 requested 事件持久化，并在 Loop 前进前写入
   completed/failed。只有 requested、没有终态的 Attempt 是待协调事实，不能静默重放有副作用的
   工具。
6. Normalized events 服务跨框架观察和分析；framework-native records 服务无损恢复。Pi 的 entry
   tree、active leaf 等语义只属于 Pi Adapter，不能提升为 Core Session 模型。
7. Store 追加必须保持原子批次、canonical-content 幂等、乐观并发和 Session 内 event ID 唯一。
   Store 不负责 run ownership、lease、fencing 或调度。
8. Reader 必须保留未知事件类型、payload 字段与 extensions；同一 major schema 内只做兼容性
   扩展。

## 开发约定

- 修改 Event envelope 或追加契约时，同步检查 JSON Schema、RFC、跨语言类型、canonical digest
  向量和 Store contract tests，避免各语言形成隐式方言。
- 新增框架能力优先放入独立 Adapter；只有能跨框架保持相同语义和约束的事实才进入 Core。
- Adapter 的 capability descriptor 必须描述实际安装后的保证。不能把 telemetry-only hook 标成
  strict，也不能用 normalized events 冒充 lossless native state。
- 大输入输出通过 `ArtifactRef` 引用，业务选择的 Artifact Store 保存内容；不要把大对象直接
  塞入事件流。
- 仓库公开发布，提交内容不得包含公司内部链接、标识、凭据、个人机器路径或仅适用于内部环境的
  约定。
- 开发和验证入口统一使用根目录 `Makefile`；更细的环境与命令说明以 README 为准。

## References

- `README.md` — 使用者视角的项目定位、模型和最短开发入口
- `spec/rfcs/0001-agent-ledger.md` — Ledger 核心契约、因果模型与读侧投影
- `spec/rfcs/0002-polyglot-adapters.md` — 多语言 Adapter、能力声明与恢复边界
- `spec/schemas/event.schema.json` — 规范化事件 envelope
- `spec/schemas/adapter.schema.json` — Adapter descriptor
- `conformance/README.md` — 跨语言一致性要求
