# Checkpoint

Checkpoint 是 Harness 原生状态的版本化恢复基线。它与 Event Ledger 位于同一个项目中，但不要求
一起使用：上游可以只保存和加载 Checkpoint，也可以将 Checkpoint 锚定到 Lane，以 Ledger 补齐
Checkpoint 之后已经发生的动作。

## 边界

- Harness 与 Adapter 决定状态由什么组成，以及如何 dump、restore 和应用已完成动作的结果。
- Checkpoint Store 只保存不透明状态、格式标识、版本和可选的 Ledger 锚点，不理解状态内容。
- Event Ledger 记录不可变执行事实，用于审计、轨迹投影和 Checkpoint 之后的恢复。
- Agent Loop、调度和 `running / waiting / frozen` 等控制状态不属于 Agent Ledger。

因此，`Action.type = checkpoint` 只表示 Harness 执行过一次 checkpoint 动作；`Checkpoint` 才是该
动作产出的持久化恢复材料，二者不是同一个对象。

## 对象

```text
Checkpoint key
  ├─ Checkpoint revision 1
  ├─ Checkpoint revision 2
  └─ Checkpoint revision 3 (latest)
```

每个 Checkpoint 包含：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | Checkpoint envelope 的协议版本，约束 Ledger 字段 |
| `id` | 本次保存请求的 UUIDv7，也作为幂等键 |
| `key` | Harness 原生可恢复实例的稳定标识，用于组织多个 revision |
| `revision` | Store 分配的单调版本；首个版本为 1 |
| `actor_id` | 产生该状态的 Actor |
| `format` | Adapter 解释的不透明格式，例如 `application/vnd.compforge.agentgo.message+json;version=1` |
| `state` / `artifact_ref` | 二选一；小状态内联为 JSON，大状态引用 Artifact Store |
| `anchor` | 可选的 Ledger 恢复位置 |
| `extensions` | 不影响 Core 语义的扩展信息 |

`format` 由对应 Adapter 判断兼容性。Ledger 不解析 media type、厂商名或 `version` 参数，也不根据
它转换状态。`schema_version` 与 `format` 是两条独立演进轴：前者描述 Checkpoint envelope，后者
描述 Harness state；Anchor 等 Ledger 字段变化不应迫使 Harness state 升版，反之亦然。

## Ledger anchor

Anchor 是一个完整三元组：

```text
lane_id + last_applied_seq + last_applied_event_id
```

它表示该 Checkpoint 已经包含指定 Lane 上 `last_applied_seq` 及之前的状态影响，并且该位置对应
`last_applied_event_id`。Store 在保存时验证 Event 确实属于该 Lane 且 seq 相等。没有使用 Ledger
时不设置 Anchor。

恢复时读取 `seq > last_applied_seq` 的 Events：

1. 加载指定 `key` 的最新 Checkpoint。
2. Adapter 校验 `format` 并恢复 Harness 原生 State。
3. 若存在 Anchor，按 Lane seq 重放已经完成动作的结果，而不是重新执行动作。
4. 对只有 requested、没有终态的 Attempt 做对账、重试或人工处理。
5. Harness 继续未完成的 Turn，并在新的安全边界保存下一 revision。

只有当 Adapter 能将 completed outcome 幂等地应用到 Harness State 时，Ledger 才具有 redo 能力；
否则它仍然只是审计与 trajectory 数据。外部 Tool 的副作用也不能仅凭 Event 安全重试。

## 保存契约

`save_checkpoint(expected_revision, proposed_checkpoint)` 使用 Checkpoint key 级 OCC：

公开模型统一使用 `key`；SQL Store 的 DB Model 可将其映射为 `checkpoint_key`，避免把数据库命名细节
泄漏到 Harness 契约。Actor 的 `key` 与物理列 `actor_key` 采用同样边界。

- 第一次保存传 `expected_revision = 0`，Store 返回 revision 1。
- 后续保存必须携带当前 revision，成功后返回 revision + 1。
- 同一个 `id` 与相同内容重复提交时返回原结果，不产生新 revision。
- 同一个 `id` 携带不同内容时是幂等冲突。
- `state` 和 `artifact_ref` 必须且只能设置一个。

Checkpoint revision 一经保存不可修改。Checkpoint 的保留与垃圾回收可以采用独立策略；这不改变
Event Ledger 的 append-only 语义。

## Run 完成关联

当某个 Checkpoint 同时是 Run 的安全终止边界时，Adapter 先保存 Checkpoint，再在同一个 Lane append
中依次记录 `lane.framework.checkpoint.linked` 与 `run.completed`。三个语言的 `LaneRecorder` 都提供
通用的原子批量 append；`run.completed.causation_id` 指向 link Event，link payload 使用
`checkpoint_id`、`profile`、`profile_version` 和可选 `metadata`。

这条原子性只覆盖两个 Ledger Event，不跨越 Checkpoint Store 与 Event Store。Checkpoint 保存后、
Event append 前崩溃会留下未关联 revision；它不构成已完成 Run，也不应自动成为上游采用的恢复点。
反过来，同批 Event 成功后，读侧不会看到缺少 Checkpoint 引用的 `run.completed`。

Run inspection 会汇总全部终态 Event、Checkpoint link 和未决 Attempt，但不从中选择当前状态或恢复点。
`run.completed` 只表示生产者声明该 Run 范围已经完成；Run 的业务含义以及上游是否接受结果、处理输入
或提交控制状态，仍由上游决定。

## 与数据库恢复的类比

可以把组合恢复理解为：

```text
Harness Checkpoint + WAL-like Agent Ledger
      数据基线       +      后续执行事实
```

这个类比只描述恢复结构。Agent 的 Tool 可能修改外部系统，因此并不具备数据库事务的封闭性。
