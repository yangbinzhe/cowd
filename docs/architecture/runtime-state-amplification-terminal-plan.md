# Runtime 状态放大治理终态方案

> 当前执行权威。本文覆盖 Runtime 派生状态、Session 持久化、Gateway 投影和
> WebUI 消费链。此前关于执行图、Mission 图和三层展示的文档仍是视觉与语义证据，
> 但不得继续定义第二套持久化或投影实现。

## 1. 终态目标

在不删除任何不可逆业务事实、审批证据、外部副作用回执、模型/工具原始载荷和
恢复能力的前提下，完成以下终态：

1. 活跃 Session、Execution、Team、Agent、Tool 状态由 Runtime Hot State Plane
   承担；数据库不参与每次在线状态查询。
2. Runtime Event Journal 只保存不可逆事实和恢复栅栏，不保存 projector cursor、
   reader presence、每事件完整 live snapshot 等可重建派生状态。
3. 可变投影由有 revision/fence 的 Projection State Store 维护；写入可合并，
   不反向触发源事件订阅。
4. 大型、原始、需要精确追溯的内容统一进入内容寻址 Artifact Store；事件、outbox
   和投影只保存引用、哈希和必要摘要。
5. Runtime/Session 产生唯一 typed projection。Gateway 只做鉴权、传输、分页和组合，
   WebUI 只维护一份规范化状态并渲染正文、活动详情和执行图。
6. 所有历史兼容读路径、同义索引、重复投影和未接线 API 被删除，不保留过渡基线。

## 2. 基线与代码事实

### 2.1 仓库基线

| 仓库 | HEAD | Tree | 状态 |
| --- | --- | --- | --- |
| `cowd` | `b44037f748fd8c83a6a04aa524bf33587296d07f` | `cb2737762d74ff05b4fa29994b29189101255726` | 干净 |
| `cowd-edge` | `e403539cdf6c817e1a12fa7d1b58215c94cbd737` | `2d0ddfee540bfc5f10a4bb680da6d61c8a48b739` | 已有图形源码和构建产物修改，工作区 diff SHA-256 为 `f677d697ad3a8c2c76bdca62d8c4075365b32135be85a031762ff75ade51d73b` |

`cowd-edge` 的现有改动不由本计划回退。其内容属于统一图形语义与走线工作，
本计划只能在完成差异审查后接续。

### 2.2 数据基线

当前 PostgreSQL 样本只有 35 条 Session message，却包含：

| 指标 | 数量/大小 |
| --- | ---: |
| `runtime_events` | 3371 行 / 10 MB |
| `runtime_commits` | 3035 行 |
| `runtime_transaction_streams` | 3287 行 |
| projector checkpoint | 1486 行 |
| `execution.live.snapshot.v1` | 544 行 |
| 数据库 | 27 MB |

projector checkpoint 和 live snapshot 合计占 Runtime event 的 60.2%。主要事件表
没有 MVCC 死元组，问题是语义和写入模型放大，不是 VACUUM 不足。

### 2.3 关键源代码事实

| 事实 | 当前文件/符号 | 终态判定 |
| --- | --- | --- |
| 单事件 append 是兼容便利接口 | `runtime_event_store.rs::RuntimeEventStoreBackend::append` | canonical 命令可用事务接口；派生状态禁止调用 |
| PG batch read 为 N+1 | `runtime-postgres/src/lib.rs::events_after_cursor` | 改为一条 CTE/JOIN 查询 |
| 三个 projector 用事件保存 checkpoint | knowledge/evolution/outcome projector | 改为统一 mutable checkpoint |
| live reducer 每次语义变化保存完整记录 | `execution_live.rs::persist` | 改为热状态 + 合并 checkpoint |
| live 恢复读取整个 stream 并保留 V504 fallback | `execution_live.rs::load_record` | 只读 mutable latest；删除 fallback |
| ContextEnvelope 同时保存 selected 和 rendered assembled | `conversation.rs::persist_context_envelope` | 保存 canonical selection 和精确 packed artifact |
| turn report 两份 model summary 相同 | `conversation.rs::build_context_turn_report` | 单一 summary |
| terminal payload 被内联到 event/outbox/message | `host.rs`、`session_runtime_bridge.rs` | artifact ref |
| Gateway 为 Session/Execution 重建第二套投影 | `session_routes.rs` | Runtime/Session typed projection 唯一所有者 |
| WebUI 关闭右栏仍加载 turn/execution projection | `app.ts`、`chatSessions.ts` | transcript 首屏与 inspector 订阅分离 |
| live subscription 写触发全局 revision 失效 | `api/client.ts` | 合同化 invalidation scope |

## 3. 状态真相表

| 状态 | 唯一所有者 | 持久化 | 写入语义 | 恢复 |
| --- | --- | --- | --- | --- |
| 输入接收、图计划/跃迁、审批、effect intent/receipt、终态栅栏 | Runtime Journal | 必须 | durable-before-observe | 按 commit cursor replay |
| projector cursor、live latest、turn index | Projection State Store | 必须 | revision CAS/UPSERT，可合并 | 载入 checkpoint 后 replay journal |
| reader presence、Surface heartbeat | Hot State + TTL projection | 条件持久化 | heartbeat/expiry，不写事件 | 过期即失效 |
| text delta、tool progress、UI 展开状态 | Hot State / transport | 不逐条持久化 | 有界流、背压 | 终态 transcript/工具回执恢复 |
| transcript、原始工具输出、模型请求、完整上下文包 | Artifact Store | 必须 | content-addressed immutable | selector + hash |
| Session/Execution 摘要、业务活动树、技术详情 | Typed Projection | 可重建 | journal delta 驱动 | checkpoint + replay |

## 4. 能力守恒矩阵

| 原能力 | 删除/改变 | 替代证据 | 不允许的退化 |
| --- | --- | --- | --- |
| projector 断点恢复 | 删除 checkpoint event | mutable checkpoint 的 cursor/revision/payload | 重启后重复副作用或漏投影 |
| live execution 恢复 | 删除每事件全快照和旧 stream fallback | latest checkpoint + canonical replay | 丢失 terminal/waiting/error 状态 |
| 完整上下文审计 | 删除重复 assembled/metadata | canonical selection + formatter version + packed request artifact | 无法重建精确模型请求 |
| terminal exactly-once | 删除内联 transcript | artifact hash + terminal fence + outbox materialization | 重复 message 或不可追溯 |
| Session 在线读者 | 删除生命周期全量 attachments | TTL presence projection | writer/control 权限漂移 |
| WebUI 全景展示 | 删除 Gateway/前端重复投影 | 同一 typed projection 的三种 renderer | 图、正文、活动详情状态不一致 |
| 历史检索 | 删除 legacy stream 合并 | typed session/turn/root/activity identity | 历史 Session 缺活动或证据 |

## 5. 版本依赖与实施边界

```text
V1 Derived State Plane
  ├── mutable projector checkpoint
  ├── single-query commit batch
  └── coalesced live latest + recovery
          |
          v
V2 Canonical Payload Plane
  ├── ContextEnvelope / turn report 规范化
  ├── terminal artifact reference
  └── presence TTL/delta
          |
          v
V3 Single Projection Plane
  ├── typed session/turn/root/activity binding
  ├── Runtime/Session summary-detail projection
  └── Gateway legacy projection 删除
          |
          v
V4 Surface Consumption And Closure
  ├── WebUI normalized store / lazy subscription
  ├── scoped invalidation
  ├── migration/index cleanup
  └── integrated recovery/performance/visual validation
```

### 5.1 V1：Derived State Plane

**写入范围**

- `crates/runtime/src/recovery/runtime_event_store.rs`
- `crates/runtime-postgres/src/lib.rs`
- `crates/runtime/src/recovery/{knowledge_candidate_projector,outcome_projector}.rs`
- `crates/runtime/src/evolution/projector.rs`
- `crates/runtime/src/execution_core/execution_live.rs`
- 对应测试

**必须完成**

1. 新增统一 `RuntimeProjectionCheckpoint` 合同和 backend CAS API。
2. SQLite/PG 新增 mutable checkpoint 与 live checkpoint 表。
3. 三个 projector 全部迁移，不再产生任何 `*.projector.checkpoint.v1` event。
4. checkpoint 更新不得发布 commit signal。
5. `events_after_cursor` 在 PG 通过单查询返回 commit 与 event。
6. live 更新只写 Hot State；等待外部输入、周期边界和 terminal 状态写合并 checkpoint。
7. restart 使用 latest checkpoint + checkpoint cursor 之后的 canonical event 恢复。
8. 删除 V504 fallback、旧 checkpoint stream 读取和对应测试。

**验收**

- projector checkpoint event 生产符号扫描为零。
- 单个 batch 的 SQL 查询数量与 commit 数量无关。
- 同一 execution 高频事件不会线性增加 durable snapshot。
- kill/restart 后状态、指标、terminal ref 与未优化路径一致。

### 5.2 V2：Canonical Payload Plane

**写入范围**

- `crates/runtime/src/context/*`
- `crates/runtime/src/conversation/{conversation,host}.rs`
- `crates/runtime/src/session/*`
- `crates/session/src/*`
- Artifact 与 terminal materializer 对应实现

**必须完成**

1. Context event 只保留一次 budget/diagnostics/profile。
2. `selected` 是 canonical；`dynamic_tail` 按 formatter version 派生。
3. 精确 provider request 只保存一个内容寻址 artifact。
4. observation 只保留一份 model summary。
5. terminal event/outbox 只保存 artifact selector/hash 和 fence。
6. reader presence 使用 TTL/delta；不在每次事件复制 attachments。
7. 大于配置阈值的 raw/tool/context/transcript body 自动进入 blob tier。

**验收**

- Context event 中不存在结构相等的内外 metadata。
- report 中不存在两份相同 summary。
- outbox `payload_ref` 不含 `assistant_terminal_v*:` 内联正文。
- artifact 缺失、hash 不符、orphan GC 都有失败/恢复测试。
- Session writer/control authority 行为与当前实现等价。

### 5.3 V3：Single Projection Plane

**写入范围**

- `crates/harness-contract/src/projection/*`
- `crates/runtime/src/projection/*`
- `crates/runtime-postgres/src/lib.rs`
- `crates/gateway/src/api_routes/session_routes.rs`
- Gateway projection service 和契约测试

**必须完成**

1. execution 相关 canonical event 必须携带 typed session/turn/root/activity identity。
2. summary、business activity、technical detail 是同一 projection 的 detail scope。
3. 单 turn 查询复杂度与目标 turn 的事件量相关，不与 Session 总历史相关。
4. Runtime projection 按 execution revision/detail scope 缓存和 delta 更新。
5. Gateway 删除 `turn_projection_from_event_values`、legacy `session:{id}` 合并及
   重复 run tree 构造。
6. 历史与实时读取返回同一 schema、同一状态机和同一排序规则。

**验收**

- production event producer 的未绑定 execution event 扫描为零。
- `/turns` 只返回轻量 index；详情显式按 turn/execution 查询。
- 新旧 Session 的正文、活动详情和执行图使用同一 projection revision。
- legacy runtime stream、旧 projection helper 的生产引用为零。

### 5.4 V4：Surface Consumption And Closure

**写入范围**

- `cowd-edge/surfaces/webui/src/api/client.ts`
- `cowd-edge/surfaces/webui/src/stores/{app,chatSessions}.ts`
- projection registry、活动/图 renderer 及测试
- PG/SQLite 清理迁移、文档和最终证据

**必须完成**

1. 一个 normalized execution store 服务正文、活动详情和图。
2. 首屏只加载 Session metadata、transcript tail、turn summary 和 active pointer。
3. 右栏/图未打开时不请求 full projection，不建立 execution detail SSE。
4. invalidation scope 由 API contract 决定，控制类写入不得全局清空缓存。
5. 删除精确重复索引；其他索引在 `pg_stat_statements` 证据后决定。
6. 迁移旧 projector/live 派生事件并清理无事件 commit/transaction stream/head。
7. 对桌面和移动端做历史、实时、并行 Team/Tool、等待审批、失败恢复视觉验证。

**验收**

- 右栏关闭时网络请求中没有 execution full/detail/evidence。
- transcript 与 active progress 不依赖刷新。
- 三个 renderer 对同一 activity 的 status/title/result/evidence 一致。
- 历史 turn detail 不触发全 Session 事件重建。

## 6. 性能门禁

使用同一场景、同一模型、同一工具输入做前后对照：

| 指标 | 终态门禁 |
| --- | --- |
| projector checkpoint event | 0 |
| live snapshot 写入 | 每 configured coalescing window 最多一次，另加 waiting/terminal 边界 |
| `events_after_cursor` SQL | 每批常数次 |
| 单轮 DB 写入 | 相比基线降低至少 60%，且 canonical event 数量不减少 |
| `/turns` 摘要响应 | 不包含 raw context/evidence/tool I/O；当前样本目标低于 250 KB |
| 单 turn detail | 随目标 turn 大小增长，不随 Session 总历史增长 |
| WebUI 首屏 | 右栏关闭时不加载 full projection；消息先渲染 |
| 恢复 | crash 后无重复 effect、无重复 message、无丢失 terminal |

百分比是当前样本的最低门禁，不是通过删除能力获得的限额。canonical event、artifact
和审批/副作用证据数量必须单独核对，任何减少都要有一对一替代证据。

## 7. 对抗性审查结论

### 7.1 明确保留

- PostgreSQL，不因当前 27 MB 更换数据库。
- SSE，不用 WebSocket 掩盖投影重复。
- canonical event journal 和 exactly-once terminal fence。
- raw evidence 和完整模型请求，只改变为 artifact 引用。
- per-key ordering、审批、租约、资源准入和恢复语义。

### 7.2 明确删除

- projector checkpoint event。
- 每事件 full live snapshot。
- V504 live fallback 和 Gateway legacy session stream。
- Context/turn report/terminal 的结构重复。
- reader attachment 全量生命周期快照。
- 精确重复索引和未消费的第二套前端投影。

### 7.3 暂不删除

`execution_node.transitioned` 与 graph node transition 存在语义重复，但当前仍承担
独立 Activity identity。只有 V3 的 canonical projection 完成并通过关系、证据、
恢复等价测试后，才能决定改为 projection row；不得在 V1/V2 提前删除。

## 8. 最终反向证据链

```text
WebUI activity / graph / terminal
  -> typed projection revision
  -> canonical event or mutable projection row
  -> execution/session/turn/activity identity
  -> graph transition / approval / effect / terminal fence
  -> admitted user input and authorization
```

每条边必须能给出 owner、表/事件、revision、失败语义和测试。无法反向到原始输入的
展示不属于业务真相；无法从 canonical journal 恢复的 mutable state 不得上线。

## 9. 实施状态与证据入口

本方案的 V1-V4 已于 2026-08-07 按同一终态完成，不保留并行旧实现。权威证据：

- [基线证据](evidence/runtime-state-amplification/baseline.md)
- [实施映射](evidence/runtime-state-amplification/implementation.md)
- [验证报告](evidence/runtime-state-amplification/validation.md)
- [最终门禁](evidence/runtime-state-amplification/final-gate.md)

代码完成度与运行环境部署状态分别留证后已经闭环：迁移先用当前 PostgreSQL 数据库的
完整副本通过正式 `cowd storage upgrade` 验证，再于 2026-08-07 受控停止旧 Gateway，
对权威数据库执行同一升级并原子替换 full Release。新 Gateway、WebUI、PostgreSQL
catalog 和单实例进程复核结果见验证报告。
