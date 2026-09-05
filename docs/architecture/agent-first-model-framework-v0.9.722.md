# Agent-first 模型框架终态（v0.9.722）

日期：2026-09-05
状态：实现权威；取代 v0.9.721 及更早的 Team recipe、V2 编排、模板驱动和双控制面方案。
外部架构权威：`/media/yi/Datas/workspace/plan/cowd-agent-first-model-framework-2026-09-04.md`

## 1. 终态目标

框架只承担模型不擅长、但系统必须可靠承担的工作：提供准确上下文和可调用能力；执行模型作出的合法决策；维持身份、权限、并发、幂等、恢复、证据和终态事实。模型保留任务理解、组织设计、角色选择、工作分解、讨论、实验、取舍、综合和复盘的控制权。

这意味着：

- Team、Agent、Task、依赖和讨论主题从任务证据动态产生，不来自固定拓扑；
- 模型用小型 typed Action 表达意图，长正文进入 ArtifactStore，不把长文本塞进编排 JSON；
- Runtime 不代替模型“完成”任务，也不以提示词、节点数量或配置值伪造进展；
- 安全、身份、资源容量和事实一致性是硬边界；业务步骤、角色数量、执行顺序和推理深度不是硬编码流程；
- 预算和 usage 是观测与成本反馈，不是已准入任务的隐性截断器。

## 2. 唯一所有权

| 事实/决策 | 唯一 owner | 非 owner 的职责 |
|---|---|---|
| 目标理解、Team/Agent/Task 设计 | 模型 | Runtime 提供事实与能力清单 |
| Program、Roster、Work、Topic、Artifact、Objective 事实 | `AgentActionService` | Gateway 适配；MFG 只读投影 |
| 物理执行、依赖、租约、幂等、恢复 | `ExecutionGraph` | Action Service 提供语义工作事实 |
| Task 完成判定 | 独立 `task_review` | ObjectiveSupervisor 不重复解释 Task disclosure |
| Objective 完成判定 | `ObjectiveSupervisor`；在线仅由 Program projection lane 触发 | Gateway 只持久化 `objective_complete_request`；启动恢复只补偿未完成请求 |
| 身份与权限 | Runtime attested actor/binding | 模型只使用已签发身份和能力 |
| Session/Team/Agent 前端状态 | Runtime snapshot/delta/resync | MFG 不维护第二份 registry |

同一事实不得存在两个可写 owner。Task reviewer 已接受的 Task 不再被 ObjectiveSupervisor 以 `TaskSubmit.unresolved` 二次否决；Task 的 `unresolved` 是交付披露，Objective 的 `unresolved` 才是目标终态阻断项。

## 3. 模型—框架契约

### 3.1 小动作、长内容

模型通过以下稳定 Action 自主推进：

- `team_create`
- `agent_invite`
- `task_publish` / `task_claim` / `task_release` / `task_supersede`
- `artifact_commit`
- `task_submit` / `task_review`
- `message_publish`
- `objective_complete_request`

Action 仅包含引用、短摘要、决策和必要元数据。报告、代码、实验日志、研究正文先由普通工具写入工作区或 ArtifactStore，再以 `artifact:*`、`artifact://*`、`tool://*` 引用进入 Action。Gateway 负责 JSON/schema 校验和内容引用适配，不要求模型转义或回显大段长字符串。执行 Agent 的 `artifact_commit` 会由 Runtime 根据已认证的 claim/execution binding 自动补入当前 Task 关系；模型的 `relates_to` 只表达额外语义血缘，不能因漏抄当前 Task ID 产生孤儿 Artifact 和重复提交。

### 3.2 原生工具循环

根 Agent、Team worker 和 reviewer 运行同一种 provider tool loop。Runtime 在每轮只注入当前可行动的 Program 增量、精确身份、Task 状态、未读 Topic 和已认证证据；模型可主动领取、拒绝、讨论、执行、提交或复核。

Action receipt 改变持久状态后，Runtime 再把紧凑的 canonical checkpoint 反哺模型。重复 checkpoint 以语义进展 digest 熔断；状态真实变化不受固定“N 轮”业务上限影响。

### 3.3 结论动作优先

证据已充分且采集开始饱和时，普通 evidence role 进入无工具综合；但未终态的 Agent-first worker/reviewer 不进入展示型文本终态。v0.9.722 把“证据已足够”转换成只暴露下一项持久动作的收敛轮：执行者依次完成 `artifact_commit`、`task_submit`，评审者完成 `task_review`。这避免 Agent 先生成一份无工具结论、再由外层自治 checkpoint 恢复同一动作所造成的额外模型轮和费用；它只改变动作时机，不替模型生成内容或裁决结论。

## 4. 语义工作与物理执行

```text
User objective
  -> Root model chooses teams/tasks/dependencies
  -> AgentActionService commits semantic Program facts
  -> Work market exposes dependency-ready Tasks
  -> Scheduler admits physical Agent graphs under shared capacity
  -> Agent claims -> tools/work -> artifact -> submit
  -> independent reviewer graph -> task_review
  -> dependent Tasks become ready
  -> final synthesis -> independent review
  -> objective_complete_request -> ObjectiveSupervisor
  -> Verified terminal projection
```

语义 Agent 和物理 Agent instance 分开投影，但由不可变 binding 关联。前端不得把 reviewer instance 当作匿名单节点，也不得用物理 instance id 覆盖业务 Agent 的 display identity。

## 5. 自主并发

- 无依赖 Task 进入共享 work market 后即可同时 claim 和执行；
- `JoinSet` 执行独立图节点，资源管理器只限制真实 CPU/IO/provider 容量；
- 单 Session 的控制事件保持有序，不把跨 Agent 的物理工作串行化；
- root barrier 由 Program revision、Task 终态和子图终态唤醒，不占 provider permit 轮询；
- 审批只阻塞自身依赖路径；本场景的合法读写和 Agent Action 不应产生人工审批；
- provider 费用、token usage 和 cache 指标不拒绝已经通过权限与资源准入的工作。

“最大并发”指依赖允许且资源可用时尽快填满容量，不等于无视依赖或无限创建 Agent。业务分解仍由模型根据工作价值决定。

## 6. 身份、授权与证据

每个 Agent actor 同时绑定：

- Session、Turn、Objective、Program；
- Team、semantic Agent、Task；
- physical execution graph、node、attempt/generation；
- model lease、permission ceiling、resource scopes；
- display name 与角色说明。

执行 Task 必须属于绑定 Team；独立 review 可以跨 Team。旧实现把执行 fence 复用于 review，导致语义 `task_review` 已接受后物理 reviewer 图失败；v0.9.722 明确只对 `mode=execute` 使用同 Team fence。

证据边界：

- `tool://` 必须能在 Session journal 中解析为真实、已完成 receipt；
- `artifact://` 必须通过 ArtifactStore 的 scope/readability 校验；
- Runtime 自动把认证 artifact content 绑定到 Task evidence，模型不回显内部 selector；
- reviewer 可跨执行读取同 Session 中授权的提交证据；
- Packet 不能跨 graph/node/generation 重绑定；
- 终态不得仅依据模型文字、文件名或 Action 参数声称成功。

## 7. 完成权威

Task lifecycle：

```text
Published -> Claimed -> Submitted -> Accepted
                     \-> Rework -> Claimed ...
                     \-> Blocked
```

- author 负责 `task_submit`，不可自审；
- reviewer 负责 `task_review`，accept/rework 必须引用实际检查证据；
- accepted Task 的 `unresolved` 仍在报告中保留，但不被第二 owner 重新否决；
- 最终 Artifact 通过“实际提交该 Artifact 的 accepted Task → Task depends_on”传递血缘证明跨 Team 集成；依赖已被合法 supersede 时，遍历同 Team replacement lineage，并仍只让 accepted successor 贡献覆盖；模型只需维护正常依赖 DAG，不必在最终 Artifact 重复枚举全部上游引用；模型可写的 `Artifact.relates_to` 只作语义导航，不能单独证明 Team 覆盖；Runtime 以循环安全的确定性图遍历验收；
- Objective 只有在依赖 Task 已接受、物理 Agent 图无失败、目标级 `unresolved` 为空、关键交付存在时才 Verified；
- evaluator 同时检查语义 Program 和物理 Agent 图，禁止“任务看起来都 accepted、但 reviewer/worker graph 实际 failed”的假通过。

## 8. Provider、上下文与缓存

稳定 system/tool/schema/通用能力信息位于请求前缀；Program 增量、当前 Task、最新 receipts 和用户变化信息位于后部。canonical 内容按稳定顺序序列化；长内容以引用按需读取。缓存优化不得删除业务所需历史，也不得把模型降格为字段抽取器。

本版本验收只允许指定生成模型。DeepSeek 场景禁止自动回退到百炼原生生成模型；百炼只可保留 embedding 配置，TokenPlan 是独立授权通道。报告必须列出所有实际 provider/model attempts，发现非指定生成模型即失败。

## 9. Projection 与 MFG

Runtime 输出 versioned snapshot、ordered delta 和 resync。投影至少包含：

- Team 名称、使命、状态和成员；
- Agent display name、角色、语义状态与物理 activity；
- Task 状态、owner、claimant、依赖、reviewer 和 acceptance；
- Topic/message、Artifact、wait、approval、recovery；
- semantic edge 与 physical execution edge。

MFG 只归约 canonical projection；乱序/重复 delta 幂等，epoch/cursor 不连续时 resync。页面刷新和断线重连不得退回单节点或匿名 Agent。

## 10. 被删除的旧架构

v0.9.721 → v0.9.722 完成大规模不兼容切除：

- 删除 orchestration coordinator/compiler/planner/recipe/template/team-instantiation 双路径；
- 删除 `runtime_orchestrate`、`submit_collaboration_decision`、`request_collaboration_escalation` 生产入口；
- 删除 Team-local 第二状态机、结果 reducer 和模板候选控制面；
- 删除长 JSON 编排与 prompt repair 作为状态机修复的做法；
- Gateway executor 拆分为授权执行、证据、Runtime tools、introspection 和 host 适配模块；
- Agent worker 拆分为 model loop、tool turn、scope、evidence collector、terminal 模块。

代码差异为 290 个文件、约 29.7k 行新增和 71.1k 行删除；旧三工具与核心旧类型生产引用为零。删除量大于新增量，能力由统一内核承接，而不是 facade 包装。

## 11. 必须持续成立的门禁

1. 至少两个有真实 Task 的 Team；测试规模要求时每 Team 至少两个实际 Agent。
2. 独立 Task 的物理执行时间存在重叠，且观测到的并发大于 1。
3. 每个 accepted Task 都有 artifact、submit、独立 review 和真实 evidence chain。
4. 跨 Team review 合法；跨 Team execute 拒绝。
5. 语义接受不能掩盖任一物理 Agent graph failure。
6. 最终综合依赖真实上游 accepted Tasks；最终交付再经独立 review。
7. Objective Verified，pending approvals=0，recovery required=0。
8. Session、Runtime DB、workspace artifacts、harness report 四方事实一致。
9. MFG snapshot/delta/resync 和生产 dist 浏览器门禁通过。
10. 指定模型唯一，provider 不静默回退；版本、分支、tag、安装二进制 SHA 可追溯。

## 12. 非目标与边界

- 框架不保证弱模型在任意领域都能得出世界最优结论；它保证模型获得可用信息、自由编排空间、真实工具和可恢复执行，并且失败不会被伪装成成功。
- 框架不把“更多 Agent”当作质量本身；高耦合工作可由少量 Agent 深做，独立工作才并发。
- 安全和权限边界不会为追求自治而取消；业务流程硬编码、固定团队模板和费用型截断则不属于安全边界。
- SQLite 可用于隔离测试；生产仍按配置使用 PostgreSQL 优先拓扑，不能静默热切换或双写。
