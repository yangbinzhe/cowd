# Session、Turn、Task 与 Mission 终态治理

本文定义 Cowd 对话载体、执行片段、业务目标和长期目标的唯一关系。实现以
`harness-contract` 合同、`runtime::task`、Session 持久入口和 Mission Control 投影为准。

## 领域关系

```text
Workspace
  |
  +-- Session A ---------------- Turn A1 --primary--> Root Task 1 --owns--> Mission X
  |      |                         |                     |
  |      +---------------- Turn A2 --continue----------+
  |      +---------------- Turn A3 --primary--> Root Task 2 --owns--> Mission Y
  |
  +-- Session B ---------------- Turn B1 --handoff-----> Root Task 1
                                                        |
                                                        +-- Delegated Task 1.1
                                                        |      +-- Team run
                                                        |             +-- Agent run
                                                        |                    +-- Skill/Tool activity
                                                        +-- Execution graph/evidence
```

- Session 是对话、消息顺序、恢复和参与者权限的所有者，不拥有 Mission。
- Turn 是一次被接纳的输入或执行片段。每个 Turn 最多有一个 primary Root Task；复合输入可绑定额外 Root Task。
- Task 是可验收目标，可跨多个 Turn 和受控跨 Session。Root Task 表示用户目标，Delegated Task 只表示 Team/Agent 分工。
- Mission 是跨 Session、跨 Task 的长期治理边界，只直接聚合 Root Task。
- `TaskAggregate.mission_id` 是 Mission 归属唯一真相；Session 参与 Mission 由 `TaskTurnBinding` 派生。

## 普通消息主链

```text
Surface/WebUI/TUI
      |
      v
Gateway Session admission + durable outbox
      |
      v
Runtime Task Router
  +-- task focus valid ----------> continue selected Root Task
  +-- active Task matches -------> continue active Root Task
  +-- terminal predecessor ------> create successor Root Task
  +-- independent objectives ----> create bounded additional Root Tasks
  +-- otherwise -----------------> create one new Root Task
      |
      v
atomic TaskTurnBinding
      |
      +--> Turn ingress / Execution / Team / Agent / Skill / Tool
      +--> asynchronous Mission Organizer
```

Gateway 负责可靠接入、持久化和调用 Runtime，不自行判断业务 Task。Runtime Task Router 先重放已持久绑定，
再依据显式 focus、路由提示和当前 Task 状态作确定性决定。Task 创建或绑定失败时，Turn 不进入无归属执行。

## Mission 组织

每个 Workspace 有确定性默认 Mission。没有显式 Mission focus 的新 Root Task 可立即执行并进入默认 Mission；
后台 Organizer 只对 Root Task 做异步治理：

```text
KeepDefault
JoinExisting(mission_id, task_ids, evidence)
CreateCluster(deterministic_id, objective, task_ids, evidence)
ProposeConflict(review_required)
```

模型只能提出带证据的建议。Runtime 校验 Workspace、Task kind、人工锁、候选范围、CAS revision 和幂等键。
Delegated Task 继承 Root Task 的 Mission，不独立参与聚类。人工显式指派优先，已有专属 Mission 不被自动合并。

## 权限与恢复

```text
Session permission ceiling
      |
      v
Turn authorization snapshot/revision
      +-- Strategy lease mismatch -> Runtime replan
      +-- Team/Agent ceiling ------> only narrow
      +-- Tool effect -------------> approval profile / grant / receipt
```

权限上限只有 `read-only`、`workspace-write`、`danger-full-access`。审批 profile 和作用域 grant 是独立维度。
运行中变更会发布 permission revision；未完成动作在下一个安全授权点重新评估，已提交副作用凭幂等 receipt
禁止重复执行。没有安全检查点时明确标记仅对下一 Turn 生效。

## 投影与前端

Runtime 生产 canonical activity、relation、evidence 和完整 lineage；Gateway 物化统一 snapshot/delta/live：

```text
Mission
  +-- Root Task
       +-- Execution
            +-- Team
                 +-- Agent

Session --contributes--> Root Task
```

WebUI 的 Chat 显示当前 Task/Mission 和 future routing focus；Mission Control 展示 Root Task、Turn bindings、
显式 Mission 指派、Organizer 决策和 Session contribution。TUI 只显示当前 Session/Task/Mission、路由 revision、
Organizer 状态和可执行控制，不复制复杂图。Surface 不根据文本或旧字段猜关系。

## 持久化边界

- SQLite 与 PostgreSQL 都持久化 Task aggregate、Task-Turn binding、Session routing focus 和 Organizer decision。
- Session outbox 只持有 typed Task route hint，不再持有 Mission membership outbox。
- PostgreSQL 历史迁移不可改写；新迁移负责删除废弃表并保证旧安装可升级。
- Task aggregate 保持有界，不内嵌无限 Turn 列表；详情通过绑定索引读取。

## 不变量

1. 一个 Task 只有一个 Mission 主归属。
2. 一个 Delegated Task 必须有 parent/root lineage，且继承 Root Mission。
3. 一个 Turn 最多一个 primary Task binding。
4. 终态 Task 不被新消息静默重新打开，只能创建 successor。
5. 未通过 Session admission、Task route 和 lineage 校验的执行不能进入主链。
6. Mission、Session、Task 和 Activity 关系只能由后端规范合同产生，前端不得建立第二套关系真相。
