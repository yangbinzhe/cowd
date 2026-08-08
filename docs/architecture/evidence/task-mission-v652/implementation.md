# v0.9.652 Session、Task、Mission 终态治理实施证据

## 1. 权威关系

```text
Session -> Turn -> TaskTurnBinding -> Root Task -> Mission
                               \-> Delegated Task -> Team/Agent/Tool Activity
```

- Session 只拥有对话、Turn、输入队列、恢复和路由 focus。
- Runtime Task service 是 Task aggregate、Turn binding、Mission assignment 的唯一写入者。
- Mission 只直接聚合 Root Task；Session 参与 Mission 的关系由 TaskTurnBinding 派生。
- ExecutionGraph 和 canonical activity 在准入时固化 Session、Turn、Task、Root Task、Mission lineage。
- Gateway 和 Surface 只能通过类型化 application service 修改 focus/assignment，不能直接写执行图或关系边。

## 2. A-G 实施闭环

| 包 | 代码落点 | 生产/消费闭环 |
|---|---|---|
| A 合同 | `harness-contract::{task,turn,mission,policy,projection,execution_graph}` | Root/Delegated、origin、binding、route、assignment、lineage 和三档权限均为严格类型 |
| B Task 领域 | `runtime::task::{aggregate,store,router,lifecycle,runtime_port}` | Session ingress 路由创建/续接 Task；Team/Agent 创建 Delegated Task；SQLite/PG 等价持久化 |
| C Mission | `runtime::mission::organizer`、Mission runtime/control、Gateway Task/Mission service | 显式 assignment 走 preview/CAS；后台 organizer 消费 Root Task 决策；图和计数从 Task/binding 派生 |
| D 权限 | Runtime policy、Tool executor、Gateway runtime control | 只接受 `read-only/workspace-write/danger-full-access`；权限 revision、审批和恢复保持独立 |
| E Activity | Runtime activity producer/reducer、Gateway projection | 所有业务活动使用稳定 identity、parent、lineage、generation/turn fence；历史/live 使用同一合同 |
| F Surface | generated OpenAPI、WebUI Chat/Mission/Runtime、TUI gateway client/control store | Surface 消费 typed focus、Task detail、organizer 和 canonical graph，不重建关系真相 |
| G 清理 | Session migration 17、API/README/配置/脚本 | Session Mission outbox 和任意 Mission relation 写入已删除；旧权限值被严格拒绝 |

## 3. 关键调用链

```text
POST Session message
  -> SessionService durable ingress + typed TaskRouteHint
  -> session runtime worker claim/fence
  -> TaskRouter (existing Turn binding first)
  -> Task aggregate + TaskTurnBinding atomic commit
  -> TurnIngressRef.primary_task_id + task_bindings
  -> ExecutionGraphLineage admission
  -> Team/Agent/Tool canonical activities
  -> terminal/outcome/evidence
  -> Session/Task/Mission materialized projections
  -> WebUI/TUI/Edge
```

后台 Mission 组织器独立于前台执行：Root Task 变化只写 durable decision，Gateway 托管的 worker 有界 claim、
调用 Provider、校验严格 JSON，再通过 Task batch CAS 提交。显式锁定、Delegated/System Task 不进入自动组织。

## 4. 删除而非兼容

- 删除 `runtime::mission::task`，不存在兼容 re-export。
- 删除 Session Mission membership outbox、worker、schema 和 recovery 位。
- 删除 Mission aggregate 五类成员 refs 及任意 link/unlink relation mutation。
- 删除 Surface/Gateway 写 Task execution graph 的入口；该图由 Runtime 独占。
- 删除 PermissionMode 旧枚举和旧配置值回退；解析器只保留明确错误诊断。

