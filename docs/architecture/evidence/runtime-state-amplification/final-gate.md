# Runtime 状态放大治理最终门禁

> 历史门禁记录：本文件对应 `0.9.645` 首轮实现。真实 PostgreSQL collation、
> Evolution 死信恢复、Context canonical durability、未知 API fallback 和 WebUI
> mutation fence 的补偿结论，以
> `../runtime-state-amplification-compensation/final-gate.md` 为最终依据。

## 逐项判定

| 门禁 | 证据 | 判定 |
| --- | --- | --- |
| 不删除业务事实 | 副本迁移删除数精确等于两类派生 event 数 | 通过 |
| projector event producer 为零 | 生产扫描仅剩 migration/test 引用 | 通过 |
| live snapshot producer 为零 | 生产扫描仅剩 migration/test 引用 | 通过 |
| mutable state 有 revision/fence | SQLite/PG CAS 与 stale writer 测试 | 通过 |
| PG batch 查询常数化 | Runtime PG contract 和实现审查 | 通过 |
| exact provider request 可追溯 | wire builder -> artifact -> Session ref/pin 测试 | 通过 |
| terminal exactly-once 不退化 | Runtime/Gateway/Session terminal replay 测试 | 通过 |
| presence 不污染历史 | SQLite/PG history-empty 测试 | 通过 |
| 单一 projection owner | legacy helper/类型生产引用为零 | 通过 |
| Gateway 不吞投影错误 | typed error 合同测试 | 通过 |
| WebUI 首屏不拉 full detail | app/store/component 合同测试 | 通过 |
| terminal 不全量重载 | sequence tail/coalescing 测试 | 通过 |
| scoped invalidation | API race、scope revision 测试 | 通过 |
| 重复索引归零 | 权威数据库升级后两个目标索引均不存在 | 通过 |
| Core/Edge 可编译 | Workspace check、Vite build | 通过 |
| 桌面/移动布局 | 生产构建视觉门禁 | 通过 |
| 权威环境迁移 | 正式部署入口离线升级并启动新 Gateway | 通过 |
| 安装资源一致 | WebUI dist 与 HTTP 返回 SHA-256 一致 | 通过 |
| 单实例进程 | 隔离进程组测试及连续两次安装环境 restart | 通过 |

## 反向证据抽样

```text
WebUI terminal
  -> transcript next_seq
  -> Session durable message
  -> runtime terminal artifact ref
  -> terminal fence / outbox
  -> execution + turn + input generation
  -> admitted user message
```

```text
WebUI execution node
  -> execution projection revision/detail scope
  -> Runtime projection cache key
  -> canonical activity identity
  -> graph transition / tool receipt / approval
  -> execution root / Session / turn
```

```text
Provider request evidence
  -> Session context.provider_request_packed
  -> scoped artifact selector + sha256
  -> exact secret-free wire request
  -> provider attempt/request identity
  -> ContextEnvelope budget and selected input
```

## 历史结论

V1-V4 的代码、迁移、前后端接线、权威数据库升级、Release 部署和测试证据均已闭环。
没有保留第二套投影、旧派生事件生产路径或 inline 大载荷兼容路径；安装环境中也没有
旧 Gateway 或旧 auth-broker 进程。该结论已由补偿审计修正，不能脱离补偿门禁继续作为
当前版本的最终结论。
