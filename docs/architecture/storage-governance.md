# Storage Governance

状态：V581 已实施 process-wide selected backend、全域 SQLite→PostgreSQL 迁移证据与原子切换。

Cowd 把业务存储放在稳定 port 之后，并由 Gateway 启动时唯一的
`SelectedStorageTopology` 一次性选择 SQLite 或 PostgreSQL。业务 route、service、Runtime
turn 与 App 都不得在请求期间自行判断后端或重新打开数据库。

## Principles

- Runtime entry surfaces do not open concrete stores directly.
- Gateway routes call Gateway services；service 只持有已选择的 port。
- 一个进程只有一个 backend owner 和一个有总预算约束的 PostgreSQL `PoolSet`；关键写、
  在线读和后台任务使用相互隔离的连接池。
- Matrix facts, Memory records, Session state, Approval state, and tool
  execution evidence must remain addressable through stable service contracts.
- PostgreSQL 启动必须通过 active cutover manifest；secret、目标身份、二进制版本、工作区、
  App source lock、启用 App 集合或逐域证据任一不一致即拒绝启动，不回退 SQLite。

## Selected domains

| domain | owner | notes |
|---|---|---|
| session | `session` | unified session metadata and event log |
| memory / knowledge | `memory` | recall, layers, packets, knowledge fabric |
| runtime event / task | `runtime` / `gateway::TaskKernel` | execution evidence and durable task state |
| fact / growth | `fact-kernel` | facts and growth ledger |
| matrix | `matrix-repository` | structured facts, entities, relations, evidence |
| approval | `approval` | approval history and decisions |
| surface message | `surface` | inbox/outbox/delivery evidence |
| connector directory | `connector` | external resource directory |
| artifact | `runtime` | Resource、Tool raw 与 Evidence 的统一 CAS、授权读取、配额和 GC |
| App relational storage | each App bundle | App-owned schema, snapshot and migration hook |

## Runtime configuration

SQLite remains the default. PostgreSQL configuration contains only a logical identity and secret
reference; the resolved URL never enters config projection, health or cutover evidence.

```yaml
storage:
  backend: postgres
  sessionExecution:
    workers: 8
    queueCapacity: 64
  artifacts:
    compactThresholdBytes: 262144
    maxObjectBytes: 536870912
    totalQuotaBytes: 21474836480
    gcHighWaterBytes: 19327352832
    gcLowWaterBytes: 17179869184
    orphanGraceMs: 86400000
  postgres:
    logicalIdentity: cowd-primary
    secretRef: env:COWD_POSTGRES_URL
    maxConnections: 48
    serverReserve: 8
    critical:
      maxConnections: 16
      minIdleConnections: 3
      checkoutTimeoutMs: 250
    onlineRead:
      maxConnections: 24
      minIdleConnections: 4
      checkoutTimeoutMs: 500
    background:
      maxConnections: 8
      minIdleConnections: 2
      checkoutTimeoutMs: 2000
```

`maxConnections` 是 Cowd 进程可使用的总预算，不是每个池的预算。三个 lane 的
`maxConnections` 可以全部省略，由系统按 `16:24:8` 的比例分配；也可以全部显式配置，
但三者之和必须等于总预算。启动时系统读取 PostgreSQL `max_connections`，扣除
`serverReserve` 后再次校准，保证数据库仍有管理和其他客户端所需的连接。不得只配置部分
lane，也不得继续使用旧的根级 `minIdleConnections` 或 `checkoutTimeoutMs`。

Artifact 元数据和小对象随 selected backend 使用 SQLite/PostgreSQL；大对象只通过
`StorageDomainId::Blobs` 的选定目录访问。公开 Resource/Evidence DTO 仅返回
`artifact://` selector、hash、大小、媒体类型和可见域，不返回宿主路径。Gateway 启动时会
幂等迁移旧 `storage/resources/objects` 与 Session 内联 raw evidence，并在
`~/.cowd/migrations/artifact-v1-report.json` 留下 hash 校验、游标和完成状态；迁移未完成
会阻止新旧语义混用。

`secretRef` currently accepts the strict `env:VARIABLE` form. Environment variables resolve
credentials at the process boundary; they do not replace the configuration file as the topology
truth. A PostgreSQL App lease receives a validated, schema-scoped executor. Every pool checkout
sets its `search_path` explicitly, so an App schema cannot leak into core or another App through a
reused connection.

`PostgresExecutor` 只公开运行时安全的连接包装，不把同步驱动连接直接交给 async 调用方。
Repository 方法必须在 checkout 前显式选择 `critical`、`online_read` 或 `background`；
禁止根据 SQL 文本猜测工作负载，同一事务也不能跨 lane。关键输入、审批、副作用和终态写入
使用 `critical`，交互式历史和召回使用 `online_read`，治理、索引、导入导出和全量扫描使用
`background`。后台池耗尽不会占用关键写入连接。

同步驱动执行规则：
多线程 Tokio worker 使用 `block_in_place`，current-thread runtime 使用有界 OS 线程桥接。
生产 Gateway 也直接从 `SelectedStorageTopology` 组装 service，不先构造 SQLite baseline 再覆盖，
因此选择 PostgreSQL 后不会暗中创建第二套业务 SQLite executor。通用 App 的健康、路由和能力
由 `AppRegistry` 验证，核心 service readiness 不硬编码任何具体 App 名称。

## Offline cutover

Gateway 必须停止，且四步必须使用同一份配置、工作区和已编译产品：

```bash
cowd storage plan
cowd storage migrate
cowd storage verify
cowd storage cutover
```

- `plan` 只输出经过脱敏的源 inventory、目标逻辑身份、逐域和启用 App 清单。
- `migrate` 在 maintenance barrier 下并行迁移 11 个核心/应用域；每个域重新读取源和目标，
  只在 canonical digest 相等时写 staging evidence。
- `verify` 校验完整 evidence envelope，并通过生产 composition code 重开所有 PostgreSQL
  adapter 和启用 App storage。
- `cutover` 只把已验证 manifest 原子发布为 active；不修改配置、不双写、不自动回退。

active manifest 绑定 Cowd 版本、工作区、目标 logical identity、目标 secret reference、全部
App immutable source lock 和当前 enabled App 集合。更换数据库、App revision 或启停集合后，
旧 manifest 不可复用，必须显式重新迁移和验证。

## App ownership

Cowd 只编排通用 `StaticAppProduct` storage migration hook，不认识 App 的表或 DTO。拥有
relational requirement 的 App 必须在自己的 bundle 中 export/import/re-read canonical snapshot，
并返回 source/target digest evidence；enabled App 缺 hook 或 evidence 时 cutover fail closed。
PostgreSQL 中每个 App 使用由宿主分配的独立 schema，MFG 只是首个真实实现。

## Evolution boundary

新增 SQL 后端必须实现现有 domain port 与 canonical migration contract，不得让 route、TUI、
WebUI 或 App 业务层出现 backend 分支。文件型 vector index、blob 与 definitions 可以作为明确的
可重建 artifact 存在，但不能成为 PostgreSQL 模式下隐式写入的第二份业务真相。
