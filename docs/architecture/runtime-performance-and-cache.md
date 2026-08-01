# Runtime Performance and Cache

状态：V626 已实现单机运行时快路径、Provider 分层准入、PostgreSQL 工作负载隔离和有界系统缓存。

本文描述当前代码的真实执行边界。缓存只保存可恢复的热状态或派生数据；持久事件、Session
历史、审批、副作用结果和业务事实仍由选定存储后端负责。

## 1. 执行原则

```text
请求
  -> Gateway 鉴权与路由
  -> Runtime 内存态调度/读取
  -> Provider/Tool/MCP 执行
  -> 关键事件同步提交
  -> 派生证据、统计和索引有界异步物化
  -> SQLite/PostgreSQL 持久账本
```

- 活动 Session、执行图、输入队列和运行状态以内存投影作为读取快路径。
- 关键用户输入、审批、外部副作用和终态在确认成功前必须形成持久证据。
- 派生统计和可重建索引允许合并或异步写入；队列饱和必须计数并进入健康状态，不能静默丢失。
- Durable Store 是恢复权威，不参与每一次热状态查询；进程重启时从账本恢复内存投影。
- 各缓存由领域 owner 管理容量和失效，不设置跨领域全局锁，也不把缓存变成第二套业务真相。

## 2. Provider 容量与重试

Runtime 在一次请求准入中同时检查进程、账户、模型和估算 token 压力。DeepSeek 内置起始值为：

| 模型类别 | minimum | target | maximum | interactive reserve |
|---|---:|---:|---:|---:|
| Pro | 4 | 32 | 128 | 8 |
| Flash | 8 | 64 | 256 | 16 |

显式 `runtime.resources.provider` 配置优先。准入值是单机保护与调度起点，不是 Provider
合同保证；Runtime 会根据排队、吞吐、错误和 `Retry-After` 反馈收敛。大上下文请求按估算
token 压力占用更多容量，而不是和短请求等价计数。

Runtime Provider client 关闭传输层自动重试，由 Runtime 统一处理一次有类型的瞬态恢复：

1. 保存失败分类和已提交证据；
2. 在等待前释放资源租约；
3. 遵守 `Retry-After` 或治理后的退避；
4. 重新准入后再请求。

独立使用的 `provider` client 保留自身默认重试。两条路径不能叠加重试，否则会放大流量和
延迟。HTTP 连接模板按 host 有界复用，但每次请求的身份、headers 和 trace 都重新构造。

## 3. PostgreSQL PoolSet

进程只创建一个 `PostgresPoolSet`，默认总预算 48 个连接：

| lane | 默认连接 | 用途 |
|---|---:|---|
| `critical` | 16 | 用户输入、审批、副作用、终态、混合事务 |
| `online_read` | 24 | Session 历史、召回和交互式查询 |
| `background` | 8 | 治理、索引、导入导出和全量扫描 |

Gateway 启动时读取 PostgreSQL `max_connections`，扣除 server reserve 后校准三个 lane。
每次 checkout 都显式设置或重置 `search_path`，防止连接复用造成 App schema 泄漏。Repository
必须选择 workload class；旧的通用 `checkout_runtime` 已删除，禁止通过 SQL 文本猜测 lane。

Session 精确历史范围在 PostgreSQL 中使用一次 `unnest` 查询，在 SQLite 中使用一次组合查询。
首次未命中的精确投影最多访问一次持久层；同一投影的重复命中直接复用缓存。该保证不等于
任意历史查询永远零数据库访问。

## 4. Runtime Hot State

`RuntimeHotStatePlane` 统一承载活动执行所需的 Session、graph、residency 和 materializer
状态。默认使用可用内存的 60%，并支持 `max_bytes` 绝对上限、高低水位和保留比例：

```yaml
runtime:
  hot_state:
    memory:
      ratio: "0.60"
      max_bytes: null
      reserve_ratio: "0.20"
      high_watermark: "0.90"
      low_watermark: "0.75"
```

达到高水位时只逐出可从持久账本恢复的终态或空闲投影，直到低水位；不能逐出关键输入或未提交
执行状态。执行图快照使用共享不可变 `Arc`，容量估算按字段计算，不通过完整 JSON 序列化估算。

## 5. Session、Memory 与 Reality

- Conversation Runtime 持有活动对话 transcript。
- Session 热状态保存 manifest、cards 和精确历史投影。
- Memory 召回统计采用合并写；`selected` 只表示进入候选，不等同于已验证或已固化。
- Fact 与 Matrix 召回可以并发执行，再由 Reality 上下文进行统一选择。
- 上下文压缩、长期记忆和事实存储仍保留原始证据 selector，摘要不覆盖原文。

热状态失效只影响性能，不影响恢复能力。Session 关闭、空闲卸载或内存压力逐出后，可从 durable
event/history 恢复。

## 6. Skill 与 Tool 缓存

Skill 遵循简单的渐进披露：

1. 所有轻量 catalog/profile 元数据常驻内存；
2. 模型选中的完整 `SKILL.md` 按需加载；
3. 已加载正文通过 singleflight 去重并进入 32 MiB 字节 LRU；
4. 附属脚本和资源只保存 locator，调用时读取，不缓存整个 Skill 包。

Skill reload 生成新的不可变 generation；正在执行的 Turn 继续持有旧 generation，新的 Turn
只看到新 generation。未选择 Skill 不读取正文，不维护持久热度或复杂 pin 规则。

工具结果缓存只接受明确的幂等读策略，默认总量 64 MiB、单项 2 MiB、TTL 5 分钟，并按 workspace、
权限和工具参数作用域失效。写工具、审批工具和外部副作用不能通过结果缓存跳过执行。

## 7. MCP 生命周期

Gateway service 是 MCP generation 的唯一 owner。每个 MCP server 有独立 worker：

- 同一 server 内串行保护协议和 stdio 生命周期；
- 不同 server 可并行；
- list/read resource 和 tool call 使用同一 generation；
- ToolHost 持有 generation lease，reload 后旧 worker 在最后一个 lease 释放后关闭；
- service drop 会关闭全部 worker，不保留第二个全局 Runtime MCP 状态。

## 8. 健康与验证

运行健康至少应观察：

- Provider 各层租约、排队、错误分类、`Retry-After` 与自适应目标；
- PostgreSQL 三个 lane 的 active/idle/checkout timeout；
- Hot State resident bytes、水位、逐出和 materializer 队列；
- evidence writer 的队列深度、合并、drop 和 shutdown drain；
- Skill/Tool cache 的条目、字节、命中、失效；
- MCP generation、worker 数和关闭状态。

发布门禁包括 workspace 全目标编译、Provider/Runtime/Gateway/Session/Storage/Tools 测试，以及真实
PostgreSQL lane 隔离、`search_path` 重置和 Session 单查询投影测试。详细结果记录在版本计划的
实施证据文档中。
