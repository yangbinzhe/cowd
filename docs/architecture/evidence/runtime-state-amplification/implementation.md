# Runtime 状态放大治理实施映射

## V1 Derived State Plane

| 目标 | 实现 |
| --- | --- |
| projector cursor 脱离事件日志 | `RuntimeProjectionCheckpoint`、SQLite/PG mutable checkpoint、CAS revision |
| checkpoint 不触发源订阅 | checkpoint backend 独立于 append/commit signal |
| PG batch read 消除 N+1 | commit、transaction stream、event 通过一次集合查询装配 |
| live latest 合并写 | Hot State 承担高频进度；等待、周期和终态边界持久化 checkpoint |
| restart 恢复 | latest checkpoint 加 checkpoint cursor 之后的 canonical event replay |
| 清理旧派生历史 | Runtime migration 0011-0013 迁移有效 latest 后删除旧 event 和孤儿 commit/stream/head |

三个业务 projector 和 Mission evidence projector 均只写 mutable checkpoint。生产代码
不再产生 `*.projector.checkpoint.v1` 或 `execution.live.snapshot.v1`。

## V2 Canonical Payload Plane

| 目标 | 实现 |
| --- | --- |
| ContextEnvelope 去重 | envelope schema v3 只保存 canonical selection、render manifest 和 artifact 关系 |
| 模型请求精确证据 | Provider transport builder 产生真实 method/endpoint/headers/body/hash |
| 大载荷单份保存 | exact wire request 写 content-addressed Artifact Store |
| Session 可追溯 | `context.provider_request_packed` 只保存 artifact ref、hash、预算和请求身份 |
| artifact 生命周期 | 请求写入后持久 pin；失败不发布伪成功事件 |
| terminal 去内联 | event、outbox、Session materialization 使用 terminal artifact selector/hash/fence |
| reader presence 去历史化 | 独立 TTL projection、revision CAS、TTL/3 合并 heartbeat，不写 Session event |

ContextEnvelope 的完整 `assembled` 只存在于当前运行内存中。持久态以 `selected` 作为
dynamic tail 的唯一事实来源；小对象内联 canonical body，大对象进入 Artifact Store，
列表只读取摘要，详情按需校验并加载 Artifact。

Provider 请求证据不是从高层 request 二次猜测，而是由 OpenAI compatible、Responses、
Anthropic 三条真实 wire builder 在发出请求前生成。凭据不进入证据。

## V3 Single Projection Plane

| 目标 | 实现 |
| --- | --- |
| 唯一 typed identity | Runtime event 持有 session/turn/root execution/activity 绑定 |
| 唯一投影 owner | Runtime 生成 summary/business/technical detail scope |
| 有界历史查询 | Session history index 不含正文；turn/execution detail 显式按身份读取 |
| 投影缓存 | cache key 包含 execution、revision、cursor、detail、authorization、redaction |
| fresh live overlay | durable projection cache 命中后叠加当前 live state |
| Gateway 清洁化 | 删除 Gateway 的 turn/event 重建、legacy stream 合并和重复树构造 |
| 显式失败 | projection 错误返回 typed error，不降级成“没有数据” |

生产代码已无 `turn_projection_from_event_values`、`SessionTurnProjection`、
`turnProjection` 和旧 terminal inline selector。

## V4 Surface Consumption And Closure

| 目标 | 实现 |
| --- | --- |
| 首屏轻量 | metadata、body-free history index 和 transcript tail 并行加载 |
| 详情延迟 | Companion/图未打开时不挂载组件、不 acquire full projection |
| 增量 transcript | durable `next_seq` 游标增量追赶，分页至 terminal，可并发合并 |
| 避免全量 reload | terminal/recovery 只同步 durable tail，不重载完整 Session |
| 一份前端状态 | transcript 活动、详情和图消费 canonical execution projection |
| 精确失效 | API contract 分为 transcript/execution/resources/catalog scope |
| stale response fence | scope revision 拒绝旧请求覆盖新状态，并执行有界重读 |
| presence 续租 | 使用 Gateway 返回 TTL，以 TTL/3 刷新，关闭 Session 后停止 |
| 重复索引清理 | Runtime migration 0014、Session migration 0015 只删除精确同义索引 |

WebUI 的 terminal tail 窗口最多保留最近 1000 条渲染记录，但 durable history 不删除；
向上翻页和按 sequence 查询仍由 Session Store 提供。

## 部署生命周期收口

权威环境验收发现正常 `gateway stop/restart` 与失败启动回滚的进程树语义不一致：
Gateway 虽以独立进程组启动，正常停止却只向父 PID 发信号，超时强杀时可能遗留
auth-broker。`gateway::server` 现在只对已证明属于当前可执行文件的 `gateway run`
进程建立受管目标；若该进程是组长则向整个进程组发送 TERM/KILL，否则只处理目标 PID。
隔离子进程组测试、全 Gateway 测试及安装环境连续两次 restart 共同证明 helper 不再遗留，
同时仍不会根据端口误杀其他 worktree 或 tmux 中的进程。

## 删除清单

- projector checkpoint event producer
- per-event full live snapshot producer
- V504 live fallback
- Gateway legacy Session/runtime stream projection
- Gateway event-value turn projector
- WebUI 第二份 turn projection store
- terminal inline payload selector
- Context/turn report 的重复 summary/metadata
- presence append-only lifecycle history
- 两个与唯一约束/主键完全重复的 PostgreSQL 索引

旧事件名称仅允许出现在一次性迁移与迁移测试中。
