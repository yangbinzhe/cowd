# Runtime 状态放大补偿版实施映射

## P1 PostgreSQL 与 Evolution

| 终态要求 | 实际实现 |
| --- | --- |
| 前缀查询不依赖 collation | Runtime PostgreSQL 使用 `starts_with(stream_id, prefix)` |
| 防止邻近前缀混入 | 真实 PostgreSQL 合同写入目标、相邻和无关 stream 并精确断言 |
| 已推进 checkpoint 的失败可恢复 | Evolution worker 每轮优先执行有界 dead-letter repair |
| lifecycle 不因同 ID 不同 payload 冲突 | signal 已存在时直接复用其 ID 创建缺失 lifecycle |
| 失败证据不删除 | 保留 failed event，以 recovered event 引用 failure/source 关闭未解决状态 |
| 恢复可重放 | source event ID、signal ID 和 recovery idempotency key 均确定化 |

Outcome、Mission Evidence 和 Knowledge Candidate 的 dead letter 不共用 Evolution
恢复语义：前两者的测试合同明确把永久 malformed fact 作为隔离证据保存；Knowledge
Candidate 会在后续轮次重新治理已持久化的 AwaitingApproval/Blocked candidate。补偿版
没有把不同 projector 的永久隔离误改为无限重试。

## P2 Context canonical durability

| 终态要求 | 实际实现 |
| --- | --- |
| 运行能力不下降 | 内存中的 `ContextEnvelope` 继续持有完整 `assembled` |
| 持久态不重复正文 | schema v3 只保存 canonical `selected` 与 `render_manifest` |
| 渲染可复现 | manifest 固定 formatter version、stable head、runtime header，并声明 dynamic source 为 selected |
| 大对象不放大 Session event | 超过 Artifact compact threshold 后写内容寻址 Artifact，event 只保存摘要和引用 |
| 失败不产生悬空引用 | staging pin、event append、durable pin 和 duplicate/failure cleanup 形成完整生命周期 |
| 列表保持轻量 | summary 不读取 Artifact；详情、完整历史和投影按需 hydrate |
| Surface 不退化 | Gateway 与 TUI 同时识别 schema v3，旧历史仍可读取 |

## P3 Gateway、TUI 与文档

| 终态要求 | 实际实现 |
| --- | --- |
| 删除接口必须真实失败 | 未知 `/api` 与 `/api/*` 固定返回 JSON 404 |
| SPA history 仍可用 | 仅非 API 路径进入 WebUI static fallback |
| TUI 不调用旧投影 | attach/smoke 使用 Session endpoint；生产验收使用 execution index + typed projection |
| 文档与实现一致 | README 删除旧 Session projection 路由，旧门禁标记为历史记录 |

## P4 WebUI mutation fence

| 终态要求 | 实际实现 |
| --- | --- |
| write 明确授权归属 | write context 显式携带 authorization Session |
| 精确失效 | execution command 只递增 `session:{id}:execution` scope |
| 防止旧响应覆盖 | read cache key 和安装门禁同时携带 scope revision |
| 不误伤其他能力 | transcript、resources、catalog 和其他 Session 的 in-flight read 不被取消 |
| 唯一消费者接线 | projection registry 将 canonical entry 的 Session 传给 command client |

## P5 发布列车一致性

审计发现 MFG Rust 依赖、Core 产品 source lock 和 Edge WebUI source lock 曾指向不同提交。
补偿版先独立发布并测试 MFG `0.9.646`，再用同一个完整 commit SHA 更新 Core 生成清单、
直接测试消费者和 Edge WebUI lock。运行时仍是编译期 App，不新增动态兼容层。

## 生产扫描

- 旧 `/api/sessions/:id/projection` 仅允许出现在补偿方案的缺口说明中。
- 生产 PostgreSQL 查询不再构造 Unicode 最大字符上界。
- 新 Context schema 不包含 `assembled.dynamic_tail`。
- WebUI 不恢复第二份 turn projection store 或旧 Session projection client。
- Core/Edge 的 MFG source lock 与 Cargo MFG 依赖必须为同一 immutable revision。
