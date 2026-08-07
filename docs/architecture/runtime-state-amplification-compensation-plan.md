# Runtime 状态放大治理补偿版终态方案

> 版本目标：在 `runtime-state-amplification-terminal-plan.md` 已实现能力之上，
> 修复真实 PostgreSQL、Context 持久化、Gateway 路由、TUI 验收脚本与 WebUI
> 缓存失效链中未闭环的问题。本方案不引入兼容层，不恢复已删除的旧投影接口。

## 1. 审计结论

上一版通过了模块测试，但没有完整覆盖“真实 PostgreSQL 排序规则 -> 演化投影 ->
Gateway API -> Surface 验收”的生产链，因此出现了测试成功、能力实际失效的偏差。
本次确认的终态缺口如下。

| 编号 | 代码事实 | 业务影响 | 终态决策 |
| --- | --- | --- | --- |
| C1 | PostgreSQL 使用 `prefix <= stream_id < prefix + U+10FFFF` | 非 `C` collation 下演化信号无法按前缀回读 | 改为 collation 无关的 `starts_with`，增加真实 PG 合同测试 |
| C2 | projector 失败后写 dead letter 并推进 checkpoint | 已记录 signal 永久缺少 diagnosis/mission/proposal | 增加有界、幂等的死信修复和 recovered 事实 |
| C3 | `ContextEnvelope` 同时持久化 `selected` 和派生 `dynamic_tail` | 重复写入；大上下文不能进入 blob tier | 内存态保留 assembled；持久态只保存 canonical selection 和 render manifest，大包进入 Artifact |
| C4 | TUI 场景脚本仍调用已删除的 `/api/sessions/:id/projection` | 验收脚本不再验证真实接口 | 改用 Session execution index 和 Runtime typed projection |
| C5 | 未知 `/api/*` 被 SPA fallback 返回 `200 text/html` | 删除接口和拼错路由不能尽早失败 | API namespace 固定返回结构化 JSON 404，SPA fallback 仅服务浏览器路由 |
| C6 | WebUI execution command 不携带 Session scope | 旧的 in-flight projection 可覆盖命令后的状态 | endpoint 显式传入 authorization Session 和 execution invalidation scope |
| C7 | README 与最终证据仍宣称旧接口/全部门禁通过 | 文档和代码事实冲突 | 删除旧接口说明，最终证据只在验证后更新 |
| C8 | MFG Rust、产品 source lock 与 WebUI lock 指向不同提交 | 同一发布物可能组合不同修订的 App 后端和前端 | MFG 先独立发布，再把 Core/Edge 全部 immutable lock 统一到同一提交 |

## 2. 能力守恒与边界

### 2.1 保留

- Runtime Hot State、mutable projection checkpoint、typed execution projection。
- 事件日志、dead letter 与 recovered marker；历史失败证据不删除。
- 内存中的完整 `ContextEnvelope.assembled`，供模型请求编译和实时 UI 使用。
- 完整 provider wire request、terminal transcript、raw tool output 的 Artifact 证据。
- TUI、WebUI 对 Context budget、diagnostics、selected/omitted 和 hash 的查看能力。

### 2.2 删除

- PostgreSQL 基于最大 Unicode 字符构造前缀范围的实现。
- 新 Context event 中重复持久化的 `assembled.dynamic_tail`。
- 所有 Session legacy projection HTTP 调用和 README 描述。
- API namespace 进入 WebUI SPA fallback 的路径。
- execution command 依赖 URL/body 猜测 Session scope 的行为。

### 2.3 不做

- 不恢复 `/api/sessions/:id/projection`。
- 不删除或改写历史 dead letter；以 recovered event 关闭未解决状态。
- 不把运行中热状态改回数据库查询。
- 不新增第二套 projection、Context store 或 WebUI store。

## 3. 实施包

### P1 PostgreSQL 与演化投影修复

**文件**

- `crates/runtime-postgres/src/lib.rs`
- `crates/runtime/src/evolution/projector.rs`

**动作**

1. `list_scope_stream_prefix_page_asc` 使用 `starts_with(stream_id, prefix)`。
2. 在隔离 PostgreSQL 合同中写入相邻前缀和非前缀 stream，证明只返回目标集合。
3. projector 每轮先处理有界数量的 unresolved dead letter。
4. 根据 dead letter 的 `source_cursor/source_event_id` 找回原始 source event。
5. 复用同一 projection core；成功后写
   `evolution.signal.projector.recovered.v1`，并引用 failure/source event。
6. `dead_letter_count` 只统计没有 recovered marker 的失败。
7. 恢复必须幂等：重复运行不新增 lifecycle、不新增 recovered marker。

**验收**

- 非 `C` collation PostgreSQL 前缀合同通过。
- 现有失败记录自动产生完整 lifecycle。
- signal、diagnosis、mission、proposal 均可通过 API 查询。
- health unresolved dead letter 为零，历史 failure/recovered 数量可审计。

### P2 Context canonical persistence

**文件**

- `crates/runtime/src/context/context_runtime.rs`
- `crates/runtime/src/conversation/conversation.rs`
- `crates/gateway/src/services/context_service/history.rs`
- `crates/tui/src/components/context_panel.rs`
- 对应测试

**动作**

1. 定义 typed `PersistedContextEnvelope` 和 `ContextRenderManifest`。
2. canonical body 保存 identity/profile/intent/selected/omitted/source registry/
   epoch report/budget/diagnostics/created_at。
3. render manifest 保存 formatter version、stable head、runtime header；dynamic tail
   明确由 `selected` 派生，不重复保存。
4. canonical body 超过 Artifact compact threshold 时，先写带 Session visibility 的
   Artifact，再写只含摘要和 artifact ref 的 Context event。
5. event 成功后永久 pin；重复或失败时清理 staging pin 和未引用 artifact。
6. Context 专用历史/详情 API 按需解析 artifact；列表 summary 不读取大对象。
7. TUI 对新 schema 使用 selected 数量和 render manifest，同时仍可读取历史 schema。

**验收**

- 新 Context event 中没有 `assembled.dynamic_tail`。
- 小包可直接读取，大包按需 hydrate，hash/缺失/无权限失败显式返回。
- 内存 prompt、provider request 与优化前一致。
- TUI/Gateway 的 Context 关键指标不退化。

### P3 路由与验收链收口

**文件**

- `crates/gateway/src/runtime_host/mod.rs`
- `scripts/scenarios/tui-{smoke,daemon-attach,production-acceptance}.sh`
- `README.md`
- API 文档与测试

**动作**

1. fallback 在静态资源解析前识别 `/api` 和 `/api/`，返回
   `404 application/json`，错误码为 `api_route_not_found`。
2. 浏览器非 API 路径继续支持 SPA index fallback。
3. TUI attach 使用 canonical Session endpoint。
4. DSML 验收先读取 Session execution index，再读取 full typed execution projection，
   按 tool activity 验证 tool call identity/status。
5. 删除无消费者的旧 projection 抓取和 README 旧路由描述。

**验收**

- 未知 API 绝不返回 HTML。
- 全仓旧 Session projection URL 扫描为零。
- TUI 场景脚本 shell 语法、mock 和真实链路通过。

### P4 WebUI mutation fence

**文件**

- `cowd-edge/surfaces/webui/src/api/client.ts`
- `cowd-edge/surfaces/webui/src/stores/projectionRegistry.ts`
- 对应测试与构建产物

**动作**

1. write API 接受 endpoint 声明的 authorization Session 与 invalidation scopes。
2. execution command 显式传入 registry 已绑定的 Session。
3. 命令成功立即递增 `session:{id}:execution` scope revision，取消相关 in-flight read。
4. command 后 reload 不得 join 命令前的旧请求，stale response 不得安装。

**验收**

- 单测证明 command 只失效目标 Session execution，不清空无关 Session/catalog。
- 延迟旧响应不能覆盖新 revision。
- WebUI 全量测试和 build 通过。

### P5 发布列车与 App source lock 收口

**文件**

- `cowd-app-mfg/{Cargo.toml,Cargo.lock,README.md}`
- `apps/mfg/source.lock.toml`
- `crates/product-apps/{Cargo.toml,src/generated.rs}`
- `crates/gateway/Cargo.toml`
- `surfaces/webui/apps.generated.ts`
- `cowd-edge/surfaces/webui/apps/mfg/source.lock.json`

**动作**

1. MFG 独立完成 `0.9.646` 全量测试、commit、annotated tag 与 push。
2. Core 的 source lock 作为唯一修订输入，通过 `cargo xtask apps sync --locked`
   生成 Rust/WebUI 产品清单和直接测试消费者。
3. Edge WebUI lock 指向同一 MFG commit；`apps:sync` 必须解析出精确提交。
4. Core/Edge/MFG workspace 版本统一为 `0.9.646`。

**验收**

- `cargo xtask apps verify --locked` 通过。
- Core Cargo.lock 中 MFG 五个 package 均为 `0.9.646` 且来自同一 commit。
- Edge apps sync 报告同一 commit。
- 三仓 tag 和远端分支均指向各自已验证提交。

## 4. 失败、恢复与资源表

| 链路 | 失败状态 | 恢复 | 幂等键/栅栏 |
| --- | --- | --- | --- |
| evolution projection | failed event | bounded repair -> recovered event | source event id |
| Context artifact write | staging artifact | event 失败即 unpin/delete | envelope id + content hash |
| Context event append | duplicate | 删除本次未引用 artifact | envelope id |
| Context artifact read | missing/hash/auth error | typed API error，不伪造空 envelope | selector + Session scope |
| WebUI execution command | HTTP/receipt failure | 不失效成功缓存；显示原错误 | command id |
| command 后 projection read | stale response | scope revision 拒绝并重读 | Session execution revision |

## 5. 最终门禁

1. `cargo fmt --check`、workspace `cargo check --all-targets`。
2. Runtime、runtime-postgres、Gateway、TUI、Session 定向及全量测试。
3. WebUI 全量测试、type/build、静态产物同步。
4. 隔离 PostgreSQL 执行被忽略的 backend 合同。
5. 正式 Release 替换后检查 health/ready、evolution API、unknown API 404。
6. 对生产历史 dead letter 执行自动修复，验证 unresolved 为零且证据未删除。
7. 全仓扫描旧路由、prefix-end、Context dynamic-tail 持久化生产符号为零。
8. Core/Edge/MFG 版本一致，所有 MFG source lock 指向同一提交。
9. 工作树只包含本版本改动；中文 commit/tag 推送成功。
