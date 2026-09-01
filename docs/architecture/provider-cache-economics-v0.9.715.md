# Provider Cache Economics v0.9.715

## 0. 决策、状态与范围

本文件是 `v0.9.715` 对 Provider 请求缓存、成本归因与多 Agent 共享前缀的**唯一实施权威**。它取代
`provider-prompt-cache-hardening-v0.9.714.md` 中仍未兑现的“所有深度协同负载全程 >=90%”承诺；后者保留为
历史设计与 `v0.9.714` 已交付功能的证据，不得再被当作本版本验收依据。

本版本只改变 Runtime 如何把已获授权的业务真相编译成模型输入、如何协调同一安全域中的缓存预热，以及如何计量；
不改变 CollaborationProgram/Execution Graph 的终态所有权，不增加模型工具、资源、数据或跨 Team 的读取权限。

本次冻结的生产证据来自 `cache-canary-v0914-final3-20260902-014000`：11 个 Provider leaf attempt
返回原始 usage，`cache_read_input_tokens=101,888`、`input_miss_tokens=61,992`、输出 `26,960`，故实际
输入缓存命中为 **62.17%**。这是 Provider 原始 usage、保留的 exact wire 和本地最长公共前缀三方一致的结果，
不是展示层统计错误。不得清理、伪造、丢弃低命中样本，亦不得为了提高比例注入无业务价值的重复文本。

### 0.1 目标重新定义

缓存只可能减少已出现且从第 0 token 开始逐字节相同的输入；首次真实信息、模型输出和新的工具/evidence
结果不能被缓存消除。因此目标按可复用性资格分流：

| 工作负载 | 可承诺目标 | 不允许的做法 |
| --- | --- | --- |
| 高复用同域 cohort（多个 Agent 共享同一 Package、工具 schema 和模型，且离线 wire 预测 `>=92%`） | 真实 Provider cold-inclusive 输入命中 `>=90%`，warm `>=95%` | 用无意义历史或伪共享内容填充输入 |
| 短生命周期、低复用 cohort（少量角色、各自目标或工具证据立即分叉） | 最小真实输入成本；保持不同 cohort 并行，不将 90% 作为失败条件 | 为追逐比例串行任务、添加大量共享噪声 |
| 任意工作负载 | usage 可审计、私有数据不泄漏、功能/质量/并发不退化 | 将上下文提升为 system 权威、跨 session 或 Team 共用缓存 identity |

这不是降低标准：把“90%”施加给三个只运行 3–5 次且首轮目标私有的 Agent，在数学上要求用数十 KB 的
无价值公共前缀掩盖真实 miss，会同时增加绝对费用和注意力干扰。Codex/Pi 类高命中通常描述的是长期、同一
缓存域的稳态会话；本框架必须明确区分该场景和一次性异构协作。

## 1. 已审计根因

### 1.1 请求布局而非 Provider 故障

当前 `InProcessAgentWorker` 调用 `submit_turn(&packet.objective, ...)`，该私有 objective 是第一条
history user message。随后 `ContextRuntime::agent_context_view` 将 child contract 放在 inherited context 之前，
`ConversationRuntime::provider_prompt_from_envelope` 又把 runtime header 先于所有 context packet 放入
append-only 历史之后的 contextual tail。于是两个并行 role 在约 5 KB 的 shared system/program dossier 后就分叉；
两个首请求仅有约 `4,973` byte（约 21%）公共前缀。其后的每个 Agent 仅有 3–5 次请求，无法摊薄首次私有信息与
持续新增的 assistant/tool evidence。

同一 Agent 内 append-only 已有效：实测 exact extension 在若干后续请求得到 67.32%、69.93%、79.69% 命中；
root 则有 77.93%、62.01%、81.43%。因此“缓存没有生效”的根因判定不成立，正确判定是**有真实命中，
但可共享、可摊薄的输入不足**。

### 1.2 次要但真实的框架问题

| 事实 | 根因 | 影响 | 修复归属 |
| --- | --- | --- | --- |
| 共享 data 在 private objective 后出现 | Provider input 没有类型化 immutable user-prefix | sibling 首轮无法复用有价值共同资料 | Runtime prompt compiler |
| runtime header 与 child contract 混入一般 contextual tail | 业务 context、执行控制、共享资格未被类型化区分 | 无法解释 LCP 损失，也难以安全重排 | Context runtime / prompt assembly |
| `team_prompt_cache` key 是 `binding_digest:team_binding_digest`，值只取决于 Team digest/instructions | 本地 fragment cache 被不必要的 Agent binding 切分 | CPU/分配浪费，掩盖真实复用模型 | In-process Agent worker |
| cache 的 runtime 事件缺少按段字节账本和 SLO applicability | 只有总体 LCP 与 usage，没有“哪一类真实信息造成 miss”的可审计归因 | 容易把短任务低命中误判成框架退化 | Provider evidence |

工具 schema 不进入本版本的“强制合并”范围：不同 role 的工具集合若被强行并集，会扩大模型可执行面并降低质量/安全。
它应被计量为 cohort 分裂原因；只有已经完全一致的 schema 才天然同 cohort。

## 2. 不可变设计契约

### 2.1 类型与边界

新增 `CohortPromptPackage`（名称、字段可在代码评审时微调，但以下语义不可变）：

| 字段 | 含义 | 安全条件 |
| --- | --- | --- |
| `schema_version`, `package_id`, `digest` | 可重建且规范化的不可变 shared package 身份 | digest 覆盖规范化内容、scope、来源和渲染版本 |
| `scope` | `Session` 或精确 `TeamBinding(team_id,binding_digest)` | 不允许跨 session；Team package 不允许跨 team/binding |
| `packets` | Team admission 已传给每个 role 的、按确定顺序渲染的 immutable user-role context packets | 只允许本次 Team request 的 objective、acceptance、upstream evidence/artifact/result context；仅同 binding 的 Team 可使用 |
| `source_refs`, `evidence_refs` | 可追溯来源，不是额外授权 | 只读；校验失败即拒绝包而非回退为“全量共享” |
| `rendering_revision` | wire 语义版本 | 任一变化更换 digest，旧历史不混接 |

它是**已被 Team admission 授予每个 role、有任务价值的公共 data**，不是把 role instruction、private task、
clock、agent id、team board 或 raw memory 移到 system。`TeamInstantiationRequest` 已经将 objective、acceptance 与
upstream evidence/artifact/result context 交给每个 role；本版本只将这部分相同、冻结的事实从重复的 role objective
中显式抽出，绝不新读取可变 context。所有 package 内容继续以 provider `user` message 身份发送；Policy/system
权威仍只属于 `PromptAssembly.trusted_system`。

### 2.2 最终 wire 顺序

对带 package 的新 Agent child session，模型可见顺序必须为：

```text
[stable shared system]
-> [CohortPromptPackage shared user messages]
-> [Runtime-attested private role brief]
-> [private turn starter / prior private conversation history]
-> [private runtime-control header and private context]
-> [new append-only contextual evidence]
```

role-specific system suffix 若留在 shared package 前，严格前缀仍会在 suffix 处断开，因此不是可接受的最终布局。
Task/definition/resource/acceptance 的 role brief 改为 Runtime-attested **private user** context：它不具备 system
authority，且所有工具、资源、租约、审批、验收和终态本来就由 Runtime 强制，不能由该 brief 放宽。首项 public
package 在同一合格 cohort 中逐字节相同，故能在 role brief 前形成真正可复用前缀。私有内容依旧留在 package 后并
受现有 Team binding、session security domain 和 policy fences 保护。
无 package、旧持久 packet、直连 Agent 继续走当前布局；它们绝不伪装为高复用 cohort。

历史只能在相同 `package.digest`、相同 provider security identity、模型、协议、endpoint、transport 和工具 schema
下 append；任一不同时完整重建该 session 的 wire history，记录明确 reset reason。retry 使用同一 frozen package 与
相同 payload，保持 exact retry。clock、agent identity 等 operational facts 永远不得成为 package 内容。

### 2.3 真相、状态与生命周期

| 状态 | Owner | 允许转移 | 不变量/恢复 |
| --- | --- | --- | --- |
| `ContextEnvelope` | Context Runtime | selection 完成后生成 | 现有 visibility/lease/omission 语义不变 |
| `CohortPromptPackage` | Team instantiation compiler | Team binding digest 已冻结后、graph persist 前 freeze | 可从 durable packet 重建；非法或 digest 不符 fail closed |
| `ProviderPromptHistory` | Provider Runtime Client | 相同 identity + package digest 时 append | package 变更/reset 不可与旧 history 拼接 |
| `ProviderCacheIdentity` | Provider Transport Pool | 包含 session security domain、provider/model/protocol/endpoint/transport/tool schema/package digest | 不跨 tenant/session/team 使用 warm state |
| `ProviderPromptSegmentLedger` | Provider evidence owner | 每个 leaf attempt 写入，terminal 前完成 | write 失败不得吞掉 attempt；usage unknown 明确标记 |
| `CacheSloApplicability` | Pure economics classifier | preflight 与 outcome 都可计算 | 不控制权限/调度；证据不足为 `Unknown`，不能声称达标 |

### 2.4 业务全链路审计

| 链路步骤 | producer | consumer | 本版本改变 | 必须保持 |
| --- | --- | --- | --- |
| Team/Agent intent | planner/Team compiler | definition binding compiler | 选择已经获准的 shared items，生成 package input | role、acceptance、lease、tools 不变 |
| frozen packet | binding compiler | graph persistence/worker | 带可验证 package | packet 未绑定仍 fail closed |
| child context | Context Runtime | PromptAssembly | package 与 private context 分开表达 | visibility/omission 语义不扩大 |
| provider compile | runtime client | provider adapter | package 在 private objective 前；tail 保持 append-only | context 仍是 user authority |
| cold/warm dispatch | transport pool | HTTP provider | 仅 same identity 协调预热 | 不同 key 并发、无 mutex await |
| outcome/evidence | provider runtime | API/Eval/WebUI | segment ledger + applicability | raw usage 是费用唯一分母 |
| terminal/recovery | Runtime | Team/graph | 不改 terminal ownership | retry/cancel/timeout/idempotency 语义不变 |

### 2.5 并发、资源与失败审计

| 场景 | 正确行为 | 禁止行为 | 证据门 |
| --- | --- | --- | --- |
| 100 个同 identity/package 的 cold sibling | 仅 cache discovery 所需 leader/第二样本受控，其余在 barrier 后放行 | 将不同 package 串行；在 mutex 内 await | leader 数、barrier、并发时间线 |
| 不同 session/team/schema/package | 立即并行 | 因全局“命中率优化”排队 | parallel start/permit trace |
| package validation/digest 失败 | 拒绝该 executable packet，写结构化 reason | 退化为共享全部 context 或静默改变内容 | fail-closed unit/property test |
| Provider retry/fallback/cancel | 同 frozen package exact retry；fallback 新 identity | 在 retry 中重选 shared data 或跨 key借 warm state | exact wire digest / cancellation test |
| ledger persistence 失败 | attempt 明确 `usage_unknown`/evidence failure，不能算命中 | 吞掉失败或双写重计 | idempotent leaf attempt key test |
| package 超过 context capacity | 由现有 admission/required packet 策略在 freeze 前决定并留 omission | provider 临时裁掉 required shared item | capacity/recovery test |

## 3. 实施批次（必须按序，批间不能有发布）

### A. 事实账本与资格分类

1. 在 `provider_runtime_client.rs` 的唯一 wire 编译点构造 `ProviderPromptSegmentLedger`；记录每段的
   类别、digest、byte/token estimate、是否是共享前缀，绝不记录原文或 secret。
2. 按 leaf `attempt_id` 持久化 `ProviderCacheEconomics`: raw provider hit/miss/output、精确 LCP、
   package/schema/identity digest、cold/warm state、reset 原因。
3. 添加纯函数 `CacheSloApplicability`：只有足够 requests、同一受控 cohort、预测 reusable/miss 比例与
   完整 raw usage 才可标为 `HighReuseEligible`；否则 `LowAmortization` 或 `Unknown`。它只报告，不拒绝任务。
4. API/Gateway 仅投影这份 ledger；既有 usage 汇总继续兼容，但不得用估算替换 Provider raw usage。

### B. 安全公共 package

1. 在 `harness-contract::agent` 定义 versioned、serde-default 的 package 结构和 validate；`AgentTaskPacket`
   加可选字段，旧 packet decode 为 `None`。Intent、definition compiler、Team binding freeze 都必须传递它。
2. 仅 `team/instantiation.rs` 可从已验证的 `TeamInstantiationRequest.objective`、acceptance、upstream evidence/
   artifact/result context 编译 canonical package 内容；在现有 Team binding 已产出其 frozen digest 的后处理阶段，
   再以 `team_id + frozen binding digest` scope/digest freeze 到每个 packet。Private context、other-agent data、
   unbound Team、clock、agent contract、role objective、runtime board、project/memory 临时发现一律不能进入。
3. package 在 graph persist 前 freeze；worker 只消费 packet 中的 frozen package，不在执行时重新查可变 memory，
   并将原 `packet.objective` 收窄为 role-private objective（公共 parent facts 不再重复在该 message 中）。
   `ContextRuntime` 只负责将它按原样放入 typed immutable prefix，并继续在 private tail 管理动态 context/omission。
4. `PromptAssembly` 获得分离的 shared-cohort user-prefix 与 private-role user-prefix；`ProviderRuntimeClient`/
   history compiler 将二者依次放在 turn history 前，并使 shared package digest 成为 cache identity 成分、完整
   prefix 成为 history reset 成分。role-specific system suffix 必须被移除，不能挡在 shared package 之前。

### C. 复用边界与局部去重

1. 修正 `InProcessAgentWorker::cached_team_markdown_fragment` key，使其仅由 team binding digest 与 normalized
   instruction digest 决定；更新注释和命中/失效测试。不得把 Agent-specific binding 放入值或 key。
2. 将工具 schema、execution system suffix、runtime control 和 package 分别计入 segment ledger。只观察 schema
   fragmentation；本版本不把不同 role 工具并集化。
3. 保留 Transport Pool 的低复用 bypass：预测低复用 package 不等待公共缓存发现；高复用 package 才协调。

### D. 输出与成本的诚实治理

本批只增加观测：将 input miss、cache read、output、reasoning（如 Provider 返回）分开。不得引入 arbitrary
`max_tokens`、硬编码回答、删减 acceptance/evidence，或用“继续多轮”来制造虚高命中。若测试显示输出是主要费用，
后续版本必须另做语义输出契约和质量对照设计，不能在本版本偷偷截断。

## 4. 代码依赖锥与测试落点

| 文件/模块 | 责任 | 预期变化 | 必测 |
| --- | --- | --- | --- |
| `crates/harness-contract/src/agent/mod.rs` | durable packet contract | package types/validation/optional field | serde legacy、scope、digest、fail closed |
| `crates/harness-contract/src/agent/definition.rs` | intent -> packet | 只编译已验证 package | binding propagation |
| `crates/runtime/src/context/context_runtime.rs` | selected dynamic context | 识别 immutable prefix 与 private tail，不重排私有事实 | private isolation、team boundary、restart equivalence |
| `crates/runtime/src/conversation/prompt_assembly.rs` | typed model input | immutable user-prefix 与 tail 分离 | authority、deterministic render/order |
| `crates/runtime/src/conversation/context_plane.rs` | envelope -> prompt | 保持 runtime control 在 private tail | existing context mode coverage |
| `crates/runtime/src/provider/provider_runtime_client.rs` | history/wire/evidence | exact prefix compile、ledger、reset reason | append/retry/reset/LCP/property |
| `crates/runtime/src/provider/transport_pool.rs` | same-key warmup | consume package-aware identity only | 100 follower + low-reuse parallel |
| `crates/runtime/src/agent/in_process_worker.rs` | frozen child execution | inject package before first objective; fix local key | cross-agent shared prefix / key tests |
| `crates/provider/src/providers/openai_compat.rs` | wire adapter | only if exact user-message ordering requires adapter test | OpenAI-compatible wire order |
| Runtime/Gateway/Harness tests | projection/evaluation | economics display and workload classification | raw usage dedup / browser E2E |

The optional packet field deliberately avoids a mass migration of existing struct literals and durable data; however new Team-bound
packets with a purported package must validate it. A package cannot be silently dropped during Team instantiation, binding, persistence,
worker handoff, host construction, or retry.

## 5. Acceptance gates

### 5.1 Deterministic correctness gates

- 100 same-package sibling packets yield byte-identical system + immutable user-prefix before their private objective.
- `Private` context never appears in another Agent's package; `Team` visible context never crosses team id or binding digest;
  direct agent and legacy packet have no package.
- package digest change, schema change, endpoint/model change, session change and retry behavior have explicit expected history results;
  same package retry is byte-identical.
- dynamic header, clock and child contract remain after private objective/package boundary as specified and cannot obtain system authority.
- ledger emits one record per leaf provider attempt; raw usage is not double-counted; missing usage cannot improve ratio.
- existing collaboration completion, acceptance, review, tool, permission, cancel, timeout and recovery suites remain green.

### 5.2 Performance gates

- Different cache identities retain current parallel admission; P95 queue/dispatch has no regression against baseline.
- Same identity cold coordination creates no more than its documented discovery requests; no lock is held across await.
- package construction is once per frozen packet/binding, not reselected every provider turn; Team markdown cache has cross-Agent
  hit when team digest/instructions match and invalidates when either changes.
- no raw prompt payload, secret or private content is newly persisted in observability fields.

### 5.3 End-to-end gates

1. Build an isolated candidate and run unit/property/integration suites first. No paid Provider call before the resulting exact-wire
   high-reuse scenario predicts `>=92%` structural reuse and all privacy gates pass.
2. Run a local API + installed WebUI browser scenario that visibly renders Program, Team and Agent states—not merely a terminal log—
   and assert no failed browser request/console error.
3. Run one DeepSeek Flash calibration only with a high-reuse, semantically useful multi-Team workload: all tasks, accepted
   evidence, final artifact and dependency order must complete; compare required-acceptance/evidence/quality with the baseline.
4. Report three values separately: raw input hit ratio, total input/output cost components, and SLO applicability. The real
   three-Agent canary remains a low-amortization regression workload and must not be relabeled as a 90% failure or success.
5. Release is blocked unless functionality is unchanged and the eligible calibration meets >=90% cold-inclusive / >=95% warm;
   if Provider best-effort fails despite >=92% exact-wire evidence, retain evidence and report that provider variability honestly.

## 6. Pre-implementation audit checklist and stop rules

Before changing code, the implementer must prove every row in section 4 has a concrete constructor/caller/test. Before a phase merges,
run the matching section 5 gates plus the complete affected crate suite. The following stop rules prevent drift:

- Discovery of a needed permission, Team visibility, durable schema migration, or provider API change not covered above stops
  implementation and requires an amendment to this document before code proceeds.
- A failed privacy, append-only or parallelism invariant is a framework failure; do not compensate with prompt wording or paid retries.
- A high ratio achieved by increasing total input, removing business evidence, weakening acceptance, changing the workload, or excluding
  cold attempts is rejected.
- No cleanup, release/tag, service replacement or paid Provider run is part of this planning phase.

## 7. Expected result and non-claims

This design should increase sibling structural reuse only when the task actually has meaningful shared facts, eliminate needless local
Team fragment recomputation, and make every miss explainable. It is expected to qualify large repeated-package Team workloads for the
90% SLO while keeping low-reuse work fast and cheaper in absolute terms. It does **not** promise 90% for every arbitrary cold or
heterogeneous task, nor does it promise that provider-side best-effort caching can be forced to 100%.
