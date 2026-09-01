# Provider Prompt Cache Hardening v0.9.714

## 1. 决策与边界

本文件是 `v0.9.714` 在 Provider 输入缓存、请求布局和成本治理范围内的唯一实施权威。
它补充而不替代 `collaboration-program-hardening.md` 的协同语义、权限、调度、证据和
终态所有权。若两者发生交叉，以如下边界解释：

- `CollaborationProgram`、Execution Graph、Team/Agent Binding 仍拥有业务真相；
- 本方案只改变这些真相如何被编译为模型可见请求、如何复用 Provider 前缀、如何计量；
- 缓存命中不得扩大权限、隐藏证据、缩短任务、降低质量或改变业务终态；
- `v0.9.714` 是一个原子版本，内部可分阶段实现，但不得发布半新半旧的兼容状态；
- 在确定性门禁通过之前，不运行新的付费模型测试；实现不允许“边测边猜”。

终局目标不是伪造一个 90% 数字，而是同时满足以下四个条件：

1. 对适用的深度协同负载，Provider 实际计费口径的全程缓存命中率 `>= 90%`；
2. Runtime 可证明请求在结构上有 `>= 95%` 的精确可复用前缀，为供应商尽力而为留余量；
3. 冷启动、重试、压缩、故障转移和缓存预热请求全部计入成本，不排除坏样本；
4. 任务质量、功能、权限、证据、并发上限和终态正确性不退化，也不靠无意义填充放大分母。

DeepSeek 官方说明决定了框架的可承诺边界：缓存自动开启；只有从第 0 token 开始的完整
前缀单元才能命中；请求输入结束、模型输出结束、公共前缀检测和固定 token 间隔会落盘；
构建耗时是秒级；缓存为尽力而为而非 100% 保证。DeepSeek 的 Responses 兼容接口目前
不支持 `previous_response_id`、`prompt_cache_key` 或有状态 conversation，因此 Cowd 不能
照搬 Codex 的服务器端增量链，只能以精确稳定的无状态请求前缀、冷启动协调和 Provider
实测门禁达成目标。

官方依据：

- [DeepSeek 上下文硬盘缓存](https://api-docs.deepseek.com/zh-cn/guides/kv_cache/)
- [DeepSeek Responses API 兼容边界](https://api-docs.deepseek.com/guides/responses_api/)
- [DeepSeek 并发与 `user_id` KVCache 隔离](https://api-docs.deepseek.com/quick_start/rate_limit/)
- [DeepSeek 当前模型与价格](https://api-docs.deepseek.com/quick_start/pricing/)

## 2. 冻结基线

### 2.1 Cowd 与失败样本

| 项目 | 冻结值 |
| --- | --- |
| Cowd 分支 | `dev` |
| Cowd commit | `06ce256916fce89c41427180e92b289e0f16c417` |
| Cowd tree | `e5075a0a0bf23c6392a0a88f2b9506fed8ebafd1` |
| 工作树 | 方案落盘前 clean；本方案文档是后续唯一预期改动 |
| 远端关系 | 本地 `dev` 领先 `origin/dev` 38，禁止用旧远端覆盖 |
| 真实样本 | `target/acceptance/real-qwen/runs/v0.9.713-1788245558-mission-harness-deep/report.json` |
| 模型 | `deepseek-v4-flash` |
| 场景结果 | failed，127 个真实 Provider rounds |

样本的 canonical round 指标：

| 指标 | 实测 |
| --- | ---: |
| cache miss input | 5,157,007 tokens |
| cache hit input | 202,240 tokens |
| prompt total | 5,359,247 tokens |
| output | 393,099 tokens |
| 真实全程命中率 | 3.7737% |
| 平均 prompt / request | 42,198.8 tokens |
| 平均 miss / request | 40,606.4 tokens |
| 达到 90% 时允许的平均 miss / request | 4,219.9 tokens |

按 2026-09-01 DeepSeek `deepseek-v4-flash` 非高峰价格计算：当前样本约 `$1.395403`；
在 prompt/output 总量不变而命中率达到 90% 时约 `$0.411112`，节省 `$0.984291`
（70.538%）。该估算只用于说明损失规模，不是未来价格合同。

要达到 90%，同一负载最多允许 535,925 个 miss token，必须从当前 miss 中消除约
4,621,082 个 token。小修统计展示或减少一两个提示段不可能达标。

### 2.2 最新参考实现

`agents/` 下源码已获授权覆盖更新并快进到远端主线：

| 实现 | commit | tree | 分支/状态 |
| --- | --- | --- | --- |
| OpenAI Codex | `2b7c279735d0d096cf7b34fe98938f46792f4d4f` | `6379a38ef9dda6ba0a5a2867775baf4095d62c6d` | `main`, clean |
| Pi | `853a80d26c90a14c1886f0ebb8ffaae133ca2185` | `51833874449fe8ec0b1381496592cc54d0a77e8f` | `main`, clean |
| Hermes Agent | `71a82401706213f27799300096753733be7b7f41` | `3cfab850df5345ab18a7678c0a30616b43629d66` | `main`, clean |

这些提交是分析基线，不形成 Cowd 的运行时依赖或代码复制来源。

### 2.3 此前任务闭环账本与当前工作上下文

本节冻结 2026-09-01 继续实施时的真实状态，防止把历史计划中的旧“下一步”误当成仍未完成，
也防止确定性测试通过后再次遗漏真实 Provider、安装态和发布态闭环。

| 工作项 | 当前事实 | 闭环状态 | `v0.9.714` 处理 |
| --- | --- | --- | --- |
| `v0.9.706`–`v0.9.712` 协同框架阶段 | 后续提交和 evidence 已覆盖旧 handoff 中当时未完成的实现项 | 历史已闭环，不重做 | 仅做能力不退化回归 |
| `v0.9.713` 自治协同确定性修复 | 当前 HEAD 已含 `bbd4945b`（保留 delegated provider progress）和 `06ce2569`（关闭 review control plane） | 代码/确定性门已闭环 | 作为真实场景复验基线 |
| `v0.9.713` Candidate 6 真实 DeepSeek 终验 | 修复前场景失败，修复后从未重跑同一场景 | **未闭环** | 作为本版本最终真实验收，禁止换简单任务规避 |
| `v0.9.713` release evidence | `docs/evidence/autonomous-collaboration-convergence-v0.9.713.md` 仍标记 `Release status: pending` | **未闭环** | 最终 evidence 必须同时引用旧缺陷与新运行结果 |
| 当前 test-governance release gate | `tests/test-governance/test-inventory.yaml` 仍把上述 pending 文件声明为 0.9.713 release evidence，因此 `validate.sh quick` 的编译和全部静态边界通过后仍会在 governance 处 fail closed | **预期未闭环，不是代码回归** | 不得把旧证据提前改成 passed；真实复验通过并形成新 evidence 后，再按版本关闭权限原子更新版本、inventory、authority/evidence |
| 安装态与运行服务 | 已安装二进制报告 `0.9.713@fe492507`，运行中的 Gateway PID 2739；落后当前源码 HEAD `06ce2569` | **未闭环** | 确定性及真实验收通过后才替换服务并跑安装态 smoke |
| 版本/tag/分支同步 | 源码版本为 0.9.713；最新 tag 仍是 `v0.9.712`；本地 `dev` 领先远端 38 | **未闭环且需独立授权** | 本轮完成代码/evidence/version gate；没有明确 push/tag 授权时不得擅自发布 |
| Provider 配置治理 | 先前要求禁用百炼原模型、仅保留嵌入与 token plan，且真实执行只用 DeepSeek | 需在安装态验收复核 | 配置只做只读合规检查；不得回填百炼生成模型或泄露 secret |
| 本地安装/编译缓存清理 | 先前要求在最终通过后清理并安装最新包 | 尚未到安全执行时点 | 只在所有测试和 evidence 固化后执行，避免破坏失败诊断材料 |
| Provider prompt cache | 冻结样本全程仅 3.7737%，统计、布局、schema epoch、冷启动均有缺陷 | **本版本核心未闭环** | Phase A–G 原子实施 |

实施前工作树只含本文件及 `docs/architecture/README.md` 索引；基线 commit/tree 已冻结。进入实施
阶段后，工作树包含本方案依赖锥中的 Runtime、Model Protocol、Harness Eval 与两份文档改动，
仍无 staged 内容、无与方案无关的用户改动。不得为了满足 clean-worktree 脚本而提交主仓库；真实
Provider 验收使用从当前工作树生成的隔离、不可变临时候选，主仓库 commit/tag/push 继续等待明确授权。

历史权威关系固定如下：

1. `collaboration-program-handoff-2026-08-27.md` 只解释早期迁移历史，已被后续版本取代；
2. `autonomous-collaboration-convergence-v0.9.713.md` 和对应 evidence 拥有自治协同语义；
3. 本文件只拥有 Provider 请求布局、缓存、计量及其对协同链的集成；
4. 若最终真实场景再次暴露自治业务缺陷，必须回到其业务 owner 修复，缓存层不得吞掉终态失败。

### 2.4 上个未完成任务的唯一真实复现场景

最终不得新造更容易的演示任务。必须复用 Candidate 6 的同类、同深度 group-theory 场景：

| 字段 | 冻结值 |
| --- | --- |
| scenario | `live_autonomous_collaboration_deepseek` |
| session | `d443f89d-6f8d-4b85-8dac-aa83a1b9b448`（旧失败样本，仅作关联） |
| execution | `session-ingress-graph:c627614d40b21d78276b7147a63a8dbe`（旧失败样本） |
| 模型 | 只允许 `deepseek-v4-flash` |
| 任务类型 | 群论在当前 AI 中应用的研究、调研、分析、测试测评、模拟与独立产物生成 |
| 目标拓扑 | 至少 4 Team、16 Agent，并包含研究、实验/模拟、审计/反驳和综合物化 |
| 目标产物 | `group-theory-ai-autonomous-evaluation.html` 及可追溯 evidence |

旧运行的业务事实：12 个 Agent、3 个 Team 被观察到；8 个 Agent 完成、4 个失败；0 个 Team
完成；28 work items、6 proposals、5 bids、15 claims、7 accepted、9 discussions；没有 review、
没有跨 Team edge、没有物化产物。终端最终还出现 DeepSeek `402 Insufficient Balance`，但在
外部 402 之前已经存在 role cropping、partial-terminal 重试、review/challenge 语义混淆和
evaluator bid floor 四类框架问题。它们已由 `v0.9.713` 后续确定性修复覆盖，但尚无修复后的
真实证据，因此不能宣称协同任务已完成。

旧运行同时留下 28 个 `ContextTurnReport`：27 个 delegated turn 只有 1,126-byte stable head，
root 为 15,956 bytes；127 次 catalog lookup、127 次 provider request 和 127 次 schema
compilation。旧 report 另有包含 reviewer/多层投影的 usage 聚合，数值高于 canonical business
round 汇总。新 ProviderAttemptLedger 必须消除这类歧义；最终报告只允许 leaf attempt 去重后
的 usage 作为费用与缓存分母。

### 2.5 第一性原理可达性与完成后预估

缓存不能减少“首次出现的真实信息”。设一次运行所有 prompt token 为 `P`，每个安全域/模型/
schema cohort 的首次公共前缀为 `C`，Agent/Team 私有首轮绑定为 `B`，后续真实新增历史为 `D`，
retry/fallback/epoch 损失为 `E`，则理想上界为：

```text
max_hit_ratio = 1 - (C + B + D + E) / P
```

因此 90% 不是可由配置保证的普遍常数：单轮冷请求、每轮完全不同的短任务、跨安全域、频繁
换模型/schema 或真实新增内容占比超过 10% 时，数学上不可能达到。框架能保证的是不额外制造
miss、准确标注 applicability，并在可复用深度负载上达到硬门。

冻结样本 `P=5,359,247` 时，90% 只允许 `535,925` unique/miss token。基于旧运行的 127 round、
393,099 output 和 28 个 context report，实施前给出三档估算；最终必须由 exact wire replay 和
Provider 原始 usage 替换估算：

| 情形 | `C+B+D+E` 估算 | 全程 hit 估算 | 判定 |
| --- | ---: | ---: | --- |
| 只修统计、clock 和 schema no-op | 2.5M–4.8M | 10%–53% | 明确不达标 |
| 每 Agent 内 append-only，但没有跨 Agent cohort | 0.9M–1.8M | 66%–83% | 大概率不达标 |
| typed common prefix + 稳定 schema/dispatcher + append-only + cold singleflight | 0.38M–0.52M | 90.3%–92.9% | 对该深度场景可达但余量有限 |

第三档成立必须同时满足：公共框架/Program 信息确实相关且逐字节相同；私有绑定只在公共前缀
之后分叉；上一轮 assistant/tool delta 原样保留；相同 schema 不换 epoch；同 cohort 只有一个
cold leader；没有 usage_unknown 或重复聚合。若 exact wire 预测低于 92%，说明实现仍未形成
足够公共前缀或 unique delta 本身超过预算，禁止付费深测，更禁止靠 padding 或删证据造 90%。

完成后的稳妥预期是：

- 统计正确性：旧反例从伪 100% 修正为 31.91%，运行级 usage 无重复；
- 本地结构：同 epoch warm exact LCP `>=95%`，满足长前缀/小 delta 条件时 `>=99%`；
- 真实 Provider：该冻结深度场景 cold-inclusive `>=90%`、warm `>=95%`；DeepSeek 为 best-effort，
  单次波动不得被描述为所有任务的永久保证；
- 成本：在输出和信息量不缩水时，按冻结价格模型输入相关费用预期下降 65%–72%；
- 质量：Required/HighValue 覆盖 100%，Team/Agent 自治、工具能力、证据和最终物化不得退化；
- 性能：只序列化同 key 的冷窗口，不降低不同 key 和 warm 请求的并发宽度。

### 2.6 2026-09-01 实施前二次审计：DeepSeek 真实持久化语义

本轮在写代码前再次核对了 DeepSeek 官方
[Context Caching](https://api-docs.deepseek.com/guides/kv_cache) 文档。它补充了原方案不能靠
经验猜测的三个事实：

1. 缓存默认启用，但命中要求完整匹配一个已经持久化的 cache prefix unit；
2. 第一条 `A+B` 完成后，严格追加的 `A+B+C` 可以命中；但 `A+B` 后直接发 `A+C` 时第二条
   仍不命中，只负责让 Provider 发现并持久化公共 `A`，第三条 `A+D` 才能命中；
3. 缓存构建需要数秒且是 best-effort，终端 `message_stop` 不等于公共前缀已经持久化。

这推翻了“一个 cohort 只要一个 leader 完成就立即全放行”的初版协调器。修订后的状态机是：

```text
Cold
  -> PrimingFirst
  -> FirstRequestPersisted
       | exact extension -> Ready（复用第一条 input boundary unit）
       | divergent tail  -> PrimingCommonPrefix（只允许一条第二样本）
  -> Warm（bounded persistence barrier 后并发放行）
```

不同稳定首段、模型、协议、工具 schema 或安全域绝不共用这个状态机。2 秒 persistence barrier
是集中在 `ProviderCapabilityProfile::prompt_cache_behavior` 的 DeepSeek v4 capability fact，不散落
模型字符串分支，也不施加给其他 Provider；HTTP transport permit 在 barrier 前释放。由此，失败
样本的 cold/miss 上界要额外包含“每个真正分叉 cohort 的第二条发现请求”，预计区间从
`90.3%–92.9%` 下调为 `90.0%–92.4%`。余量更小，因此离线 exact-wire 门使用 `>=92%`，低于
该值不启动付费深测。

二次审计还纠正了一个本地设计错误：coordinator key 不能只有 provider/model/schema；它必须
包含稳定首个 model-visible item 的 digest。否则两个无共同稳定前缀的任务会错误共享 Warm
状态，既不能提高命中，还会错误串行。当前设计将该 digest 纳入 `ProviderCacheIdentity`。

## 3. 根因判定

结论：这是 Cowd 请求编译与状态纪元设计的系统性问题，不是 DeepSeek 缓存失效，也不是
单纯的字符预算问题。Provider 已返回真实 hit/miss，3.77% 是框架请求形态的结果。

### 3.1 前缀在历史之前被每轮重写

`ConversationRuntime::build_context_envelope` 当前先拼接 Agent 动态 system、Runtime
identity、`runtime_clock_section()` 和按用户输入派生的 governance id，再由
`provider_prompt_from_envelope` 放入 system。Provider 发送时又把 contextual packets 放在
持久历史之前。于是时间、身份、context selection 或治理 id 的任一变化，都会让后面的
完整历史失去前缀复用资格。

失败样本的 28 个 ContextTurnReport 中：

- 27 个 Agent turn 的稳定前缀只有 1,126 bytes，fingerprint 相同；
- root turn 的稳定前缀为 15,956 bytes；
- 每个 turn 的动态 Runtime system 最大为 10,575–29,396 bytes；
- 这意味着真正昂贵、重复的部分绝大多数被错误放在“动态但位于历史之前”的区域。

### 3.2 Agent 提示词缺少类型化边界

`PromptAssembly` 用字符串 sentinel `SYSTEM_PROMPT_DYNAMIC_BOUNDARY` 判断稳定段。Agent
worker 生成的长提示没有边界时，`host.rs` 将整段归入 dynamic。框架级执行协议、通用
工具规则、Team 协作约束和少量角色差异被编成一整块，每个 Agent 都从极早位置分叉。

这不是“把 boundary 往前挪”就能长期修好的问题。字符串位置约定不能表达 fragment 的
作用域、生命周期、authority、digest 和失效原因，必须改成类型化 Prompt Ledger。

### 3.3 工具 exposure 每个模型步骤都制造新 revision

`provider_plane.rs` 在每个模型步骤无条件 `fetch_add(1)`；随后
`configure_tool_exposure` 无条件清空 schema cache。即使实际工具集合与 schema 字节完全
相同，也会重新编译。失败样本 127 个 Provider 请求发生 127 次 schema compilation，
最大 schema 9,718 tokens；同一 Agent 内只有对象级 cache hit 统计，但 revision 仍让请求
属性和 wire schema 看起来不断换代。

动态工具发现的业务目标是正确的，错误在于把“catalog 观察发生了”当成“模型可见 schema
字节变化了”。版本必须由内容 digest 驱动，而不是请求计数驱动。

### 3.4 缓存统计公式错误并掩盖问题

Provider adapter 已正确把 DeepSeek 的 `prompt_cache_miss_tokens` 规范化为
`TokenUsage.input_tokens`，把 hit 规范化为 `cache_read_input_tokens`。但
`UsageTracker::cache_hit_ratio_bp` 和 session JSON 使用：

```text
read / (read + cache_creation)
```

DeepSeek 不单独报告 cache creation，因此只要有少量 read 且 creation 为 0，现有公式就会
显示 100%。正确 Provider 计费口径必须是：

```text
read / (input_miss + cache_creation + read)
```

失败样本中已经出现 7,101 miss + 3,328 hit 被持久化为 10,000 bp 的反例。

### 3.5 “cache friendly” 只比较局部 stable head

`ContextRuntimeKernel::cache_stability_report` 只要 stable-head hash 相同，就设置
`prompt_cache_friendly=true`，即使它同时知道 runtime header 已变化。Provider 要求从
第 0 token 精确匹配；局部头相同不能证明后续大段历史可复用。这个字段形成了错误安全感。

### 3.6 高并发形成缓存冷启动踩踏

DeepSeek 明确说明缓存构建耗时为秒级。若 12/16/28 个 Agent 在同一个公共前缀尚未落盘时
同时发出首请求，它们会一起支付 cold miss。现有并发控制只管理执行资源，不管理
`(provider cache domain, model, wire prefix)` 的单飞预热，因此“并发越充分”反而扩大费用。

修复不能全局降低并发。只允许同一冷前缀的短暂 followers 等待；不同前缀和已经 warm 的
请求继续并发。

### 3.7 大结果、补充 context 和重试缺少一致的 append-only 纪律

动态 context 被插到历史前面；工具结果虽然有总预算和 artifact 能力，但所有工具没有统一的
价值/相关性/载荷分类，既可能固定裁剪高价值文本，也可能把病理性 binary/重复 payload 原样
回灌；fallback/retry 仍可能重新组装请求。
只要已发送的旧消息在下一轮被 trim、merge、重排或重新渲染，DeepSeek 就无法命中。

### 3.8 Harness 有 token 数，没有硬门

Harness 能聚合 `cache_tokens`，却没有使用唯一 Provider-attempt ledger 计算全程 hit/miss，
没有 90% acceptance gate，也没有检查冷启动 leader 数、精确 LCP、schema digest 换代和无
意义 prompt padding。因此运行既可能失败又可能高费用，仍要到事后人工发现。

### 3.9 固定预算被误当成智能策略

当前 `config-default.yaml` 仍以 `subsystem_budget_ratio_bp=7000`、`preserve_recent=6`、
`summary_max_tokens=2000` 控制上下文；`conversation/compact.rs` 自身还有保留 4 条、估算
10K token 的默认值，工具结果预算也由固定比例推导。这些值可作为旧实现的保护阀，却不能
继续充当“哪些信息值得模型看到”的智能策略：它们既不知道 Provider 的实际 context window，
也不知道事实的任务价值、证据义务、跨轮依赖和注意力干扰。

缓存命中降低的是输入计算与价格，不扩大模型 context window，也不消除长上下文中的注意力
稀释。因此正确结论不是“继续裁剪”或“无限塞满”中的任一个，而是：默认保留完整且相关的
高价值内容，只有达到硬窗口、传输限制或经评测确认的注意力退化时才降级；降级必须可逆、
可追溯，且不得由缓存率或美元预算单独触发。

## 4. Codex、Pi、Hermes 的可迁移经验

### 4.1 Codex：严格增量链，而非每轮重建世界

Codex 的顶层 `AGENTS.md` 明确要求历史增量构建、避免频繁 context 变化、限制注入片段，
并把模型可见 fragment 类型化。实现中：

- session 派生稳定 `prompt_cache_key`；
- Responses Lite 为 tools/instructions 生成基于内容 hash 的确定性 item id；
- 只有模型、instructions、tools、reasoning、store、include、service tier、cache key、text
  等所有缓存敏感属性一致，且当前 input 是前次 input + server output 的精确扩展时，才发送
  incremental items；
- WebSocket 支持时传 `previous_response_id`，否则安全回退完整请求；
- WorldState 首轮写 full，后续只把 diff 追加到历史；compaction 才开启新 full baseline。

参考：

- [Codex `client.rs`（冻结提交）](https://github.com/openai/codex/blob/2b7c279735d0d096cf7b34fe98938f46792f4d4f/codex-rs/core/src/client.rs)
- [Codex `session/mod.rs`（冻结提交）](https://github.com/openai/codex/blob/2b7c279735d0d096cf7b34fe98938f46792f4d4f/codex-rs/core/src/session/mod.rs)

可迁移：请求属性全比较、确定性 item id、WorldState full/diff、append-only、压缩新纪元。
不可直接迁移：DeepSeek 不支持 `previous_response_id` 和 `prompt_cache_key`。

### 4.2 Pi：简单稳定的 AgentState 与“缓存浪费”观察

Pi 的 `AgentState` 保存一次 system prompt、tools 和 messages snapshot；主 loop 将 user、
assistant、toolResult 按发生顺序 append。compaction 在 session tree 中形成显式 boundary，
旧 entries 从活动 context 中移除，summary + kept tail 成为新上下文。

Provider 层正确兼容 DeepSeek 的 `prompt_cache_hit_tokens`，并用：

```text
missed = min(previous_prompt, current_prompt) - current_cache_read
```

估算“上一轮本应复用却重新计费”的 token 与美元浪费；compaction/branch summary 会明确重置
比较基线。Pi 也按 Provider 能力发送 session affinity、OpenAI cache key 或 Anthropic
cache-control，而不是把一个参数假设为所有 Provider 通用。

参考：

- [Pi `agent-loop.ts`（冻结提交）](https://github.com/earendil-works/pi/blob/853a80d26c90a14c1886f0ebb8ffaae133ca2185/packages/agent/src/agent-loop.ts)
- [Pi `cache-stats.ts`（冻结提交）](https://github.com/earendil-works/pi/blob/853a80d26c90a14c1886f0ebb8ffaae133ca2185/packages/coding-agent/src/core/cache-stats.ts)

可迁移：缓存浪费而非只看 hit、显式 reset boundary、Provider capability 分派。
不可照搬：Pi 是单 Agent session 模型，不解决 Cowd 多 Agent 同前缀冷启动踩踏。

### 4.3 Hermes：会话 prompt/tool snapshot 冻结和最后一刻装饰

Hermes 将系统提示词第一次构建后持久化；继续会话时逐字节恢复，持久化失败会以 warning
暴露。工具定义在 Agent 初始化时冻结，只在 compaction commit boundary 刷新，因为该处
本来就会失效缓存。它对消息先完成 orphan 修复、thinking 处理、whitespace 规范化、工具
参数 canonicalization，最后才在请求副本上放 cache marker，避免同一历史消息在不同轮次
因装饰顺序变化而改变字节。Provider failover 会先 strip 再按目标能力重新规划。

参考：

- [Hermes `prompt_caching.py`（冻结提交）](https://github.com/NousResearch/hermes-agent/blob/71a82401706213f27799300096753733be7b7f41/agent/prompt_caching.py)
- [Hermes `conversation_loop.py`（冻结提交）](https://github.com/NousResearch/hermes-agent/blob/71a82401706213f27799300096753733be7b7f41/agent/conversation_loop.py)
- [Hermes `conversation_compression.py`（冻结提交）](https://github.com/NousResearch/hermes-agent/blob/71a82401706213f27799300096753733be7b7f41/agent/conversation_compression.py)

可迁移：存储完整 prompt snapshot、工具按 compaction epoch 更新、canonicalize-before-mark、
failover 目标本地化。不可照搬：Anthropic 四断点不适用于 DeepSeek 自动前缀缓存。

### 4.4 综合决策

Cowd 采用：

- Codex 的 typed WorldState full/diff 与严格 request-property identity；
- Pi 的真实 miss/waste 计量和显式 reset boundary；
- Hermes 的 session snapshot、tool epoch、最后一刻目标 Provider 编译；
- Cowd 自己新增多 Agent `CacheWarmupCoordinator` 和权限不扩大的稳定工具调度协议。

不引入对三套仓库的编译依赖，不复制其 Provider 特有兼容分支。

### 4.5 “Pi 99%”的可验证结论

在冻结的 Pi 源码、README、CHANGELOG 和测试中没有找到“所有任务缓存命中率 99% 以上”的
正式合同。Pi 的 `/session` 显示：

```text
cacheRead / (input + cacheWrite + cacheRead)
```

它的 `cache-stats.ts` 只比较相邻请求中“上轮已有但本轮未读缓存”的浪费；第一轮、compaction
和 branch-summary 后的第一轮会重置比较基线。这是合理的诊断口径，但不能替代包含 cold、
write、retry 和 compaction 的全程成本口径。

长会话暖态接近 99% 在数学上完全可能。设稳定前缀为 `S`，第 `i` 轮只追加 `d_i`，则该轮
理想命中率近似：

```text
hit_i ~= (S + Σ[j<i] d_j) / (S + Σ[j<=i] d_j)
```

当累计历史为 100K token、当前只新增 1K token 时，单轮就是约 99%；累计多轮时，重复输入
总量按近似二次增长，真正首次处理的独特 token 只按线性增长，暖态聚合值自然趋近 100%。
这不表示短任务、冷启动、超 TTL、模型切换、工具 schema 变化、压缩纪元或跨 Agent 请求也能
达到 99%。任何“99%”报告必须同时给出 denominator、cold inclusion、epoch 数、TTL、请求数、
Provider 原始 usage 和是否使用服务器端 continuation。

### 4.6 主流框架/Provider 的共同策略与差异

官方 Provider 文档和参考实现给出的共同策略不是“删上下文”，而是“把可复用、高价值内容
放在前面并保持精确不变，把变化只追加到后面”：

- OpenAI 缓存完整渲染上下文，包括 developer instruction、工具定义、历史、文档和多模态；
  完整前缀与缓存敏感设置必须匹配，`prompt_cache_key` 只改善路由亲和性，不能修复字节漂移；
- Anthropic 的前缀层级固定为 tools -> system -> messages，支持显式/自动 breakpoint；工具
  搜索把按需 schema 追加为 `tool_reference`，避免修改早期前缀；
- Gemini 建议把大型公共内容放在 prompt 开头，也支持把大语料显式建成带 TTL 的 cache object；
- DeepSeek 自动缓存从第 0 token 起的公共前缀，没有显式 cache object 或 continuation，因而
  对精确布局、同域并发预热和 append-only 要求最高；
- Codex 进一步用 `previous_response_id`/WebSocket 只传新增 items，并在复用前穷举比较所有
  cache-sensitive request properties；Pi 则保持单 Agent state 简单追加。

官方依据：

- [OpenAI Prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching)
- [Anthropic Prompt caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)
- [Anthropic Tool use with prompt caching](https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-use-with-prompt-caching)
- [Gemini Context caching](https://ai.google.dev/gemini-api/docs/caching)

Cowd 要吸收的是这些不变量，而不是某个 Provider 的专用字段。对支持 continuation/explicit
cache 的 Provider 走能力适配；对 DeepSeek 仍以同一个 Canonical Ledger 生成精确公共前缀。
这样既不降低能力，也不会把系统绑死在 OpenAI、Anthropic 或 Gemini 的单一协议上。

## 5. 指标合同：90% 到底指什么

### 5.1 唯一 Provider 计费指标

对每个实际发送并获得 canonical usage 的 Provider attempt：

```text
provider_prompt_tokens = input_miss + cache_creation + cache_read
provider_cache_hit_ratio_bp =
    cache_read * 10_000 / provider_prompt_tokens
```

运行级：

```text
run_cache_hit_ratio_bp =
    Σ cache_read * 10_000 /
    Σ (input_miss + cache_creation + cache_read)
```

硬规则：

- 首次冷请求、cache prime、retry、fallback attempt 全部进入 Σ；
- 以 `provider_request_id + logical_request_id + attempt` 去重，每个实际 wire send 只计一次；
- graph/root/team 的聚合投影不得二次累加同一个 leaf usage；
- 无 usage 的失败 attempt 记录为 `usage_unknown`，验收失败，不能从分母消失；
- 不再使用 `read/(read+creation)`；不把 output token 放入命中率分母。

### 5.2 结构指标

`WirePrefixOracle` 在请求发出前对同 cache domain 的已发送 wire canonical form 计算 exact
longest common prefix，并输出：

- `wire_prompt_tokens_estimated`；
- `reusable_prefix_tokens_estimated`；
- `unique_suffix_tokens_estimated`；
- `cache_sensitive_properties_digest`；
- `prefix_predecessor_request_id`；
- `invalidation_reason`；
- `structural_reuse_ratio_bp`。

Provider tokenizer 可用时按 token；不可用时同时记录 canonical bytes LCP 和保守 token
估算。结构指标不能替代 Provider usage，只负责付费前准入和根因归属。

### 5.3 分层 SLO

| SLO | 门槛 | 适用范围 | 失败动作 |
| --- | ---: | --- | --- |
| 结构可复用 | `>= 95%` | warm 后、同一 cache epoch 的可复用请求 | 发送前阻断 paid acceptance；生产进入成本降级策略 |
| warm Provider 实测 | `>= 95%` | 每个 epoch 除唯一 cold leader 外 | 标记 degraded，暂停同 key followers 并诊断 |
| 全程 Provider 实测 | `>= 90%` | 深度协同 acceptance，全含 cold/prime/retry | 版本门失败，不得 tag/install |
| 99% qualified structural | `>= 99%` | 上轮可复用前缀 >=100K 且本轮独特 suffix <=1% | 作为 Pi/Codex 可比的稳态能力门，失败必须归因 |

单次、完全冷、没有可复用前缀的请求在数学上不可能达到 90%。框架必须标记
`cache_slo_applicability=cold_singleton`，不能伪报失败或成功。以下负载必须适用：同一安全
cache domain 内至少 3 次请求且投影 prompt 总量 `>= 64K tokens`，以及所有深度多 Team
验收场景。

`99%` 只允许作为 `qualified_steady_state` 单列报告，绝不替换 cold-inclusive `>=90%` 主门。
若本地 exact LCP 已达 99% 而 Provider 实测未达，报告为 Provider best-effort/TTL/alignment 差异；
若本地 LCP 未达，则是 Cowd 请求构造缺陷。这样既追求 99% 的工程上限，也不伪造不可承诺的
供应商结果。

### 5.4 付费前预测

对计划请求集合：

```text
projected_hit_ratio = 1 -
  (unique_cold_prefix_tokens + Σ unique_suffix_tokens + retry_risk_tokens)
  / Σ projected_prompt_tokens
```

Harness 在付费前要求预测 `>= 92%`；2% headroom 用于 Provider 固定间隔、tokenizer 误差和
尽力而为波动。预测低于 92% 时不发起深度 paid acceptance，而是输出按 fragment/tool/output
归因的阻断报告。禁止通过复制无用文本提升预测分母。

### 5.5 基线诊断与自适应价值包络

以冻结样本的 5,359,247 个 prompt token 为数学对照，90% 确实只允许 535,925 个 token
首次计算；这个数只证明现状必须消除 4,621,082 个重复 miss，不是未来实现的 token 上限，
更不能反向迫使系统缩短答案、证据或历史。

每次请求由 `ContextValuePlanner` 根据实际 Provider hard window、输出预留、载荷字节上限、
安全边界和任务证据义务计算自适应包络：

```text
usable_context = min(provider_context_window, transport_effective_window)
                 - output_reserve - protocol_safety_reserve

admit(fragment) only if:
  authority/visibility permits
  && task relevance or evidence obligation is proven
  && no exact/semantic duplicate already admitted
  && quality frontier does not regress
```

其中 `output_reserve` 由任务类型、已观测输出分布和剩余 obligation 动态估算；它可以扩展，
不可用固定 70% 或固定 last-N 替代。业务必要 fragment 即使增加 prompt 总量也必须保留；
无关、重复、padding fragment 即使完全可缓存也必须拒绝。

离线 replay 仍应把 miss 分解为 shared cold、Agent binding、真实新 delta、retry/fallback 和
Provider alignment 五类，并验证总 miss 不超过该具体负载总 prompt 的 10%。分类值是可观测
归因，不设置 50K/70K/360K 等跨任务 magic cap。超标时先改布局、去重复、稳定工具和收敛
cold leader，不得先删高价值内容。

### 5.6 质量优先的多目标合同

优化顺序固定为：正确性/完整性/证据/长期记忆第一，安全与硬窗口是约束，缓存、价格、吞吐
在前两者满足后优化。新增指标：

- `required_fact_recall`：验收所需事实能否在跨轮、跨 Team 场景准确召回；
- `retained_value_coverage`：已认定高价值 fragment 在模型可见上下文中的覆盖率；
- `cross_turn_dependency_recall`：远距离约束、决策和未决事项的召回率；
- `cached_value_tokens` 与 `uncached_delta_tokens`：区分“有价值且复用”与单纯放大分母；
- `attention_interference_score`：加入额外 context 后的事实混淆、指令冲突和检索退化；
- `quality_per_dollar`、TTFT、总 wall time 和完成率：形成质量-成本-性能 Pareto frontier。

任何候选方案只要降低 required fact recall、证据覆盖、工具正确率或任务完成率，即使达到
99% cache hit 也失败；反之，有价值的稳定内容使 prompt 增长并提升质量时，不因 token 数比
旧基线高而失败。

## 6. 终局架构

### 6.1 类型化 Canonical Prompt Ledger

删除字符串 sentinel 作为稳定性真相，新增不可变类型：

```text
PromptFragment {
  id
  revision
  authority
  scope: Global | Program | Agent | Turn | Event
  lifetime: Process | Program | Session | Turn | OneRequest
  cache_class: SharedStatic | EpochStatic | AppendOnlyDelta | NeverModelVisible
  visibility: SecurityDomain | Program | Team | Agent | EvidenceOnly
  value_class: Required | HighValue | Supporting | Retrievable | PayloadOnly
  canonical_bytes
  content_digest
  provenance
  applicability
}

CanonicalPromptLedger {
  shared_static[]
  epoch_static[]
  append_only_deltas[]
  wire_epoch
}
```

渲染顺序固定：

```text
L0  StableKernel: identity/protocol/authority/safety   SharedStatic
L1  stable tool/skill/navigation contracts             SharedStatic
L2  ProgramDossier + shared reference corpus           EpochStatic
L3  TeamDossier + common applicable skills/tools       EpochStatic
L4  AgentBinding/private objective/capability delta    EpochStatic or first delta
L5  full conversation/tool/peer/event journal          AppendOnlyDelta
L6  current RuntimeTurnCapsule/continuation             AppendOnlyDelta
```

`StableKernelV1` 编译进版本化代码或构建产物，并由 digest 标识。允许硬编码的是框架长期
不变量：Cowd identity、authority 顺序、协作协议状态机、工具事务/证据生命周期、canonical
serialization、cache layout、稳定 dispatcher schema、安全/恢复不变量和通用导航语法。
这些内容当前散落在 `in_process_worker.rs` 的长字符串里，提炼后既能提高所有请求共享率，
也能让 Agent 获得更完整一致的协作能力。

禁止硬编码的是业务语义与智能选择：用户目标、Team/Agent 名称和拓扑、事实证据、工具权限、
模型/Provider、执行策略、Agent 数量、并发上限、TTL/价格、固定 token 阈值，以及哪些内容
值得保留。原则是“硬编码协议结构，不硬编码任务答案和决策空间”。

`ProgramDossier` 在 Program admission 时一次编译并在 epoch 内冻结，包含完整共同目标、术语、
验收、拓扑、共享约束、初始证据地图和适用参考资料；`TeamDossier` 保存 Team 共同任务和已知
依赖。它们不是源码硬编码，却在同一 cohort 内保持逐字节相同。Skill、工具知识、源码地图、
规范、示例和参考文档形成 content-addressed `StableKnowledgePackage`，带版本、来源、适用性
和保密级别。只要相关、有价值且可见，就可以完整进入稳定前缀，不因“历史裁剪”理念被删掉。

只有真实相同且对该 Agent 相关的内容可以上移。`PromptFragmentDeduplicator` 做 exact/near-
duplicate 去重和冲突标记；不把无关 Team 材料或所有可用工具强塞给每个 Agent，不放 padding，
也不把“更多上下文”误解成“所有上下文”。

### 6.1.1 Stable Knowledge Package 供应链

每个 package 必须有 `content_digest`、`schema_version`、`provenance`、`security_domain`、
`applicability_predicate` 和 canonical order key。构建时生成 manifest；运行时只选择适用 package，
按 common-first 确定性排序并冻结到 epoch。相同内容只存/渲染一次，变更通过新 digest 进入新
epoch，不在会话中原地重写。

这允许大胆加入高价值的框架规则、完整设计文档、代码索引、领域语料和稳定示例，因为其后续
读取可缓存；同时通过适用性、去重、冲突检测和质量门阻止无关信息污染，而非用固定字符预算
压低 Agent 智能。

### 6.2 Dynamic WorldState 只追加，不前插

在顶层 turn admission 时冻结一个不可变 `RuntimeTurnCapsule`：

```text
turn_id, observed_at, session/program/agent binding refs,
permission/policy revision, context selection refs,
governance report id, current workspace/environment diffs
```

它在第一轮 Provider call 前作为一条 Runtime-attested context message 追加到 SessionHistory。
同一 turn 的工具 loop 复用同一 capsule。时间变化、peer message、approval、work market 和
context recall 后续都作为新事件追加；永远不回写 system 或插到旧历史之前。

`current_time` 仍可提供实时值；模型不因 system 内每秒变化而失去整段缓存。

### 6.3 ProviderCacheIdentity 与 CacheEpoch

```text
ProviderCacheIdentity = digest(
  provider account/security principal,
  provider protocol,
  model wire id,
  DeepSeek user_id isolation domain,
  shared static digest,
  model-visible tool schema digest,
  cache-sensitive request properties digest
)
```

`user_id` 必须按安全主体 + workspace/program 安全域稳定派生；禁止每个 Agent 生成不同
`user_id`，也禁止跨租户共享。值只含 `[A-Za-z0-9_-]`，不含隐私数据。

`CacheEpochState` 持久化：

```text
epoch_id, identity_digest, predecessor_wire_digest,
state, leader_request_id, waiters, warmed_at, expires_at,
observed_hit/miss/write, structural_lcp,
invalidation_reason, compaction_generation
```

状态机：

```text
Cold -> Priming -> Warm -> Degraded
  |       |          |       |
  +----> Invalidated/Expired <-+
```

只有 static/tool/cache-sensitive property digest 变化才换 epoch。clock、turn id、tool receipt
和 peer update 只产生 append delta。compaction 产生新的 session-history generation，但仍可
复用 L0/L1 shared prefix。

### 6.4 稳定工具调度协议

当前“每轮动态 native schema 集合”改为 Provider capability 驱动的两种模式：

1. `StableNativeEpoch`：工具集合在整个 Agent/compaction epoch 冻结；适合 schema 小且权限
   在 epoch 内不变的 Provider/Binding。
2. `GovernedDispatcher`：DeepSeek 深度协同默认使用固定 `runtime_discover` 与
   `runtime_invoke` 两个窄 schema。模型通过 Runtime 签发的 descriptor ref 获取具体工具
   schema，再提交 `{descriptor_ref, arguments}`；Runtime 仍执行原始 JSON Schema、Binding、
   permission、lease、effect 和 evidence 校验。

关键安全规则：

- 看见 dispatcher 不等于获得任何工具权限；descriptor 必须由当前 Binding/epoch 签发；
- revoked/expired descriptor fail closed；旧 prompt 提到的工具也不能绕过 Runtime；
- `tool_choice=required` 指向稳定 dispatcher，并由 Runtime 对 required descriptor 二次验证；
- 高价值工具可在 Agent bootstrap tail 携带紧凑 descriptor，避免无意义的 discovery round；
- catalog revision 不改变 dispatcher schema；只在 descriptor 内容/权限层更新 append-only diff；
- schema cache key 使用 canonical schema digest，内容相同不因 revision 计数失效。

这保持所有功能和权限验证，同时把失败样本中的 127 次 schema compilation 收敛为每个实际
schema digest 一次。禁止简单把所有原生工具暴露给所有 Agent 来换命中率。

### 6.5 CacheWarmupCoordinator：同 key 单飞、不同 key 并发

DeepSeek cache build 是秒级。新增进程级、可由 RuntimeHotStatePlane 恢复的 singleflight：

```text
acquire(cache_identity):
  Warm       -> immediately dispatch
  Priming    -> register cancellable follower and wait bounded time
  Cold       -> atomically become leader
  Degraded   -> apply retry/backoff/budget policy, never stampede
```

leader 默认使用第一条真实业务请求，不额外生成模型输出。只有当预测器证明一个有界
`CachePrimeRequest` 的总成本可被后续请求回收，且 Provider 有可靠输入结束落盘语义时，才
允许专用 prime。prime 必须：

- 与后续请求拥有相同 shared prefix、tool schema 和 cache identity；
- `max_output_tokens` 极小，结果不进入业务历史；
- 记入真实 attempt ledger 和全程命中率分母；
- 有固定语义 marker，后续请求精确扩展该 marker；
- 每个 epoch 最多一次，失败不得循环 prime。

leader 完成后等待 Provider capability 配置的 bounded persistence barrier；followers 被一次
唤醒并充分并发。等待只按 cache key 局部发生，不占 Provider/Agent 执行 permit，不持有
全局 mutex，不阻塞其他 key。

### 6.5.1 CacheCohortPlanner：跨 Agent 公共前缀因子化

Program 编译时按 security domain、Provider/model/protocol、StableKernel、共同 dossier/corpus、
skill/tool digest 和缓存敏感属性建立 cohort。`CacheCohortPlanner` 使用 prefix trie 计算可安全
共享的最长公共序列，并按 common-first 排列：

```text
StableKernel -> ProgramDossier/reference corpus -> common skills/tools
             -> TeamDossier -> AgentBinding -> append-only private journal
```

同 Program 的 Agents 不必拥有完全相同的工具权限才可共享前半段；权限差异只能出现在后续
binding/descriptor，ToolHost 仍按实际 principal 验证。Planner 不得为合并 cohort 扩大权限、
暴露私有信息或加入无关 package。公共 Program 更新在当前 epoch 内作为各 Agent 尾部事件追加，
不插入旧前缀；只有显式 epoch boundary 才重建 dossier。

这一步是 Cowd 超越 Pi 单 Agent 模式的核心：一次 ProgramDossier/知识包冷构建可服务多个 Team
和 Agent，same-key singleflight 避免并发踩踏，不同 cohort 仍最大并发。

### 6.6 全保真上下文驻留与分级载荷

Canonical Prompt Ledger 同时是 durable journal 的模型可见投影，禁止维护第二套会漂移的
“精简历史”。默认模式是 `FullReplay`：在实际 Provider hard window、输出预留和质量余量
允许时，完整保留所有相关 user/assistant/tool/peer/event 内容和高价值参考资料；生成导航索引、
稳定 anchor、目录和摘要 overlay 帮助定位，但 overlay 不替换原文。

降级顺序必须非破坏、可逆：

1. `FullReplay`：完整相关历史和知识都在 prompt；
2. `AnchoredFullReplay`：原文仍在，附加层级目录、anchor、冲突/未决索引；
3. `LosslessReferencedReplay`：仅在硬 context/byte 上限或实测 attention interference 触发时，
   大型/低价值/重复 payload 在模型视图中转为 digest + selector，完整原文仍 durable 且可取回；
4. `CompactedEpoch`：前述方式仍无法满足硬约束时才创建新历史纪元，且保存可验证 handoff、
   原始 transcript ref 和 required-fact coverage proof。

统一 ToolHost 输出策略因此改为价值与载荷类型驱动，而非固定 token 双阈值：

- 文本事实、代码、日志、表格和证据只要相关且窗口允许，默认完整进入历史；
- binary/base64、病理性超大重复输出、Provider 不支持的媒体或会越过硬载荷上限的内容先写
  `ArtifactStore`，模型得到 typed ref、metadata、精确 range selector 和必要预览；
- exact/near-duplicate 内容引用已存在 digest，不重复注入；
- Agent 可主动拉取任意精确 range，结果继续 append，终态 verifier 使用完整 digest/receipt；
- 已进入历史的消息永不因 last-N、TTL、缓存费用或固定比例而改变 shape/whitespace。

缓存 token 仍消耗 context window 和注意力，所以“可缓存”不是无条件准入理由；但缓存费用也
绝不能成为删除有价值信息的理由。唯一裁决是任务价值、权限、硬限制和实测质量 frontier。

### 6.7 PreparedProviderRequest 冻结

一次 logical model step 先生成不可变 `PreparedProviderRequest`：canonical wire body、
cache identity、fragment manifest、tool digest、LCP proof、value envelope 和 request id。发送之后：

- 网络 retry 重发相同 model-visible bytes，attempt metadata 放在 wire 外；
- provider fallback 从同一 canonical fragment ledger 编译目标 Provider 形态，不重新读取
  clock/memory/workspace；
- failover 使用新的 cache identity 和 coordinator，绝不宣称继承原 Provider cache；
- 收到任何模型输出后禁止静默重放；必须按明确恢复状态机处理，避免重复动作和重复费用；
- wire evidence 在发送前持久化，usage 在终态 event 中关联同一 attempt。

### 6.8 Compaction 是显式新历史纪元

沿用现有 durable semantic compaction owner，但把 compaction 从固定 last-N/token 比例触发改为
最后手段：只有 `FullReplay`/`AnchoredFullReplay`/`LosslessReferencedReplay` 都无法满足 Provider
硬窗口或已经由质量评测证明长上下文干扰时才允许。缓存费用和命中率不得独立触发压缩。
同时增加：

- compaction commit 产生 `history_generation + 1` 和 cache invalidation receipt；
- summary + preserved tool pair + current turn 是新 append-only baseline；
- tool snapshot/descriptor epoch 只在该边界或显式 capability epoch 更新；
- 先持久化 compaction bundle，再切换内存和 cache epoch；失败保留旧 transcript；
- 首个 post-compaction request 作为新历史 leader，旧 usage 不参与 warm SLO 比较；
- L0/L1 shared prefix 不变时仍可命中，不把整个 Provider cache 全盘清零。
- compaction 前后运行 required-fact/evidence/decision/unresolved-item coverage diff，任何必需项丢失
  都拒绝 commit；完整 transcript 始终可通过 ref 恢复，不因摘要成为事实孤岛。

## 7. 业务链、反向证据链与所有权

### 7.1 正向链

```text
User/Gateway input
 -> CollaborationProgram / Agent Binding admission
 -> PromptFragment producers
 -> CanonicalPromptLedger commit
 -> CacheSloPlanner projection
 -> CacheWarmupCoordinator admission
 -> Provider-specific canonical wire compile
 -> WirePrefixOracle proof + ProviderAttemptStarted
 -> Provider stream
 -> canonical usage (miss/write/read/output)
 -> ProviderAttemptTerminal
 -> Session/Graph usage projection
 -> Harness report / UI diagnostics / cost gate
```

### 7.2 反向证据链

```text
90% acceptance claim
 -> unique Provider attempt ledger totals
 -> each terminal usage + request id
 -> exact wire artifact and cache identity
 -> LCP predecessor proof / cold leader receipt
 -> fragment manifest and canonical digests
 -> Program/Binding/permission/context provenance
 -> authenticated user request
```

### 7.3 所有权表

| 概念 | 唯一 owner | 非 owner 禁止事项 |
| --- | --- | --- |
| 业务任务/Team/Agent truth | CollaborationProgram / Execution Graph | cache layer 改终态或跳过义务 |
| 模型可见 fragment | CanonicalPromptLedger | Provider adapter 临时拼动态 system |
| StableKernel/package manifest | versioned build + PromptFragmentRegistry | Agent/Provider 临时改 framework protocol |
| Program/Team dossier | CollaborationProgram compiler | cache layer 合并安全域或改业务事实 |
| 模型可见驻留模式 | ContextValuePlanner | cost planner 因命中率裁剪 Required/HighValue |
| Provider cache capability | Provider registry snapshot | 按模型字符串散落 if/else |
| cache identity/epoch | CacheEpochStore | Agent 自己生成 cache key |
| cold singleflight | CacheWarmupCoordinator | scheduler 全局降并发 |
| tool authority | Binding + ToolHost | dispatcher descriptor 扩权 |
| exact wire | Provider adapter from PreparedProviderRequest | retry 重建请求 |
| usage truth | ProviderAttemptLedger | graph projection重复累计 leaf usage |
| 90% gate | Harness Eval policy | Runtime 伪造 provider hit |
| complete evidence | ArtifactStore / Runtime event store | prompt 摘要替代原始 receipt |

## 8. 状态真相与 producer-consumer 审计

### 8.1 状态真相表

| 状态 | durable | revisioned | restart 行为 | failure 行为 |
| --- | --- | --- | --- | --- |
| Prompt fragment manifest | 是 | content digest | 恢复同 bytes | digest 不符 fail closed |
| Stable knowledge manifest | 是 | package digest | 恢复同 package/order | provenance/visibility 不符拒绝 |
| Program/Team dossier | 是 | program/team epoch | 逐字节恢复 | 业务更新只追加或新 epoch |
| Context residency receipt | 是 | request/history generation | 重放同 value decision | Required/HighValue 缺失阻断 |
| Cache epoch | 是（元数据） | epoch id | 恢复为 `Expired/Unknown`，由 usage 验证后 warm | 不假定 Provider cache 仍在 |
| Warmup waiters | 否 | cache key | restart 后重新 singleflight | cancellation 自动移除 |
| Provider attempt | 是 | logical id + attempt | terminal 可重放投影 | usage unknown 阻断验收 |
| Tool descriptor | 是/可重建 | catalog + binding digest | 重新验证权限 | stale/revoked 拒绝 |
| Turn capsule | 是 | turn id | 逐字节恢复 | 禁止重新读取 clock 生成另一个版本 |
| Compaction generation | 是 | monotonic | 从 committed bundle 恢复 | atomic failure 保留旧 history |

### 8.2 producer-consumer 表

| Producer | 载荷 | Consumer | 必须证明 |
| --- | --- | --- | --- |
| PromptFragmentRegistry | typed fragments | Ledger | scope/lifetime/digest 完整 |
| Program compiler | frozen dossier/packages | Ledger/cohort planner | applicability/visibility/epoch 完整 |
| ContextValuePlanner | residency mode/value receipt | Ledger/compactor/harness | full first，required coverage，触发原因可证 |
| Ledger | canonical ordered manifest | Provider compiler | 顺序和 bytes 确定 |
| Tool exposure planner | descriptor refs/tool digest | Ledger + ToolHost | 模型可见能力不大于 Binding |
| CacheSloPlanner | projected reuse/cost | coordinator/harness | 无 padding、含 cold/retry 风险 |
| Coordinator | leader/warm/degraded receipt | Provider dispatcher | 同 key 最多一个 leader |
| Provider adapter | wire artifact | Prefix oracle/provider | serialization deterministic |
| Provider usage parser | normalized TokenUsage | attempt ledger | miss/read/write 无双算 |
| Attempt ledger | unique attempt totals | graph/report/harness | projection只引用不复制计费事实 |
| Compactor | generation receipt | ledger/coordinator | old/new epoch 原子切换 |

## 9. 并发、等待、失败恢复和背压

### 9.1 并发不变量

- 同一个 cold cache identity：`active_leaders <= 1`；
- 不同 cache identity：不增加全局串行边；
- Warm followers 立即进入既有 Provider 并发池，保持 Execution Graph 宽度；
- warmup wait 不持有 model/tool/session admission permit；
- 一个 waiter 取消不取消仍被其他 waiter 需要的 leader；最后一个消费者取消时才允许终止 prime；
- coordinator key 使用安全域，不能跨 tenant 合并；
- DeepSeek `user_id` 与 key 对齐，避免逻辑共享但 Provider 物理隔离。

### 9.2 失败矩阵

| 失败 | 检测 | 恢复 | 禁止 |
| --- | --- | --- | --- |
| leader timeout | bounded deadline | key -> Degraded；一次退避后真实业务 leader | followers 同时重试 |
| Provider 低 hit | canonical usage | 对比 LCP，归因 TTL/bytes/schema/user_id | 自动改写历史再碰运气 |
| wire digest drift | pre-send oracle | 阻断 paid gate，打印首个差异 fragment | 只记 warning 继续烧费 |
| schema drift | content digest | 新 tool epoch；旧 descriptor revoke | revision 每轮自增 |
| usage missing | stream terminal | attempt usage_unknown；验收失败 | 当 0 token 或从分母删除 |
| retry after no bytes | transport state | 重发冻结 bytes，attempt+1 | 重新读 clock/context |
| retry after output | output cursor | fail/recovery protocol | 静默重复模型与工具动作 |
| fallback | provider error scope | 新 identity 编译、按剩余收益决定 prime | 继承旧 warm 状态 |
| compaction crash | durable bundle CAS | old history remains active | 内存先替换 |
| descriptor revoke | ToolHost validation | typed denied receipt + discover refresh | 依赖 prompt 中旧权限 |
| artifact unavailable | evidence read error | typed retry/degraded evidence | 用摘要假装完整证据 |

### 9.3 资源与成本背压

`CacheSloPlanner` 输出每个 planned wave 的：cold token、reusable token、预计美元、prime
回收点、最大 follower 等待和风险余量。ResourceManager 只接收一个 cache-aware admission
hint，不成为第二业务 scheduler。

当预测低于 92%：

1. 先做零模型成本的 fragment dedup、canonical ordering、schema stabilization 和 cohort factoring；
2. 若只是 cold build 秒级窗口，让同 key followers 等待；
3. 若剩余调用足以回收，允许一次 prime；
4. 只有硬窗口/载荷上限或质量评测触发时才做 lossless reference shaping，不为提高比例删内容；
5. 若仍不达标，paid harness fail-fast；生产按用户已选 cost/finish policy 继续或降级，但必须
   显示预计额外成本，绝不伪报 90%。

## 10. 源码事实图与目标改动

| 当前文件/符号 | 当前事实 | `v0.9.714` 终态 |
| --- | --- | --- |
| `conversation/prompt_assembly.rs::PromptAssembly` | string boundary，动态 system 仍在历史前 | typed fragment manifest + deterministic ordered render |
| `conversation/context_plane.rs::build_context_envelope` | clock/governance/identity 每次加入 runtime header | turn capsule 首次追加，后续 diff 追加 |
| `provider/provider_runtime_client.rs::stream_with_activity...` | contextual packets 前插历史 | 只消费已提交 ledger/history，不前插 |
| `conversation/provider_plane.rs` tool exposure | 每 step revision 自增 | digest 驱动 epoch；相同内容 no-op |
| `provider_runtime_client.rs::configure_tool_exposure` | 无条件清 schema cache | 仅 canonical schema/provider registry digest 变化失效 |
| `conversation/request_compiler.rs` | history revision 混入整体 key | static basis 与 append delta 分离；指标不再冒充 Provider cache |
| `context/context_runtime.rs::cache_stability_report` | stable head 相同即 friendly | exact wire LCP + cache-property identity；删除假阳性 |
| `provider/usage.rs` | ratio 漏掉 normalized input miss | `read/(miss+write+read)` |
| `session/session.rs` usage JSON | 同样错误 | 与唯一公式共用 helper |
| `provider/openai_compat.rs::OpenAiUsage` | DeepSeek hit/miss 解析已正确 | 保留并补 Responses cached detail/unknown usage tests |
| `conversation/compact.rs` | durable semantic summary baseline | 显式 history/cache generation receipt |
| `agent/in_process_worker.rs` | framework/role prompt 大段混合 | shared protocol fragment + compact Agent binding fragment |
| `conversation/host.rs` | 按字符串 boundary 分类 Agent prompt | 只接受 typed PromptFragment producer |
| `config-default.yaml` / `context/budget_policy.rs` | 固定 70%、last-N、比例式工具预算近似智能选择 | Provider hard window + value/evidence/quality 自适应包络；固定值仅作安全 fallback |
| `harness-contract/context` metrics | stable bytes + native read，无真实 ratio/LCP | CacheSloMetrics/AttemptUsage/InvalidationReason |
| `harness-eval/live_scenario_runner.rs` | 报 cache_tokens，无 90% gate | unique attempt 聚合 + 预测/实测硬门 |

## 11. 单版本实施计划

每一阶段先实现、运行确定性门、审计 diff，再进入下一阶段。中间 commit 可用于可恢复工作，
但只有全部阶段通过才允许一个 annotated `v0.9.714` release tag。

### Phase A — 计量真相和 Provider attempt ledger

目标：先让所有后续优化可被正确验证，不改变请求内容。

实施：

1. 新建共享 `ProviderCacheUsage`/ratio helper，修复 Runtime 和 Session 两处公式；
2. 扩展 Provider attempt evidence：logical id、attempt、cache identity、miss/write/read、usage
   status 和 wire artifact ref；
3. Harness 仅从唯一 attempt records 聚合，检测 graph projection double count；
4. 增加 `cache_waste_tokens/cost`，以 Pi 思路比较上次可复用 prompt 与本次 read；
5. 保留原始 DeepSeek 字段解析测试，增加 hit>0/write=0/miss>0 不得为 100% 的回归。

门：公式 truth table、duplicate attempt、missing usage、DeepSeek Chat/Responses usage 全通过。

### Phase B — 类型化 Prompt Ledger 和 exact wire oracle

目标：建立唯一 request truth，尚不移动业务 fragment。

实施：

1. 引入 `PromptFragment`、`CanonicalPromptLedger`、manifest/digest；
2. Provider wire evidence schema 升级，保存 model-visible canonical payload digest；
3. 实现 request property matcher 和 byte/token LCP oracle；
4. retry 复用不可变 `PreparedProviderRequest`；
5. 将旧 `prompt_cache_friendly` 删除/替换为结构指标，所有 consumer 一次迁移。

门：同输入 1,000 次渲染 byte-identical；随机 metadata 不进入 model-visible digest；任一真实
fragment 改变能指出第一个 mismatch。

### Phase C — Prompt 分层与 append-only WorldState

目标：消除 clock/context/Agent prompt 对长历史的前缀破坏。

实施：

1. 将 Agent 通用执行协议从 `in_process_worker` 长字符串提为 versioned `StableKernelV1` typed fragments；
2. 实现 `StableKnowledgePackage` manifest、适用性、来源、安全域和 deterministic common-first order；
3. Program admission 编译冻结 `ProgramDossier`，Team admission 编译 `TeamDossier`；
4. Agent binding/objective/acceptance 变成小型 epoch fragment/首条 Runtime context item；
5. `RuntimeTurnCapsule` 在 turn admission 追加一次；
6. context selection、peer/work/approval 更新改为 typed diff 追加；
7. 删除 provider 层 contextual packet 前插；
8. 删除 clock/governance id 每 request system 重建和 string boundary 推断。

门：同 Agent 100 个 tool loop 的旧 wire 是严格前缀；跨 Agent 的 shared prefix digest 相同；
clock 变化只增加 tail；相同 package 只渲染一次；授权内容仍由 Runtime 验证；StableKernel 与
旧协议做行为等价 contract test，任一协作能力不得消失。

### Phase D — 工具 schema epoch 与 GovernedDispatcher

目标：消灭每请求 schema revision/compilation，不牺牲工具能力。

实施：

1. 替换无条件 exposure revision 为 canonical content digest/CAS；
2. 相同 projection 配置成为 no-op，不清 schema cache；
3. 实现 `StableNativeEpoch` 与 `GovernedDispatcher` capability；
4. descriptor ref 绑定 principal/Binding/catalog/permission/expiry；
5. 所有原工具 schema 在 ToolHost 侧继续执行原验证；
6. one-shot required tool overlay 映射到稳定 dispatcher required descriptor；
7. compaction/capability change 是唯一 schema epoch 更新边界。

门：127 个相同 dispatcher 请求最多 1 次 schema compilation；所有现有工具 contract suite
原样通过；未授权 descriptor、过期 descriptor、参数错、effect 越权全部 fail closed。

### Phase E — 多 Agent cache singleflight 和成本准入

目标：在最大并发前先完成同前缀冷启动收敛。

实施：

1. Provider registry 增加 typed cache capability，不按模型字符串分支；
2. 实现 `CacheCohortPlanner`、prefix trie 和权限/隐私不扩大的 cohort factoring；
3. 派生安全 `user_id`/cache identity；
4. 实现 `CacheWarmupCoordinator` 状态机、waiter cancellation 和 bounded barrier；
5. 实现预测器、prime 收益判定、每 epoch 一次约束；
6. ResourceManager 释放 wait 中的模型 permit；
7. fallback/restart/TTL expiry 进入明确 invalidation reason。

门：100 followers 同 key 只有一个 cold leader；10 个不同 key 保持并发；取消/timeout/restart
无 waiter/permit 泄漏；无 prime 循环；跨 Team 公共前缀最大化但任何私有 fragment/权限不越界。

### Phase F — 全保真驻留、分级载荷与 compaction epoch

目标：默认保留完整高价值上下文，只在硬限制或实测质量退化时无损降级，让长期 session 可恢复。

实施：

1. 实现 `ContextValuePlanner` 与 Full/Anchored/LosslessReferenced/Compacted 四态状态机；
2. 将固定 70%、last-N、固定 summary/tool token cap 从智能决策降为 Provider 未知时的安全 fallback；
3. 为完整历史生成稳定目录、anchor、事实/决策/未决事项索引，不替换原文；
4. ToolHost 结果按文本价值、重复度、binary/media、硬 bytes/window 分类；相关文本默认完整回灌；
5. 只对触发硬限制或质量干扰的 payload 生成 digest/range selector，verifier 始终引用完整值；
6. compaction commit 增加 history/cache generation 和 required-fact coverage proof；
7. tool snapshot 只在 compaction/capability epoch 原子更新；
8. post-compaction 第一次请求重新建立 session history baseline，L0/L1 继续共享。

门：窗口允许时完整文本不被固定阈值裁剪；多 MB binary/重复 payload 不直接进入 prompt；分页
可重建原始 digest；压缩 crash 不丢 transcript；工具 call/result pair 不分裂；远距离事实召回、
证据覆盖、任务完成率均不下降。

### Phase G — Harness、性能和真实 DeepSeek 终验

目标：只在全部确定性证据通过后发起一次受控真实测试。

顺序：

1. fmt/clippy/check/unit/contract/property/concurrency/fault suites；
2. 离线重放失败样本 wire manifests，预测 `>=92%`；
3. 本地/fixture Provider 验证 cold leader、warm followers、retry/fallback/compaction；
4. clean candidate commit + tree freeze；
5. 运行小型真实 DeepSeek 3-request cache calibration；
6. calibration 达门后，运行一次独立 3+ Team、12+ Agent 深度协同场景；
7. 全程监控 input/output、是否重复循环、Agent/Team 活动、leader/waiter、schema digest 和
   terminal obligations；
8. 失败只按证据回到拥有该根因的阶段，不反复重复同一 paid scenario。

离线和 fixture 先以相同任务、模型参数、工具能力、随机种子/样本集跑四条对照 lane：

| lane | 上下文策略 | 用途 |
| --- | --- | --- |
| crop-baseline | 当前固定预算/last-N 行为 | 证明历史丢失与费用基线 |
| lean-stable | 稳定前缀，但只放最小必要 context | 隔离“稳定性”自身收益 |
| rich-stable | 完整相关历史/知识 + 稳定前缀，不做自适应 | 测量丰富信息的质量收益与干扰风险 |
| rich-adaptive | rich + 去重/导航/冲突检测/硬约束下无损引用 | 最终候选 |

`rich-adaptive` 必须在任务完成、事实召回、证据、工具正确率上不低于所有 lane，并在命中率、
费用或延迟至少一项严格更优；如果 `lean-stable` 质量更高，则继续修正价值选择/导航，不得靠
提高 cache ratio 宣布成功。真实 DeepSeek 只运行 calibration 和最终候选，不为四 lane 重复付费。

真实终验硬门：

- `run_cache_hit_ratio_bp >= 9000`，含 cold/prime/retry/fallback；
- warm request Provider hit `>= 9500 bp`；
- structural reuse `>= 9500 bp`；
- 对满足 qualified 条件的请求，structural reuse `>=9900 bp`，并单列 Provider 实测值；
- 同 cache key cold leader 恰好 1，schema compilation 等于实际 distinct digest 数；
- 无 padding、无 exact/near-duplicate 重复注入；有价值的 cached context 可以超过旧 token 基线；
- 任务完成，Team/Agent 数量和拓扑正确，所有 required work/review/challenge/discussion 终结；
- output materialization、证据、权限、终态和前端 projection 完整；
- 无重复空转、无 usage_unknown、无未回收 waiter/permit、无孤儿 Agent；
- `required_fact_recall=100%`、`retained_value_coverage=100%`（对 Required/HighValue）；远距离依赖、
  事实引用、分析覆盖、工具正确率和质量 rubric 不低于基线；
- `attention_interference_score` 不劣于 lean-stable lane；若 rich context 产生干扰，自适应 lane 必须
  通过去重、导航或选择恢复，而不是静默丢 Required/HighValue；
- 按当前价格，输入缓存相关费用相对该失败样本降低至少 65%；
- warm 后吞吐不低于基线；含秒级 priming 的总 wall time 退化不得超过 5%，除非省费收益证据
  证明并经版本审计接受。

## 12. 源码写入清单

实施前以此为初始 allowlist；新增跨范围文件必须先修改本节并解释依赖锥。

### Runtime/Provider

- `crates/runtime/src/conversation/{prompt_assembly,context_plane,provider_plane,request_compiler,turn_engine,evidence_terminal_plane,compact,conversation,host}.rs`
- `crates/runtime/src/agent/in_process_worker.rs`
- `crates/runtime/src/agent/agent_capability.rs`
- `crates/runtime/src/orchestration/{intent_compiler,team_authority}.rs`
- `crates/runtime/src/team/instantiation.rs`
- `crates/gateway/src/runtime/gateway_tool_executor.rs`
- `crates/runtime/src/provider/{provider_runtime_client,usage}.rs`
- 新建 `crates/runtime/src/conversation/{prompt_ledger,prompt_cache}.rs`
- 新建 `crates/runtime/src/conversation/{stable_knowledge,context_value}.rs`
- 新建 `crates/runtime/src/provider/cache_coordinator.rs`
- `crates/runtime/src/session/session.rs`
- `crates/runtime/src/context/context_runtime.rs`
- `crates/model-protocol/src/provider_config.rs`
- `crates/provider/src/types.rs`
- `crates/provider/src/providers/openai_compat.rs`
- `crates/runtime/src/provider/provider_registry.rs`

### Contracts/Harness/Surface transport

- `crates/harness-contract/src/context/mod.rs`
- Provider attempt/usage projection 的现有 contract 文件（实施审计时精确列名）
- `crates/harness-eval/src/{live_scenario_runner,report}.rs`
- `crates/gateway/src/runtime/terminal_codec.rs`（仅当已有 usage transport 需扩字段）
- 对应 tests/fixtures/generated contract consumers
- `config-default.yaml`（仅 typed policy 默认值）

### 文档

- 本文件
- `docs/architecture/README.md`
- 最终 evidence 文档 `docs/evidence/provider-prompt-cache-v0.9.714.md`

禁止创建第二 SessionHistory、第二工具权限存储、第二业务 scheduler、Provider response 内容
cache，或用 compatibility flag 永久保留旧请求布局。

## 13. 删除与 residual scan

完成前必须删除或消除：

- `SYSTEM_PROMPT_DYNAMIC_BOUNDARY` 作为缓存真相的所有生产调用；
- `prompt_cache_friendly` 的 stable-head-only 语义；
- `read/(read+cache_creation)` 两处及所有复制公式；
- 每 Provider step 无条件 `tool_exposure_revision.fetch_add`；
- 相同 tool projection 无条件 `tool_schema_cache=None`；
- Provider request 前动态 contextual packet 插入旧 history 之前的路径；
- retry/fallback 重新读取 clock/context 的路径；
- 任何为命中率 padding prompt、排除 cold/prime/retry、graph usage double count 的逻辑；
- 只打印 `cache_tokens` 而无 hit/miss/ratio/applicability 的 acceptance 报告。

建议 residual commands：

```bash
rg -n "SYSTEM_PROMPT_DYNAMIC_BOUNDARY|prompt_cache_friendly" crates
rg -n "cache_read.*cache_creation|cache_creation.*cache_read" crates/runtime crates/gateway
rg -n "tool_exposure_revision.*fetch_add|tool_schema_cache.*None" crates/runtime
rg -n "prompt_cache_key|previous_response_id" crates/runtime crates/provider
```

最后一条不是要求结果为空，而是审计不得错误声称 DeepSeek 支持这些参数。

## 14. 测试矩阵

| 维度 | 场景 | 预期 |
| --- | --- | --- |
| 计量 | hit=3328, miss=7101, write=0 | 3191 bp，不是 10000 |
| 计量 | graph 同 leaf usage 多层投影 | attempt ledger 只计一次 |
| 计量 | stream 无 usage | usage_unknown，验收失败 |
| 稳定性 | clock/turn/governance 变化 | 旧 wire prefix 不变，只 append delta |
| 稳定性 | 同内容不同 map 插入顺序 | canonical bytes/digest 相同 |
| 工具 | revision 变、schema bytes 不变 | cache epoch/schema compilation 不变 |
| 工具 | descriptor revoke | prompt 可旧，ToolHost 仍拒绝 |
| 工具 | required one-shot | dispatcher required + exact descriptor 验证 |
| 并发 | 100 cold same-key Agents | 1 leader，99 bounded followers |
| 并发 | 10 warm distinct keys | 并发宽度不下降 |
| 恢复 | leader cancel/timeout | 无泄漏、无 stampede、一次 typed recovery |
| retry | EOF before any output | 相同 model-visible digest，attempt+1 |
| retry | EOF after output | 不静默 replay |
| fallback | DeepSeek -> other provider | 新 identity，不继承 warm，canonical facts 不重读 |
| 压缩 | compaction commit/crash/restart | 原子 generation，旧或新之一，无混合 |
| 输出 | 10MB tool result | artifact full，prompt bounded，可 range 重建 |
| 驻留 | 相关 80K 文本且窗口充足 | FullReplay 原文完整、索引附加、无固定 last-N 裁剪 |
| 驻留 | Required 事实位于早期历史 | 多轮/compaction 前后 100% recall 和 evidence binding |
| 干扰 | 注入大量可缓存但无关 corpus | 即使 hit 上升也必须因质量/相关性 gate 失败 |
| 对照 | crop / lean-stable / rich-stable / rich-adaptive 四 lane | rich-adaptive 在质量优先 Pareto frontier 胜出 |
| 权限 | 两 tenant 同 prefix | 不共享 cache domain/user_id |
| TTL | expired warm state | 新 leader，明确 expired receipt |
| 质量 | 原生工具 vs dispatcher | 完成率/参数正确率/证据 rubric 不退化 |
| 真实 Provider | calibration + deep collaboration | 全程 >=90%，warm >=95% |

## 15. 能力不退化清单

| 既有能力 | 保护机制 | 证明 |
| --- | --- | --- |
| 多 Team/多 Agent 最大并发 | 仅 same-key cold window 单飞 | warm/distinct-key concurrency tests |
| Agent 主动工作市场 | work events append-only | autonomous market suites |
| 多 Skill/Tool 协同 | descriptor discovery/invoke，不删工具 | complete tool catalog matrix |
| 权限/审批/沙箱 | ToolHost 仍是唯一执行 authority | negative permission/effect tests |
| 模板自定义/任意角色名 | binding fragment 数据化 | arbitrary Chinese/custom role tests |
| Peer discussion/review/challenge | typed event delta 追加 | terminal obligation suites |
| 长任务与 compaction | durable generation boundary | restart/fault/long-session tests |
| 证据与长期记忆完整性 | FullReplay 优先；必要时 artifact full + 可逆 view | recall/reconstruction/verifier tests |
| Provider fallback | destination-local compile | failover matrix |
| UI/报告可解释性 | ratio/applicability/invalidation/leader receipts | contract/projection tests |

## 16. 实施前严格审计

### 16.1 已接受的设计

- 接受“typed ledger + append-only delta”作为唯一模型上下文模式；
- 接受“Provider actual usage 为 90% 真相，local LCP 只做结构证据”；
- 接受“same-key cold singleflight，不降低全局/不同 key 并发”；
- 接受“稳定 governed dispatcher + Runtime 二次验证”解决工具 schema churn；
- 接受“compaction/capability change 是显式 epoch boundary”；
- 接受“FullReplay first、lossless reference second、compaction last”保护能力与完整证据；
- 接受先确定性验证、后一次 calibration、再一次深度付费终验。

### 16.2 明确拒绝的方案

| 被拒方案 | 原因 |
| --- | --- |
| 只修 ratio 展示 | 不改变 3.77% 的真实费用 |
| 只移动 clock | Agent prompt、context 前插和 schema churn 仍破坏前缀 |
| 给所有 Agent 暴露全工具 superset | 扩大模型可见权限、增加 tokens、质量噪声 |
| 全局限制并发为 1 | 牺牲框架能力，且 warm 后没有必要 |
| 每个 Agent 单独 prime | 放大 cold cost，违背 shared prefix 目标 |
| 排除 cold/prime/retry 后报 90% | 指标作弊，不能解释真实账单 |
| 填充重复无用 prompt | 降低注意力质量并虚增分母 |
| 因为可缓存就注入所有历史/语料 | cache 降成本但不消除 context window 与注意力干扰 |
| 固定 70%、last-N、2K summary 作为智能策略 | 与任务价值、实际窗口和证据义务无关，会静默丢能力 |
| 以 prompt token 不得超过旧基线 105% 为门 | 惩罚有价值的稳定上下文，和质量提升目标冲突 |
| 依赖 `prompt_cache_key` | DeepSeek 明确不支持 |
| 依赖 `previous_response_id` | DeepSeek Responses 明确无状态 |
| 缓存模型输出作为本地答案 | 改变语义/新鲜度，不是 Provider prompt cache |
| 每次工具变化重建 system | 把可追加状态重新变成前缀失效 |

### 16.3 风险审计

| 风险 | 控制 |
| --- | --- |
| dispatcher 降低工具选择质量 | bootstrap compact descriptors、schema-on-demand、与 native A/B rubric |
| prime 产生无效模型调用 | 收益预测、每 epoch 一次、计入分母、优先真实 leader |
| Provider best-effort 波动 | 95% 结构目标、92% 预测 headroom、90% 实测硬门 |
| append-only 导致 context 增长 | 去重/导航 -> lossless refs -> 最后才 durable compaction；required fact coverage gate |
| rich context 引起注意力干扰 | 四 lane A/B、相关性/冲突检测、adaptive value planner；缓存率不能覆盖质量失败 |
| StableKernel 硬编码僵化 | 仅硬编码协议不变量，版本/digest 管理；业务目标/权限/策略保持数据化 |
| 动态权限在 frozen prompt 中陈旧 | ToolHost 每次验证，revoke 不依赖 prompt 更新 |
| coordinator 变成第二 scheduler | 只拥有 cache-key wait，不拥有业务 readiness/priority |
| restart 误认为仍 warm | 恢复为 Unknown/Expired，靠新 usage 建立事实 |
| 统计重复 | ProviderAttemptLedger identity 去重，projection 只引用 |

### 16.4 参考实现吸收完整性审计

| 精华/约束 | 来源 | Cowd 落点 | 不照搬或增强点 | 验收证据 |
| --- | --- | --- | --- | --- |
| 历史严格追加 | Pi/Codex | Canonical Ledger + RuntimeTurnCapsule | 扩展到 peer/work/evidence/多 Agent 事件 | 100 tool-loop strict-prefix property test |
| 请求属性完整比较 | Codex | PreparedProviderRequest + property digest | DeepSeek 无 continuation，仍做 wire LCP | 任一属性变更准确指出 invalidation |
| stable session/cache affinity | Pi/Codex | ProviderCacheIdentity/capability adapter | 不假设 DeepSeek 支持 cache key | Provider matrix + wrong-capability negative test |
| system/tool snapshot 冻结 | Pi/Hermes | StableKernel/KnowledgePackage/tool epoch | 从单 Agent 扩展到 Program/Team cohort | restart byte identity + distinct digest compile count |
| 显式 compaction boundary | Pi/Codex/Hermes | history/cache generation CAS | FullReplay first，压缩最后手段 | crash/restart + required-fact coverage |
| 正确 cache read/miss 计量 | Pi | ProviderAttemptLedger | cold/prime/retry/fallback 全计入，不重置主分母 | frozen 3.77% sample + truth table |
| 大型共同语料进入缓存 | OpenAI/Anthropic/Gemini | ProgramDossier/reference corpus | 适用性/权限/去重/质量 gate | rich/lean/crop/adaptive 四 lane |
| 动态工具不破坏前缀 | Anthropic/Codex | StableNativeEpoch/GovernedDispatcher | 保持 ToolHost 原权限与 schema 校验 | 完整工具 contract/negative matrix |
| 跨 Agent 冷启动收敛 | Cowd 增强 | CacheCohortPlanner + singleflight | Pi/Codex 单 session 未解决的场景 | 1 leader + 99 followers + distinct-key concurrency |
| 99% 可比稳态能力 | 数学模型/Pi 暖态展示 | qualified structural gate | 与 cold-inclusive 90% 分开，拒绝口径作弊 | 同报告双口径 + raw Provider usage |

审计结论是：原方案已经吸收了 append-only、cache identity、工具 epoch、singleflight 和真实
计量，但对“高价值稳定信息越充分，缓存越能同时降成本和提效果”的吸收不完整，且固定预算、
大结果默认摘要、105% token 门可能造成能力退化。本次修订已经删除这些冲突设计，并加入
StableKernel、Program/Team Dossier、StableKnowledgePackage、CacheCohortPlanner、FullReplay
first 和质量优先四 lane；这是实施时必须一次完成的架构范围，不是可选优化。

### 16.5 审计结论

方案通过实施前架构审计，条件是实施严格保持以下不变量：

1. 缓存层不拥有业务状态和终态；
2. Provider attempt 是费用唯一事实源；
3. 所有动态变化 append，不重写已发送前缀；
4. 工具可见性和权限分离，dispatcher 不能扩权；
5. same-key 只在 cold build 窗口单飞，warm 后恢复最大并发；
6. 90% 包含 cold/prime/retry，真实 Provider 未达门不得封板；
7. 不以字符预算、padding、减少业务证据或缩短任务换命中率；
8. 未通过确定性审计前不得重复付费深测。

补充能力不退化不变量：

9. 默认完整保留相关高价值上下文；缓存成本不是 compaction/裁剪触发器；
10. `StableKernel` 只能固定框架协议，不固定业务策略、并发、工具权限或 Agent 自主决策；
11. 任一 99% 声明必须同时报告 cold-inclusive 与 warm-epoch 两种口径；
12. rich-adaptive 候选必须通过 crop/lean/rich-static 对照，不能只凭命中率自证效果。

任一实现若引入第二历史、第二工具 authority、全局串行、usage 排除、prompt padding 或
Provider 专用散落分支，本方案自动判定为审计失败，必须在运行真实模型前返工。

### 16.6 实施中方案—代码一致性审计

本节记录实现事实与原计划的差异，防止“文档说完成、源码只有名字”。最终真实验收前持续更新。

| 原计划能力 | 当前落点 | 审计结论 |
| --- | --- | --- |
| 正确 cache ratio | `model-protocol::TokenUsage` 唯一公式，Runtime/Session/Harness 调用 | 已实现；miss/write/read 全入分母 |
| Provider attempt truth | exact wire request + `ProviderAttemptOutcome`，以 request id 关联 usage/unknown | 已实现 Runtime durable evidence；Harness 同时拒绝 unknown |
| typed stable/runtime 分层 | `PromptAssembly.trusted_system/runtime_context/contextual_packets` | 已实现 Provider 边界类型化；旧字符串 marker 仅保留上游兼容装配，不再是 cache truth，后续 residual 以“不得越过 typed boundary”验收，不为删除常量大改公开构造 API |
| append-only history | `ProviderPromptHistory` 保留上次真实 wire capsule，retry byte-identical，后继只追加 | 已实现；compaction/branch 明确换 generation |
| exact LCP oracle | `ProviderTransportPool::observe_prompt_prefix` | 已实现；仅结构证据，不冒充 Provider 计费命中 |
| schema epoch | model-visible exposure digest + registry revision；相同 projection no-op | 已实现 `StableNativeEpoch` 路线 |
| GovernedDispatcher | 原计划与 stable native 二选一仍写成“两者都做” | 审计后不强行上线：当前工具 schema churn 已由 content epoch 消除，dispatcher 会改变模型工具选择语义；只有真实 calibration 证明 native schema 仍阻断 90% 才作为独立版本方案，不在本版冒险替换所有工具协议 |
| cold singleflight | stable cohort digest + two-stage DeepSeek common-prefix discovery + cancellation | 已实现；不同 key/warm 保持并发，等待不占 HTTP permit |
| 并发唤醒与前台性能 | Notify 预注册 + 确定性 waiter 屏障；维护投影按前台 commit 活性自适应让步 | 全回归曾暴露 follower 丢唤醒窗口并已根修；release 成组探针又证明仅靠 200/300ms 固定节拍都会在约 570ms burst 内竞争 SQLite writer。最终保留空闲时 200ms×128（约 640 commits/s），pass 前要求 50ms 前台静默，连续忙时最多延迟 1s 后强制执行一批，兼顾前台性能与无饥饿追赶 |
| FullReplay/value planner | 现有 utility/context envelope + 完整 history 优先，硬窗口才 pack | 本版不创建第二历史或第二 planner；以 required-fact、完整证据和现有 compaction 回归验收 |
| 跨 Agent 公共知识 | versioned universal collaboration kernel + immutable Program dossier + Team fragment置于私有 binding 前 | 已实现；业务目标/权限/Agent 绑定仍是动态数据，不硬编码 |

审计决定遵循最小机制原则：能由既有唯一状态真相安全扩展的，不另建平行 scheduler/history；会
改变模型工具语义而当前根因不要求的，不为了“勾选计划”强上。删除这两类过度设计不会降低缓存
目标，反而减少回归面。真实 DeepSeek 如果证明此判断错误，证据必须明确指出 schema epoch 的
miss 占比，再回到 Phase D，而不是无证据扩权。

## 17. 版本关闭与回滚

关闭顺序：

1. 完成 Phase A–F 与所有确定性门；
2. residual scan、依赖锥、完整 workspace tests、architecture/governance gate；
3. 从当前工作树生成隔离、不可变且可哈希的临时候选并记录 commit/tree；不得借此修改主仓库历史；
4. calibration 通过后只运行一次深度真实 Provider 验收；
5. 生成不可变 evidence report，确认 90%/质量/并发/终态全部通过；
6. 安装新包并以安装态运行 smoke/doctor；
7. 得到明确发布授权后才应用 `commit-version-gate`、更新正式版本/evidence、创建 annotated tag
   `v0.9.714` 并按授权 push/sync 分支；无授权时把这些项目明确留在未发布账本，不能伪造通过。

回滚以完整 `v0.9.713` release commit/tag 为单位，不保留运行时双布局开关。cache epoch 和
attempt evidence 是附加治理数据；旧二进制忽略未知 projection 时不得破坏 Session/Program
业务事件。若 public schema 需要升级，所有 consumer 必须同版本迁移并在 mismatch 时明确
resync/fail closed，禁止静默部分降级。

## 18. 真实 Candidate 1 失败证据与跨层修复增补

### 18.1 为什么主动终止，而不是继续花费

2026-09-01 以 DeepSeek `deepseek-v4-flash` 启动了本方案第一次不可变候选真实复验：

- candidate commit `ad3527805bb6ecbe6e5260387d04fd2aa2cd4ac7`，tree
  `78050a41e7ec68beebd59e4f23971aafcd212318`；
- Session `f6133e5b-e216-4ce5-ad20-af229a5deaf5`；
- root execution `session-ingress-graph:76c012d2e715fecc1bb1ef898e15177b`；
- 运行目录 `/tmp/cowd-real-qwen-gateway.JQ2wss`；
- 场景仍是 4 Team × 4 Agent 的群论在 AI 中应用研究、实验、红队和 HTML 交付，没有降低难度。

候选运行到 Runtime cursor 3335 时，A/B/C 三个首波 Team 已真实创建，12 个 Agent 身份均存在，
Team B 已完成，Team A/C 继续运行；过程产生 90 余次工具调用、8 次以上自治 checkpoint、Team
working-state 发布/消费以及真实 bid/claim/submit。它证明自治执行不是单节点假图，也不是因
DeepSeek 没有发出工具调用而停止。

但是 Team B 三个网络研究角色都以独立 receipt 证明其最终工具目录只有
`tool_search/collaboration_control/context_retrieve/glob_search/grep_search/read_file/team_board`，
没有 `web_search/web_fetch`，其 `sources=[]` 并按契约上报 blocker。此时继续运行不可能生成
合格来源，也不可能让 Team D 交付满足业务门，因此在 93 个已完成 Provider attempt 后主动
终止，避免把确定性失败继续变成费用。终止点 usage 全部 known：miss 2,685,710、cache read
3,521,024、output 246,719，阶段性 cold-inclusive provider hit 为 56.7291%。该比例只作为失败
诊断，不能冒充最终全程结果。

### 18.2 端到端反向证据链

该缺陷沿完整链条可复现，不能归因于模型：

1. AI authored role 正确声明 `required_capabilities=[network,read,search]`；
2. intent compiler 正确把不可变角色 contract 编译为含 `web_search/web_fetch` 的
   `allowed_tool_contract_refs`；
3. Gateway 的物理 ToolHost 确实注册这两个网络工具，host inventory bind 没有缺口；
4. 同一个 Team 还包含 `[read,search,write,test]` 的终态汇总角色；
5. `team_authority_profile` 把整个 Team 的多维需求压缩为互斥枚举
   `WorkspaceRead | ExternalResearch | WorkspaceWrite`，写能力覆盖了网络能力；
6. Team 只得到 `read:. + write:... + session:...`，丢失 `network:*`；
7. `crop_tools_to_resource_lease` 按真实租约正确裁掉 `web_search/web_fetch`；
8. Agent Binding 因而出现“effective_capabilities 含 network、tool_contract_refs 不含网络工具”的
   语义裂缝，直到模型运行后才以 blocker 暴露。

同一反向审计还发现第二个必败点：用户明确要求的根目录新文件
`group-theory-ai-autonomous-evaluation.html` 是带扩展名的合法相对文件，但
`explicit_workspace_paths` 的前置过滤只接受绝对路径、`./` 或包含 `/` 的 token，导致后续本已
支持“带扩展名 planned artifact”的校验永远看不到该文件。路径启发式随后被“group/theory”
等业务词误导到既有源码目录，给出 `write:crates/fact-*`。因此即使网络问题单独修好，最终发布
角色仍无法落盘用户指定文件。这同样是 authority producer 的数据丢失，不应通过放开全工作区
写权限绕过。

主服务按用户授权直接替换后曾暴露旧数据恢复冲突：旧版本已经为同一终态 Program revision
持久化 `collaboration-experience:<episode-id>`，当前 projector 生成的 payload 字节不同，
EventStore 因“同 transaction id、不同 request hash”正确拒绝。曾评估 first-valid-commit-wins
兼容层，但用户明确决定历史数据无需兼容，应清空后建立新基线，避免长期演化负担。因此该兼容
代码已撤销，不进入产品；本版本只保留一次性、明确范围的数据重置证据，新数据仍执行严格 hash
冲突保护。

根因是框架将集合型资源需求错误建模成互斥分类（lossy state compression）。工具裁剪本身是
正确的 fail-closed 防线，不能删除；正确修复点在 authority producer 和 admission invariant。

### 18.3 第一性原理修复设计

资源授权必须满足下面的集合关系，而不是选择一个“最强类别”：

`TeamLease = union(required_effect_scopes(each immutable role)) ∩ SessionAuthority`

`AgentLease(role) = TeamLease ∩ role capabilities ∩ focus/evidence scopes`

`AgentTools(role) = HostTools ∩ capability tools ∩ tools executable under AgentLease(role)`

据此一次完成以下修复：

1. 用正交的 `requires_workspace_write`、`uses_external_network` 需求位替代互斥
   `TeamAuthorityProfile`；一个异构 Team 可以同时为 true；
2. 对 ephemeral/custom Team，从已经通过 intent compiler 和定义校验的不可变 role
   `grant_ceiling` 直接求需求并集；只有不存在冻结模板事实时才回退全局 task understanding；
3. `custom_team_resource_scopes` 对 declared evidence scopes、Runtime 派生的 bounded workspace
   scopes 和 `network:*` 做 canonical union，不再用任一分支覆盖另一分支；
4. 修复明确的 bare relative planned file 识别：仅在 write 场景接受安全、ASCII、无父目录跳转且
   带合法扩展名的单文件 token，并继续经 workspace containment 和 action-clause 判定生成精确
   `write:<file>`；
5. 保留 role capability 裁剪：网络角色没有 write tools，writer 没有 network tools；Team 联合租约
   不等于每个 Agent 联合扩权；
6. 在 Team 实例化前新增 effect-capability fulfillment invariant：任何声明 network/write/test 的
   非 upstream-only 角色，都必须在最终 `resource_scopes + allowed_tools` 中至少有一个可执行
   物理 effect tool，否则在 Provider 调用前 fail closed；
7. 继续保留 Gateway host inventory bind 和 in-process activation drift check，形成
   compile → authority → instantiate → activate 四道一致性防线。
8. 不增加 terminal experience 的跨版本兼容分支；清理历史 Session/Runtime/Projection/Memory
   业务数据后以当前 schema 建立空基线，模型/provider/APP 配置、token plan、嵌入配置、安装包与
   源码不在清理范围。

### 18.4 安全、并发、恢复和回归审计

| 风险 | 设计控制 | 验收证据 |
| --- | --- | --- |
| Team 联合租约导致单 Agent 扩权 | capability 工具交集先于 resource crop；禁止 network+write 的角色契约不变 | 异构 Team 测试逐角色断言网络研究员无写工具、writer 无网络工具 |
| 模型伪造 `network:*` | 网络需求只能来自通过 catalog/ceiling 校验的冻结 role grant；model scope 只可收窄具体证据 | 非 network role 即使模型写 scope 也拿不到网络工具 |
| declared scope 意外删除必要 effect | required effect scope 由 Runtime union，declared scope 不能抹去已承诺 capability | declared read + network role 仍有 network；无 network role 不增加 network |
| 只修当前 Team B | 不按 role id、Team 名或中文关键词分支；对任意 custom/ephemeral heterogeneous Team 生效 | property/table tests 覆盖 read、network、write、network+write 四组合 |
| 激活后才发现假 capability | 实例化前验证最终物理工具和 scope | negative test 在任何 Provider attempt 前报 typed diagnostic |
| 并发或缓存退化 | authority 纯本地、确定性集合计算，不加锁、不做 I/O、不进入 prompt 动态前缀 | 全 workspace、并发/performance gate、exact-wire/cache calibration 重跑 |
| 恢复后权限漂移 | template snapshot、Team lease、Binding digest 都持久化；恢复沿同一编译函数重建 | serialize/replay 与 agent prepared snapshot 断言 |

审计结论：修复必须改 authority 数据模型并增加闭环 invariant；仅在场景模板中显式写
`evidence_scope:network:*`、把 Team B writer 拆出、给 Agent 直接塞网络工具、或降低来源验收，
都只是规避并会留下同类异构 Team 缺陷，全部拒绝。

### 18.5 修复后执行与付费重跑门

严格顺序如下，禁止边测边改真实候选：

1. 落实正交需求、scope union、最终 effect fulfillment 及单元/集成测试；
2. 运行 formatter、check、Runtime/Gateway/Contract/Harness 全量、architecture/residual、并发与
   performance 门；
3. 重跑 3-request DeepSeek calibration，暖态 provider/structural 仍须 `>=95%`；
4. 用包含所有 tracked 与新增文件的新临时 index 生成真正完整的 candidate tree，校验候选源码、
   构建二进制和报告三者 digest 一致；
5. 仅在上述全部通过后，重跑同一个 4×4 群论真实场景；必须观察到 Team B 网络角色真实调用
   `web_search/web_fetch` 并取得 source receipts，A/B/C handoff 后 Team D 启动，最终 HTML 落盘、
   重读并有 digest receipt；
6. 最终同时验收全程 provider hit `>=90%`、warm `>=95%`、unknown usage=0，以及 4 Team、16
   Agent、24 WorkItem、12 claim、3 challenge、2 review、1 challenge-driven revision 的业务门；
7. 隔离 Gateway 的真实 E2E 只证明后端链，不能证明用户实际界面。后端候选通过后，必须让最新版
   测试服务承载同一类真实 Session，以浏览器级 E2E 验证前端连接的 Gateway 中 Session 可见，且
   Session/Execution id 与 API、Runtime event store 一致；逐项断言 Program、4 Team、16 Agent、
   running/completed 状态、WorkItem/claim/challenge/review、working-state 消息、typed handoff、成本/
   cache 指标、终态和 persisted artifact 均有正确展示。不得用截图里的单节点、mock 数据或仅 API
   响应替代 UI 验收。

Candidate 1 是被替代的失败证据，不得改写成 passed；只有 source changed、全量确定性门通过且
新 candidate identity 不同，才允许第二次真实付费运行。

### 18.6 空基线重置与安装态生命周期闭环

用户于 2026-09-01 明确授权删除历史数据、拒绝兼容负担。执行前确认 PostgreSQL 数据库
`cowd_primary` 的 `public` schema 共 84 张用户表，全部为 Cowd 所有；随后停掉主 Gateway，并
执行精确重置：

- 保留 `cowd_schema_catalogs`、`cowd_schema_migrations` 共 63 行 schema 元数据；
- 清空其余 82 张 Session、Runtime、Projection、Surface、Memory、Knowledge、Matrix、Fact、
  Artifact、Growth 与 Connector 业务表，共删除 95,558 行，逐表复核总剩余 0 行；
- 删除 `~/.cowd/runtime/workspaces`、`~/.cowd/memory`、`~/.cowd/storage/blobs` 中的历史文件，
  并删除本地 session/memory/knowledge/matrix/fact SQLite 及 WAL/SHM 残留，复核均为 0；
- 不修改 `config.yaml`、storage cutover 清单、Provider/模型/token plan/嵌入配置、密钥、APP
  安装配置、二进制和源码。

该动作建立不可从 Cowd 自身恢复的空业务基线，正式拒绝旧事件 payload 的跨版本兼容。严格
transaction hash 冲突检测继续保留；任何恢复冲突都必须被视为当前数据或投影不一致，而不是由
first-valid-commit-wins 吞掉。

替换主服务时还暴露出安装态生命周期的双 authority：安装器创建带 `Delegate=yes` 的用户
systemd unit，并通过 `run-cowd-gateway` 建立 APP 子 cgroup；但 CLI 的 `gateway start/stop/restart`
直接 spawn/signal 二进制，绕过 wrapper，导致安装态启动在 cgroup admission 处失败。修复约束为：

1. 默认 config home 且 `cowd-gateway.service` 已加载时，CLI start/stop/restart 统一委托
   `systemctl --user`，并等待 Cowd `/readyz` 成功后才报告 start/restart 完成；
2. 设置 `COWD_CONFIG_HOME` 的隔离测试和显式 `COWD_GATEWAY_DIRECT_LIFECYCLE=1` 继续走原来的
   进程级路径，不得触碰主服务；
3. systemd action 失败时 fail closed，不静默回退到第二套直启 authority；
4. 最终安装态验收必须真实执行 CLI stop/start/restart，并验证 service PID、readyz 与 APP cgroup，
   不能只直接调用 systemctl 掩盖 CLI 缺陷。

## 19. Candidate 2 真实负载证伪与最终修正案

### 19.1 结论先行

受控三请求校准曾得到 warm provider cache `99.28%`、warm structural reuse `99.55%`，但这只证明
单一客户端、单一 schema、线性 append 场景正确，不能外推到多 Agent 分支。随后在主 Gateway、
主 PostgreSQL、主 WebUI 上运行同一类 4 Team × 4 Agent DeepSeek 场景，Session
`a11d68db-b2ca-4d3c-bcf9-90a954967cce` 的 101 个模型节点中 100 个完成、1 个在安全停机时运行；
唯一 usage 口径汇总为：

| 指标 | 真实值 |
| --- | ---: |
| miss input | 3,300,355 tokens |
| cache-read input | 3,524,352 tokens |
| output | 330,649 tokens |
| cold-inclusive provider hit | **51.64%** |
| Provider request evidence | 96 |
| known outcome evidence | 95；最后 1 个随安全停机中止 |

因此“缓存已发挥预期作用”被真实证据否定：缓存部分生效，但远未达到 90%。运行已立即停止，主
Gateway 也停止，避免失败架构继续产生费用。该失败不得用 99.28% 校准数字覆盖。

### 19.2 精确根因

直接关联 96 份 exact-wire artifact、`ProviderRequestPacked` 和 `ProviderAttemptOutcome` 后得到：

1. 96/96 请求均记录 `cache_cold_leader=true`；同一 identity 的第三、第四乃至第三十四次请求仍
   反复成为 cold leader，违反“DeepSeek 每 identity 最多首请求与一次 common-prefix discovery”
   的状态机不变量。
2. Provider 已先持久化 `terminal_status=completed` 和 known usage，但生产任务随后仍未完成。原因是
   `forward_provider_stream` 先向消费者发送 `MessageStop`，Conversation 收到终态后 drop
   `ProviderEventStream`，其 Drop 调用 `reap_join_handle` 立即 abort producer；producer 当时正在
   执行 DeepSeek 2 秒 persistence barrier，abort 触发 warmup guard 的失败 Drop，将状态重新置为
   Cold。事件时间也证明 outcome 到下一 leader 只有约 80ms，不可能经过 2 秒屏障。
3. 单一真实 Session 形成 8 种 model-visible tool schema、16 种 cache identity。工具集合随
   bootstrap、discovery、one-shot mutation、text-only terminal 在 0/1/2/5/6/7/9/10 个 schema 间
   反复缩放。0-tool 请求 provider hit 仅 14.43%，1-tool 仅 17.50%；唯一稳定的 6-tool cohort 有
   34 请求，仍因上述终态 abort 只有 75.65%。这证明 schema churn 是第二主因。
4. delegated Agent 的 objective、workspace、resource scopes、definition、acceptance 和工具契约
   都由冻结的 `AgentTaskPacket/Binding` 决定，在一个 Agent host 生命周期内不可变，却位于 dynamic
   boundary 后；每次模型迭代都会再次作为 runtime capsule 追加。真实 prompt 因而增长到约 938KB，
   既重复付费又让分支历史在共享 system 后过早分叉。
5. 安全停止还暴露取消语义错误：in-process backend 已接受 cancellation token 的责任，却把“10 秒
   内全部清理完成”误当成 command accepted 的前提；超时返回 `UnsupportedByBackend`，导致整个
   Session cancel cascade 报错。取消意图提交与异步清理完成必须是两个事实，不能同步耦合。

### 19.3 经审计的原子修复

本次不以增加等待、减少 Agent、裁剪上下文或 padding 修数字，而原子实施四项源级修复：

1. **Provider terminal commit barrier**：上游 `MessageStop` 只证明 Provider terminal；先持久化
   outcome、完成适用的 cache persistence barrier、提交 warm state，最后才把 Runtime
   `AssistantEvent::MessageStop` 发布给消费者。取消发生在 Provider terminal 前仍 abort；terminal
   后的已提交缓存状态不得被消费者 Drop 回滚。
2. **Delegated immutable lane + cohort boundary**：Program dossier 以前是跨 Program 公共稳定段；
   Program dossier 是同一执行树共享段；Team/Agent binding、objective、scope、acceptance 是执行
   实例稳定段；只有 clock、最新 Team board、memory/fact/handoff 增量是 request-local。新增显式
   cache-cohort boundary，由 typed `PromptAssembly` 消费，marker 不上 wire。共享 cohort 用于跨
   Agent singleflight，完整稳定系统仍按原顺序发送，权限和信息边界不变。
3. **Delegated stable schema superset**：delegated host 构造时的 `tool_definitions` 已是
   Binding capability、物理 Host 和 resource lease 的交集，故将该有界集合固定为此 Agent 生命周期
   的 model-visible schema；每轮 `ToolExposureProjection` 仍是唯一逻辑执行租约，Runtime 继续拒绝
   overlay 外调用。schema 可见性不再等于执行授权，固定 schema 不扩权、不绕过 ToolHost。
   root/大目录客户端继续使用动态 schema，避免无界 MCP catalog 污染上下文。
4. **异步、幂等取消**：backend 一旦把 cancellation token 交付给 active run，command 即 accepted，
   AgentRuntime 立即提交 Cancelled；实际 unwind、receipt 保留和 active-map 清理由原执行 owner 异步
   完成。已 terminal 的重复 cancel 返回幂等成功；只有持久化 Running 快照却没有活动 backend handle
   的孤儿运行才 fail closed 为 `unsupported_by_backend`，不遗留无人消费的 pending tombstone，也不再
   用 10 秒同步等待把慢清理伪装成“不支持取消”。

附带但独立的真实 E2E 缺陷同版本修正：PostgreSQL `SUM(BIGINT)` 的 NUMERIC 解码显式 cast 回
BIGINT；artifact activity 按 producer execution scope 投影而不是向每个祖先 namespace 复制；
WebUI observer header 由每个 tab 自己绑定；Mission Team label/agent count 使用 canonical control
projection。这些修复均已有定向回归，不参与缓存分母。

### 19.4 审计门与停止条件

实现后必须先通过 deterministic 门，再发生任何付费复测：

- terminal ordering 测试证明第三个同 cohort 请求不再是 cold leader，且消费者收到 MessageStop
  时 warm phase 已提交；terminal 前取消仍回滚 cold；
- 16-Agent 等价请求编译测试证明 delegated schema 每 Agent 恒定、logical overlay 仍 fail closed、
  immutable assignment 不在 runtime capsule 重复出现、marker 不出现在 wire；
- cancel 命令延迟为本地确定性路径，不等待 Provider，最终 run cleanup 无泄漏；
- Runtime/Gateway/Session PostgreSQL/Harness/Contract/Frontend 全量与 build 全绿；
- 先跑小规模 DeepSeek E2E canary，只验证 cold leader 上界、schema identity 数和终态；任一失败立即
  停止，不扩大任务；
- 最后才重跑冻结的 4×4 真实场景。全程 provider hit `>=90%`、warm `>=95%`、unknown=0、
  任务/产物/Team/Agent/讨论/验收全部终态完成，并由实际安装 WebUI 可见，方可宣布完成。

审计判断：四项修复分别拥有 terminal lifecycle、prompt authority、tool execution authority 和
Agent lifecycle 的既有唯一真相，没有创建第二历史、第二权限系统或全局串行器；它们共同解释
校准与真实负载差距，且每项都有可证伪的源级验收，因此允许进入实施。

## 20. Candidate 3/4：ContextEnvelope 边界修复与控制平面自治修复

### 20.1 Candidate 3：校准通过但多 Agent identity 仍被拆分

Provider terminal barrier、delegated stable schema、immutable prompt lane 和异步取消落地后，三次
DeepSeek Flash 受控校准得到 warm provider hit **99.37%**、cold-inclusive provider hit
**97.26%**、warm structural reuse **99.38%**，3 次 usage 全部 known，证明 Provider 缓存与
terminal warm commit 在稳定线性请求上真实生效。

随后真实三 Team canary（Session `88390209-6710-45bd-b169-0649735e7b24`）在 11 个请求时因
provider hit 约 **50.75%** 被安全停止。exact-wire 对照证明 Team A/B 具有相同 Provider、模型、
transport、7 个 tools、schema/property 顺序与 7,562 bytes 公共 wire 前缀，却形成 6 个 cache
identity。根因不是 Provider：`PromptAssembly` 已解析 cache cohort boundary，但
`ContextEnvelope.assembled.stable_head` 没有携带 `cache_cohort_segment_count`；Provider 重建时使用
`PromptAssembly::new(stable_head)`，把角色专属稳定尾部误当成共享 cohort，破坏跨 Agent identity。

修复将 boundary 作为 typed `AssembledContext` 和持久化 render manifest 的一部分；root 构建默认
整个 stable head，delegated context 显式继承 parent boundary，Provider rebuild 必须使用同一计数。
新增回归证明 envelope round-trip 后 identity 不变、marker 不出 wire。Runtime 2020/2023（3
ignored）、Gateway 814/827（13 ignored）、Contract、Protocol、Harness 与 workspace all-target check
通过。

### 20.2 Candidate 4：identity 在线修复成立，但隐藏冗余契约造成昂贵重试

替换主服务后的下一次三 Team canary（Session `f11569c0-11f1-4a33-b694-77032f54714b`）证明上述
修复已在线生效：首批规划请求全部使用同一 cache identity
`sha256:19063c...`，仅首请求为 cold leader，后续请求复用同一前缀。运行在 4 个付费请求处停止，
因为 Teams 尚未创建而 root 控制平面已经产生超过 26k output tokens；继续运行只会扩大费用。

持久化 `model.item_completed` 的完整工具参数反向证明，两次有效
`submit_collaboration_decision` 都已经把每个 Team 的 `result.required_artifacts` 交给唯一拓扑终结
角色；第二次还按诊断更名并重申了这些字段。编译器仍连续返回同一个
`completion_terminal_role_missing`。精确原因是 `evidence_required=true` 会在编译器内部额外追加
字面 artifact `evidence`，而模型使用了业务命名的 `runtime_evidence` 等字段。Schema 没有说明该
隐藏追加；错误提示也只说“assign every required result artifact”，所以模型无法知道重复声明
`evidence` 才是实现所需。这不是模型未修复，而是 Team-level result 与 role-level output 两份
权威互相校验形成的冗余、隐藏契约。

### 20.3 第一性原理修复与安全边界

`team.result` 是权威终态契约；role outputs 是局部数据流契约。Runtime 先按有向拓扑求终结角色：

1. 若恰有一个终结角色，确定性地把 `required_artifacts` 以及 `evidence_required` 派生的
   `evidence` 下沉到该角色的 compiled outputs、acceptance、semantic snapshot 和 binding digest；
2. 若多个终结角色中恰有一个已完整声明结果，保留模型的明确归属；
3. 若多个角色都完整声明，返回 ambiguous；若多个角色均未完整声明，仍返回 missing，禁止猜选；
4. 该派生不增加 capability、skill、tool、permission、resource scope 或 evidence scope，只消除
   同一结构事实的重复书写；binding material 记录 `resolved_output_artifacts` 和
   `terminal_result_artifacts_derived`，恢复与审计可重放；
5. compiler revision 提升至 `collaboration-intent/v4`，Schema 明示 Team result 的权威性以及
   `evidence_required` 的字面含义。

回归同时覆盖：唯一终结角色缺少业务 result 字段时自动继承、缺少隐藏 `evidence` 时自动继承、
两个无归属终结角色仍 fail closed。由此把原来每次需要完整 DeepSeek 推理与约 8k–10k 工具参数的
语义修订，降为本地确定性 lowering，不以字符预算、裁剪历史或放松验收换取费用下降。

### 20.4 Candidate 5：缓存工作正常，但控制平面互锁造成 output 空耗

安装 Candidate 4 后的真实三 Team E2E（Session
`cd00c9cf-2833-4e0b-8c7a-23879b6f22f7`）继续证明缓存链正确：在线请求保持同一 identity
`sha256:cb60e48e15b2210cd...`，只有首请求是 cold leader，结构复用从 0% 升至 35.67%，再升至
83.87%。但是第一次协同提交因任务路径相对主 workspace 错误而 fail closed；同轮补充正确路径后，
模型连续产生至少四个 JSON 合法的修订版 `submit_collaboration_decision`，只有第一次提交进入
`tool.invocation.started`，其余只形成 `model.item_completed`，最终仍未创建 Team。累计 output 约
62.6k tokens，因此“有缓存仍昂贵”不是输入缓存失效，而是输出被控制循环重复生成；输出 token
不能由输入前缀缓存消除。

源级因果链是两个各自合理、组合后矛盾的约束：`pending_disposition_inputs` 要求下一步唯一调用
`runtime_orchestrate(route_input)`，未准入的 root collaboration 同时要求下一步唯一调用
`submit_collaboration_decision`；后者的 one-shot allowlist 覆盖前者，而响应后处理又因缺少
`route_input` 丢弃合法的协同提交。下一步继续重复相同冲突，直到安全预算耗尽。

统一修复确立一个控制平面优先级：运行中输入路由 > root collaboration 准入 > 旧单步约束。待路由
输入非空时，Runtime 只暴露并强制 `runtime_orchestrate`，注入带精确 slot 数的系统级路由契约，
暂停协同准入、review prefetch、terminal synthesis 和 safety-fuse 终止；成功应用后清除旧终态候选、
标记真实进展，并在下一模型步重新计算协同提交要求。旧的 submit/text-only/tool allowlist 不延期
复用，因为它们基于旧拓扑；权威状态机会在新拓扑上重新派生仍适用的约束。能力清单同步改为：
`route_input` 是仅在 Runtime 明确要求时可用的内部 active-Turn 路径，Gateway 直达仍 fail closed，
不再同时宣称“可解析但不支持”。这项修复不裁剪上下文、不降低 Team 数、不减少 Agent 能动性，
只消除无效 Provider 循环。

### 20.5 Candidate 6：命中率口径、低复用串行与证据双份装配

控制平面修复通过全量确定性门后，主服务上的三 Team DeepSeek Flash E2E（Session
`cache-e2e-20260901-235626`，root execution
`session-ingress-graph:6bf8ef6dad0bb6db167a5dbb35847496`）首次真实创建 A/B 两个并行 Team；C 按
A/B 依赖等待。root 相邻请求使用同一 identity，provider hit 从 **11.30%** 升到 **83.56%**，证明
稳定前缀缓存在线发挥作用。但首个 delegated Agent 对唯一源码文件的请求只有 **1.02%** hit，且
产生 25,082 output tokens；运行立即安全停止，未让 B/C 继续扩大费用。

exact-wire artifact 给出三个不能被“总体命中率”掩盖的事实：

1. A/B 虽被粗粒度 Program cohort 归为同一 identity，实际 wire 最长公共前缀仅 5,331 bytes，约
   **1.65%**。旧 transport pool 仍让 B 等待 A 完成约 128 秒，再成为 common-prefix discovery
   leader；这不是缓存收益，而是以几乎不可复用的前缀串行化本应并行的 Team。
2. `cold_leader` 同时表示 FirstRequest 与 CommonPrefix discovery，观测层把 B 误报成第二个冷启动，
   无法区分缓存失效和正常的公共前缀发现。
3. A 的 `read_file` 完整结果约 99.9k chars，既已作为原生 ToolResult 进入消息历史，又被
   `Runtime-verified Focus evidence` 再复制约 100.8k chars；B 同样重复约 133.5k chars。结果是 A/B
   model-visible request 分别达到约 254k/323k bytes，唯一业务证据被支付两遍。

第一性原理修复不通过 padding 公共 system prompt 伪造 90%，也不裁掉业务证据：

- transport pool 先计算真实 wire LCP。exact extension 始终等待并复用；只有 LCP 占 follower prompt
  至少 50% 才做一次 common-prefix discovery；低于阈值的 sibling 记录
  `cache_warmup_bypassed_low_reuse=true` 并立即并发。FirstRequest 与 CommonPrefix leader 分字段持久化；
- Focus 上下文只引用紧邻的原生 ToolResult 及 tool-call receipts，不再复制内容；完成证据采集后改走
  Runtime 已有的 clean terminal synthesis，只携带原始目标和去重后的完整 receipts、零工具、零探索
  transcript。全部证据仍保留一次，预计上述 A/B 请求分别下降约 40%/42%；
- hit 目标分成两个可审计口径：对“已有可复用稳定前缀”的 eligible warm 请求要求 provider hit
  `>=90%`；首次读取的唯一大文件、首次 identity 和不同 Team 的角色专属正文属于 irreducible miss，
  必须报告但不能数学上承诺 90%。cold-inclusive 总体值只作成本实况，禁止拿 warm 校准值替代。

DeepSeek v4 Flash 的默认 thinking 还解释了剩余费用：其当前兼容协议不支持将一次性
`reasoning_effort=none` 作为可靠 wire 能力，输入缓存也不能抵扣 private reasoning/output tokens。
框架通过干净归纳减少无关输入和重复推理诱因，但不得用任意字符上限截断结果来伪造节省；最终真实
canary 必须同时报告 input miss、cache read、output/reasoning、eligible warm hit、cold-inclusive hit、
低复用旁路次数和实际并发重叠，才能回答“缓存是否发挥作用”和“为什么仍贵”这两个不同问题。

### 20.6 Candidate 7：真实完成态证明缓存有效，同时证伪总体 90% 与发现 Skill 空授权

安装 Candidate 6 后，主 Gateway 上的三 Team DeepSeek Flash E2E（Session
`cache-canary-v0914-20260902-002559`，root execution
`session-ingress-graph:0a5e78cff6ee91d1f203f65e1e5742f0`）完整终态：A/B 实际并发，C 在 A/B
产物就绪后启动；3 Team、7 child executions 与 40/40 graph nodes 全部 terminal，零
running/waiting/blocked，最终报告含精确文件、行级证据、风险和空 unresolved。child 汇总记录
34,560 cached、42,617 miss input、8,594 output、最大工具并发 2，证明协同、依赖和终态归纳已成立。

11 个 provider outcome 全部 known，唯一 usage 口径为：

| 口径 | miss input | cache read | output | hit |
| --- | ---: | ---: | ---: | ---: |
| 冷启动全程 | 71,689 | 90,240 | 24,581 | **55.72%** |
| 非冷请求 | 44,794 | 80,512 | 16,991 | **64.25%** |
| exact extension | 20,290 | 78,592 | 15,062 | **79.48%** |
| 低复用并发旁路 | 12,141 | 9,856 | 2,490 | **44.80%** |

这组数据同时给出两个不可混淆的结论：90,240 个真实 provider cache-read token 证明缓存在线且
显著省费；但异构短任务的总体 55.72% 和 eligible extension 79.48% 证伪了“已经达到 90%”。剩余
成本不是一个开关：71,689 是首次角色/证据或新增 suffix，24,581 是不可由输入缓存抵扣的
output/reasoning。不得通过加入无业务价值的固定 padding 改善比例。

exact-wire 继续暴露两个框架级原因：

1. A/B 的第一条 system 文本实际共享 4,205 chars，但 Provider evidence 的本地 canonical prompt
   对每条完整 JSON 消息先写长度；Team 后缀长度不同使结构 LCP 在第 1 byte 即分叉。修复为
   OpenAI-compatible wire 的两段 system：byte-identical Program cohort 独立作为首 item，
   Team/Agent immutable suffix 作为第二 item；Anthropic scalar system 保持原序列化。该分段只改变
   provider cache 边界，不改变文本、顺序或 authority。
2. 更严重的是无 `required_skills` 的角色被 Team instantiator 解释为“继承 Agent Definition 全部
   Skill”。A 因此注入约 18k chars 的无关重构 Skill，B 注入无关的 Lark standup Skill；root 还可
   因多个 generic summary term 累计越过阈值误激活联网 Skill。修复确立空集合的标准语义：Definition
   Skill inventory 只是 authority ceiling；role 未请求即 grant 为空，delegated host 不暴露全局
   catalog；root 自动发现必须有显式 visible grant 或 Skill name/id 直接匹配，摘要词只进入候选观测，
   不能触发整页 prompt 注入。

Candidate 7 的验收同时看绝对成本和比例：相同冻结场景必须保持 3 Team 完成、A/B 并发、C 依赖、
产物质量不退化；无 Skill 角色的 `skill.activation.selected` 必须为零；A/B 首 wire item 必须精确相同；
总输入 miss 与 output 均不得高于 Candidate 6；eligible exact-extension hit 必须单独报告。90% 是长期、
高复用 eligible cohort 的门，不是通过冷启动分母或 padding 可以强造的全任务承诺。

### 20.7 Candidate 8：干净终态破坏 ProviderModel 证据链与 Skill identity 误匹配

安装 Candidate 7 后的冻结 canary（Session
`cache-canary-v0914-skillfix-20260902-005409`）证明 delegated 空 Skill grant 已生效：叶 Agent 不再
出现 Skill activation，A/B 也同时启动并完成真实源码读取与结果生成。但两支最终都被 Runtime 正确地
拒绝为 satisfied：required obligation 要求 `ProviderModel` observation，持久化
`ObservedEvidenceAcquisition` 有精确 target、digest 和 ToolResult receipt，却没有
`model_observation` attestation；C 因依赖未满足没有启动。运行在 15/15 节点安全取消，避免恢复路径继续
付费。

根因位于 clean terminal synthesis 的证据表示转换，而非验收器或模型：正常 continuation 会把原生
assistant `ToolUse` 与 Runtime `ToolResult` 一起装进下一次 Provider 请求，
`packed_model_observation_candidates` 才能按 invocation id/name/output digest 生成候选 attestation；
旧 clean terminal 为避免探索循环，只发送“目标 + receipt 文本摘要”，把 native blocks 全部丢弃。
模型可以据此生成正确报告，但 Runtime 无法证明精确 receipt 真正进入了有效 Provider 请求，于是终态
失败并触发昂贵恢复。这个优化违反了“压缩表示不得损失验收所需类型信息”的框架不变量。

通用修复把 clean terminal 输入改为最小 provider-valid evidence history：从已提交 transcript 中只投影
精确匹配的 `ToolUse -> ToolResult` 对，剥离所有探索文本、thinking、无结果 tool call 和重复 receipt；
紧随其后的用户消息只引用这些原生 receipts，不再复制完整正文。early-dispatch receipt 若尚未进入普通
assistant history，则用 Runtime 已持有的原始 call id/name/input 补齐配对。这样同时保留三项性质：

- Provider 能实际读取完整业务证据，Runtime 能在有效响应后生成 digest-bound model observation；
- strict OpenAI-compatible sanitizer 不会因 orphan ToolResult 丢弃证据；
- clean terminal 仍不携带循环诱因，且 evidence 不再以 native block 和文本摘要双份计费。

后续 canary 进一步覆盖 Runtime focus-prefetch：此类只读证据由内核主动采集，按定义不存在
assistant-authored ToolUse，旧投影仍会留下 orphan ToolResult。统一规则因此扩展为：优先保留真实
assistant ToolUse；对所有剩余的 Runtime-owned receipt，按原 invocation id/tool name 生成带
`runtime_evidence_replay=true` 标记的最小历史 carrier，再附原字节 ToolResult。标记明确区分内核预取
与模型自主调用，provider observation 仍只按 id/name/output digest 验证，不把 synthetic input 当作
执行事实。

root Skill 自动选择也从“任一名称 token 命中”收紧为“完整 skill id/name phrase 命中”；普通任务中的
`Agent` 不再激活 `agent-reach`，显式写出 `agent-reach` 或 Agent profile visible grant 仍保留原能力。
摘要和单词命中继续作为可观测候选分数，但不能授权 PromptOnly 指令注入。这是最小权限与缓存稳定性的
共同边界，不硬编码业务选择，也不降低显式 Skill 编排能力。

### 20.8 Candidate 9：跨 Team handoff 未成为物理就绪屏障

Candidate 8 在线 canary（Session `cache-canary-v0914-final-20260902-011500`）验证了最小 native
receipt 历史修复：A/B 在相同启动窗口并行执行，均完成真实文件读取，ProviderModel observation 成功，
Runtime acceptance 为 Satisfied，Team terminal delivery 完整；上一轮“有结果但无模型观察证明”的失败已
消失。运行同时暴露一个独立调度缺陷，因此在 C 失败后立即取消 root，未允许恢复路径继续付费。

模型与 CollaborationProgram 都正确声明 `team_a -> team_c`、`team_b -> team_c`；Program edge 的
typed input contract 也要求 producer 的 `terminal_synthesis`、ObservedEvidence 与 AcceptanceVerdict。
但是 graph compiler 只生成非调度型 `CrossTeamHandoff`，仅当 consumer 另有
`required_evidence_refs` 时才附加 `ArtifactRequires`。本场景的 required facts 已在 Program input
contract 中，semantic node 无需重复声明 evidence ref，于是 C 与 A/B 同时变成 ready；C 在 0ms 内尝试
记录 delivery，CommitService 正确拒绝尚无 terminal result 的 A，节点却被标为不可重试 failed。

修复保持两类边的单一职责：`CrossTeamHandoff` 继续作为 Program/dataflow identity，不直接改变通用
scheduler；每一个 executable Team-to-Team `depends_on` 都由 compiler 无条件生成 companion
`ArtifactRequires` 物理 readiness edge。这样 A/B 等无互相依赖的 producer 仍最大并行，C 只有在两者
terminal result 与 acceptance 已原子提交后才 ready；此时 delivery record/claim 仍由既有 CAS、attempt
fence 和 typed contract 校验，未引入第二调度器或放松证据要求。候选级 Skill 观测同时与执行选择解耦：
generic token 命中的候选保留在 candidates，但 `selected`、Skill activity、memory capture 和 prompt
page-in 只能来自 `SkillSelectionResult.selected`，前端不再把未注入的 `agent-reach` 显示为已执行。

### 20.9 Candidate 10：完整终态、真实计费口径与可机械修复的跨 Team 冗余边

安装 Candidate 9 后的冻结 DeepSeek Flash canary（Session
`cache-canary-v0914-final3-20260902-014000`，root execution
`session-ingress-graph:55de5bd715d4021403e1eb54e7fc545b`）达到完整终态。投影 revision 31、cursor
1840，root health 为 `terminal`，terminal presentation 为 `committed/valid`；3 个 Team、3 个 Agent
全部 completed，0 个 Skill activity。A/B 的 `agent.execution.started` 相差 10ms，C 直到 B（较晚的
producer）terminal 后 252ms 才启动，证明最大可行并发和物理依赖屏障同时成立。最终回答、精确文件
digest、风险与 `unresolved: []` 已写入 durable transcript。

11 个 `context.provider_attempt_outcome` 全部 usage known，Provider 返回的真实计费字段汇总为：

| 口径 | 请求 | miss input | cache read | output | hit |
| --- | ---: | ---: | ---: | ---: | ---: |
| 全程 | 11 | 61,992 | 101,888 | 26,960 | **62.17%** |
| 非 cold leader | 8 | 48,159 | 84,864 | 15,745 | **63.80%** |
| exact extension | 6 | 31,767 | 82,816 | 13,990 | **72.28%** |
| 未旁路低复用 warmup | 7 | 36,905 | 80,512 | 20,241 | **68.57%** |

因此结论必须保持双重真实性：101,888 个 cache-read token 证明缓存在线并显著降低输入价格；62.17%
也证明该异构短任务没有达到总体 90%。三个新 identity 的首请求、不同 Team 的角色/授权/证据正文、
新增对话 suffix 和 26,960 output/reasoning 都是实际成本。不能用 padding、删证据或把 99% 线性校准
冒充真实多 Agent 负载。

该会话唯一 failed activity 是第一次 `submit_collaboration_decision`：模型已正确声明 workstream
`depends_on` 和跨 Team input artifacts，却又把同一 handoff 重复写进 consumer Team 的本地 role
dependencies，local validator 因 source role 不属于该 Team 而拒绝。第二次提交成功，但这次可机械推导
的重试仍造成额外输入与输出。compiler revision 因此提升到 `collaboration-intent/v5`：仅当 target 是
本地角色、source 唯一属于已声明 predecessor、artifacts 完整满足 upstream output/local input，且边为
`evidence_feed`/`handoff`/`aggregate` 时，确定性删除冗余 local spelling，保留 workstream barrier；
`review_of`/`dispute`、歧义 source、缺失 artifact 继续 fail closed。该修复不推断权限、Agent、Skill、
工具、证据范围或独立评审语义，定向回归证明同一错误不再需要付费模型修订。

最后的浏览器验收还发现安装闭环曾只替换 Core binary，主服务仍提供 Edge/WebUI `0.9.713`，导致 API
已存在的 Session 在页面不可见。重新构建并原子安装 cowd-edge 后，主服务实际返回 `0.9.714`；Playwright
直接访问 `127.0.0.1:8642/index.html#/chat`，加载上述 durable Session，展开 canonical activity tree，
验证 3 Team completed、3 Agent completed、0 Skill，Agent A/B/C 名称和最终回答全部可见，且无 console
error 或 failed request。此证据将“API 完成”提升为已安装 Core + Edge + 浏览器渲染的真正 E2E。
