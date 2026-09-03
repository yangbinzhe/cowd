# Autonomous Objective Runtime v0.9.721：五项闭环修复设计

> 状态：历史候选修复，未代表当前实施状态；已并入并由
> [`autonomous-objective-runtime-root-cause-and-execution-plan-2026-09-03.md`](./autonomous-objective-runtime-root-cause-and-execution-plan-2026-09-03.md)
> 重新编排。文中的局部修复不得脱离当前 Objective/transport/TaskMarket owner 体系单独实施。

## 真实 E2E 失败的统一根因

v0.9.720 的 DeepSeek 16 Agent 场景并非“模型没有努力”，而是五条业务链在
Runtime 契约中没有同时闭合：

1. `write:team.dependencies` 在目录尚未创建时被路径解析器误判为一个计划文件，
   使合法子路径写入被拒绝；空 `glob_search.path` 又以空资源键失败。
2. 调度器在 Agent 首次模型轮前替它直接 claim 指派 WorkItem，绕开 bid/claim
   市场；因此 UI 虽有 Agent，真实主动 work-market 却没有 proposal/bid 事实。
3. 主动 peer-check 只存在于长提示，没有一个由 durable graph state 驱动的、有限的
   Runtime checkpoint；模型忽略一次提示便会让“自治”退化为一轮执行。
4. Team 交付失败后根节点仍走干净综合路径，直到超时才以 partial 收敛；终态应以
   Team 交付和持久化重读事实为准，不能把模型文本重试当作恢复。
5. provider cache coordinator 已按 cohort 聚合，但角色私有 brief 位于共享用户前缀
   之前，真实 wire 字节在早处发散；cohort identity 相同并不代表 provider 可重用。

## 一次性修复边界

| 链路 | Runtime 规则 | 禁止的伪修复 | 验收 |
|---|---|---|---|
| 虚拟资源 | 保留 `team.dependencies` 的目录前缀语义；同仓库、无穿越、仅子路径 | 将任意不存在文件都扩大成目录 | 首写、glob、同级拒绝、穿越拒绝 |
| 工作市场 | scheduler 只 offer；绑定 Agent 经 checkpoint 原生 bid/claim | 将自动 claim 改名为 Agent claim | proposer/bidder/claimant/reviewer 均可追溯 |
| 主动性 | 每个绑定角色至多一个真实 peer-check proposal opportunity；实际状态驱动后续动作 | 为满足数字阈值虚造 WorkItem | proposal→bid→claim→submit→独立 review/challenge→accept |
| 根终态 | 所有 Team delivery terminal 后读取事实；缺交付即 typed partial/block，不无限综合 | 以根 HTML 或模型 prose 掩盖 Team failed | 单次 bounded final synthesis 或明确 blocker |
| 缓存 | system/共享 cohort/append-only history 在前，role-private/current context 在后；使用真实 wire LCP | 修改 cache 指标或把 coordinator warm 当计费命中 | cold/warm/structural 三项分别通过 |

## 执行顺序与审计门

1. 修复资源与市场状态机，新增正反例单测；不进行真实模型调用。
2. 修复 provider wire 排序与根终态收敛，新增 deterministic prompt/terminal tests；跑
   workspace 全回归和静态禁用扫描。
3. 仅在前两步证据通过后，从真实浏览器入口完成 Gateway/Surface E2E；最后使用一次
   受控 DeepSeek 场景复验。真实模型失败只能产生新的事故记录，不能直接改代码。

## 当前状态

本文件记录候选修复，不构成完成声明。只有同一候选提交的全量 deterministic、浏览器
E2E、以及真实 provider 报告同时通过，且四 Team 全部 verified、市场与 HTML reread
证据完整、缓存目标由原始 provider usage 支持时，才可关闭。
