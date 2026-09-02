# Autonomous Objective Runtime 终态收敛修复方案（v0.9.719）

## 目的与完成定义

本版本只收敛 v0.9.716–v0.9.718 已声明、但尚未被完整证明的终态能力；不新增第二套调度器、
不降低 Agent/Team 的模型决策空间、不以 activity count 代替业务完成。只有以下四个问题同时
得到正向证据，才允许报告完成：

1. 模型可在边界内自主提出有价值工作，Runtime 可安全拉取、租约、重试和回收；
2. Objective 是唯一业务终态，Team/Graph/Agent 局部结果带 scope 且不能越权提升；
3. durable artifact/evidence/verifier/re-read 能证明用户目标，而不是只证明图结束；
4. Gateway、安装服务和浏览器展示同一份 Session/Team/Objective 事实，成本与缓存证据可审计。

## 统一缺口矩阵

| 缺口 | 根因 | 目标 owner | 修复载体 | 必须删除/禁止 | 验收证据 |
|---|---|---|---|---|---|
| Objective 绑定可被绕过 | observer 以 graph id 查 Goal，且无 admission 硬绑定 | ObjectiveSupervisor + admission | 显式 objective_id/program_ref 绑定校验；child graph 仅 team_local | 无绑定的 root Program 进入 Team admission | root/child 正负测试、重启回放 |
| 终态检查不一致 | `terminal_event` 与 `complete_objective` 约束不同 | GoalStore/ObjectiveSupervisor | 所有成功终态统一走同一 verifier | 任意直接 Satisfied 写入 | 每个生产终态 writer 负测、符号扫描 |
| Team 成功语义越界 | Team Outcome 没有强 scope 类型 | Outcome contract + consumers | `execution_scope=team` 并在 Objective consumer 强校验 | Team Succeeded 推导 Objective Satisfied | serde、scope negative test |
| 旧自治比例路径残留 | 兼容 helper/测试仍暗示固定比例 | Agent initiative protocol | 删除生产/测试旧 helper，保留 typed initiative | `div_ceil(2)`、required proposal fallback | forbidden-symbol scan |
| 前端证据缺失 | API smoke 被当成 browser E2E | Gateway/Surface | 安装服务 + 浏览器真实入口；资产缺失即失败 | `missing_index` 仍算通过 | browser trace、截图、build SHA |
| 缓存目标与报告不一致 | 旧目标 90% 与 cohort-only 方案未统一 | Provider/cache evidence | 固定前缀/动态后缀、cohort 指标、miss 归因 | 不能用校准值宣称全局 90% | cold/warm/structure/value 报告 |
| 证据文档漂移 | 状态、commit、实际 rerun 未同步 | Release evidence owner | 自动生成单一状态源，旧记录标 superseded | 文档 status 与表格 contradiction | evidence consistency check |

## 实施顺序与边界

### R1：契约与终态统一

- 为 `OutcomeIdentity` 增加显式 `execution_scope`，更新所有 writers/serde fixtures。
- 在 `GoalStore` 提供唯一成功终态 verifier；`terminal_event` 与 `complete_objective` 复用同一检查。
- 修复 Objective/Program 绑定解析：root 使用 `goal:{graph_id}`，child 图必须显式标记 team_local。
- 让 Objective reconcile 在提交前同步 criterion/evidence，并在重启后可重放。

### R2：自治与重复路径治理

- 删除生产和测试中固定比例提案 helper、fallback 和旧断言。
- 保留 `InitiativeProposal` 作为唯一 Agent 主动提案协议；接纳、去重、能力和证据仍由 Runtime
  验证，模型不获得 scheduler/permission/terminal 写权限。
- 添加重复 owner/旧符号/直接 terminal 写入扫描。

### R3：Surface、缓存和证据闭环

- 为浏览器验收提供可启动的安装服务和明确资产目录；`missing_index`、旧 SHA、无 Team card
  均硬失败。
- 统一缓存报告：cold-inclusive、eligible-warm、structural reuse、output/retry、observer、
  business value；固定前缀只放稳定公共信息，动态身份和状态放后缀。
- 重新生成 v0.9.716–v0.9.719 evidence，旧文档标记 `superseded`，禁止保留矛盾状态。

## 测试与门禁

R1/R2 只运行 contract/state/fault/concurrency/projection/local-full-product 测试；不使用真实
Provider。R3 完成后才运行真实 DeepSeek 与浏览器 E2E。每个失败必须归属 owner 版本，修复后重跑
该阶段门禁和最终门禁。

最终硬门：

- workspace fmt/check/test 全通过；
- forbidden-symbol/duplicate-owner scan 无生产残留；
- Objective、Program、Team、Agent、Task、Session 身份/revision/cursor 一致；
- crash/restart、lease loss、provider timeout、tool partial effect、cancel、stale write、
  subscriber lag、shutdown 至少各一条回归证据；
- 浏览器从真实入口显示完整 Team 信息和最终 Objective 终态；
- 真实报告 status=passed，且所有声明中的成本/缓存 cohort 门通过；
- evidence status、commit/tag、scenario id、生成时间与实际完全一致。

## 禁止的完成声明

以下任一情况只能报告“未完成/待修复”：

- 只有 graph completed、Team succeeded、model terminal paragraph 或 capability metadata；
- 只有 API/live gateway，没有浏览器渲染；
- 任何证据表仍有 pending/in-progress/reserved；
- cache gate 失败但总报告仍写 passed；
- 旧 helper、旧 owner 或兼容 shim 仍在生产路径；
- 真实运行卡在 finalizing/calling_tool，或目标状态与用户可见状态不一致。

## v0.9.719 真实 DeepSeek 复盘与统一收敛（不可省略）

真实 `deepseek-v4-flash` 场景（16 Agent / 4 Team）在 2026-09-02 的结果为
`status=failed`，不是可接受的过渡交付。Runtime 观测到 A/B/C 完成，而 D 的一个
上游整合角色只有 `collaboration_control` 与 `team_board` 两个控制工具，却在
前驱写入后被动态追加了 `verify_upstream_change:<path>` 的精确读取义务；这是准入
能力、工具暴露和运行时证据生成三处不一致造成的“不可能契约”，模型的两次工具
尝试失败后被正确地 fail-closed。该失败不是模型质量问题，也不是应当通过放宽门禁
掩盖的偶发错误。

统一规则已经固化为：

1. `upstream_evidence_only:no_tool_reacquisition` 的 reducer 只消费 Runtime 已认证
   的 typed handoff，不再生成需要本地读取工具的二次校验义务；明确声明独立复核的
   角色仍必须获得精确资源 lease 并执行 digest 读取。
2. 有 bounded evidence lease 的 delegated leaf 在“连续工具失败且尚无证据”时，
   只允许一次 Runtime-owned semantic replan（改变工具路径、读取诊断、禁止重复
   指纹）；第二次仍失败立即 blocked。该计数与 graph/session 一起恢复，不能通过
   新模型轮次或重启清零，也不能无限重试。
3. Provider cache identity 只表示同一 provider/account/security cohort；不再把
   角色工具 overlay、tool choice、采样等请求局部字段当作不同 cache identity。
   真实 canonical prompt bytes 仍逐请求观测，DeepSeek 返回的 cache read/miss
   才是成本事实，任何 internal warmup 合并都不得改写账单或验收阈值。

本节的三条规则必须分别有 source-level negative/positive tests、运行时事件证据和
最终浏览器/E2E 报告；缺任何一项都不得把本版本标记为完成。
