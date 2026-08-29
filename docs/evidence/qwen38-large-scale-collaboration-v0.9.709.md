# Qwen 3.8-max 大规模协同验收（v0.9.709）

日期：2026-08-29（Asia/Shanghai）

## 目标与拓扑

使用真实 `qwen3.8-max`、隔离 Gateway、单一 Collaboration Program 执行六个 Team。
每个 Team 包含串行的 investigator 和 reviewer 两个 Agent。A/B/C/D 为第一并发波，
E 同时依赖 A/B，F 同时依赖 C/D/E；跨 Team 交付边为 A→E、B→E、C→F、D→F、
E→F。所有工具权限限定为只读源码检查。

永久复现场景：`live_qwen38_large_scale_collaboration`，由
`COWD_EVAL_LARGE_SCALE_COLLABORATION=1` 显式启用。

## 首次失败与根因

基线 v0.9.708 运行在四个首波 Team 同时完成时触发
`program_admission_conflict_exhausted`。Program 控制面采用完整 revision CAS，却只有
三次无退避重试；健康并发提交数大于重试预算时，输掉 CAS 的 Team 被错误判为失败，随后
根 Program 被整体重放。该次产生 8 个完成 Team / 16 个完成 Agent，但五条跨 Team 边均未
进入 claimed，因此验收正确失败。

基线证据：
`/tmp/cowd-qwen38-large-evidence/runs/v0.9.708-1787964777-mission-harness-deep/report.json`
（655,888 tokens，450,634 ms，32 model rounds，60 tool calls）。

第二次诊断确认 CAS 修复有效：A/B/C/D 正常并发收敛，E 在 A/B 完成后启动，A→E、B→E
均形成 delivery/claim receipt。随后一次 Qwen reviewer 响应遗漏必需结构化字段；原实现只
允许一次 presentation-only 恢复，导致已完成的大型 Program 面临整图重放。

## 通用修复

1. Program CAS 重试预算按冻结拓扑规模计算，最少 8、最多 128，并加入上限 32 ms 的
   指数退避和确定性 jitter；revision fence 保持不变。准入、拒绝、跨 Team delivery/claim、
   wait 与 terminal reconcile 使用同一策略。
2. 结构化终态输出允许两次有界、工具禁用、仅复用既有 receipt 的本地 presentation
   recovery，避免一次供应商格式抖动重放整个多 Team Program。
3. Harness 终态指标从持久化 Team task 历史保留 Agent 总数，避免当前活跃 Agent 列表在
   closure 后清空而把真实完成数误报为零。

## 最终真实结果

最终报告：
`/tmp/cowd-qwen38-large-evidence-final/runs/v0.9.708-1787967389-mission-harness-deep/report.json`

- 场景与 report gate：passed；19/19 required report gates passed。
- 单一 Program：6/6 Team completed，12/12 Agent completed，0 failed。
- 并发：A/B/C/D 四 Team、四 investigator 同时运行；每个 Team 内 investigator→reviewer
  串行复核；之后 E 双路扇入、F 三路扇入。
- 跨 Team 真交付：5/5 typed edges reached claimed。
- 质量：canonical Program projection、durable lineage、checked source receipts 为 3/3。
- 模型：仅 `qwen3.8-max`，无 fallback；23 model rounds，49 tool calls。
- 用量：402,198 input + 135,471 output + 8,192 cache = 545,861 tokens。
- 墙钟：721,183 ms；报告记录 wall throughput 187.85 output tokens/s。
- 恢复：最终运行 `recovery_required=0`，没有 Program 整图重放。

## 代码门禁

- `cargo check --workspace --all-targets`：通过。
- `cargo test -p harness-eval --lib`：104 passed。
- `cargo test -p runtime --lib collaboration_coordinator`：7 passed。
- `cargo test -p runtime --lib delegated_structured_recovery_isolated_from_exploratory_history`：
  1 passed。
- 六 Team 并发准入回归连续执行 20 次：20/20 passed。
- `cargo fmt --all -- --check` 与 `git diff --check`：通过。

## 结论与边界

真实证据证明当前框架能稳定执行“并行研究 → 团队内独立复核 → 双路交叉审查 → 三路最终
综合”的有向无环协同，并保持 Program 真相、交付领取、证据和终态一致。已实测规模是
6 Team / 12 Agent / 5 cross-Team edges / 4-way Team concurrency；更高配置上限不是本次
实测结论。721 秒中明显长尾来自真实模型响应与串行依赖波次，而不是 Runtime CAS 冲突或
调度空转。
