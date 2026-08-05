# 能力保持审计

版本：`0.9.640`

| 原有能力 | 终态实现 | 验证 |
|---|---|---|
| Direct/Agent/Team/Review/Synthesis/SessionDispatch | recipe 保留；模型合同与 Runtime command 分层 | orchestration/compiler/session dispatch 测试 |
| multiplicity 与并行 Agent/Tool | compiler/supervisor 仍拥有并行组；不再由时间重叠猜测 | Team fanout、parallel tool 和 graph tests |
| focus/resource/evidence/completion/cancellation/control | 模型语义字段保留，物理授权由 Runtime 注入 | harness-contract schema 与 Runtime 1426 tests |
| Replan/Recovery | canonical relations、revision、fence 和 receipt 保留 | recovery/replay/旧 fence 测试 |
| Session 补充和跨 Session dispatch | ingress lineage 与显式 `target_session_id` | session dispatch 真实 target stream 测试 |
| Tool 权限、审批和回执 | Tool host/ApprovalQueue 所有权不变，增加 execution index | pending/decision/recovery 测试 |
| Skill 渐进披露 | selector/catalog 不变，activation 获得 canonical identity | 主 Agent、Team Agent skill activation 测试 |
| Memory/Fact/Matrix | 继续通过 context/evidence refs 消费 | 没有进入 Surface 猜测或被 activity 重构删除 |
| Mission Control | 原有 materialized projection 原地增强 | selected Mission 与 durable membership 测试 |
| 原始事件、证据和技术诊断 | 技术模式/detail 按需读取 | business summary 不预载，technical full 可下钻 |
| WebUI/TUI live | 同一 snapshot/delta/live 合同 | WebUI 374 tests、TUI 1050 tests |

## 裁决

本版本删除的是重复合同、猜测关系和全局扫描，不删除执行、恢复、审批、证据、
Mission、Memory/Fact/Matrix 或技术诊断能力。能力收缩项为零。
