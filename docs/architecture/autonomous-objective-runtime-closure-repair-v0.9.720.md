# Autonomous Objective Runtime v0.9.720：真实失败收敛与缓存 cohort 修复

## 变更范围

本版本只处理 v0.9.719 DeepSeek 真实 E2E 暴露的两个框架级缺口：

1. 上游只读 reducer 不得被动态追加无法执行的 `verify_upstream_change` 义务；有独立复核声明的角色仍保留精确 digest 读取。
2. delegated leaf 的连续工具失败最多触发一次 Runtime-owned semantic replan；计数写入 Goal intervention 流并在新 Host 初始化时恢复，第二次失败严格 blocked。
3. provider cache coordinator identity 收敛为 provider/model/transport/security cohort；角色工具 overlay、tool choice、采样和 user-prefix 不再制造虚假 cold identity。真实 canonical prompt、DeepSeek cache read/miss 仍是唯一账单事实。

## 证据门

| 阶段 | 状态 | 证据要求 |
|---|---|---|
| source/contract | passed pending integrated | cargo fmt/check；正负单测；无不可能 evidence contract |
| deterministic regression | pending | workspace tests、scenario smoke、forbidden scan、version gate |
| browser surface | pending | 当前 Core/Edge 构建、Gateway 健康、真实静态入口和 Playwright |
| paid DeepSeek E2E | pending | 同一 live 场景；四 Team/16 Agent、market/review/reread、cache SLO 全通过 |

在 paid 报告 `status=passed` 且 browser 与 backend 事实一致之前，本文件不得被解释为完成声明。
