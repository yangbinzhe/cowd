# Phase 3 记忆框架重构 — 代码质量与能力补全 (v0.7.8)

> Oracle审定: 待定 | 工期: 2-3周 | 并行波: 3波

## 当前代码基线

v0.7.8, commit 8d17d08. Phase 1+2 已完成10项修复。

## 并行执行架构

```
Wave 1 (3 parallel tasks):
├── F11: TuningConfig 阈值配置化 [deep, 3h]
├── F12: std::sync::Mutex → parking_lot::Mutex [deep, 4h]
└── F13: 清理 deprecated session_manager [quick, 1h]

Wave 2 (2 parallel):
├── F14: 错误处理一致性 [deep, 2h]
└── F16: Vector/LLM 配置规范化 [quick, 1h]

Wave 3 (1 task):
└── F15: 提取管线 LLM 增强 [deep, 2d]

Wave FINAL:
├── F1: 全量回归测试
└── F2: Oracle 合规审计
```

## TODOs

- [ ] 1. F11 — TuningConfig: 6个硬编码阈值集中配置化
- [ ] 2. F12 — cognitive.rs std::sync::Mutex → parking_lot::Mutex
- [ ] 3. F13 — 清理 deprecated session_manager 模块
- [ ] 4. F14 — 错误处理一致性: L3召回+提取失败+矛盾counter
- [ ] 5. F15 — 提取管线 LLM 增强 (Pass 5, opt-in)
- [ ] 6. F16 — Vector/LLM 配置注释 + 启动日志 + doctor检查

## Final Verification Wave

- [ ] F1. 全量回归 — `cargo build --workspace && cargo test -p cowd-memory`
- [ ] F2. Oracle 合规审计

## Commit Strategy

每条独立提交。
