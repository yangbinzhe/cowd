# Cowd Test Governance

本目录记录测试治理口径：哪些测试是日常默认门禁，哪些是阶段门禁、最终回归、手工诊断或删除候选。

测试执行入口统一放在 [scripts/test/README.md](../../scripts/test/README.md) 中维护。当前推荐顺序：

```bash
scripts/test/quick.sh
scripts/test/changed-crates.sh
scripts/test/full-regression.sh
```

也可以通过 `scripts/validate.sh` 调用：

```bash
scripts/validate.sh quick
scripts/validate.sh changed-crates
scripts/validate.sh full-regression
```

## 核心原则

- 稳定内核合同留在 Rust unit/contract tests。
- 高风险跨模块行为只保留少量黄金路径，不为同一闭环堆叠重复场景。
- 修改进程全局 env/cwd/provider/session 状态的测试进入 serial/global lane。
- interactive、live provider、LLM judge、视觉和探索性测试默认不进入发布门禁，除非明确提升。
- 新默认测试必须替换或收敛重叠覆盖，不应只新增低价值断言。
- 架构重构必须配硬门禁：源码扫描、依赖方向检查、架构测试或关键行为测试。

## 当前实测基线

2026-06-26 缓存态抽样：

- `cargo check --workspace --all-targets`：约 2.6 秒。
- `cargo test -p gateway --all-targets -- --test-threads=1`：约 30 秒。
- `cargo test -p tui --lib -- --test-threads=1`：约 23 秒，约 901 个测试。
- `cargo test -p runtime --lib -- --test-threads=1`：约 12 秒，约 827 个测试。
- `cargo test -p memory --all-targets -- --test-threads=1`：约 10 秒。

结论：

- 全量测试有价值，但不适合作为每次小改的默认反馈。
- 日常使用 `quick`，提交前使用 `changed-crates`，版本末尾使用 `full-regression`。

## Inventory

`test-inventory.yaml` 是当前测试分类的来源。更新测试策略时，应同步更新 inventory，而不是只在脚本中暗改。

旧入口说明：

- `scripts/test/gateway-global-env.sh` 是 serial global-state 的 canonical gateway 入口。
- `scripts/test/gateway-slow.sh` 只是兼容别名。
