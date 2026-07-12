# Cowd 测试入口规范

本目录只放测试执行入口和测试治理辅助脚本。目标是让日常开发、阶段验收、最终回归分层执行，避免每次小改都跑完整工作区测试。

## 三层测试入口

## Clippy 门禁

发布阻断使用：

```bash
scripts/ci/clippy-safety.sh
```

该门禁将 Rust warning、未处理 `unwrap/expect`、显式 panic、未完成路径、debug 宏和进程退出视为错误。`clippy::all` 与 `clippy::pedantic` 仍应定期运行：

```bash
scripts/ci/clippy-style-report.sh
```

后者是非阻断的风格债务报告。其内容可能随 Rust/Clippy 版本变化；涉及截断、转换或 API 形态的建议必须在对应业务模块中单独审查，不能在发布修复中机械改写。

### 1. 日常快速门禁

```bash
scripts/test/quick.sh
```

用途：

- 普通代码修改后的默认检查。
- 物理目录治理、边界调整、非高风险业务修改后的第一轮反馈。

内容：

- `cargo fmt --all --check`
- `cargo check --workspace --all-targets`
- 核心架构边界测试：gateway/runtime/memory architecture tests
- 小型边界 crate：`harness-contract`、`model-protocol`、`surface`、`matrix-core`

原则：

- 快速证明代码可编译、核心边界未回退。
- 不跑所有 gateway/runtime/tui/memory 大测试集。

### 2. 变更包精准门禁

```bash
scripts/test/changed-crates.sh [base]
```

默认 `base` 为 `HEAD`，也可以通过 `COWD_CHANGED_BASE` 指定。

用途：

- 提交前针对当前变更涉及的 crate 精准补测。
- 大重构阶段内部完成后，用于快速确认 touched crates。

规则：

- 先跑 `fmt` 和全工作区 `check`。
- 根据变更文件映射到 crate。
- 普通 crate 跑 `cargo test -p <crate> --all-targets`。
- `gateway`、`runtime`、`memory` 会额外跑各自架构测试。
- `tui` 默认跑 `cargo test -p tui --lib`，避免每次都拉起全部终端交互相关目标。

### 3. 最终全量回归

```bash
scripts/test/full-regression.sh
```

用途：

- 版本提交前。
- 大规模重构完成后。
- 准备打 tag 或发布前。

内容：

- `cargo fmt --all --check`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-targets -- --test-threads=1`

原则：

- 这是最终回归工具，不是日常开发工具。
- 单线程执行用于降低全局状态、端口、环境变量、临时目录等测试互相干扰。

## 为什么不默认全量测试

当前缓存态实测显示：

- `gateway --all-targets` 约 30 秒，主测试集约 373 个测试。
- `tui --lib` 约 23 秒，约 901 个测试。
- `runtime --lib` 约 12 秒，约 827 个测试。
- `memory --all-targets` 约 10 秒，lib 约 530 个测试。

这些测试有价值，但它们不是每次小改的最佳反馈路径。日常应先跑快速门禁和变更包精准门禁，最终再跑全量回归。

## 新测试准入原则

- 新默认测试必须证明关键边界、关键合同、关键行为，不添加低价值重复断言。
- 如果已有测试覆盖同一行为，新测试应替换或收敛旧测试，而不是叠加一层。
- 会修改进程全局状态、工作目录、环境变量、端口、provider 配置的测试，应进入 serial/global 类门禁。
- live provider、LLM judge、视觉交互、人工探索测试不进入默认门禁，除非被明确提升为发布门禁。
- 架构重构必须有源码扫描或架构测试作为硬门禁，不能只依赖“能编译”。

## AI Harness 报告规范

深度评测、真实 provider 评测、场景化评测的结果包必须遵守
[docs/ai-harness-report-spec.md](../../docs/ai-harness-report-spec.md)：

- `report.md` 只是摘要入口。
- `analysis-context.json`、`full-analysis-report-template.md`、
  `full-analysis-report-prompt.md` 是报告生成契约，必须由评测器自动生成。
- `full-analysis-report.md` 是 AI reviewer 基于证据包生成的人工审查完整分析报告。
- `report.json`、`execution-trace.json`、`provider-rounds/`、`tool-calls/`、
  `evidence/` 是可追溯证据包。
- 即使总体状态为 passed，局部工具失败、降级行为、未证明内容也必须写入
  `full-analysis-report.md`。
