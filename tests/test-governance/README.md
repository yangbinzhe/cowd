# Cowd Test Governance

当前治理版本：`0.9.615`

测试入口统一由 [scripts/test/README.md](../../scripts/test/README.md) 维护。日常、
提交前和封版依次使用：

```bash
scripts/validate.sh quick
scripts/validate.sh changed-crates
scripts/validate.sh full-regression
```

## 原则

- 每个默认测试必须保护唯一的业务失败模式、公开合同或架构边界。
- 不允许用函数名、源码空格、文件布局、历史版本号或测试数量证明业务能力。
- 新测试如果与现有测试覆盖同一故障，应替换或合并现有测试。
- 会修改进程全局 env/cwd/provider/session 的测试只进入 `serial-global`。
- live provider、真实 Lark、真实 PostgreSQL、人工 TUI、视觉和探索性检查显式进入 manual/nightly。
- 所有 `#[ignore]` 必须能从唯一入口运行，不能成为永久不可达测试。
- 测试统计必须是静态扫描；运行时通过数和耗时从真实回归报告获取。

## 门禁

| Lane | 用途 | 内容 |
| --- | --- | --- |
| `quick` | 编辑反馈 | fmt、workspace check、静态架构/治理边界、小合同 crate |
| `changed-crates` | 提交前 | metadata 动态定位变更 package 并跑其 all-targets |
| `contract` | 阶段合同 | 非重包 workspace tests + Runtime/Gateway/Memory 边界 |
| `serial-global` | 全局状态 | Gateway env/cwd/provider/session 串行测试 |
| `scenario` | 黄金路径 | Session、Memory、Tool、Skill/MFG |
| `surface` | 交互入口 | CLI、TUI、TUI/MFG、WebUI 四个真实控制点 |
| `release` | 安装产物 | 安装、doctor、OpenAPI、完整产品、TUI attach |
| `full-regression` | Rust 封版 | workspace all-targets + serial-global；不重复执行 check |
| `manual` | 外部依赖 | live provider、Lark、PostgreSQL、人工/诊断场景 |

## 架构测试口径

架构测试只能保护：

1. Cargo 生产依赖方向。
2. 禁止出现的第二执行循环、反向依赖或越权业务实现。
3. Runtime 公开模块映射中的生命周期域与唯一 owner。

物理目录、私有函数名和源码调用文本不属于稳定合同。

## AI Harness 报告

深度评测遵守
[AI Harness Report Specification](../../docs/ai-harness-report-spec.md)。摘要报告不是完整
证据；结果包必须保留 provider rounds、tool calls、execution trace、memory/matrix/session
证据和 AI reviewer 生成的完整分析。
