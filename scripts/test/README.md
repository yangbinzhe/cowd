# Cowd 测试入口

测试分层的目标是快速反馈和完整发布证据同时成立。所有入口由
`scripts/validate.sh` 调度，分类来源是
`tests/test-governance/test-inventory.yaml`。

## 日常

```bash
scripts/validate.sh quick
scripts/validate.sh changed-crates
```

`changed-crates` 使用 `cargo metadata` 动态识别 package，不维护手写 crate 清单。根
Cargo manifest 或 lockfile 变化会自动提升为 workspace all-targets；独立
`tests/interactive` 变化会检查自己的 manifest。位于 crate 外的 APP source lock
显式归属 `cowd-product-apps`；该映射由治理门禁自测。

## 阶段

```bash
scripts/validate.sh contract
scripts/validate.sh serial-global
scripts/validate.sh scenario
scripts/validate.sh surface
```

- `contract`：稳定 crate 合同与依赖边界。
- `serial-global`：唯一需要串行执行的 Gateway 全局状态测试。
- `scenario`：少量跨模块黄金路径。
- `surface`：CLI、TUI、TUI/MFG、WebUI 的真实控制点。

## 封版

```bash
scripts/validate.sh full-regression
scripts/validate.sh release
```

`full-regression` 让普通 Rust 测试使用 Cargo 默认并行度，完成后单独运行
`gateway-global-env.sh`。不得为了少量全局状态测试把整个 workspace 强制单线程。
各 lane 默认复用仓库唯一 `target` 且关闭 incremental，不保留版本备份；需要验证干净
构建时显式设置 `COWD_ISOLATED_TARGET=1`，退出后会删除一次性 target。
验证默认不递归统计大型 `target` 目录，构建体积专项才设置
`COWD_MEASURE_TARGET_SIZE=1`。Gateway 架构边界是静态依赖/禁用路径门禁，不再为源码
扫描链接完整 Gateway 测试二进制；Runtime 生命周期映射和 Memory 依赖合同集中在
`contract`，避免编辑门禁重复构建重包。

## 外部依赖

```bash
scripts/validate.sh manual live-provider
scripts/validate.sh manual lark-live
scripts/validate.sh manual memory-performance
scripts/validate.sh manual postgres-contract
scripts/validate.sh manual tui-production-acceptance
```

PostgreSQL 合同要求隔离、可清空的 `COWD_TEST_POSTGRES_URL`；跨 PostgreSQL 复制还可
提供 `COWD_TEST_POSTGRES_TARGET_URL`。统一入口覆盖 Fact、Surface、Gateway、
Memory、Matrix、Runtime、Session、Approval、Connector 和 Product App 存储合同；
每项必须真实执行且恰好有一个测试通过，禁止因环境缺失静默返回成功。这些入口不会
读取用户生产数据库。TUI 生产验收使用真实 Gateway、受控 Provider 和 PTY，并把证据
写入版本化报告目录。

## 新测试准入

1. 说明它能独立发现的失败模式。
2. 优先断言公开输出、状态、持久化或依赖方向。
3. 不断言私有函数名、源码空格、目录形状、历史版本号或固定测试数量。
4. 与现有测试重叠时合并或替换。
5. ignored 测试必须登记唯一 runner。
6. 外部依赖测试必须显式 ignored，并由 runner 对环境做 fail-fast 校验，不能在测试
   内打印 skipped 后成功返回。
7. 测试治理修改必须通过 `scripts/test/governance-gate.sh`。

AI Harness 深度报告遵守
`docs/ai-harness-report-spec.md`，必须保留完整证据包，不能只输出摘要。
