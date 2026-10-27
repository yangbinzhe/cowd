# Cowd 测试入口

测试分层的目标是快速反馈和完整发布证据同时成立。所有入口由
`scripts/validate.sh` 调度，分类来源是
`tests/test-governance/test-inventory.yaml`。

## 日常

```bash
scripts/validate.sh quick
scripts/validate.sh changed-crates
```

`quick` 只编译、发现和执行四个边界 package 的 target，保留静态架构与治理门；
不再先全仓 check。`changed-crates` 使用 `cargo metadata` 的本地依赖路径，选择文件
所属 package 及 normal/dev/build/optional 反向消费者的传递闭包；不是只测文件所属
crate。任意 Cargo manifest/lock、workspace 配置或无法映射的路径保守提升至全部
workspace package。改名同时计入旧、新路径，删除文件仍按目录映射；无修改使用
quick 集合；Git 不可用或 base 无法解析时退回全 workspace 选择，不阻止测试。独立 `tests/interactive` 变化仍检查自己的 manifest。
APP 由独立仓库构建为签名 Bundle，不进入 Cowd 源码映射。

源码指纹从实际文件读取，不依赖 Git 跟踪列表。Git 仅用于可选的变更范围优化。

三个原入口共用 `cargo-selection.py`，不维护第二份 package/feature/target 清单。
所有受影响 target 使用 `--all-features`，包括 manifest 声明的 required-features；
这不能替代最终默认 features 的 workspace 回归。`full-regression`、`contract`、
scenario、surface、独立 APP 与 ignored/manual/model 各 lane 的验收义务保持。

只读选择不构建：

```bash
scripts/test/changed-crates.sh --dry-run HEAD
scripts/test/inventory.sh plan --quick
scripts/test/inventory.sh plan --workspace
scripts/test/inventory.sh plan crates/mcp/src/lib.rs
```

默认报告写入当前 worktree 父目录的 `.cowd-test-evidence`，每次使用唯一文件名；
可用 `COWD_TEST_EVIDENCE_ROOT` 指定计划证据目录，或显式提供源树之外、尚不存在的
`COWD_TEST_SELECTION_REPORT`。构建 target 由调用者指定，避免与其它 worktree 共写。
报告与日志只新建，不覆盖已有证据。`scripts/validate.sh` 继续是统一
lane 调度入口，例如：

```bash
export COWD_TARGET_DIR=/path/on/data-disk/build/this-worktree
export CARGO_TARGET_DIR="$COWD_TARGET_DIR"
export COWD_TEST_SELECTION_REPORT=/path/on/data-disk/plan/evidence/selection-run.json
scripts/validate.sh changed-crates
```

`inventory.sh discover --output /absolute/new-report.json` 会编译并发现，不执行测试；
`inventory.sh run --output ...` 才执行。`--plan /absolute/saved-plan.json` 可与原 `plan`
输出比对；源码、metadata、目标集合变化即拒绝旧清单。报告分别标记 Planned、
Discovered、TestsPassed/TestsFailed、NotStarted/Incomplete，业务验收始终独立登记
为 NotAssessed。qualified selector 集合和执行集合逐项比较，不只比较数量。
整体零选中、未知 ignored 原因、缺 target/feature、源码或二进制变化显式失败。
合法空 bin/example 记 CompiledOnly，全 ignored target 逐项登记原因和既有 manual
归属，均不冒充测试通过；若整个选择只包含这些 target，仍以零覆盖拒绝。
ignored 原因仍来自 `tests/test-governance/test-inventory.yaml` 的原 marker，源码扫描
仅关联原因，不再把 Rust attribute 数量当作发现或通过。

资源准入按 package 本地依赖闭包保守分类（可能多要求资源，不代表每个测试都使用）：
依赖 `*-postgres` 要求 `COWD_TEST_POSTGRES_URL`；依赖 sandbox/managed-worker
要提供 `COWD_SANDBOX_LAUNCHER_BINARY` 和 `COWD_TEST_LAUNCHER_IDENTITY`。
后者引用构建 owner 的 JSON 原件，包含 `path`（canonical 绝对路径）、`sha256`
（十六进制）、`version`、`source`（plan 的 source 摘要）；同时检查 launcher 的原
`__cowd_internal sandbox-launcher --protocol-version` 协议。准入缺失记 NotStarted，
不归咎业务。报告不保存连接串，日志脱敏 URL 和已知秘密环境变量。
PG 的隔离/连接可用性仍由原 `pg-test-env.sh` 与各 fixture namespace owner 保证；
仅设置变量不证明 PG 健康。fixture 使用本地 loopback 不意味着真实模型验收。

脚本正反测试（自建小 Cargo workspace；不编译产品、不连接 DB）：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test/cargo-selection-test.py -v
```

运行保留完整 build/list/run 日志及真实退出码，默认单个 build/test 最长 1200 秒，
测试执行窗口可用 `COWD_TEST_TIMEOUT_SECONDS` 明确设置。相同 package/all-features
的 all-targets no-run 只准备一次；随后逐 target 进行 list/run，调用原 Cargo runner
接缝的启动守卫核对 source 与确切制品后
exec；package cwd、`[env]`、OUT_DIR、动态库搜索路径仍由 Cargo 原实现管理。
普通 CLI 等供集成测试调用的非 test 制品也绑定 hash，不能仅验证 harness。
不用跨 package 批构建的 feature union 冒充单 package 的 feature basis；这些配置可能产出不同
制品。额外 discovery/守卫进程属于核验开销，未声称总命令数或耗时一定减少。
自定义 target/runner/include 保留原专用 lane，当前 helper 以 NotStarted 显式拒绝。
未测 cold/warm 产品耗时
不得从选择集合减少推算性能倍数。非 libtest 自定义 harness 必须走其原专用 lane；
本入口不会用空发现集合替它宣布通过。

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
- `surface`：CLI、TUI、WebUI 与签名 reference Bundle 的真实控制点。

## 封版

```bash
scripts/validate.sh full-regression
scripts/validate.sh release
```

`full-regression` 让普通 Rust 测试使用 Cargo 默认并行度，完成后单独运行
`gateway-global-env.sh`，再通过 `reference-app.sh` 构建、签名、发现并调用独立 APP。
不得为了少量全局状态测试把整个 workspace 强制单线程。
本轮统一执行前先 `source /media/yi/Datas/workspace/.cowd-deps/env.sh`，离线复用唯一
`next-kernel-dev-20261022` target 和共享 registry/git 源码缓存。不再启用旧的
`COWD_ISOLATED_TARGET` 入口或复制依赖、每轮 target、历史 ELF。
验证默认不递归统计大型 `target` 目录，构建体积专项才设置
`COWD_MEASURE_TARGET_SIZE=1`。Gateway 架构边界是静态依赖/禁用路径门禁，不再为源码
扫描链接完整 Gateway 测试二进制；Runtime 生命周期映射和 Memory 依赖合同集中在
`contract`，避免编辑门禁重复构建重包。

## 外部依赖

```bash
scripts/validate.sh manual live-provider
scripts/validate.sh manual certification
scripts/validate.sh manual lark-live
scripts/validate.sh manual memory-performance
scripts/validate.sh manual postgres-contract
scripts/validate.sh manual public-search-live
scripts/validate.sh manual tui-production-acceptance
```

本轮 PostgreSQL 测试使用外层唯一资源 owner，不提供已有数据库 URL：

```bash
python3 scripts/test/pg-owned-run.py --run-id local-contract --port 36435 \
  --seconds 120 --report /absolute/path/to/local-contract.json -- \
  /absolute/path/to/shared-target/test-binary exact_selector --exact --ignored --nocapture
```

该入口仅使用本机已存在的固定 PG 镜像；为一条命令创建全新容器、卷和私有临时目录，
内部调用原 `pg-test-env.sh` 做真实 TCP 可用性验证。失败、超时、panic、可捕获信号或
业务验收未知时，默认保留本轮独占卷、停止的容器及恢复所需的私有配置；仍停止子进程和
PG、释放连接/端口。最多 15 秒采样；原件仅保存每个输出流最后 64KiB 和清单的加密副本，
不是完整 wire 或数据库 dump。报告含密文 SHA 和原 owner 的私有密钥路径；关闭库时不能
销毁密文的唯一密钥。没有到期自动删库，child exit 0 也不能代替业务验收。
报告保留前后 DB 大小、schema/表/关系数量、top20 heap/index/TOAST 与行数估计，以及
原子进程结果和清理结果；失败不会改写成通过。没有行数估计或持久 caller 列时记未知。
Rust `PostgresTestScope::Drop` 仍可能在 panic 时留下本轮 schema，不能代替外层 owner；
故意 panic 探针只供此入口验证失败生命周期，不是业务通过。SIGKILL/主机失效不能执行
finally，必须按报告标签识别并报告未完成清理，不能宣称已回收。

删除仅接受外层 owner 在 child 私有目录之外写入的 0600 `--closeout-receipt`。
回执绑定报告的 `run_id/container/volume/owner_nonce`（nonce 取 `labels.cowd.test.owner`），
并引用独立验收/复验原件 `evidence: {path: 绝对路径, sha256: 文件摘要}`。两种关闭条件为：

- `kind: "business_acceptance"`，`business_success: true`、`acceptance_complete: true`；
  本轮执行也必须正常退出且没有原执行错误。
- `kind: "issue_closure"`，具名非空 `issue/root_cause/fix`，`revalidation_passed: true`；
  表示原问题已修复且复验通过，不能只写“已知问题”或未闭环的失败说明。

回执是原 owner 审查实际原件后的授权，脚本只校资源身份、完整字段和原件摘要，不是新的
业务 grader。缺失、foreign、未闭环或原件摘要不符时拒删，原业务失败仍保持失败。
修复复验完成后，用同一个入口关闭原保留资源，不重跑 child 或重复增长采样：

```bash
python3 scripts/test/pg-owned-run.py --close-retained \
  --report /absolute/path/to/local-contract.json \
  --closeout-receipt /absolute/path/to/owner-closeout.json
```

此操作复核原 Docker labels、原 PID/starttime 和卷身份；只删这份回执对应的资源。
原失败结果与关闭记录分别保留。加密原件的私有密钥/配置仍由原 owner 保管，不能将
端口释放、进程退出或一次成功停止容器表述成业务验收完成。

`postgres-contract.sh` 作为该 owner 内部的原合同命令，仍通过生成的隔离 URL 执行。
统一合同覆盖 Fact、Surface、Gateway、
Memory、Matrix、Runtime、Session、Approval、Connector 存储合同；APP 私库合同由各自
独立仓库负责，Cowd 只验证 Bundle、supervisor 与通用 transport 边界。
每项必须真实执行且恰好有一个测试通过，禁止因环境缺失静默返回成功。这些入口不会
读取用户生产数据库。TUI 生产验收使用真实 Gateway、受控 Provider 和 PTY，并把证据
写入版本化报告目录。

终态认证要求设置 `COWD_CERTIFICATION_MANIFEST`。该 manifest 必须引用场景运行后
真实产生的 Runtime 事件、数据库状态、进程日志、Provider/Tool 轨迹和 Surface 回执；
runner 会在比较预期前独立采集并固化原始字节与 SHA-256，缺少观测不得通过。

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
