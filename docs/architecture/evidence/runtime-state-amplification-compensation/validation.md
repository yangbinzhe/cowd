# Runtime 状态放大补偿版验证报告

> 验证日期：2026-08-07。本文只记录实际执行结果；提交、tag 和远端指针由封版门禁
> 在代码冻结后验证，不用文档声明替代 Git 证据。

## 已完成的定向证据

| 范围 | 结果 |
| --- | --- |
| Evolution 历史 dead letter 修复测试 | 通过 |
| Evolution projector 全部单元测试 | 通过 |
| Context schema v3 canonical serialization | 通过 |
| 大 Context Artifact 生命周期 | 通过 |
| Gateway Context 按需 hydrate | 通过 |
| TUI schema v3 读取 | 通过 |
| 未知 API fallback 合同 | 通过 |
| WebUI Session-scoped mutation fence | 通过 |
| 非 `C` collation PostgreSQL 合同 | `en_US.utf8` 隔离实例通过 |

## 全量门禁

| 范围 | 命令/证据 | 结果 |
| --- | --- | --- |
| Core 全量测试 | `cargo test --workspace` | 通过，零失败 |
| Core 受锁 App 消费者 | `cargo test -p cowd-product-apps -p gateway -p tui -p cli` | Gateway 698、TUI 1049 等全部通过 |
| Core 全目标编译 | `cargo check --workspace --all-targets` | 通过 |
| Core 格式 | `cargo fmt --all -- --check` | 通过 |
| Core 架构边界 | `scripts/architecture/check-boundaries.sh` | 全部门禁通过 |
| App source lock | `cargo run -p xtask -- apps verify --locked` | MFG 精确锁定 `b0f849eb40beec1355a3309e4a12a2ea981d7664` |
| API 文档 | `generate_gateway_api_docs.py --check` | 432 条路由一致 |
| TUI 脚本 | 三个场景脚本 `bash -n` | 通过 |
| Edge Rust | `cargo test --workspace` | 398 项通过 |
| WebUI | `npm test` | 51 个文件、406 项测试通过 |
| WebUI i18n | coverage gate | 中文 2952、英文 2952，完全对齐 |
| WebUI 治理 | governance 与 capability gates | 115 条需求及全部合同门禁通过 |
| WebUI 构建 | `npm run build` | 生产构建通过 |
| MFG | `cargo test --workspace` | 153 项通过 |
| 真实 PostgreSQL | isolated ignored contract | `en_US.utf8` 下通过 |
| 三仓差异格式 | `git diff --check` | 通过 |

首次受影响包重验曾因版本文件已经升级到 `0.9.646`、而一个升级前启动的
`0.9.645` 测试二进制仍在运行，触发安装器的版本漂移拒绝。该门禁正确阻止了旧二进制
覆盖新版本，不是产品代码失败；停止旧进程并以最终 source lock 重编译后，全量和定向
测试均通过。

## 安装态证据

使用同一份 full Release 候选替换 `~/AI/cowd`，并把 Edge 安装根收敛为 6 个可执行
artifact、9 个 connector manifest 和一份 WebUI production dist。安装过程中不创建
历史备份，不触碰 `~/AI` 下其他项目。

| 检查 | 结果 |
| --- | --- |
| Cowd 版本 | `0.9.646` |
| Storage upgrade | PostgreSQL schema upgrade completed |
| Gateway status | running |
| Gateway doctor | 7 OK、0 warning、0 failure |
| `/health`、`/readyz` | 200 |
| 未知 `/api/*` | 404、`application/json`、`api_route_not_found` |
| Edge registry | ready、0 degraded、0 failed、0 circuit open |
| Feishu managed sidecar | ready、0 consecutive failure |
| WebUI 资源 | HTTP index 与安装文件 SHA-256 完全一致 |
| 进程模型 | 单一 Gateway、单一 auth broker、受管 Feishu sidecar |

## Evolution 历史恢复

安装新 Gateway 前的历史 signal 已存在 failed 证据且 checkpoint 已推进。新 worker 启动
后实际计数为：

```text
evolution.signal.recorded.v1            125
evolution.signal.projector.failed.v1    125
evolution.signal.projector.recovered.v1 125
evolution.diagnosis.recorded.v1         125
evolution.mission.opened.v1             125
evolution.proposal.created.v1           125
```

Gateway 的 signals、diagnoses、proposals API 也分别返回 125 项。失败证据没有被删除，
每条历史失败都有 recovered marker 和完整 lifecycle，unresolved 数量为零。

## 非阻断观察

Vite 对独立按需加载的 graph worker（约 1.4 MB）和 ChartPanel（约 597 KB）给出 chunk
体积提示。它们不进入首屏主包，不影响本次功能、正确性或加载门禁；后续可以作为纯体积
专项继续拆分，但不应与本补偿版混为未完成能力。
