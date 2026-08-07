# Runtime 状态放大治理验证报告

> 本报告记录 `0.9.645` 首轮验收。后续真实 PostgreSQL 链路发现的缺口及修复结果以
> `../runtime-state-amplification-compensation/` 为准；不得单独引用本报告证明补偿版通过。

## 自动化结果

| 范围 | 命令 | 结果 |
| --- | --- | --- |
| Workspace 编译 | `cargo check --workspace` | 通过 |
| 格式 | `cargo fmt --all -- --check` | 通过 |
| Provider | `cargo test -p provider --lib` | 149 通过 |
| Session | `cargo test -p session --lib` | 113 通过 |
| Runtime | `cargo test -p runtime --lib` | 1439 通过 |
| Gateway | `cargo test -p gateway --lib` | 687 通过，9 个外部环境测试忽略 |
| TUI | `cargo test -p tui --lib` | 1048 通过 |
| PostgreSQL Runtime | 隔离库 ignored contract tests | 3 通过 |
| PostgreSQL Session | 隔离库 ignored contract tests | 14 通过 |
| WebUI | `npm test` | 51 个文件、405 个测试通过；全部合同门禁通过 |
| WebUI build | `npm run build` | 生产构建通过 |
| API 文档 | `generate_gateway_api_docs.py --check` | 432 个路由一致 |
| 差异格式 | Core/Edge `git diff --check` | 通过 |

外部环境忽略项只包含飞书真实账号、全局环境串行和需要独立 PostgreSQL 的 Gateway
跨存储测试；本次涉及的 Runtime/Session PostgreSQL 合同已在独立数据库单独执行。

## PostgreSQL 真实副本迁移

从正在使用的本地 PostgreSQL 做一致副本，使用当前二进制正式执行：

```text
cowd storage upgrade
operation=postgres_schema_upgrade
status=completed
```

结果：

| 指标 | 迁移前 | 迁移后 | 结论 |
| --- | ---: | ---: | --- |
| Runtime events | 6538 | 2566 | 只剩 canonical event |
| projector checkpoint event | 2959 | 0 | 完全迁移 |
| live snapshot event | 1013 | 0 | 完全迁移 |
| Runtime commits | 5886 | 1914 | 3972 个空 commit 清理 |
| transaction streams | 6379 | 2407 | 3972 个空 stream 清理 |
| mutable projection checkpoint | 无表 | 15 | active/latest 状态保留 |
| 精确重复索引组 | 2 | 0 | 主键/唯一约束保留 |

删除的 event 数量 `6538 - 2566 = 3972`，严格等于
`2959 + 1013 = 3972`。没有通过删除 canonical fact、审批、effect receipt、
terminal fence 或原始 artifact 获得体积下降。

## 恢复与一致性

- SQLite 旧 checkpoint/live history 迁移测试通过。
- PostgreSQL 迁移、重启、并发启动和 schema checksum 测试通过。
- projector checkpoint 更新不发布 commit signal。
- live checkpoint 后 canonical replay 恢复 waiting/terminal 状态。
- Provider exact wire artifact 能按 Session scope 读取，且有 durable pin。
- terminal fence、Session materialization、ack/replay 的 exactly-once 测试通过。
- presence CAS 冲突、过期和重启语义在 SQLite/PG 均通过。
- transcript terminal sync 使用 sequence cursor；并发同步只有一个 flight。
- scope invalidation 与 stale in-flight response fencing 测试通过。

## WebUI 视觉结果

使用生产构建检查聊天页：

| 视口 | 结果 |
| --- | --- |
| 390 x 844 | 通过；14 个高密度图标低于 44px，记录为审阅项 |
| 1180 x 900 | 通过 |
| 1440 x 960 | 通过 |

三个视口均满足：无横向溢出、无关键控件裁切、输入框位于首屏、右栏默认关闭、
正文无 raw tool evidence、无裸 i18n key、icon 均有可访问名称。

截图与机器报告位于：

```text
/media/yi/Datas/workspace/plan/0716-WebUI全景与MFG终态补救计划/
  artifacts/0.9.645/visual-audit/
  reports/0.9.645/0.9.645-visual-audit.md
```

## 权威环境部署

2026-08-07 使用与测试相同的 `0.9.645` full Release 候选执行仓库正式部署入口：

```text
COWD_BIN=target/release/cowd scripts/release/deploy-postgres-to-ai.sh
operation=postgres_schema_upgrade
status=completed
```

部署入口先停止旧 Gateway，再原子替换二进制、离线执行 `cowd storage upgrade`，最后启动
新 Gateway。权威数据库与运行环境复核结果：

| 检查 | 结果 |
| --- | --- |
| projector checkpoint event | 0 |
| live snapshot event | 0 |
| mutable projection checkpoint | 11 |
| session presence projection | 2 |
| `idx_runtime_commits_cursor` | 0 |
| `idx_session_messages_session_sequence` | 0 |
| Gateway active driver/task | 0 / 0 |
| Gateway / auth-broker | 连续两次 restart 后仍各 1 个受管进程 |
| Doctor | 7 项通过、0 警告、0 失败 |
| WebUI 生产资源 | 本地 dist、安装目录与 HTTP 返回 SHA-256 一致 |

迁移后的 Gateway 启动会继续写入新的 canonical runtime event，因此首次启动后总量为 2719，
不是副本迁移结束瞬间的 2566；被删除的两类派生 event 没有重新出现。部署时同时清理了
旧版停止流程遗留的 deleted-inode auth-broker，并修正正常 stop/restart 只向父 PID 发信号、
未按既有进程组设计回收 helper 的缺口。隔离进程组测试和安装环境连续两次 restart 都证明
旧 broker 会随 Gateway 一起退出，最终只保留新 Gateway 派生的 broker。
