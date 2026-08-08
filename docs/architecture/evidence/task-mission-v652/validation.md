# v0.9.652 验证记录

## 自动化结果

| 范围 | 命令/环境 | 结果 |
|---|---|---|
| Runtime | `cargo test -p runtime --lib --no-fail-fast` | 1478 passed，0 failed，2 ignored |
| Gateway | `cargo test -p gateway --lib --no-fail-fast` | 692 passed，0 failed，9 ignored |
| TUI | `cargo test -p tui --lib --no-fail-fast` | 1049 passed，0 failed |
| Tools | `cargo test -p tools --lib --no-fail-fast` | 170 passed，0 failed，1 network test ignored |
| Session SQLite | `cargo test -p session --all-targets` | 131 passed，0 failed |
| Session PG | `COWD_TEST_POSTGRES_URL=... cargo test -p session-postgres --all-targets -- --ignored --test-threads=1` | 20 passed，0 failed |
| Workspace | `cargo check --workspace --all-targets` | passed |
| Edge WebUI | `npm test -- --run && npm run build` | 51 files / 409 tests passed；2420 modules built |
| Shell | changed scenario/version scripts through `bash -n` | passed |

忽略项均为显式外部网络、隔离 PostgreSQL 或全局环境串行测试，不包含隐藏失败。PostgreSQL 契约另使用隔离
Docker 实例执行了全部 ignored backend tests。

## PostgreSQL 实库证据

- 用户配置数据库执行 `cowd storage upgrade` 成功。
- Session schema migration 17 已存在，历史 migration 1/2/6/9 未被改写。
- `session_mission_outbox` 已删除。
- `session_runtime_outbox.task_route_hint_json` 已存在且唯一。
- SQLite/PG shared backend contract 均覆盖 typed route hint、重放和恢复。
- Runtime Task migration 6 已把 7 条旧 Task 行前向转换为锁定的 Root Task；旧字段为 0，Root/locked 均为 7，
  Turn binding 为 2 条 primary、5 条 additional，migration 1-6 全部登记。

## 正常线程栈真实模型证据

- 真实用户配置、PostgreSQL 和 `deepseek-v4-pro` 下启动 Gateway，进程环境没有 `RUST_MIN_STACK` 覆盖。
- `RuntimeExecutionSupervisor` 只接收已在调用点装箱的 owned future，消除了大型 Turn future 在 Tokio worker
  默认栈上的展开；没有通过增大线程栈掩盖问题。
- Session `c6092bbb-198c-4d3e-a6a9-1a87cd692511` 连续执行 3 个 Turn，第三轮 16 秒完成，Gateway 未崩溃。
- 3 个 Turn 均绑定到 Root Task `task:root:release-v652-71d2540a695b43f48065c46aff6ba9e0`；未创建第二个
  Root Task。该 Task 归属默认 Mission，organizer decision 为 `keep_default/applied`。
- 三次 assistant 输出均带各自 canonical execution id，输入投影为 `consumed_count=3`，无 pending/queued 输入。

## 定义版本升级证据

- Agent/Team revision store 以解析后的 manifest 与 Markdown 判断语义幂等；新增有默认值的合同字段不会把同一旧
  revision 误判为内容篡改。
- 已存 revision 的原始 digest 和 release assignment 保持不变；真实合同或 Markdown 变化仍返回
  `RevisionConflict`。

## 行为门禁

- 同 Turn 重放先读取 TaskTurnBinding，不重复创建 Root Task。
- 一个 Turn 最多一个 primary binding，复合目标的 additional binding 有界。
- terminal Task 不被隐式 reopen；后续工作创建 successor。
- Delegated Task 必须具有 parent/root/Mission lineage，不能形成独立 Mission 聚类候选。
- 任意 Mission relation 写入返回 422；Task execution graph POST 返回 405。
- graph/activity 缺少 canonical lineage 时在 Runtime owner 边界拒绝。
- Session focus 仅影响后续路由，不回填历史执行或生成 Mission membership。
