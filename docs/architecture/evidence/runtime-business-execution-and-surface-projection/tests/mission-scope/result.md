# Mission/Task 范围测试

- 命令：`cargo test -p harness-contract -p runtime -p gateway -p tui --all-features --quiet`
- 退出码：`0`
- 覆盖：
  - selected Mission 仅包含自身 Task/Team/Agent/Session；
  - 同一 Mission 可关联多个 Session；
  - Session 当前 membership 不改写历史 Execution 归属；
  - Mission graph 复用现有 materialized projection；
  - digest/recovery 使用 mission/root indexed data，不用全局近似窗口；
  - Task graph refs 在 SQLite/PostgreSQL 重启后保持。

最终结果：通过。
