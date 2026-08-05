# Runtime 身份测试

- 命令：`cargo test -p harness-contract -p runtime -p gateway -p tui --all-features --quiet`
- 退出码：`0`
- Runtime：1426/1426 单元测试通过，后续 integration batches 全部通过。
- 关键断言：
  - Team/Agent graph node 与 run identity 归并为一个 activity；
  - Team、Agent、Skill、Tool 缺少必需 binding 时事件原子拒绝；
  - 旧 revision/fence 不覆盖新状态；
  - tool_call、skill_activation、agent_run、team_run 反查 activity 使用 typed index。

最终结果：通过。
