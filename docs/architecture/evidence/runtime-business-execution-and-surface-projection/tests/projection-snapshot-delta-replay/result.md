# 投影等价测试

- 命令：`cargo test -p harness-contract -p runtime -p gateway -p tui --all-features --quiet`
- 退出码：`0`
- 覆盖：
  - fresh snapshot、delta reduce、durable replay 的 activities/relations 等价；
  - 单活动事件使用 identity reducer，不 Replace 全部 activities；
  - terminal snapshot 与 live activity 使用相同 identity；
  - cursor/revision 乱序和重复更新被拒绝；
  - activity detail 走 activity/root scoped index，不构建 full snapshot。

代表性测试：

```text
projection_delta_materializes_the_same_state_as_a_fresh_snapshot
canonical_outcome_covers_direct_and_parallel_tool_turns_without_graph_ref
```

最终结果：通过。
