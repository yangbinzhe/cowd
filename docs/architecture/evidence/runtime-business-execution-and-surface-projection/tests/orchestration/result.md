# 编排测试

- 命令：`cargo test -p harness-contract -p runtime -p gateway -p tui --all-features --quiet`
- 退出码：`0`
- 覆盖：
  - 顶层和错误层级 `input_refs` 返回 typed contract error，不提交副作用；
  - 双 Team、并行 Agent、并行 Tool 编译和执行；
  - 前驱 result/artifact/evidence 由 Runtime 解析；
  - Session dispatch 使用显式 `target_session_id`；
  - required evidence、focus、resource、multiplicity、completion、
    cancellation 和 control 合同保持；
  - 参数修复由 Conversation Runtime 所有，Gateway/compiler 不创建第二模型循环。

代表性测试：

```text
team_runtime_compiles_parallel_agents_and_emits_one_verified_terminal_result
fanout_team_uses_runner_parallelism_without_a_team_scheduler
session_dispatch_routes_into_the_real_target_session_stream_without_fabricating_a_result
```

最终结果：通过。
