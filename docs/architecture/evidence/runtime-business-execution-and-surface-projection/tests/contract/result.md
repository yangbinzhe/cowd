# 合同测试

- 命令：`cargo test -p harness-contract -p runtime -p gateway -p tui --all-features --quiet`
- 退出码：`0`
- `harness-contract`：133/133 通过。
- `gateway`：696 通过，9 项明确标记的外部环境测试忽略。
- 关键断言：
  - 模型 schema 不含 `input_refs`、lease、binding、grant 和物理 ID；
  - Tool schema 与 typed fixture 来自同一 Rust 合同；
  - Activity V3 包含精确 Team/Agent/Skill/Tool identity；
  - snapshot/delta DTO 保持同一 identity/status 语义。

最终结果：通过。
