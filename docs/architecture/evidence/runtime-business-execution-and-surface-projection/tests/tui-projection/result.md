# TUI 投影测试

- 命令：`cargo test -p harness-contract -p runtime -p gateway -p tui --all-features --quiet`
- 退出码：`0`
- TUI：1050/1050 通过，7 项显式外部环境测试忽略。
- 覆盖：
  - canonical Team/Agent tree；
  - Tool/Skill count；
  - projection/live revision 单调门禁；
  - terminal 状态不回退；
  - Mission materialized projection 继续使用原数据源。

最终结果：通过。
