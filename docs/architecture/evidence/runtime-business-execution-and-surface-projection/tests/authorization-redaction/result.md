# 授权与脱敏测试

- 命令：`cargo test -p harness-contract -p runtime -p gateway -p tui --all-features --quiet`
- 退出码：`0`
- 覆盖：
  - activity summary/detail 使用现有 authorization/redaction scope；
  - 私有思维链不进入 public reasoning summary；
  - 原始 input/output/evidence 通过 detail capability 按需下钻；
  - authorization/redaction revision 进入 projection consumer/cache identity；
  - ApprovalQueue execution index 不绕过审批所有权。

最终结果：通过。
