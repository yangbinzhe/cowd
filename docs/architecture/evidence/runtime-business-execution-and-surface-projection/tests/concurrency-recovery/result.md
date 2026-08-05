# 并发、恢复与慢消费者测试

- 命令：`cargo test -p harness-contract -p runtime -p gateway -p tui --all-features --quiet`
- 退出码：`0`
- 覆盖：
  - Team fanout 和 Tool batch 真实并行；
  - 调度 parallel group 来自 compiler/supervisor；
  - 慢服务不降低全局 capacity，graph host 不等待慢 Surface；
  - cursor gap 触发有界 resync；
  - 重启、receipt、approval waiting、cancel、replan 和旧 fence；
  - SQLite/PostgreSQL 语义一致。

真实 PostgreSQL 命令：

```text
COWD_TEST_POSTGRES_URL=postgres://... cargo test -p runtime-postgres \
  --all-features -- --ignored --test-threads=1
```

结果：3/3 真实 PostgreSQL 场景通过，临时容器随后销毁。
