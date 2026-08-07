# Runtime 状态放大治理基线证据

## Snapshot

- 日期：2026-08-07
- Core HEAD：`b44037f748fd8c83a6a04aa524bf33587296d07f`
- Core tree：`cb2737762d74ff05b4fa29994b29189101255726`
- Edge HEAD：`e403539cdf6c817e1a12fa7d1b58215c94cbd737`
- Edge tree：`2d0ddfee540bfc5f10a4bb680da6d61c8a48b739`
- Edge dirty diff：`f677d697ad3a8c2c76bdca62d8c4075365b32135be85a031762ff75ade51d73b`

## PostgreSQL

```text
session_messages=35
runtime_events=3371
runtime_commits=3035
runtime_transaction_streams=3287
runtime_events_size=10 MB
database_size=27 MB
```

```text
knowledge.candidate.projector.checkpoint.v1=757
evolution.signal.projector.checkpoint.v1=719
execution.live.snapshot.v1=544
```

projector checkpoint 与 live snapshot 共 2030 条，占 Runtime event 的 60.2%。

## 基线判断

1. Runtime 主表无显著 dead tuple，信息放大是写入模型问题。
2. projector cursor、live latest、reader presence 属于 mutable projection，不是
   immutable business fact。
3. 原始事实、审批、外部副作用、终态和内容寻址 artifact 必须保留。
4. `cowd-edge` 已有未提交图形改动，实施不得回退或覆盖。

## 执行时复测

实施期间旧 Gateway 仍在运行，因此样本继续增长到：

```text
runtime_events=6538
runtime_commits=5886
runtime_transaction_streams=6379
projector_checkpoint_events=2959
execution.live.snapshot.v1=1013
```

两类派生事件共 3972 条，占 Runtime event 的 60.75%。数据库同时存在两组语义完全
重复的索引：

```text
runtime_commits:
  runtime_commits_pkey
  idx_runtime_commits_cursor

session_messages:
  session_messages_session_id_sequence_key
  idx_session_messages_session_sequence
```

该复测数据是迁移前证据，不是新实现产生的数据。
