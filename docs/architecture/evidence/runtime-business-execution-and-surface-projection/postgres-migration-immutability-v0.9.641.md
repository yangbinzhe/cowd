# PostgreSQL 迁移不可变性补丁证据

版本：`0.9.641`

## 问题

`v0.9.640` 已通过全新 PostgreSQL 数据库测试，但真实安装升级时发现
`runtime_event.0001.initial` 的 SQL 被追加了
`root_execution_id` 与 `activity_id`。历史迁移的内容变化导致既有
`cowd_schema_migrations` 账本校验失败，Gateway 按设计拒绝启动。

## 修复

- 恢复 `runtime_event.0001.initial` 的历史 SQL，不修改既有迁移。
- 保留 `runtime_event.0010.activity-identity-index` 作为唯一前向演进 owner：
  增加两列、回填 Runtime activity binding，并建立三个索引。
- 增加 `runtime_event_initial_migration_remains_immutable` 回归测试，将初始迁移
  校验和固定为
  `c29d153132dcd497b6665b9f7a1cbe376d5ce1f39f2f37db308963bb1bc3bd3d`。

## 真实升级证据

在 Gateway 停止且使用本地 PostgreSQL 正式配置的情况下执行：

```text
cowd storage upgrade
status: completed
backend: postgres
cowd_version: 0.9.641
```

升级后迁移账本：

```text
runtime_event.0001.initial                  version 1   checksum c29d1531...
runtime_event.0010.activity-identity-index  version 10  checksum 4bc5743b...
runtime_task.0003.graph-reference-index     version 3   checksum 39220411...
```

结构验证：

```text
runtime_events.activity_id       text
runtime_events.root_execution_id text

idx_runtime_events_activity_commit
idx_runtime_events_root_execution_commit
idx_runtime_events_root_kind_commit
```

升级后真实数据计数：

```text
runtime_events  376038
runtime_commits 358206
```

该结果证明修复没有绕过迁移账本、没有重建数据库，也没有丢失既有 Runtime
事件；新索引能力由前向迁移完整接线。
