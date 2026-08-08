# PostgreSQL 迁移不可变性证据

本版本没有重写已发布 migration 1、2、6、9。新增变化按前向迁移表达：

- migration 16：为 `session_runtime_outbox` 增加 `task_route_hint_json`；
- migration 17：重建当前 recovery 逻辑，并删除已经终止的 Session Mission outbox 表。

验证包括：

1. 旧 schema 升级到 17；
2. 重复运行 upgrade 幂等；
3. SQLite snapshot 导入 PostgreSQL 前完整校验；
4. 失败事务不留下部分 Task/binding/organizer 状态；
5. 配置 PostgreSQL 时不静默回退 SQLite。

用户数据库检查结果：migration 15/16/17 存在；旧 outbox 表不存在；typed hint 列存在一次。

Runtime Task store 另以独立 migration ledger 追加 migration 6：把旧 `source_session_id/source_turn_id` Task
前向升级为锁定 Root Task，并按同一 Turn 唯一 primary 规则生成 primary/additional binding。该迁移不修改既有
migration 1-5，重复执行不新增 Task 或 binding。
