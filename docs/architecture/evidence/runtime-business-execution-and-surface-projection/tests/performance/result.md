# 性能结构门禁

版本：`0.9.640`

## 已验证

1. Activity detail 不调用 full execution snapshot。
2. 单 activity lifecycle event 使用 identity reducer。
3. strategy 只读取四种精确 kind，不读取整个 root history。
4. Mission digest、related entities 和 scope projection 不使用生产 `all_events(N)` 过滤。
5. WebUI business 模式只 acquire summary，technical 模式才 acquire full。
6. 右栏离开 activity tab 或卸载时 release consumer。
7. topology 未变化时 GraphSurface patch 状态，不重复布局。

## PostgreSQL 实际查询计划

在 `postgres:16-alpine` 临时实例上执行真实 migration、读写、重启和 EXPLAIN：

```text
root execution + event kind:
  Index Scan using idx_runtime_events_root_kind_commit

activity identity:
  Index Scan using idx_runtime_events_activity_commit
```

真实 PostgreSQL 测试：3/3 通过，总测试耗时 1.89s。

## 不虚构的指标

本证据不把单元测试耗时冒充 UI p95，也不写没有采样的延迟数字。真实浏览器的首屏、
summary/full 请求和 live/history 场景由提交后的 release browser gate 记录。
