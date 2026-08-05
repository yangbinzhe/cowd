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
8. WebUI 打开历史对话后只为可见 Turn 获取 summary projection。
9. Writer promotion 与消息 mutation 共用串行临界区，不用失败重试掩盖跨标签竞态。

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

本证据不把单元测试耗时冒充 UI p95，也不写没有采样的延迟数字。真实 Gateway
发布浏览器门禁 22/22 通过，完整运行耗时 24.4s；其中包含首屏、summary/full、
live/history、双观察者和三 Surface 策略呈现。
