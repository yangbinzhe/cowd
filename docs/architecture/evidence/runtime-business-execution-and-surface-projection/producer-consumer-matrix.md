# 生产者与消费者接线证据

版本：`0.9.640`

| 终态合同 | 权威生产者 | 持久化/投影 | 实际消费者 | 接线证据 |
|---|---|---|---|---|
| 模型编排输入 | `harness-contract::orchestration` | Gateway 从 Rust 类型生成 schema | Gateway tool executor、Runtime command resolver | 合同测试拒绝 `input_refs` 和 Runtime-owned 字段；OpenAPI generation check 通过 |
| Runtime resolved command | `runtime::orchestration::request` | 原子 graph mutation | compiler、validator、session dispatch | `target_session_id` 为显式字段；依赖、证据和前驱产物由 Runtime 解析 |
| `RuntimeActivityBinding` | Graph、Team、Agent、Skill、Tool、Approval writers | Runtime event store typed columns/index | activity reducer、detail、recovery | kind-specific writer invariant 测试通过；缺少身份的事件原子拒绝 |
| canonical activity | Runtime activity reducer | snapshot/delta/replay | Gateway、WebUI、TUI | snapshot/delta/replay 测试通过；Surface 不再合并 Session 业务拓扑 |
| activity detail | Runtime scoped reader | root/activity indexed event query | Gateway detail route、WebUI drawer | detail 不构建 full snapshot；按 activity identity 查询 |
| Mission/Task scope | Mission/Task Runtime | 现有 Mission Control materialized projection | Mission service、WebUI/TUI | selected Mission 隔离、跨 Session membership 和 durable recovery 测试通过 |
| summary/full 生命周期 | Runtime projection cache/Gateway | summary/full consumer registry | WebUI Companion panel | 业务模式 summary；技术模式才 acquire full；离开后 release |
| live execution update | Runtime live store | bounded cursor/revision stream | WebUI live transport、TUI | 乱序/重复拒绝、断线重连和 terminal 收敛测试通过 |

## 反向追溯

```text
WebUI/TUI node
  -> activity_id
  -> RuntimeActivityBinding
  -> durable runtime event / graph node / run identity
  -> resolved orchestration command
  -> admitted Session Turn
```

不存在由 Surface 根据时间、ID 前缀或相邻事件补造父子关系的生产路径。
