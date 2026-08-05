# 差异范围审计

版本：`0.9.640`

## Core

变更集中在以下所有权边界：

1. `harness-contract`：模型编排合同、Activity V3 identity 和 delta。
2. `runtime`：编排解析、Team/Agent/Skill/Tool/Approval writer、事件索引、
   canonical activity、detail、Mission/Task scope、recovery。
3. `runtime-postgres`：与 SQLite 语义一致的 root/activity/mission/task 索引和查询。
4. `gateway`：类型派生 schema、typed input failure、Mission scoped consumer。
5. `tui`：同一 canonical activity 的紧凑 Team/Agent/Skill/Tool 投影。
6. 版本、生成合同引用、架构方案和证据。

没有修改 Memory/Matrix/Reality、Provider、Tool 实现、Surface transport 或其他业务所有者。

## Edge

变更集中在：

1. WebUI canonical activity adapter 与 lineage；
2. 正文活动树、业务图和右侧 business/technical 生命周期；
3. live transport 的 cursor/revision 收敛；
4. i18n、类型、测试和最终 hash assets；
5. Edge/MFG 版本与 source lock。

Connectors 仅同步 manifest 版本，没有修改协议和业务逻辑。

## 非计划修改分类

未发现无法解释的修改。MFG 只做版本统一并已独立提交；Core 和 Edge 的所有未提交文件均属于
W0-W7 或最终生成物。没有 mixed index/worktree 状态，也没有其他写入进程。

## 审计门禁

- `git diff --check`：Core/Edge 均通过。
- `cargo fmt --all -- --check`：Core/Edge 均通过。
- OpenAPI generation check：通过。
- production residual scan：零真实残留。
- TODO/stub/fake scan：未发现本版本新增占位实现。
