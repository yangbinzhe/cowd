# Cowd 文档

本目录只保存当前产品有效的系统说明、架构合同、API 参考和运维手册，不保存版本计划、实现过程、智能体工作状态、临时验证记录、历史规划归档或本地运行产物。

## 系统说明书

- [HTML 总入口](manual/index.html)：默认中文，可切换英文和明暗主题。
- [架构与边界](manual/architecture.html)
- [Runtime 与协同执行](manual/runtime.html)
- [Reality、记忆与 Matrix](manual/reality.html)
- [Gateway、API 与 Surface](manual/gateway.html)
- [快速使用与运维](manual/operations.html)

说明书基于 Core `v0.9.672`、提交 `ce8f972d` 的源码、配置、API、测试和活跃架构文档。机器接口真相仍以 `GET /api/gateway/capability-contract`、OpenAPI 投影和源码 route registry 为准。

## 活跃文档边界

当前保留的文档域：

- `api/`：生成的 API 参考与稳定 HTTP 合同。
- `architecture/`：当前有效的架构、边界和模块关系。
- `operator/`：操作者手册和运行就绪说明。

### 应用架构

- [应用开发与产品组装](architecture/application-development-and-product-composition.md)：多 APP 所有权、源码锁定、开发/发布模式、产品组装和验收规则。
- [APP 激活与构建](architecture/app-activation-and-build.md)：已编译 APP 的统一启用与构建行为。
- [Session、Task 与 Mission 治理](architecture/session-task-mission-governance.md)：对象所有权、路由、权限、持久化和投影合同。
- [Session 执行策略与授权](architecture/session-execution-policy-and-authorization.md)：Session 策略、Agent 能力上限、审批、授权、writer 和实时 revision 边界。
- `architecture/evidence/task-mission-v652/`：Task/Mission 终态实现的历史验收证据，只作为追溯材料。

### 存储

- [存储治理](architecture/storage-governance.md)：全进程 SQLite/PostgreSQL 选择、共享连接池、APP schema、迁移 hook 和 `plan → migrate → verify → cutover` 过程。
- [Runtime 性能与缓存](architecture/runtime-performance-and-cache.md)：热路径、Provider 准入、PostgreSQL workload lane、Skill/Tool 缓存和 MCP 生命周期。

### Gateway 运维

- [Gateway 生命周期](operator/gateway-lifecycle.md)：安全启动、停止、重启、二进制替换、授权状态迁移和单实例验证。
- [Session 权限与审批](operator/session-permissions-and-approvals.md)：配置、检查、修改和排查执行策略与审批。

### Gateway API

- [Gateway API 参考](api/gateway-api-reference.md)：由 `crates/gateway/src/api_routes/**/*.rs` 生成的全量路由清单。
- [Gateway API 框架](architecture/gateway-api-framework.md)：接口架构、路由族关系和主要执行链。
- [能力合同终态方案](architecture/gateway-capability-contract-terminal-plan.md)与[落地证据](architecture/gateway-capability-contract-terminal-evidence.md)。
- Gateway 运行时能力真相：`GET /api/gateway/capability-contract`。
- 机器投影：`GET /api/gateway/openapi.json` 与 `GET /api/gateway/openai-tools`。
- WebUI 和 TUI 通过能力合同发现功能；业务 API 只负责执行，不重复维护能力清单。
