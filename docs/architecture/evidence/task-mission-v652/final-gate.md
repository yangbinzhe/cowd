# v0.9.652 最终门禁

## 必须同时成立

- [x] Session、Task、Mission 所有权无重叠。
- [x] 普通消息进入同一 Task Router，不要求 Surface 创建 Task。
- [x] Root/Delegated、Turn binding、Mission assignment 和 canonical lineage 均可反向追溯。
- [x] SQLite 与 PostgreSQL 合同等价，迁移只向前追加。
- [x] Gateway/TUI/WebUI 只消费同一 API/投影合同。
- [x] 旧 Mission membership、任意 relation writer、旧权限枚举和旧 Task 模块已删除。
- [x] Core Rust 全工作区类型检查和 owner 测试通过。
- [x] Edge generated contract、单元测试和 production build 通过。
- [x] 真实 PostgreSQL + DeepSeek 在正常 Tokio 线程栈下连续执行三个 Turn，并复用同一 Root Task。
- [x] 旧定义 schema 与旧 Task 数据均通过不可变、幂等的前向升级进入终态。

发布前还必须执行版本一致性、git diff 审核、release gate、双仓中文 commit/tag/push；结果记录在版本提交和
`plan/0808-Cowd权限执行投影与Mission聚类终态治理/05-V652实施证据.md`。
