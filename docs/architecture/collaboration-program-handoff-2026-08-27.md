# Cowd 自主编排与多 Team 协同交接计划（2026-08-27）

> Historical handoff: superseded by
> `docs/architecture/model-observation-attestation-v0.9.710.md`. The progress and
> “next step” statements below describe the state on 2026-08-27 and are not a
> current release-status source.

## 使用方式

这是给下一个 Session 的连续执行交接，而不是已完成报告。新的执行者应先读
本文件，再读以下权威文档，保持其顺序和边界：

1. `docs/architecture/collaboration-program-hardening.md`（跨版本唯一架构权威）；
2. `docs/architecture/collaboration-semantic-harness-plan-audit.md`（审查结论）；
3. `docs/architecture/collaboration-semantic-harness-v0.9.706.md`；
4. `docs/architecture/collaboration-semantic-harness-v0.9.707.md`；
5. `docs/evidence/collaboration-semantic-compiler-v0.9.705.md`；
6. `docs/evidence/collaboration-capacity-surface-v0.9.706.md`（进行中）。

用户的硬性要求：持续执行，不得把启动命令、单测或中间说明误报为完成；只在
`v0.9.706`、`v0.9.707`、双仓库版本门、真实 Provider/浏览器/多 Team 场景均闭环
后做最终汇报。中间只能在 commentary 给可验证进度，不能发送 `final` 结束工作。

## 目标与已锁定设计

用户可以直接指定任意 Team 与任意角色名称，且不需要匹配本地模板。模型只提交
语义：目标、角色职责、依赖、输入输出、能力/Skill/Tool 需求、基数、验收条件。
Runtime 负责精确解析已发布 Agent Definition、权限、行为、容量、审批、图和持久化
事实。显示名称和自然语言不得驱动执行分支。

关键边界：

- **弹性**：角色/团队名称、职责、依赖图、语义验收、需求的能力/Skill/Tool、展示。
- **刚性**：已发布 Definition revision、Tool/Skill 约束、权限、资源、审批、图提交、
  终态、证据和版本化经验资产。
- **审批**：Trust-All/Autonomous 自动执行并记审计；确认型策略是有界否决窗口，超时
  自动继续；显式 veto 阻断。审批不应成为用户明确 Team 请求的无限阻塞点。
- **容量**：唯一执行排队者是 `ExecutionResourceManager`；Gateway 连接容量不是第二
  调度器。一个 admission 冻结容量/审批快照。
- **经验**：单次成功只能产生 episode/candidate；只有评价、版本化和治理晋升后的资产
  才能成为可执行默认项（v0.9.707）。

## 版本状态

### 已完成：v0.9.705（不可重做）

核心仓库 `cowd-0821-terminal`：

- 语义 v2 Team 合同与确定性 `intent_compiler`；
- 无模型物理 Agent ref、行为 facet、grant 或模板创建；
- 精确 Agent/Capability/Skill/Tool 解析、无 builtin/name/default fallback；
- 语义 provenance 写入 `CollaborationProgram`；
- v2 Gateway narrow tool 入站；
- 核心提交 `4816b5c8`，证据提交 `ee2b0ff4`，注释标签 `v0.9.705` 指向后者。

边缘仓库 `cowd-edge`：

- 生成 Gateway 合同提交 `0a2a018`，注释标签 `v0.9.705`。

已记录的门禁：Runtime library `1852 passed / 0 failed / 2 ignored`、workspace
`cargo check --workspace --all-targets`、Gateway bootstrap 3/3、带认证 OpenAPI 生成、
WebUI API matrix 和 production build 均通过。没有远端 push。

### 进行中：v0.9.706（P9 + 实时 Surface）

核心 worktree 当前**有意未提交**的改动：

| 范围 | 已实现的内容 | 仍缺失 |
| --- | --- | --- |
| 审批 | 编排确认等待改用 `ApprovalCoordinator` 的 Notify wait；deadline scheduler 同时唤醒 coordinator 和 graph supervisor；移除 100ms busy poll | 加入 approval/timeout/veto/cancel race 的专门场景及审计快照 |
| 容量 | `RuntimeControlPolicy.capacity`、`CollaborationCapacityPolicy`、1024 representability guard；Gateway composition 将 policy 传入 RuntimeServices，ResourceManager 从它创建 admission policy；veto window 从 frozen policy 读取 | 将 profile snapshot 贯穿 Program ledger、TeamInstantiationRequest、compiler/validator 的形状限制与 projection |
| 编译/Team | 移除 compiler `unwrap_or(32)`，Team 的旧 static 32 改为 1024 分配保护 | Team 总角色实例数须在具体 Team（而非配置最大值相乘）上校验并冻结 |
| 投影 | schema/reducer 升至 v3；新增 `ReplaceGraphOrchestration`，Runtime 在同一 graph revision 发送，Rust reducer 原子应用 | `SetDeliveryTruth` 完整载荷、Gateway/OpenAPI、TUI、WebUI reducer/生成类型/卡片更新 |

当前核心未提交文件（不得丢弃）：

```text
config-default.yaml
crates/gateway/src/runtime_host/mod.rs
crates/harness-contract/src/projection/{snapshot,delta}.rs
crates/harness-contract/tests/fixtures/projection-v2/materialization.json
crates/runtime/src/{approval/coordinator.rs,execution_core/services.rs,
 infrastructure/runtime_control.rs,lib.rs,orchestration/{compiler,mod}.rs,
 projection/delta.rs,team/instantiation.rs}
docs/evidence/collaboration-capacity-surface-v0.9.706.md
```

已通过的 v0.9.706 定向门禁：approval 57/57、team instantiation 22/22、orchestration
103/103、contract projection 8/8、runtime projection 76/76，以及
`cargo check -p harness-contract -p runtime -p gateway -p tui --all-targets`。

### 未开始：v0.9.707（P10 + 最终 P11）

不要在 v0.9.706 闭合前把 v0.9.707 改动混入索引。其工作是：

- versioned `ExperienceEpisode`、语义 pattern candidate、评估 baseline；
- owner-specific promotion/canary/rollback；
- 防止 episode 直接成为模板、Agent 或默认执行知识；
- Provider + Browser + restart/fault/concurrency 的完整集成验收。

## 当前服务和真实测试环境

截至交接写入时：

- 用户授权停止空闲 Cowd 服务后，已重新通过受管 wrapper 启动；Gateway PID 会变化，
  不要硬编码，必须运行 `/home/yi/.cowd/bin/cowd gateway status`；
- `http://127.0.0.1:8642/api/gateway/openapi.json` 健康检查曾返回 200；
- 该 8642 服务是**已安装旧二进制**，其 OpenAPI 仍是 projection v2，不能用来证明
  当前源码的 v3；
- 直接运行 `target/debug/cowd gateway run` 会因缺少受管 wrapper 的 cgroup v2 环境使
  resident `mfg` worker 失败；必须复用 `/home/yi/.cowd/bin/run-cowd-gateway` 的 cgroup
  环境或等价 service manager；
- 当前持久化 Definition store 含旧 `builtin/cowd/long-running-workstreams@2`。源码版启动
  曾被“same revision, different content”拒绝。不可覆盖/删除已发布 revision。

推荐真实验证环境：保留用户配置、密钥和生产数据；使用独立、可回收的 config/storage
根创建空 Definition store，给该实例配置不同端口，且在受管 cgroup wrapper 中运行当前
源码二进制。先验证它能 bootstrap 所有 builtin revisions，再用其 OpenAPI 生成 edge API。
不要用旧 8642 的 v2 文档更新 v3 前端。

## 后续严格执行顺序

1. **冻结 v0.9.706 board**：确认 core/edge worktree 仅含本计划文件列出的 v0.9.706
   变化；为任何新生产文件先写 allowlist amendment。
2. **补完容量快照**：以 `CollaborationCapacityPolicy` 解析不可变
   `ExecutionCapacityProfile`（id/revision/digest/数值）；冻结到 Program ledger 和 Team
   请求；具体 graph 计算 cardinality total，超载返回 typed diagnostic，不静默裁剪。
3. **补完审批场景**：trust-all auto receipt、confirmation timeout auto-approve、explicit
   veto、veto-vs-timeout、cancel-vs-timeout、restart waiter，无泄漏 waiters/permits。
4. **补完 projection v3**：实现 delivery truth，Gateway OpenAPI、TUI protocol/reducer、
   WebUI generated API、`executionProjection.ts`、Program card 和 i18n 同步。所有 consumer
   在 schema/reducer mismatch 必须请求 resync，不能局部降级。
5. **干净源码 Gateway**：解决隔离 config/home/cgroup 启动；用实际 v3 endpoint 运行
   `npm run generate:api`（通过安全读取本机 `gateway.platforms[].auth.token` 注入
   `COWD_API_TOKEN`，绝不打印 token）。
6. **WebUI/TUI**：更新生成合同后跑 unit/contract/i18n/governance/build；浏览器验证任意
   中文角色名、无模板、多 Team、三次以上连续 revision、审批倒计时/auto receipt、终态。
7. **v0.9.706 close**：版本升 `0.9.706`、双仓库独立 commit/annotated tags；不 push。
8. **实施 v0.9.707**，再运行最终 provider/browser/concurrency/restart/fault/performance
   门禁；失败必须归属到拥有该能力的版本后修复、重测。

## 常用安全命令与禁止事项

```bash
# 状态与健康
/home/yi/.cowd/bin/cowd gateway status
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8642/api/gateway/openapi.json

# 核心阶段质量门
cargo fmt --all -- --check
cargo check -p harness-contract -p runtime -p gateway -p tui --all-targets
git diff --check

# 认证 OpenAPI 生成（token 只能在 shell 内注入，禁止回显）
# 在 cowd-edge/surfaces/webui：
# COWD_API_TOKEN="$(node ...仅解析 /home/yi/.cowd/config.yaml...)" npm run generate:api
```

禁止：`git reset --hard`/`git checkout --`、删除持久化数据库或密钥、覆盖已发布 revision、
把 v2 旧服务当 v3 证据、硬编码角色名称/模板/模型行为、在中间阶段发 `final`。

## 版本门与交接要求

每个版本关闭前应用 `immersive-loop-programming` 与 `commit-version-gate`：allowlist、
删除旧路径、调用者重连、静态 residual scan、依赖锥与完整回归、证据、双仓库独立提交和
annotated tag。未经用户明确授权不得 push。

最终报告必须包含：两个仓库 commit/tree/tag、所有 test/build/E2E 命令与结果、Provider
配置/认证是否真实成功、浏览器观察到的 revision/终态、性能与并发指标、剩余风险（理想为
零）及可回滚点。
