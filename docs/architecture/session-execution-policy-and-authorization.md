# Session 执行策略与授权架构

本文定义 Cowd 运行时权限的唯一业务链。它解决的不是“怎样少弹审批”，而是让任务尽可能连续完成，同时让每一次能力扩大、外部副作用和人工决策都有明确边界与证据。

## 1. 设计结论

1. `Runtime` 是执行策略、工具授权和审批协调的唯一内核所有者。
2. `Gateway` 负责 Session 策略的持久化、并发更新、API 和 Surface 接入，不执行第二套授权判断。
3. `WebUI`、`TUI`、Edge Surface 只读取或提交策略，不保存自己的默认权限真相。
4. Session 策略由四个同步维度和一个单调 revision 组成。UI 预设是四个维度的原子模板，避免各层独立推导后漂移。
5. 子 Agent 的 Packet 能力上限不可被 Session 的后续提权隐式扩大；需要更大能力时，由父级显式重新规划或创建新 Agent。
6. 降权在下一个授权检查点立即生效。已提交的外部副作用保留证据，不伪造回滚。
7. 浏览器 writer、工具权限、审批、操作系统权限和 Provider 故障是五类不同问题，必须分别报告。

### 1.1 完整权限栈

Cowd 不使用一个无限膨胀的“权限对象”处理所有问题。每一层只回答自己的问题，并按以下顺序收紧能力：

```text
Credential / AuthBroker
  身份是谁，core/app profile 授予哪些 Gateway capability
                    |
                    v
Gateway route + Session access
  能否访问接口，能否读取这个 Session
                    |
                    v
Surface attachment + writer lease
  谁能修改同一个 Session，谁只能观察
                    |
                    v
SessionExecutionPolicy
  本 Session 的自治、工具上限、审批和中断意图
                    |
                    v
Agent packet + resource scopes
  子 Agent 继承后还能继续收紧到什么范围
                    |
                    v
Tool effect + PermissionPolicy
  本次具体调用是 allow、ask 还是 hard deny
                    |
                    v
ApprovalCoordinator + revision-fenced Grant
  谁批准、批准多久、对哪些资源和副作用有效
                    |
                    v
OS sandbox / external credentials
  操作系统和外部服务最终是否允许执行
```

| 控制面 | 唯一所有者 | 不能替代的边界 |
|---|---|---|
| 身份与 capability entitlement | `auth-broker` + Gateway auth middleware | 不决定工具效果风险 |
| Session 读写访问 | Gateway Session API | 不扩大 Runtime 工具能力 |
| writer/observer 仲裁 | Gateway admission/lease | 不等同审批 Grant |
| Session 执行策略 | Runtime live control，Gateway 持久化 | 不替代 OS 权限或 Provider 余额 |
| Agent/Team 委派上限 | Runtime Packet/resource scope | 不能超过父 Session |
| 工具效果与权限规则 | Runtime PermissionPolicy | 不负责浏览器 writer |
| 审批与 Grant | Runtime ApprovalCoordinator/Queue | 不得绕过 hard deny、Secret 和 revision |
| OS/外部系统 | sandbox、文件系统、Provider/Connector | 失败必须按真实来源报告 |

## 2. 唯一运行链

```text
Config defaults
      |
      v
SessionExecutionPolicy (durable, revisioned)
  autonomy_profile
  permission_mode
  approval_profile
  interruption_policy
      |
      +---- Gateway GET/PUT + /permissions + config hot reload
      |
      v
SessionExecutionPolicyControl (one atomic live snapshot)
      |
      +-----------------------------+
      |                             |
      v                             v
ConversationRuntime           AgentTaskPacket
approval/autonomy             immutable permission ceiling
      |                             |
      +---------------+-------------+
                      v
        PermissionPolicy = min(live Session, child ceiling)
                      |
           allow / ask / hard deny
                      |
              ApprovalCoordinator
                      |
     +----------------+----------------+
     |                |                |
 deterministic     bounded         human decision
 low-risk policy   steward         via ApprovalQueue
     |                |                |
     +----------------+----------------+
                      v
         revision-fenced ApprovalGrant
                      |
                      v
              governed tool effect
```

所有运行中的权限、自治和审批强度都从同一个 `SessionExecutionPolicyControl` 快照读取，避免“权限已切到 YOLO，但审批仍按 supervised”这类撕裂状态。
活跃 control 的注册表只属于 `RuntimeServices`；Gateway 仅保留持久化/未激活 Session 所需的策略热缓存，不维护第二个活跃 control 索引。
`interruption_policy` 是同一策略快照中的中断意图，由既有授权检查点、审批协调和重规划链执行，不再建立一套独立中断状态机。

## 3. 策略合同

`harness-contract::policy::SessionExecutionPolicy` 是跨层合同：

| 字段 | 作用 |
|---|---|
| `autonomy_profile` | AI 在遇到障碍时可自行继续、改路或升级的强度 |
| `permission_mode` | 工具能力上限：只读、工作区写入、完整能力 |
| `approval_profile` | 已知风险由人工或托管策略处理的强度 |
| `interruption_policy` | 何时暂停当前执行等待人类 |
| `revision` | Session 内单调递增的策略版本 |
| `origin` | 配置默认、Session 显式、Surface 命令或恢复重规划 |

内置预设：

| 预设 | 工具上限 | 审批 | 中断 |
|---|---|---|---|
| `cautious` | `read-only` | `supervised` | 总是等待人类 |
| `supervised` | `workspace-write` | `balanced` | 风险时暂停 |
| `solo` | `danger-full-access` | `autonomous` | 审计后继续 |
| `yolo` | `danger-full-access` | `autonomous` | 仅无法安全继续时阻断 |
| `stewarded` | `workspace-write` | `autonomous` | 审计后继续 |

四个维度不精确匹配任何预设时，Surface 必须显示“自定义”，不得猜测成某个预设。接口不可用时必须显示“不可用”，不得用占位策略冒充真实状态。

## 4. 父子能力不变量

```text
Session live ceiling: danger-full-access
              |
              v
Agent packet ceiling: read-only
              |
              v
effective Agent ceiling: read-only
```

Session 提权只能提高父运行环境的可用上限，不能篡改已经编译到 Agent Packet 中的最小权限原则。父级可以用现有编排能力重新分派到具备能力的 Agent，或以新 Packet 创建实例。Session 降权则会压低所有子 Agent 的有效上限。

该规则同时保护 Mission、Team 和多 Agent 协作：协作规模增加不等于权限自动扩大。

## 5. 工具授权与智能审批

`PermissionPolicy` 先处理：

1. 显式 deny 规则：确定性拒绝，不允许 AI 绕过。
2. 当前 Session 与 Agent Packet 上限。
3. 工具自身要求和显式 ask 规则。
4. 已匹配的 Approval Grant。

需要审批时，`ApprovalCoordinator` 按工具效果描述符判断：

- 已知低风险的读取或网络访问，且不触及 Secret、系统、包管理或外部写入：确定性自动批准一次并记录证据。
- `autonomous` 审批模式下，已知可逆或可补偿的中风险动作：可由受限 Steward 批准一次。
- 用户/管理员专属、Secret、外部写入、系统修改、不可逆或未知效果：必须人工决定。

AI 可以提出权限请求和调整执行路径，但不能修改硬边界、伪造 Grant 或把未知副作用降级成低风险。

## 6. Approval 的业务隔离

审批按领域分类：`execution`、`knowledge`、`evolution`、`skill`、`application`、`system`。

- Chat 只主动弹出当前 Session、`execution` 领域且 `blocks_execution=true` 的请求。
- 知识晋升、Skill 发布和进化评审进入各自工作台，不打断当前对话。
- 全局顶部入口仍可查看所有当前 Principal 有权处理的请求。

Grant 不因 scope 为 `global` 就变成无限权限。任何 Grant 都绑定 Principal、能力、资源、效果摘要和签发时的 `policy_revision`。Session 策略变化后，旧 revision 的 Grant 不再命中。

## 7. writer 与工具权限不是一回事

```text
Browser/TUI writer ownership       Runtime tool authorization
----------------------------       --------------------------
谁能修改同一个 Session             该执行能调用什么工具
Gateway admission boundary         Runtime authorization boundary
observer + attachment + lease      policy + packet + grant
409/403 writer error               typed allow/ask/deny result
```

多个 Surface 可以同时观察一个 Session，但同一时刻的变更由 writer admission 仲裁。获得 writer 不会自动获得文件、网络或系统权限；失去 writer 也不代表运行中的 Agent 被降权。

Gateway capability entitlement 还位于 writer 之前：Principal 即使拥有 Session writer，也只能调用其 core/app profile 暴露的接口；反过来，拥有管理 capability 也不会自动抢占另一个 Surface 的 Session writer。

## 8. 更新与恢复语义

- `PUT /api/sessions/:id/execution-policy` 必须携带 `expected_revision`，并要求当前 Surface 是 writer。
- 更新先持久化到 Session metadata，再原子替换活跃 Runtime 的完整策略快照。
- 同 revision 不同内容被拒绝；旧 revision 更新返回 `409 Conflict`。
- 配置热加载只推进 `origin=config_default` 的 Session；显式选择的 Session 保持不变。
- 某个默认 Session 持久化失败时保留旧策略并报告 `attention`；配置指纹未变化时 watcher 只重试失败的策略同步，不重复重载 Provider、MCP 或工具快照。
- 未激活 Session 在下次激活时应用持久策略。
- 活跃执行在下一个工具授权或审批检查点读取新策略。
- 提权后，受 Packet 限制的子 Agent 必须显式重新规划；不创建隐式的第二套“旧节点重编译器”。

## 9. 代码归属

| 层 | 主要实现 |
|---|---|
| 合同 | `crates/harness-contract/src/policy/` |
| Runtime 策略 | `crates/runtime/src/policy/permissions.rs` |
| Runtime 审批 | `crates/runtime/src/approval/` |
| Agent 限制 | `crates/runtime/src/agent/in_process_worker.rs` |
| Runtime 聚合控制 | `crates/runtime/src/execution_core/services.rs` |
| Gateway 持久化/API | `crates/gateway/src/runtime/runtime_service.rs`、`api_routes/session_routes.rs` |
| Surface 命令 | `crates/gateway/src/services/slash_controller.rs` |
| WebUI | `cowd-edge/surfaces/webui/src/pages/ChatPage.vue`、`ApprovalInbox.vue` |
| TUI | `crates/tui/src/gateway/`、`components/status_bar.rs` |

Edge 连接器和消息 Surface 只携带 Gateway Session 身份，不实现独立权限默认值或审批队列。

## 10. 不允许重新引入的设计

- 在 WebUI、TUI 或 Edge 保存第二份有效执行策略。
- 仅切换 `permission_mode`，却不同时更新审批和中断语义。
- 让 Session 提权隐式扩大既有子 Agent Packet。
- 用 writer lease 代替工具授权，或用工具权限错误解释 Provider/OS 故障。
- 让 knowledge/evolution/skill 审批自动弹入 Chat 阻断当前执行。
- 使用 revision 为零的 Session 执行 Grant 绕过策略变化。
