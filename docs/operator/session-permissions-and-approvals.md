# Session 权限与审批使用手册

本手册用于配置、查看、切换和排查 Cowd 的 Session 执行权限。

## 1. 默认配置

```yaml
permissions:
  default_mode: "workspace-write"
approval:
  profile: "balanced"
  low_risk_timeout: "auto_approve_once"
```

`permissions.default_mode` 和 `approval.profile` 共同生成默认 Session 策略。配置热加载只更新仍使用配置默认值的 Session；用户显式修改过的 Session 不会被覆盖。
若个别 Session 的策略持久化暂时失败，Gateway 会保留该 Session 的旧策略、显示 `attention`，并在后续自动轮询中只重试这部分同步。

建议：

- 本地可信开发环境需要连续自主执行时可显式使用 `danger-full-access + autonomous`。
- 需要人工把关写入时使用 `workspace-write + balanced`。
- 检查不可信项目时使用 `read-only + supervised`。

### 1.1 先判断是哪一种权限

| 看到的错误或状态 | 应检查的位置 |
|---|---|
| credential、core/app profile、capability denied | `auth-broker` 与 Gateway 身份授权 |
| Session forbidden/not found | Gateway Session 访问范围 |
| observer、writer conflict、lease lost | Surface attachment 与 writer lease |
| tool hard deny、permission ceiling | Session 策略、Agent Packet、工具规则 |
| pending approval | Runtime 审批队列与 Grant |
| OS permission denied | 工作区、sandbox、文件属主 |
| Provider `402/429`、Connector 凭据错误 | 外部服务配置；不要改 Session 权限 |

查看当前凭据授权：

```bash
printf '%s\n' "$COWD_API_TOKEN" | cowd auth profile show
```

core/app profile 管理的是 Gateway 能力入口，不会直接改变 Session 的 `permission_mode`；Session 预设也不会反向授予 APP 管理能力。

## 2. WebUI

聊天输入框下方的“执行模式”状态显示当前 Session 的真实策略：

1. 点击状态打开策略面板。
2. 查看工具上限、审批强度、revision 和来源。
3. writer 页面可以选择谨慎、监督、独立、全自主或托管。
4. observer 页面可以查看，但不能修改。
5. “自定义”表示四个维度不匹配内置预设；“不可用”表示 Gateway 未返回可信状态。

顶部审批入口显示全局待处理项。Chat 中仅自动弹出会阻塞当前 Session 执行的审批，其他治理审批应在对应工作台处理。
Chat 和 Runtime 工作台都以平铺按钮选择 `once/turn/task/session/global`，默认 `once`，不使用语义隐蔽的下拉框。

## 3. TUI 与 Slash

查询当前策略：

```text
/permissions
```

切换预设：

```text
/permissions cautious
/permissions supervised
/permissions solo
/permissions yolo
/permissions stewarded
```

兼容的权限标签只作为命令输入别名，最终仍写入完整 Session 策略：

```text
/permissions read-only
/permissions workspace-write
/permissions danger-full-access
```

TUI attach 或切换 Session 后会重新读取 Gateway 中的策略和 revision，不保存独立默认值。

## 4. HTTP API

读取策略不要求 writer：

```http
GET /api/sessions/{session_id}/execution-policy
```

修改策略要求认证、Session 写权限、准确的 Surface observer writer 身份和 expected revision：

```http
PUT /api/sessions/{session_id}/execution-policy
X-Cowd-Observer-Id: webui:tab-id
Content-Type: application/json

{
  "preset": "yolo",
  "expected_revision": 7
}
```

并发冲突返回 `409`。客户端应重新 GET，展示真实状态，再由用户决定是否重试，不得静默覆盖。

审批查询支持业务过滤：

```http
GET /api/approval/pending?session_id={id}&domain=execution&blocks_execution=true
```

## 5. 审批范围

人工批准时可选择 `once`、`turn`、`task`、`session` 或 `global`。范围只是有效生命周期，不会取消以下约束：

- Principal 身份；
- 工具或能力名称；
- 资源目标；
- 副作用描述；
- Session/Turn/Task 归属；
- Session 执行策略 revision。

策略变化、资源目标变化或效果描述变化后，旧 Grant 不会继续命中。

## 6. 常见状态判断

| 现象 | 实际边界 | 处理方法 |
|---|---|---|
| 页面是 observer，不能发消息/改策略 | Session writer | 取得 writer attachment；不要放宽工具权限 |
| 工具请求等待批准 | Runtime approval | 在当前 Chat 或全局审批入口决定 |
| 工具被 hard deny | Permission rule/ceiling | 检查 Session 预设、Agent Packet 和 deny 规则 |
| 子 Agent 在 YOLO 下仍只读 | Agent immutable ceiling | 由父级重新规划并创建具备所需上限的新 Agent |
| `402/429` | Provider 余额/限流 | 更换 Provider、账号或等待恢复；与文件权限无关 |
| `Permission denied` 来自系统调用 | OS 文件权限 | 修正工作区、文件属主或系统权限 |
| 输入合同/参数错误 | Tool/Graph contract | 修正输入或重规划；不得通过提权掩盖 |
| WebUI 显示“不可用” | API/认证/连接失败 | 查看错误详情和 Gateway 健康状态 |

## 7. 低风险自动处理

满足全部条件的读取/网络操作会由 Runtime 自动批准一次并留证：

- 风险为低；
- 效果类型为读取或网络；
- 不修改系统或包；
- 不涉及 Secret；
- 不属于外部写入、系统或未知效果；
- 工具没有显式要求用户/管理员审批。

`autonomous` 模式还允许 Steward 处理可逆或可补偿的中风险动作。外部写入、Secret、不可逆、系统和未知操作始终升级给人类。

## 8. 验证清单

变更配置或升级版本后至少确认：

1. GET 返回的四个策略维度、origin 和 revision 正确。
2. observer 可读但不能 PUT；writer 可以按 expected revision PUT。
3. 旧 expected revision 返回 409。
4. 活跃 Session 降权后，下一次工具授权立即按新上限处理。
5. Session 提权不会扩大现有只读子 Agent。
6. 新 Agent Packet 可以按父级显式重规划获得新上限。
7. execution 审批出现在当前 Chat；knowledge/evolution/skill 不阻断 Chat。
8. 低风险读取/网络自动留证，高风险外部动作仍需人工。
9. TUI、WebUI 和 Slash 显示相同 revision。
10. Gateway 重启后策略从 Session metadata 恢复。

架构不变量和代码归属见 [Session 执行策略与授权架构](../architecture/session-execution-policy-and-authorization.md)。
