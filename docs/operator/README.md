# 运维

## 部署

```bash
cowd gateway start          # 启动
cowd gateway stop           # 停止
cowd gateway status         # 状态
cowd gateway doctor         # 诊断
curl http://127.0.0.1:8642/healthz | jq .storage
```

## 存储操作

```bash
cowd storage plan           # 查看平迁计划
cowd storage upgrade        # PG schema 幂等升级（停机）
cowd storage verify         # 校验迁移证据
cowd storage fallback-status
cowd storage adopt-postgres # 回退后显式接管 PG（停机）
```

## 实时订阅

- 每 principal + Surface instance 上限默认 16（`gateway.live.max_subscriptions_per_principal_instance`）。
- v0.9.675 起前端 live 订阅使用 `observerId:tab:<nonce>` 作为 surface instance：每个标签页独立计数，
  多标签不再共享同一实例的订阅额度；同一页面内所有来源仍收敛为单条物理订阅。
- 授权失效自动恢复每浏览器会话最多一次（sessionStorage 记录），再次失效只提示、不循环刷新。
- `live subscription count exceeded` 表示同一实例的活跃订阅超过上限：确认没有重复标签页后
  刷新页面，或提升配置。

## 常见故障

| 现象 | 原因 | 处理 |
|---|---|---|
| 会话显示“受限会话” | 浏览器会话 credential epoch 过期 | 刷新/重登一次；新版会自动识别并恢复 |
| `live subscription count exceeded` | 旧订阅未释放 | 刷新页面；已提升默认上限 |
| `live subscription count exceeded`（多标签） | 标签共享实例额度 | 升级到 v0.9.675：surface_instance 已加入 tab nonce，每标签独立计数 |
| 团队任务无终态 | 必需节点未完成（工具适配缺失/超时） | 已修复 team_board/evidence_retrieve 委托；查看执行图错误 |
| 审批不弹出 | 旧前端版本 | 更新到 v0.9.673+，任意页面自动弹出 |
| 记忆 L0 为空 | 未配置 identity | 配置 `memory.identity.role/language` 后重启 |
| Windows 沙箱 | 平台能力未实现 | v0.9.675 明确不支持；生产环境请使用 Linux |

## 工具策略环境变量

```bash
export COWD_NETWORK_DOMAIN_MODE=allow        # allow|ask|deny
export COWD_NETWORK_DOMAIN_ALLOW=docs.rs,github.com
export COWD_NETWORK_DOMAIN_BLOCK=evil.example
export COWD_ALLOW_PRIVATE_NETWORK=0          # 默认拒绝私网/回环目标
```

bash 环境策略在模型侧通过 `bash.env` 传入；默认 `inherit: safe` 自动屏蔽 secrets 与 `COWD_*`。

## 记忆 L0 引导

```yaml
memory:
  identity:
    role: "资深工程与系统架构助手：..."
    language: "zh-CN"
```

系统启动时写入 `assistant-role` 与 `response-language` 两条 L0 条目；后续抽取与回复将遵循该角色与语言。L0 不参与 LLM 自动写入，避免污染。
