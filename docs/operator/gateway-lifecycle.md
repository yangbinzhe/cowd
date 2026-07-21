# Gateway 生命周期运行手册

本手册说明已安装 Cowd 二进制的 Gateway 启动、停止、更新和核验方式。它只描述当前静态产品组合和本地 Auth Broker 的运行边界；不会在运行期拉取 App 源码，也不会把凭据写入命令行或日志。

## 日常服务管理

```bash
# 已安装二进制的服务管理
cowd gateway start
cowd gateway status
cowd gateway restart
cowd gateway stop
```

`start` 已有实例时不会重复拉起。用户不需要直接执行内部 `gateway run` 角色；服务管理器会以该角色拉起子进程。`restart` 仅回收与当前二进制匹配、命令行为 `gateway run` 的受管进程，不根据端口 8642 盲杀其他 worktree 或其他 Cowd 安装。它先发送 TERM 并等待最多 3 秒；仍未退出才发送 KILL 并最多再等待 1 秒。进程仍存在时命令失败，不会在旧进程之上伪造成功。

Gateway 启动成功的判定是**新 child 自己的 PID**成为服务 PID，而不是“端口上已有任意 Cowd 监听者”。这避免旧进程尚未退出时把旧监听误判为新服务。

## 二进制更新

以完整 Debug 产品为例：

```bash
cargo build -p cli --features full
install -m 755 target/debug/cowd /home/yi/AI/cowd
/home/yi/AI/cowd gateway restart
/home/yi/AI/cowd --version
/home/yi/AI/cowd gateway status
```

覆盖正在运行的可执行文件会使 Linux `/proc/<pid>/exe` 指向 deleted inode。Gateway 同时比对该进程最初的命令行启动路径，因此仍能识别同一安装路径的旧实例并收敛到一个进程。不要通过手工 `rm` PID 文件或并行执行多个 `gateway run` 来更新服务。

## App 启用与认证状态

构建期决定 App 是否进入二进制；`apps.<id>.enabled` 决定已编译 App 是否注册到 Gateway、Auth catalog、AI tools、TUI 和 WebUI。变更配置后使用 `gateway restart`，并通过公共 manifest 核对实际投影：

```bash
curl --fail http://127.0.0.1:8642/api/webui/manifest
```

Auth Broker 保存的是凭据摘要、目录版本、profile selection 和按当前目录重算的能力，不保存原始凭据。V564 首次读取历史 v2 状态时，在正确凭据验证后一次性迁移为 v3：

- 未识别的历史 core profile 降到 `core_operator`；
- 未识别的历史 App profile 降到该 App 的当前默认 profile；
- capability 从当前 catalog 重算；
- credential epoch 和 profile revision 递增，使旧签名主体失效；
- 验证失败时状态保持不变。

不要手工编辑、复制、降级或删除 `credential-state.json` 来处理启动问题。若迁移失败，先检查当前配置提供的凭据是否与既有状态匹配，并保留状态文件用于诊断。

## 单实例与会话核验

`UnifiedSessionStore` 使用 r2d2 SQLite 连接池、WAL 和 SQLite busy timeout；它不再用进程内全局 async mutex 串行化无关会话。事务内的原子性、同一 session 的 sequence 语义和 outbox 状态机保持不变。

每次更新后至少核验：

```bash
# 版本与单一服务状态
cowd --version
cowd gateway status
ps -eo pid=,args= | awk '$0 ~ /cowd gateway run/ {print}'

# 受保护的会话 API：从安全配置来源读取 token；不要把 token 粘贴进 shell 历史、文档或日志。
curl --fail -H "Authorization: Bearer <配置中的 API token>" \
  'http://127.0.0.1:8642/api/sessions?limit=1'
```

通过条件是：Gateway status 的 PID 与唯一 `gateway run` 进程一致；会话 API 有限时响应；`/api/webui/manifest` 的 `enabled_app_ids` 与当前配置相符。真实模型验收应建立新会话、明确选择已配置的模型，并在不调用工具的短提示中验证持久化 assistant 回复；它是 provider/会话/授权/App 投影的端到端检查，不应替代常规确定性测试。
