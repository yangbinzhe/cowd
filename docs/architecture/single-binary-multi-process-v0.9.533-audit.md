# Cowd v0.9.533 单文件多进程实施与审计

审计日期：2026-07-17

状态：实现、代码审查、测试、发布门禁与真实部署验收全部通过。

## 1. 终态

安装和运行只需要一个物理文件：

```text
cowd
```

运行时仍保持三个操作系统进程角色：

```text
cowd gateway run
├── cowd __cowd_internal auth-broker
└── bwrap -> cowd __cowd_internal sandbox-launcher --inner
```

`auth-broker` 和 `sandbox-launcher` crate 继续作为代码所有者，但不再定义独立 binary target。

## 2. 完整性矩阵

| 目标 | Owner | 修改与接线 | 删除目标 | 验收 |
|---|---|---|---|---|
| 唯一 Cowd 可执行文件 | `cli` | 公开 CLI 解析前分发隐藏角色 | 两个 helper `[[bin]]` | Cargo metadata、干净 target 构建 |
| 认证进程隔离 | `auth-broker` + Gateway | Gateway 通过 `/proc/self/exe` 固定当前运行 inode，并通过 stdin 注入凭据 | `auth-broker/src/main.rs`、外部路径覆盖 | Unix Socket、签名、lifecycle、运行映像固定测试 |
| 沙箱进程隔离 | `sandbox-launcher` | 外层以受控 FD 固定当前 Cowd inode，bwrap 只读绑定，inner 安装 Landlock/seccomp | `sandbox-launcher/src/main.rs`、磁盘路径竞态 | deny probe、FD 泄漏、系统写入/挂载阻断、真实命令 |
| 原子安装 | release scripts | 同目录临时文件 + 原子 rename；替换前验证二进制版本；主动删除旧 helper | 直接覆盖、伪版本元数据、helper 复制 | 运行中替换、inode、版本漂移负向测试 |
| 场景迁移 | validation/scenarios | Gateway 自执行，不再注入 broker 路径 | 12 个 `COWD_AUTH_BROKER_BIN` 依赖 | 5 条黄金场景、TUI smoke |

### 平台支持边界

| 平台 | 编译审计 | 完整功能 | 结论 |
|---|---|---|---|
| Linux x86_64 | 原生 full build、全工作区测试 | Gateway、Auth Broker、bwrap、Landlock、seccomp 全部真实验收 | 本版本唯一完整支持平台 |
| macOS x86_64 | `sandbox-launcher` 交叉 `cargo check` 通过；完整 CLI 交叉检查受本机缺少 Apple C/SDK 工具链阻断 | 无 bwrap/Landlock/seccomp；内部进程托管明确失败关闭 | 不宣称完整支持 |
| Windows x86_64 GNU | `sandbox-launcher` 交叉 `cargo check` 通过；完整 CLI 交叉检查受本机缺少 MinGW C 编译器阻断 | Auth Broker 缺少 Unix Socket，内核沙箱不可用；内部进程托管明确失败关闭 | 不宣称完整支持 |

Linux 不是偶然只编译一个平台，而是当前安全模型的明确功能边界。交叉 `cargo check` 只证明本次非 Linux 分支可编译，不替代真实系统运行验收。

## 3. 关键代码事实

- `crates/cli/src/main.rs`
  - 首先注册当前进程为 Cowd host。
  - 随后分发隐藏内部角色。
  - 未命中内部角色才进入公开 TUI/Gateway/静态命令。
- `crates/gateway/src/runtime_host/mod.rs`
  - Auth Broker 只允许通过统一进程 host 构造器拉起。
  - Linux 使用 `/proc/self/exe` 执行当前运行 inode；`argv[0]` 仍显示安装路径。
  - Gateway 继续拥有 broker 生命周期和 Unix Socket 清理。
- `crates/sandbox-launcher/src/lib.rs`
  - 外层可信 bootstrap 打开 `/proc/<gateway-pid>/exe` 为 FD 3。
  - bwrap 从自身 `/proc/self/fd/3` 只读绑定同一 inode 到 `/run/cowd-sandbox-inner`。
  - 进入 inner 前关闭 FD 3，并清理 bootstrap 新增环境变量。
  - inner 角色继续安装 Landlock V5 和 seccomp。
  - 生产 Cowd host 固定运行映像；测试 harness 才通过协议寻找旁边的 Cowd。
  - 每次 launch 只验证/选择一次 Cowd 路径，probe 与正式命令复用。
- `scripts/release/install-debug-to-ai.sh`
  - 安装前核对候选二进制自报版本与 workspace 版本。
  - `mktemp` 后复制，再以同目录 `mv` 原子替换安装入口。
  - 清除旧 helper、历史备份和临时 staging。
- `crates/gateway/src/entry/install_entry.rs`
  - 已删除。公开最小 CLI 早已拒绝 `cowd install`，该文件、action 和 parser 是未接线死代码。

## 4. 删除与残留审计

已删除：

- `crates/auth-broker/src/main.rs`
- `crates/sandbox-launcher/src/main.rs`
- 两个 crate 的 `[[bin]]`
- `COWD_AUTH_BROKER_BIN`
- `COWD_SANDBOX_LAUNCHER`
- `COWD_INTERNAL_PROCESS_BIN`
- 场景脚本中的独立 broker 检查和注入
- 发布脚本中的 sandbox helper 复制与报告
- 未接线的 `install_entry`、`CliAction::Install` 和 `parse_install_args`
- `target/debug` 中两个历史 helper `.d` 构建残留

允许残留：

- 安装脚本中的旧文件名仅用于升级时删除历史 helper。
- `/etc/cowd-auth-broker` 仅作为禁止暴露的历史控制面路径。
- auth-broker 单元测试临时目录名称不属于生产 binary 或安装依赖。

## 5. 测试证据

### 单文件专属测试

```text
6 passed; 0 failed
```

覆盖：

- Cargo/source 架构只有一个 Cowd executable target。
- 同一 Cowd 返回版本化 sandbox 协议。
- 同一 Cowd 启动真实 auth broker 并通过 Unix Socket readiness。
- 同一 Cowd 完成真实 bwrap + Landlock + seccomp 命令。
- 安装器可在 Cowd 正运行时原子替换目录项，旧进程继续固定原 inode。
- 候选版本不一致时安装失败，目标文件和目录不发生变化。

### 原安全能力回归

```text
auth-broker:       4 passed
sandbox-launcher: 12 passed
Gateway sandbox:   2 passed
RuntimeHost arch: 41 passed
```

### 全工作区

```text
cargo check --workspace --all-targets
cargo test --workspace --all-targets -- --test-threads=1
```

均通过。关键大包结果：

- Gateway：517 passed，10 个既有 serial-global 用例按治理规则 ignored。
- Runtime：1050 passed。
- TUI：956 passed。

最终权威全量结果：

```text
104 suites
4032 passed
0 failed
15 ignored
```

完整日志：

```text
/tmp/cowd-v0.9.533-final-push-audit.log
```

第一次 0.9.533 全量运行触发一个既有 Gateway 索引测试的非确定性失败：

```text
session_execution_index_exposes_running_only_and_retains_terminal_reference
```

该文件和调用链未被本次修改。单测连续复跑 10 次为 9 次通过、1 次失败，确认是原有时序波动；未按本次范围修改 Runtime 业务逻辑。失败日志保留于：

```text
/tmp/cowd-v0.9.533-full-workspace-test.log
/tmp/cowd-v533-flake-1.log ... /tmp/cowd-v533-flake-10.log
```

### 黄金场景

从空临时 target 构建并执行：

| 场景 | 结果 |
|---|---:|
| cargo_build_cli | 通过 |
| ai_harness | 通过 |
| gateway_baseline | 通过 |
| session_runtime | 通过 |
| memory_context | 通过 |
| tool_permission | 通过 |
| skill_mfg | 通过 |

报告：

```text
/tmp/cowd-single-binary-scenario-report/report.md
```

### TUI 真实 smoke

第一次运行使用了全量测试最后生成的非 full binary，按预期失败并明确报告 TUI 未编入。

重新执行 `cargo build -p cli --features full` 后：

```text
TUI smoke test passed
```

报告：

```text
/tmp/cowd-single-binary-tui-report-r2/report.md
```

该结果同时证明最终发布必须安装 full build。

## 6. 安装与性能评估

临时安装目录实际文件：

```text
cowd
install.json
```

唯一 executable：

```text
cowd
```

完整产品 smoke：

```text
release full-product smoke passed
```

Debug 进程启动测量：

| 路径 | 平均耗时 |
|---|---:|
| 旧 helper protocol | 1.041 ms |
| Cowd internal protocol | 4.366 ms |
| 旧 helper 完整 sandbox CLI | 17.244 ms |
| 单文件完整 sandbox CLI | 26.930 ms |
| 最终 FD 固定与关闭加固后的已安装 Cowd sandbox CLI | 33.760 ms |

完整 CLI 对比包含外层和内层两次可执行文件启动。真实 Gateway 已经常驻，只增加内层 Cowd 启动；按协议进程差值估算约增加 3.3ms/次 sandbox launch。模型调用、工具实际执行和 bwrap 隔离通常远高于该开销。

本次还删除了同一 launch 内重复协议探测，避免额外自执行成本。

推送前加固增加了固定的 FD 绑定和 bootstrap `exec`，不复制第二份安装文件。最终已安装 full debug binary 连续 30 次真实 bwrap + Landlock + seccomp 启动结果：

```text
average 33.760 ms
p50     33.450 ms
p95     37.873 ms
min     29.915 ms
max     39.287 ms
```

相对加固前单文件完整路径平均增加约 6.8ms，换取原子升级期间严格的运行 inode 一致性和 FD 不泄漏保证；仍远低于模型推理与绝大多数实际工具执行耗时。

## 7. 格式与静态门禁

通过：

- 本次修改 Rust 文件的 `rustfmt --check`。
- 所有修改 Shell 文件的 `bash -n`。
- `git diff --check`。
- `cargo check --workspace --all-targets`。
- macOS/Windows target 的 `sandbox-launcher` 交叉 `cargo check`。
- 生产源码旧环境变量扫描为 0。
- 两个旧 binary target 声明扫描为 0。
- 旧 helper 构建产物和安装目录残留扫描为 0。
- GitHub release gate 已从不存在的 `cowd-cli`/0-test 过滤器接到真实 Gateway release-gate 测试。

全仓 `cargo fmt --all --check` 仍会命中 48 个本次开始前已经存在的格式差异。按既定范围没有改写 Connector、Memory、Runtime、TUI 等无关文件；该非标基线不影响本次修改文件的格式门禁。

## 8. 审查结论

达成：

- 一个安装文件。
- 三个独立进程角色。
- Auth 私钥/签名内存不回到 Gateway。
- bwrap、Landlock、seccomp、环境清理和 fail-closed 不损失。
- 不再存在 Gateway/helper 版本漂移。
- 安装、更新、回滚和校验和只面向一个文件。
- 原子升级期间旧 Gateway 与其新启动子进程保持同一 inode，不会混用磁盘新版本。
- bwrap 专用 FD 在 inner Cowd 执行前关闭，不向不可信命令泄漏。

已知权衡：

- Sandbox inner 从约 1 MB text 的小 helper 变为完整 Cowd 映像，debug 启动增加约 3.3ms。
- 进程隔离仍在，但可执行映像的代码面比原 helper 更大。

综合判断：

Linux 单文件部署目标完整达成，安全行为保持，升级竞态、FD 泄漏、版本漂移和未接线安装空壳均已闭环。macOS/Windows 的完整能力没有被虚假宣称；它们在缺少对应安全后端时明确失败关闭。

## 9. 真实部署闭环

版本：

```text
0.9.533
```

标签：

```text
v0.9.533
Cowd v0.9.533：单一 cowd 文件承载 Gateway、认证 Broker 与内核沙箱多进程角色，消除辅助二进制版本漂移
```

安装目录清理后，与 Cowd 运行相关的普通文件只有：

```text
/home/yi/AI/cowd
```

`cowd-debug-current` 是指向该文件的软链接，不产生第二份物理二进制。旧文件和本次部署备份均已删除：

```text
cowd-auth-broker
cowd-sandbox-launcher
.cowd-auth-broker.prev-*
.cowd-sandbox-launcher.prev-*
```

真实进程树：

```text
/home/yi/AI/cowd gateway run
└── /home/yi/AI/cowd __cowd_internal auth-broker ...
```

真实运行结果：

```text
cowd-gateway.service: active/running
NRestarts: 0
GET http://127.0.0.1:8642/health: OK
installed Cowd sandbox command: passed
installed full-product smoke: passed
installed TUI smoke: passed
```

安装文件与最终构建产物 SHA-256 必须在每次发布时相等；本次门禁已验证相等。
