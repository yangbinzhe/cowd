# V550 Edge UDS/H2 传输实施与审计证据

版本：`0.9.550`
范围：Cowd Gateway + cowd-edge managed transport
结论：通过 V550 版本门禁；Message/Source 业务内部的 coarse lock 与资源生命周期按计划留给 V551/V552，不计作 V550 完成。

## 已落地的生产链路

- `SurfaceRuntimeSpec` 将 OneShot 和 Managed 变为 sealed runtime；Managed 只接受可信 artifact/profile 名和 `uds-http2`，旧 `entry + lifecycle + transport` 顶层组合直接反序列化失败。
- Gateway `ProcessSupervisor` 创建权限隔离的 runtime 目录、`0600` 一次性 credential 和 UDS，使用 sandbox 启动受信 artifact；stdout/stderr 持续有界 drain，不再承担协议。
- Gateway `EdgeH2Client` 在一个 UDS/H2 连接上多路复用 configure/connect/health/message/action/source；response/event 均有 2 MiB 上限，取消通过 H2 RST 传播。
- Edge server 校验同 UID peer 与 constant-time token，消费后删除 credential，UDS 权限为 `0600`；请求上限 1 MiB、并发上限 256、过载 429、超限 413。
- event 使用 sequence/replay/ACK；replay 有界 4096，future ACK 被拒绝，满队列等待 ACK，不静默覆盖。
- 9 个逻辑 manifest 由一个 profile registry 生成并引用 6 个 artifact；Feishu/Lark Bitable 使用同一 artifact、独立 profile/进程/域名/状态。

## 发现并修正的关键缺陷

首次真实 H2 测试发现 Hyper client 的相对 URI 会产生 `MissingUriSchemeAndAuthority`。Gateway client 与测试 client 已统一改为带 authority 的 HTTP/2 URI。该问题只靠 `cargo check` 无法发现，现由真实 UDS/H2 request 测试封口。

并发 bootstrap 的 check-then-set 竞态已改为单 owner；event `notify_waiters` 的 missed wakeup 风险改为保留 permit 的 `notify_one`；response 由“collect 后检查长度”改为读取期间硬限制。

## 自动化证据

通过项：

- `cargo check --workspace --all-targets`（Cowd 0.9.550）。
- `cargo test -p gateway --lib`：518 passed / 0 failed / 10 ignored。
- `cargo test -p surface`：16 passed / 0 failed / 0 ignored。
- 本次涉及的 Rust 源文件使用 `rustfmt --config skip_children=true --check` 全部通过；`git diff --check` 通过。
- `cargo check --workspace --all-targets --features source-db`（cowd-edge 0.9.550）。
- `cargo test -p surface edge_v2_contract` 与 `cargo test -p edge-contract edge_v2_contract`。
- Gateway client：单 UDS/H2 连接 64 个 50 ms action，总耗时门槛 `<500 ms`，观测最大并发 `>=8`；单 stream abort 后 server handler 在 1 秒内释放。
- Edge server：单连接 64 个 50 ms action，总耗时门槛 `<500 ms`，观测最大并发 `>=8`；事件 replay/ACK、future ACK、1 MiB request limit、stream cancellation 全通过。
- driver profile matrix：9 个逻辑实例、6 个唯一 artifact；全部 manifest 校验通过。
- Feishu/Lark isolation：同一 Bitable artifact 同时构造两个实例，分别保持 `open.feishu.cn` / `open.larksuite.com` 与独立 adapter identity。

真实生产二进制跨进程矩阵 `node scripts/eval-managed-edge-h2.mjs`：同时启动 9 个真实进程，逐项完成 credential/UDS/H2/bootstrap 和 8 路并发 health，共 72 个请求全部成功。观测 health 批次耗时范围 `1.819–5.583 ms`；credential 全部被消费，socket 全部为 `0600`。

## 规范与残留扫描

- canonical schema 两仓 SHA-256：`b75aa5d5bfa21afa0899828de3ba43349bae5754c60af95f79cd3a895cc13ed7`。
- generated Rust 两仓 SHA-256：`16fcd9e56834b716c6e5fdf1f87fd729e4b24fad6090ffb5d5e242b6030a61f4`。
- 两仓完整 `surface/edge-contract` lib 字节一致。
- `ChildStdin|invoke_managed|pending:.*SurfaceFrame` 在 Gateway managed 路径无匹配。
- managed stdio manifest 和已合并的旧 artifact 名无匹配。
- 冻结的独立 App 方案哈希保持 `b91e9f1c0e54258fcac6e5947c5ce47dae88a79d9af0d922bb311a6806574034`。

## 边界

V550 不宣称真实 provider 收发或数据库吞吐已经提升；本版证明的是 transport、进程、身份、并发、取消、流控和 profile 安装单元闭环。Message 的 provider 并发 owner 在 V551，Source pool/stream/watermark owner 在 V552，Gateway durable ingress 在 V553。

仓库全量 `cargo fmt --all -- --check` 仍会命中本次未修改的历史格式残留；本版未扩大范围修改这些文件。Gateway 测试还发现两个 worktree 共用固定 `/tmp/cowd-gateway-test-auth` 会发生认证夹具串扰：隔离残留夹具后 518 项测试全部通过。该问题不属于 Edge H2 产品链路，但必须在两分支融合时将测试目录改为每进程/每工作区唯一目录。
