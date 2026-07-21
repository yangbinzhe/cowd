# 已编译 App 的统一启停与构建模型

状态：已实施（V563：显式 catalog、静态产品组合与统一启停）

## 结论

Cowd 当前采用“**发行时纳入代码，启动时统一启用**”的模型。它刻意不支持从配置中拉取 Git 源码、动态执行未知二进制或把未审核代码接进 Gateway。

```text
Cargo / 前端构建                 配置启动                         用户界面
----------------                 --------                         --------
apps/catalog.toml + source lock  apps.mfg.enabled                WebUI / TUI
        │                                │                              │
        ├─ Rust full 产品 ───────────────┼─> Gateway AppRegistry ───────┼─> 只展示已启用 App
        │                                │       │                      │
        └─ WebUI MFG 静态资源 ───────────┘       ├─ API 路由             └─ 不请求已禁用 App 能力
                                                ├─ Skill 目录
                                                ├─ OpenAPI / AI 工具目录
                                                └─ Auth Broker capability 目录
```

配置是唯一的运行期开关：

```yaml
apps:
  mfg:
    enabled: true
```

变更后重启 Gateway 才生效。`enabled: false` 会使 MFG 从同一个 Gateway 注册表中被移除，因此：

- `/api/apps` 不再列出 MFG，MFG 路由不会注册；
- Gateway capability contract、OpenAPI、OpenAI tools 和 Auth Broker capability 目录不再发布 MFG；
- TUI 在连接 Gateway 后按该目录过滤 App 面板；
- WebUI 在挂载前读取 `/api/webui/manifest`，过滤页面、路由、导航和 App capability 请求。

这不是单纯隐藏 UI。已禁用 App 不可通过已公开的 HTTP、AI tool 或授权目录绕过访问。MFG 自己的数据文件不会因禁用而删除；重新启用并重启后可继续使用。

## 如何编译

MFG 是经过 catalog 与锁定来源校验后、静态纳入 Cowd 产品的业务 App。完整构建按以下顺序进行：

```bash
# 1. 校验锁定的 App 源码；更新版本时先人工审核 source.lock.toml，再更新锁定来源。
cargo run -p xtask -- apps verify --locked

# 2. 构建包含 Gateway、TUI 与当前已审核 App 的 Cowd full 二进制。
cargo build -p cli --features full

# 3. 构建包含 MFG 前端贡献的 WebUI 静态资源。
cd ../cowd-edge/surfaces/webui
COWD_WEBUI_APPS=mfg npm run build
```

开发机已缓存前端依赖且需要离线构建时，可在第三步附加 `COWD_WEBUI_OFFLINE=1`。只构建 WebUI 核心壳而不编入 MFG 前端贡献时使用 `COWD_WEBUI_APPS=none npm run build`；这只影响 WebUI 静态资源，不会改变 Rust full 二进制的组成。

`cargo build -p cli --features full` 会把当前审核的 MFG Rust 代码链接进 Cowd 二进制。`apps.mfg.enabled: false` 禁止注册和执行入口，但不会缩小该二进制；需要物理 core-only 发行物时使用：

```bash
cargo build -p cli --no-default-features
```

`app-mfg` 是构建期开关，`apps.mfg.enabled` 是启动期开关；二者不可互相替代。

## 新 App 的边界

`apps.<id>.enabled` 的解析和运行时投影是通用的；但配置项本身不能让任意第三方代码获得执行权。新增 App 必须先完成：

1. 审核并锁定其来源；
2. 在 App 自己的 bundle 中显式导出静态 product contribution，并在 Cowd catalog/source lock 中审核纳入其 Rust 后端与前端贡献；
3. 为 API、技能、授权、TUI、WebUI 写入同一 `AppRegistry` 投影和验收测试；
4. 再以 `apps.<id>.enabled` 控制该已编译能力的启动。

这样保留统一配置和自动界面发现，同时维持供应链边界、可复现构建与可审计的能力发布。
