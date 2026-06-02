//! Cowd 首次启动引导模块
//!
//! 当检测到用户目录下没有配置文件时，自动创建默认配置并引导用户进行关键配置。

use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

/// 检查是否需要引导配置
pub fn needs_bootstrap() -> bool {
    let config_home = default_config_home();
    let config_paths = vec![
        config_home.join("config.yaml"),
        config_home.join("config.yml"),
        config_home.join("config.json"),
    ];
    
    // 同时检查 ~/.cc 目录（兼容 CC 配置）
    let cc_path = PathBuf::from(&std::env::var("HOME").unwrap_or_default())
        .join(".cc")
        .join("config.yaml");
    
    // 如果 ~/.cc/config.yaml 存在，直接复用
    if cc_path.exists() {
        return false;
    }
    
    !config_paths.iter().any(|p| p.exists())
}

/// 获取默认配置目录
fn default_config_home() -> PathBuf {
    std::env::var_os("COWD_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cowd"))
        })
        .unwrap_or_else(|| PathBuf::from(".cowd"))
}

/// 运行交互式引导配置
pub fn run_bootstrap() -> Result<(), Box<dyn std::error::Error>> {
    let config_home = default_config_home();
    let config_file = config_home.join("config.yaml");
    
    // 创建配置目录
    if !config_home.exists() {
        fs::create_dir_all(&config_home)?;
    }
    
    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║                    Cowd 首次启动引导                              ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");
    
    println!("检测到您是首次使用 Cowd，让我们进行一些基本配置...\n");
    
    // 引导配置
    let api_key = prompt_user("请输入您的 OpenAI API Key (sk-...): ");
    let gateway_enabled = prompt_yes_no("是否启用 Gateway 服务? (用于 HTTP API 和 WebSocket 接口) [y/N]: ");
    let gateway_port: u16 = if gateway_enabled {
        prompt_port("请输入 Gateway 端口 (默认 8642): ")
    } else {
        8642
    };
    
    let gateway_token = if gateway_enabled {
        generate_token()
    } else {
        String::new()
    };
    
    // 生成配置
    let config_content = generate_config(&api_key, gateway_enabled, gateway_port, &gateway_token);
    
    // 写入配置文件 (0o600 — only owner can read the API key)
    fs::write(&config_file, config_content)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&config_file, std::fs::Permissions::from_mode(0o600)).ok();
    }

    println!("\n✓ 配置文件已创建: {}", config_file.display());
    println!("⚠️  WARNING: API keys are stored in plaintext in this file.");
    println!("   Ensure the file is protected (0o600 permissions set) and");
    println!("\n下一步:");
    println!("  1. 编辑配置文件添加更多 Provider 配置");
    println!("  2. 运行 'cowd --help' 查看使用说明");
    println!("  3. 运行 'cowd' 启动交互式 REPL\n");
    
    Ok(())
}

/// 提示用户输入
fn prompt_user(prompt: &str) -> String {
    print!("{}", prompt);
    io::stdout().flush().unwrap();
    let mut input = String::new();
    let stdin = io::stdin();
    stdin.lock().read_line(&mut input).ok();
    input.trim().to_string()
}

/// 提示用户输入端口
fn prompt_port(prompt: &str) -> u16 {
    let input = prompt_user(prompt);
    input.parse().unwrap_or(8642)
}

/// 提示用户选择是/否
fn prompt_yes_no(prompt: &str) -> bool {
    let input = prompt_user(prompt);
    let input = input.trim().to_lowercase();
    input == "y" || input == "yes"
}

/// 生成随机 Token
fn generate_token() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", timestamp)
}

/// 生成配置文件内容
fn generate_config(api_key: &str, gateway_enabled: bool, port: u16, token: &str) -> String {
    let gateway_enabled_str = if gateway_enabled { "true" } else { "false" };
    let auth_enabled_str = if gateway_enabled { "true" } else { "false" };
    
    format!(r#"# =============================================================================
# Cowd 用户全局配置
# =============================================================================
#
# 配置路径优先级（从低到高）:
#   ~/.cowd/config.yaml          ← 本文件（用户全局）
#   ./.cowd/config.yaml          （项目共享）
#   ./.cowd/config.local.yaml    （本地覆盖，应加入 .gitignore）
#   COWD_* 环境变量               （最高优先级）
#
# =============================================================================

# 主模型
model: "gpt-4o"

# =============================================================================
# Provider 配置
# =============================================================================
providers:
  openai:
    base_url: "https://api.openai.com/v1"
    api_key: "{api_key}"
    models:
      - "gpt-4o"
      - "gpt-4o-mini"
      - "gpt-3.5-turbo"

# 模型别名（快速切换）
aliases:
  main: "gpt-4o"
  fast: "gpt-4o-mini"

# =============================================================================
# 提供商故障转移（当主模型返回 429/5xx 时自动切换至此列表）
# =============================================================================
fallbacks:
  - "gpt-4o-mini"

# =============================================================================
# 静态环境变量注入
# =============================================================================
# env:
#   OPENAI_BASE_URL: "https://api.openai.com/v1"
#   OPENAI_API_KEY: "{api_key}"

# =============================================================================
# 全局权限配置
# default_mode: "plan"(只读) | "acceptEdits"(可写工作区) | "dontAsk"(危险全访问)
# =============================================================================
permissions:
  default_mode: "acceptEdits"
  allow: []
  deny: []
  ask: []

# 可信工作区根目录
trusted_roots:
  - "/media/yi/Datas/workspace"

# =============================================================================
# 记忆系统配置
# =============================================================================
memory:
  enabled: true
  storePath: "~/.cowd/memory"
  layers:
    l0_enabled: true
    l1_max_tokens: 3000
    l2_max_tokens: 8000
    l3_search_limit: 5
    l4_enabled: false
  extraction:
    auto_extract: true
  vector:
    enabled: false
    embeddingModel: "text-embedding-3-small"
    apiUrl: "https://api.openai.com/v1/embeddings"
    apiKey: ""
    dimension: 1536
    timeout_secs: 30
    batch_size: 32

# =============================================================================
# 压缩管线配置
# =============================================================================
compression:
  micro:
    enabled: true
    tool_result_max_chars: 6000
    time_decay_factor: 0.9
  session:
    threshold_tokens: 180000
    preserve_recent: 10
    summary_max_tokens: 2000
    buffer_tokens: 13000
  deep:
    enabled: true
    iterative_update: true
  circuit_breaker:
    max_retries: 3
    cooldown_secs: 30

# =============================================================================
# 运行时配置
# =============================================================================
runtime:
  model: "gpt-4o"
  permission_mode: "acceptEdits"

# =============================================================================
# 多渠道网关配置
# =============================================================================
gateway:
  enabled: {gateway_enabled_str}
  session_reset: "none"
  platforms:
    - platform_type: "api_server"
      enabled: {gateway_enabled_str}
      host: "127.0.0.1"
      port: {port}
      auth:
        enabled: {auth_enabled_str}
        token: "{token}"

# =============================================================================
# 沙箱配置（默认关闭）
# =============================================================================
sandbox:
  enabled: false
  namespace_restrictions: false
  network_isolation: false
  filesystem_mode: "none"
  allowed_dirs: []

# =============================================================================
# MCP 服务器配置
# =============================================================================
mcp_servers: {{}}

# =============================================================================
# 钩子配置
# =============================================================================
hooks:
  PreToolUse: []
  PostToolUse: []
  PostToolUseFailure: []

# =============================================================================
# 插件配置
# =============================================================================
plugins:
  enabled: {{}}
  external_dirs: []
  installRoot: null
  registryPath: null
  bundledRoot: null
  max_output_tokens: null
"#)
}
