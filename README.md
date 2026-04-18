# Cowd — AI Coding Agent CLI

A high-performance Rust implementation of an AI coding agent CLI. Supports Anthropic Claude, OpenAI, and any OpenAI-compatible API provider. Built for speed, safety, and native tool execution.

## Quick Start

### Install from Source

```bash
git clone https://gitee.com/eyeout/cowd.git
cd cowd
chmod +x install.sh
./install.sh
```

The installer will guide you through:
1. Prerequisites check (Rust toolchain)
2. Building from source
3. Interactive API configuration (URL, key, model, workdir)
4. Writing config to `~/.cowd/config.yaml`
5. Adding `cowd` to your shell PATH

### Manual Build

```bash
cd cowd/
cargo build --release
# Binary at target/release/cowd
```

### First Run

```bash
# Interactive REPL
cowd --model gpt-4o

# One-shot prompt
cowd prompt "explain this codebase"

# JSON output for automation
cowd --output-format json prompt "summarize src/main.rs"
```

## Configuration

### Environment Variables

All environment variables use the `COWD_` prefix:

| Variable | Description |
|----------|-------------|
| `COWD_CONFIG_HOME` | Override config directory (default: `~/.cowd`) |
| `COWD_MODEL` | Default model to use |
| `COWD_API_KEY` | API key for the provider |
| `COWD_BASE_URL` | Override API base URL |
| `COWD_PERMISSION_MODE` | Permission mode (default, strict, etc.) |
| `COWD_MAX_OUTPUT_TOKENS` | Max output tokens per response |
| `COWD_DIR_NAME` | Override dot-directory name (default: `.cowd`) |

### Config File

The primary config file is `~/.cowd/config.yaml`:

```yaml
provider:
  api_url: "https://api.openai.com/v1"
  api_key: "sk-..."
  model: "gpt-4o"
```

Project-level overrides go in `.cowd.json` at the project root.

### Directory Layout

All Cowd artifacts live under a single dot-directory (default `.cowd`, configurable via `COWD_DIR_NAME`):

```
.cowd/
├── config.yaml          # Global provider config
├── settings.json        # User settings
├── settings.local.json  # Local project settings
├── agents/              # Agent definitions
├── skills/              # Skill definitions
├── plugins/             # Installed plugins
├── sandbox-home/        # Sandbox home directories
└── sandbox-tmp/         # Sandbox temporary files
```

## CLI Reference

```text
cowd [OPTIONS] [COMMAND]

Flags:
  --model MODEL
  --output-format text|json
  --permission-mode MODE
  --dangerously-skip-permissions
  --allowedTools TOOLS
  --resume [SESSION.jsonl|session-id|latest]
  --version, -V

Top-level commands:
  prompt <text>
  help
  version
  status
  sandbox
  dump-manifests
  bootstrap-plan
  agents
  mcp
  skills
  system-prompt
  init
  serve
```

## Slash Commands (REPL)

- **session / visibility**: `/help`, `/status`, `/sandbox`, `/cost`, `/resume`, `/session`, `/version`, `/usage`, `/stats`
- **workspace / git**: `/compact`, `/clear`, `/config`, `/memory`, `/init`, `/diff`, `/commit`, `/pr`, `/issue`, `/export`, `/hooks`, `/files`, `/release-notes`
- **discovery / debugging**: `/mcp`, `/agents`, `/skills`, `/doctor`, `/tasks`, `/context`, `/desktop`
- **automation / analysis**: `/review`, `/advisor`, `/insights`, `/security-review`, `/subagent`, `/team`, `/telemetry`, `/providers`, `/cron`
- **plugin management**: `/plugin` (aliases `/plugins`, `/marketplace`)

## Features

| Feature | Status |
|---------|--------|
| Anthropic / OpenAI-compatible provider flows + streaming | Done |
| Direct bearer-token auth | Done |
| Interactive REPL (rustyline) | Done |
| Tool system (bash, read, write, edit, grep, glob) | Done |
| Web tools (search, fetch) | Done |
| Sub-agent / agent surfaces | Done |
| Todo tracking | Done |
| Notebook editing | Done |
| Project memory (COWD.md) | Done |
| Config file hierarchy (`.cowd.json` + merged config sections) | Done |
| Permission system | Done |
| MCP server lifecycle + inspection | Done |
| Session persistence + resume | Done |
| Cost / usage / stats surfaces | Done |
| Git integration | Done |
| Markdown terminal rendering (ANSI) | Done |
| Model aliases | Done |
| Direct CLI subcommands | Done |
| Plugin management | Done |
| Skills inventory / install | Done |
| Machine-readable JSON output | Done |

## Workspace Layout

```text
cowd/
├── Cargo.toml              # Workspace root
├── Cargo.lock
├── install.sh              # Interactive installer
├── scripts/                # Development scripts
└── crates/
    ├── api/                # Provider clients + streaming + request preflight
    ├── commands/           # Slash-command registry + help rendering
    ├── compat-harness/     # TS manifest extraction harness
    ├── config/             # Unified config loading + env var overrides
    ├── memory/             # Project memory (COWD.md) management
    ├── mock-anthropic-service/ # Deterministic local mock server
    ├── plugins/            # Plugin metadata, install/enable/disable
    ├── runtime/            # Session, config, permissions, MCP, prompts, auth
    ├── rusty-claude-cli/   # Main CLI binary (`cowd`)
    ├── telemetry/          # Session tracing and usage telemetry
    └── tools/              # Built-in tools, skill resolution, agent runtime
```

## Mock Parity Harness

The workspace includes a deterministic Anthropic-compatible mock service for end-to-end parity checks:

```bash
# Run the scripted clean-environment harness
./scripts/run_mock_parity_harness.sh

# Or start the mock service manually
cargo run -p mock-anthropic-service -- --bind 127.0.0.1:0
```

## Stats

- **~20K lines** of Rust
- **11 crates** in workspace
- **Binary name:** `cowd`
- **Default permissions:** `danger-full-access`

## License

MIT
