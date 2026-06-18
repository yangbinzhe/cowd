# Cowd

Cowd is a Rust-native AI agent runtime. It provides a minimal CLI entrypoint,
an interactive TUI, an HTTP Gateway, session state, memory, tools, skills,
provider configuration, Matrix structured facts, and an MFG application layer.

Current kernel version: `0.9.300`

## Install Layout

- Main binary: `~/AI/cowd`
- WebUI assets: external build output configured by `gateway.webui_dir`

## Quick Start

```bash
~/AI/cowd --version
~/AI/cowd doctor
~/AI/cowd
```

Running `cowd` without a business command opens the TUI. One-shot `run`,
`chat`, and `prompt` entrypoints are intentionally removed; use the TUI or
WebUI for interactive work.

Resume a session through the TUI entrypoint:

```bash
~/AI/cowd --resume latest
```

Start the Gateway:

```bash
~/AI/cowd gateway
```

The Gateway owns the RuntimeHost and exposes HTTP/SSE APIs for WebUI and TUI
projections. If `gateway.webui_dir` points to a directory containing
`index.html`, Gateway serves that WebUI. If the key is missing or the directory
has no `index.html`, the root route returns health/status information instead
of failing startup.

Example configuration:

```yaml
model: "claude-sonnet-4-6"
gateway:
  enabled: true
  host: "127.0.0.1"
  port: 8642
  webui_dir: "/path/to/cowd-webui/dist"
```

## Runtime Architecture

```text
entrypoints
  cli     static environment/config/tool/skill/doctor management
  tui     interactive user surface through Gateway APIs
  webui   browser surface served by Gateway
  gateway HTTP/SSE runtime service entrypoint

runtime services
  provider/config
  commands
  session
  approval
  mcp
  memory
  matrix
  tools
  app-mfg
  storage

facts
  Memory  unstructured and recalled knowledge
  Matrix  structured facts, entities, relations, and evidence

applications
  MFG     manufacturing application layer built on Matrix/Memory
```

Design rules:

- CLI remains minimal and does not own business runtime state.
- Gateway is the runtime service entrypoint and hosts RuntimeHost.
- TUI and WebUI consume the same Gateway HTTP/SSE APIs.
- Daemon/socket business management is not exposed as a user control plane.
- Model/API credentials belong to config/secrets, not a top-level auth module.
- Matrix and Memory are bottom-layer fact capabilities.
- MFG is an application layer, not a core runtime dependency.
- Internal Rust crate/module names should not carry the project prefix unless
  listed in `docs/architecture/internal-name-allowlist.md`.

## Workspace

Primary crates:

```text
crates/cli
crates/gateway
crates/tui
crates/provider
crates/commands
crates/command-runtime
crates/runtime
crates/session
crates/approval
crates/mcp
crates/memory
crates/matrix
crates/tools
crates/storage
crates/app-mfg
crates/plugins
crates/telemetry
```

`crates/gateway` remains the implementation crate behind the slim `cowd`
binary entrypoint. Its library target is `gateway` while CLI, Gateway,
and TUI responsibilities continue to be separated by crate and service
boundaries.

## Development

```bash
cargo fmt --check
cargo check --workspace --no-default-features
cargo test -p cli --no-default-features -- --nocapture
cargo test -p gateway --test gateway_runtimehost_architecture --no-default-features -- --nocapture
cargo build -p cli --no-default-features
```

Install the debug binary:

```bash
cargo build -p cli --no-default-features
mkdir -p ~/AI
cp target/debug/cowd ~/AI/cowd
```

Use `scripts/validate.sh` for curated validation lanes. Full legacy
`gateway` unit tests are not the default entry migration gate because some old
tests still spawn long-running Gateway child processes.
