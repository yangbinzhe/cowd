# Core Architecture

Cowd is split around entry surfaces and runtime services.

## Entry Surfaces

| surface | role | runtime access |
|---|---|---|
| CLI | static environment, config, tool, skill, and doctor management | starts TUI or Gateway; no business state ownership |
| TUI | interactive terminal surface | Gateway HTTP/SSE API |
| WebUI | browser surface | Gateway HTTP/SSE API |
| Gateway | HTTP/SSE runtime service entrypoint | owns RuntimeHost and service adapters |

`cowd` without a subcommand enters the TUI. `run`, `chat`, `prompt`, and direct
daemon business management are intentionally not part of the CLI surface.

## Runtime Services

Gateway hosts RuntimeHost and coordinates these services:

- provider/config
- command
- session
- approval
- mcp
- memory
- matrix
- tools
- app-mfg
- storage

Routes should call Gateway services rather than opening concrete stores or
kernels directly. TUI/WebUI should consume Gateway projections instead of
importing runtime, command, matrix, storage, memory, or tool internals.

## Fact Engines

Memory and Matrix are the two fact capabilities:

- Memory handles unstructured knowledge, recall, and contextual packets.
- Matrix handles structured facts, entities, relations, evidence, and
  manufacturing inputs.

MFG is an application layer that consumes Memory/Matrix facts. It is not a
runtime core dependency.

## Static WebUI

The WebUI is external to this repository in `cowd-edge/surfaces/webui`.
Gateway reads `gateway.webui_dir` and
serves the configured directory only when it contains `index.html`. Without a
valid configured WebUI directory, Gateway remains healthy and returns runtime
health/status at the root route.

## Naming

Internal Rust names should not repeat the project name. External-stable names
and temporary migration exceptions are tracked in
`docs/architecture/internal-name-allowlist.md`.
