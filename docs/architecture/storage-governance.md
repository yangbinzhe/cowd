# Storage Governance

Cowd keeps storage access behind service boundaries so future SQLite, Postgres,
vector, and graph backends can be governed consistently.

## Principles

- Runtime entry surfaces do not open concrete stores directly.
- Gateway routes call Gateway services.
- Services own store/repository construction and health projection.
- Matrix facts, Memory records, Session state, Approval state, and tool
  execution evidence must remain addressable through stable service contracts.
- SQLite busy/locked behavior is treated as a runtime health concern and should
  be surfaced through Gateway health/status.

## Current Stores

| domain | owner | notes |
|---|---|---|
| session | `session` / legacy RuntimeHost adapters | unified session metadata and event log |
| memory | `memory` | recall, layers, packets, maintenance |
| matrix | `matrix` | structured facts, entities, relations, evidence |
| approval | `approval` | approval policy and pending decisions |
| tools | `tools` / command runtime | tool invocation evidence |
| config | `runtime::config` | local configuration including `gateway.webui_dir` |

## Migration Direction

The service contracts should remain stable while backing stores can move from
local SQLite/files to Postgres-backed structured, vector, and graph storage.
New code should add service methods instead of bypassing the service layer from
routes or TUI components.
