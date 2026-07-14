# Gateway API Inventory

Full generated route reference: [`../api/gateway-api-reference.md`](../api/gateway-api-reference.md).
Gateway API framework and relationship model: [`gateway-api-framework.md`](gateway-api-framework.md).

Gateway is the shared runtime API for TUI, WebUI, surfaces, connectors, and AI
runtime control. The runtime source of truth is
`GET /api/gateway/capability-contract`; `GET /api/gateway/openapi.json` and
`GET /api/gateway/openai-tools` are derived machine-readable projections.
This inventory records the active contract groups that must remain available
during entrypoint and surface evolution.

| group | status | consumers | purpose |
|---|---|---|---|
| health/ready/root | active | TUI, WebUI, operators | RuntimeHost, storage, provider, WebUI static status |
| config/provider/model | active | TUI, WebUI, CLI doctor | model/provider projection and updates |
| command | active | TUI, WebUI | command registry, slash projection, action dispatch |
| sessions/timeline | active | TUI, WebUI | session metadata, event log, timeline projection |
| memory/context | active | TUI, WebUI | recall, context packets, maintenance state |
| matrix | active | WebUI, MFG, reports | structured facts, entities, relations, evidence |
| app-mfg | active | WebUI, reports | manufacturing incidents, analysis, actions, reports |
| approvals | active | TUI, WebUI | approval gates and decisions |
| tools/mcp | active | TUI, WebUI | tool and MCP status/action surface |
| static WebUI | active | browser | serves `cowd-edge/surfaces/webui` build output from `gateway.webui_dir` when valid |

## Rules

- TUI client methods must map to an active Gateway endpoint or be marked
  deleted/replaced in a migration report.
- Gateway routes should delegate to services.
- WebUI and TUI may differ in projection shape, but they should not use
  different business execution paths.
- Gateway root must degrade to health/status when no valid WebUI `index.html`
  exists.
