# Gateway API Inventory

Gateway is the shared runtime API for TUI and WebUI. This inventory records the
active contract groups that must remain available during the entrypoint split.

| group | status | consumers | purpose |
|---|---|---|---|
| health/ready/root | active | TUI, WebUI, operators | RuntimeHost, storage, provider, WebUI static status |
| config/provider/model | active | TUI, WebUI, CLI doctor | model/provider projection and updates |
| commands | active | TUI, WebUI | command registry, slash projection, action dispatch |
| sessions/timeline | active | TUI, WebUI | session metadata, event log, timeline projection |
| memory/context | active | TUI, WebUI | recall, context packets, maintenance state |
| matrix | active | WebUI, MFG, reports | structured facts, entities, relations, evidence |
| app-mfg | active | WebUI, reports | manufacturing incidents, analysis, actions, reports |
| approvals | active | TUI, WebUI | approval gates and decisions |
| tools/mcp | active | TUI, WebUI | tool and MCP status/action surface |
| static WebUI | active | browser | serves `gateway.webui_dir` when valid |

## Rules

- TUI client methods must map to an active Gateway endpoint or be marked
  deleted/replaced in a migration report.
- Gateway routes should delegate to services.
- WebUI and TUI may differ in projection shape, but they should not use
  different business execution paths.
- Gateway root must degrade to health/status when no valid WebUI `index.html`
  exists.
