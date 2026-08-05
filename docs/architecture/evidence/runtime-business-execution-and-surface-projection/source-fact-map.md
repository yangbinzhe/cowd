# Runtime Business Execution Terminal Refactor: Source Fact Map

Snapshot: `runtime-business-execution-terminal-20260805T232922+0800`

## Existing Dirty Changes

| Area | Current files | Classification | Terminal decision |
|---|---|---|---|
| Activity contract | `harness-contract/src/projection/activity.rs`, `delta.rs` | W1 early implementation | Keep labels, phase, status reason, required and result summary; add exact identities and Skill kind |
| Activity projection | `runtime/src/projection/activity.rs` | W4 early implementation | Keep bounded labels and completion rules; delete duplicate Team/Agent activities and observed parallel inference |
| Event query | `runtime_event_store.rs`, `runtime-postgres/src/lib.rs` | W4 early implementation | Replace string stream/ref scope query with typed root execution/activity indexes |
| WebUI projection | `cowd-edge/surfaces/webui/src` | W5 early implementation | Keep live transport and useful presentation fields; delete multi-source topology and inferred ownership |
| WebUI assets | `cowd-edge/surfaces/webui/assets/app`, `index.html` | Generated | Rebuild once from final W5 source during W7 |

## Source Facts and Decisions

| Symbol/capability | Current owner and evidence | Current problem | Terminal owner and decision |
|---|---|---|---|
| Mission panorama | `runtime/src/mission/mission_control.rs::projection`, `mission_graph` | Already implemented; a new graph would duplicate truth | Extend existing `MissionControlProjection` only |
| Execution scheduling | `runtime/src/execution_core`, `orchestration` | Model-visible and Runtime-owned fields are mixed | `ExecutionGraph` remains sole scheduling truth; split model contract and resolved Runtime command |
| Activity identity | `runtime/src/projection/activity.rs` | Graph node plus TeamRun/AgentRun can become duplicate narrative nodes | One graph-backed activity, enriched by typed run binding |
| Event identity vocabulary | Runtime event refs use `node`/`execution_node` and `execution`/`execution_graph` variants | String vocabulary is not a safe identity contract | `RuntimeActivityBinding` is authoritative; refs remain domain/evidence references |
| Skill activation | `ConversationRuntime::activate_skills_for_turn` | Session event lacks stable execution/agent identity | One canonical Skill activation event bound to execution and Agent run |
| Tool/public output live path | `ModelStreamReducer`, `ExecutionLiveStore` | Live output exists, but business activity still depends on separate event reconstruction | Keep high-frequency output in `ExecutionLiveUpdate`; lifecycle updates canonical Activity Delta |
| Orchestration input | `runtime/orchestration/request.rs`, Gateway hand-written schema | `input_refs` mixes model semantics and physical Runtime refs | Remove it; use typed model input, dependencies, evidence refs and explicit `target_session_id` |
| Projection delta | `runtime/src/projection/delta.rs` | A relevant event rebuilds all activities | Event-local reducer keyed by typed activity binding |
| Activity detail | `runtime/src/projection/mod.rs::activity_detail` | Builds a full snapshot to return one node | Indexed detail read |
| Related entities | `projection/snapshot.rs::related_event_entities` | Global `all_events(512)` can omit relevant events | Root execution/activity indexed query |
| WebUI business topology | `executionActivity.ts`, `executionLineage.ts` | Canonical + Session events + inferred owners | Canonical Activity only; Session events are technical audit |
| TUI | Existing Runtime projection consumers | Must not gain a second graph or status interpreter | Consume the same Activity status, identity and delta contract |

## Whole Chain

```text
Session input
  -> ingress execution identity
  -> model orchestration proposal
  -> typed validation and Runtime input resolution
  -> atomic ExecutionGraph
  -> Team / Agent / Skill / Tool lifecycle writers
  -> RuntimeActivityBinding
  -> durable event and in-memory reducer
  -> Activity snapshot/delta/detail
  -> Gateway subscription
  -> WebUI/TUI filtered views
```

Reverse evidence:

```text
Surface activity
  -> activity_id and binding
  -> durable event / graph node / run
  -> resolved plan
  -> model proposal and admitted Session input
```
