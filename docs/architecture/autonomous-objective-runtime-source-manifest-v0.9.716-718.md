# Autonomous Objective Runtime 源码清单（v0.9.716–v0.9.718）

本文是统合方案的可执行 source manifest。目录级 glob 不等于允许编辑；除下列文件外，
任何生产代码、测试、生成文件和配置都必须先做 allowlist amendment。`[new]` 表示目标文件
尚不存在，`[delete]` 表示在完成 caller rewiring 和 deletion preflight 后删除。

## v0.9.716 — Objective/Program Truth

### Phase evidence

```text
docs/evidence/autonomous-objective-runtime-v0.9.716.md [new]
```

### Core production files

```text
crates/harness-contract/src/goal/mod.rs
crates/harness-contract/src/execution_graph/contract.rs
crates/harness-contract/src/execution_graph/state.rs
crates/harness-contract/src/execution_graph/validation.rs
crates/harness-contract/src/acceptance.rs
crates/runtime/src/execution_core/goal/mod.rs
crates/runtime/src/execution_core/goal/policy.rs
crates/runtime/src/execution_core/goal/supervisor.rs [new]
crates/runtime/src/execution_core/supervisor.rs
crates/runtime/src/execution_core/services.rs
crates/runtime/src/execution_core/graph/runner.rs
crates/runtime/src/execution_core/graph/commit_pipeline.rs
crates/runtime/src/execution_core/graph/commit_service.rs
crates/runtime/src/execution_core/graph/events.rs
crates/runtime/src/orchestration/facade.rs
crates/runtime/src/orchestration/compiler.rs
crates/runtime/src/orchestration/collaboration_coordinator.rs
crates/runtime/src/orchestration/intent_compiler.rs
crates/runtime/src/orchestration/validator.rs
crates/runtime/src/orchestration/team_authority.rs
crates/runtime/src/orchestration/mod.rs
crates/runtime/src/mission/mission_runtime.rs
crates/runtime/src/conversation/host.rs
crates/runtime/src/conversation/host_backend.rs
crates/runtime/src/team/instantiation.rs
crates/runtime/src/team/team_binding.rs
crates/runtime/src/team/working_state.rs
crates/runtime/src/team/result_reducer.rs
```

### Contract/runtime tests and fixtures

```text
crates/harness-contract/src/goal/mod.rs                         # inline contract tests
crates/harness-contract/src/execution_graph/contract.rs          # inline Program tests
crates/harness-contract/src/execution_graph/validation.rs       # inline validation tests
crates/runtime/src/execution_core/goal/mod.rs                    # inline reducer tests
crates/runtime/src/execution_core/goal/supervisor.rs             # inline supervisor tests
crates/runtime/src/execution_core/tests/commit.rs
crates/runtime/src/execution_core/tests/services.rs
crates/runtime/src/orchestration/tests/mod.rs
crates/runtime/src/conversation/tests/collaboration.rs
crates/runtime/src/conversation/tests/provider.rs
crates/runtime/src/team/instantiation.rs                         # inline capability/authority tests
crates/runtime/src/team/working_state.rs                         # inline local-evidence boundary tests
crates/runtime/src/team/result_reducer.rs                         # inline local-delivery/non-terminal tests
crates/runtime/tests/ai_harness_deep_scenarios.rs
crates/runtime/tests/ai_harness_e2e.rs
crates/runtime/tests/concurrency_lock_test.rs
crates/runtime/tests/cross_scope_revision_fence.rs
crates/runtime/tests/raw_commit_compile_fail.rs
crates/runtime/tests/release_eligibility_truth_table.rs
crates/runtime/tests/runtime_module_architecture.rs
crates/runtime/tests/verified_principal_and_decision_lease.rs
crates/harness-eval/src/certification.rs
crates/harness-eval/src/live_scenario_runner.rs
crates/harness-eval/src/report.rs
crates/harness-eval/src/terminal_gate.rs
crates/harness-eval/src/terminal_matrix.rs
crates/harness-eval/src/runner.rs
crates/harness-eval/tests/architecture_dependencies.rs
crates/harness-eval/templates/certification-manifest-v1.json
crates/harness-eval/templates/certification-manifest-v1.md
crates/harness-eval/templates/autonomous-collaboration-deepseek-v1.json
```

### v0.9.716 delete candidates (not deletions until preflight)

```text
crates/runtime/src/conversation/host.rs:
  verified_team_terminal_summary
  completed_program_team_ids
  completed_program_team_ids_from_receipt
  has_completed_program_terminal
  root_acceptance_disposition
  activity-count branches in collaboration_program_progress_from_graph
crates/runtime/src/conversation/host_backend.rs:
  control-plane repair loops whose only source is model prose
crates/runtime/src/orchestration/facade.rs:
  duplicate Team semantic ingress branch after single decoder callers are rewired
```

## v0.9.717 — Event-driven Agent, projection, concurrency and cost

### Phase evidence

```text
docs/evidence/autonomous-objective-runtime-v0.9.717.md [new]
```

### Core production files

```text
crates/harness-contract/src/task.rs
crates/harness-contract/src/agent/mod.rs
crates/harness-contract/src/team/mod.rs
crates/harness-contract/src/execution_graph/contract.rs
crates/harness-contract/src/execution_graph/projection.rs
crates/harness-contract/src/execution_graph/state.rs
crates/runtime/src/task/store.rs
crates/runtime/src/task/runtime_port.rs
crates/runtime/src/task/lifecycle.rs
crates/runtime/src/agent/runtime.rs
crates/runtime/src/agent/managed_agent.rs
crates/runtime/src/agent/in_process_worker.rs
crates/runtime/src/agent/run_handle.rs
crates/runtime/src/team/team_runtime.rs
crates/runtime/src/team/projection.rs
crates/runtime/src/team/working_state.rs
crates/runtime/src/execution_core/graph/resources/manager.rs
crates/runtime/src/execution_core/execution_live.rs
crates/runtime/src/projection/mod.rs
crates/runtime/src/projection/delta.rs
crates/runtime/src/projection/snapshot.rs
crates/runtime/src/projection/activity.rs
crates/runtime/src/conversation/prompt_assembly.rs
crates/runtime/src/context/tool_exposure.rs
crates/runtime/src/conversation/context_plane.rs
crates/runtime/src/orchestration/collaboration_coordinator.rs
crates/runtime/src/orchestration/collaboration_continuation.rs
crates/runtime/src/execution_core/services.rs
crates/gateway/src/runtime/gateway_tool_executor.rs
crates/gateway/src/core/event_bus.rs
crates/gateway/src/services/mission_service.rs
crates/gateway/src/services/mod.rs
crates/gateway/src/api_routes/mission_routes.rs
crates/gateway/src/api_routes/session_routes.rs
crates/gateway/src/api_routes/capability_contract.rs
crates/gateway/src/api_routes/route_registry.rs
crates/gateway/src/runtime_host/mod.rs
crates/gateway/src/runtime_host/task_set.rs
/media/yi/Datas/workspace/cowd-edge/crates/edge-contract/src/lib.rs
/media/yi/Datas/workspace/cowd-edge/crates/edge-contract/src/message.rs
/media/yi/Datas/workspace/cowd-edge/crates/edge-contract/src/edge_v2_generated.rs
/media/yi/Datas/workspace/cowd-edge/crates/edge-adapters/src/lib.rs
/media/yi/Datas/workspace/cowd-edge/crates/edge-adapters/src/mirror.rs
/media/yi/Datas/workspace/cowd-edge/contracts/edge/v2/schema.json
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/stores/projectionRegistry.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/stores/liveTransport.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/adapters/executionProjection.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/components/runtime/CollaborationProgramSummary.vue
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/components/runtime/ExecutionTruthSummary.vue
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/types/graph.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/types/evidence.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/generated/gateway-api.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/generated/projection-contract-meta.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/generated/live-contract-meta.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/generated/projection-v3-golden.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/i18n/keys.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/i18n/messages/zh-CN.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/i18n/messages/en-US.ts
```

### Task/Agent/Surface tests

```text
crates/runtime/src/agent/in_process/tests.rs
crates/runtime/src/team/projection.rs                         # inline projection tests
crates/runtime/src/task/store.rs                              # inline backend tests
crates/runtime/src/orchestration/tests/mod.rs                  # market/control tests
crates/runtime/tests/agent_multi_instance.rs
crates/runtime/tests/managed_agent.rs
crates/runtime/tests/managed_agent_dispatcher_fencing.rs
crates/runtime/tests/managed_agent_event_trigger.rs
crates/runtime/tests/team_instantiation.rs
crates/runtime/tests/team_working_state_commit.rs
crates/runtime/tests/concurrency_lock_test.rs
crates/runtime/tests/integration_tests.rs
crates/runtime/tests/mission_harness_e2e_eval.rs               # only deterministic path in 717
crates/gateway/tests/route_contract_parity.rs
crates/gateway/tests/surface_trigger_event_routes.rs
crates/gateway/tests/team_template_routes.rs
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/stores/projectionRegistry.test.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/stores/liveTransport.test.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/stores/chatSessions.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/adapters/executionProjection.test.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/components/runtime/ExecutionTruthSummary.test.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/components/runtime/StrategyDecisionSummary.test.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/components/mission/ExecutionNodeDetail.test.ts
/media/yi/Datas/workspace/cowd-edge/surfaces/webui/src/components/mission/ExecutionGraphCanvas.test.ts
```

### v0.9.717 delete candidates (not deletions until preflight)

```text
runtime-local claim/lifecycle maps in TeamRuntime and ManagedAgentDispatcher
duplicate root polling in harness-eval live_scenario_observer.rs
projection reducer branches that infer terminal from activity counts
obsolete generated fixtures and old semantic codec fixtures after regeneration
agent worker helpers that require a fixed proposer fraction or invoke a Runtime default proposal
absolute leaf-agent prompt prohibiting valid typed initiative proposals (nested runtime identity
creation remains forbidden; typed child-task/team proposals remain allowed)
Team `DeliveryStatus::Satisfied` or execution `OutcomeTerminalClass::Succeeded` branches that are
consumed as Objective success without an Objective Evidence Ledger verdict
```

Concrete symbol and line-level targets must be appended to the phase evidence before deletion.

## v0.9.718 — Final integration only

No new runtime architecture files are allowed after the first paid E2E request. The pre-approved
release/write set is limited to:

```text
scripts/build-release.sh [existing or new release wrapper]
scripts/verify-installed-build.sh [new if absent]
docs/evidence/autonomous-objective-runtime-v0.9.718.md [new]
docs/evidence/autonomous-objective-runtime-v0.9.718-report.json [generated artifact]
```

If a production source change is required after E2E begins, stop the run, classify it to 716/717,
amend the DAG and repeat the corresponding code-only gates before paying for E2E again.

## Cross-repository restrictions

- Core and Edge are edited one version at a time; no overlapping writers.
- Edge generated files are changed only from the audited Core contract and recorded generation command.
- No source path outside this manifest may be changed silently.
- New files require owner, caller, schema, test and deletion decision before creation.
- Evidence files are not completion proof until their referenced commit/tree/build hashes exist.

## v0.9.719 — Closure repair and evidence convergence amendment

This amendment owns repairs discovered by the reverse audit after the v0.9.718 candidate. It is
not a new scheduler or a relaxation of the three-version architecture.

```text
docs/architecture/autonomous-objective-runtime-closure-repair-v0.9.719.md [new]
scripts/manual/webui-live-workbench.sh
crates/harness-contract/src/outcome.rs
crates/runtime/src/execution_core/goal/mod.rs
crates/runtime/src/execution_core/goal/supervisor.rs
crates/runtime/src/orchestration/collaboration_coordinator.rs
crates/runtime/src/conversation/host_presentation.rs
crates/runtime/src/execution_core/services.rs
crates/runtime/src/agent/in_process_worker.rs
crates/runtime/src/agent/in_process/tests.rs
crates/runtime/src/agent/runtime.rs
crates/runtime/src/conversation/evidence_terminal_plane.rs
crates/runtime/src/execution_core/outcome_service.rs
crates/runtime/src/recovery/outcome_projector.rs
crates/runtime/src/evolution/projector.rs
crates/runtime/src/skill/maintenance.rs
docs/evidence/autonomous-objective-runtime-v0.9.716.md
docs/evidence/autonomous-objective-runtime-v0.9.717.md
docs/evidence/autonomous-objective-runtime-v0.9.718.md
```

Required deletion/scan targets: fixed-ratio autonomous proposal helpers and tests; direct
Objective `Satisfied` writers outside `ObjectiveSupervisor`; unscoped Team/Graph success
consumers; browser scripts referencing missing specs/configs; evidence status contradictions.
