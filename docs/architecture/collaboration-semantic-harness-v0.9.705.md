# v0.9.705 — Semantic Intent and Deterministic Compilation

## Contract status

This document is the subordinate execution contract for `v0.9.705`. The sole
cross-version authority remains
`docs/architecture/collaboration-program-hardening.md`. Implementation may
start only after the user accepts the three-version plan and its audit.

Baseline:

- `cowd-0821-terminal`: tag `v0.9.704`, commit
  `31f578078727c59035a2a2c47a219e50ae429676`.
- `cowd-edge`: tag `v0.9.704`, commit
  `04b63861e9e332576d08a2f81326942b22c92e9a`.
- This version depends only on those baselines. It does not depend on
  `v0.9.706` or `v0.9.707`.

## Closed outcome

A user may name an arbitrary Team and arbitrary localized role names without
a local Team template. The model describes business semantics, dataflow and
acceptance. Runtime alone:

1. validates one canonical semantic contract;
2. resolves approved Agent Definitions by capability, Skill and Tool
   predicates;
3. derives behavior facets and exact bindings without inspecting display
   names;
4. persists a turn-scoped immutable Team snapshot and Program provenance;
5. emits a typed, repairable gap instead of selecting a builtin fallback or
   inventing missing semantics.

The version is incomplete if a valid custom Team still needs
`ModelProposedRole.behavior`, `agent_definition_ref`, a local Team template, a
role-name branch, or a fallback Agent substitution.

## Explicit non-goals

- No operational queue, parallelism or veto-window policy change; those belong
  to `v0.9.706`.
- No experience auto-promotion or reusable catalog publication; those belong
  to `v0.9.707`.
- No visual redesign. `cowd-edge` receives generated contract compatibility
  only; role/provenance rendering belongs to `v0.9.706`.
- No new scheduler, Team registry, approval store or execution lifecycle.

## Current source facts

| Current owner | Fact observed in `v0.9.704` | Defect to close |
| --- | --- | --- |
| `crates/harness-contract/src/orchestration.rs` | `ModelCollaborationWorkstream.template` embeds `ModelTemplateProposal`; each role authors an exact Agent ref, grant ceiling and `RoleBehaviorFacet`. Dependencies accept five wire shapes. | One narrow semantic tool still exposes compiler mechanics and multiple internal meanings. |
| `ModelCollaborationControlDecision::into_runtime_orchestration_input` | The contract crate lowers a collaboration decision to generic graph nodes and JSON template wrappers. | A transport contract owns lowering that requires Runtime registry and policy facts. |
| `crates/gateway/src/runtime/gateway_tool_executor.rs` | `submit_collaboration_decision` deserializes and immediately calls the conversion above. | Gateway cannot distinguish canonical v2 input from legacy compatibility or attach trusted ingress provenance. |
| `crates/runtime/src/team/template_candidate.rs` | Missing or unknown Agent refs become `builtin/cowd/explore@1`, `explore@2` or `execute@1`; missing responsibility is synthesized; group dependencies may resolve by substring/team suffix. | Silent semantic repair causes template contamination and direction changes. |
| `crates/runtime/src/agent/catalog.rs` | Catalog eligibility matches capability strings only; approved Definition Skill refs are not projected. | Runtime cannot resolve a role by the complete semantic requirement. |
| `crates/runtime/src/agent/definition_registry.rs` | Approved stable Definition revisions already carry capability ceilings, Skill refs, exact revision identity and evaluation contracts. | The data exists but the catalog projection discards Skill predicates. |
| `crates/runtime/src/orchestration/compiler.rs` | Team compilation consumes exact ephemeral snapshots or exact catalog selectors. | This is the correct lowering boundary, but no semantic `IntentCompiler` precedes it. |
| `crates/runtime/src/infrastructure/capability_manifest.rs` and `execution_core/model_affordance.rs` | Model guidance asks the model to author physical Agent refs, grant ceilings and behavior facets. | The prompt reinforces the wrong abstraction. |
| `crates/harness-contract/src/execution_graph/contract.rs` | `CollaborationProgram` persists Team instances, edges, obligations and terminals, but not the surface-safe semantic intent/provenance that produced them. | UI, recovery audit and later experience extraction cannot explain why a role exists. |

## One canonical model-facing contract

The v2 model contract is small, versioned and semantic. Names below are the
required Rust concepts; exact field comments and `schemars` descriptions are
part of the implementation.

```text
ModelCollaborationControlDecisionV2
  schema_version = 2
  decision_id
  intent
  workstreams[]
  reason

ModelCollaborationWorkstreamV2
  workstream_id
  objective
  depends_on[]                  # cross-Team semantic ids
  team: ModelTurnScopedTeamIntent
  output_artifacts[]
  evidence_contract[]           # typed criteria, not free-form opcodes
  managed_agent_escalation

ModelTurnScopedTeamIntent
  team_key
  display_name?
  roles[]
  dependencies[]               # canonical producer -> consumer pairs
  result
  instructions

ModelRoleIntent
  role_id
  display_name?
  responsibility
  required_capabilities[]       # registry names, never effective grants
  required_skills[]             # semantic Skill refs/predicates
  required_tools[]              # Tool contract ids
  cardinality { min, preferred, max }
  acceptance[]                  # tagged semantic criteria
  input_artifacts[]
  output_artifacts[]

ModelRoleDependency
  from
  to
  kind = evidence_feed | review_of | handoff | aggregate | dispute
  artifacts[]

ModelTeamResultIntent
  required_artifacts[]
  evidence_required
  synthesis_required
```

The contract deliberately excludes:

- Agent Definition ids/revisions, Agent instances and leases;
- permission modes, grants, approval ids and resource limits;
- `RoleBehaviorFacet`, scheduler recipes and physical graph ids;
- catalog publication lifecycle;
- success, terminal or evidence claims.

`required_capabilities`, Skill refs and Tool ids remain extensible registry
data. Unknown values fail with a typed gap; production code does not contain a
finite list of business roles. A finite enum is retained only for stable
mechanical relation kinds and acceptance criterion kinds.

## Semantic acceptance representation

New writers use a tagged `ModelSemanticAcceptanceCriterion`:

- `artifact { kind }`
- `evidence_scope { operation, resource }`
- `structured_field { path }`
- `terminal_fact { kind }`
- `committed_effect { kind }`
- `independent_review { subject_role_id }`

The compiler lowers these values to the existing typed acceptance/evidence
contracts. Legacy string criteria are decoded only at ingress. Runtime never
parses a role display name or responsibility sentence to recover mechanics.

## Deterministic lowering rules

`crates/runtime/src/orchestration/intent_compiler.rs` becomes the only
semantic-to-compiled lowering owner.

### Syntax normalization

- Trim and Unicode-normalize presentation strings without translating them.
- Preserve an arbitrary user role name in `display_name`.
- If `role_id` is not a valid machine id, derive `role-<digest>` from the
  original value and retain the original as display data.
- Sort/deduplicate unordered predicates before hashing.
- Reject duplicate logical ids, cycles, unknown dependency endpoints, missing
  responsibilities, empty outputs needed by a dependency, and ambiguous
  synthesis. Do not invent semantic content.

### Behavior derivation

Compiled `RoleBehaviorFacet` remains a rigid Runtime carrier and is derived
only from typed relations and acceptance:

| Semantic fact | Compiled facet |
| --- | --- |
| Any incoming dependency | `UpstreamConsumption { required: true }` |
| Incoming `review_of` or `independent_review` criterion | `Verification` |
| Incoming `aggregate` edges and unique result sink | `Reducer` |
| Source role with a required evidence-scope criterion | `ReacquireEvidence { required: true }` |
| Role whose outputs satisfy the Team result contract and has no downstream consumer | `TerminalCandidate { required: true }` |

If `synthesis_required` has zero or multiple valid reducers, compilation
returns `AmbiguousCompletionGap`; it never picks the last role, a role named
“reviewer”, or a lexical winner.

### Agent resolution

`AgentCatalogEntry` adds approved `skill_refs`. A new
`RoleResolutionRequirement` carries capabilities, Skill refs and Tool
contracts. Eligible Definitions must cover all predicates and remain exact,
approved and runnable.

Deterministic ranking is:

1. all required predicates satisfied;
2. smallest excess capability set;
3. smallest excess Skill set;
4. policy-approved scope preference;
5. immutable Definition id and revision as the final stable tie-breaker.

The selected exact Definition revision is passed to the existing
`AgentBindingCompiler`. Effective grants equal the requested semantic
capabilities intersected with the authenticated permission ceiling. An empty
or insufficient intersection is an `AuthorizationGap`, not a downgrade to
Read. Required Tool contracts are validated against their required capability
before the binding is frozen.

### Program provenance

`CollaborationProgram` gains an additive, legacy-readable
`semantic_intent: Option<CollaborationSemanticIntentSnapshot>` containing:

- schema version, decision id and canonical intent digest;
- `origin = user_directed_turn_scoped | explicit_catalog | runtime_replan`;
- `lifecycle = turn_scoped | publish_candidate | catalog_revision`;
- source Session/Turn refs;
- surface-safe Team and role display/responsibility/predicate/dataflow facts;
- compiler revision and exact binding digest refs;
- `ai_composed = true` for the no-template path;
- `published_template_ref = None` unless a separate governed publication
  command succeeds.

The snapshot contains no prompt, private reasoning, secret, raw Tool input or
mutable catalog pointer.

## Trusted ingress and compatibility boundary

Gateway first attempts the strict v2 decoder. A private, version-named legacy
wire DTO accepts `template`, old behavior facets, group dependency shapes and
string acceptance. It immediately converts to v2 and is then discarded.

Compatibility rules:

- old behavior facets may determine the corresponding typed relation during
  decoding, but never enter the active Runtime IR;
- exact old Agent refs are ignored for a turn-scoped custom Team unless the
  authenticated ingress is the existing explicit-catalog path;
- group labels must resolve exactly; the substring/team-suffix heuristic is
  deleted;
- a legacy value that cannot be converted without inventing semantics returns
  `LegacySemanticGap` with field paths;
- no v2 writer emits the v1 shape, and no Runtime scheduler accepts the legacy
  DTO.

`submit_collaboration_decision` assigns trusted
`UserDirectedTurnScopedCollaboration` provenance in Runtime code. The model is
not allowed to assert that origin. `runtime_orchestrate` retains explicit
catalog selection and non-Team graph operations, but it no longer offers a
second custom-Team lowering path.

## Typed compensation loop

All compiler failures use `CollaborationCompileDiagnostic`:

```text
code
phase = decode | validate | resolve | bind | lower
field_paths[]
semantic_ids[]
missing_capabilities[]
missing_skills[]
missing_tools[]
authorization_gap?
repairability = model_revise | user_decision | config_change | non_repairable
allowed_repairs[]
evidence_refs[]
```

Gateway returns the diagnostic as structured Tool output. The parent model may
submit one bounded revised decision using the same turn fence and a new
decision revision. It may not claim execution happened, mutate explicit user
names, or relax authorization. Repeated identical diagnostics terminate as a
typed failure instead of looping.

## Exact source allowlist

### `cowd-0821-terminal`

- version surfaces: root `Cargo.toml`, `Cargo.lock`, version-owned README and
  release scripts selected by the version gate;
- `crates/harness-contract/src/orchestration.rs`;
- `crates/harness-contract/src/execution_graph/contract.rs`;
- `crates/harness-contract/src/execution_graph/mod.rs` and
  `crates/harness-contract/src/lib.rs` only if needed to export new contracts;
- `crates/runtime/src/orchestration/{mod,request,validator,compiler,intent_compiler,team_authority,collaboration_coordinator}.rs`;
- `crates/runtime/src/team/{template_candidate,instantiation,team_binding}.rs`;
- `crates/runtime/src/agent/{catalog,definition_registry,binding}.rs`;
- `crates/runtime/src/infrastructure/capability_manifest.rs`;
- `crates/runtime/src/execution_core/model_affordance.rs`;
- `crates/runtime/src/conversation/host.rs` only for tool activation/guidance
  and bounded compensation wiring, not lifecycle ownership;
- `crates/gateway/src/runtime/{runtime_bootstrap,gateway_tool_executor}.rs`;
- directly affected OpenAPI/capability-contract tests;
- `docs/evidence/collaboration-semantic-compiler-v0.9.705.md`.

### `cowd-edge`

- generated Gateway API/projection metadata required for compilation;
- generator inputs/scripts if the generated diff proves they are the owner;
- no presentation component change in this version.

Any required production file outside this allowlist pauses the phase for an
allowlist amendment with a caller/state explanation. Test fixtures colocated
with an allowed source file are included.

## Deletion and caller-reconnection table

| Retired production behavior | Replacement | Completion proof |
| --- | --- | --- |
| `ModelCollaborationControlDecision::into_runtime_orchestration_input` lowering | Runtime `IntentCompiler` with registry/policy access | No contract-layer semantic-to-physical lowering caller remains. |
| Turn-scoped `ModelTemplateProposal` as active IR | `ModelTurnScopedTeamIntent` | Narrow tool schema contains no behavior, exact Agent or grant field. |
| `normalize_agent_definition_ref` builtin default | capability/Skill resolver | Source scan finds no fallback substitution; negative test returns typed gap. |
| unknown Definition fallback in `TemplateCandidateCompiler` | exact approved resolver | Invented ref never executes. |
| missing responsibility default | model compensation diagnostic | Missing responsibility fails at `roles[n].responsibility`. |
| dependency substring/team-suffix inference | exact canonical edges | Arbitrary names and adversarial substrings preserve topology. |
| model-authored behavior in custom-Team path | compiler-derived facets | Every persisted role has non-empty valid facets and no name-based branch. |
| generic `runtime_orchestrate` custom-Team duplicate | narrow v2 admission path | Tool schemas and Gateway tests prove only explicit catalog selection remains generic. |

Published Team manifests and historical compiled snapshots keep
`RoleBehaviorFacet` and exact Agent refs because those are correct immutable
execution facts. Only their model authorship is removed.

## State and concurrency map

| State | Canonical owner | Writer | Reader | Recovery |
| --- | --- | --- | --- | --- |
| v2 semantic decision | current Turn admission | Gateway decoder then Runtime compiler | validator/compiler | replay same decision id + Turn fence; digest must match |
| compiled semantic snapshot | `CollaborationProgram` graph metadata | compiler/graph commit | recovery, projection, experience | graph event replay |
| ephemeral Team revision | Program/Team request | Runtime compiler | Team instantiation | immutable snapshot in graph payload |
| exact Agent binding | Agent binding store/Team snapshot | `AgentBindingCompiler` | Agent runtime | digest/revision replay; no re-resolution |
| compile diagnostic | Tool result/event evidence | Runtime compiler | model/Surface | idempotent diagnostic digest |

Compilation is synchronous and bounded by the admitted semantic shape. It
holds no graph mutation lock while resolving registry data and performs no
provider, Tool or user wait. The graph commit uses the existing optimistic
revision fence.

## Failure and recovery map

| Failure | Required terminal behavior |
| --- | --- |
| malformed v2 | typed decode diagnostic; no Program or template write |
| convertible legacy v1 | one canonical v2 digest; legacy object discarded |
| ambiguous legacy v1 | typed compatibility gap; no guessed edge |
| missing capability/Skill/Tool | typed resolver gap with eligible predicate evidence |
| permission ceiling too low | authorization gap; no clipped success |
| stale catalog revision during binding | retry exact resolution within bounded CAS, then typed registry conflict |
| crash before graph commit | no admitted Program; idempotent retry safe |
| crash after graph commit | exact semantic snapshot and binding digest replay; no model re-composition |
| duplicate tool call | same Turn/decision digest returns the existing admission receipt |

## Evidence gates

### Contract and property tests

- strict v2 JSON Schema golden;
- native function and structured-output codecs produce the same canonical
  digest;
- each legacy shape maps to the same digest or an explicit gap;
- generated arbitrary Unicode Team/role names never affect mechanics;
- generated DAGs preserve producer-to-consumer direction;
- cycles, ambiguous reducers, missing artifacts and duplicate ids fail;
- no role-name/display-name text is read outside normalization/presentation.

### Resolver and binding tests

- capability-only, Skill-constrained and Tool-constrained selection;
- deterministic tie-breaking across insertion order and restart;
- unknown predicates and insufficient permission fail closed;
- exact binding digest survives restart;
- turn-scoped compilation never creates a catalog pointer or revision.

### Build and scan gates

- `cargo fmt --all -- --check`;
- focused Debug check/tests for harness-contract, runtime and gateway;
- full workspace Debug check before version close;
- OpenAPI generation and `cowd-edge` TypeScript compilation;
- source scans for retired fallback helpers, model-facing `behavior`, model-
  facing `agent_definition_ref`, substring dependency resolution and duplicate
  custom-Team ingress.

### Version close

Only after the evidence document records commands, output hashes, source
scans, worktree state and reverse audit:

1. bump all terminal version surfaces to `0.9.705`;
2. commit each repository independently with no mixed/untracked residue;
3. create annotated `v0.9.705` tags in both repositories at their matching
   phase commits;
4. do not push without explicit user authorization.

## Reverse audit

For every persisted custom Team, an auditor must be able to traverse:

```text
exact Agent binding
  <- resolver receipt and approved Definition revision
  <- compiled role requirement and derived behavior facets
  <- canonical role/dataflow/result intent
  <- decision digest and authenticated Turn fence
  <- original user-directed collaboration admission
```

If any link depends on model prose, a display name, a mutable latest pointer or
a fallback substitution, `v0.9.705` does not close.
