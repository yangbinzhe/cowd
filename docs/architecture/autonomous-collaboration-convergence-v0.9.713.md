# Autonomous Collaboration Convergence v0.9.713

## Decision

Autonomous Team work is a Runtime state-machine obligation, not a prompt-only
convention. A valid execution must satisfy ten coupled invariants:

1. A successor receives one causally terminal `runtime_change` receipt per
   workspace path. Historical writes remain on the predecessor result but do
   not become impossible successor digest obligations. Divergent terminal
   writes fail closed.
2. `write:.` maps to the checkpoint subsystem's canonical whole-workspace
   representation (an omitted path list). A model-supplied path list cannot
   narrow or redirect this Runtime-owned guard.
3. A proposal is admitted only when another eligible Agent is runnable now or
   remains reachable later in the admitted dependency topology. This permits a
   serial Team to hand work to its next role, while any Failed/Cancelled
   ancestor makes that future capacity ineligible.
4. Claim leases cover at least the work contract's declared duration, bounded
   by a 5-second minimum and 1-hour maximum. Expired claims advertise reclaim,
   never heartbeat or submit. The agent checkpoint carries the same rule.
5. Required autonomous work may block terminal reducers only while a Team Agent
   can still act. If every Agent is terminal, the reducer moves the unstarted
   terminal reducer to the legal `Blocked` state with a typed
   `autonomous_work_orphaned` failure instead of leaving a forever-running
   graph or violating the graph state machine with `Planned -> Failed`.
6. A Team whose frozen instructions explicitly require
   `collaboration_control propose_work` turns that statement into a Runtime
   checkpoint obligation. The topological first half of its Agents (2/4 or
   3/6) must each author one proposal. After three bounded native-tool
   opportunities, Runtime commits one identity-attested idempotent bootstrap
   proposal if the model still omitted it; only failure of that governed
   mutation fails the Agent.
7. Independent work review and epistemic red-team discussion are different
   protocols. A reviewer accepts correct submitted work and challenges it only
   for a concrete evidence-backed defect; unresolved work challenges block
   completion. A Team-level counterargument uses a response-required
   `challenge` entry and must close through a thread-linked `resolution`.
   Runtime supplies revision fencing and control-plane reachability but never
   fabricates a negative verdict merely to satisfy an activity counter.
8. A Focus role whose business receipts and exact scopes are already satisfied
   cannot be downgraded solely because its model omitted presentation keys.
   After bounded recovery, Runtime may copy actual ToolHost receipts into the
   evidence carrier and wrap custom artifacts; missing disclosure fields remain
   explicit unknowns and are never fabricated as empty lists.
9. A direct Agent workspace write becomes a delivery materialization only when
   a later read observes the same digest and a positive byte length. This
   produces the same typed `WorkspaceMaterializationReceipt` consumed by root
   delivery and evaluation.
10. `detail_scope=summary` is a bounded transport contract: it preserves public
    Team/Agent identity, topology, lifecycle, numeric usage and work-market
    receipts while cropping lossless result bodies, acceptance observations and
    private path lists behind durable references.

The live evaluator uses Summary projections for revision polling. Full
projections remain available for one-shot failure diagnostics, but are not a
progress transport. Acceptance still consumes the public graph, work states,
bounded activities, delivery envelope and terminal evidence references.

## Ownership

- `AgentTaskExecutor` owns predecessor evidence normalization and dynamic
  successor obligations.
- `ScopedRuntimeToolExecutor` owns checkpoint authority compilation.
- `TeamRuntime` owns actionable market capacity, proposal admission, lease
  duration and owner-scoped control actions.
- `ExecutionGraphRunner` owns fail-closed orphan convergence.
- Harness Eval owns observation cost and acceptance polling policy; it cannot
  change Runtime state to make a scenario pass.

No model response, Team-board entry, UI node or evaluator heuristic may replace
these owners.

## Acceptance gates

- repeated writes select only the causally terminal digest;
- divergent final writes are rejected;
- bounded and root checkpoint scopes compile to their canonical forms;
- serial dependency successors are advertised as future schedulable peers but
  never as currently active peers;
- failed dependency ancestry is never proposal capacity;
- explicit 4-Agent and 6-Agent market contracts require respectively two and
  three topological proposers;
- independent work acceptance/challenge is revision fenced, and no unresolved
  work challenge may remain at completion;
- epistemic challenges are response-required and must have a thread-linked
  resolution;
- receipt-backed terminal normalization never invents evidence or empty risk
  declarations;
- Agent write/read-back truth produces one typed delivery materialization;
- Summary graph payloads are bounded without losing terminal Agent identity;
- a requested short lease expands to the declared task duration;
- unresolved work is not orphaned while any Agent is active and is orphaned
  after all Agents become terminal;
- Runtime, Gateway, Harness Contract and Harness Eval suites pass;
- one isolated DeepSeek `deepseek-v4-flash` 4-Team/16-Agent run completes all
  required work, reviews, challenges, discussions and the single materialized
  output without fallback-provider evidence;
- observed projection traffic is bounded and no longer repeats full lineage
  projections on every graph revision.

## Release rule

The real-provider run occurs once, only after deterministic gates and a clean
candidate commit. Installation, cache cleanup and the local annotated tag occur
only after that immutable report passes. No remote push is part of this phase.

## Candidate-3 incident and amended closure design

The clean `3976d58e` candidate falsified three assumptions in the preceding
design. The immutable report is
`target/acceptance/real-qwen/runs/v0.9.713-1788236592-mission-harness-deep/report.json`
and the retained Gateway event store ends at commit cursor `4875`.

1. A checkpoint prompt is not actionable merely because the Agent Binding
   allows `collaboration_control`. Dynamic exposure can leave a continuation
   with an empty native function-call contract. A checkpoint that contains a
   market mutation must install a one-request governed tool overlay and require
   one real native action. The overlay is restricted to tools already present
   in the immutable Binding; it cannot elevate authority.
2. Failing a designated proposer after three prompts makes model protocol
   compliance a graph-liveness dependency. After the bounded model opportunity,
   Runtime must apply exactly one identity-derived, idempotent default proposal
   when another schedulable peer exists. The proposal contains no invented
   evidence or research conclusion and still goes through `TeamRuntime` CAS,
   eligibility and Binding checks. Failure of that mutation remains fatal.
3. Dependency and orphan convergence cannot depend only on a later scheduler
   wake-up. Every successful terminal wave must run the dependency reducer
   before executor cleanup, and a completion pump may not return while there is
   no active/ready node but the durable graph remains non-terminal. Required
   autonomous work with all Agent nodes terminal must therefore commit a legal
   terminal `Blocked` transition carrying `autonomous_work_orphaned` in the
   same convergence cycle.
4. Summary graph cropping after activity construction is too late. The root
   and every descendant graph must be cropped before activity reduction, and
   high-frequency model/tool/runtime activity collections must have stable
   newest-first limits. Team, Agent, execution, verification and autonomous
   work identities remain complete. Lossless details stay behind the existing
   activity detail and Full projection authorities.

`coordinate_and_reinspect` is observational state, not a required action. It
must not keep a proposer in bounded continuation rounds while only a future
peer can advance the work. Bounded continuations stop as soon as no mutation is
currently executable by the bound Agent.

### Audited ownership and non-regression rules

| Concern | Sole owner | Required behavior | Forbidden shortcut |
| --- | --- | --- | --- |
| Checkpoint tool availability | `ConversationRuntime` one-request overlay, requested by `AgentTaskExecutor` | expose only Binding-allowed schemas and require a real call when a mutation is pending | prompt text claiming a tool is available |
| Default proposal | `TeamRuntime::apply_collaboration_control` | derive actor/role/capability from the immutable packet; stable idempotency key; normal CAS and peer eligibility | direct graph mutation or model-authored identity |
| Market semantics | Agent plus `TeamRuntime` | model remains responsible for evidence, execution, submission and peer verdicts; Runtime default covers only the required bootstrap proposal | fabricated submission, review, finding or evidence |
| Terminal convergence | `ExecutionGraphRunner` | repump dependencies immediately after terminal commit; block orphaned required work with a typed failure; never return a non-terminal quiescent report | illegal `Planned -> Failed` transition or evaluator-side cancellation presented as completion |
| Summary transport | Runtime projection reducer | complete control-plane identity/topology with bounded noisy detail | evaluator-specific response rewriting |

The source audit found an existing safe API for every authorized mutation:
`require_next_model_tool_action` accepts only the current catalog subset;
`apply_collaboration_control` derives identity from the bound packet and already
implements proposal idempotency; `advance_dependencies` already owns typed
orphan failure. The implementation therefore extends these owners and adds no
parallel state store, compatibility path or evaluator mutation.

### Unambiguous implementation sequence

1. Replace the string checkpoint result with a typed plan carrying prompt,
   actionable flag and Binding-cropped tool ids. Require a provider tool action
   only for an actionable plan. Remove non-actionable proposer coordination
   from the mutation list.
2. Replace the post-checkpoint proposer failure with an async helper that
   reloads graph truth, applies the stable default proposal through
   `TeamRuntime`, records `agent.autonomy.runtime_default_applied`, reloads and
   verifies the postcondition, and fails closed on any remaining omission.
3. Add post-terminal dependency convergence to batched and non-batched success
   paths plus isolated failure paths. Add a quiescent non-terminal guard to the
   supervisor completion pump.
4. Crop the root and descendant graphs before activity projection. Bound noisy
   activity/entity collections without dropping Team/Agent/work identities or
   numeric graph usage.
5. Add focused tests for required tool exposure, default-proposal identity and
   replay, non-designated/no-market exclusion, serial future-peer eligibility,
   actual runner orphan convergence, non-terminal quiescence rejection and a
   byte-bounded 16-Agent Summary projection.
6. Run focused suites, then full Runtime/Gateway/Harness Contract/Harness Eval,
   formatting, diff, architecture, version, governance and performance gates.
   Commit only a clean deterministic candidate. Only then run one new isolated
   DeepSeek acceptance and monitor it to a real terminal.

### Audit verdict

The plan is accepted for implementation because all mutations preserve a
single durable owner, all defaults are idempotent and identity-attested, model
evidence is never synthesized, terminal failure remains fail-closed, Full
projection semantics remain unchanged, and the evaluator remains read-only
apart from its existing timeout cancellation path. The plan is rejected if an
implementation introduces a second market store, bypasses `TeamRuntime`, marks
unreviewed work accepted, weakens required work, or treats polling heartbeat as
business progress.

## Candidate-4 identity-boundary correction

The clean `fd42d606` run proved that the default proposal architecture was
correctly owned and fail-closed, but its idempotency identity serialization was
not valid for production-length graph and Agent identifiers. Concatenating two
unbounded compositional ids violates the existing 160-character
`TeamRuntime` input contract before idempotent replay can occur. This is a
framework boundary bug, not a provider-compliance or scheduling failure.

The correction has one owner and no compatibility branch:

1. `autonomous_proposal_idempotency_key(graph_id, agent_id)` hashes a
   domain-separated, length-delimited pair with SHA-256 and emits
   `autonomy:sha256:<digest>:follow-up-v1`.
2. The model-visible mutation template and governed Runtime fallback call that
   same helper; they cannot drift to different replay identities.
3. Regression coverage uses identities whose former concatenation exceeds 160
   characters, asserts the bounded/stable key, asserts changed graph or Agent
   identity changes the key, and asserts both proposal paths expose the same
   value.
4. The existing `TeamRuntime` limit remains strict. Truncation, lossy prefixes,
   random UUIDs and bypassing collaboration control are forbidden because they
   weaken collision safety, replay stability or authority.

The correction is accepted because its fixed output is below the validated
boundary, the hash input is unambiguous, all mutations still pass through
`TeamRuntime`, and it changes neither market semantics nor scheduling policy.

## Candidate-5 provider-capacity correction

The clean `a5f58fdb` run proved the identity correction in the real path: six
Agents completed, serial successors started after their predecessors, and no
proposal-key validation failed. It also exposed a provider-resource invariant
violation that deterministic saturation at the initial target could not see.

After provider failures, the adaptive manager legally contracted the global,
account and Flash model effective limits to `8`, while their interactive
reserves were respectively `8`, `8` and `16`. Non-interactive capacity is
defined as `effective_limit - interactive_reserve`, so every delegated
Foreground request became permanently inadmissible even with zero active
leases. The token pool contracted to `256` with a reserve of `256`, producing
the same zero-progress state. Its 30-second admission deadline was also below
the observed 69-second provider service p95 and 176-second maximum. This is a
Runtime policy deadlock, not provider quota, authentication or model failure.

The accepted correction preserves generic resource-manager semantics and has
three provider-owned parts:

1. Every provider quota must keep `interactive_reserve < minimum` and must
   leave enough maximum headroom for one maximum token-pressure request.
   Defaults retain four ordinary Foreground slots at adaptive minimum: global
   and fallback `12/8`, Flash `20/16`, Pro `12/8`.
2. Provider token-pressure derivation raises its adaptive minimum to the
   smaller of maximum capacity or reserve plus four maximum-pressure requests.
   It never lowers target or maximum and cannot overflow quota ordering.
3. Interactive requests retain a 30-second admission deadline because they own
   the reserve. Foreground, Background and Maintenance provider requests use a
   bounded 300-second deadline, while turn cancellation remains authoritative.

The correction is rejected if it weakens the generic interactive reserve,
disables adaptive contraction, adds evaluator-only capacity, retries committed
provider effects, or treats a longer bounded queue wait as a provider call
timeout. Regression gates must cover invalid starvation policies, all default
provider layers, four maximum-pressure requests at the degraded token floor,
and service-class admission deadlines before another paid run.

## Candidate-6 control-plane and semantic correction

The clean `bbd4945b` candidate proved that provider admission now sustains the
large run: it reached 127 DeepSeek rounds, 219 native Agent tool calls and
5,752,346 live canonical tokens. It also isolated two framework faults before
an external `402 Insufficient Balance` fenced the account.

First, role resource cropping incorrectly treated the Team control plane as a
business evidence source. An `upstream_only` verifier therefore received an
empty tool contract even though its job still required independent market
review. Business-tool declarations could erase the same controls. The repair
defines `team_board` and `collaboration_control` as invariant Runtime controls:
explicit business contracts retain them, while upstream-only roles retain only
those controls and lose source, workspace, context and evidence-reacquisition
tools. This preserves least authority without making coordination impossible.

Second, the checkpoint loop interpreted an `Ok(TurnSummary)` transport result
as permission to continue even when `terminal_completion` was `Partial` after
the provider failure. That converted one terminal external failure into repeat
rounds. Continuation now advances only after `GoalCompletion::Satisfied`; every
other typed terminal completion records `agent.autonomy.checkpoint_stopped` and
ends the bounded loop without another provider request.

The same trace falsified the former forced-challenge policy. Manufacturing a
negative review for the first proposal is semantically dishonest when the work
is correct and can deadlock a serial final reviewer because no later peer can
revise and independently accept it. Work review now accepts correct submissions
and reserves `collaboration_control(challenge)` for actual defects. Required
red-team reasoning is represented separately by a Team-board `challenge` with
`response_required=true`; graph verification rejects it until a `resolution`
explicitly names the challenged entry. The evaluator counts these epistemic
challenges and resolutions, still requires independent work reviews, and still
fails on any unresolved challenged work item.

Finally, the evaluator's bid floor was inconsistent with the protocol: assigned
scheduler work is claimed directly and only proposed market work is bid. The
minimum bid count is therefore the proposal floor, not the total claim floor.
This corrects measurement only; it does not synthesize actions or relax proposal,
claim, review, discussion, materialization or terminal-completion requirements.

The correction is rejected if business cropping restores external evidence
tools to upstream-only roles, if a Partial/Blocked/Failed turn may re-enter the
checkpoint loop, if Runtime fabricates a challenge finding, if a Team challenge
can complete without a linked resolution, or if the evaluator mutates Runtime
state to manufacture acceptance.
