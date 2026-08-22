# P1 Program Coordinator evidence

Candidate baseline: `7d8b8078` (P0/P2 baseline); P1 changes are still in the
integration worktree until the complete version gate is run.

## Durable ownership

- `CollaborationProgram` admission control is compiled before the root graph
  is registered. The registration snapshot therefore contains the Program,
  exact physical Team obligations, frozen binding digests and static technical
  demand (predicted context, output ceiling, compiled Agent parallel slots and
  deadline).
- Coordinator owns bounded revision-fenced Program commands only. Runner
  remains the scheduler; `TeamRuntime` remains the sole Team graph/binding
  compiler and admitter. There is no Program worker, polling loop or sleep
  retry.
- A successful child admission records its exact child graph ID. A rejected
  admission records `BlockedPolicy/team_admission_rejected`; it cannot be
  presented as a partial completed Program. Terminal reconciliation accepts
  `Completed` only when every required physical Team root is completed and
  every obligation is admitted.
- Host now submits `ConversationProgramIntent`; semantic nodes, dependencies,
  completion contract and capability scopes are compiled by Coordinator. The
  old `start_selected_strategy` production symbol has no source references.
- Approval waiting is projected from durable graph state. Runtime resource
  overload is intentionally not projected as `AwaitingResource` yet: Runner
  presently exposes only an in-memory waiter, whereas P4 owns the required
  durable reservation/reconcile receipt. The static Program ledger is durable
  and no queue state is fabricated.

## Deterministic checks run

```text
cargo fmt --all -- --check
cargo check -p runtime --all-targets
git diff --check
cargo test -p runtime conversation_program_intent_compiles_exact_explicit_team_topology --lib
cargo test -p runtime collaboration_coordinator_persists_every_compiled_team_obligation_before_admission --lib
cargo test -p runtime collaboration_coordinator_records_rejected_team_admission_as_typed_program_truth --lib
cargo test -p runtime host_admission_materializes_every_explicitly_required_team_before_parent_model --lib
cargo test -p runtime team_admission_recovers_crash_after_the_first_task_link --lib
cargo test -p runtime one_agent_slot_does_not_deadlock_a_parent_child_handoff --lib
cargo test -p runtime semantic_compiler_materializes_three_teams_and_a_review_team --lib
cargo test -p runtime fanout_team_uses_runner_parallelism_without_a_team_scheduler --lib
```

All commands above passed in this worktree. P3--P6, generated Surface/API
projection and controlled real-Qwen A12 remain open and are not implied by
this record.
