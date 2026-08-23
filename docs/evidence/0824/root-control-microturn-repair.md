# 0824 root-control microturn repair

Phase: P1.1 — control-codec isolation in an ordinary conversation

## Observed failure

The isolated real Gateway evaluation of candidate `d45e3129` used the
configured Token Plan `qwen3.8-max` route. A direct, minimal named-function
probe successfully returned `submit_collaboration_decision` with two
workstreams. The full conversation scenario still stopped at
`missing_control_plane_proposal` before any Team was admitted.

The failed scenario's recorded root metrics had one tool call and zero Team
executions. This establishes that the narrow function schema alone is not
enough when a compatibility endpoint sees the full ordinary-conversation
context after capability discovery.

## Repair

At durable `ProposalOnly`, Runtime now attaches one latest private system
instruction that:

- requires exactly one `submit_collaboration_decision` call;
- derives the exact workstream count from the bound user requirement;
- forbids prose, capability rediscovery, and workspace tools for that turn;
- names no role, template, graph id, resource, permission, or business flow.

The provider named-tool constraint and non-thinking request remain in force.
This is a control-codec microturn, not a semantic fallback or a hidden Team
constructor.

## Deterministic checks

- `cargo fmt --check`
- `cargo test -p runtime capability_receipt_advances_root_control_plane_to_proposal_only --lib --no-fail-fast`
- `cargo test -p runtime only_a_successful_team_proposal_satisfies_root_control_plane_action --lib --no-fail-fast`
- `cargo check -p gateway --all-targets`

## Next evidence gate

Run the isolated real Gateway suite once against the new immutable candidate.
Do not rerun the unchanged previous candidate; its failure report is retained
under the ignored acceptance evidence directory.
