# Collaboration Execution Obligation v0.9.713

## Decision

Every admitted Team strategy owns one durable
`CollaborationExecutionObligation`. `TaskUnderstanding` remains the normalized
ingress meaning; it is no longer reused as an execution-completion counter.

The contract records:

- whether authority came from an explicit request or automatic strategy;
- the minimum required Team count;
- an exact Team count for explicit cardinality;
- sorted, unique Runtime-authored focus identifiers;
- the requirement for a typed model-authored collaboration proposal.

The canonical instance is nested in `RuntimeExecutionDecision`, which is owned
by `TurnStrategyDecisionState`. The graph turn keeps a clone only for hot-path
checks. Every durable strategy lifecycle event serializes the contract, and
recovery rejects invalid serialized forms.

## Why the former design failed

Automatic strategy could select the Team candidate while
`TaskUnderstanding.required_team_count` remained zero. Root control-plane
admission, provider tool exposure and final-answer acceptance each inspected
that raw explicit-request field independently. The selected Team was therefore
advisory: a provider could skip the typed proposal and a zero-Team final gate
would accept prose.

The defect was a split source of truth, not a frontend rendering defect or a
single model's tool-call behavior.

## Lifecycle

1. Strategy admission selects a candidate under one decision id and lease.
2. Delegated leaves and forbidden-Team evaluation topology downgrade before
   obligation creation.
3. Runtime derives bounded focus partitions.
4. For a Team candidate, Runtime atomically freezes focus plans and the
   obligation, rebinds the executor cache, and persists an updated selected
   event if graph binding already occurred.
5. Root model admission uses the obligation count. Provider exposure forces
   `runtime_capabilities` and `submit_collaboration_decision` active.
6. The model authors semantic workstreams. Runtime validates proposal Team
   cardinality against the same obligation before lowering a Program.
7. Final acceptance requires completed required Program instances to satisfy
   the obligation. Delegated leaves and isolated judges explicitly consume
   zero parent Team requirements.
8. Revisions away from Team clear the obligation and persist the downgrade.

Runtime never invents Team names, roles, dependencies or objectives. Missing or
invalid proposals consume only the bounded control-plane repair budget and
terminate honestly when exhausted.

## Cardinality semantics

Explicit requests preserve exact cardinality. Automatic selection freezes a
nonzero minimum within the observed Team-slot ceiling; it may accept a larger
semantic proposal, but never a collapsed one. `team_slots` is a resource
ceiling, not a second semantic source.

Generic independently accountable units such as responsibility domains,
modules, dimensions, tracks and evidence sources now contribute to normalized
workstream width. Product-specific names remain compatibility signals, not the
only path to automatic collaboration.

## Recovery and failure behavior

Durable events include the obligation beside focus plans and the strategy
identity. Invalid obligation JSON is not admitted as authority. A legacy event
without the field may be paired with the freshly admitted/frozen decision for
the exact turn and graph; a root control-plane call without a frozen obligation
fails closed. A resource or policy downgrade clears the contract rather than
leaving a stale Team requirement.

## Evaluation claims

Live evaluation now declares either `focused` or `release-certification` claim
scope. Focused paid scenarios may pass as component evidence but never set
`release_certified`. Certification requires the complete registered core
scenario set and a passing baseline-versus-Team comparison; skipped comparison
is a certification failure.

`live_implicit_collaboration_obligation` contains no Team, Agent, role, template
or topology instruction. It requests three generic responsibility domains with
real tool evidence and accepts only a projection with at least three completed
Teams. This is the primary production-path regression for the original defect.

## Projection contract

The Web UI continues to render only canonical backend activities. It never
synthesizes expected Team nodes. The graph header now derives its Team label and
Team/Agent/Tool counts from the actual rendered graph; child execution links no
longer mislabel a non-Team tool run as a Team graph.

## Compatibility

The new serialized fields use serde defaults. Non-Team turns add no provider
round and retain the same execution path. Explicit Team behavior remains exact.
Existing focused scenario invocations remain available, with their restricted
claim scope made machine-readable.
