//! Canonical terminal custody for override-terminalized nodes.
//!
//! An intervention (policy block, provider block, Focus acceptance, goal write
//! gap, orchestration/precommitted terminal, protocol receipt, text-only
//! conclusion) short-circuits the turn by installing a
//! `terminal_override` and planning one Synthesize/InlineModel node in the same
//! graph transaction. The turn then ends on the override disclosure, so that
//! node can be started and admitted and still never publish a committed attempt.
//! `binding_callback_disposition` has exactly three answers for a binding:
//! a committed attempt, a canonical retirement, or Pending. A node with neither
//! fact stays Pending forever, which the runner reports as
//! `callback has no canonical original outcome or retirement` and every later
//! reconciliation repeats: the original call custody never becomes decidable.
//!
//! The invariant this module enforces: a node that was planned into the graph
//! must land exactly one canonical terminal fact - a committed attempt or a
//! binding retirement. This module only ever supplies the missing *retirement*,
//! using the `cancel_node` command the state store already understands. It adds
//! no state, no state machine and no new disposition branch, and it never
//! touches an identity, digest, lease or revision fence.

use harness_contract::execution_graph::{
    ExecutionGraphCommand, ExecutionNodeSpec, ExecutionNodeStatus,
};

use super::commit_service::{CallbackDisposition, ExecutionCommitService};
use super::state_store::ExecutionGraphStateStore;
use crate::runtime_event_store::RuntimeEventStore;
use crate::RuntimeServices;

/// The override node classes this seal covers. Every entry is a node id suffix
/// that `host_backend.rs` plans together with a `terminal_override`; the suffix
/// is part of the durable node id, so the class survives recovery and does not
/// need any turn-local bookkeeping.
pub(crate) const OVERRIDE_TERMINAL_NODE_SUFFIXES: [&str; 10] = [
    "policy-block-synthesize",
    "provider-block-synthesize",
    "final-answer-block-synthesize",
    "focus-acceptance-block-synthesize",
    "focus-acceptance-synthesize",
    "goal-write-gap-synthesize",
    "orchestration-terminal-synthesize",
    "precommitted-terminal-synthesize",
    "agentic-protocol-receipt-synthesize",
    "policy-text-only-conclusion",
];

/// Reason carried into the durable `cancel_node` command. It names the seal so a
/// trace can distinguish it from an original cancellation owned by a live caller.
pub(crate) const OVERRIDE_CUSTODY_SEAL_REASON: &str = "override-terminal-custody-seal";

#[must_use]
pub(crate) fn override_terminal_node_suffix(node_id: &str) -> Option<&'static str> {
    let suffix = node_id.rsplit(':').next().filter(|value| !value.is_empty())?;
    OVERRIDE_TERMINAL_NODE_SUFFIXES
        .into_iter()
        .find(|name| *name == suffix)
}

#[must_use]
pub(crate) fn is_override_terminal_node_id(node_id: &str) -> bool {
    override_terminal_node_suffix(node_id).is_some()
}

/// What the seal did. Every variant other than `Sealed` leaves the graph stream
/// byte-for-byte unchanged, so a seal can never rewrite an existing fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OverrideCustody {
    /// The canonical retirement was written by a `cancel_node` command.
    Sealed,
    /// A terminal status already exists: this binding already has its fact.
    AlreadySettled,
    /// The node was never admitted and started, so no retirement can own it.
    RefusedNotStarted,
    /// The node is planned but its status is not one the retirement reader
    /// accepts for this attempt.
    RefusedNotRetirable,
    /// The commit service refused the command (stale revision, invalid target).
    Refused(String),
    /// The graph is not readable, so nothing was claimed.
    Unavailable(String),
}

impl OverrideCustody {
    #[must_use]
    pub(crate) fn sealed(self) -> bool {
        matches!(self, Self::Sealed)
    }
}

/// The states `binding_retirement_record` accepts as the retirement's original
/// status. Anything else has no canonical retirement to write.
const RETIRABLE_STATUSES: [ExecutionNodeStatus; 6] = [
    ExecutionNodeStatus::Running,
    ExecutionNodeStatus::WaitingInput,
    ExecutionNodeStatus::WaitingApproval,
    ExecutionNodeStatus::WaitingExternal,
    ExecutionNodeStatus::Paused,
    ExecutionNodeStatus::Ready,
];

/// Land the missing canonical terminal fact for one override node: a binding
/// retirement produced by the existing `cancel_node` command.
///
/// Prerequisites, all read from the durable graph rather than from turn state:
/// the node exists; it is not already terminal; its current attempt was
/// admitted and started (`binding_start_ticket` resolves for the exact binding
/// the state store derives from that attempt). A node that was never started
/// cannot own a retirement and is refused, because planning a node the turn
/// will not run is itself the defect this seal must not paper over.
pub(crate) fn seal_override_node_terminal(
    store: &ExecutionGraphStateStore,
    commits: &ExecutionCommitService,
    graph_id: &str,
    node: &ExecutionNodeSpec,
    disclosure: &str,
) -> OverrideCustody {
    let Ok(graph) = store.load(graph_id) else {
        return OverrideCustody::Unavailable("override seal graph unreadable".into());
    };
    let Some(status) = graph.node_statuses.get(&node.id).copied() else {
        return OverrideCustody::RefusedNotStarted;
    };
    if status.is_terminal() {
        return OverrideCustody::AlreadySettled;
    }
    if !RETIRABLE_STATUSES.contains(&status) {
        return OverrideCustody::RefusedNotRetirable;
    }
    let Some(attempt) = graph.recovery_cursor.node_attempts.get(&node.id).copied() else {
        return OverrideCustody::RefusedNotStarted;
    };
    let key = format!("{OVERRIDE_CUSTODY_SEAL_REASON}:{attempt}:{}", node.id);
    let reason = if disclosure.trim().is_empty() {
        key
    } else {
        format!("{key}:{}", disclosure.trim())
    };
    let custody = seal_started_override_binding(store, commits, &graph, &node.id, attempt, &reason);
    match &custody {
        OverrideCustody::Sealed => tracing::warn!(
            target: "cowd::override_custody",
            graph_id,
            node_id = %node.id,
            attempt,
            reason = %reason,
            "an override node that landed no committed attempt now owns a canonical retirement"
        ),
        OverrideCustody::Refused(text) | OverrideCustody::Unavailable(text) => {
            tracing::warn!(
                target: "cowd::override_custody",
                graph_id,
                node_id = %node.id,
                attempt,
                outcome = %text,
                "override custody seal did not write a retirement"
            );
        }
        _ => {}
    }
    custody
}

fn seal_started_override_binding(
    store: &ExecutionGraphStateStore,
    commits: &ExecutionCommitService,
    graph: &harness_contract::execution_graph::ExecutionGraph,
    node_id: &str,
    attempt: u32,
    reason: &str,
) -> OverrideCustody {
    let Ok(binding) = store.current_binding(graph, node_id, attempt) else {
        return OverrideCustody::RefusedNotStarted;
    };
    // The start ticket is the identity fence: it re-derives the exact original
    // admission for this binding and fails on a foreign binding, a changed
    // digest or a stale attempt. A node that was never started is refused here.
    if store.binding_start_ticket(graph, node_id, &binding).is_err() {
        return OverrideCustody::RefusedNotStarted;
    }
    match store.binding_retirement_record(graph, node_id, &binding) {
        Ok(None) => {}
        Ok(Some(_)) => return OverrideCustody::AlreadySettled,
        // A stream the canonical reader rejects (forged actor, wrong schema,
        // legacy writer) is never papered over: the seal writes nothing.
        Err(_) => {
            return OverrideCustody::Unavailable(
                "override seal refused a non-canonical retirement stream".into(),
            )
        }
    }
    match commits.apply_command(
        graph,
        &ExecutionGraphCommand::CancelNode {
            expected_revision: graph.revision,
            node_id: node_id.to_string(),
            reason: reason.to_string(),
        },
    ) {
        Ok(_) => OverrideCustody::Sealed,
        Err(error) => OverrideCustody::Refused(error.to_string()),
    }
}

/// True once a durable Session terminal exists for this exact execution. Then
/// the turn is already terminalized, so any override node that was started and
/// still owns no canonical fact can never publish one: the terminal it would
/// declare is already declared. Without this fact the override node is the
/// legitimate terminal publisher and must not be touched.
fn turn_is_terminalized(
    services: &RuntimeServices,
    session_id: &str,
    turn_id: &str,
    graph_id: &str,
) -> bool {
    if session_id.is_empty() || turn_id.is_empty() || graph_id.is_empty() {
        return false;
    }
    services
        .event_store()
        .session_terminal_for_execution(session_id, graph_id, turn_id)
        .unwrap_or(None)
        .is_some()
}

/// Guarded per-node seal used by the override application sites: the caller
/// names the exact node it is terminalizing, and the seal runs only once a
/// durable Session terminal proves that this node can never publish its own
/// canonical fact. Returns `None` when the guard says the node still owns the
/// terminal presentation, which is the normal path.
pub(crate) fn seal_override_node_terminal_when_terminalized(
    services: &RuntimeServices,
    session_id: &str,
    turn_id: &str,
    graph_id: &str,
    node: &ExecutionNodeSpec,
    disclosure: &str,
) -> Option<OverrideCustody> {
    if !turn_is_terminalized(services, session_id, turn_id, graph_id) {
        return None;
    }
    Some(seal_override_node_terminal(
        services.graph_state_store(),
        services.commit_service(),
        graph_id,
        node,
        disclosure,
    ))
}

/// Seal every started-but-factless override node in one execution, but only when
/// the turn is already durably terminalized. Returns one entry per candidate
/// node so a caller can count what the seal actually wrote.
pub(crate) fn seal_superseded_override_nodes(
    services: &RuntimeServices,
    session_id: &str,
    turn_id: &str,
    graph_id: &str,
) -> Vec<(String, OverrideCustody)> {
    if !turn_is_terminalized(services, session_id, turn_id, graph_id) {
        return Vec::new();
    }
    let store = services.graph_state_store();
    let Ok(graph) = store.load(graph_id) else {
        return Vec::new();
    };
    graph
        .nodes
        .iter()
        .filter(|node| is_override_terminal_node_id(&node.id))
        .filter(|node| {
            graph
                .node_statuses
                .get(&node.id)
                .is_some_and(|status| !status.is_terminal())
        })
        .map(|node| {
            let custody = seal_override_node_terminal(
                store,
                services.commit_service(),
                graph_id,
                node,
                "override node superseded by the durable Session terminal",
            );
            (node.id.clone(), custody)
        })
        .collect()
}

/// Seal one node addressed by id inside an already loaded graph. The callback
/// criterion uses this at the exact point where a binding would otherwise be
/// reported as unknown custody: only the override family is ever addressed, and
/// a node outside it returns `None` without touching the commit path at all.
#[must_use]
pub(crate) fn seal_override_custody_for_graph(
    store: &ExecutionGraphStateStore,
    commits: &ExecutionCommitService,
    graph: &harness_contract::execution_graph::ExecutionGraph,
    node_id: &str,
    disclosure: &str,
) -> Option<OverrideCustody> {
    if !is_override_terminal_node_id(node_id) {
        return None;
    }
    let node = graph.nodes.iter().find(|candidate| candidate.id == node_id)?;
    Some(seal_override_node_terminal(
        store, commits, &graph.id, node, disclosure,
    ))
}

/// Silence bound for the 034-5(a) supplementary trigger surface.
///
/// The streak guard counts custody-naming receipts, so an admission that
/// interrupts reconciliation keeps resetting the streak and a genuinely wedged
/// turn is never judged (taxo certv3: 63.6 min silent, 0 custody markers).
/// Silence is measured from two durable facts instead - the newest durable
/// event of this execution and the override bindings that still own no
/// canonical terminal fact - so an unrelated admission cannot move the bound.
pub(crate) const OVERRIDE_CUSTODY_SILENCE_RELEASE_MS: u64 = 900_000;

/// The two durable measurements behind the silence trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OverrideCustodySilence {
    /// Milliseconds since the newest durable event of the execution stream.
    pub idle_ms: u64,
    /// Override bindings that were started and still own neither a committed
    /// attempt nor a retirement: the only nodes whose custody can be unknown.
    pub unfinalized_node_ids: Vec<String>,
}

impl OverrideCustodySilence {
    #[must_use]
    pub(crate) fn is_stall(&self, threshold_ms: u64) -> bool {
        override_custody_silence_is_stalled(self.idle_ms, self.unfinalized_node_ids.len(), threshold_ms)
    }
}

/// The bound itself: pure arithmetic over durable counts, no clock, no state
/// and no new gate. It is `const` so a caller cannot invent a looser rule.
#[must_use]
pub(crate) const fn override_custody_silence_is_stalled(
    idle_ms: u64,
    unfinalized_override_nodes: usize,
    threshold_ms: u64,
) -> bool {
    unfinalized_override_nodes > 0 && idle_ms >= threshold_ms
}

/// Newest durable event timestamp of one execution stream, read through the
/// existing paged reader. `None` means the stream is empty or unreadable, which
/// is never treated as silence: no facts, no release.
#[must_use]
pub(crate) fn graph_last_durable_ms(
    events: &RuntimeEventStore,
    graph_id: &str,
    revision: u64,
) -> Option<u64> {
    if revision == 0 {
        return None;
    }
    events
        .list_stream_after(graph_id, revision - 1, revision, 1, 1024 * 1024)
        .ok()?
        .last()
        .map(|record| record.created_at_ms)
}

/// Measure how long an execution has produced no durable event and which
/// override bindings are still factless. Nodes that were never admitted are
/// excluded: they own no binding, so they cannot hold custody unknown.
#[must_use]
pub(crate) fn override_custody_silence(
    store: &ExecutionGraphStateStore,
    commits: &ExecutionCommitService,
    events: &RuntimeEventStore,
    graph_id: &str,
    now_ms: u64,
) -> Option<OverrideCustodySilence> {
    let graph = store.load(graph_id).ok()?;
    let last_ms = graph_last_durable_ms(events, graph_id, graph.revision)?;
    let unfinalized_node_ids = graph
        .nodes
        .iter()
        .filter(|node| is_override_terminal_node_id(&node.id))
        .filter(|node| {
            !graph
                .node_statuses
                .get(&node.id)
                .is_some_and(|status| status.is_terminal())
        })
        .filter(|node| {
            let Some(attempt) = graph.recovery_cursor.node_attempts.get(&node.id).copied() else {
                return false;
            };
            let Ok(binding) = store.current_binding(&graph, &node.id, attempt) else {
                return false;
            };
            commits
                .binding_callback_disposition(&graph, &node.id, &binding)
                .is_ok_and(|decision| decision == CallbackDisposition::Pending)
        })
        .map(|node| node.id.clone())
        .collect();
    Some(OverrideCustodySilence {
        idle_ms: now_ms.saturating_sub(last_ms),
        unfinalized_node_ids,
    })
}

/// Silence-type release: an override binding that has held custody unknown for
/// the whole bound gets the one canonical fact it still owes, written through
/// the existing `cancel_node` retirement path. Unlike the terminalized seal this
/// does not require a durable Session terminal - the missing terminal *is* the
/// stall - but it keeps every fence: an unstarted or foreign binding is refused
/// and a node that already owns a fact is left byte-for-byte unchanged.
pub(crate) fn release_silent_override_custody(
    store: &ExecutionGraphStateStore,
    commits: &ExecutionCommitService,
    events: &RuntimeEventStore,
    graph_id: &str,
    now_ms: u64,
    threshold_ms: u64,
) -> Vec<(String, OverrideCustody)> {
    let Some(silence) = override_custody_silence(store, commits, events, graph_id, now_ms) else {
        return Vec::new();
    };
    if !silence.is_stall(threshold_ms) {
        return Vec::new();
    }
    let Ok(graph) = store.load(graph_id) else {
        return Vec::new();
    };
    silence
        .unfinalized_node_ids
        .iter()
        .filter_map(|node_id| {
            let node = graph.nodes.iter().find(|candidate| &candidate.id == node_id)?;
            let custody = seal_override_node_terminal(
                store,
                commits,
                graph_id,
                node,
                &format!("silent-custody-release:{}ms", silence.idle_ms),
            );
            tracing::warn!(
                target: "cowd::override_custody",
                graph_id,
                node_id = %node_id,
                idle_ms = silence.idle_ms,
                threshold_ms,
                outcome = ?custody,
                "the silence-type 034-5(a) trigger surface released an override custody stall"
            );
            Some((node_id.clone(), custody))
        })
        .collect()
}

/// Wall clock for the production callers. Tests always pass an explicit value,
/// so no test depends on this function and none of them can drift apart.
#[must_use]
pub(crate) fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn override_node(graph_id: &str, iteration: usize, suffix: &str) -> ExecutionNodeSpec {
        let mut node = ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::Synthesize,
            "synthesize",
            format!("team:{graph_id}"),
        );
        node.id = format!("{graph_id}:{iteration}:{suffix}");
        node.idempotency_key = format!("{}:attempt", node.id);
        node
    }

    #[test]
    fn seal_covers_every_override_terminal_node_class() {
        assert_eq!(OVERRIDE_TERMINAL_NODE_SUFFIXES.len(), 10);
        for (iteration, suffix) in OVERRIDE_TERMINAL_NODE_SUFFIXES.into_iter().enumerate() {
            let node = override_node("graph-override", iteration, suffix);
            assert_eq!(override_terminal_node_suffix(&node.id), Some(suffix));
            assert!(is_override_terminal_node_id(&node.id));
        }
    }

    #[test]
    fn seal_does_not_claim_nodes_outside_the_override_family() {
        for suffix in [
            "model",
            "inline_model",
            "tool_batch",
            "answer-wait-program",
            "provider-protocol-clean-synthesis-model",
            "input-cursor-replan-model",
            "terminal-source-replan",
            "",
        ] {
            let node = override_node("graph-override", 9, suffix);
            assert!(
                !is_override_terminal_node_id(&node.id),
                "unplanned suffix `{suffix}` must never be sealed as an override node"
            );
        }
    }

    #[tokio::test]
    async fn seal_refuses_a_started_override_node_without_a_canonical_start() {
        let services = RuntimeServices::in_memory().unwrap();
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new("override custody");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let graph_id = graph.id.clone();
        let node = override_node(&graph_id, 3, "policy-block-synthesize");
        graph.nodes.push(node.clone());
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Running);
        graph
            .recovery_cursor
            .node_attempts
            .insert(node.id.clone(), 1);
        services
            .commit_service()
            .register_graph(graph)
            .expect("register override graph");
        let loaded = services
            .graph_state_store()
            .load(&graph_id)
            .expect("load the registered execution graph");
        let custody = seal_override_node_terminal(
            services.graph_state_store(),
            services.commit_service(),
            &graph_id,
            &node,
            "no canonical original start",
        );
        // `register_graph` normalizes a planned node to its own status, so a
        // node that never bought a start ticket is refused as not-started or as
        // not-retirable. Either way the seal writes nothing: the original
        // fail-closed custody report below is untouched.
        assert!(
            matches!(
                custody,
                OverrideCustody::RefusedNotStarted | OverrideCustody::RefusedNotRetirable
            ),
            "a node without a canonical start must be refused, custody={custody:?} status={:?}",
            loaded.node_statuses.get(&node.id).copied(),
        );
        let after = services
            .graph_state_store()
            .load(&graph_id)
            .expect("reload the execution graph");
        assert_eq!(
            after.revision, loaded.revision,
            "a refused seal must not advance the graph"
        );
    }

    #[tokio::test]
    async fn seal_does_not_run_without_a_durable_session_terminal() {
        let services = RuntimeServices::in_memory().unwrap();
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new("override guard");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let graph_id = graph.id.clone();
        let node = override_node(&graph_id, 4, "provider-block-synthesize");
        graph.nodes.push(node.clone());
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Running);
        graph
            .recovery_cursor
            .node_attempts
            .insert(node.id.clone(), 1);
        services
            .commit_service()
            .register_graph(graph)
            .expect("register override graph");
        let sealed = seal_superseded_override_nodes(&services, "session", "turn", &graph_id);
        assert!(
            sealed.is_empty(),
            "a node that still owns the terminal presentation must never be retired"
        );
        assert!(seal_superseded_override_nodes(&services, "", "", &graph_id).is_empty());
    }

    #[test]
    fn silence_bound_is_pure_arithmetic_and_never_loosens() {
        // A stall needs both facts: an unfinalized override binding and a
        // durable silence at least as long as the bound.
        assert!(override_custody_silence_is_stalled(
            OVERRIDE_CUSTODY_SILENCE_RELEASE_MS,
            1,
            OVERRIDE_CUSTODY_SILENCE_RELEASE_MS
        ));
        assert!(!override_custody_silence_is_stalled(
            OVERRIDE_CUSTODY_SILENCE_RELEASE_MS - 1,
            1,
            OVERRIDE_CUSTODY_SILENCE_RELEASE_MS
        ));
        // The 63.6 minute taxo silence with an interrupted streak: the bound
        // does not care about receipts, so it still fires.
        assert!(override_custody_silence_is_stalled(3_816_000, 1, OVERRIDE_CUSTODY_SILENCE_RELEASE_MS));
        // No unfinalized binding, no release - a busy turn is never judged from
        // silence alone, and neither is an empty or unreadable stream.
        for idle in [0, 60_000, OVERRIDE_CUSTODY_SILENCE_RELEASE_MS, u64::MAX] {
            assert!(!override_custody_silence_is_stalled(idle, 0, OVERRIDE_CUSTODY_SILENCE_RELEASE_MS));
        }
        assert!(!OverrideCustodySilence { idle_ms: 900_000, unfinalized_node_ids: Vec::new() }.is_stall(OVERRIDE_CUSTODY_SILENCE_RELEASE_MS));
        assert!(OverrideCustodySilence {
            idle_ms: 900_000,
            unfinalized_node_ids: vec!["graph:0:policy-block-synthesize".into()],
        }
        .is_stall(OVERRIDE_CUSTODY_SILENCE_RELEASE_MS));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn silence_measurement_never_invents_a_stream_and_is_parallel_safe() {
        // Every task owns its services and its own graph id, so nothing is
        // shared and the tasks cannot disturb each other when the suite runs
        // them concurrently.
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..8 {
            tasks.spawn(async move {
                let services = RuntimeServices::in_memory().expect("services");
                let graph_id = format!("override-silence-{index}");
                let mut graph = harness_contract::execution_graph::ExecutionGraph::new(&graph_id);
                graph.id = graph_id.clone();
                crate::test_support::attach_execution_graph_lineage(&mut graph);
                let node = override_node(&graph_id, index, "policy-block-synthesize");
                graph.nodes.push(node);
                services
                    .commit_service()
                    .register_graph(graph)
                    .expect("register graph");
                let store = services.graph_state_store();
                let commits = services.commit_service();
                let events = commits.event_store();
                // An admitted-but-never-started node owns no binding, so the
                // silence reader must not claim it.
                let silence = override_custody_silence(store, commits, events, &graph_id, 900_000)
                    .expect("a registered graph has a durable head");
                assert!(silence.unfinalized_node_ids.is_empty());
                assert!(!silence.is_stall(OVERRIDE_CUSTODY_SILENCE_RELEASE_MS));
                assert!(release_silent_override_custody(
                    store, commits, events, &graph_id, u64::MAX, OVERRIDE_CUSTODY_SILENCE_RELEASE_MS,
                )
                .is_empty());
                // A stream that does not exist yields no measurement, never a
                // release.
                assert!(override_custody_silence(store, commits, events, "unreadable-graph", 900_000)
                    .is_none_or(|value| value.unfinalized_node_ids.is_empty()));
                graph_id
            });
        }
        let mut done = 0;
        while let Some(joined) = tasks.join_next().await {
            assert!(joined.is_ok(), "parallel override custody tasks must not disturb each other");
            done += 1;
        }
        assert_eq!(done, 8);
    }

    #[test]
    fn seal_reason_is_stable_and_names_the_source() {
        assert!(OVERRIDE_CUSTODY_SEAL_REASON.starts_with("override-terminal"));
        for suffix in OVERRIDE_TERMINAL_NODE_SUFFIXES {
            let node = override_node("graph-override", 1, suffix);
            assert_eq!(override_terminal_node_suffix(&node.id), Some(suffix));
        }
    }
}
