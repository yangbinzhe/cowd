#!/usr/bin/env python3
"""Deep ACTIVE run monitor: detect, investigate, and conclude.

Passive monitoring reports state. This monitor treats every suspicious
signature as a case to investigate: it drills down to the looping task, the
repeated inputs, the rework reasons and the per-actor step distribution, then
emits a root-cause finding the caller can act on (stop the run, preserve
evidence) instead of a bare status line.

Usage: deep-run-monitor.py <postgres-url> [--investigate]
"""
import json
import subprocess
import sys
import os
import re
import time
from collections import Counter
from difflib import SequenceMatcher


def psql(url, sql):
    out = subprocess.run(
        ["psql", url, "-t", "-A", "-F\u0001", "-c", sql],
        capture_output=True, text=True, env=dict(os.environ),
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip())
    return [line.split("\u0001") for line in out.stdout.splitlines() if line.strip()]


def finding_fingerprints(conclusion):
    """Stable identity of a report's findings: the root causes, nothing else.

    The evidence list is the proof, not the identity, and it moves while the
    loop is real. A live run (2026-10-12, live_one_team_execution) reset its own
    escalation twice because a second and then a third repeated input crossed
    the reporting threshold and joined the evidence roster: the finding was the
    same finding with more examples, but the fingerprint grew with the roster
    and the counter restarted. The same instability sits in the
    `objective never requested` finding, whose evidence quotes the recent
    actions and therefore changes whenever any action is applied.

    The root cause is what the finding asserts; the evidence merely shows it.
    Two reports carry the same finding when they assert the same things, and a
    loop that accumulates more examples is still the same loop - a worsening
    loop must not look like a new one.
    """
    fingerprints = []
    for finding in conclusion.get("findings", []):
        if isinstance(finding, str):
            # The terminal conclusion reports a plain sentence ("verdict
            # reached"), not a structured finding. It is its own fingerprint;
            # the caller short-circuits on TERMINAL before it matters.
            fingerprints.append(finding)
            continue
        fingerprints.append(str(finding.get("root_cause", "")))
    return sorted(fingerprints)


def findings_text(conclusion):
    """The findings as one comparable string, for the similarity evidence."""
    return json.dumps(conclusion.get("findings", []), sort_keys=True)


def finding_counts(conclusion):
    """Each finding's evidence counts, keyed the same way as the fingerprint.

    The count is the loop's pulse: it rises while the model keeps issuing the
    call and freezes when the behavior stops. A finding whose count has not
    moved since the last report is history, not a live loop - run one31's
    counts rose every interval (x10 -> x11 -> x13), while a run whose agent
    stopped re-reading keeps reporting the same x8 forever.
    """
    counts = {}
    for finding in conclusion.get("findings", []):
        if isinstance(finding, str):
            counts[finding] = 0
            continue
        total = 0
        for evidence in finding.get("evidence", []):
            match = re.search(r"x(\d+)\s*$", str(evidence).strip())
            if match:
                total += int(match.group(1))
            else:
                digits = re.findall(r"\b\d+\b", str(evidence))
                total += int(digits[0]) if digits else 0
        counts[str(finding.get("root_cause", ""))] = total
    return counts


def converged(previous_milestones, milestones):
    """Did the Objective actually move toward closure since the last report?

    Only a completion request or a verdict counts. Reviews and reworks are
    work-phase churn that a spinning run also produces: run one31's reviewers
    reviewed the same looping output three times (objective_reviews 2 -> 3 -> 4)
    while the loop ran, so treating a review as convergence would renew the
    loop's lease every interval.
    """
    if not previous_milestones:
        return False
    for key in ("completion_requests", "verdicts"):
        if int(milestones.get(key, 0) or 0) > int(previous_milestones.get(key, 0) or 0):
            return True
    return False


def verify(previous, conclusion, milestones, threshold=None):
    """Verify this report against the previous one, and say when to stop waiting.

    A finding that survives into the next report with its counts only growing
    is not a new observation, it is the same observation confirmed. This
    function counts those confirmations; `escalate` turns true once the same
    finding has been reported `threshold` times in a row with no convergence
    milestone in between. That is the signal the harness acts on: the monitor
    has now told the operator the same thing three times and nothing has
    changed, which is itself the proof that the run is stuck.

    Identity is the set of root causes. The evidence roster is excluded
    because it grows while a loop worsens, and a loop that accumulates more
    examples is still the same loop.

    The similarity between consecutive reports is computed and reported, but it
    is not the detector. Run one30's seven real conclusions measured 0.34
    average pairwise similarity while the loop was real; fingerprint equality
    sees what a similarity threshold cannot.
    """
    if threshold is None:
        threshold = int(os.environ.get("COWD_MONITOR_ESCALATE_AFTER", "3"))
    state = conclusion.get("state", "")
    if state == "TERMINAL":
        return {
            "repeat_count": 0,
            "threshold": threshold,
            "escalate": False,
            "similarity_to_previous": None,
            "fingerprint": [],
            "finding_counts": {},
            "counts_grew": False,
            "reason": "terminal",
        }
    fingerprints = finding_fingerprints(conclusion)
    if not fingerprints:
        return {
            "repeat_count": 0,
            "threshold": threshold,
            "escalate": False,
            "similarity_to_previous": None,
            "fingerprint": fingerprints,
            "finding_counts": {},
            "counts_grew": False,
            "reason": "no finding to repeat",
        }
    previous = previous or {}
    previous_fingerprint = previous.get("fingerprint") or []
    previous_text = previous.get("findings_text") or ""
    previous_counts = previous.get("finding_counts") or {}
    current_text = findings_text(conclusion)
    counts = finding_counts(conclusion)
    grew = any(
        count > int(previous_counts.get(cause, 0) or 0)
        for cause, count in counts.items()
    )
    if previous_fingerprint == fingerprints and not converged(
        previous.get("milestones") or {}, milestones
    ):
        # A repeat counts only when the loop is still running: the same finding
        # with the same counts is history. A live loop's counts rise every
        # interval (one31: x10 -> x11 -> x13); a finding frozen at x8 describes
        # waste that already happened, and stopping the run now recovers none
        # of it while killing work that has moved on.
        repeat_count = int(previous.get("repeat_count", 0) or 0) + 1 if grew else 1
    else:
        repeat_count = 1
    similarity = (
        round(SequenceMatcher(None, previous_text, current_text).ratio(), 3)
        if previous_text
        else None
    )
    return {
        "repeat_count": repeat_count,
        "threshold": threshold,
        "escalate": repeat_count >= threshold and state == "SPINNING",
        "similarity_to_previous": similarity,
        "fingerprint": fingerprints,
        "finding_counts": counts,
        "counts_grew": grew,
        "reason": (
            "the same finding, %d consecutive reports, no convergence milestone"
            % repeat_count
            if repeat_count > 1
            else "first report of this finding"
        ),
    }


MILESTONE_KEYS = (
    "completion_requests",
    "verdicts",
    "reopens",
    "objective_reviews",
    "reworks",
)


def load_state(path):
    """Read the previous report's verification state, or None.

    A missing, unreadable or corrupt state file is a first observation, never a
    crash: a monitor that dies on unexpected data is worse than no monitor (the
    multi-line payload incident kept the monitor dark for a 35-minute run).
    """
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            state = json.load(handle)
    except (OSError, ValueError):
        return None
    return state if isinstance(state, dict) else None


def save_state(path, state):
    """Persist the verification state atomically. Returns True when durable.

    Written to a sibling temp file and renamed, so a monitor killed mid-write
    leaves either the old state or the new one, never half of one. A failure to
    persist is reported, not swallowed into silence: without state the
    escalation cannot count, and an escalation that silently cannot happen is
    the failure this whole mechanism exists to remove.
    """
    if not path:
        return None
    tmp = "%s.tmp.%d" % (path, os.getpid())
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(state, handle)
        os.replace(tmp, path)
        return True
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return False


def longest_idle_answer_run(events, threshold=None):
    """Longest run of TEXT answers with no action between them.

    `events` is a list of (kind, payload_kind, status) in time order. Only a
    `text` item extends the run; anything that IS an action resets it: a
    completed tool invocation, an applied agentic action, a started invocation,
    an execution effect - or the model emitting a tool_call item, which is the
    model acting regardless of what the tool then did.

    The first spelling counted every `model.item_completed` as an answer and
    reset only on `tool.invocation.completed`. Measured against a live
    multi-agent run that was working, it reported a run of 8 whose composition
    was ['tool_call','tool_call','tool_call','public_reasoning','tool_call',
    'public_reasoning','tool_call','public_reasoning'] - zero text answers,
    because agentic actions (task_submit, artifact_commit) apply without a
    tool invocation event and reasoning items are not conclusions. It would
    have stopped a healthy run. The one30 signal it was built for is real:
    that root wrote final TEXT answers, seven of them, with one tool call in
    the whole window.
    """
    if threshold is None:
        threshold = int(os.environ.get("COWD_MONITOR_IDLE_ANSWERS", "3"))
    best = 0
    run = 0
    for kind, payload_kind, status in events:
        if kind == "model.item_completed" and payload_kind == "text":
            run += 1
            best = max(best, run)
        elif kind == "model.item_completed" and payload_kind == "tool_call":
            run = 0
        elif kind in (
            "tool.invocation.completed",
            "tool.invocation.started",
            "agentic.action_applied",
            "execution.effect.intent",
        ):
            run = 0
    return {"run": best, "threshold": threshold}


def similar_answer_pairs(texts, threshold=None):
    """Consecutive answer pairs above a lexical-similarity threshold.

    Reported as evidence, not used as the detector: measured against a real
    reworded loop this stays low while the loop is real.
    """
    if threshold is None:
        threshold = float(os.environ.get("COWD_MONITOR_SIMILARITY", "0.9"))
    if len(texts) < 2:
        return []
    ordered = list(reversed(texts))
    return [
        round(SequenceMatcher(None, ordered[index - 1], ordered[index]).ratio(), 3)
        for index in range(1, len(ordered))
        if SequenceMatcher(None, ordered[index - 1], ordered[index]).ratio() >= threshold
    ]


def repeated_tool_call_ratio_bp(tool_calls, distinct_inputs):
    """Share of this run's tool calls that carried no new information, in basis points.

    Pure so it can be checked without a database. An earlier version broke the
    assignment across two lines, which Python reads as `ratio_bp = 10_000` followed
    by a discarded negation - the metric was pinned at 100% forever. A metric that
    is wrong in a stable direction is worse than a missing one, because it looks
    like a finding.
    """
    if tool_calls <= 0:
        return 0
    repeated = max(0, tool_calls - distinct_inputs)
    return min(10_000, 10_000 * repeated // tool_calls)


def pairs(rows):
    """Yield only the rows that really are (value, count) pairs.

    A value containing a newline arrives as several physical lines, so the
    arity of a parsed row is not guaranteed. Skipping the malformed ones keeps
    one unexpected payload from taking the monitor down; the caller still sees
    every well-formed row.
    """
    for row in rows:
        if len(row) == 2 and row[1].strip().lstrip("-").isdigit():
            yield row[0], int(row[1])


def pairs3(rows):
    """Yield only the rows that really are (actor, value, count) triples.

    The actor-scoped repeat queries select three columns; a value containing a
    newline still arrives as several physical lines, so the arity of a parsed
    row is not guaranteed. Skipping the malformed ones keeps one unexpected
    payload from taking the monitor down.
    """
    for row in rows:
        if len(row) == 3 and row[2].strip().lstrip("-").isdigit():
            yield row[0], row[1], int(row[2])


class Monitor:
    def __init__(self, url):
        self.url = url

    def q(self, sql):
        return psql(self.url, sql)

    def scalar(self, sql):
        rows = self.q(sql)
        return int(rows[0][0]) if rows and rows[0][0] else 0

    def snapshot(self):
        s = {}
        s["model_steps"] = self.scalar("SELECT count(*) FROM runtime_events WHERE kind='model.item_completed'")
        # Repeat ratio: how much of the tool work carried no new information. It is
        # a first-class projection rather than a post-mortem statistic because the
        # waste is invisible while it happens - run one20 finished with 458 tool
        # calls of which 179 were distinct, and nothing in the run said so.
        s["tool_calls_distinct_inputs"] = self.scalar(
            "SELECT count(DISTINCT payload->>'content') FROM runtime_events "
            "WHERE kind='model.item_completed' AND payload->>'kind'='tool_call'"
        )
        s["tool_calls"] = self.scalar("SELECT count(*) FROM runtime_events WHERE kind='tool.invocation.started'")
        s["tool_failures"] = self.scalar("SELECT count(*) FROM runtime_events WHERE kind='tool.invocation.failed'")
        s["completion_requests"] = self.scalar(
            "SELECT count(*) FROM runtime_events WHERE kind='agentic.action_applied' "
            "AND payload->'envelope'->'action'->>'kind'='objective_complete_request'")
        s["reopens"] = self.scalar("SELECT count(*) FROM runtime_events WHERE kind='agentic.completion_reopened'")
        s["objective_reviews"] = self.scalar(
            "SELECT count(*) FROM runtime_events WHERE kind='agentic.action_applied' "
            "AND payload->'envelope'->'action'->>'kind'='objective_review'")
        s["verdicts"] = self.scalar("SELECT count(*) FROM runtime_events WHERE kind LIKE '%verdict%'")
        s["reworks"] = self.scalar(
            "SELECT count(*) FROM runtime_events WHERE kind='agentic.action_applied' "
            "AND payload->'envelope'->'action'->'input'->>'decision'='rework'")
        s["task_reviews"] = self.scalar(
            "SELECT count(*) FROM runtime_events WHERE kind='agentic.action_applied' "
            "AND payload->'envelope'->'action'->>'kind'='task_review'")
        s["task_submits"] = self.scalar(
            "SELECT count(*) FROM runtime_events WHERE kind='agentic.action_applied' "
            "AND payload->'envelope'->'action'->>'kind'='task_submit'")
        elapsed = self.q("SELECT (max(created_at_ms)-min(created_at_ms))/1000 FROM runtime_events")
        s["elapsed_s"] = int(elapsed[0][0]) if elapsed and elapsed[0][0] else 0
        # Silence is wall-clock now minus the last durable event: a stalled run
        # stops emitting, so its event span freezes while the clock does not.
        import time
        # Only substantive events count: background projection writes
        # (evolution signals, case indexes) keep flowing while the work is
        # stalled, which would mask the silence.
        last = self.q(
            "SELECT max(created_at_ms) FROM runtime_events "
            "WHERE kind IN ('model.item_completed','agentic.action_applied',"
            "'tool.invocation.started','execution.effect.intent')"
        )
        now_ms = int(time.time() * 1000)
        s["last_event_age_s"] = (
            (now_ms - int(last[0][0])) // 1000 if last and last[0][0] else 0
        )
        return s

    def investigate(self, snap):
        """Active drill-down: answer WHY the run looks stuck, with evidence."""
        inv = {}
        # 1. Loop center: which task absorbs the most claims/submits/reviews?
        rows = self.q(
            "SELECT payload->'envelope'->'action'->'input'->>'task_ref' AS t, count(*) "
            "FROM runtime_events WHERE kind='agentic.action_applied' "
            "AND payload->'envelope'->'action'->>'kind' IN ('task_claim','task_submit','task_review') "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 3")
        inv["task_churn"] = [{"task": t, "events": int(n)} for t, n in rows if t]

        # 2. Repeated tool inputs (loop signature) with exact shapes.
        # Grouped by actor AND input: one actor re-issuing the same call is a
        # loop, while N agents each issuing it once is N first reads - the
        # receipt-reuse contract is per-agent by design. Summing across actors
        # turns independent information gathering into a finding.
        rows = self.q(
            "SELECT payload->>'_runtime_activity_binding', substr(payload->>'content',1,90), "
            "count(*) FROM runtime_events "
            "WHERE kind='model.item_completed' AND payload->>'kind'='tool_call' "
            "GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 6")
        inv["repeated_tool_inputs"] = [
            {"input": t, "count": n} for _, t, n in pairs3(rows) if n >= 3]
        # 3. Redundant full reads of the same path.
        # Same actor scoping as above: three agents opening one file once each
        # is three first reads, not a redundant-read loop.
        rows = self.q(
            "SELECT payload->>'_runtime_activity_binding', payload->>'content', count(*) "
            "FROM runtime_events "
            "WHERE kind='model.item_completed' AND payload->>'kind'='tool_call' "
            "AND payload->>'tool_name' IN ('read_file','read_ref') "
            "GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 4")
        inv["repeated_reads"] = [
            {"call": t[:90], "count": n} for _, t, n in pairs3(rows) if n >= 3]

        # 3b. Repeated identical final-answer text. A model that has finished the
        # work but does not know the next rung writes the same summary over and
        # over; events keep flowing, so the silence signal never fires and the
        # run burns its whole window. One live run emitted the same answer seven
        # times over fourteen minutes while the monitor reported WORKING.
        # `chr(10)`/`chr(13)` are replaced because a text payload is multi-line
        # markdown: psql emits the newlines verbatim and the row parser then sees
        # one physical line per output line, so the unpack fails and the whole
        # monitor dies. It died exactly this way on a live run - every iteration
        # raised, the shell loop treated the empty report as "no data" and
        # continued silently, and the monitor went dark for the entire 35-minute
        # run while the run itself was working. A monitor that crashes on
        # unexpected data is worse than no monitor.
        # Actor-scoped like the tool queries: two agents independently writing
        # a similar summary is not one model restating itself.
        rows = self.q(
            "SELECT payload->>'_runtime_activity_binding', "
            "substr(replace(replace(payload->>'content', chr(10), ' '), "
            "chr(13), ' '),1,120), count(*) FROM runtime_events "
            "WHERE kind='model.item_completed' AND payload->>'kind'='text' "
            "GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 4")
        inv["repeated_tool_call_ratio_bp"] = repeated_tool_call_ratio_bp(
            snap["tool_calls"], snap["tool_calls_distinct_inputs"]
        )
        inv["repeated_text_answers"] = [
            {"text": t, "count": n}
            for _, t, n in pairs3(rows)
            if n >= 3
        ]
        # 3c. Consecutive conclusions with no action in between.
        #
        # The first attempt at this checked lexical similarity between consecutive
        # answers. Measured against run one30's seven real final answers, the
        # consecutive similarities were [0.95, 0.31, 0.16, 0.11, 0.23, 0.30] - the
        # model rewrote substantially every time, so a similarity threshold cannot
        # see the loop. What is invariant is not the words: it is that the model
        # concluded, was sent back, and concluded again without doing anything.
        #
        # So the signal is structural: the longest run of text answers with no
        # completed tool invocation between them.
        rows = self.q(
            "SELECT kind, payload->>'kind', status FROM runtime_events "
            "WHERE kind IN ('model.item_completed','tool.invocation.completed',"
            "'tool.invocation.started','agentic.action_applied','execution.effect.intent') "
            "ORDER BY created_at_ms")
        inv["consecutive_idle_answers"] = longest_idle_answer_run(rows)
        # A high lexical similarity is still worth reporting when it happens - it
        # is the same loop in an easier-to-see spelling - but it is no longer the
        # detector.
        # Per actor: two agents independently writing a similar summary is not
        # one model restating itself, and a cross-actor window invents pairs
        # that no single writer produced.
        rows = self.q(
            "SELECT payload->>'_runtime_activity_binding', "
            "substr(replace(replace(payload->>'content', chr(10), ' '), "
            "chr(13), ' '), 1, 600) FROM runtime_events "
            "WHERE kind='model.item_completed' AND payload->>'kind'='text' "
            "ORDER BY created_at_ms DESC LIMIT 48")
        per_actor = {}
        for row in rows:
            if len(row) == 2 and row[0] and row[1]:
                per_actor.setdefault(row[0], []).append(row[1])
        pairs_found = []
        for texts in per_actor.values():
            window = list(reversed(texts))[:8]
            pairs_found.extend(similar_answer_pairs(window))
        inv["similar_answer_pairs"] = pairs_found
        # 4. Rework reasons verbatim (the acceptance-criteria fingerprint).
        rows = self.q(
            "SELECT substr(payload->'envelope'->'action'->'input'->>'reason',1,220) "
            "FROM runtime_events WHERE kind='agentic.action_applied' "
            "AND payload->'envelope'->'action'->'input'->>'decision'='rework' "
            "ORDER BY created_at_ms DESC LIMIT 2")
        inv["rework_reasons"] = [r[0] for r in rows if r and r[0]]

        # 5. Where the steps go: root vs delegated actors.
        rows = self.q(
            "SELECT CASE WHEN payload->>'_runtime_activity_binding' LIKE '%owner:root:%' THEN 'root' "
            "WHEN payload->>'_runtime_activity_binding' LIKE '%owner:run:%' THEN 'agent' ELSE 'other' END AS who, "
            "count(*) FROM runtime_events WHERE kind='model.item_completed' GROUP BY 1")
        inv["steps_by_actor"] = {w: int(n) for w, n in rows}

        # 6. Last actions applied (what the collaboration is doing right now).
        rows = self.q(
            "SELECT payload->'envelope'->'action'->>'kind' FROM runtime_events "
            "WHERE kind='agentic.action_applied' "
            "ORDER BY created_at_ms DESC LIMIT 6")
        inv["recent_actions"] = [r[0] for r in rows if r and r[0]]
        return inv

    def conclude(self, snap, inv):
        """Root-cause finding with the evidence that supports it."""
        findings = []
        if snap["verdicts"] > 0:
            return {"state": "TERMINAL", "findings": ["verdict reached"]}
        advancing = snap["objective_reviews"] > 0 and (
            snap["completion_requests"] > snap["reopens"])
        if advancing:
            return {"state": "ADVANCING", "findings": []}
        if snap["reworks"] >= 2:
            reasons = inv.get("rework_reasons") or []
            findings.append({
                "root_cause": "rework loop: reviewers enforce criteria beyond the operator's stated requirement",
                "evidence": reasons[:1] or ["rework decisions recorded"],
                "action": "stop; the task acceptance criteria were inflated at authoring time",
            })
        # Time-based as well as step-based: a single-stream run burns wall
        # clock far slower than a concurrent one, so a wall-clock threshold is
        # the one that works for both (Qwen v11: 13 steps in 14 minutes, frozen).
        #
        # A step count is deliberately NOT a symptom. Steps measure how much
        # work the framework did, not whether that work converged: a healthy
        # run of this scenario legitimately crosses 150 steps (one reached a
        # verdict at 112, another ran 349), and three agents each reading the
        # same file once is information gathering, not waste. Judging health by
        # volume reported a healthy run as spinning and trained the operator to
        # ignore the monitor. Only convergence toward the Objective counts.
        stalled_long = (
            snap.get("last_event_age_s", 0) > 600 and snap["completion_requests"] == 0
        )
        if stalled_long:
            findings.append({
                "root_cause": "objective never requested: work phase never converges to a completion request",
                "evidence": [f"{snap['model_steps']} steps, 0 completion requests, recent actions {inv.get('recent_actions')}"],
                "action": "stop; inspect task authoring and rework pressure",
            })
        # An `artifact_commit` on `current_message_block:<i>` names a block of
        # the response being written right now, so its input is identical every
        # time while the content is different each time. Counting those as
        # re-issued calls reported a healthy run as spinning. The signature
        # carries no content, so it cannot tell them apart; exclude them rather
        # than guess. The filter belongs with the judgment, not the collection,
        # so every caller of `conclude` gets the same answer.
        repeats = [
            r for r in inv.get("repeated_tool_inputs", [])
            if r["count"] >= 5 and "current_message_block:" not in r["input"]
        ]
        if repeats:
            findings.append({
                "root_cause": "identical tool inputs repeated: the model is re-issuing the same call",
                "evidence": [f"{r['input'][:60]} x{r['count']}" for r in repeats[:3]],
                "action": "stop; the loop guard or task framing is not breaking the cycle",
            })
        # A model repeating its own final answer is spinning even though every
        # round emits events: the work is done and the exit rung is not being
        # taken. The silence signal cannot see it, so this one has to.
        repeated_answers = [
            r for r in inv.get("repeated_text_answers", []) if r["count"] >= 3]
        if repeated_answers and snap.get("verdicts", 0) == 0:
            findings.append({
                "root_cause": "the model is repeating the same final answer instead of taking the next rung",
                "evidence": [f"{r['text'][:80]}... x{r['count']}" for r in repeated_answers[:2]],
                "action": "stop; the closure view is not naming the remaining rung, or the model is not reading it",
            })

        # A repeated read is only waste when the reader keeps going and the
        # count is far past what independent agents would produce. Three agents
        # each opening the same file once is three first reads, not a loop; the
        # deduplication contract is keyed on session + path + content hash and
        # deliberately does not share receipts across agents. Require both a
        # high count and live events, so a quiet run is never called wasteful.
        reads = [
            r for r in inv.get("repeated_reads", [])
            if r["count"] >= 8 and snap.get("last_event_age_s", 0) < 300
        ]
        if reads:
            findings.append({
                "root_cause": "redundant full reads of the same source: receipt reuse not happening",
                "evidence": [f"{r['call'][:60]} x{r['count']}" for r in reads[:3]],
                "action": "stop; efficiency contract not reaching delegated actors",
            })
        idle = inv.get("consecutive_idle_answers") or {}
        if idle.get("run", 0) >= idle.get("threshold", 3) and snap.get("verdicts", 0) == 0:
            findings.append({
                "root_cause": (
                    "the model keeps concluding without acting: N text answers in a "
                    "row with no completed tool call between them"
                ),
                "evidence": [
                    "%d consecutive text answers with no tool call between them"
                    % idle["run"]
                ],
                "action": (
                    "stop; the model has stopped acting. A lexical similarity check "
                    "cannot see this - one30's answers averaged 0.34 similarity while "
                    "the loop was real"
                ),
            })
        if inv.get("similar_answer_pairs") and snap.get("verdicts", 0) == 0:
            findings.append({
                "root_cause": "consecutive answers are near-identical in wording",
                "evidence": [
                    "pairs above threshold: %s" % inv["similar_answer_pairs"]
                ],
                "action": "stop; the model is restating itself",
            })
        state = "SPINNING" if findings else "WORKING"
        return {"state": state, "findings": findings}


def parse_args(argv):
    """Positional postgres url, plus `--investigate` and `--state-file`.

    An unknown or missing state file is a first observation, never an error:
    the monitor's job is to observe this run, and it must not refuse to start
    because its memory of the last interval is unreadable.
    """
    url = None
    investigate = False
    state_file = os.environ.get("COWD_MONITOR_STATE_FILE")
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--investigate":
            investigate = True
        elif arg == "--state-file":
            index += 1
            state_file = argv[index] if index < len(argv) else None
        elif arg.startswith("--state-file="):
            state_file = arg.split("=", 1)[1]
        elif url is None:
            url = arg
        index += 1
    return url, investigate, state_file


def main():
    url, investigate, state_file = parse_args(sys.argv[1:])
    if not url:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    m = Monitor(url)
    snap = m.snapshot()
    inv = m.investigate(snap) if investigate else {}
    conclusion = m.conclude(snap, inv) if investigate else {"state": "UNKNOWN", "findings": []}
    out = dict(snap)
    out["investigation"] = inv
    out["conclusion"] = conclusion
    # Verify this report against the previous one before anyone reads it. A
    # finding that comes back with only its counts growing is the same finding
    # confirmed, and `escalate` is what turns that confirmation into a stop
    # instead of another line in a log nobody acts on.
    verification = verify(load_state(state_file), conclusion, snap)
    verification["state_persisted"] = save_state(
        state_file,
        {
            "fingerprint": verification["fingerprint"],
            "findings_text": findings_text(conclusion),
            "finding_counts": verification["finding_counts"],
            "repeat_count": verification["repeat_count"],
            "milestones": {key: snap.get(key, 0) for key in MILESTONE_KEYS},
            "updated_at_ms": int(time.time() * 1000),
        },
    )
    out["verification"] = verification
    print(json.dumps(out, indent=1, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    main()
