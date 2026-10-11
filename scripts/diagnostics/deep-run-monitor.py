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
from collections import Counter


def psql(url, sql):
    out = subprocess.run(
        ["psql", url, "-t", "-A", "-F\u0001", "-c", sql],
        capture_output=True, text=True, env=dict(os.environ),
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip())
    return [line.split("\u0001") for line in out.stdout.splitlines() if line.strip()]


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
        rows = self.q(
            "SELECT substr(payload->>'content',1,90), count(*) FROM runtime_events "
            "WHERE kind='model.item_completed' AND payload->>'kind'='tool_call' "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 6")
        inv["repeated_tool_inputs"] = [
            {"input": t, "count": int(n)} for t, n in rows if int(n) >= 3]
        # 3. Redundant full reads of the same path.
        rows = self.q(
            "SELECT payload->>'content', count(*) FROM runtime_events "
            "WHERE kind='model.item_completed' AND payload->>'kind'='tool_call' "
            "AND payload->>'tool_name' IN ('read_file','read_ref') "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 4")
        inv["repeated_reads"] = [
            {"call": t[:90], "count": int(n)} for t, n in rows if int(n) >= 3]

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
        state = "SPINNING" if findings else "WORKING"
        return {"state": state, "findings": findings}


def main():
    url = sys.argv[1]
    investigate = "--investigate" in sys.argv
    m = Monitor(url)
    snap = m.snapshot()
    inv = m.investigate(snap) if investigate else {}
    conclusion = m.conclude(snap, inv) if investigate else {"state": "UNKNOWN", "findings": []}
    out = dict(snap)
    out["investigation"] = inv
    out["conclusion"] = conclusion
    print(json.dumps(out, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
