#!/usr/bin/env python3
"""Regression tests for the deep monitor's progress definition.

The monitor judges health by convergence toward the Objective, never by volume.
Both mistakes were made in production: a step threshold and a four-repeat read
threshold each reported a healthy run as spinning, which trained the operator to
ignore the monitor. These cases pin the definition so it cannot drift back.

Run: python3 scripts/diagnostics/test_deep_run_monitor.py
"""
import importlib.util
import os
import sys
import tempfile

# The module is named with a hyphen, so it cannot be imported directly.
_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "deep_run_monitor", os.path.join(_HERE, "deep-run-monitor.py")
)
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
Monitor = _module.Monitor
pairs = _module.pairs
repeated_tool_call_ratio_bp = _module.repeated_tool_call_ratio_bp
longest_idle_answer_run = _module.longest_idle_answer_run
similar_answer_pairs = _module.similar_answer_pairs
verify = _module.verify
finding_fingerprints = _module.finding_fingerprints
findings_text = _module.findings_text
converged = _module.converged
load_state = _module.load_state
save_state = _module.save_state


def snap(**overrides):
    base = {
        "model_steps": 0,
        "tool_calls": 0,
        "tool_failures": 0,
        "completion_requests": 0,
        "reopens": 0,
        "objective_reviews": 0,
        "verdicts": 0,
        "reworks": 0,
        "task_reviews": 0,
        "task_submits": 0,
        "elapsed_s": 0,
        "last_event_age_s": 0,
    }
    base.update(overrides)
    return base


def inv(**overrides):
    base = {
        "task_churn": [],
        "repeated_tool_inputs": [],
        "repeated_reads": [],
        "repeated_text_answers": [],
        "rework_reasons": [],
        "steps_by_actor": {},
        "recent_actions": [],
    }
    base.update(overrides)
    return base


ONE31_REFS = [
    '{"reference": "artifact:83a19d51623c6e8987c1ac51"}',
    '{"reference": "artifact:d19f537ffb5070bccb9249f3"}',
    '{"reference": "artifact:46df7642c86b72c61dccb17c"}',
]


def spinning(counts=(10, 10, 9)):
    """Run one31's conclusion, verbatim in shape.

    The same three artifact references under two findings, with the repetition
    counts as the only thing that changes between reports.
    """
    return {
        "state": "SPINNING",
        "findings": [
            {
                "root_cause": "identical tool inputs repeated: the model is re-issuing the same call",
                "evidence": ["%s x%d" % (ref, n) for ref, n in zip(ONE31_REFS, counts)],
                "action": "stop; the loop guard or task framing is not breaking the cycle",
            },
            {
                "root_cause": "redundant full reads of the same source: receipt reuse not happening",
                "evidence": ["%s x%d" % (ref, n) for ref, n in zip(ONE31_REFS, counts)],
                "action": "stop; efficiency contract not reaching delegated actors",
            },
        ],
    }


def milestones(**overrides):
    base = {"completion_requests": 0, "verdicts": 0, "objective_reviews": 0, "reworks": 0}
    base.update(overrides)
    return base


def carried(result, conclusion, marks):
    """The state main() persists after a report, so tests carry it forward."""
    return {
        "fingerprint": result["fingerprint"],
        "findings_text": findings_text(conclusion),
        "repeat_count": result["repeat_count"],
        "milestones": marks,
    }


CASES = []


def case(name):
    def decorate(fn):
        CASES.append((name, fn))
        return fn
    return decorate


@case("a healthy high-volume run is not spinning")
def _():
    # v14 at 02:12:48: 156 steps, no completion request yet, four first reads of
    # one file, events 12s old. Every floor was met and the review later landed.
    conclusion = Monitor("").conclude(
        snap(model_steps=156, tool_calls=73, task_submits=3, task_reviews=3,
             last_event_age_s=12),
        inv(repeated_reads=[{"call": '{"path":"crates/tools/src/lib.rs"}', "count": 4}]),
    )
    assert conclusion["state"] == "WORKING", conclusion


@case("a run still emitting events is not spinning on volume alone")
def _():
    conclusion = Monitor("").conclude(
        snap(model_steps=349, completion_requests=0, last_event_age_s=94),
        inv(),
    )
    assert conclusion["state"] == "WORKING", conclusion


@case("a repeated tool input is spinning")
def _():
    conclusion = Monitor("").conclude(
        snap(model_steps=120, last_event_age_s=5),
        inv(repeated_tool_inputs=[{"input": '{"path":"a.rs"}', "count": 6}]),
    )
    assert conclusion["state"] == "SPINNING", conclusion
    assert conclusion["findings"], conclusion


@case("a rework loop is spinning")
def _():
    conclusion = Monitor("").conclude(
        snap(model_steps=304, reworks=2, last_event_age_s=5),
        inv(rework_reasons=["invented criterion"]),
    )
    assert conclusion["state"] == "SPINNING", conclusion


@case("a genuinely redundant read loop is spinning")
def _():
    # Eight repeats of the same range with events still flowing: far past what
    # independent agents produce, and the reader is clearly not reusing receipts.
    conclusion = Monitor("").conclude(
        snap(model_steps=175, last_event_age_s=94),
        inv(repeated_reads=[{"call": '{"limit":30,"offset":90}', "count": 8}]),
    )
    assert conclusion["state"] == "SPINNING", conclusion


@case("a quiet run that never asked for completion is spinning")
def _():
    conclusion = Monitor("").conclude(
        snap(model_steps=13, completion_requests=0, last_event_age_s=840),
        inv(),
    )
    assert conclusion["state"] == "SPINNING", conclusion


@case("a satisfied verdict is terminal")
def _():
    conclusion = Monitor("").conclude(snap(verdicts=1), inv())
    assert conclusion["state"] == "TERMINAL", conclusion


@case("a message-block commit is not a repeated call")
def _():
    # `artifact_commit` on `current_message_block:0` has an identical input every
    # time while the content differs; the signature carries no content.
    conclusion = Monitor("").conclude(
        snap(model_steps=120, last_event_age_s=5),
        inv(repeated_tool_inputs=[
            {"input": '{"content_ref":"current_message_block:0"}', "count": 6}
        ]),
    )
    assert conclusion["state"] == "WORKING", conclusion


@case("a model repeating its own final answer is spinning")
def _():
    # Events keep flowing while the answer is copied, so the silence signal
    # cannot see it. One live run emitted the same summary seven times over
    # fourteen minutes and the monitor reported WORKING throughout.
    conclusion = Monitor("").conclude(
        snap(model_steps=200, completion_requests=1, last_event_age_s=4),
        inv(repeated_text_answers=[
            {"text": "Source Verification Team 执行结果 ...", "count": 7}
        ]),
    )
    assert conclusion["state"] == "SPINNING", conclusion
    assert any("repeating the same final answer" in f["root_cause"]
               for f in conclusion["findings"]), conclusion


@case("a repeated answer after the verdict is terminal, not spinning")
def _():
    conclusion = Monitor("").conclude(
        snap(model_steps=200, verdicts=1, last_event_age_s=4),
        inv(repeated_text_answers=[{"text": "done", "count": 9}]),
    )
    assert conclusion["state"] == "TERMINAL", conclusion


@case("a multi-line text payload never breaks the monitor's row parsing")
def _():
    # A `text` payload is multi-line markdown. psql emits the newlines verbatim,
    # so the parsed rows are not guaranteed to be (value, count) pairs. The
    # repeated-answer detector unpacked them blindly and raised on every
    # iteration of a live run: the shell loop treated each empty report as "no
    # data" and continued, so the deep monitor emitted four lines across a
    # 35-minute run that was working the whole time. The monitor must survive a
    # payload it did not expect.
    rows = pairs(
        [
            ["# Audit Report", "2"],
            ["", ""],
            ["## Executive Summary", ""],
            ["a normal single-line answer", "4"],
            ["orphan line with no count"],
            ["not a number", "abc"],
        ],
    )
    parsed = list(rows)
    assert ("a normal single-line answer", 4) in parsed, parsed
    assert all(isinstance(count, int) for _, count in parsed), parsed
    # Malformed rows are dropped, not fatal.
    assert len(parsed) == 2, parsed


@case("the repeat ratio measures waste, not volume")
def _():
    # one20 finished with 443 tool calls of which 179 were distinct: 59.6% of the
    # tool work carried no new information, and nothing in the run said so. one21,
    # which reached a satisfied verdict, ran 50 calls with 47 distinct: 6%. The
    # ratio has to separate those two, or it is just another counter.
    ratio = repeated_tool_call_ratio_bp
    # one20: 443 calls, 179 distinct.
    assert 5800 <= ratio(443, 179) <= 6100, ratio(443, 179)
    # one21, which reached a satisfied verdict: 50 calls, 47 distinct.
    assert ratio(50, 47) <= 1000, ratio(50, 47)
    # No calls yet is not 100% waste.
    assert ratio(0, 0) == 0
    # Every call distinct is no waste at all.
    assert ratio(10, 10) == 0
    # Every call a repeat is total waste, and never over 100%.
    assert ratio(10, 0) == 10_000
    assert ratio(3, 9) == 0, "a nonsensical snapshot must not read as negative waste"


@case("consecutive conclusions with no action between them are the real loop signal")
def _():
    # Run one30's seven final answers, measured: their consecutive lexical
    # similarities were [0.95, 0.31, 0.16, 0.11, 0.23, 0.30]. A similarity
    # threshold cannot see that loop. What is invariant is that the model
    # concluded, was sent back, and concluded again without doing anything.
    events = []
    # real work first
    events += [("model.item_completed", "completed")] * 2
    events += [("tool.invocation.completed", "completed")]
    # then the loop: six conclusions, one tool call in the whole window
    events += [("model.item_completed", "completed")] * 6
    result = longest_idle_answer_run(events)
    assert result["run"] >= 6, result
    assert result["run"] >= result["threshold"], result

    # Two answers, a tool call, one more answer: the run is 2, below the
    # threshold, so nothing fires. A verdict suppresses the finding separately,
    # in the caller.
    healthy = [("model.item_completed", "completed")] * 2
    healthy += [("tool.invocation.completed", "completed")]
    healthy += [("model.item_completed", "completed")]
    assert longest_idle_answer_run(healthy)["run"] == 2, longest_idle_answer_run(healthy)
    assert longest_idle_answer_run(healthy)["run"] < longest_idle_answer_run(healthy)["threshold"]

    # Work in between is progress, not a loop.
    interleaved = []
    for _ in range(6):
        interleaved += [("model.item_completed", "completed"),
                        ("tool.invocation.completed", "completed")]
    assert longest_idle_answer_run(interleaved)["run"] == 1


@case("a lexical similarity is reported as evidence, not used as the detector")
def _():
    # Same words twice: worth reporting.
    same = ["the team finished and three facts were verified",
            "the team finished and three facts were verified"]
    assert len(similar_answer_pairs(same)) == 1
    # Reworded: the real loop, invisible to similarity.
    reworded = ["the team finished and three facts were verified",
                "Team complete: three verifiable facts confirmed"]
    assert similar_answer_pairs(reworded) == []


@case("growing counts do not change a finding's fingerprint")
def _():
    # one31's evidence ran x10 -> x11 -> x13 while the loop spun. If the
    # fingerprint moved with the count, every interval would look like a new
    # finding and the repeat could never be counted - which is exactly what
    # happened for ten intervals on the live run.
    assert finding_fingerprints(spinning((10, 10, 9))) == finding_fingerprints(
        spinning((13, 13, 12))
    ), "counts are not identity"
    # The subject is identity: a different artifact is a different finding.
    other = spinning((10, 10, 9))
    other["findings"][0]["evidence"][0] = '{"reference": "artifact:0000"} x10'
    assert finding_fingerprints(other) != finding_fingerprints(spinning())


@case("the first report of a finding is not yet an escalation")
def _():
    result = verify(None, spinning(), milestones())
    assert result["repeat_count"] == 1, result
    assert result["escalate"] is False, result
    assert result["similarity_to_previous"] is None, result


@case("the same finding on the next report counts a repeat")
def _():
    first = verify(None, spinning((10, 10, 9)), milestones())
    state = carried(first, spinning((10, 10, 9)), milestones())
    second = verify(state, spinning((11, 11, 10)), milestones())
    assert second["repeat_count"] == 2, second
    assert second["escalate"] is False, second
    # The counts grew, but the result is still recognizably the same one.
    assert second["similarity_to_previous"] is not None, second
    assert second["similarity_to_previous"] > 0.9, second


@case("the third consecutive report of one finding escalates")
def _():
    # The one31 signature, compressed: three intervals, same finding, counts
    # only growing, nothing converging. This is the stop the harness acts on.
    state = carried(verify(None, spinning((10, 10, 9)), milestones()),
                    spinning((11, 11, 10)), milestones())
    state["repeat_count"] = 2
    third = verify(state, spinning((13, 13, 12)), milestones())
    assert third["repeat_count"] == 3, third
    assert third["escalate"] is True, third
    assert "3 consecutive" in third["reason"], third


@case("a completion request resets the count: the run converged")
def _():
    state = carried(verify(None, spinning(), milestones()), spinning(), milestones())
    state["repeat_count"] = 2
    result = verify(state, spinning((13, 13, 12)), milestones(completion_requests=1))
    assert result["repeat_count"] == 1, result
    assert result["escalate"] is False, result


@case("a verdict resets the count and never escalates")
def _():
    state = carried(verify(None, spinning(), milestones()), spinning(), milestones())
    state["repeat_count"] = 5
    result = verify(state, spinning((13, 13, 12)), milestones(verdicts=1))
    assert result["repeat_count"] == 1, result
    assert result["escalate"] is False, result


@case("a review is not convergence: one31 reviewed the same loop three times")
def _():
    # objective_reviews moved 2 -> 3 -> 4 while the loop ran on the live run.
    # Counting a review as convergence renews the loop's lease every interval.
    assert converged({"objective_reviews": 2}, {"objective_reviews": 3}) is False
    assert converged({"reworks": 1}, {"reworks": 4}) is False
    assert converged({"completion_requests": 0}, {"completion_requests": 1}) is True
    assert converged({"verdicts": 0}, {"verdicts": 1}) is True
    assert converged({}, {"completion_requests": 1}) is False, "no history is not convergence"


@case("a different finding resets the count")
def _():
    state = carried(verify(None, spinning(), milestones()), spinning(), milestones())
    state["repeat_count"] = 2
    other = {
        "state": "SPINNING",
        "findings": [{
            "root_cause": "rework loop: reviewers enforce criteria beyond the operator's stated requirement",
            "evidence": ["invented criterion"],
            "action": "stop; the task acceptance criteria were inflated at authoring time",
        }],
    }
    result = verify(state, other, milestones())
    assert result["repeat_count"] == 1, result


@case("a run with no finding has nothing to repeat")
def _():
    healthy = {"state": "WORKING", "findings": []}
    result = verify(None, healthy, milestones())
    assert result["repeat_count"] == 0, result
    assert result["escalate"] is False, result
    terminal = {"state": "TERMINAL", "findings": []}
    result = verify(None, terminal, milestones(verdicts=1))
    assert result["repeat_count"] == 0, result
    assert result["escalate"] is False, result


@case("a terminal conclusion's plain-sentence finding never breaks the fingerprint")
def _():
    # conclude() reports a satisfied verdict as findings: ["verdict reached"] -
    # a string, not the structured shape every other finding has. The replay
    # crashed on exactly this until the fingerprinting tolerated it.
    terminal = {"state": "TERMINAL", "findings": ["verdict reached"]}
    result = verify(None, terminal, milestones(verdicts=1))
    assert result["repeat_count"] == 0, result
    assert result["escalate"] is False, result
    assert finding_fingerprints(terminal) == ["verdict reached"]


@case("a missing or corrupt state file is a first observation, never a crash")
def _():
    # The monitor went dark for 35 minutes once because it raised on data it
    # did not expect. Its memory must fail the same way its queries do: into a
    # first observation, not into an exception.
    assert load_state("/nonexistent/dir/state.json") is None
    assert load_state("") is None
    assert load_state(None) is None
    with tempfile.TemporaryDirectory() as tmp:
        garbage = os.path.join(tmp, "garbage.json")
        with open(garbage, "w", encoding="utf-8") as handle:
            handle.write("{not json at all")
        assert load_state(garbage) is None
        listed = os.path.join(tmp, "a-directory")
        os.makedirs(listed)
        assert load_state(listed) is None, "a directory is not a state file"
        array = os.path.join(tmp, "array.json")
        with open(array, "w", encoding="utf-8") as handle:
            handle.write("[1, 2, 3]")
        assert load_state(array) is None, "a non-object state is not a state file"


@case("state round-trips through JSON, and an unwritable path fails loudly")
def _():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "nested", "state.json")
        state = carried(verify(None, spinning(), milestones()), spinning(), milestones())
        assert save_state(path, state) is True
        assert load_state(path) == state, "the carried state must survive a round-trip"
        # A path under a regular file cannot be created; the monitor must
        # report that instead of raising, because an escalation that cannot be
        # counted is the failure this mechanism exists to remove.
        blocker = os.path.join(tmp, "blocker")
        with open(blocker, "w", encoding="utf-8") as handle:
            handle.write("x")
        assert save_state(os.path.join(blocker, "state.json"), state) is False
        assert save_state(None, state) is None


@case("the harness stop gate reads the escalation the monitor emits")
def _():
    # The monitor and the harness are two layers; the escalation is only real
    # if the layer that can stop the run reads the field the monitor writes.
    # This pins the contract so neither side can drift.
    scenario = os.path.join(_HERE, "..", "scenarios", "harness-eval-real-qwen.sh")
    with open(scenario, encoding="utf-8") as handle:
        text = handle.read()
    assert ".verification.escalate" in text, "the harness must act on the escalation"
    assert "--state-file" in text, "the harness must give the monitor durable state"
    assert "harness_eval_deep_monitor_stop" in text, "the stop must be observable"


def main():
    failures = 0
    for name, fn in CASES:
        try:
            fn()
        except AssertionError as error:
            failures += 1
            print(f"FAIL {name}: {error}")
        else:
            print(f"ok   {name}")
    print(f"\n{len(CASES) - failures}/{len(CASES)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
