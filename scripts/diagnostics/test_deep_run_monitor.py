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
pairs3 = _module.pairs3
repeated_tool_call_ratio_bp = _module.repeated_tool_call_ratio_bp
longest_idle_answer_run = _module.longest_idle_answer_run
similar_answer_pairs = _module.similar_answer_pairs
verify = _module.verify
finding_fingerprints = _module.finding_fingerprints
findings_text = _module.findings_text
finding_counts = _module.finding_counts
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
        "finding_counts": result["finding_counts"],
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
    events += [("model.item_completed", "tool_call", "completed")]
    events += [("tool.invocation.completed", None, "completed")]
    # then the loop: six conclusions, one tool call in the whole window
    events += [("model.item_completed", "text", "completed")] * 3
    events += [("model.item_completed", "tool_call", "completed")]
    events += [("model.item_completed", "text", "completed")] * 3
    result = longest_idle_answer_run(events)
    assert result["run"] >= 3, result
    assert result["run"] >= result["threshold"], result

    # Two answers, a tool call, one more answer: the run is 2, below the
    # threshold, so nothing fires. A verdict suppresses the finding separately,
    # in the caller.
    healthy = [("model.item_completed", "text", "completed")] * 2
    healthy += [("tool.invocation.completed", None, "completed")]
    healthy += [("model.item_completed", "text", "completed")]
    assert longest_idle_answer_run(healthy)["run"] == 2, longest_idle_answer_run(healthy)
    assert longest_idle_answer_run(healthy)["run"] < longest_idle_answer_run(healthy)["threshold"]

    # Work in between is progress, not a loop.
    interleaved = []
    for _ in range(6):
        interleaved += [("model.item_completed", "tool_call", "completed"),
                        ("tool.invocation.completed", None, "completed")]
    # Every round acts, so there is no conclusion run at all.
    assert longest_idle_answer_run(interleaved)["run"] == 0


@case("tool calls and reasoning are not conclusions, and agentic actions reset")
def _():
    # The live 2026-10-12 run this detector nearly stopped: its reported run of
    # 8 was ['tool_call','tool_call','tool_call','public_reasoning','tool_call',
    # 'public_reasoning','tool_call','public_reasoning'] - zero text answers,
    # because agentic actions (task_submit, artifact_commit) apply without a
    # tool-invocation event and reasoning is not a conclusion.
    working = [
        ("model.item_completed", "tool_call", "completed"),
        ("model.item_completed", "tool_call", "completed"),
        ("model.item_completed", "tool_call", "completed"),
        ("model.item_completed", "public_reasoning", "completed"),
        ("model.item_completed", "tool_call", "completed"),
        ("model.item_completed", "public_reasoning", "completed"),
        ("model.item_completed", "tool_call", "completed"),
        ("model.item_completed", "public_reasoning", "completed"),
    ]
    assert longest_idle_answer_run(working)["run"] == 0, longest_idle_answer_run(working)
    # Agentic actions are work and reset the run even without a tool event.
    agentic = [("model.item_completed", "text", "completed")] * 2
    agentic += [("agentic.action_applied", None, "applied")]
    agentic += [("model.item_completed", "text", "completed")]
    assert longest_idle_answer_run(agentic)["run"] == 2, longest_idle_answer_run(agentic)
    # Three conclusions with nothing in between is the real signal.
    looping = [("model.item_completed", "text", "completed")] * 3
    assert longest_idle_answer_run(looping)["run"] == 3
    assert longest_idle_answer_run(looping)["run"] >= longest_idle_answer_run(looping)["threshold"]
    # A started invocation is action too.
    started = [("model.item_completed", "text", "completed")] * 2
    started += [("tool.invocation.started", None, "started")]
    started += [("model.item_completed", "text", "completed")]
    assert longest_idle_answer_run(started)["run"] == 2


@case("repeat queries are actor-scoped: independent agents are not a loop")
def _():
    # Three agents each reading the same file once is three first reads; the
    # receipt-reuse contract is per-agent by design. A cross-actor sum turns
    # independent information gathering into a finding.
    rows = [
        ["actor-a", '{"path": "crates/runtime/src/lib.rs"}', "1"],
        ["actor-b", '{"path": "crates/runtime/src/lib.rs"}', "1"],
        ["actor-c", '{"path": "crates/runtime/src/lib.rs"}', "1"],
        ["actor-a", '{"path": "other.rs"}', "2"],
    ]
    parsed = list(pairs3(rows))
    assert ("actor-a", '{"path": "crates/runtime/src/lib.rs"}', 1) in parsed, parsed
    assert all(isinstance(count, int) for _, _, count in parsed), parsed
    # No actor reached the redundant-read bound, so no finding is possible.
    conclusion = Monitor("").conclude(
        snap(model_steps=90, last_event_age_s=10),
        inv(repeated_reads=[{"call": '{"path": "crates/runtime/src/lib.rs"}', "count": 3}]),
    )
    assert conclusion["state"] == "WORKING", conclusion
    # One actor reading it eight times is the real loop.
    conclusion = Monitor("").conclude(
        snap(model_steps=90, last_event_age_s=10),
        inv(repeated_reads=[{"call": '{"path": "crates/runtime/src/lib.rs"}', "count": 8}]),
    )
    assert conclusion["state"] == "SPINNING", conclusion
    # Malformed rows are dropped, not fatal.
    assert list(pairs3([["a", "b"], ["a", "b", "x"], ["a", "b", "3"]])) == [("a", "b", 3)]


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


@case("a growing evidence roster does not change a finding's identity")
def _():
    # The live 2026-10-12 run reset its own escalation twice because a second
    # and then a third repeated input crossed the reporting threshold and joined
    # the evidence list. The finding was the same finding with more examples.
    # A worsening loop must not look like a new one.
    two_examples = spinning((10, 10, 9))
    three_examples = spinning((10, 10, 9))
    three_examples["findings"][0]["evidence"].append(
        '{"reference": "artifact:015125cb446fef55279efeae"} x7'
    )
    assert finding_fingerprints(two_examples) == finding_fingerprints(three_examples)
    # The subject is not identity either: looping on a different file is still
    # the same class of waste, and the post-mortem keeps the current evidence.
    other_file = spinning((10, 10, 9))
    other_file["findings"][0]["evidence"][0] = '{"path": "crates/other.rs"} x10'
    assert finding_fingerprints(other_file) == finding_fingerprints(spinning())
    # The assertion is identity: a different root cause is a different finding.
    different = {
        "state": "SPINNING",
        "findings": [{
            "root_cause": "rework loop: reviewers enforce criteria beyond the operator's stated requirement",
            "evidence": ["invented criterion"],
            "action": "stop",
        }],
    }
    assert finding_fingerprints(different) != finding_fingerprints(spinning())


@case("a finding set that only gained evidence keeps counting its repeat")
def _():
    # The exact regression the live run exposed: the second report carried one
    # more evidence line under the same root causes, and the counter restarted
    # from 1 instead of advancing to the escalation.
    first = verify(None, spinning((14, 14, 14)), milestones())
    state = carried(first, spinning((14, 14, 14)), milestones())
    grown = spinning((14, 14, 14))
    grown["findings"][0]["evidence"].append(
        '{"reference": "artifact:015125cb446fef55279efeae"} x9'
    )
    grown["findings"][1]["evidence"].append(
        '{"reference": "artifact:015125cb446fef55279efeae"} x9'
    )
    second = verify(state, grown, milestones())
    assert second["repeat_count"] == 2, second
    assert second["escalate"] is False, second
    state = carried(second, grown, milestones())
    still_going = spinning((15, 15, 15))
    still_going["findings"][0]["evidence"].append(
        '{"reference": "artifact:015125cb446fef55279efeae"} x9'
    )
    still_going["findings"][1]["evidence"].append(
        '{"reference": "artifact:015125cb446fef55279efeae"} x9'
    )
    third = verify(state, still_going, milestones())
    assert third["repeat_count"] == 3, third
    assert third["escalate"] is True, third


@case("a frozen finding is history, not a live loop")
def _():
    # The live 2026-10-13 run: the agent stopped re-reading after the runtime
    # forced it to conclude, but the finding kept reporting the same x8, the
    # repeat counted 1 -> 2 -> 3 on frozen evidence, and the monitor stopped a
    # run whose team had moved on. A repeat counts only when the counts moved.
    first = verify(None, spinning((8, 8, 8)), milestones())
    state = carried(first, spinning((8, 8, 8)), milestones())
    second = verify(state, spinning((8, 8, 8)), milestones())
    assert second["repeat_count"] == 1, second
    assert second["escalate"] is False, second
    assert second["counts_grew"] is False, second
    # One more emission and the loop is live again.
    third = verify(state, spinning((9, 8, 8)), milestones())
    assert third["repeat_count"] == 2, third
    assert third["counts_grew"] is True, third
    state = carried(third, spinning((9, 8, 8)), milestones())
    fourth = verify(state, spinning((10, 9, 9)), milestones())
    assert fourth["repeat_count"] == 3, fourth
    assert fourth["escalate"] is True, fourth


@case("finding counts read the loop's pulse out of the evidence")
def _():
    counts = finding_counts(spinning((8, 8, 8)))
    # Two findings, three evidence lines each, every line x8.
    assert counts["identical tool inputs repeated: the model is re-issuing the same call"] == 24
    idle = {"state": "SPINNING", "findings": [{
        "root_cause": "the model keeps concluding without acting",
        "evidence": ["7 consecutive text answers with no tool call between them"],
    }]}
    assert finding_counts(idle)["the model keeps concluding without acting"] == 7
    # A terminal sentence finding has no counts.
    assert finding_counts({"state": "TERMINAL", "findings": ["verdict reached"]}) == {
        "verdict reached": 0
    }


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


@case("the reviewer's own report re-entering the delivered set is spinning")
def _():
    # Mon7's livelock, compressed: seventeen review-report commits for five
    # reviews across sixty minutes with no verdict, while every count-based
    # signal said WORKING. The closure activity was feeding the gate judging
    # it - each commit grew the delivered set and invalidated its own review.
    conclusion = Monitor("").conclude(
        snap(model_steps=514, last_event_age_s=5),
        inv(closure={
            "objective_reviews": 5,
            "completion_requests": 2,
            "reopens": 2,
            "review_report_commits": 17,
        }),
    )
    assert conclusion["state"] == "SPINNING", conclusion
    assert any(
        "re-entering the delivered set" in f["root_cause"]
        for f in conclusion["findings"]
    ), conclusion


@case("a healthy closure is not the reviewer's report loop")
def _():
    # One review, one report commit: the closure did its job once.
    conclusion = Monitor("").conclude(
        snap(model_steps=100, last_event_age_s=5),
        inv(closure={
            "objective_reviews": 1,
            "completion_requests": 1,
            "reopens": 0,
            "review_report_commits": 1,
        }),
    )
    assert conclusion["state"] == "WORKING", conclusion
    # A gap review then a satisfied one, one report each: still healthy.
    conclusion = Monitor("").conclude(
        snap(model_steps=100, last_event_age_s=5),
        inv(closure={
            "objective_reviews": 2,
            "completion_requests": 2,
            "reopens": 1,
            "review_report_commits": 2,
        }),
    )
    assert conclusion["state"] == "WORKING", conclusion
    # After the verdict, nothing spins however the closure looked.
    conclusion = Monitor("").conclude(
        snap(model_steps=100, verdicts=1, last_event_age_s=5),
        inv(closure={
            "objective_reviews": 5,
            "completion_requests": 2,
            "reopens": 2,
            "review_report_commits": 17,
        }),
    )
    assert conclusion["state"] == "TERMINAL", conclusion


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


@case("the harness records semantic findings without terminating the measured run")
def _():
    scenario = os.path.join(_HERE, "..", "scenarios", "harness-eval-real-qwen.sh")
    with open(scenario, encoding="utf-8") as handle:
        text = handle.read()
    body = text.split("monitor_semantic_health() {", 1)[1].split("\n}", 1)[0]
    assert "--investigate" in body
    assert "harness_eval_deep_monitor" in body
    assert "kill -" not in body, "heuristic repetition must not terminate a real run"
    assert "harness_eval_deep_monitor_stop" not in body


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
