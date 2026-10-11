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

# The module is named with a hyphen, so it cannot be imported directly.
_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "deep_run_monitor", os.path.join(_HERE, "deep-run-monitor.py")
)
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
Monitor = _module.Monitor


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
        "rework_reasons": [],
        "steps_by_actor": {},
        "recent_actions": [],
    }
    base.update(overrides)
    return base


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
