#!/usr/bin/env python3
"""Cross-layer consistency check for a harness-eval report.

A layer disconnect is not a missing field. It is two layers answering the same
question from different sources, and it is dangerous precisely when the answers
happen to agree on the verdict while disagreeing on the evidence.

The case that motivated this checker: `agentic_program_closure_verified` failed
with evidence `program=missing, teams=0, agents=0, tasks=0, artifacts=0`, while
the same report's `live_gateway_scenarios.scenarios[0].production_trace
.agentic_program` carried one Team, four Agents and three Tasks, and the
report's own `agentic_program_closure.facts` carried the same. The gate's
evidence came from re-evaluating an already-evaluated closure, so every
projection field read as empty. The verdict was "failed" either way, which is
why nothing noticed.

So the rule this checker enforces is: **every gate's evidence must be derivable
from the report body, and every fact the report states twice must agree.** A
gate that reports a fact the body contradicts is a disconnect regardless of
whether its verdict is right.

Usage:
    python3 scripts/diagnostics/layer-consistency-check.py <report.json> [more.json ...]

Exit code is 0 when no disconnect is found, 1 otherwise.
"""
import json
import sys


def load(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def as_int(value):
    return value if isinstance(value, int) else None


def program_facts(report):
    """The Program facts, from every layer that states them."""
    found = {}
    closure = report.get("agentic_program_closure")
    if isinstance(closure, dict):
        facts = closure.get("facts")
        if isinstance(facts, dict):
            found["agentic_program_closure.facts"] = facts
    live = report.get("live_gateway_scenarios") or {}
    for index, scenario in enumerate(live.get("scenarios") or []):
        program = (scenario.get("production_trace") or {}).get("agentic_program")
        if isinstance(program, dict):
            found[f"live_gateway_scenarios.scenarios[{index}].production_trace"
                  ".agentic_program"] = {
                "teams": len(program.get("teams") or {}),
                "agents": len(program.get("agents") or {}),
                "tasks": len(program.get("tasks") or {}),
                "artifacts": len(program.get("artifacts") or {}),
                "program_id": program.get("program_id"),
            }
    return found


GATE_EVIDENCE_KEYS = {
    "teams": "teams",
    "agents": "agents",
    "tasks": "active_tasks",
    "artifacts": "artifacts",
    "topics": "topics",
}


def check_report(path):
    report = load(path)
    problems = []

    # ---- 1. A gate's evidence must be derivable from the report body --------
    gate = report.get("report_gate") or {}
    for item in gate.get("gates") or gate.get("items") or []:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("gate")
        evidence = item.get("evidence") or ""
        if name != "agentic_program_closure_verified":
            continue
        if "program=missing" not in evidence:
            continue
        # The gate says the Program is absent. Does any layer hold one?
        holders = program_facts(report)
        real = [label for label, facts in holders.items()
                if facts.get("teams") or facts.get("agents") or facts.get("tasks")]
        if real:
            problems.append(
                "agentic_program_closure_verified evidence says the Program is "
                f"absent ({evidence}) while {', '.join(real)} carries a real "
                "Program. The gate is reading a different source than the "
                "report body."
            )

    # ---- 2. The same fact stated in two layers must agree ------------------
    facts_by_layer = program_facts(report)
    if len(facts_by_layer) > 1:
        labels = sorted(facts_by_layer)
        base_label, base = labels[0], facts_by_layer[labels[0]]
        for other_label in labels[1:]:
            other = facts_by_layer[other_label]
            for key in ("teams", "agents", "tasks", "artifacts"):
                left, right = as_int(base.get(key)), as_int(other.get(key))
                if left is not None and right is not None and left != right:
                    problems.append(
                        f"{base_label} reports {key}={left} while "
                        f"{other_label} reports {key}={right}. One fact, two "
                        "answers."
                    )

    # ---- 2b. A report with no gate at all cannot be called consistent ------
    # Several stored reports carry the body but no `report_gate`. Reporting those
    # as "ok" would let the checker certify reports it never actually checked.
    if not gate:
        problems.append(
            "report carries no `report_gate`; the cross-layer check had nothing "
            "to verify. Treat this as unobserved, not as consistent."
        )

    # ---- 2c. A gate's verdict and its own evidence must not contradict -----
    # one15 carried a gate item with `status: passed` whose evidence string read
    # `status=failed, program=missing, teams=0, agents=0, tasks=0`. The verdict
    # and the evidence disagree inside a single item, so whichever one a reader
    # trusts, the other one is a lie. This is the same double-evaluation defect
    # seen from the other side: the second pass produced the evidence, the first
    # pass's verdict survived.
    for item in gate.get("gates") or gate.get("items") or []:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("gate")
        status = item.get("status")
        evidence = item.get("evidence") or ""
        if not status or "status=" not in evidence:
            continue
        claimed = evidence.split("status=", 1)[1].split(",", 1)[0].strip()
        # `not_observed` is deliberately not a contradiction: the gate logic
        # explicitly allows it for a focused run, so a `passed` verdict beside
        # `status=not_observed` is the designed behaviour. Flagging it would
        # train the reader to ignore this checker, which is the failure mode
        # that already discredited an earlier monitor.
        if claimed != "not_observed" and claimed != status:
            problems.append(
                f"gate {name} verdict is `{status}` but its own evidence claims "
                f"`status={claimed}`. The verdict and the evidence disagree "
                f"inside one item: {evidence[:160]}"
            )

    # ---- 3. A gate that failed must name a reason the body can show --------
    for item in gate.get("gates") or gate.get("items") or []:
        if not isinstance(item, dict) or item.get("status") != "failed":
            continue
        name = item.get("name") or item.get("gate")
        evidence = item.get("evidence") or ""
        if not evidence.strip():
            problems.append(
                f"gate {name} failed with no evidence; a silent failure cannot "
                "be diagnosed from the report."
            )
    return problems


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    failures = 0
    for path in argv[1:]:
        try:
            problems = check_report(path)
        except (OSError, json.JSONDecodeError) as error:
            print(f"FAIL {path}: {error}")
            failures += 1
            continue
        if problems:
            failures += 1
            print(f"FAIL {path}")
            for problem in problems:
                print(f"  - {problem}")
        else:
            print(f"ok   {path}")
    print(f"\n{len(argv) - 1 - failures}/{len(argv) - 1} reports consistent")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
