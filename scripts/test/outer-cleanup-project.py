#!/usr/bin/env python3
"""Project the durable outer cleanup facts onto a harness report.

This is the owner's own driver: it runs the trusted build's harness binary and
reports a **structured** result. It never decides the verdict by matching
stdout text, and it never executes a program the child named - the harness
binary is the one in the trusted build target.

Usage:
  outer-cleanup-project.py --harness-eval <binary> --harness-report <p>
    --inner-receipt <p> --final-receipt <p> --cleanup-result <p>
    --owner-report <p> --manifest <p> --output <p> [--shutdown-validation <p>]
    [--sealed-key <p>] [--scenario-artifact <p> --scenario-output <p>]
"""
import argparse
import hashlib
import json
import pathlib
import subprocess
import sys


def digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--harness-eval", required=True)
    parser.add_argument("--harness-report", required=True)
    parser.add_argument("--inner-receipt", required=True)
    parser.add_argument("--final-receipt", required=True)
    parser.add_argument("--cleanup-result", required=True)
    parser.add_argument("--owner-report", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--shutdown-validation")
    parser.add_argument("--sealed-key")
    parser.add_argument("--scenario-artifact")
    parser.add_argument("--scenario-output")
    args = parser.parse_args()
    output = pathlib.Path(args.output)
    command = [
        str(args.harness_eval),
        "project-outer-cleanup",
        "--harness-report", args.harness_report,
        "--inner-receipt", args.inner_receipt,
        "--final-receipt", args.final_receipt,
        "--cleanup-result", args.cleanup_result,
        "--owner-report", args.owner_report,
        "--manifest", args.manifest,
        "--output", str(output),
    ]
    if args.shutdown_validation:
        command += ["--shutdown-validation", args.shutdown_validation]
    if not args.sealed_key:
        # The sealed evidence key is named by the owner report itself; the
        # projection verifies the file's digest against the recorded one.
        try:
            owner = json.loads(pathlib.Path(args.owner_report).read_text())
            named = owner.get("encrypted_evidence", {}).get("key_reference")
            if named:
                command += ["--sealed-key", named]
        except Exception:
            pass
    if args.sealed_key:
        command += ["--sealed-key", args.sealed_key]
    if args.scenario_artifact and args.scenario_output:
        command += ["--scenario-artifact", args.scenario_artifact,
                    "--scenario-output", args.scenario_output]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=120)
    status = "failed"
    gate = ""
    reason = ""
    detail = ""
    for line in completed.stdout.splitlines():
        if line.startswith("outer-cleanup-projection: "):
            value = line.split(": ", 1)[1].strip()
            if value == "observed":
                status = "observed"
            else:
                status = "unobserved"
                reason = value
        elif line.startswith("outer-cleanup-projection-detail: "):
            detail = line.split(": ", 1)[1].strip()
        elif line.startswith("outer-cleanup-gate: "):
            gate = line.split(": ", 1)[1].strip()
    output_digest = digest(output) if output.is_file() else None
    if status == "observed" and output_digest is None:
        status = "failed"
        reason = "the projection wrote no output"
    if status == "observed":
        projected = json.loads(output.read_text())
        if projected.get("outer_cleanup_projection", {}).get("status") != "observed":
            status = "failed"
            reason = "the projected report does not carry the observation"
    result = {
        "status": status,
        "exit": completed.returncode,
        "gate": gate,
        "reason": reason,
        "detail": detail,
        "output": str(output),
        "output_sha256": output_digest,
        "stderr_tail": completed.stderr[-1024:],
    }
    print(json.dumps(result))
    # Observation of cleanup and the business gate are independent. Preserve
    # the harness' exit 4 so the owner can record observed-but-business-failed.
    if status == "observed" and completed.returncode in (0, 4):
        return completed.returncode
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
