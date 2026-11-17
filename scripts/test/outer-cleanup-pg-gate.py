#!/usr/bin/env python3
"""Real-Postgres, zero-model gate for the outer cleanup projection.

The focused U5 scenarios are decided by two facts that live outside the
harness process: the launcher stops the Gateway and the owner returns the
Postgres container only after the harness exits. This gate proves the
projection that reads those facts back works against a REAL owner run, with
no model call at all:

  1. a child runs inside a real exclusive Postgres owner (scripts/test/pg-owned-run.py),
     writes the harness-side report and inner receipt, and exits with the
     reserved pending exit;
  2. the owner returns the container, the volume, the port and the processes,
     and writes its own durable report;
  3. the outer driver writes the launcher final receipt for that run and runs
     `harness-eval project-outer-cleanup` over the real receipts;
  4. the gate asserts the outer cleanup is observed and the gate is passed.

The harness-side report in this gate is a contract-shaped fixture: this gate
proves the binding and the cleanup path, not the business facts of a live
scenario (those come from the real scenario artifacts).

Usage:
  outer-cleanup-pg-gate.py --harness-eval <binary> [--tree <repo>] [--report <json>]
"""
import argparse
import hashlib
import json
import os
import pathlib
import secrets
import stat
import subprocess
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parents[2]
OWNER = REPO / "scripts" / "test" / "pg-owned-run.py"


def digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def starttime(pid: int) -> str:
    stat_line = (pathlib.Path("/proc") / str(pid) / "stat").read_text()
    return stat_line.rsplit(") ", 1)[1].split()[19]


def child_script(root: pathlib.Path, run_id: str) -> str:
    """The zero-model child: it writes the harness-side facts and exits 75."""
    return f'''
import hashlib, json, os, pathlib, secrets, sys
root = pathlib.Path({str(root)!r})
run_id = {run_id!r}
pid = os.getpid()
stat_line = pathlib.Path("/proc/self/stat").read_text()
started = stat_line.rsplit(") ", 1)[1].split()[19]
binding = {{
    "candidate": "1a2918059affc324393cdb02acab50da3fbeec6b",
    "harness_sha256": "sha256:gate-zero-model",
    "image_sha256": "sha256:gate-zero-model",
    "namespace": "gate:" + root.name,
    "nonce": secrets.token_hex(32),
    "pid": pid,
    "source_sha256": "sha256:gate-zero-model",
    "starttime": int(started),
}}
scenario = {{"scenario_id": "live_u5_active_cancel_isolation", "status": "pending_outer_cleanup",
  "acceptance": {{"passed": False, "checks": {{"internal_passed": True, "outer_cleanup_observed": False}}}},
  "outer_cleanup": "not_observed",
  "sessions": [
    {{"session_id": "gate-session-left", "turn_id": "gate-turn-left", "execution_id": "gate-execution-left"}},
    {{"session_id": "gate-session-right", "turn_id": "gate-turn-right", "execution_id": "gate-execution-right"}}
  ]}}
report = {{
    "kind": "harness_eval.u5_internal",
    "level": "deep",
    "authorized_real_model": False,
    "status": "pending_outer_cleanup",
    "run_id": run_id,
    "u5_host": binding,
    "u5_usage": {{"physical_calls": 0, "total_tokens": 0}},
    "live_gateway_scenarios": {{"status": "pending_outer_cleanup", "claim_status": "pending_outer_cleanup",
      "claim_scope": "focused", "health_status": "passed", "selection_status": "passed",
      "selected_scenario_ids": ["live_u5_active_cancel_isolation"], "passed": 0, "failed": 0,
      "scenarios": [scenario]}},
    "live_gateway_scenario_details": {{"scenarios": [scenario]}},
    "scenarios": [{{"capability": "live_u5_active_cancel_isolation", "status": "pending_outer_cleanup"}}],
}}
report_path = root / "harness-report.json"
report_path.write_text(json.dumps(report, indent=1))
inner = {{"schema_version": 1, "disposition": "pending_outer_cleanup", "run_id": run_id,
  "owner": binding, "report_path": str(report_path),
  "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
  "scenario": "live_u5_active_cancel_isolation", "usage": report["u5_usage"]}}
(root / "u5-inner-receipt.json").write_text(json.dumps(inner, indent=1))
# The launcher stand-in: the cleanup result and the final receipt are the
# launcher's own durable facts, produced during its cleanup.
cleanup = {{"business_exit": 75, "cleanup_failed": False, "gateway_wait_status": "0", "remaining_owned": []}}
(root / "cleanup-result.json").write_text(json.dumps(cleanup, indent=1))
final = {{"schema_version": 1, "status": "passed", "scope": "focused_U5_C_local_only",
  "original_harness_exit": 75, "run_id": run_id, "owner": binding,
  "inner_sha256": hashlib.sha256((root / "u5-inner-receipt.json").read_bytes()).hexdigest(),
  "report_sha256": hashlib.sha256(report_path.read_bytes()).hexdigest(),
  "cleanup_sha256": hashlib.sha256((root / "cleanup-result.json").read_bytes()).hexdigest(),
  "usage": report["u5_usage"]}}
(root / "u5-final-acceptance.v1.json").write_text(json.dumps(final, indent=1))
# The manifest names the durable receipts; the owner projects them after its
# own natural cleanup. The manifest carries paths only, never a verdict.
manifest_path = os.environ.get("COWD_OUTER_CLEANUP_MANIFEST")
if manifest_path:
    manifest = {{
        "harness_run_id": run_id,
        "scenario": "live_u5_active_cancel_isolation",
        "owner_run_id": os.environ.get("COWD_OWNER_RUN_ID", ""),
        "owner_nonce": os.environ.get("COWD_OWNER_NONCE", ""),
        "harness_report": str(report_path),
        "inner_receipt": str(root / "u5-inner-receipt.json"),
        "final_receipt": str(root / "u5-final-acceptance.v1.json"),
        "cleanup_result": str(root / "cleanup-result.json"),
        "output": str(root.parent / "outer-cleanup-projection.json"),
        "scenario_output": str(root.parent / "outer-cleanup-projection.scenario.json"),
    }}
    path = pathlib.Path(manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as out:
        json.dump(manifest, out)
        out.flush()
        os.fsync(out.fileno())
sys.stderr.write("gate child wrote the harness-side facts\\n")
# The real launcher exits 0 once it has written the final receipt; the owner
# sees a successful child, not the harness' reserved pending exit.
sys.exit(0)
'''


def canonical_reread(harness_eval, projected):
    """The canonical read of the persisted projection must agree with it."""
    completed = subprocess.run(
        [str(harness_eval), "canonical-gate", "--report", str(projected)],
        capture_output=True,
        text=True,
    )
    status = ""
    for line in completed.stdout.splitlines():
        if line.startswith("canonical-gate-status: "):
            status = line.split(": ", 1)[1].strip()
    stored = ""
    if projected.is_file():
        stored = json.loads(projected.read_text()).get("report_gate", {}).get("status", "")
    return {"stored": stored, "canonical": status, "matches_stored": bool(stored) and stored == status}


def negative_controls(harness_eval, harness_report, inner_receipt, final_receipt,
                      cleanup_result, owner_report, work, manifest_path=None):
    """The negative controls, run against the receipts of the real owner run.

    The Postgres run is real; the negatives mutate the projection inputs so
    that a missing, foreign or unclean outer fact must be refused.
    """
    cases = []

    def run(name, args):
        completed = subprocess.run(
            [str(harness_eval), "project-outer-cleanup", *args],
            capture_output=True,
            text=True,
        )
        reason = ""
        for line in completed.stdout.splitlines():
            if line.startswith("outer-cleanup-projection: "):
                reason = line.split(": ", 1)[1].strip()
        cases.append({"case": name, "exit": completed.returncode, "reason": reason})

    def base_args(name):
        return [
            "--harness-report", str(harness_report),
            "--inner-receipt", str(inner_receipt),
            "--final-receipt", str(final_receipt),
            "--cleanup-result", str(cleanup_result),
            "--owner-report", str(owner_report),
            "--manifest", str(manifest_path),
            "--output", str(work / ("negative-%s.json" % name.replace(" ", "-"))),
        ]

    without = lambda args, value: [x for x in args if x != value]
    run("missing owner report", without(base_args("missing owner report"), str(owner_report)))
    run("missing cleanup result", without(base_args("missing cleanup result"), str(cleanup_result)))
    foreign = work / "foreign-final-receipt.json"
    receipt = json.loads(final_receipt.read_text())
    receipt["run_id"] = "another-run"
    foreign.write_text(json.dumps(receipt))
    run("wrong run", [x if x != str(final_receipt) else str(foreign) for x in base_args("wrong run")])
    foreign_owner = work / "foreign-owner-receipt.json"
    receipt = json.loads(final_receipt.read_text())
    receipt["owner"] = dict(receipt["owner"])
    receipt["owner"]["nonce"] = "0" * 64
    foreign_owner.write_text(json.dumps(receipt))
    run("wrong owner", [x if x != str(final_receipt) else str(foreign_owner) for x in base_args("wrong owner")])
    dirty_owner = work / "dirty-owner-report.json"
    owner = json.loads(owner_report.read_text())
    owner["remaining_processes"] = [{"pid": 4242}]
    dirty_owner.write_text(json.dumps(owner))
    run("resources not returned",
        [x if x != str(owner_report) else str(dirty_owner) for x in base_args("resources not returned")])
    return cases


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--harness-eval", required=True)
    parser.add_argument("--tree", default=str(REPO))
    parser.add_argument("--report", type=pathlib.Path)
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--evidence-dir")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    tree = pathlib.Path(args.tree).resolve()
    harness_eval = pathlib.Path(args.harness_eval).resolve()
    owner = tree / "scripts" / "test" / "pg-owned-run.py"
    if not owner.is_file():
        print(f"owner script missing: {owner}", file=sys.stderr)
        return 2
    parent = pathlib.Path(f"/run/user/{os.getuid()}")
    if parent.stat().st_uid != os.getuid() or stat.S_IMODE(parent.stat().st_mode) != 0o700:
        print("runtime parent must be owned and actually 0700", file=sys.stderr)
        return 2
    # The gate's own run root lives on the data disk: /run/user/1000 is
    # reclaimed by other lines and the receipts must survive the run.
    data_root = pathlib.Path("/media/yi/Datas/workspace/.cowd-build/round1385h-outer")
    data_root.mkdir(parents=True, exist_ok=True)
    root = pathlib.Path(tempfile.mkdtemp(prefix="cowd-outer-gate-", dir=data_root))
    os.chmod(root, 0o700)
    run_id = f"outer-gate-{secrets.token_hex(6)}"
    script = root / "gate-child.py"
    script.write_text(child_script(root, run_id))
    os.chmod(script, 0o700)
    owner_report = root / "owner-report.json"
    port = args.port
    if not port:
        import socket
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
    environment = dict(os.environ)
    environment.pop("COWD_TEST_POSTGRES_URL", None)
    environment.pop("COWD_TEST_POSTGRES_TARGET_URL", None)
    # The manifest is per run and lives on a POSIX filesystem, because the
    # owner requires an owned 0600 regular file: the data disk is a fuse mount
    # that does not carry POSIX modes.
    manifest_parent = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))
    manifest_parent.mkdir(parents=True, exist_ok=True)
    manifest = manifest_parent / ("outer-cleanup-manifest-%s.json" % run_id)
    environment["COWD_OUTER_CLEANUP_MANIFEST"] = str(manifest)
    # The owner runs a fixed, operator-trusted harness binary; the child never
    # names it.
    environment["COWD_HARNESS_EVAL_BINARY"] = str(harness_eval)
    environment["COWD_OUTER_CLEANUP_HARNESS_EVAL"] = str(harness_eval)
    completed = subprocess.run(
        [
            sys.executable,
            str(owner),
            "--run-id",
            run_id,
            "--port",
            str(port),
            "--report",
            str(owner_report),
            "--seconds",
            str(args.seconds),
            "--",
            sys.executable,
            str(script),
        ],
        cwd=str(tree),
        env=environment,
        capture_output=True,
        text=True,
    )
    stdout = completed.stdout[-4000:]
    stderr = completed.stderr[-4000:]
    print(f"owner exit={completed.returncode}")
    if completed.returncode not in (0, 75):
        print(stdout)
        print(stderr, file=sys.stderr)
        return 1
    if not owner_report.is_file():
        print("owner report missing", file=sys.stderr)
        return 1
    owner_facts = json.loads(owner_report.read_text())
    harness_report = root / "harness-report.json"
    inner_receipt = root / "u5-inner-receipt.json"
    binding = json.loads(inner_receipt.read_text())["owner"]
    # The child wrote the launcher stand-in receipts (the cleanup result and
    # the final receipt) during its own cleanup; the driver never rewrites
    # them.
    final_receipt = root / "u5-final-acceptance.v1.json"
    # Per run: two runs never publish into the same file.
    projected = root / "outer-cleanup-projection.json"
    owner_facts = json.loads(owner_report.read_text()) if owner_report.is_file() else {}
    # The owner report is final; the projection outcome is a sibling artefact
    # that binds it by digest.
    automatic_path = owner_report.with_name(owner_report.name + ".outer-cleanup.json")
    automatic = json.loads(automatic_path.read_text()) if automatic_path.is_file() else {}
    # The owner projected the outer cleanup by itself, after its own natural
    # cleanup; nothing outside the owner ran the projection for it. The run
    # below only repeats it for the negative controls.
    projection = subprocess.run(
        [
            str(harness_eval),
            "project-outer-cleanup",
            "--harness-report",
            str(harness_report),
            "--inner-receipt",
            str(inner_receipt),
            "--final-receipt",
            str(final_receipt),
            "--cleanup-result",
            str(root / "cleanup-result.json"),
            "--owner-report",
            str(owner_report),
            "--manifest",
            str(manifest),
            "--output",
            str(projected),
        ],
        capture_output=True,
        text=True,
    )
    print("owner automatic projection: " + json.dumps(automatic, ensure_ascii=False)[:400])
    print(projection.stdout.strip())
    if projection.stderr.strip():
        print(projection.stderr.strip(), file=sys.stderr)
    # The receipts are evidence: copy them out of the run root immediately, so
    # a concurrent space reclamation cannot take them away.
    evidence_dir = pathlib.Path(args.evidence_dir) if args.evidence_dir else None
    if evidence_dir:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        for name in ("owner-report.json", "owner-report.json.outer-cleanup.json",
                     "harness-report.json", "u5-inner-receipt.json",
                     "u5-final-acceptance.v1.json", "cleanup-result.json",
                     "outer-cleanup-manifest.json"):
            source = root / name
            if source.is_file():
                (evidence_dir / name).write_bytes(source.read_bytes())
    negatives = negative_controls(harness_eval, harness_report, inner_receipt, final_receipt,
                                  root / "cleanup-result.json", owner_report, root, manifest)
    canonical = canonical_reread(harness_eval, projected)
    verdict = json.loads(projected.read_text()) if projected.is_file() else {}
    scenario = (
        verdict.get("live_gateway_scenarios", {}).get("scenarios", [{}])[0]
        if verdict.get("live_gateway_scenarios", {}).get("scenarios")
        else {}
    )
    gate = verdict.get("report_gate", {})
    # This gate proves the OUTER projection against a real owner run. The
    # fixture has no live scenario, so the five business gate items are not
    # proven here; what must hold is that the outer cleanup was observed and
    # consumed, and that the report is no longer stuck on the outer cleanup.
    observed = (
        automatic.get("status") == "observed"
        and canonical.get("matches_stored") is True
        and verdict.get("outer_cleanup_projection", {}).get("status") == "observed"
        and scenario.get("acceptance", {}).get("checks", {}).get("outer_cleanup_observed")
        is True
        and gate.get("status") != "pending_outer_cleanup"
        and all(case["exit"] == 3 for case in negatives)
    )
    owner_cleanup = {
        "cleanup_errors": owner_facts.get("cleanup_errors"),
        "remaining_processes": owner_facts.get("remaining_processes"),
        "remaining_db_clients": owner_facts.get("remaining_db_clients"),
        "container_stopped": owner_facts.get("container_stopped"),
        "PG_process_returned": owner_facts.get("PG_process_returned"),
        "port_bindable": owner_facts.get("port_bindable"),
        "owned_harness_process": any(
            entry.get("pid") == binding["pid"]
            and str(entry.get("starttime")) == str(binding["starttime"])
            for entry in owner_facts.get("owned_processes", [])
        ),
    }
    summary = {
        "run_id": run_id,
        "owner_exit": completed.returncode,
        "projection_exit": projection.returncode,
        "observed": observed,
        "automatic_projection": automatic,
        "negative_controls": negatives,
        "canonical_reread": canonical,
        "gate_status": gate.get("status"),
        "gate_items": [
            {"name": item.get("name"), "status": item.get("status")}
            for item in gate.get("items", [])
        ],
        "gate_scope": "the outer cleanup projection only; the business gate items need a live scenario and are not proven by this fixture",
        "owner_cleanup": owner_cleanup,
        "owner_report": str(owner_report),
        "outer_cleanup_record": str(automatic_path),
        "projected_report": str(projected),
    }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))
    return 0 if observed else 1


if __name__ == "__main__":
    raise SystemExit(main())
