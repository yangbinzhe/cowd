#!/usr/bin/env python3
"""Run frozen workloads and compare robust performance summaries."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import time

import yaml


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def summary(samples: list[float]) -> dict[str, float | int]:
    median = statistics.median(samples)
    deviations = [abs(value - median) for value in samples]
    mad = statistics.median(deviations)
    return {
        "samples": len(samples),
        "median": median,
        "p95": percentile(samples, 0.95),
        "p99": percentile(samples, 0.99),
        "mad": mad,
        "ci95_low": median - 1.96 * mad / math.sqrt(len(samples)),
        "ci95_high": median + 1.96 * mad / math.sqrt(len(samples)),
    }


def load_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def git(root: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", *arguments], cwd=root, text=True).strip()


def worktree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    digest.update(subprocess.check_output(["git", "diff", "--binary", "HEAD", "--"], cwd=root))
    untracked = git(root, "ls-files", "--others", "--exclude-standard").splitlines()
    for relative in sorted(filter(None, untracked)):
        digest.update(relative.encode())
        digest.update(b"\0")
        path = root / relative
        if path.is_file():
            digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def run_workload(root: Path, workload: dict, warmups: int, runs: int) -> dict:
    command = workload["command"]
    environment = os.environ.copy()
    environment.update({str(key): str(value) for key, value in workload.get("env", {}).items()})
    for _ in range(warmups):
        completed = subprocess.run(
            command, cwd=root, env=environment, shell=True, text=True, capture_output=True
        )
        if completed.returncode:
            raise SystemExit(completed.stdout[-4000:] + completed.stderr[-4000:])
    samples: list[float] = []
    for _ in range(runs):
        started = time.perf_counter_ns()
        completed = subprocess.run(
            command, cwd=root, env=environment, shell=True, text=True, capture_output=True
        )
        if completed.returncode:
            raise SystemExit(completed.stdout[-4000:] + completed.stderr[-4000:])
        samples.append((time.perf_counter_ns() - started) / 1_000_000_000)
    return {
        "id": workload["id"],
        "unit": "seconds",
        "direction": workload["direction"],
        "command": command,
        "summary": summary(samples),
        "raw": samples,
    }


def run(args: argparse.Namespace) -> None:
    root = Path.cwd()
    manifest_path = Path(args.manifest)
    manifest = load_yaml(manifest_path)
    status = git(root, "status", "--porcelain").splitlines()
    allowed = manifest["environment"].get("allowed_dirty_prefixes", [])
    unexpected = [
        line
        for line in status
        if not any(line.split(maxsplit=1)[-1].startswith(prefix) for prefix in allowed)
    ]
    if manifest["environment"].get("require_clean_worktree") and unexpected and args.mode == "baseline":
        raise SystemExit(f"performance run has unexpected dirty paths: {unexpected}")
    workloads = []
    for entry in manifest["workloads"]:
        workload = json.loads((manifest_path.parent / entry["file"]).read_text(encoding="utf-8"))
        workload["direction"] = entry["direction"]
        workloads.append(
            run_workload(root, workload, manifest["warmup_runs"], manifest["runs"])
        )
    report = {
        "schema_version": 1,
        "mode": args.mode,
        "head": git(root, "rev-parse", "HEAD"),
        "tree": git(root, "rev-parse", "HEAD^{tree}"),
        "worktree_digest": worktree_digest(root),
        "governance_dirty_paths": status,
        "workloads": workloads,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(output)


def compare(args: argparse.Namespace) -> None:
    baseline = json.loads(Path(args.baseline).read_text(encoding="utf-8"))
    candidate = json.loads(Path(args.candidate).read_text(encoding="utf-8"))
    current = {item["id"]: item for item in candidate["workloads"]}
    failures = []
    for previous in baseline["workloads"]:
        actual = current[previous["id"]]
        old = previous["summary"]["median"]
        new = actual["summary"]["median"]
        change = (old - new) / old * 100
        if actual["direction"] == "higher_is_better":
            change = -change
        print(f"{previous['id']}: {change:+.2f}%")
        if change < -float(args.max_regression):
            failures.append(previous["id"])
    if failures:
        raise SystemExit(f"performance regression: {', '.join(failures)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    runner = commands.add_parser("run")
    runner.add_argument("--manifest", required=True)
    runner.add_argument("--mode", choices=["baseline", "candidate"], required=True)
    runner.add_argument("--output", required=True)
    comparator = commands.add_parser("compare")
    comparator.add_argument("--baseline", required=True)
    comparator.add_argument("--candidate", required=True)
    comparator.add_argument("--max-regression", default="5")
    args = parser.parse_args()
    if args.command == "run":
        run(args)
    else:
        compare(args)


if __name__ == "__main__":
    main()
