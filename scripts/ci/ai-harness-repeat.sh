#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

COUNT="${COWD_AI_HARNESS_REPEAT:-3}"
REPORT_DIR="${COWD_AI_HARNESS_REPEAT_REPORT_DIR:-target/ai-harness-repeat}"
mkdir -p "$REPORT_DIR"

summary="$REPORT_DIR/summary.tsv"
: >"$summary"

for idx in $(seq 1 "$COUNT"); do
  run_dir="$REPORT_DIR/run-$idx"
  mkdir -p "$run_dir"
  started="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  start_s="$(date +%s)"
  status="PASS"
  if ! env \
    COWD_AI_HARNESS_REPORT_DIR="$run_dir" \
    COWD_AI_HARNESS_FULL_WORKSPACE=0 \
    COWD_AI_HARNESS_SCENARIO=0 \
    COWD_AI_HARNESS_LIVE=0 \
    COWD_AI_HARNESS_REPEAT_ENABLED=0 \
    scripts/ci/ai-harness-health-report.sh >"$run_dir/stdout.log" 2>&1; then
    status="FAIL"
  fi
  end_s="$(date +%s)"
  duration=$((end_s - start_s))
  printf '%s\t%s\t%s\t%s\t%s\n' "$idx" "$status" "$duration" "$started" "$run_dir/latest.json" >>"$summary"
  if [[ "$status" != "PASS" ]]; then
    cat "$run_dir/stdout.log" >&2 || true
    exit 1
  fi
done

python3 - "$summary" "$REPORT_DIR/latest.json" "$COUNT" <<'PY'
import json
import sys

summary_path, output_path, expected_count = sys.argv[1], sys.argv[2], int(sys.argv[3])
runs = []
with open(summary_path, encoding="utf-8") as fh:
    for line in fh:
        idx, status, duration, started, evidence = line.rstrip("\n").split("\t")
        runs.append({
            "run": int(idx),
            "status": status,
            "duration_seconds": int(duration),
            "started": started,
            "evidence": evidence,
        })
payload = {
    "kind": "cowd.ai_harness.repeat_report",
    "status": "PASS" if len(runs) == expected_count and all(r["status"] == "PASS" for r in runs) else "FAIL",
    "expected_runs": expected_count,
    "runs": runs,
}
with open(output_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, ensure_ascii=False, indent=2)
    fh.write("\n")
PY

cat "$REPORT_DIR/latest.json"
