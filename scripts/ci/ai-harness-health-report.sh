#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

REPORT_DIR="${COWD_AI_HARNESS_REPORT_DIR:-target/ai-harness-health}"
REPORT_PATH="${COWD_AI_HARNESS_REPORT_PATH:-$REPORT_DIR/latest.md}"
JSON_REPORT_PATH="${COWD_AI_HARNESS_JSON_REPORT_PATH:-$REPORT_DIR/latest.json}"
mkdir -p "$REPORT_DIR"
ROWS_TSV="$REPORT_DIR/lanes.tsv"
: >"$ROWS_TSV"

STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
GIT_REV="$(git rev-parse --short HEAD)"
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

declare -a ROWS=()
FAILURES=0

run_check() {
  local name="$1"
  local command="$2"
  local log_file="$REPORT_DIR/${name//[^A-Za-z0-9_.-]/_}.log"
  local started
  local ended
  local duration
  started="$(date +%s)"
  if bash -lc "$command" >"$log_file" 2>&1; then
    ended="$(date +%s)"
    duration=$((ended - started))
    ROWS+=("| ${name} | PASS | ${duration}s | \`${command}\` | \`${log_file}\` |")
    printf '%s\t%s\t%s\t%s\t%s\n' "$name" "PASS" "$duration" "$command" "$log_file" >>"$ROWS_TSV"
  else
    ended="$(date +%s)"
    duration=$((ended - started))
    ROWS+=("| ${name} | FAIL | ${duration}s | \`${command}\` | \`${log_file}\` |")
    printf '%s\t%s\t%s\t%s\t%s\n' "$name" "FAIL" "$duration" "$command" "$log_file" >>"$ROWS_TSV"
    FAILURES=$((FAILURES + 1))
  fi
}

run_check "format-and-whitespace" "cargo fmt --all --check && git diff --check"
run_check "ai-harness-core" "scripts/ci/ai-harness.sh"
run_check "mission-harness-quick-eval" "cargo run -p harness-eval -- quick"
run_check "runtime-lib-tests" "cargo test -p runtime --lib"
run_check "tool-closure" "cargo test -p tools --test ai_harness_tool_closure"
run_check "provider-failure-classification" "cargo test -p provider --test provider_failure_classification"

if [[ "${COWD_AI_HARNESS_FULL_WORKSPACE:-0}" == "1" ]]; then
  run_check "workspace-all-targets" "cargo test --workspace --exclude gateway --all-targets && cargo test -p gateway --all-targets -- --test-threads=1"
fi

if [[ "${COWD_AI_HARNESS_SCENARIO:-0}" == "1" ]]; then
  run_check "scenario-e2e" "scripts/ci/scenario.sh"
else
  ROWS+=("| scenario-e2e | SKIP | 0s | \`scripts/ci/scenario.sh\` | opt-in: set \`COWD_AI_HARNESS_SCENARIO=1\` |")
  printf '%s\t%s\t%s\t%s\t%s\n' "scenario-e2e" "SKIP" "0" "scripts/ci/scenario.sh" "opt-in: set COWD_AI_HARNESS_SCENARIO=1" >>"$ROWS_TSV"
fi

if [[ "${COWD_AI_HARNESS_LIVE:-0}" == "1" ]]; then
  run_check "live-deep-validation" "scripts/ci/ai-harness-live.sh"
else
  ROWS+=("| live-deep-validation | SKIP | 0s | \`scripts/ci/ai-harness-live.sh\` | opt-in: set \`COWD_AI_HARNESS_LIVE=1\` |")
  printf '%s\t%s\t%s\t%s\t%s\n' "live-deep-validation" "SKIP" "0" "scripts/ci/ai-harness-live.sh" "opt-in: set COWD_AI_HARNESS_LIVE=1" >>"$ROWS_TSV"
fi

if [[ "${COWD_AI_HARNESS_REPEAT_ENABLED:-0}" == "1" ]]; then
  run_check "health-repeat" "COWD_AI_HARNESS_REPEAT=${COWD_AI_HARNESS_REPEAT:-3} scripts/ci/ai-harness-repeat.sh"
else
  ROWS+=("| health-repeat | SKIP | 0s | \`scripts/ci/ai-harness-repeat.sh\` | opt-in: set \`COWD_AI_HARNESS_REPEAT_ENABLED=1\` |")
  printf '%s\t%s\t%s\t%s\t%s\n' "health-repeat" "SKIP" "0" "scripts/ci/ai-harness-repeat.sh" "opt-in: set COWD_AI_HARNESS_REPEAT_ENABLED=1" >>"$ROWS_TSV"
fi

ENDED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
OVERALL="PASS"
if [[ "$FAILURES" -gt 0 ]]; then
  OVERALL="FAIL"
fi

{
  echo "# AI Harness Health Report"
  echo
  echo "- Status: ${OVERALL}"
  echo "- Branch: ${GIT_BRANCH}"
  echo "- Revision: ${GIT_REV}"
  echo "- Started: ${STARTED_AT}"
  echo "- Finished: ${ENDED_AT}"
  echo "- Failed checks: ${FAILURES}"
  echo
  echo "## Capability Lanes"
  echo
  echo "| Lane | Status | Duration | Command | Evidence |"
  echo "| --- | --- | ---: | --- | --- |"
  printf '%s\n' "${ROWS[@]}"
  echo
  echo "## Health Criteria"
  echo
  echo "- AI kernel, eval, runtime, tools, and provider checks compile and pass the current core tests."
  echo "- Mission Harness quick eval validates mission/session/team/agent/steward/recovery deterministic core loops."
  echo "- Runtime lib tests cover the current in-crate mission, agent, steward, recovery, and policy contracts."
  echo "- Architecture boundaries remain clean."
  echo "- Full-capability scenario validates document ingestion, memory, fact checking, session persistence, agent evidence, and structured runtime evidence."
  echo "- Scenario E2E validates gateway, session/runtime, memory, tool permission, and skill/matrix surfaces."
  echo "- Tool closure validates readonly execution, write denial, and readonly batch isolation."
  echo "- Provider failure classification validates retryability, safe failure classes, and request-id preservation."
  echo "- Live validation is explicitly opt-in and must record the bounded command used."
  echo
  echo "## Manual"
  echo
  echo "- See \`docs/ai-harness-testing.md\` for commands, layers, and acceptance rules."
} >"$REPORT_PATH"

python3 - "$ROWS_TSV" "$JSON_REPORT_PATH" "$OVERALL" "$GIT_BRANCH" "$GIT_REV" "$STARTED_AT" "$ENDED_AT" "$FAILURES" <<'PY'
import json
import sys

rows_tsv, report_path, status, branch, revision, started, finished, failures = sys.argv[1:9]
lanes = []
with open(rows_tsv, encoding="utf-8") as fh:
    for line in fh:
        name, lane_status, duration, command, evidence = line.rstrip("\n").split("\t")
        lanes.append({
            "lane": name,
            "status": lane_status,
            "duration_seconds": int(duration),
            "command": command,
            "evidence": evidence,
        })

payload = {
    "kind": "cowd.ai_harness.health_report",
    "status": status,
    "branch": branch,
    "revision": revision,
    "started": started,
    "finished": finished,
    "failed_checks": int(failures),
    "lanes": lanes,
}
with open(report_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, ensure_ascii=False, indent=2)
    fh.write("\n")
PY

cat "$REPORT_PATH"

if [[ "$FAILURES" -gt 0 ]]; then
  exit 1
fi
