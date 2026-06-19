#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

REPORT_DIR="${COWD_AI_HARNESS_REPORT_DIR:-target/ai-harness-health}"
REPORT_PATH="${COWD_AI_HARNESS_REPORT_PATH:-$REPORT_DIR/latest.md}"
mkdir -p "$REPORT_DIR"

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
  else
    ended="$(date +%s)"
    duration=$((ended - started))
    ROWS+=("| ${name} | FAIL | ${duration}s | \`${command}\` | \`${log_file}\` |")
    FAILURES=$((FAILURES + 1))
  fi
}

run_check "format-and-whitespace" "cargo fmt --all --check && git diff --check"
run_check "ai-harness-core" "scripts/ci/ai-harness.sh"
run_check "ai-platform-contracts" "cargo test -p ai-agent-spec -p ai-behavior-policy -p ai-harness -p ai-policy --all-targets"
run_check "runtime-full-capability-eval" "cargo test -p runtime --test cowd_full_capability_eval"

if [[ "${COWD_AI_HARNESS_FULL_WORKSPACE:-0}" == "1" ]]; then
  run_check "workspace-all-targets" "cargo test --workspace --all-targets"
fi

if [[ "${COWD_AI_HARNESS_SCENARIO:-0}" == "1" ]]; then
  run_check "scenario-e2e" "scripts/ci/scenario.sh"
else
  ROWS+=("| scenario-e2e | SKIP | 0s | \`scripts/ci/scenario.sh\` | opt-in: set \`COWD_AI_HARNESS_SCENARIO=1\` |")
fi

if [[ "${COWD_AI_HARNESS_LIVE:-0}" == "1" ]]; then
  run_check "live-deep-validation" "scripts/ci/ai-harness-live.sh"
else
  ROWS+=("| live-deep-validation | SKIP | 0s | \`scripts/ci/ai-harness-live.sh\` | opt-in: set \`COWD_AI_HARNESS_LIVE=1\` |")
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
  echo "- Strategy/context/growth/eval crates compile and pass unit tests."
  echo "- Runtime scenario suite passes simple, complex, blocked-finalization, multi-agent adaptation, matrix quality, and growth-signal checks."
  echo "- Memory pulse converts runtime and AI-kernel events into auditable candidates without blocking turn completion."
  echo "- Architecture boundaries remain clean."
  echo "- Full-capability scenario validates document ingestion, memory, fact checking, session persistence, agent evidence, and structured runtime evidence."
  echo "- Scenario E2E validates gateway, session/runtime, memory, tool permission, and skill/matrix surfaces."
  echo "- Live validation is explicitly opt-in and must record the bounded command used."
} >"$REPORT_PATH"

cat "$REPORT_PATH"

if [[ "$FAILURES" -gt 0 ]]; then
  exit 1
fi
