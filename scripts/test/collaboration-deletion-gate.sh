#!/usr/bin/env bash
set -euo pipefail

# P6 residual gate for the 0821 CollaborationProgram migration. The patterns
# are exact retired authority symbols/encodings, not generic words such as
# "cost" or "role", which remain valid in unrelated technical algorithms.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

failures=0
fail() {
  printf 'collaboration deletion gate: %s\n' "$*" >&2
  failures=$((failures + 1))
}

expect_absent() {
  local description="$1"
  local pattern="$2"
  shift 2
  local path
  for path in "$@"; do
    if [[ ! -e "$path" ]]; then
      fail "$description (scan root is missing: $path)"
      return
    fi
  done
  local matches
  if matches="$(rg -n --glob '*.rs' -P "$pattern" "$@")"; then
    printf '%s\n' "$matches" >&2
    fail "$description"
  else
    local status=$?
    if [[ "$status" -ne 1 ]]; then
      fail "$description (search failed with status $status)"
    fi
  fi
}

expect_absent \
  'retired Host direct orchestration entry point returned' \
  'start_selected_strategy' \
  crates/runtime/src
expect_absent \
  'retired hand-written builtin Team selection summary returned' \
  'builtin_team_template_summaries' \
  crates/runtime/src
expect_absent \
  'retired role/slot runtime string dispatch returned' \
  '(?:contains|strip_prefix|starts_with|split_once|==|match)\([^\n]*(?:role_slot:|team_role:)|(?:role_slot:|team_role:)[^\n]*(?:contains|strip_prefix|starts_with|split_once|==|match)' \
  crates/runtime/src crates/harness-contract/src
expect_absent \
  'retired monetary model-pricing contract returned' \
  'ModelPricing|UsageCostEstimate|estimated_cost_usd|_cost_microusd|max_cost' \
  crates

if [[ "$failures" -ne 0 ]]; then
  exit 1
fi

echo 'collaboration deletion gate passed'
