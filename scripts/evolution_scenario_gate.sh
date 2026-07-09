#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 2
  fi
}

require_cmd curl
require_cmd jq
require_cmd python3

RUN_ID="${COWD_EVOLUTION_SCENARIO_RUN_ID:-$(date +%Y%m%d%H%M%S)}"
REPORT_DIR="${COWD_EVOLUTION_SCENARIO_REPORT_DIR:-$ROOT/reports/evolution-terminal/$RUN_ID}"
RESPONSES_DIR="$REPORT_DIR/responses"
mkdir -p "$RESPONSES_DIR"

BIN="${COWD_BIN:-$ROOT/target/debug/cowd}"
if [[ -z "${COWD_BIN:-}" && "${COWD_EVOLUTION_SCENARIO_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build -p cli
elif [[ ! -x "$BIN" ]]; then
  cargo build -p cli
fi

CONFIG_HOME="$(mktemp -d /tmp/cowd-evolution-scenario.XXXXXX)"
PORT="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
BASE_URL="http://127.0.0.1:$PORT"
GATEWAY_PID=""

cleanup() {
  if [[ -n "${GATEWAY_PID:-}" ]] && kill -0 "$GATEWAY_PID" >/dev/null 2>&1; then
    kill "$GATEWAY_PID" >/dev/null 2>&1 || true
    wait "$GATEWAY_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$CONFIG_HOME"
}
trap cleanup EXIT

cat >"$CONFIG_HOME/config.yaml" <<YAML
gateway:
  enabled: true
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: $PORT
YAML

COWD_CONFIG_HOME="$CONFIG_HOME" "$BIN" gateway run >"$REPORT_DIR/gateway.log" 2>&1 &
GATEWAY_PID="$!"

for _ in $(seq 1 80); do
  if curl -sf "$BASE_URL/api/evolution/missions/summary" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

curl -sf "$BASE_URL/api/evolution/missions/summary" >"$RESPONSES_DIR/00-initial-missions.json"

post_json() {
  local path="$1"
  local payload="$2"
  curl -sf -H 'content-type: application/json' -d "$payload" "$BASE_URL$path"
}

signal_payload() {
  local signal_type="$1"
  local severity="$2"
  local summary="$3"
  local action="$4"
  local evidence="$5"
  jq -n \
    --arg signal_type "$signal_type" \
    --arg severity "$severity" \
    --arg summary "$summary" \
    --arg action "$action" \
    --arg evidence "$evidence" \
    '{
      signal_type: $signal_type,
      source: {
        owner: "runtime",
        session_id: "evolution-scenario-session",
        agent_id: "evolution-scenario-agent",
        team_id: "evolution-scenario-team",
        run_id: "evolution-scenario-run"
      },
      evidence_refs: [$evidence],
      severity: $severity,
      summary: $summary,
      suggested_action: $action,
      immediate_task_can_continue: true
    }'
}

create_signal() {
  local signal_type="$1"
  local severity="$2"
  local summary="$3"
  local action="$4"
  local evidence="$5"
  local output="$6"
  post_json "/api/evolution/signals" "$(signal_payload "$signal_type" "$severity" "$summary" "$action" "$evidence")" >"$output"
}

create_signal "memory_noise" "warning" \
  "scenario: memory packet included unrelated cross-project facts" \
  "tighten scope and salience gates before context injection" \
  "memory:pollution" \
  "$RESPONSES_DIR/01-memory-signal.json"
MEMORY_SIGNAL_ID="$(jq -r '.signal.signal_id' "$RESPONSES_DIR/01-memory-signal.json")"

post_json "/api/evolution/proposals" "$(jq -n --arg sid "$MEMORY_SIGNAL_ID" '{signal_ids:[$sid]}')" >"$RESPONSES_DIR/02-memory-proposal.json"
PROPOSAL_ID="$(jq -r '.proposal.proposal_id' "$RESPONSES_DIR/02-memory-proposal.json")"
MISSION_ID="$(jq -r '.proposal.mission_id' "$RESPONSES_DIR/02-memory-proposal.json")"
post_json "/api/evolution/proposals/$PROPOSAL_ID/candidates" '{"baseline_ref":"baseline:scenario","candidate_ref":"candidate:scenario"}' >"$RESPONSES_DIR/03-memory-candidate.json"
CANDIDATE_ID="$(jq -r '.candidate.candidate_id' "$RESPONSES_DIR/03-memory-candidate.json")"
post_json "/api/evolution/candidates/$CANDIDATE_ID/run" '{}' >"$RESPONSES_DIR/04-memory-run.json"
post_json "/api/evolution/candidates/$CANDIDATE_ID/evaluate" '{}' >"$RESPONSES_DIR/05-memory-comparison.json"
post_json "/api/evolution/candidates/$CANDIDATE_ID/promote" '{}' >"$RESPONSES_DIR/06-memory-promotion.json"
VERSION_ID="$(jq -r '.promotion.version_record.version_id' "$RESPONSES_DIR/06-memory-promotion.json")"
curl -sf "$BASE_URL/api/evolution/active-capabilities" >"$RESPONSES_DIR/06a-active-before-rollback.json"
curl -G -sf \
  --data-urlencode "intent=请分析自我进化 runtime policy" \
  --data-urlencode "detail=summary" \
  "$BASE_URL/api/runtime/capabilities" >"$RESPONSES_DIR/06b-runtime-capabilities.json"
curl -G -sf \
  --data-urlencode "task=请分析自我进化 runtime policy" \
  "$BASE_URL/api/evolution/memory/activation" >"$RESPONSES_DIR/06c-memory-activation.json"
post_json "/api/evolution/versions/$VERSION_ID/rollback" '{}' >"$RESPONSES_DIR/07-memory-rollback.json"
curl -sf "$BASE_URL/api/evolution/active-capabilities" >"$RESPONSES_DIR/07a-active-after-rollback.json"
curl -sf "$BASE_URL/api/evolution/memory" >"$RESPONSES_DIR/08-evolution-memory.json"
curl -sf "$BASE_URL/api/evolution/missions/$MISSION_ID/detail" >"$RESPONSES_DIR/09-memory-mission-detail.json"

declare -A EXPECTED_KIND=(
  [low_novelty_tool_loop]=runtime_policy
  [missing_tool_capability]=tool_contract
  [memory_noise]=memory_governance
  [agent_failure_pattern]=team_template
  [recovery_gap]=runtime_policy
  [eval_failure]=eval_scenario
  [slow_progress]=runtime_policy
  [context_pressure]=context_policy
)

: >"$RESPONSES_DIR/10-signal-kind-matrix.jsonl"
for signal_type in low_novelty_tool_loop missing_tool_capability memory_noise agent_failure_pattern recovery_gap eval_failure slow_progress context_pressure; do
  severity="warning"
  if [[ "$signal_type" == "eval_failure" ]]; then
    severity="critical"
  fi
  create_signal "$signal_type" "$severity" \
    "scenario matrix for $signal_type" \
    "select terminal typed candidate for $signal_type" \
    "matrix:$signal_type" \
    "$RESPONSES_DIR/matrix-$signal_type-signal.json"
  sid="$(jq -r '.signal.signal_id' "$RESPONSES_DIR/matrix-$signal_type-signal.json")"
  post_json "/api/evolution/proposals" "$(jq -n --arg sid "$sid" '{signal_ids:[$sid]}')" >"$RESPONSES_DIR/matrix-$signal_type-proposal.json"
  pid="$(jq -r '.proposal.proposal_id' "$RESPONSES_DIR/matrix-$signal_type-proposal.json")"
  post_json "/api/evolution/proposals/$pid/candidates" '{"baseline_ref":"baseline:matrix","candidate_ref":"candidate:matrix"}' >"$RESPONSES_DIR/matrix-$signal_type-candidate.json"
  jq -n \
    --arg signal_type "$signal_type" \
    --arg expected "${EXPECTED_KIND[$signal_type]}" \
    --slurpfile proposal "$RESPONSES_DIR/matrix-$signal_type-proposal.json" \
    --slurpfile candidate "$RESPONSES_DIR/matrix-$signal_type-candidate.json" \
    '{
      signal_type: $signal_type,
      expected_candidate_kind: $expected,
      root_cause: $proposal[0].diagnosis.root_cause_kind,
      proposal_kind: $proposal[0].proposal.kind,
      goal_ids: $proposal[0].proposal.goal_ids,
      candidate_kind: $candidate[0].candidate.kind,
      adapter: $candidate[0].candidate.promotion_adapter,
      pass: ($candidate[0].candidate.kind == $expected)
    }' >>"$RESPONSES_DIR/10-signal-kind-matrix.jsonl"
done
jq -s '.' "$RESPONSES_DIR/10-signal-kind-matrix.jsonl" >"$REPORT_DIR/signal-kind-matrix.json"

jq -n \
  --arg run_id "$RUN_ID" \
  --arg report_dir "$REPORT_DIR" \
  --arg base_url "$BASE_URL" \
  --arg mission_id "$MISSION_ID" \
  --arg proposal_id "$PROPOSAL_ID" \
  --arg candidate_id "$CANDIDATE_ID" \
  --arg version_id "$VERSION_ID" \
  --slurpfile initial "$RESPONSES_DIR/00-initial-missions.json" \
  --slurpfile candidate "$RESPONSES_DIR/03-memory-candidate.json" \
  --slurpfile run "$RESPONSES_DIR/04-memory-run.json" \
  --slurpfile comparison "$RESPONSES_DIR/05-memory-comparison.json" \
  --slurpfile promotion "$RESPONSES_DIR/06-memory-promotion.json" \
  --slurpfile activeBefore "$RESPONSES_DIR/06a-active-before-rollback.json" \
  --slurpfile runtimeCaps "$RESPONSES_DIR/06b-runtime-capabilities.json" \
  --slurpfile memoryActivation "$RESPONSES_DIR/06c-memory-activation.json" \
  --slurpfile rollback "$RESPONSES_DIR/07-memory-rollback.json" \
  --slurpfile activeAfter "$RESPONSES_DIR/07a-active-after-rollback.json" \
  --slurpfile memory "$RESPONSES_DIR/08-evolution-memory.json" \
  --slurpfile detail "$RESPONSES_DIR/09-memory-mission-detail.json" \
  --slurpfile matrix "$REPORT_DIR/signal-kind-matrix.json" \
  '{
    kind: "cowd.evolution_scenario_gate",
    run_id: $run_id,
    report_dir: $report_dir,
    base_url: $base_url,
    ids: {
      mission_id: $mission_id,
      proposal_id: $proposal_id,
      candidate_id: $candidate_id,
      version_id: $version_id
    },
    checks: {
      initial_missions: $initial[0].count,
      memory_candidate_kind: $candidate[0].candidate.kind,
      runner_modes: [$run[0].runner_results[].mode],
      runner_exit_codes: [$run[0].runner_results[] | {mode, exit_code}],
      runner_all_passed: ([$run[0].runner_results[] | select(.exit_code != 0 or ((.policy_violations // []) | length > 0))] | length == 0),
      runner_has_required_modes: (
        ([$run[0].runner_results[].mode] | index("artifact")) != null
        and ([$run[0].runner_results[].mode] | index("baseline")) != null
        and ([$run[0].runner_results[].mode] | index("candidate")) != null
        and ([$run[0].runner_results[].mode] | index("verification")) != null
      ),
      mainline_modified: ([$run[0].runner_results[].mainline_modified] | any(. == true)),
      regression_count: $comparison[0].comparison.regression_count,
      comparison_recommendation: $comparison[0].comparison.recommendation,
      promotion_accepted: $promotion[0].promotion.accepted,
      active_before_rollback: $activeBefore[0].active_count,
      active_overlay_before_rollback: $activeBefore[0].overlay.active_count,
      runtime_capabilities_active_count: $runtimeCaps[0].active_evolution_capabilities.active_count,
      memory_activation_count: $memoryActivation[0].count,
      rollback_accepted: $rollback[0].rollback.accepted,
      active_after_rollback: $activeAfter[0].active_count,
      rolled_back_after_rollback: $activeAfter[0].rolled_back_count,
      memory_count: $memory[0].count,
      mission_detail_counts: {
        proposals: ($detail[0].proposals | length),
        candidates: ($detail[0].candidates | length),
        comparisons: ($detail[0].comparisons | length),
        promotions: ($detail[0].promotions | length),
        memory: ($detail[0].memory | length)
      },
      signal_kind_matrix_passed: ([$matrix[0][] | select(.pass != true)] | length == 0)
    }
  }' >"$REPORT_DIR/summary.json"

jq -e '
  .checks.memory_candidate_kind == "memory_governance"
  and .checks.runner_all_passed == true
  and .checks.runner_has_required_modes == true
  and .checks.mainline_modified == false
  and .checks.regression_count == 0
  and .checks.comparison_recommendation == "promote_after_human_approval"
  and .checks.promotion_accepted == true
  and .checks.active_before_rollback >= 1
  and .checks.active_overlay_before_rollback >= 1
  and .checks.runtime_capabilities_active_count >= 1
  and .checks.memory_activation_count >= 1
  and .checks.rollback_accepted == true
  and .checks.active_after_rollback == 0
  and .checks.rolled_back_after_rollback >= 1
  and .checks.memory_count >= 2
  and .checks.mission_detail_counts.proposals == 1
  and .checks.mission_detail_counts.candidates == 1
  and .checks.mission_detail_counts.comparisons == 1
  and .checks.mission_detail_counts.promotions == 1
  and .checks.mission_detail_counts.memory >= 2
  and .checks.signal_kind_matrix_passed == true
' "$REPORT_DIR/summary.json" >/dev/null

cat >"$REPORT_DIR/report.md" <<EOF
# Evolution Scenario Gate

- Run ID: \`$RUN_ID\`
- Gateway: \`$BASE_URL\`
- Config home: temporary isolated directory
- Responses: \`$RESPONSES_DIR\`

## Summary

\`\`\`json
$(jq '.' "$REPORT_DIR/summary.json")
\`\`\`

## Signal Kind Matrix

\`\`\`json
$(jq '.' "$REPORT_DIR/signal-kind-matrix.json")
\`\`\`
EOF

echo "evolution scenario gate passed: $REPORT_DIR"
