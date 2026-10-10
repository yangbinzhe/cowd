#!/usr/bin/env bash
set -euo pipefail

: "${COWD_TEST_POSTGRES_URL:?set COWD_TEST_POSTGRES_URL to an isolated disposable PostgreSQL database}"

# Run the deep-real Harness lane through an isolated, short-lived Gateway.
#
# The evaluated route is resolved from the installed Cowd configuration. The
# isolated Gateway receives a minimal copy of that route, but never a literal
# credential. This prevents a scenario from silently testing a different
# provider from the interactive Gateway.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CALLER_HOME="$HOME"
CARGO_HOME_DIR="${CARGO_HOME:-$CALLER_HOME/.cargo}"
RUSTUP_HOME_DIR="${RUSTUP_HOME:-$CALLER_HOME/.rustup}"
SOURCE_CONFIG_HOME="${COWD_EVAL_SOURCE_CONFIG_HOME:-${COWD_CONFIG_HOME:-$CALLER_HOME/.cowd}}"
SOURCE_CONFIG_FILE="$SOURCE_CONFIG_HOME/config.yaml"
SOURCE_MODELS_FILE="$SOURCE_CONFIG_HOME/models.yaml"
MODEL="${COWD_EVAL_MODEL:-}"
BIN="${COWD_BIN:-$ROOT/target/debug/cowd}"
PORT="${COWD_EVAL_GATEWAY_PORT:-18657}"
BASE_URL="http://127.0.0.1:$PORT"
TOKEN="harness-eval-real-qwen-${RANDOM}-${RANDOM}"
RUN_ROOT="$(mktemp -d /tmp/cowd-real-qwen-gateway.XXXXXX)"
# Keep evidence under ignored build artifacts by default so a matching run can
# be reused. A caller may choose another durable evidence root explicitly.
EVIDENCE_ROOT="${COWD_AI_HARNESS_REPORT_DIR:-$ROOT/target/acceptance/real-qwen}"
CONFIG_HOME="$RUN_ROOT/config"
# Source-analysis scenarios default to the candidate tree. Artifact-producing
# scenarios may point the Gateway at an explicit disposable workspace without
# changing where the evaluator binary is built or where evidence is stored.
WORKSPACE="${COWD_EVAL_WORKSPACE:-$ROOT}"
GATEWAY_LOG="$RUN_ROOT/gateway.log"
GATEWAY_PID=""
PROGRESS_PID=""
DEEP_MONITOR_PID=""
DEEP_MONITOR_STALL_SECS="${COWD_EVAL_DEEP_MONITOR_STALL_SECS:-600}"
# A single-stream local inference server cannot serve concurrent requests: the
# runtime's default admission ceiling (provider target 64 / max 256, agents 42)
# queues them behind each other, which shows up as multi-minute round latency
# and starved agents. The in-flight ceiling is resolved after the provider is
# known (local providers default to one); set COWD_EVAL_MAX_CONCURRENCY to
# override explicitly.
MAX_CONCURRENCY="${COWD_EVAL_MAX_CONCURRENCY:-}"
DEEP_MONITOR_LAST_MILESTONE=""
DEEP_MONITOR_LAST_CHANGE_TS=0
PROVIDER_ID=""
PROVIDER_BASE_URL=""
PROVIDER_PROTOCOL=""
PROVIDER_CREDENTIAL=""
STAGED_CREDENTIAL_REF=""
STAGED_CREDENTIAL_ENV=""

# One invocation owns exactly one paid scenario.  Live opt-in is sufficient to
# run it: an optional provider-token value is advisory accounting only, never
# a scenario-specific quality floor or a task-execution stop condition.
SCENARIO_ID="${COWD_EVAL_LIVE_SCENARIOS:-}"
MAX_PROVIDER_TOKENS="${COWD_EVAL_MAX_PROVIDER_TOKENS:-unbounded-explicit-opt-in}"
[[ -n "$SCENARIO_ID" && "$SCENARIO_ID" != *,* ]] || {
  echo 'set COWD_EVAL_LIVE_SCENARIOS to exactly one paid scenario id' >&2
  exit 2
}
case "$SCENARIO_ID" in
  live_direct_terminal|live_tool_evidence|live_single_architecture_baseline|live_one_team_execution|live_small_two_team_collaboration|live_explicit_team_requirement|live_team_projection|live_agent_escalation|live_group_theory_ai_research_simulation|live_qwen38_large_scale_collaboration|live_autonomous_collaboration_deepseek|live_minimal_batch_read|live_minimal_compute_reduce) ;;
  *) echo "unsupported paid live scenario: $SCENARIO_ID" >&2; exit 2 ;;
esac

cleanup() {
  local status=$?
  if [[ -n "$PROGRESS_PID" ]]; then
    kill "$PROGRESS_PID" >/dev/null 2>&1 || true
    wait "$PROGRESS_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$GATEWAY_PID" ]]; then
    kill "$GATEWAY_PID" >/dev/null 2>&1 || true
    wait "$GATEWAY_PID" >/dev/null 2>&1 || true
  fi
  if [[ "$status" -ne 0 || "${COWD_EVAL_KEEP_GATEWAY_ARTIFACTS:-}" == "1" ]]; then
    printf 'isolated Gateway artifacts: %s\n' "$RUN_ROOT" >&2
  else
    rm -rf "$RUN_ROOT"
  fi
  printf 'Harness evidence: %s\n' "$EVIDENCE_ROOT" >&2
}
trap cleanup EXIT

# A real-provider evaluation can legitimately run several nested Team graphs.
# Keep the operator informed through the same public Mission Control projection
# used by Surfaces, rather than leaving only an opaque provider wait. The
# compact line deliberately contains state/counters only: it never reads
# generated config, headers, credentials, prompts, or model/tool payloads.
monitor_progress() {
  local interval="${COWD_EVAL_PROGRESS_INTERVAL_SECS:-15}"
  while kill -0 "$GATEWAY_PID" >/dev/null 2>&1; do
    curl -fsS \
      -H "Authorization: Bearer $TOKEN" \
      "$BASE_URL/api/mission/control/summary" 2>/dev/null \
      | jq -c '{
          event: "harness_eval_progress",
          cursor: .summary.cursor,
          revision: .summary.revision,
          teams: [.summary.projection.teams[]? | {
            team_id,
            status,
            agent_count
          }],
          agents: [.summary.projection.agents[]? | {
            agent_id,
            team_id,
            status
          }],
          pending_approvals: (.summary.projection.summary.pending_approval_count // 0),
          recovery_required: (.summary.projection.summary.recovery_required_count // 0),
          readiness: (.summary.projection.control_readiness.actions // [])
        }' \
      || true
    curl -fsS \
      -H "Authorization: Bearer $TOKEN" \
      "$BASE_URL/api/sessions/executions" 2>/dev/null \
      | jq -c '{
          event: "harness_eval_execution_progress",
          sessions: [.items[]? | {
            session_id,
            latest_execution_id,
            active_execution_ids,
            executions: [.executions[]? | {
              execution_id,
              turn_id,
              status,
              graph_id,
              updated_at_ms
            }]
          }]
        }' \
      || true
    sleep "$interval"
  done
}

# Deep semantic monitoring. Surface progress counters count events; a rework
# loop or a redundant-read loop emits events forever while the Objective never
# advances. This loop compares SEMANTIC milestones (verdicts, reopens,
# reviews, requests) and stops the run when they have not moved for
# COWD_EVAL_DEEP_MONITOR_STALL_SECS while the run keeps burning steps.
monitor_semantic_health() {
  local interval="${COWD_EVAL_DEEP_MONITOR_INTERVAL_SECS:-60}"
  local monitor="$ROOT/scripts/diagnostics/deep-run-monitor.py"
  [[ -f "$monitor" ]] || return 0
  while kill -0 "$HARNESS_PID" >/dev/null 2>&1; do
    local report state milestones findings now
    report="$(python3 "$monitor" "$COWD_TEST_POSTGRES_URL" --investigate 2>/dev/null || true)"
    [[ -n "$report" ]] || { sleep "$interval"; continue; }
    state="$(printf '%s' "$report" | jq -r '.conclusion.state // ""' 2>/dev/null || true)"
    # Only convergence toward the Objective counts as a semantic milestone.
    # `task_submits` is work-phase churn: it rises while the program spins (one
    # frozen run submitted three tasks and never asked to finish), so counting
    # it hides a real freeze behind activity. Reviews, requests, reopens,
    # reworks and verdicts are the milestones.
    milestones="$(printf '%s' "$report" | jq -r '[.verdicts,.reopens,.objective_reviews,.completion_requests,.reworks] | @csv' 2>/dev/null || true)"
    # How long since the last durable event of ANY kind. Semantic milestones are
    # frozen for the whole work phase by design - before the first completion
    # request there is no Objective milestone to move - so using them alone as
    # the freeze signal stops a run that is still doing real work. One run was
    # killed at 605s while actively publishing and reading, purely because no
    # completion request had been issued yet. Event liveness is the honest
    # freeze signal.
    local last_event_age
    last_event_age="$(printf '%s' "$report" | jq -r '.last_event_age_s // 0' 2>/dev/null || echo 0)"
    findings="$(printf '%s' "$report" | jq -c '.conclusion.findings' 2>/dev/null || echo '[]')"
    now="$(date +%s)"
    if [[ -n "$milestones" && "$milestones" != "$DEEP_MONITOR_LAST_MILESTONE" ]]; then
      DEEP_MONITOR_LAST_MILESTONE="$milestones"
      DEEP_MONITOR_LAST_CHANGE_TS="$now"
    fi
    printf '{"event":"harness_eval_deep_monitor","state":"%s","milestones":"%s","stall_for_s":%d,"findings":%s}\n' \
      "$state" "$milestones" "$(( now - DEEP_MONITOR_LAST_CHANGE_TS ))" "$findings"
    if [[ "$DEEP_MONITOR_LAST_CHANGE_TS" -gt 0 ]]; then
      local stalled_for=$(( now - DEEP_MONITOR_LAST_CHANGE_TS ))
      # Stop only on CONFIRMED waste: an active finding AND semantic milestones
      # frozen for the stall window AND no durable event at all in that window.
      # A run that is still emitting events is working, whatever the Objective
      # milestones say. A single recoverable rework never trips it.
      if [[ "$stalled_for" -ge "$DEEP_MONITOR_STALL_SECS" && "$state" == "SPINNING" \
            && "${last_event_age:-0}" -ge "$DEEP_MONITOR_STALL_SECS" ]]; then
        printf '{"event":"harness_eval_deep_monitor_stop","reason":"confirmed spinning: findings present, semantic milestones frozen and no durable event in the stall window","state":"%s","stall_for_s":%d,"findings":%s}\n' \
          "$state" "$stalled_for" "$findings" >&2
        kill "$HARNESS_PID" >/dev/null 2>&1 || true
        return 0
      fi
    fi
    sleep "$interval"
  done
}

yaml_model_field() {
  local file="$1"
  local model="$2"
  local field="$3"
  awk -v target="$model" -v key="$field" '
    function scalar(line, pos, value) {
      pos = index(line, ":")
      value = substr(line, pos + 1)
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+#.*$/, "", value)
      sub(/^\047/, "", value); sub(/\047$/, "", value)
      sub(/^\"/, "", value); sub(/\"$/, "", value)
      return value
    }
    $0 ~ "^[[:space:]]*" target ":[[:space:]]*$" { matched = 1; next }
    matched && $0 ~ "^[[:space:]]{2}[^[:space:]]" { exit }
    matched && $0 ~ "^[[:space:]]{4}" key ":[[:space:]]*" { print scalar($0); exit }
  ' "$file"
}

yaml_provider_field() {
  local file="$1"
  local provider="$2"
  local field="$3"
  awk -v target="$provider" -v key="$field" '
    function scalar(line, pos, value) {
      pos = index(line, ":")
      value = substr(line, pos + 1)
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+#.*$/, "", value)
      sub(/^\047/, "", value); sub(/\047$/, "", value)
      sub(/^\"/, "", value); sub(/\"$/, "", value)
      return value
    }
    /^providers:[[:space:]]*$/ { providers = 1; next }
    providers && $0 ~ "^[[:space:]]{2}" target ":[[:space:]]*$" { matched = 1; next }
    providers && matched && $0 ~ "^[^[:space:]]" { exit }
    providers && matched && $0 ~ "^[[:space:]]{2}[^[:space:]]" { exit }
    providers && matched && $0 ~ "^[[:space:]]{4}" key ":[[:space:]]*" { print scalar($0); exit }
  ' "$file"
}

resolve_installed_route() {
  [[ -f "$SOURCE_CONFIG_FILE" && -f "$SOURCE_MODELS_FILE" ]] || {
    echo "real-model evaluation requires ${SOURCE_CONFIG_FILE} and ${SOURCE_MODELS_FILE}" >&2
    exit 2
  }
  if [[ -z "$MODEL" ]]; then
    MODEL="$(awk '/^model:[[:space:]]*/ { value = $0; sub(/^model:[[:space:]]*/, "", value); sub(/[[:space:]]+#.*$/, "", value); gsub(/^\"|\"$/, "", value); gsub(/^\047|\047$/, "", value); print value; exit }' "$SOURCE_CONFIG_FILE")"
  fi
  [[ -n "$MODEL" ]] || {
    echo "real-model evaluation cannot resolve the configured default model" >&2
    exit 2
  }
  PROVIDER_ID="$(yaml_model_field "$SOURCE_MODELS_FILE" "$MODEL" provider)"
  [[ -n "$PROVIDER_ID" ]] || {
    echo "model ${MODEL} has no provider mapping in ${SOURCE_MODELS_FILE}" >&2
    exit 2
  }
  model_lower="${MODEL,,}"
  provider_lower="${PROVIDER_ID,,}"
  if [[ "$provider_lower" == *bailian* || "$provider_lower" == *dashscope* ]]; then
    echo "native Bailian generation is forbidden; Bailian is reserved for embedding only" >&2
    exit 2
  fi
  if [[ "$model_lower" == qwen* && "$provider_lower" != "qwen-tokenplan" && "$provider_lower" != "local-ninfer" && "$provider_lower" != "local-strata" ]]; then
    echo "Qwen generation must use qwen-tokenplan or a configured local inference provider, got ${PROVIDER_ID}" >&2
    exit 2
  fi
  if [[ "$model_lower" == deepseek* && "$provider_lower" != "deepseek" && "$provider_lower" != "qwen-tokenplan" ]]; then
    echo "DeepSeek generation must use the configured deepseek provider, got ${PROVIDER_ID}" >&2
    exit 2
  fi

# Resolve the in-flight request ceiling now that the provider is known.
if [[ -z "$MAX_CONCURRENCY" ]]; then
  case "$provider_lower" in
    *local*|*ninfer*|*strata*) MAX_CONCURRENCY=1 ;;
  esac
fi
if [[ -n "$MAX_CONCURRENCY" ]]; then
  printf 'In-flight request ceiling: %s (provider %s)
' "$MAX_CONCURRENCY" "$PROVIDER_ID" >&2
fi
  PROVIDER_BASE_URL="$(yaml_provider_field "$SOURCE_CONFIG_FILE" "$PROVIDER_ID" base_url)"
  PROVIDER_PROTOCOL="$(yaml_provider_field "$SOURCE_CONFIG_FILE" "$PROVIDER_ID" protocol)"
  PROVIDER_CREDENTIAL="$(yaml_provider_field "$SOURCE_CONFIG_FILE" "$PROVIDER_ID" api_key)"
  [[ -n "$PROVIDER_BASE_URL" && -n "$PROVIDER_CREDENTIAL" ]] || {
    echo "provider ${PROVIDER_ID} must declare base_url and api_key in ${SOURCE_CONFIG_FILE}" >&2
    exit 2
  }
  case "$PROVIDER_CREDENTIAL" in
    env:*) STAGED_CREDENTIAL_REF="$PROVIDER_CREDENTIAL" ;;
    file:*)
      echo "provider ${PROVIDER_ID} uses an unsupported file credential reference; use Cowd's env: credential reference" >&2
      exit 2
      ;;
    *)
      # The user configuration is the credential authority. A literal value
      # is passed only in the child process environment and is never written
      # to the generated isolated configuration or evidence artifacts.
      STAGED_CREDENTIAL_ENV="COWD_ISOLATED_EVAL_PROVIDER_API_KEY"
      STAGED_CREDENTIAL_REF="env:${STAGED_CREDENTIAL_ENV}"
      ;;
  esac
}

resolve_installed_route
if [[ "$SCENARIO_ID" == "live_autonomous_collaboration_deepseek" && "${MODEL,,}" != deepseek* ]]; then
  echo "autonomous collaboration acceptance requires an explicit DeepSeek model, got ${MODEL}" >&2
  exit 2
fi
[[ -d "$WORKSPACE" ]] || {
  echo "evaluation workspace does not exist: $WORKSPACE" >&2
  exit 2
}
WORKSPACE="$(cd "$WORKSPACE" && pwd -P)"
command -v curl >/dev/null || { echo 'curl is required.' >&2; exit 2; }
command -v ss >/dev/null || { echo 'ss is required.' >&2; exit 2; }
command -v jq >/dev/null || { echo 'jq is required.' >&2; exit 2; }
if [[ -n "$(git -C "$ROOT" status --porcelain=v1 --untracked-files=all)" ]]; then
  if [[ "${COWD_EVAL_EXPERIMENTAL:-0}" != "1" ]]; then
    echo 'real-model evaluation requires a clean candidate worktree; run deterministic checks on the current changes, then evaluate the immutable candidate once.' >&2
    echo 'set COWD_EVAL_EXPERIMENTAL=1 to measure the working tree as-is (evidence is then marked experimental and is never reused).' >&2
    exit 2
  fi
  # Experiment mode. Faithfully fingerprint what ACTUALLY ran - HEAD plus the
  # dirty working tree and untracked files - instead of refusing, so the
  # evidence stays honest: it cannot be mistaken for a certification of the
  # committed source, and two different dirty states cannot collide on one key.
  CANDIDATE_STATE="experimental-dirty"
  TMP_INDEX="$(mktemp -u)"
  GIT_INDEX_FILE="$TMP_INDEX" git -C "$ROOT" read-tree HEAD >/dev/null 2>&1
  GIT_INDEX_FILE="$TMP_INDEX" git -C "$ROOT" add -A >/dev/null 2>&1
  STATE_TREE="$(GIT_INDEX_FILE="$TMP_INDEX" git -C "$ROOT" write-tree 2>/dev/null || true)"
  rm -f "$TMP_INDEX"
  CANDIDATE_SHA="$(git -C "$ROOT" rev-parse HEAD)"
  if [[ -n "$STATE_TREE" ]]; then
    # The field is named sha256 and the report gate validates it as one, so the value
    # has to be a sha256. A git tree id is 40 hex chars: it does separate two dirty
    # states, but it can never satisfy the gate, so a dirty-tree run always fails
    # `evidence_manifest_complete`. Hashing the tree content keeps the property the
    # comment asks for and makes the field honest about its name.
    CANDIDATE_SOURCE_SHA256="$(git -C "$ROOT" cat-file tree "$STATE_TREE" | sha256sum | awk '{print $1}')"
  else
    CANDIDATE_SOURCE_SHA256="$(git -C "$ROOT" status --porcelain=v1 --untracked-files=all | sha256sum | awk '{print $1}')"
  fi
else
  CANDIDATE_STATE="clean"
  CANDIDATE_SHA="$(git -C "$ROOT" rev-parse HEAD)"
  CANDIDATE_SOURCE_SHA256="$(git -C "$ROOT" archive --format=tar "$CANDIDATE_SHA" | sha256sum | awk '{print $1}')"
fi
ROUTE_FINGERPRINT="$(printf '%s\n' "$MODEL" "$PROVIDER_ID" "$PROVIDER_BASE_URL" "$PROVIDER_PROTOCOL" | sha256sum | awk '{print $1}')"
SCENARIO_FINGERPRINT="$(printf '%s\n' "$SCENARIO_ID" "$MAX_PROVIDER_TOKENS" "${COWD_EVAL_AUTONOMOUS_AGENT_SCALE:-}" | sha256sum | awk '{print $1}')"
EVIDENCE_KEY="$(printf '%s\n' "deep-real-v4" "$CANDIDATE_STATE" "$CANDIDATE_SHA" "$CANDIDATE_SOURCE_SHA256" "$ROUTE_FINGERPRINT" "$SCENARIO_FINGERPRINT" | sha256sum | awk '{print $1}')"
if [[ "${COWD_EVAL_FORCE_RERUN:-0}" != "1" && -d "$EVIDENCE_ROOT/runs" ]]; then
  prior_evidence="$({
    find "$EVIDENCE_ROOT/runs" -type f -name report.json -exec jq -r --arg key "$EVIDENCE_KEY" '
      if .evidence_manifest.execution_key == $key then
        if .status == "passed" and .authorized_real_model == true then
          "passed\t" + (.result_package_dir // input_filename)
        else
          "gap\t" + (.status // "unknown") + "\t" + (.result_package_dir // input_filename)
        end
      else empty end
    ' {} + 2>/dev/null
  } | head -n 1)"
  case "$prior_evidence" in
    passed$'\t'*)
      printf 'reusing matching real-model evidence: %s\n' "${prior_evidence#*$'\t'}"
      exit 0
      ;;
    gap$'\t'*)
      printf 'matching evidence has an unresolved gap; inspect it before any rerun: %s\n' "${prior_evidence#*$'\t'}" >&2
      printf 'set COWD_EVAL_FORCE_RERUN=1 only after the candidate, route, fixture, or external provider state has changed.\n' >&2
      exit 3
      ;;
  esac
fi
if ss -ltn | rg -q ":${PORT}\\b"; then
  echo "isolated Gateway port $PORT is already in use" >&2
  exit 2
fi
cd "$ROOT"
# The Gateway and harness evaluator are both part of the system under test.
# Building only harness-eval after the Gateway starts leaves an old
# `target/debug/cowd` serving the scenario; conversely, rebuilding only Cowd
# can make the evaluator send a stale fixture. Build both before every real
# run so the recorded candidate SHA, Gateway, and scenario prompt are one
# immutable candidate. An explicit COWD_BIN remains an operator-supplied
# immutable Gateway artifact and must already be executable.
if [[ -z "${COWD_BIN:-}" ]]; then
  cargo build -p cli --bin cowd
elif [[ ! -x "$BIN" ]]; then
  echo "COWD_BIN is not executable: $BIN" >&2
  exit 2
fi
cargo build -p harness-eval --bin harness-eval
GATEWAY_BINARY_SHA256="$(sha256sum "$BIN" | awk '{print $1}')"
printf 'Gateway binary sha256: %s\n' "$GATEWAY_BINARY_SHA256" >&2

mkdir -p "$CONFIG_HOME"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "$MODEL"
providers:
  "$PROVIDER_ID":
    base_url: "$PROVIDER_BASE_URL"
    api_key: "$STAGED_CREDENTIAL_REF"
EOF
if [[ -n "$PROVIDER_PROTOCOL" ]]; then
  cat >>"$CONFIG_HOME/config.yaml" <<EOF
    protocol: "$PROVIDER_PROTOCOL"
EOF
fi
cat >>"$CONFIG_HOME/config.yaml" <<EOF
    models:
      - "$MODEL"
runtime:
  # The in-flight MODEL-REQUEST ceiling belongs to provider admission only.
  # Collapsing agent/graph parallelism here would deadlock the supervisor:
  # a root parked on the Agentic barrier holds its owned-task permit for the
  # whole wait (proven in Qwen v10/v11: the delegated agent never got a permit,
  # the barrier never released, the no-progress fuse fired at +300s). Keep the
  # supervisor at its default parallelism and let the provider admission below
  # serialize requests to a single-stream server.
  control:
    agent:
      max_parallel_agents: ${COWD_EVAL_AGENT_PARALLELISM:-42}
  resources:
    provider:
      global:
        minimum: ${MAX_CONCURRENCY:-12}
        target: ${MAX_CONCURRENCY:-64}
        maximum: ${MAX_CONCURRENCY:-256}
        interactiveReserve: $(( ${MAX_CONCURRENCY:-9} > 1 ? ${MAX_CONCURRENCY:-9} - 1 : 0 ))
permissions:
  # Permission must match the scenario's declared business profile. Read-only
  # research scenarios (e.g. group theory) run with COWD_EVAL_DEFAULT_MODE=read-only;
  # autonomous scenarios keep the disposable-workspace default.
  default_mode: "${COWD_EVAL_DEFAULT_MODE:-danger-full-access}"
memory:
  enabled: true
storage:
  backend: postgres
  postgres:
    logicalIdentity: "cowd-harness-eval-test"
    secretRef: "env:COWD_TEST_POSTGRES_URL"
gateway:
  enabled: true
  session_reset: none
  platforms:
    - platformType: api_server
      enabled: true
      host: 127.0.0.1
      port: $PORT
      auth:
        enabled: true
        token: "$TOKEN"
EOF

env COWD_CONFIG_HOME="$CONFIG_HOME" "$BIN" storage upgrade

gateway_env=(
  COWD_CONFIG_HOME="$CONFIG_HOME"
  COWD_LOG_STDERR=1
  COWD_EVAL_HARNESS=1
  COWD_EVAL_CORPUS_ID=live-scenarios-v1
  COWD_MODEL_TEMPERATURE=0
)
# Optional objective-level wall-clock budget for required-Team collaboration
# turns. Kept below the scenario safety wait so a non-converging turn ends as an
# honest partial instead of hitting the external timeout.
if [[ -n "${COWD_COLLABORATION_BUDGET_SECS:-}" ]]; then
  gateway_env+=("COWD_COLLABORATION_BUDGET_SECS=$COWD_COLLABORATION_BUDGET_SECS")
fi
if [[ -n "$STAGED_CREDENTIAL_ENV" ]]; then
  gateway_env+=("$STAGED_CREDENTIAL_ENV=$PROVIDER_CREDENTIAL")
fi
(
  cd "$WORKSPACE"
  env "${gateway_env[@]}" \
    "$BIN" gateway run >"$GATEWAY_LOG" 2>&1 &
  echo $! >"$RUN_ROOT/gateway.pid"
)
# The literal credential, if present, is now only held by the isolated child.
unset PROVIDER_CREDENTIAL
GATEWAY_PID="$(<"$RUN_ROOT/gateway.pid")"
for _ in {1..240}; do
  if curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/healthz" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$GATEWAY_PID" >/dev/null 2>&1 || {
    sed -n '1,240p' "$GATEWAY_LOG" >&2 || true
    exit 1
  }
  sleep 0.25
done
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/healthz" >/dev/null

# The disposable credential starts with the product-safe operator profile.
# Normal evaluation requests only mission.observe, while timeout cleanup needs
# runtime.maintenance.manage to cancel the exact lineage owned by this actor.
# Promote through the broker's preview/confirmation protocol so this harness
# never duplicates the manager capability list.
current_entitlement="$(printf '%s\n' "$TOKEN" | env \
  COWD_CONFIG_HOME="$CONFIG_HOME" \
  "$BIN" auth profile show)"
profile_epoch="$(jq -r '.credential_epoch' <<<"$current_entitlement")"
profile_revision="$(jq -r '.profile_revision' <<<"$current_entitlement")"
app_profiles="$(jq -r \
  '.app_profiles | to_entries | map("\(.key)=\(.value)") | join(",")' \
  <<<"$current_entitlement")"
profile_preview="$(printf '%s\n' "$TOKEN" | env \
  COWD_CONFIG_HOME="$CONFIG_HOME" \
  "$BIN" auth profile preview --core-profile core_manager --apps "$app_profiles")"
profile_confirmation="$(jq -r '.confirmation_digest' <<<"$profile_preview")"
printf '%s\n' "$TOKEN" | env \
  COWD_CONFIG_HOME="$CONFIG_HOME" \
  "$BIN" auth profile set \
    --core-profile core_manager \
    --apps "$app_profiles" \
    --expected-epoch "$profile_epoch" \
    --expected-revision "$profile_revision" \
    --confirm "$profile_confirmation" >/dev/null

# Process liveness precedes projector warm-up and control-plane latency
# stabilization. Starting the evaluator in that interval creates a false
# semantic-health failure even when the same isolated Gateway becomes ready a
# moment later. Gate scenario admission on the exact fail-closed contracts the
# evaluator will record; do not weaken or retry a failed scenario afterward.
SEMANTIC_READY_TIMEOUT_SECS="${COWD_EVAL_SEMANTIC_READY_TIMEOUT_SECS:-180}"
semantic_ready=0
semantic_deadline=$((SECONDS + SEMANTIC_READY_TIMEOUT_SECS))
control_plane_health='{}'
surface_health='{}'
while ((SECONDS < semantic_deadline)); do
  control_plane_health="$(curl -fsS \
    -H "Authorization: Bearer $TOKEN" \
    "$BASE_URL/api/runtime/control-plane" 2>/dev/null || printf '{}')"
  surface_health="$(curl -fsS \
    -H "Authorization: Bearer $TOKEN" \
    "$BASE_URL/api/surfaces/health" 2>/dev/null || printf '{}')"
  if jq -e '
      .readiness.production_ready == true and
      .readiness.required_blocked == 0
    ' >/dev/null <<<"$control_plane_health" \
    && jq -e '
      .host.status == "ready" and
      .host.failed_count == 0 and
      .host.circuit_open_count == 0 and
      .host.task_ownership.overloaded == false
    ' >/dev/null <<<"$surface_health"; then
    semantic_ready=1
    break
  fi
  kill -0 "$GATEWAY_PID" >/dev/null 2>&1 || {
    sed -n '1,240p' "$GATEWAY_LOG" >&2 || true
    exit 1
  }
  sleep 0.25
done
if [[ "$semantic_ready" -ne 1 ]]; then
  jq -cn \
    --argjson control "$control_plane_health" \
    --argjson surface "$surface_health" \
    '{
      error: "isolated Gateway did not reach semantic readiness",
      control_plane: ($control.readiness // {}),
      surface_host: ($surface.host // {})
    }' >&2
  exit 1
fi
monitor_progress &
PROGRESS_PID="$!"

cd "$ROOT"
# A deep real-provider run contains serial scenarios, each with its own
# Runtime-aware progress and inactivity bounds. A wall-clock cap must be
# handled inside the evaluator so timeout cleanup can commit a durable Session
# cancellation receipt. Never wrap the evaluator in GNU timeout: killing the
# observer must not silently become a business cancellation or strand a root.
EVAL_COMMAND=(
  cargo run -p harness-eval -- deep-real --provider "$MODEL" --budget full --allow-real-model
)
if [[ -n "${COWD_EVAL_TIMEOUT_SECS:-}" && -z "${COWD_EVAL_SCENARIO_TIMEOUT_SECS:-}" ]]; then
  export COWD_EVAL_SCENARIO_TIMEOUT_SECS="$COWD_EVAL_TIMEOUT_SECS"
fi
env \
  COWD_CONFIG_HOME="$CONFIG_HOME" \
  CARGO_HOME="$CARGO_HOME_DIR" \
  RUSTUP_HOME="$RUSTUP_HOME_DIR" \
  COWD_API_TOKEN="$TOKEN" \
  COWD_EVAL_GATEWAY_URL="$BASE_URL" \
  COWD_EVAL_REAL_MODEL=1 \
  COWD_EVAL_EVIDENCE_KEY="$EVIDENCE_KEY" \
  COWD_EVAL_BINARY_SHA256="$GATEWAY_BINARY_SHA256" \
  COWD_EVAL_CANDIDATE_SHA="$CANDIDATE_SHA" \
  COWD_EVAL_CANDIDATE_SOURCE_SHA256="$CANDIDATE_SOURCE_SHA256" \
  COWD_EVAL_TARGET_REPO_DIRTY_STATE="$CANDIDATE_STATE" \
  COWD_AI_HARNESS_REPORT_DIR="$EVIDENCE_ROOT" \
  "${EVAL_COMMAND[@]}" &
HARNESS_PID="$!"

# Run the evaluator in the background so the deep semantic monitor can stop a
# run that burns steps without advancing the Objective. `wait` preserves the
# evaluator's exit status for the caller.
monitor_semantic_health &
DEEP_MONITOR_PID="$!"
wait "$HARNESS_PID"
EVAL_STATUS=$?
if [[ -n "$DEEP_MONITOR_PID" ]]; then
  kill "$DEEP_MONITOR_PID" >/dev/null 2>&1 || true
  wait "$DEEP_MONITOR_PID" >/dev/null 2>&1 || true
fi
exit "$EVAL_STATUS"
