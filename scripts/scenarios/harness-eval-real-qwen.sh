#!/usr/bin/env bash
set -euo pipefail

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
ISOLATED_HOME="$RUN_ROOT/home"
# Evaluation must inspect the actual source tree. Isolation applies to the
# Gateway's config, credentials, authentication token and durable state, not
# to the read-only source fixture presented to the model.
WORKSPACE="$ROOT"
GATEWAY_LOG="$RUN_ROOT/gateway.log"
GATEWAY_PID=""
PROGRESS_PID=""
PROVIDER_ID=""
PROVIDER_BASE_URL=""
PROVIDER_PROTOCOL=""
PROVIDER_CREDENTIAL=""
STAGED_CREDENTIAL_REF=""
STAGED_CREDENTIAL_ENV=""

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
command -v curl >/dev/null || { echo 'curl is required.' >&2; exit 2; }
command -v ss >/dev/null || { echo 'ss is required.' >&2; exit 2; }
command -v jq >/dev/null || { echo 'jq is required.' >&2; exit 2; }
if [[ -n "$(git -C "$ROOT" status --porcelain=v1 --untracked-files=all)" ]]; then
  echo 'real-model evaluation requires a clean candidate worktree; run deterministic checks on the current changes, then evaluate the immutable candidate once.' >&2
  exit 2
fi
CANDIDATE_SHA="$(git -C "$ROOT" rev-parse HEAD)"
CANDIDATE_SOURCE_SHA256="$(git -C "$ROOT" archive --format=tar "$CANDIDATE_SHA" | sha256sum | awk '{print $1}')"
ROUTE_FINGERPRINT="$(printf '%s\n' "$MODEL" "$PROVIDER_ID" "$PROVIDER_BASE_URL" "$PROVIDER_PROTOCOL" | sha256sum | awk '{print $1}')"
SCENARIO_FINGERPRINT="$(printf '%s\n' "${COWD_EVAL_LIVE_SCENARIOS:-default}" "${COWD_EVAL_GROUP_THEORY_RESEARCH:-0}" "${COWD_EVAL_LARGE_SCALE_COLLABORATION:-0}" | sha256sum | awk '{print $1}')"
EVIDENCE_KEY="$(printf '%s\n' "deep-real-v4" "$CANDIDATE_SHA" "$CANDIDATE_SOURCE_SHA256" "$ROUTE_FINGERPRINT" "$SCENARIO_FINGERPRINT" | sha256sum | awk '{print $1}')"
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

mkdir -p "$CONFIG_HOME" "$ISOLATED_HOME/.cowd"
cp "$SOURCE_MODELS_FILE" "$ISOLATED_HOME/.cowd/models.yaml"
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
permissions:
  default_mode: danger-full-access
memory:
  enabled: true
storage:
  backend: sqlite
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

gateway_env=(
  COWD_CONFIG_HOME="$CONFIG_HOME"
  HOME="$ISOLATED_HOME"
  COWD_LOG_STDERR=1
)
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
  COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$ISOLATED_HOME" \
  "$BIN" auth profile show)"
profile_epoch="$(jq -r '.credential_epoch' <<<"$current_entitlement")"
profile_revision="$(jq -r '.profile_revision' <<<"$current_entitlement")"
app_profiles="$(jq -r \
  '.app_profiles | to_entries | map("\(.key)=\(.value)") | join(",")' \
  <<<"$current_entitlement")"
profile_preview="$(printf '%s\n' "$TOKEN" | env \
  COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$ISOLATED_HOME" \
  "$BIN" auth profile preview --core-profile core_manager --apps "$app_profiles")"
profile_confirmation="$(jq -r '.confirmation_digest' <<<"$profile_preview")"
printf '%s\n' "$TOKEN" | env \
  COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$ISOLATED_HOME" \
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
# Runtime-aware progress and inactivity bounds. Do not impose a second,
# shorter whole-suite deadline by default: it can kill a later Team graph that
# is still making durable progress. Operators who need a wall-clock cap can
# opt in with COWD_EVAL_TIMEOUT_SECS; Mission Control remains the live view in
# either mode.
EVAL_COMMAND=(
  cargo run -p harness-eval -- deep-real --provider "$MODEL" --budget full --allow-real-model
)
if [[ -n "${COWD_EVAL_TIMEOUT_SECS:-}" ]]; then
  EVAL_COMMAND=(timeout "${COWD_EVAL_TIMEOUT_SECS}s" "${EVAL_COMMAND[@]}")
fi
env \
  COWD_CONFIG_HOME="$CONFIG_HOME" \
  HOME="$ISOLATED_HOME" \
  CARGO_HOME="$CARGO_HOME_DIR" \
  RUSTUP_HOME="$RUSTUP_HOME_DIR" \
  COWD_API_TOKEN="$TOKEN" \
  COWD_EVAL_GATEWAY_URL="$BASE_URL" \
  COWD_EVAL_REAL_MODEL=1 \
  COWD_EVAL_EVIDENCE_KEY="$EVIDENCE_KEY" \
  COWD_EVAL_BINARY_SHA256="$GATEWAY_BINARY_SHA256" \
  COWD_EVAL_CANDIDATE_SHA="$CANDIDATE_SHA" \
  COWD_EVAL_CANDIDATE_SOURCE_SHA256="$CANDIDATE_SOURCE_SHA256" \
  COWD_EVAL_TARGET_REPO_DIRTY_STATE=clean \
  COWD_AI_HARNESS_REPORT_DIR="$EVIDENCE_ROOT" \
  "${EVAL_COMMAND[@]}"
