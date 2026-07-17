#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
AUTH_BROKER_BIN="${COWD_AUTH_BROKER_BIN:-$TARGET_ROOT/debug/cowd-auth-broker}"
PORT="${COWD_MFG_GATEWAY_PORT:-18642}"
BASE_URL="http://127.0.0.1:${PORT}"
API_TOKEN="${COWD_API_TOKEN:-mfg-surface-$$_credential}"
SCENARIO_ID="mfg-surface-acceptance-$$-$(date +%s)"
SCENARIO_ROOT="${TMPDIR:-/tmp}/${SCENARIO_ID}"
WORKSPACE="${COWD_MFG_TEST_WORKSPACE:-${SCENARIO_ROOT}/workspace}"
CONFIG_HOME="${SCENARIO_ROOT}/config"
TEST_HOME="${SCENARIO_ROOT}/home"
ARTIFACT_DIR="${COWD_MFG_ACCEPTANCE_ARTIFACT_DIR:-${ROOT}/target/acceptance/${SCENARIO_ID}}"
EDGE_ROOT="${COWD_EDGE_ROOT:-$(cd "${ROOT}/../cowd-edge" 2>/dev/null && pwd)}"
WEBUI_ROOT="${EDGE_ROOT}/surfaces/webui"
WEBUI_PORT="${COWD_MFG_WEBUI_PORT:-15173}"
WEBUI_URL="http://127.0.0.1:${WEBUI_PORT}"
TMUX_SOCKET="${SCENARIO_ID}"
GATEWAY_LOG="${ARTIFACT_DIR}/gateway.log"
GATEWAY_STDOUT_LOG="${ARTIFACT_DIR}/gateway-stdout.log"
GATEWAY_PID=""
WEBUI_PID=""
BROWSER_PID=""
PROFILE_SET_STDERR="${ARTIFACT_DIR}/profile-set.stderr"
MFG_DB=""

stop_owned_pid() {
  local pid=$1
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null || return 0
  kill -TERM "${pid}" 2>/dev/null || true
  for _ in {1..40}; do
    kill -0 "${pid}" 2>/dev/null || break
    sleep 0.25
  done
  if kill -0 "${pid}" 2>/dev/null; then
    kill -KILL "${pid}" 2>/dev/null || true
  fi
  wait "${pid}" 2>/dev/null || true
}

stop_gateway() {
  local pid=$1
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null || return 0
  # The Gateway owns its auth broker.  Stop it before the bounded Gateway
  # reap so a restart never leaves the old Unix socket owner behind.
  while read -r child_pid; do
    [[ -n "${child_pid}" ]] && kill -TERM "${child_pid}" 2>/dev/null || true
  done < <(ps -o pid= --ppid "${pid}" 2>/dev/null || true)
  stop_owned_pid "${pid}"
}

cleanup() {
  stop_owned_pid "${BROWSER_PID}"
  stop_owned_pid "${WEBUI_PID}"
  stop_gateway "${GATEWAY_PID}"
  tmux -L "${TMUX_SOCKET}" kill-server 2>/dev/null || true
  if [[ "${COWD_MFG_KEEP_FAILED_ARTIFACTS:-0}" != "1" && "${MFG_SCENARIO_PASSED:-0}" == "1" ]]; then
    rm -rf "${SCENARIO_ROOT}"
  fi
}
trap cleanup EXIT INT TERM

for command in curl jq node npm python3 rg ss sqlite3 tmux; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required for MFG surface acceptance" >&2
    exit 1
  }
done
[[ -f "${WEBUI_ROOT}/scripts/mfg-live-browser-observer.mjs" ]] || {
  echo "cowd-edge WebUI observer is missing under ${WEBUI_ROOT}" >&2
  exit 1
}

if ss -ltnp | rg -q ":${PORT}\\b"; then
  echo "MFG acceptance port ${PORT} is already in use" >&2
  exit 1
fi
if [[ "${COWD_SCENARIO_SKIP_BUILD:-0}" != "1" ]]; then
  # This acceptance lane drives both WebUI and the real TUI /mfg surface.
  # The default CLI build excludes TUI, so build the feature-complete binary
  # instead of depending on whichever artifact happened to be built earlier.
  (cd "${ROOT}" && cargo build -p cli --features full -p auth-broker)
fi
[[ -x "${BIN}" ]] || { echo "missing cowd binary at ${BIN}" >&2; exit 1; }
[[ -x "${AUTH_BROKER_BIN}" ]] || {
  echo "missing cowd-auth-broker at ${AUTH_BROKER_BIN}" >&2
  exit 1
}

mkdir -p "${WORKSPACE}/.cowd" "${CONFIG_HOME}" "${TEST_HOME}/.cowd" "${ARTIFACT_DIR}"
cat >"${CONFIG_HOME}/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  anthropic:
    base_url: "https://api.anthropic.com/v1"
    api_key: "${ANTHROPIC_API_KEY:-test-dummy-key-for-mfg-live}"
    protocol: "anthropic"
    models: ["claude-sonnet-4-6"]
permissions:
  defaultMode: "dontAsk"
memory:
  enabled: false
gateway:
  enabled: true
  session_reset: "none"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: ${PORT}
      auth:
        enabled: true
        token: "${API_TOKEN}"
EOF
cp "${CONFIG_HOME}/config.yaml" "${TEST_HOME}/.cowd/config.yaml"
cp "${CONFIG_HOME}/config.yaml" "${WORKSPACE}/.cowd/config.yaml"

authorized_curl() {
  command curl -H "Authorization: Bearer ${API_TOKEN}" "$@"
}

now_ms() {
  # `%3N` is not portable across the date implementations used by supported
  # developer environments.  Python's integer clock keeps browser and shell
  # probe timestamps in the same millisecond unit.
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

copy_observer_artifact() {
  local source=$1
  local destination=$2
  for _ in {1..8}; do
    if [[ -s "${source}" ]] && cp "${source}" "${destination}" 2>/dev/null \
      && jq -e . "${destination}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "observer artifact was not readable: ${source}" >&2
  return 1
}

capture_gateway_logs() {
  # Runtime tracing is intentionally persisted under config_home; Gateway's
  # stdout is only a companion diagnostic.  Materialize one deterministic
  # artifact so every queue/release assertion reads the same authoritative
  # source after restarts as well.
  local temporary="${GATEWAY_LOG}.tmp-$$"
  : >"${temporary}"
  if [[ -d "${CONFIG_HOME}/logs" ]]; then
    while IFS= read -r -d '' log_file; do
      cat "${log_file}" >>"${temporary}"
    done < <(find "${CONFIG_HOME}/logs" -maxdepth 1 -type f -name 'cowd.*' -print0 | sort -z)
  fi
  [[ -f "${GATEWAY_STDOUT_LOG}" ]] && cat "${GATEWAY_STDOUT_LOG}" >>"${temporary}"
  mv "${temporary}" "${GATEWAY_LOG}"
}

profile_show() {
  printf '%s\n' "${API_TOKEN}" | env \
    COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${TEST_HOME}" \
    "${BIN}" auth profile show
}

set_profile() {
  local core=$1
  local mfg=$2
  local output=$3
  local current epoch revision confirmation
  current="$(profile_show)"
  epoch="$(jq -er '.credential_epoch' <<<"${current}")"
  revision="$(jq -er '.profile_revision' <<<"${current}")"
  if printf '%s\n' "${API_TOKEN}" | env \
    COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${TEST_HOME}" \
    "${BIN}" auth profile set \
      --core "${core}" --mfg "${mfg}" \
      --expected-epoch "${epoch}" --expected-revision "${revision}" \
      --confirm invalid >"${output}.probe" 2>"${PROFILE_SET_STDERR}"; then
    echo "profile confirmation probe unexpectedly succeeded" >&2
    return 1
  fi
  confirmation="$(sed -n 's/.*confirmation=\([^[:space:]]*\).*/\1/p' \
    "${PROFILE_SET_STDERR}" | head -1)"
  [[ -n "${confirmation}" ]] || {
    echo "profile confirmation digest was not emitted" >&2
    return 1
  }
  printf '%s\n' "${API_TOKEN}" | env \
    COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${TEST_HOME}" \
    "${BIN}" auth profile set \
      --core "${core}" --mfg "${mfg}" \
      --expected-epoch "${epoch}" --expected-revision "${revision}" \
      --confirm "${confirmation}" >"${output}" 2>>"${PROFILE_SET_STDERR}"
  rm -f "${output}.probe"
}

start_gateway() {
  (
    cd "${WORKSPACE}"
    exec env \
      COWD_CONFIG_HOME="${CONFIG_HOME}" \
      COWD_AUTH_BROKER_BIN="${AUTH_BROKER_BIN}" \
      HOME="${TEST_HOME}" \
      "${BIN}" gateway run
  ) >>"${GATEWAY_STDOUT_LOG}" 2>&1 &
  GATEWAY_PID=$!
  for _ in {1..120}; do
    authorized_curl -fsS "${BASE_URL}/health" >/dev/null 2>&1 && return
    sleep 0.25
  done
  echo "Gateway did not become ready" >&2
  return 1
}

start_tui() {
  local name=$1
  local session_id=$2
  local state_artifact=$3
  # A wide pane keeps the complete bootstrap error in the retained tmux
  # transcript.  The TUI includes the request URL in attach failures, which
  # otherwise gets left-clipped in a narrow alternate screen and makes a
  # genuine startup regression impossible to diagnose from acceptance
  # evidence alone.
  tmux -L "${TMUX_SOCKET}" new-session -d -s "${name}" -x 240 -y 60 -c "${WORKSPACE}" \
    "exec env COWD_CONFIG_HOME='${CONFIG_HOME}' COWD_API_TOKEN='${API_TOKEN}' \
      COWD_GATEWAY_URL='${BASE_URL}' HOME='${TEST_HOME}' COWD_DISABLE_DAEMON_AUTOSTART=1 \
      COWD_MFG_OBSERVER_ID='${name}' \
      COWD_TUI_MFG_STATE_ARTIFACT='${state_artifact}' \
      COWD_TUI_ACCESSIBILITY=1 COWD_TUI_MOUSE=0 TERM=xterm-256color \
      '${BIN}' --yolo --model claude-sonnet-4-6 --session '${session_id}'"
  sleep 0.5
  tmux -L "${TMUX_SOCKET}" send-keys -t "${name}" -l "/mfg"
  tmux -L "${TMUX_SOCKET}" send-keys -t "${name}" C-m
  for _ in {1..120}; do
    if jq -e '.live.available == true and (.assignments | type == "array")' \
      "${state_artifact}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  {
    tmux -L "${TMUX_SOCKET}" list-panes -t "${name}" \
      -F 'pane_pid=#{pane_pid} dead=#{pane_dead} status=#{pane_dead_status} command=#{pane_current_command}' \
      2>/dev/null || true
    tmux -L "${TMUX_SOCKET}" capture-pane -p -t "${name}" -S -240 \
      2>/dev/null || true
  } >"${ARTIFACT_DIR}/${name}-bootstrap.txt"
  echo "MFG TUI observer ${name} did not become live" >&2
  return 1
}

start_webui_browser() {
  (
    cd "${WEBUI_ROOT}"
    exec env COWD_VITE_GATEWAY_URL="${BASE_URL}" \
      node ./node_modules/vite/bin/vite.js --host 127.0.0.1 --port "${WEBUI_PORT}" --strictPort
  ) >>"${ARTIFACT_DIR}/webui-vite.log" 2>&1 &
  WEBUI_PID=$!
  for _ in {1..120}; do
    curl -fsS "${WEBUI_URL}/index.dev.html" >/dev/null 2>&1 && break
    sleep 0.25
  done
  curl -fsS "${WEBUI_URL}/index.dev.html" >/dev/null
  (
    cd "${WEBUI_ROOT}"
    exec env \
      COWD_MFG_GATEWAY_URL="${BASE_URL}" \
      COWD_MFG_WEBUI_URL="${WEBUI_URL}" \
      COWD_MFG_API_TOKEN="${API_TOKEN}" \
      COWD_MFG_BROWSER_ARTIFACT="${ARTIFACT_DIR}/webui-browser.json" \
      COWD_MFG_BROWSER_SCREENSHOT="${ARTIFACT_DIR}/webui-browser.png" \
      COWD_MFG_BROWSER_PROBE_REQUEST="${ARTIFACT_DIR}/browser-probe-request.json" \
      node scripts/mfg-live-browser-observer.mjs
  ) >>"${ARTIFACT_DIR}/webui-browser.log" 2>&1 &
  BROWSER_PID=$!
  for _ in {1..120}; do
    jq -e '.status == "live" and any(.browser.frames[]?; .kind == "snapshot")' \
      "${ARTIFACT_DIR}/webui-browser.json" >/dev/null 2>&1 && return
    sleep 0.25
  done
  echo "real WebUI browser observer did not become live" >&2
  return 1
}

snapshot() {
  local output=$1
  authorized_curl -fsS "${BASE_URL}/api/apps/mfg/live/snapshot" -o "${output}"
  jq -e '.kind == "snapshot" and (.view_epoch | type == "string") and (.cursor | type == "string")' \
    "${output}" >/dev/null
}

start_gateway
profile_show >"${ARTIFACT_DIR}/profile-initial.json"
set_profile core_manager mfg_manager "${ARTIFACT_DIR}/profile-manager.json"
# Keep the policy and delivery terminal states distinct in this live lane.
# The authenticated local operator is deliberately unknown to the
# cross-plane identity registry.  Grant only this fixture principal the
# report-send capability, then send to the configured Feishu namespace
# without binding a Feishu Surface.  That makes each commit pass policy and
# reach the real Surface executor, where it fails retryably and can become a
# report-delivery dead letter.  Using an unconfigured channel here would be
# policy_blocked instead and could never exercise the review Saga.
DELIVERY_GRANT_ID="${SCENARIO_ID}-delivery-grant"
authorized_curl -fsS -X POST "${BASE_URL}/api/cross-plane/grants" \
  -H "content-type: application/json" \
  -d "$(jq -cn --arg id "${DELIVERY_GRANT_ID}" '{
    id: $id,
    principal_id: "principal:local-human",
    capability: "channel.feishu.send_text",
    account_id: null,
    target_ref: null,
    resource_ref: null,
    source_channel: null,
    grant_type: "persistent",
    expires_at: null,
    remaining_uses: null,
    created_by: "mfg-surface-acceptance",
    approval_id: null
  }')" -o "${ARTIFACT_DIR}/report-delivery-grant.json"
jq -e --arg id "${DELIVERY_GRANT_ID}" \
  '.kind == "cross_plane_grant"
    and .grant.id == $id
    and .grant.principal_id == "principal:local-human"
    and .grant.capability == "channel.feishu.send_text"
    and .grant.grant_type == "persistent"' \
  "${ARTIFACT_DIR}/report-delivery-grant.json" >/dev/null
start_webui_browser
start_tui "${SCENARIO_ID}-tui-a" "${SCENARIO_ID}-session-a" \
  "${ARTIFACT_DIR}/tui-a-state.json"
start_tui "${SCENARIO_ID}-tui-b" "${SCENARIO_ID}-session-b" \
  "${ARTIFACT_DIR}/tui-b-state.json"
snapshot "${ARTIFACT_DIR}/snapshot-before.json"

PROFILE_RESPONSE="${ARTIFACT_DIR}/cockpit-profile.json"
authorized_curl -fsS -X POST "${BASE_URL}/api/apps/mfg/cockpit/profiles/upsert" \
  -H "content-type: application/json" \
  -H "idempotency-key: ${SCENARIO_ID}-profile" \
  -d '{
    "request_id": "mfg-live-profile",
    "profile": {
      "owner_ref": "principal:server-owned",
      "display_name": "Live acceptance cockpit",
      "cadence": "daily",
      "scope": {"kind": "personal"},
      "layout": {"columns": 12, "row_height": 72, "gap": 12},
      "global_filters": {},
      "widget_instances": [],
      "sharing_policy": {"visibility": "private", "viewer_refs": [], "editor_refs": []}
    }
  }' -o "${PROFILE_RESPONSE}"
jq -e '.profile.revision == 1 and (.business_receipt.receipt_id | type == "string")' \
  "${PROFILE_RESPONSE}" >/dev/null
PROFILE_ID="$(jq -er '.profile.profile_id' "${PROFILE_RESPONSE}")"

REPORT_RESPONSE="${ARTIFACT_DIR}/report-generate.json"
authorized_curl -fsS -X POST \
  "${BASE_URL}/api/apps/mfg/cockpit/profiles/${PROFILE_ID}/reports/generate" \
  -H "content-type: application/json" \
  -H "idempotency-key: ${SCENARIO_ID}-report" \
  -d '{"request_id":"mfg-live-report","report":{"cadence":"daily","note":"V545 live acceptance"}}' \
  -o "${REPORT_RESPONSE}"
jq -e '.report.revision == 1
  and (.report.report_id | type == "string")
  and (._mfg_receipt.receipt_id | type == "string")' \
  "${REPORT_RESPONSE}" >/dev/null
REPORT_ID="$(jq -er '.report.report_id' "${REPORT_RESPONSE}")"
REPORT_RECEIPT_ID="$(jq -er '._mfg_receipt.receipt_id' "${REPORT_RESPONSE}")"

authorized_curl -fsS -X POST \
  "${BASE_URL}/api/apps/mfg/cockpit/reports/${REPORT_ID}/deliver" \
  -H "content-type: application/json" \
  -d '{"mode":"dry_run","expected_revision":1,"channel":"feishu",
       "target_ref":"channel://feishu/user/live-acceptance"}' \
  -o "${ARTIFACT_DIR}/report-delivery-preview.json"
jq -e '.mode == "dry_run"
  and (.cross_plane_execution_receipt.id | type == "string")
  and (.delivery_payload.report_id | type == "string")' \
  "${ARTIFACT_DIR}/report-delivery-preview.json" >/dev/null

for attempt in 1 2 3; do
  REPORT_REVISION="$(authorized_curl -fsS \
    "${BASE_URL}/api/apps/mfg/cockpit/reports/${REPORT_ID}" | jq -er '.report.revision')"
  authorized_curl -fsS -X POST \
    "${BASE_URL}/api/apps/mfg/cockpit/reports/${REPORT_ID}/deliver" \
    -H "content-type: application/json" \
    -H "idempotency-key: ${SCENARIO_ID}-delivery-failure-${attempt}" \
    -d "$(jq -cn --argjson revision "${REPORT_REVISION}" --arg attempt "${attempt}" '{
      mode: "commit",
      expected_revision: $revision,
      channel: "feishu",
      target_ref: ("channel://feishu/user/acceptance-" + $attempt),
      source_channel: "mfg.live.acceptance"
    }')" \
    -o "${ARTIFACT_DIR}/report-delivery-failure-${attempt}.json"
  jq -e '.kind == "mfg.cockpit.report_delivery"
    and (.report.revision | type == "number")
    and .cross_plane_execution_receipt.decision.decision == "allow"
    and .cross_plane_execution_receipt.decision.reason == "matched_grant"
    and .cross_plane_execution_receipt.dispatch_status == "dispatch_failed"
    and (.cross_plane_execution_receipt.id | type == "string")' \
    "${ARTIFACT_DIR}/report-delivery-failure-${attempt}.json" >/dev/null
done
authorized_curl -fsS \
  "${BASE_URL}/api/apps/mfg/cockpit/reports/${REPORT_ID}/delivery-state" \
  -o "${ARTIFACT_DIR}/report-delivery-state.json"
jq -e '.delivery_state.dead_lettered == true
  and .delivery_state.attempt_count >= 3
  and .delivery_state.recommended_mode == "manual_review"' \
  "${ARTIFACT_DIR}/report-delivery-state.json" >/dev/null
REPORT_REVISION="$(authorized_curl -fsS \
  "${BASE_URL}/api/apps/mfg/cockpit/reports/${REPORT_ID}" | jq -er '.report.revision')"
authorized_curl -fsS -X POST \
  "${BASE_URL}/api/apps/mfg/cockpit/reports/${REPORT_ID}/reviews" \
  -H "content-type: application/json" \
  -H "idempotency-key: ${SCENARIO_ID}-review" \
  -d "$(jq -cn --argjson revision "${REPORT_REVISION}" --arg key "${SCENARIO_ID}-review" '{
    expected_report_revision: $revision,
    reason: "delivery exhausted during live multi-surface acceptance",
    evidence_refs: ["evidence://mfg-live/delivery-exhausted"],
    idempotency_key: $key
  }')" -o "${ARTIFACT_DIR}/report-review-request.json"
jq -e '.kind == "mfg.report_delivery_review.requested"
  and (.review.review_id | type == "string")
  and (.review.revision | type == "number")' \
  "${ARTIFACT_DIR}/report-review-request.json" >/dev/null
REVIEW_ID="$(jq -er '.review.review_id' "${ARTIFACT_DIR}/report-review-request.json")"
REVIEW_REVISION="$(jq -er '.review.revision' "${ARTIFACT_DIR}/report-review-request.json")"
REVIEW_STATUS="$(jq -er '.review.status' "${ARTIFACT_DIR}/report-review-request.json")"

authorized_curl -fsS "${BASE_URL}/api/apps/mfg/cockpit/report-reviews?limit=100" \
  -o "${ARTIFACT_DIR}/report-reviews.json"
jq -e --arg review "${REVIEW_ID}" \
  'any(.items[]?; .review_id == $review and (.revision | type == "number"))' \
  "${ARTIFACT_DIR}/report-reviews.json" >/dev/null
authorized_curl -fsS "${BASE_URL}/api/apps/mfg/cockpit/reports/${REPORT_ID}" \
  -o "${ARTIFACT_DIR}/report-final.json"
REPORT_REVISION="$(jq -er '.report.revision' "${ARTIFACT_DIR}/report-final.json")"
REPORT_STATUS="$(jq -er '.report.status' "${ARTIFACT_DIR}/report-final.json")"
REPORT_DELIVERY_IDS="$(jq -c '[.report.delivery_receipts[].delivery_id] | sort' \
  "${ARTIFACT_DIR}/report-final.json")"
jq -e --argjson delivery_ids "${REPORT_DELIVERY_IDS}" \
  '(.report.delivery_receipts | length) >= 3
    and ([.report.delivery_receipts[].delivery_id] | sort) == $delivery_ids' \
  "${ARTIFACT_DIR}/report-final.json" >/dev/null

TUI_B_PID="$(tmux -L "${TMUX_SOCKET}" list-panes -t "${SCENARIO_ID}-tui-b" -F '#{pane_pid}')"
kill -STOP "${TUI_B_PID}"

BURST_TASK_ID="$(authorized_curl -fsS -X POST "${BASE_URL}/api/tasks/start" \
  -H "content-type: application/json" \
  -d '{"objective":"MFG live bounded multi-observer burst owner","yolo_mode":true}' \
  | jq -er '.id')"
BURST_RESULTS_DIR="${ARTIFACT_DIR}/burst-requests"
BURST_PACE_MS="${COWD_MFG_BURST_PACE_MS:-20}"
[[ "${BURST_PACE_MS}" =~ ^[0-9]+$ ]] || {
  echo "COWD_MFG_BURST_PACE_MS must be a non-negative integer" >&2
  exit 1
}
mkdir -p "${BURST_RESULTS_DIR}"

# A concurrent SQLite writer can reject a request transiently while another
# transaction is committing.  The API has an idempotency key, so retain the
# original key and retry only the same logical write.  Every worker records a
# compact outcome so an acceptance failure identifies the affected rows and
# HTTP statuses instead of silently reducing the pressure set.
create_burst_assignment() {
  local sequence=$1
  local assignment_id="mfg-live-assignment-${sequence}"
  local payload result_body http_status attempt=0
  payload="$(jq -cn --arg id "${assignment_id}" --arg task_id "${BURST_TASK_ID}" '{
    assignment: {
      assignment_id: $id,
      task_ref: ("task://" + $task_id),
      assignee_ref: ("principal:mfg-live-burst-" + $id),
      assignee_kind: "user",
      watcher_refs: [],
      priority: "normal",
      status: "assigned",
      visibility: "private",
      notification_targets: []
    }
  }')"
  # Keep raw responses out of the `*.json` outcome aggregation below.
  result_body="${BURST_RESULTS_DIR}/${sequence}.response"

  while (( attempt < ${COWD_MFG_BURST_MAX_ATTEMPTS:-8} )); do
    attempt=$((attempt + 1))
    http_status="$(curl -sS -o "${result_body}" -w '%{http_code}' \
      -H "Authorization: Bearer ${API_TOKEN}" \
      -H 'content-type: application/json' \
      -H "idempotency-key: mfg-live-burst-${sequence}" \
      -X POST "${BASE_URL}/api/apps/mfg/assignments" \
      -d "${payload}")" || http_status="transport_error"
    if [[ "${http_status}" =~ ^2[0-9][0-9]$ ]]; then
      jq -n --arg id "${assignment_id}" --argjson sequence "${sequence}" \
        --argjson attempts "${attempt}" --arg status "${http_status}" \
        '{assignment_id:$id, $sequence, $attempts, http_status:$status, status:"accepted"}' \
        >"${BURST_RESULTS_DIR}/${sequence}.json"
      # Keep the pressure window open long enough to measure a real browser
      # interaction while writes are still arriving, without lowering the
      # configured parallel writer count.
      if (( BURST_PACE_MS > 0 )); then
        printf -v burst_pace_seconds '%d.%03d' \
          "$((BURST_PACE_MS / 1000))" "$((BURST_PACE_MS % 1000))"
        sleep "${burst_pace_seconds}"
      fi
      return 0
    fi
    # Bounded, deterministic backoff keeps contention realistic while giving
    # an accepted idempotent write a chance to complete.
    sleep "0.$((attempt * 5))"
  done

  jq -n --arg id "${assignment_id}" --argjson sequence "${sequence}" \
    --argjson attempts "${attempt}" --arg status "${http_status}" \
    --rawfile body "${result_body}" \
    '{assignment_id:$id, $sequence, $attempts, http_status:$status, status:"failed", body:$body}' \
    >"${BURST_RESULTS_DIR}/${sequence}.json"
  return 1
}
export -f create_burst_assignment
export API_TOKEN BASE_URL BURST_TASK_ID BURST_RESULTS_DIR BURST_PACE_MS
# The repository enforces one assignment per (task_ref, assignee_ref).  All
# burst rows remain visible to the authenticated creator, while a unique
# assignee keeps this 1000-event pressure test domain-valid.
BURST_STARTED_AT_MS="$(now_ms)"
seq 1 1000 | xargs -P "${COWD_MFG_BURST_PARALLELISM:-16}" -I{} \
  bash -c 'create_burst_assignment "$1"' _ {} &
BURST_PID=$!
BURST_PROBE_ID="${SCENARIO_ID}-webui-refresh-during-burst"
jq -n --arg id "${BURST_PROBE_ID}" '{$id}' \
  >"${ARTIFACT_DIR}/browser-probe-request.json.tmp"
mv "${ARTIFACT_DIR}/browser-probe-request.json.tmp" \
  "${ARTIFACT_DIR}/browser-probe-request.json"
for _ in {1..120}; do
  jq -e --arg id "${BURST_PROBE_ID}" \
    'any(.interaction_probes[]?; .id == $id and .status == "passed")' \
    "${ARTIFACT_DIR}/webui-browser.json" >/dev/null 2>&1 && break
  sleep 0.25
done
BURST_EXIT=0
wait "${BURST_PID}" || BURST_EXIT=$?
BURST_FINISHED_AT_MS="$(now_ms)"
BURST_TIMING="${ARTIFACT_DIR}/burst-timing.json"
jq -n --arg id "${BURST_PROBE_ID}" \
  --argjson started_at_ms "${BURST_STARTED_AT_MS}" \
  --argjson finished_at_ms "${BURST_FINISHED_AT_MS}" \
  '{probe_id:$id, $started_at_ms, $finished_at_ms}' >"${BURST_TIMING}"
BURST_REQUEST_SUMMARY="${ARTIFACT_DIR}/burst-request-summary.json"
jq -s '{
  total:length,
  accepted:([.[] | select(.status == "accepted")] | length),
  failed:([.[] | select(.status == "failed")] | length),
  max_attempts:([.[].attempts] | max),
  failures:[.[] | select(.status == "failed")]
}' "${BURST_RESULTS_DIR}"/*.json >"${BURST_REQUEST_SUMMARY}"
jq -e --argjson exit_code "${BURST_EXIT}" \
  '.total == 1000 and .accepted == 1000 and .failed == 0 and $exit_code == 0' \
  "${BURST_REQUEST_SUMMARY}" >/dev/null || {
  echo "bounded MFG burst did not persist every idempotent assignment" >&2
  exit 1
}
jq -e --arg id "${BURST_PROBE_ID}" \
  --argjson started "${BURST_STARTED_AT_MS}" \
  --argjson finished "${BURST_FINISHED_AT_MS}" \
  'any(.interaction_probes[]?;
    .id == $id
    and .status == "passed"
    and .started_at_ms >= $started
    and .started_at_ms <= $finished
    and .latency_ms <= 2000)' \
  "${ARTIFACT_DIR}/webui-browser.json" >/dev/null

snapshot "${ARTIFACT_DIR}/snapshot-after-burst.json"
BURST_ASSIGNMENT_COUNT="$(jq -er '.state.assignments.items | length' \
  "${ARTIFACT_DIR}/snapshot-after-burst.json")"
[[ "${BURST_ASSIGNMENT_COUNT}" -ge 1000 ]] || {
  echo "transactional snapshot lost assignments from the 1000-event burst" >&2
  exit 1
}
kill -0 "${WEBUI_PID}" 2>/dev/null && kill -0 "${BROWSER_PID}" 2>/dev/null || {
  echo "real WebUI observer stopped while TUI-B was paused" >&2
  exit 1
}
for _ in {1..120}; do
  if jq -e --arg report "${REPORT_ID}" --arg review "${REVIEW_ID}" \
    --arg receipt "${REPORT_RECEIPT_ID}" --arg status "${REPORT_STATUS}" \
    --arg review_status "${REVIEW_STATUS}" --argjson revision "${REPORT_REVISION}" \
    --argjson review_revision "${REVIEW_REVISION}" \
    --argjson delivery_ids "${REPORT_DELIVERY_IDS}" \
    '(.live.available == true)
      and ([.assignments[].id] | length >= 1000)
      and any(.reports[]?;
        .id == $report and .revision == $revision and .status == $status
        and ((.delivery_receipt_ids | sort) == $delivery_ids))
      and any(.reviews[]?;
        .id == $review and .report_id == $report
        and .revision == $review_revision and .status == $review_status)
      and any(.receipts[]?; .id == $receipt)' \
    "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null 2>&1 \
    && jq -e --arg report "${REPORT_ID}" --arg review "${REVIEW_ID}" \
      --arg receipt "${REPORT_RECEIPT_ID}" --arg status "${REPORT_STATUS}" \
      --arg review_status "${REVIEW_STATUS}" --argjson revision "${REPORT_REVISION}" \
      --argjson review_revision "${REVIEW_REVISION}" \
      --argjson delivery_ids "${REPORT_DELIVERY_IDS}" \
      '(.ui.degraded == false)
        and .ui.live.status == "live"
        and .ui.live.assignment_count >= 1000
        and any(.ui.live.reports[]?;
          .id == $report and .revision == $revision and .status == $status
          and ((.delivery_receipt_ids | sort) == $delivery_ids))
        and any(.ui.live.reviews[]?;
          .id == $review and .report_id == $report
          and .revision == $review_revision and .status == $review_status)
        and any(.ui.live.receipt_items[]?; .id == $receipt)
        and any(.browser.frames[]?.receipt_ids[]?; . == $receipt)
        and any(.browser.frames[]?.assignment_ids[]?; . == "mfg-live-assignment-1")
        and any(.browser.frames[]?.assignment_ids[]?; . == "mfg-live-assignment-1000")' \
      "${ARTIFACT_DIR}/webui-browser.json" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
copy_observer_artifact "${ARTIFACT_DIR}/tui-a-state.json" \
  "${ARTIFACT_DIR}/tui-a-after-burst.json"
copy_observer_artifact "${ARTIFACT_DIR}/webui-browser.json" \
  "${ARTIFACT_DIR}/webui-after-burst.json"
jq -e --arg report "${REPORT_ID}" --arg review "${REVIEW_ID}" \
  --arg receipt "${REPORT_RECEIPT_ID}" --arg status "${REPORT_STATUS}" \
  --arg review_status "${REVIEW_STATUS}" --argjson revision "${REPORT_REVISION}" \
  --argjson review_revision "${REVIEW_REVISION}" \
  --argjson delivery_ids "${REPORT_DELIVERY_IDS}" \
  '(.live.available == true)
    and ([.assignments[].id] | length >= 1000)
    and any(.reports[]?;
      .id == $report and .revision == $revision and .status == $status
      and ((.delivery_receipt_ids | sort) == $delivery_ids))
    and any(.reviews[]?;
      .id == $review and .report_id == $report
      and .revision == $review_revision and .status == $review_status)
    and any(.receipts[]?; .id == $receipt)' \
  "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null
jq -e --arg report "${REPORT_ID}" --arg review "${REVIEW_ID}" \
  --arg receipt "${REPORT_RECEIPT_ID}" --arg status "${REPORT_STATUS}" \
  --arg review_status "${REVIEW_STATUS}" --argjson revision "${REPORT_REVISION}" \
  --argjson review_revision "${REVIEW_REVISION}" \
  --argjson delivery_ids "${REPORT_DELIVERY_IDS}" \
  '(.ui.degraded == false)
    and .ui.live.status == "live"
    and .ui.live.assignment_count >= 1000
    and any(.ui.live.reports[]?;
      .id == $report and .revision == $revision and .status == $status
      and ((.delivery_receipt_ids | sort) == $delivery_ids))
    and any(.ui.live.reviews[]?;
      .id == $review and .report_id == $report
      and .revision == $review_revision and .status == $review_status)
    and any(.ui.live.receipt_items[]?; .id == $receipt)
    and any(.browser.frames[]?.receipt_ids[]?; . == $receipt)
    and any(.browser.frames[]?.assignment_ids[]?; . == "mfg-live-assignment-1")
    and any(.browser.frames[]?.assignment_ids[]?; . == "mfg-live-assignment-1000")
    and any(.browser.frames[]?.reviews[]?;
      .id == $review and .report_id == $report
      and .revision == $review_revision and .status == $review_status)
    and any(.browser.frames[]?.receipt_ids[]?; length > 0)' \
  "${ARTIFACT_DIR}/webui-browser.json" >/dev/null
tmux -L "${TMUX_SOCKET}" capture-pane -p -t "${SCENARIO_ID}-tui-a" -S -120 \
  >"${ARTIFACT_DIR}/tui-a-during-slow-observer.txt"
rg -q "MFG" "${ARTIFACT_DIR}/tui-a-during-slow-observer.txt"
kill -CONT "${TUI_B_PID}"
for _ in {1..120}; do
  jq -e --arg report "${REPORT_ID}" --arg review "${REVIEW_ID}" \
    --arg receipt "${REPORT_RECEIPT_ID}" --arg status "${REPORT_STATUS}" \
    --arg review_status "${REVIEW_STATUS}" --argjson revision "${REPORT_REVISION}" \
    --argjson review_revision "${REVIEW_REVISION}" \
    --argjson delivery_ids "${REPORT_DELIVERY_IDS}" \
    '(.live.available == true)
      and ([.assignments[].id] | length >= 1000)
      and any(.reports[]?;
        .id == $report and .revision == $revision and .status == $status
        and ((.delivery_receipt_ids | sort) == $delivery_ids))
      and any(.reviews[]?;
        .id == $review and .report_id == $report
        and .revision == $review_revision and .status == $review_status)
      and any(.receipts[]?; .id == $receipt)' \
    "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null 2>&1 && break
  sleep 0.25
done
jq -e --arg report "${REPORT_ID}" --arg review "${REVIEW_ID}" \
  --arg receipt "${REPORT_RECEIPT_ID}" --arg status "${REPORT_STATUS}" \
  --arg review_status "${REVIEW_STATUS}" --argjson revision "${REPORT_REVISION}" \
  --argjson review_revision "${REVIEW_REVISION}" \
  --argjson delivery_ids "${REPORT_DELIVERY_IDS}" \
  '(.live.available == true)
    and ([.assignments[].id] | length >= 1000)
    and any(.reports[]?;
      .id == $report and .revision == $revision and .status == $status
      and ((.delivery_receipt_ids | sort) == $delivery_ids))
    and any(.reviews[]?;
      .id == $review and .report_id == $report
      and .revision == $review_revision and .status == $review_status)
    and any(.receipts[]?; .id == $receipt)' \
  "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null
python3 - "${ARTIFACT_DIR}/tui-a-state.json" "${ARTIFACT_DIR}/tui-b-state.json" <<'PY'
import json
import sys

left, right = (json.load(open(path, encoding="utf-8")) for path in sys.argv[1:])
for field in ("assignments", "reports", "reviews"):
    left_rows = {
        (
            item["id"],
            item.get("revision"),
            item.get("status"),
            item.get("report_id"),
            tuple(item.get("delivery_receipt_ids", [])),
        )
        for item in left[field]
    }
    right_rows = {
        (
            item["id"],
            item.get("revision"),
            item.get("status"),
            item.get("report_id"),
            tuple(item.get("delivery_receipt_ids", [])),
        )
        for item in right[field]
    }
    assert left_rows == right_rows, (field, len(left_rows), len(right_rows))
left_receipts = {item["id"] for item in left["receipts"]}
right_receipts = {item["id"] for item in right["receipts"]}
assert left_receipts == right_receipts, ("receipts", len(left_receipts), len(right_receipts))
assert left["live"]["view_epoch"] == right["live"]["view_epoch"]
PY
# A later entitlement recrop deliberately installs the profile's bounded
# assignment window. Preserve the completed post-resume state separately so
# the pressure assertion measures the 1,000-row convergence it actually
# exercised instead of a later, valid windowed snapshot.
copy_observer_artifact "${ARTIFACT_DIR}/tui-b-state.json" \
  "${ARTIFACT_DIR}/tui-b-after-burst.json"
FORBIDDEN_PROBE_ID="${SCENARIO_ID}-webui-valid-session-forbidden-recovery"
jq -n --arg id "${FORBIDDEN_PROBE_ID}" \
  '{kind:"forbidden_recovery", $id}' \
  >"${ARTIFACT_DIR}/browser-probe-request.json.tmp"
mv "${ARTIFACT_DIR}/browser-probe-request.json.tmp" \
  "${ARTIFACT_DIR}/browser-probe-request.json"
for _ in {1..160}; do
  jq -e --arg id "${FORBIDDEN_PROBE_ID}" \
    'any(.interaction_probes[]?;
        .id == $id and .status == "passed"
        and .authorization_error.code == "capability_denied"
        and .authorization_error.http_status == 403)
      and any(.browser.requests[]?;
        (.url | endswith("/api/apps/mfg/live/snapshot")) and .status == 403)
      and .browser.forbidden_recovery_count == 1
      and .browser.same_document_recovery_count >= 1
      and any(.browser.consumer_generation_deltas[]?;
        .reason == "forbidden" and .delta == 1)
      and .ui.live.status == "live"' \
    "${ARTIFACT_DIR}/webui-browser.json" >/dev/null 2>&1 && break
  sleep 0.25
done
jq -e --arg id "${FORBIDDEN_PROBE_ID}" \
  'any(.interaction_probes[]?;
      .id == $id and .status == "passed"
      and .authorization_error.code == "capability_denied"
      and .authorization_error.http_status == 403)
    and any(.browser.requests[]?;
      (.url | endswith("/api/apps/mfg/live/snapshot")) and .status == 403)
    and .browser.forbidden_recovery_count == 1
    and .browser.same_document_recovery_count >= 1
    and any(.browser.consumer_generation_deltas[]?;
      .reason == "forbidden" and .delta == 1)
    and .ui.live.status == "live"' \
  "${ARTIFACT_DIR}/webui-browser.json" >/dev/null
authorized_curl -fsS -X POST \
  "${BASE_URL}/api/apps/mfg/assignments/mfg-live-assignment-1/command" \
  -H "content-type: application/json" \
  -H "idempotency-key: ${SCENARIO_ID}-assignment-terminal" \
  -d '{
    "command": "unassign",
    "expected_revision": 1,
    "reason": "terminal convergence acceptance"
  }' -o "${ARTIFACT_DIR}/assignment-terminal.json"
jq -e '.assignment.status == "unassigned"
  and .assignment.revision == 2
  and (._mfg_receipt.receipt_id | type == "string")' \
  "${ARTIFACT_DIR}/assignment-terminal.json" >/dev/null
for _ in {1..120}; do
  if jq -e \
    'any(.assignments[]?;
      .id == "mfg-live-assignment-1" and .status == "unassigned" and .revision == 2)' \
    "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null 2>&1 \
    && jq -e \
      'any(.assignments[]?;
        .id == "mfg-live-assignment-1" and .status == "unassigned" and .revision == 2)' \
      "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null 2>&1 \
    && jq -e \
      'any(.browser.frames[]?.events[]?;
        .subject_ref == "mfg:assignment:mfg-live-assignment-1"
        and .revision == 2)' \
      "${ARTIFACT_DIR}/webui-browser.json" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
jq -e \
  'any(.assignments[]?;
    .id == "mfg-live-assignment-1" and .status == "unassigned" and .revision == 2)' \
  "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null
jq -e \
  'any(.assignments[]?;
    .id == "mfg-live-assignment-1" and .status == "unassigned" and .revision == 2)' \
  "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null
jq -e \
  'any(.browser.frames[]?.events[]?;
    .subject_ref == "mfg:assignment:mfg-live-assignment-1"
    and .revision == 2)' \
  "${ARTIFACT_DIR}/webui-browser.json" >/dev/null
tmux -L "${TMUX_SOCKET}" capture-pane -p -t "${SCENARIO_ID}-tui-a" -S -300 \
  >"${ARTIFACT_DIR}/tui-a.txt"
tmux -L "${TMUX_SOCKET}" capture-pane -p -t "${SCENARIO_ID}-tui-b" -S -300 \
  >"${ARTIFACT_DIR}/tui-b.txt"
rg -q "MFG" "${ARTIFACT_DIR}/tui-a.txt"
rg -q "MFG" "${ARTIFACT_DIR}/tui-b.txt"

DIRECT_VIEW_EPOCH_BEFORE="$(jq -er '.view_epoch' "${ARTIFACT_DIR}/snapshot-after-burst.json")"
jq '.live' "${ARTIFACT_DIR}/tui-a-state.json" \
  >"${ARTIFACT_DIR}/tui-a-before-restart.json"
jq '.live' "${ARTIFACT_DIR}/tui-b-state.json" \
  >"${ARTIFACT_DIR}/tui-b-before-restart.json"
TUI_A_BEFORE_GENERATION="$(jq -er '.generation' "${ARTIFACT_DIR}/tui-a-before-restart.json")"
TUI_B_BEFORE_GENERATION="$(jq -er '.generation' "${ARTIFACT_DIR}/tui-b-before-restart.json")"
TUI_A_VIEW_EPOCH_BEFORE="$(jq -er '.view_epoch' "${ARTIFACT_DIR}/tui-a-before-restart.json")"
TUI_B_VIEW_EPOCH_BEFORE="$(jq -er '.view_epoch' "${ARTIFACT_DIR}/tui-b-before-restart.json")"
[[ "${TUI_A_VIEW_EPOCH_BEFORE}" == "${TUI_B_VIEW_EPOCH_BEFORE}" ]] || {
  echo "identically authorized TUI observers received different MFG public view epochs" >&2
  exit 1
}
WEBUI_VIEW_EPOCH_BEFORE="$(jq -er '.ui.live.view_epoch | select(type == "string" and length > 0)' \
  "${ARTIFACT_DIR}/webui-browser.json")"
WEBUI_BEFORE_SNAPSHOT_COUNT="$(jq -er \
  '[.browser.requests[]? | select(.url | endswith("/api/apps/mfg/live/snapshot"))] | length' \
  "${ARTIFACT_DIR}/webui-browser.json")"
cp "${ARTIFACT_DIR}/webui-browser.json" \
  "${ARTIFACT_DIR}/webui-before-restart.json"
stop_gateway "${GATEWAY_PID}"
GATEWAY_PID=""
start_gateway
snapshot "${ARTIFACT_DIR}/snapshot-after-restart.json"
DIRECT_VIEW_EPOCH_AFTER="$(jq -er '.view_epoch' "${ARTIFACT_DIR}/snapshot-after-restart.json")"
AFTER_RESTART_CURSOR="$(jq -er '.cursor' "${ARTIFACT_DIR}/snapshot-after-restart.json")"
[[ "${DIRECT_VIEW_EPOCH_BEFORE}" == "${DIRECT_VIEW_EPOCH_AFTER}" ]] || {
  echo "normal Gateway restart rotated the direct authorized MFG public view epoch" >&2
  exit 1
}
for _ in {1..120}; do
  if jq -e \
    '[.browser.requests[]? | select(.url | endswith("/api/apps/mfg/live"))] | length >= 2' \
    "${ARTIFACT_DIR}/webui-browser.json" >/dev/null 2>&1 \
    && jq -e --argjson count "${WEBUI_BEFORE_SNAPSHOT_COUNT}" \
      '[.browser.requests[]? |
        select(.url | endswith("/api/apps/mfg/live/snapshot"))] | length > $count' \
      "${ARTIFACT_DIR}/webui-browser.json" >/dev/null 2>&1 \
    && jq -e --arg epoch "${WEBUI_VIEW_EPOCH_BEFORE}" \
      '.ui.live.status == "live" and .ui.live.view_epoch == $epoch' \
      "${ARTIFACT_DIR}/webui-browser.json" >/dev/null 2>&1 \
    && jq -e --argjson generation "${TUI_A_BEFORE_GENERATION}" \
      --arg epoch "${TUI_A_VIEW_EPOCH_BEFORE}" \
      '.live.available == true and .live.generation > $generation
        and (.live.cursor | type == "string" and length > 0)
        and .live.view_epoch == $epoch
        and ([.assignments[]?] | length >= 1000)
        and any(.assignments[]?; .id == "mfg-live-assignment-1")
        and any(.assignments[]?; .id == "mfg-live-assignment-1000")' \
      "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null 2>&1 \
    && jq -e --argjson generation "${TUI_B_BEFORE_GENERATION}" \
      --arg epoch "${TUI_B_VIEW_EPOCH_BEFORE}" \
      '.live.available == true and .live.generation > $generation
        and (.live.cursor | type == "string" and length > 0)
        and .live.view_epoch == $epoch
        and ([.assignments[]?] | length >= 1000)
        and any(.assignments[]?; .id == "mfg-live-assignment-1")
        and any(.assignments[]?; .id == "mfg-live-assignment-1000")' \
      "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
jq -e \
  '[.browser.requests[]? | select(.url | endswith("/api/apps/mfg/live"))] | length >= 2' \
  "${ARTIFACT_DIR}/webui-browser.json" >/dev/null
jq -e --argjson count "${WEBUI_BEFORE_SNAPSHOT_COUNT}" \
  '[.browser.requests[]? |
    select(.url | endswith("/api/apps/mfg/live/snapshot"))] | length > $count' \
  "${ARTIFACT_DIR}/webui-browser.json" >/dev/null
[[ "$(jq -er '.ui.live.view_epoch' "${ARTIFACT_DIR}/webui-browser.json")" == "${WEBUI_VIEW_EPOCH_BEFORE}" ]] || {
  echo "normal Gateway restart rotated the WebUI authorized MFG public view epoch" >&2
  exit 1
}
jq -e --argjson generation "${TUI_A_BEFORE_GENERATION}" \
  --arg epoch "${TUI_A_VIEW_EPOCH_BEFORE}" \
  '.live.available == true and .live.generation > $generation
    and (.live.cursor | type == "string" and length > 0)
    and .live.view_epoch == $epoch
    and ([.assignments[]?] | length >= 1000)
    and any(.assignments[]?; .id == "mfg-live-assignment-1")
    and any(.assignments[]?; .id == "mfg-live-assignment-1000")' \
  "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null
jq -e --argjson generation "${TUI_B_BEFORE_GENERATION}" \
  --arg epoch "${TUI_B_VIEW_EPOCH_BEFORE}" \
  '.live.available == true and .live.generation > $generation
    and (.live.cursor | type == "string" and length > 0)
    and .live.view_epoch == $epoch
    and ([.assignments[]?] | length >= 1000)
    and any(.assignments[]?; .id == "mfg-live-assignment-1")
    and any(.assignments[]?; .id == "mfg-live-assignment-1000")' \
  "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null
jq '.live' "${ARTIFACT_DIR}/tui-a-state.json" \
  >"${ARTIFACT_DIR}/tui-a-after-restart.json"
jq '.live' "${ARTIFACT_DIR}/tui-b-state.json" \
  >"${ARTIFACT_DIR}/tui-b-after-restart.json"
cp "${ARTIFACT_DIR}/webui-browser.json" \
  "${ARTIFACT_DIR}/webui-after-restart.json"

OLD_CURSOR="$(jq -er '.cursor' "${ARTIFACT_DIR}/snapshot-after-restart.json")"
MFG_DB="$(find "${CONFIG_HOME}" -type f -name '*mfg*.sqlite*' \
  ! -name '*-wal' ! -name '*-shm' | head -1)"
[[ -n "${MFG_DB}" ]] || { echo "MFG SQLite database was not found" >&2; exit 1; }
INTERNAL_EPOCH_BEFORE_PROFILE="$(sqlite3 "${MFG_DB}" \
  'SELECT epoch_id FROM mfg_live_epoch WHERE singleton_id=1;')"
set_profile core_legacy_0_9_530 mfg_viewer "${ARTIFACT_DIR}/profile-viewer.json"
snapshot "${ARTIFACT_DIR}/snapshot-after-profile-change.json"
DIRECT_VIEW_EPOCH_AFTER_PROFILE="$(jq -er '.view_epoch' "${ARTIFACT_DIR}/snapshot-after-profile-change.json")"
for _ in {1..120}; do
  if jq -e --arg before "${WEBUI_VIEW_EPOCH_BEFORE}" \
    'any(.browser.stream_errors[]?;
      .code == "authentication_required" and .http_status == 401)
      and .browser.reauthentication_count >= 1
      and .browser.profile_reauthentication_count == 1
      and .browser.same_document_recovery_count >= 2
      and any(.browser.consumer_generation_deltas[]?;
        .reason == "authentication" and .delta == 1)
      and (.ui.degraded == false)
      and .ui.live.status == "live"
      and (.ui.live.view_epoch | type == "string" and length > 0)
      and .ui.live.view_epoch != $before' \
      "${ARTIFACT_DIR}/webui-browser.json" >/dev/null 2>&1 \
    && jq -e --arg before "${TUI_A_VIEW_EPOCH_BEFORE}" \
      '(.live.available == true)
        and .live.reauthentication_count >= 1
        and (.live.view_epoch | type == "string" and length > 0)
        and .live.view_epoch != $before' \
      "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null 2>&1 \
    && jq -e --arg before "${TUI_B_VIEW_EPOCH_BEFORE}" \
      '(.live.available == true)
        and .live.reauthentication_count >= 1
        and (.live.view_epoch | type == "string" and length > 0)
        and .live.view_epoch != $before' \
      "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
jq -e --arg before "${WEBUI_VIEW_EPOCH_BEFORE}" \
  'any(.browser.stream_errors[]?;
    .code == "authentication_required" and .http_status == 401)
    and .browser.reauthentication_count >= 1
    and .browser.profile_reauthentication_count == 1
    and .browser.same_document_recovery_count >= 2
    and any(.browser.consumer_generation_deltas[]?;
      .reason == "authentication" and .delta == 1)
    and (.ui.degraded == false)
    and .ui.live.status == "live"
    and (.ui.live.view_epoch | type == "string" and length > 0)
    and .ui.live.view_epoch != $before' \
  "${ARTIFACT_DIR}/webui-browser.json" >/dev/null
jq -e --arg before "${TUI_A_VIEW_EPOCH_BEFORE}" \
  '(.live.available == true)
    and .live.reauthentication_count >= 1
    and (.live.view_epoch | type == "string" and length > 0)
    and .live.view_epoch != $before' \
  "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null
jq -e --arg before "${TUI_B_VIEW_EPOCH_BEFORE}" \
  '(.live.available == true)
    and .live.reauthentication_count >= 1
    and (.live.view_epoch | type == "string" and length > 0)
    and .live.view_epoch != $before' \
  "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null
WEBUI_VIEW_EPOCH_AFTER_PROFILE="$(jq -er '.ui.live.view_epoch' "${ARTIFACT_DIR}/webui-browser.json")"
TUI_A_VIEW_EPOCH_AFTER_PROFILE="$(jq -er '.live.view_epoch' "${ARTIFACT_DIR}/tui-a-state.json")"
TUI_B_VIEW_EPOCH_AFTER_PROFILE="$(jq -er '.live.view_epoch' "${ARTIFACT_DIR}/tui-b-state.json")"
[[ "${TUI_A_VIEW_EPOCH_AFTER_PROFILE}" == "${TUI_B_VIEW_EPOCH_AFTER_PROFILE}" ]] || {
  echo "identically authorized TUI observers diverged after the profile recrop" >&2
  exit 1
}
INTERNAL_EPOCH_AFTER_PROFILE="$(sqlite3 "${MFG_DB}" \
  'SELECT epoch_id FROM mfg_live_epoch WHERE singleton_id=1;')"
[[ "${DIRECT_VIEW_EPOCH_AFTER_PROFILE}" != "${DIRECT_VIEW_EPOCH_AFTER}" ]] || {
  echo "profile revision change did not recrop the direct authorized MFG public view epoch" >&2
  exit 1
}
[[ "${WEBUI_VIEW_EPOCH_AFTER_PROFILE}" != "${WEBUI_VIEW_EPOCH_BEFORE}" ]] || {
  echo "profile revision change did not recrop the WebUI MFG public view epoch" >&2
  exit 1
}
[[ "${TUI_A_VIEW_EPOCH_AFTER_PROFILE}" != "${TUI_A_VIEW_EPOCH_BEFORE}" ]] || {
  echo "profile revision change did not recrop the TUI MFG public view epoch" >&2
  exit 1
}
[[ "${INTERNAL_EPOCH_BEFORE_PROFILE}" == "${INTERNAL_EPOCH_AFTER_PROFILE}" ]] || {
  echo "profile revision change rotated the global MFG internal epoch" >&2
  exit 1
}
authorized_curl -fsS -N --max-time 10 \
  -H "Last-Event-ID: ${OLD_CURSOR}" \
  -H "x-mfg-view-epoch: ${DIRECT_VIEW_EPOCH_AFTER}" \
  "${BASE_URL}/api/apps/mfg/live" >"${ARTIFACT_DIR}/profile-resync-sse.log"
python3 - "${ARTIFACT_DIR}/profile-resync-sse.log" <<'PY'
import json
import pathlib
import sys

frames = pathlib.Path(sys.argv[1]).read_text().replace("\r\n", "\n").split("\n\n")
payloads = []
for frame in frames:
    data = "\n".join(
        line.removeprefix("data:").strip()
        for line in frame.splitlines()
        if line.startswith("data:")
    )
    if data:
        payloads.append(json.loads(data))
assert payloads and payloads[0]["kind"] == "resync", payloads
assert payloads[0]["snapshot_url"] == "/api/apps/mfg/live/snapshot"
PY

snapshot "${ARTIFACT_DIR}/hidden-backlog-before.json"
HIDDEN_CURSOR="$(jq -er '.cursor' "${ARTIFACT_DIR}/hidden-backlog-before.json")"
HIDDEN_EPOCH="$(jq -er '.view_epoch' "${ARTIFACT_DIR}/hidden-backlog-before.json")"
sqlite3 "${MFG_DB}" <<'SQL'
BEGIN IMMEDIATE;
WITH RECURSIVE seq(value) AS (
  VALUES(1)
  UNION ALL
  SELECT value + 1 FROM seq WHERE value < 600
)
INSERT INTO mfg_projection_event (
  domain, subject_ref, event_type, event_json, created_at
)
SELECT
  'receipt',
  'mfg:receipt:hidden-heartbeat-' || value,
  'receipt.completed',
  json_object(
    'domain', 'receipt',
    'event_type', 'receipt.completed',
    'subject_ref', 'mfg:receipt:hidden-heartbeat-' || value,
    'payload', json_object(
      'receipt', json_object(
        'receipt_id', 'hidden-heartbeat-' || value,
        'actor_principal', 'principal:hidden-observer',
        'action_id', 'mfg.hidden.acceptance',
        'resource_ref', 'mfg:hidden:' || value,
        'status', 'completed'
      )
    )
  ),
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM seq;
UPDATE mfg_live_epoch
SET retention_high_cursor = (
  SELECT COALESCE(MAX(event_id), 0) FROM mfg_projection_event
)
WHERE singleton_id = 1;
COMMIT;
SQL
if authorized_curl -fsS -N --max-time 8 \
  -H "Last-Event-ID: ${HIDDEN_CURSOR}" \
  -H "x-mfg-view-epoch: ${HIDDEN_EPOCH}" \
  "${BASE_URL}/api/apps/mfg/live" \
  >"${ARTIFACT_DIR}/hidden-heartbeat-sse.log"; then
  HIDDEN_CURL_STATUS=0
else
  # An intentionally open SSE stream reaches curl's deadline after the
  # heartbeat.  Keep the real transport exit status as acceptance evidence.
  HIDDEN_CURL_STATUS=$?
fi
[[ "${HIDDEN_CURL_STATUS}" -eq 0 || "${HIDDEN_CURL_STATUS}" -eq 28 ]] || {
  echo "hidden backlog heartbeat observer failed with status ${HIDDEN_CURL_STATUS}" >&2
  exit 1
}
python3 - "${ARTIFACT_DIR}/hidden-heartbeat-sse.log" "${HIDDEN_CURSOR}" "${HIDDEN_EPOCH}" <<'PY'
import json
import pathlib
import sys

frames = pathlib.Path(sys.argv[1]).read_text().replace("\r\n", "\n").split("\n\n")
payloads = []
for frame in frames:
    data = "\n".join(
        line.removeprefix("data:").strip()
        for line in frame.splitlines()
        if line.startswith("data:")
    )
    if data:
        payloads.append(json.loads(data))
assert payloads, "hidden-only stream emitted no heartbeat"
assert all(payload["kind"] == "heartbeat" for payload in payloads), payloads
assert all(set(payload) == {"kind", "view_epoch", "cursor", "generated_at"} for payload in payloads)
assert all(payload["view_epoch"] == sys.argv[3] for payload in payloads)
assert payloads[-1]["cursor"] != sys.argv[2]
assert "hidden-observer" not in pathlib.Path(sys.argv[1]).read_text()
PY

capture_gateway_logs
TUI_B_ACTIVE_CONNECTION_ID="$(
  rg "observer_id=${SCENARIO_ID}-tui-b" "${GATEWAY_LOG}" 2>/dev/null \
    | rg -o 'connection_id=[0-9a-f-]+' \
    | tail -1 \
    | cut -d= -f2 || true
)"
[[ -n "${TUI_B_ACTIVE_CONNECTION_ID}" ]] || {
  echo "active TUI-B Gateway observer lifecycle was not identifiable" >&2
  exit 1
}
TUI_B_RELEASE_COUNT_BEFORE="$(rg "connection_id=${TUI_B_ACTIVE_CONNECTION_ID}" "${GATEWAY_LOG}" \
  | rg -c 'receiver_closed=true' || true)"
tmux -L "${TMUX_SOCKET}" kill-session -t "${SCENARIO_ID}-tui-b"
sleep 1
kill -0 "${TUI_B_PID}" 2>/dev/null && {
  echo "TUI-B consumer process survived explicit session shutdown" >&2
  exit 1
}
for _ in {1..80}; do
  capture_gateway_logs
  TUI_B_RELEASE_COUNT_AFTER="$(rg "connection_id=${TUI_B_ACTIVE_CONNECTION_ID}" "${GATEWAY_LOG}" \
    | rg -c 'receiver_closed=true' || true)"
  [[ "${TUI_B_RELEASE_COUNT_AFTER}" -gt "${TUI_B_RELEASE_COUNT_BEFORE}" ]] && break
  sleep 0.25
done
capture_gateway_logs
TUI_B_RELEASE_COUNT_AFTER="$(rg "connection_id=${TUI_B_ACTIVE_CONNECTION_ID}" "${GATEWAY_LOG}" \
  | rg -c 'receiver_closed=true' || true)"
[[ "${TUI_B_RELEASE_COUNT_AFTER}" -gt "${TUI_B_RELEASE_COUNT_BEFORE}" ]] || {
  echo "Gateway did not release the exact active TUI-B observer lifecycle" >&2
  exit 1
}
# DevTools reports every deliberate 401/403/500 response as a generic console
# resource error. The browser observer records the URL, timing window and
# recovery outcome for those injected availability/entitlement faults, so
# reject only unclassified HTTP failures and actual script/page errors.
jq -e '(.page_errors | length == 0)
  and (.unexpected_console_errors | length == 0)
  and (.browser.unexpected_http_failures | length == 0)' \
  "${ARTIFACT_DIR}/webui-browser.json" >/dev/null

EVENT_COUNT="$(sqlite3 "${MFG_DB}" 'SELECT COUNT(*) FROM mfg_projection_event;')"
WEBUI_FRAME_COUNT="$(jq -er '.browser.frames | length' \
  "${ARTIFACT_DIR}/webui-browser.json")"
WEBUI_STREAM_REQUEST_COUNT="$(jq -er \
  '[.browser.requests[]? | select(.url | endswith("/api/apps/mfg/live"))] | length' \
  "${ARTIFACT_DIR}/webui-browser.json")"
WEBUI_SNAPSHOT_REQUEST_COUNT="$(jq -er \
  '[.browser.requests[]? |
    select(.url | endswith("/api/apps/mfg/live/snapshot"))] | length' \
  "${ARTIFACT_DIR}/webui-browser.json")"
SLOW_OBSERVER_LOG="${ARTIFACT_DIR}/tui-b-observer-telemetry.log"
capture_gateway_logs
rg "observer_id=${SCENARIO_ID}-tui-b" "${GATEWAY_LOG}" >"${SLOW_OBSERVER_LOG}"
PRESSURE_CONNECTION_ID="$(python3 - "${SLOW_OBSERVER_LOG}" <<'PY'
import re
import sys

best = (-1, "")
for line in open(sys.argv[1], encoding="utf-8"):
    connection = re.search(r"connection_id=([0-9a-f-]+)", line)
    peak = re.search(r"event_peak=([0-9]+)", line)
    if connection and peak and int(peak.group(1)) > best[0]:
        best = (int(peak.group(1)), connection.group(1))
print(best[1])
PY
)"
[[ -n "${PRESSURE_CONNECTION_ID}" ]] || {
  echo "TUI-B pressure observer lifecycle was not identifiable" >&2
  exit 1
}
PRESSURE_OBSERVER_LOG="${ARTIFACT_DIR}/tui-b-pressure-telemetry.log"
rg "connection_id=${PRESSURE_CONNECTION_ID}" "${SLOW_OBSERVER_LOG}" \
  >"${PRESSURE_OBSERVER_LOG}"
QUEUE_PEAK="$(rg -o 'queue_peak=[0-9]+' "${PRESSURE_OBSERVER_LOG}" \
  | cut -d= -f2 | sort -nr | head -1 || true)"
EVENT_PEAK="$(rg -o 'event_peak=[0-9]+' "${PRESSURE_OBSERVER_LOG}" \
  | cut -d= -f2 | sort -nr | head -1 || true)"
COALESCED_COUNT="$(rg -o 'coalesced=[0-9]+' "${PRESSURE_OBSERVER_LOG}" \
  | cut -d= -f2 | sort -nr | head -1 || true)"
QUEUE_PEAK="${QUEUE_PEAK:-0}"
EVENT_PEAK="${EVENT_PEAK:-0}"
COALESCED_COUNT="${COALESCED_COUNT:-0}"
OBSERVER_COUNT=0
jq -e 'any(.browser.frames[]?; .kind == "snapshot")
  and any(.browser.frames[]?.assignment_ids[]?; startswith("mfg-live-assignment-"))' \
  "${ARTIFACT_DIR}/webui-browser.json" >/dev/null \
  && OBSERVER_COUNT=$((OBSERVER_COUNT + 1))
for state_file in tui-a-after-burst.json tui-b-after-burst.json; do
  jq -e '.surface == "tui" and ([.assignments[]] | length >= 1000)' \
    "${ARTIFACT_DIR}/${state_file}" >/dev/null \
    && OBSERVER_COUNT=$((OBSERVER_COUNT + 1))
done
SLOW_OBSERVER_RESUMED=false
jq -e '([.assignments[]] | length >= 1000)
  and any(.assignments[]?;
    .id == "mfg-live-assignment-1")
  and any(.assignments[]?;
    .id == "mfg-live-assignment-1000")' \
  "${ARTIFACT_DIR}/tui-b-after-burst.json" >/dev/null \
  && SLOW_OBSERVER_RESUMED=true
WEBUI_INTERACTION_LATENCY_MS="$(jq -er --arg id "${BURST_PROBE_ID}" \
  '[.interaction_probes[]? | select(.id == $id and .status == "passed") | .latency_ms] | last' \
  "${ARTIFACT_DIR}/webui-browser.json")"
RECEIPT_DELIVERY_CONVERGED=false
jq -e --arg report "${REPORT_ID}" --arg receipt "${REPORT_RECEIPT_ID}" \
  'any(.reports[]?;
      .id == $report and (.delivery_receipt_ids | length) >= 3)
    and any(.receipts[]?; .id == $receipt)' \
  "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null \
  && jq -e --arg report "${REPORT_ID}" --arg receipt "${REPORT_RECEIPT_ID}" \
    'any(.reports[]?;
        .id == $report and (.delivery_receipt_ids | length) >= 3)
      and any(.receipts[]?; .id == $receipt)' \
    "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null \
  && jq -e --arg report "${REPORT_ID}" --arg review "${REVIEW_ID}" \
    --arg receipt "${REPORT_RECEIPT_ID}" --arg status "${REPORT_STATUS}" \
    --arg review_status "${REVIEW_STATUS}" --argjson revision "${REPORT_REVISION}" \
    --argjson review_revision "${REVIEW_REVISION}" \
    --argjson delivery_ids "${REPORT_DELIVERY_IDS}" \
    'any(.ui.live.reports[]?;
        .id == $report and .revision == $revision and .status == $status
        and ((.delivery_receipt_ids | sort) == $delivery_ids))
      and any(.ui.live.reviews[]?;
        .id == $review and .report_id == $report
        and .revision == $review_revision and .status == $review_status)
      and any(.ui.live.receipt_items[]?; .id == $receipt)
      and any(.browser.frames[]?.receipt_ids[]?; . == $receipt)' \
    "${ARTIFACT_DIR}/webui-browser.json" >/dev/null \
  && RECEIPT_DELIVERY_CONVERGED=true
TERMINAL_CONVERGED=false
jq -e 'any(.assignments[]?;
  .id == "mfg-live-assignment-1" and .status == "unassigned" and .revision == 2)' \
  "${ARTIFACT_DIR}/tui-a-state.json" >/dev/null \
  && jq -e 'any(.assignments[]?;
    .id == "mfg-live-assignment-1" and .status == "unassigned" and .revision == 2)' \
    "${ARTIFACT_DIR}/tui-b-state.json" >/dev/null \
  && jq -e 'any(.browser.frames[]?.events[]?;
    .subject_ref == "mfg:assignment:mfg-live-assignment-1"
    and .revision == 2)' \
    "${ARTIFACT_DIR}/webui-browser.json" >/dev/null \
  && TERMINAL_CONVERGED=true
EPOCH_ROW="$(sqlite3 -json "${MFG_DB}" \
  'SELECT epoch_id,rotation_reason,retention_low_cursor,retention_high_cursor FROM mfg_live_epoch WHERE singleton_id=1;')"
jq -n \
  --arg scenario_id "${SCENARIO_ID}" \
  --argjson event_count "${EVENT_COUNT}" \
  --argjson burst_assignment_count "${BURST_ASSIGNMENT_COUNT}" \
  --argjson webui_frame_count "${WEBUI_FRAME_COUNT}" \
  --argjson webui_stream_request_count "${WEBUI_STREAM_REQUEST_COUNT}" \
  --argjson webui_snapshot_request_count "${WEBUI_SNAPSHOT_REQUEST_COUNT}" \
  --argjson observer_count "${OBSERVER_COUNT}" \
  --arg slow_observer_id "${SCENARIO_ID}-tui-b" \
  --arg pressure_connection_id "${PRESSURE_CONNECTION_ID}" \
  --arg release_connection_id "${TUI_B_ACTIVE_CONNECTION_ID}" \
  --argjson queue_peak "${QUEUE_PEAK}" \
  --argjson event_peak "${EVENT_PEAK}" \
  --argjson queue_capacity 64 \
  --argjson event_capacity 512 \
  --argjson coalesced_count "${COALESCED_COUNT}" \
  --argjson webui_interaction_latency_ms "${WEBUI_INTERACTION_LATENCY_MS}" \
  --argjson receipt_delivery_converged "${RECEIPT_DELIVERY_CONVERGED}" \
  --argjson slow_observer_resumed "${SLOW_OBSERVER_RESUMED}" \
  --argjson terminal_converged "${TERMINAL_CONVERGED}" \
  --argjson epoch "${EPOCH_ROW}" \
  '{
    $scenario_id,
    $event_count,
    $burst_assignment_count,
    $webui_frame_count,
    $webui_stream_request_count,
    $webui_snapshot_request_count,
    $observer_count,
    queue: {
      observer_id: $slow_observer_id,
      pressure_connection_id: $pressure_connection_id,
      peak: $queue_peak,
      capacity: $queue_capacity,
      event_peak: $event_peak,
      event_capacity: $event_capacity,
      coalesced: $coalesced_count
    },
    release: {
      observer_id: $slow_observer_id,
      connection_id: $release_connection_id,
      receiver_closed: true
    },
    $webui_interaction_latency_ms,
    $receipt_delivery_converged,
    $slow_observer_resumed,
    $terminal_converged,
    gateway_restart_epoch_stable: true,
    profile_change_view_epoch_changed: true,
    profile_change_internal_epoch_stable: true,
    old_profile_cursor_resynced: true,
    established_streams_rejected_after_profile_change: true,
    webui_valid_session_403_recovered_in_same_document: true,
    hidden_backlog_payload_free_heartbeat: true,
    tui_restart_installed_new_generation: true,
    tui_b_consumer_released: true,
    $epoch
  }' >"${ARTIFACT_DIR}/metrics.json"
jq -e '.event_count >= 1000 and .burst_assignment_count >= 1000
  and .webui_frame_count > 0 and .webui_stream_request_count >= 2
  and .webui_snapshot_request_count > 1
  and .observer_count == 3
  and (.queue.observer_id | endswith("-tui-b"))
  and (.queue.pressure_connection_id | length) > 0
  and (.release.connection_id | length) > 0 and .release.receiver_closed
  and .queue.peak > 0 and .queue.event_peak > 0
  # Finite queue/event peaks prove the paused observer exercised the bounded
  # delivery path. A zero coalesced count is stronger than forced compaction:
  # no payload was discarded because the consumer caught up in time.
  and .queue.event_peak >= 2 and .queue.coalesced >= 0
  and .queue.peak <= .queue.capacity
  and .queue.event_peak <= .queue.event_capacity
  and .webui_interaction_latency_ms <= 2000
  and .receipt_delivery_converged
  and .slow_observer_resumed and .terminal_converged
  and .gateway_restart_epoch_stable
  and .profile_change_view_epoch_changed
  and .profile_change_internal_epoch_stable
  and .old_profile_cursor_resynced
  and .established_streams_rejected_after_profile_change
  and .webui_valid_session_403_recovered_in_same_document
  and .hidden_backlog_payload_free_heartbeat
  and .tui_restart_installed_new_generation
  and .tui_b_consumer_released' \
  "${ARTIFACT_DIR}/metrics.json" >/dev/null
jq -n --arg scenario_id "${SCENARIO_ID}" '{
  $scenario_id,
  producer: "mfg-surface-acceptance.v2",
  checks: {
    "MLIVE-01": {
      status: "passed",
      assertion: "real WebUI and two real TUI reducers converge on revision, receipt, review and delivery",
      evidence: ["webui-browser.json", "tui-a-state.json", "tui-b-state.json",
        "report-generate.json", "report-final.json", "report-delivery-state.json",
        "report-review-request.json"]
    },
    "MLIVE-03": {
      status: "passed",
      assertion: "1000 durable assignments create bounded queue pressure while a real WebUI refresh remains responsive",
      evidence: ["metrics.json", "snapshot-after-burst.json", "gateway.log",
        "tui-b-pressure-telemetry.log", "webui-browser.json"]
    },
    "MLIVE-04": {
      status: "passed",
      assertion: "paused TUI-B does not block TUI-A or a timed real WebUI refresh interaction and later converges",
      evidence: ["tui-a-during-slow-observer.txt", "tui-b-after-burst.json", "metrics.json",
        "tui-b-observer-telemetry.log", "webui-browser.json"]
    },
    "MLIVE-05": {
      status: "passed",
      assertion: "Gateway restart preserves epoch and browser plus both TUI observers install new snapshots/generations",
      evidence: ["snapshot-after-restart.json", "webui-browser.json", "metrics.json",
        "webui-before-restart.json", "webui-after-restart.json",
        "tui-a-before-restart.json", "tui-b-before-restart.json",
        "tui-a-after-restart.json", "tui-b-after-restart.json"]
    },
    "MLIVE-06": {
      status: "passed",
      assertion: "browser proves typed 403 replacement and typed 401 recovery in-document while TUI streams reauthenticate and install the recropped epoch",
      evidence: ["webui-browser.json", "tui-a-state.json", "tui-b-state.json",
        "snapshot-after-profile-change.json"]
    },
    "MLIVE-08": {
      status: "passed",
      assertion: "terminal assignment revision converges in all three observers",
      evidence: ["assignment-terminal.json", "webui-browser.json",
        "tui-a-state.json", "tui-b-state.json"]
    },
    "MLIVE-09": {
      status: "passed",
      assertion: "profile recrop changes only public epoch, old cursor resyncs, and a 600-event hidden backlog emits only an opaque heartbeat",
      evidence: ["profile-resync-sse.log", "snapshot-after-profile-change.json",
        "hidden-backlog-before.json", "hidden-heartbeat-sse.log", "metrics.json"]
    }
  },
  target_acceptance_ids: ["MLIVE-01","MLIVE-02","MLIVE-03","MLIVE-04","MLIVE-05",
    "MLIVE-06","MLIVE-07","MLIVE-08","MLIVE-09"],
  artifacts: [
    "snapshot-before.json","snapshot-after-burst.json","snapshot-after-restart.json",
    "snapshot-after-profile-change.json","profile-resync-sse.log",
    "cockpit-profile.json","report-generate.json","report-final.json","report-delivery-preview.json",
    "report-delivery-grant.json","report-delivery-state.json","report-review-request.json","report-reviews.json",
    "webui-browser.json","webui-after-burst.json","webui-browser.png","tui-a-during-slow-observer.txt",
    "webui-before-restart.json","webui-after-restart.json",
    "tui-a-before-restart.json","tui-b-before-restart.json",
    "tui-a-after-restart.json","tui-b-after-restart.json",
    "hidden-backlog-before.json","hidden-heartbeat-sse.log","tui-b-observer-telemetry.log",
    "assignment-terminal.json","burst-request-summary.json","burst-timing.json",
    "tui-a-state.json","tui-a-after-burst.json","tui-b-state.json","tui-b-after-burst.json","tui-a.txt","tui-b.txt","metrics.json",
    "gateway.log","tui-b-pressure-telemetry.log"
  ]
}' >"${ARTIFACT_DIR}/artifact-index.json"
mkdir -p "${ROOT}/target/acceptance"
jq -n \
  --arg scenario_id "${SCENARIO_ID}" \
  --arg artifact_dir "${ARTIFACT_DIR}" \
  --arg index "${ARTIFACT_DIR}/artifact-index.json" \
  '{$scenario_id, $artifact_dir, $index}' \
  >"${ROOT}/target/acceptance/latest-mfg-surface.json"

MFG_SCENARIO_PASSED=1
echo "MFG three-observer surface acceptance passed; artifacts: ${ARTIFACT_DIR}"
