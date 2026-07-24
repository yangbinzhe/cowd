#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EDGE_ROOT="${COWD_FRONTEND_REPO:-${ROOT}/../cowd-edge}"
WEBUI_ROOT="${EDGE_ROOT}/surfaces/webui"
LANE="${1:-}"
shift || true
[[ "${1:-}" == "--" ]] || {
  echo "usage: $0 <frontend|backend> -- <command> [args...]" >&2
  exit 2
}
shift
[[ $# -gt 0 ]] || { echo "surface lane requires a command" >&2; exit 2; }
case "${LANE}" in
  frontend|backend) ;;
  *) echo "surface lane must be frontend or backend" >&2; exit 2 ;;
esac

GATEWAY_PORT="${COWD_MFG_LANE_GATEWAY_PORT:-18642}"
WEBUI_PORT="${COWD_MFG_LANE_WEBUI_PORT:-18643}"
BASE_URL="http://127.0.0.1:${GATEWAY_PORT}"
WEBUI_URL="http://127.0.0.1:${WEBUI_PORT}"
TOKEN="mfg-surface-lane-$$-credential"
TARGET_ROOT="${CARGO_TARGET_DIR:-${ROOT}/target}"
BIN="${COWD_BIN:-${TARGET_ROOT}/debug/cowd}"
SCENARIO_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cowd-mfg-surface-lane.XXXXXX")"
CONFIG_HOME="${SCENARIO_ROOT}/config"
TEST_HOME="${SCENARIO_ROOT}/home"
WORKSPACE="${SCENARIO_ROOT}/workspace"
GATEWAY_PID=""
WEBUI_PID=""

cleanup() {
  set +e
  if [[ -n "${WEBUI_PID}" ]] && kill -0 "${WEBUI_PID}" 2>/dev/null; then
    kill "${WEBUI_PID}" 2>/dev/null || true
    wait "${WEBUI_PID}" 2>/dev/null || true
  fi
  if [[ -n "${GATEWAY_PID}" ]] && kill -0 "${GATEWAY_PID}" 2>/dev/null; then
    kill "${GATEWAY_PID}" 2>/dev/null || true
    wait "${GATEWAY_PID}" 2>/dev/null || true
  fi
  for _ in {1..40}; do
    if ! ss -ltn | grep -Eq ":${GATEWAY_PORT}([[:space:]]|$)|:${WEBUI_PORT}([[:space:]]|$)"; then
      break
    fi
    sleep 0.1
  done
  if ss -ltn | grep -Eq ":${GATEWAY_PORT}([[:space:]]|$)|:${WEBUI_PORT}([[:space:]]|$)"; then
    echo "surface lane cleanup left a fixed listener behind" >&2
    return 1
  fi
  if [[ -n "${COWD_MFG_LANE_EVIDENCE_DIR:-}" ]]; then
    mkdir -p "${COWD_MFG_LANE_EVIDENCE_DIR}"
    for artifact in gateway.log webui.log profile-set.stderr profile-manager.json; do
      if [[ -f "${SCENARIO_ROOT}/${artifact}" ]]; then
        cp "${SCENARIO_ROOT}/${artifact}" "${COWD_MFG_LANE_EVIDENCE_DIR}/${artifact}"
      fi
    done
  fi
  if [[ "${COWD_MFG_LANE_KEEP_WORKSPACE:-0}" != "1" ]]; then
    rm -rf "${SCENARIO_ROOT}"
  fi
}
trap cleanup EXIT INT TERM

for command in cargo curl jq npm sed ss; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required for the MFG Surface lane" >&2
    exit 1
  }
done
[[ -d "${WEBUI_ROOT}" ]] || { echo "missing WebUI root: ${WEBUI_ROOT}" >&2; exit 1; }
for port in "${GATEWAY_PORT}" "${WEBUI_PORT}"; do
  if ss -ltn | grep -Eq ":${port}([[:space:]]|$)"; then
    echo "MFG Surface lane port ${port} is already in use" >&2
    exit 1
  fi
done

(
  cd "${ROOT}"
  cargo build -p cli --features full
)
[[ -x "${BIN}" ]] || { echo "missing cowd binary: ${BIN}" >&2; exit 1; }

mkdir -p "${CONFIG_HOME}" "${TEST_HOME}/.cowd" "${WORKSPACE}/.cowd"
cat >"${CONFIG_HOME}/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  anthropic:
    base_url: "https://api.anthropic.com/v1"
    api_key: "test-dummy-key-for-mfg-surface-lane"
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
      port: ${GATEWAY_PORT}
      auth:
        enabled: true
        token: "${TOKEN}"
EOF
cp "${CONFIG_HOME}/config.yaml" "${TEST_HOME}/.cowd/config.yaml"
cp "${CONFIG_HOME}/config.yaml" "${WORKSPACE}/.cowd/config.yaml"

(
  cd "${WORKSPACE}"
  exec env \
    COWD_CONFIG_HOME="${CONFIG_HOME}" \
    HOME="${TEST_HOME}" \
    "${BIN}" gateway run
) >"${SCENARIO_ROOT}/gateway.log" 2>&1 &
GATEWAY_PID=$!
for _ in {1..160}; do
  if curl -fsS -H "Authorization: Bearer ${TOKEN}" "${BASE_URL}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
curl -fsS -H "Authorization: Bearer ${TOKEN}" "${BASE_URL}/health" >/dev/null

profile_show() {
  printf '%s\n' "${TOKEN}" | env COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${TEST_HOME}" \
    "${BIN}" auth profile show
}
current="$(profile_show)"
epoch="$(jq -er '.credential_epoch' <<<"${current}")"
revision="$(jq -er '.profile_revision' <<<"${current}")"
profile_stderr="${SCENARIO_ROOT}/profile-set.stderr"
if printf '%s\n' "${TOKEN}" | env COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${TEST_HOME}" \
  "${BIN}" auth profile set --core-profile core_manager --apps mfg=mfg_manager \
    --expected-epoch "${epoch}" --expected-revision "${revision}" --confirm invalid \
    >/dev/null 2>"${profile_stderr}"; then
  echo "invalid profile confirmation unexpectedly succeeded" >&2
  exit 1
fi
confirmation="$(sed -n 's/.*confirmation=\([^[:space:]]*\).*/\1/p' "${profile_stderr}" | head -n 1)"
[[ -n "${confirmation}" ]] || { echo "profile confirmation was not emitted" >&2; exit 1; }
printf '%s\n' "${TOKEN}" | env COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${TEST_HOME}" \
  "${BIN}" auth profile set --core-profile core_manager --apps mfg=mfg_manager \
    --expected-epoch "${epoch}" --expected-revision "${revision}" --confirm "${confirmation}" \
    >"${SCENARIO_ROOT}/profile-manager.json"
curl -fsS -H "Authorization: Bearer ${TOKEN}" "${BASE_URL}/api/apps/mfg/contract" \
  | jq -e '([.routes[] | select(.availability == "active")] | length) == 105' >/dev/null

(
  cd "${WEBUI_ROOT}"
  exec env COWD_VITE_GATEWAY_URL="${BASE_URL}" \
    node ./node_modules/vite/bin/vite.js --host 127.0.0.1 --port "${WEBUI_PORT}" --strictPort
) >"${SCENARIO_ROOT}/webui.log" 2>&1 &
WEBUI_PID=$!
for _ in {1..160}; do
  if curl -fsS "${WEBUI_URL}/index.html" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
curl -fsS "${WEBUI_URL}/index.html" >/dev/null

cd "${ROOT}"
env \
  COWD_BIN="${BIN}" \
  COWD_CONFIG_HOME="${CONFIG_HOME}" \
  COWD_MFG_TEST_CONFIG_HOME="${CONFIG_HOME}" \
  COWD_MFG_TEST_HOME="${TEST_HOME}" \
  COWD_MFG_TEST_WORKSPACE="${WORKSPACE}" \
  COWD_GATEWAY_URL="${BASE_URL}" \
  COWD_API_TOKEN="${TOKEN}" \
  COWD_VISUAL_BASE_URL="${WEBUI_URL}" \
  COWD_VISUAL_GATEWAY_TOKEN="${TOKEN}" \
  COWD_PERFORMANCE_BASE_URL="${WEBUI_URL}" \
  COWD_PERFORMANCE_GATEWAY_TOKEN="${TOKEN}" \
  COWD_E2E_GATEWAY_URL="${WEBUI_URL}" \
  COWD_E2E_GATEWAY_TOKEN="${TOKEN}" \
  "$@"
