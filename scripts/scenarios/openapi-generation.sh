#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EDGE_ROOT="${COWD_FRONTEND_REPO:-${ROOT}/../cowd-edge}"
WEBUI_ROOT="${EDGE_ROOT}/surfaces/webui"
MODE="${1:-check}"
PORT="${COWD_OPENAPI_GATEWAY_PORT:-18641}"
BASE_URL="http://127.0.0.1:${PORT}"
TOKEN="openapi-generation-$$-credential"
TARGET_ROOT="${CARGO_TARGET_DIR:-${ROOT}/target}"
BIN="${COWD_BIN:-${TARGET_ROOT}/debug/cowd}"
SCENARIO_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cowd-openapi-generation.XXXXXX")"
CONFIG_HOME="${SCENARIO_ROOT}/config"
TEST_HOME="${SCENARIO_ROOT}/home"
WORKSPACE="${SCENARIO_ROOT}/workspace"
GENERATED="${WEBUI_ROOT}/src/generated/gateway-api.ts"
GENERATED_LIVE_CONTRACT="${WEBUI_ROOT}/src/generated/live-contract-meta.ts"
GATEWAY_PID=""

case "${MODE}" in
  update|check) ;;
  *)
    echo "usage: $0 <update|check>" >&2
    exit 2
    ;;
esac

cleanup() {
  set +e
  if [[ -n "${GATEWAY_PID}" ]] && kill -0 "${GATEWAY_PID}" 2>/dev/null; then
    kill "${GATEWAY_PID}" 2>/dev/null || true
    wait "${GATEWAY_PID}" 2>/dev/null || true
  fi
  if [[ "${COWD_OPENAPI_KEEP_WORKSPACE:-0}" != "1" ]]; then
    rm -rf "${SCENARIO_ROOT}"
  fi
}
trap cleanup EXIT INT TERM

for command in cargo cmp curl jq npm sed ss; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required for OpenAPI generation" >&2
    exit 1
  }
done
[[ -d "${WEBUI_ROOT}" ]] || { echo "missing WebUI root: ${WEBUI_ROOT}" >&2; exit 1; }
[[ -f "${GENERATED}" ]] || { echo "missing committed generated API: ${GENERATED}" >&2; exit 1; }
[[ -f "${GENERATED_LIVE_CONTRACT}" ]] || {
  echo "missing committed live contract metadata: ${GENERATED_LIVE_CONTRACT}" >&2
  exit 1
}
if ss -ltn | grep -Eq ":${PORT}([[:space:]]|$)"; then
  echo "OpenAPI generation port ${PORT} is already in use" >&2
  exit 1
fi

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
    api_key: "test-dummy-key-for-openapi-generation"
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

set_manager_profile() {
  local current epoch revision confirmation stderr_path
  stderr_path="${SCENARIO_ROOT}/profile-set.stderr"
  current="$(profile_show)"
  epoch="$(jq -er '.credential_epoch' <<<"${current}")"
  revision="$(jq -er '.profile_revision' <<<"${current}")"
  if printf '%s\n' "${TOKEN}" | env COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${TEST_HOME}" \
    "${BIN}" auth profile set --core-profile core_manager --apps mfg=mfg_manager \
      --expected-epoch "${epoch}" --expected-revision "${revision}" --confirm invalid \
      >/dev/null 2>"${stderr_path}"; then
    echo "invalid profile confirmation unexpectedly succeeded" >&2
    return 1
  fi
  confirmation="$(sed -n 's/.*confirmation=\([^[:space:]]*\).*/\1/p' "${stderr_path}" | head -n 1)"
  [[ -n "${confirmation}" ]] || { echo "profile confirmation was not emitted" >&2; return 1; }
  printf '%s\n' "${TOKEN}" | env COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${TEST_HOME}" \
    "${BIN}" auth profile set --core-profile core_manager --apps mfg=mfg_manager \
      --expected-epoch "${epoch}" --expected-revision "${revision}" --confirm "${confirmation}" \
      >"${SCENARIO_ROOT}/profile-manager.json"
}

set_manager_profile
curl -fsS -H "Authorization: Bearer ${TOKEN}" "${BASE_URL}/api/apps/mfg/contract" \
  | jq -e '([.routes[] | select(.availability == "active")] | length) == 105' >/dev/null

if [[ "${MODE}" == "update" ]]; then
  env COWD_GATEWAY_URL="${BASE_URL}" COWD_API_TOKEN="${TOKEN}" \
    npm --prefix "${WEBUI_ROOT}" run generate:api
else
  CANDIDATE="${SCENARIO_ROOT}/gateway-api.ts"
  LIVE_CONTRACT_CANDIDATE="${SCENARIO_ROOT}/live-contract-meta.ts"
  env COWD_GATEWAY_URL="${BASE_URL}" COWD_API_TOKEN="${TOKEN}" \
    COWD_GENERATED_API_OUTPUT="${CANDIDATE}" \
    COWD_GENERATED_LIVE_CONTRACT_OUTPUT="${LIVE_CONTRACT_CANDIDATE}" \
    npm --prefix "${WEBUI_ROOT}" run generate:api
  if ! cmp -s "${GENERATED}" "${CANDIDATE}"; then
    echo "committed generated API is stale; run: bash scripts/scenarios/openapi-generation.sh update" >&2
    diff -u "${GENERATED}" "${CANDIDATE}" >&2 || true
    exit 1
  fi
  if ! cmp -s "${GENERATED_LIVE_CONTRACT}" "${LIVE_CONTRACT_CANDIDATE}"; then
    echo "committed live contract metadata is stale; run: bash scripts/scenarios/openapi-generation.sh update" >&2
    diff -u "${GENERATED_LIVE_CONTRACT}" "${LIVE_CONTRACT_CANDIDATE}" >&2 || true
    exit 1
  fi
fi

echo "OpenAPI generation ${MODE} passed against ${BASE_URL}"
