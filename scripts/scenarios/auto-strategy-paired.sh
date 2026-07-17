#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EDGE_ROOT="${COWD_FRONTEND_REPO:-${ROOT}/../cowd-edge}"
TARGET_ROOT="${CARGO_TARGET_DIR:-${ROOT}/target}"
COWD_BIN="${COWD_BIN:-${TARGET_ROOT}/debug/cowd}"
EVAL_BIN="${COWD_HARNESS_EVAL_BIN:-${TARGET_ROOT}/debug/harness-eval}"
AUTH_BROKER_BIN="${COWD_AUTH_BROKER_BIN:-${TARGET_ROOT}/debug/cowd-auth-broker}"
MODEL="${COWD_AUTO_STRATEGY_MODEL:-claude-sonnet-4-6}"
JUDGE_MODEL="${COWD_AUTO_STRATEGY_JUDGE_MODEL:-${MODEL}}"
API_TOKEN="${COWD_API_TOKEN:-auto-strategy-$$_credential}"
SCENARIO_ID="auto-strategy-paired-$$-$(date +%s)"
SCENARIO_ROOT="${TMPDIR:-/tmp}/${SCENARIO_ID}"
ARTIFACT_DIR="${COWD_AUTO_STRATEGY_ARTIFACT_DIR:-${ROOT}/target/acceptance/${SCENARIO_ID}}"
REPORT="${ARTIFACT_DIR}/auto-strategy-paired.json"
POINTER="${ROOT}/target/acceptance/latest-auto-strategy.json"
BIN_SHA256=""
WORKSPACE_REVISION=""
FRONTEND_WORKSPACE_REVISION=""
BACKEND_SOURCE_ARCHIVE_SHA256=""
FRONTEND_SOURCE_ARCHIVE_SHA256=""
PORTS=(18652 18653 18654)
CONDITIONS=(direct parallel_tools auto)
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  if [[ "${COWD_AUTO_STRATEGY_KEEP_WORKSPACE:-0}" != "1" ]]; then
    rm -rf "${SCENARIO_ROOT}"
  fi
}
trap cleanup EXIT INT TERM

for command in awk cp curl git jq rg sha256sum ss tar; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required for auto-strategy-paired" >&2
    exit 1
  }
done
git -C "${EDGE_ROOT}" rev-parse --show-toplevel >/dev/null 2>&1 || {
  echo "COWD frontend repository is required for the frozen cross-surface fixture: ${EDGE_ROOT}" >&2
  exit 1
}
require_clean_source() {
  local repo="$1"
  local label="$2"
  if [[ -n "$(git -C "${repo}" status --porcelain=v1 --untracked-files=all)" ]]; then
    echo "${label} source tree must be clean before evaluation build" >&2
    exit 1
  fi
}
require_clean_source "${ROOT}" "backend"
require_clean_source "${EDGE_ROOT}" "frontend"
WORKSPACE_REVISION="$(git -C "${ROOT}" rev-parse HEAD)"
FRONTEND_WORKSPACE_REVISION="$(git -C "${EDGE_ROOT}" rev-parse HEAD)"
BACKEND_SOURCE_ARCHIVE_SHA256="$(git -C "${ROOT}" archive --format=tar "${WORKSPACE_REVISION}" | sha256sum | awk '{print $1}')"
FRONTEND_SOURCE_ARCHIVE_SHA256="$(git -C "${EDGE_ROOT}" archive --format=tar "${FRONTEND_WORKSPACE_REVISION}" | sha256sum | awk '{print $1}')"
for port in "${PORTS[@]}"; do
  if ss -ltn | rg -q ":${port}\\b"; then
    echo "fixed auto-strategy port ${port} is already in use" >&2
    exit 1
  fi
done

(
  cd "${ROOT}"
  cargo build -p cli -p auth-broker -p harness-eval
)
[[ -x "${COWD_BIN}" ]] || { echo "missing cowd binary: ${COWD_BIN}" >&2; exit 1; }
[[ -x "${EVAL_BIN}" ]] || { echo "missing harness-eval binary: ${EVAL_BIN}" >&2; exit 1; }
[[ -x "${AUTH_BROKER_BIN}" ]] || {
  echo "missing auth broker binary: ${AUTH_BROKER_BIN}" >&2
  exit 1
}
BIN_SHA256="$(sha256sum "${COWD_BIN}" | awk '{print $1}')"

mkdir -p "${SCENARIO_ROOT}" "${ARTIFACT_DIR}"
for index in 0 1 2; do
  condition="${CONDITIONS[$index]}"
  port="${PORTS[$index]}"
  workspace="${SCENARIO_ROOT}/${condition}/workspace"
  config_home="${SCENARIO_ROOT}/${condition}/config"
  home="${SCENARIO_ROOT}/${condition}/home"
  mkdir -p "${workspace}" "${config_home}" "${home}/.cowd"
  (
    cd "${ROOT}"
    git archive --format=tar HEAD
  ) | tar -xf - -C "${workspace}"
  (
    cd "${EDGE_ROOT}"
    git archive --format=tar HEAD surfaces/webui
  ) | tar -xf - -C "${workspace}"
  cp -a "${workspace}" "${SCENARIO_ROOT}/${condition}/pristine"
  {
    echo "model: \"${MODEL}\""
    echo "fallbacks: []"
    echo "providers:"
    echo "  anthropic:"
    echo "    base_url: \"${ANTHROPIC_BASE_URL:-https://api.anthropic.com/v1}\""
    echo "    api_key: \"${ANTHROPIC_API_KEY:-}\""
    echo "    protocol: \"anthropic\""
    echo "    models: [\"${MODEL}\", \"${JUDGE_MODEL}\"]"
    echo "permissions:"
    echo "  defaultMode: \"dontAsk\""
    echo "memory:"
    echo "  enabled: false"
    echo "gateway:"
    echo "  enabled: true"
    echo "  session_reset: \"none\""
    echo "  platforms:"
    echo "    - platformType: \"api_server\""
    echo "      enabled: true"
    echo "      host: \"127.0.0.1\""
    echo "      port: ${port}"
    echo "      auth:"
    echo "        enabled: true"
    echo "        token: \"${API_TOKEN}\""
  } >"${config_home}/config.yaml"
  cp "${config_home}/config.yaml" "${home}/.cowd/config.yaml"
  (
    cd "${workspace}"
    exec env \
      COWD_CONFIG_HOME="${config_home}" \
      COWD_AUTH_BROKER_BIN="${AUTH_BROKER_BIN}" \
      COWD_API_TOKEN="${API_TOKEN}" \
      COWD_EVAL_HARNESS=1 \
      COWD_EVAL_CORPUS_ID=auto-strategy-v1 \
      COWD_EVAL_WORKSPACE_FIXTURE=workspace-v546-frozen \
      COWD_EVAL_STRATEGY_OVERRIDE="${condition}" \
      COWD_MODEL_TEMPERATURE=0 \
      HOME="${home}" \
      "${COWD_BIN}" gateway run
  ) >"${ARTIFACT_DIR}/${condition}-gateway.log" 2>&1 &
  PIDS+=("$!")
done

for port in "${PORTS[@]}"; do
  ready=0
  for _ in {1..240}; do
    if curl -fsS -H "Authorization: Bearer ${API_TOKEN}" \
      "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.25
  done
  [[ "${ready}" == "1" ]] || {
    echo "Gateway on fixed port ${port} did not become ready" >&2
    exit 1
  }
done

allow_real=()
if [[ "${COWD_AUTO_STRATEGY_ALLOW_REAL_MODEL:-0}" == "1" ]]; then
  allow_real=(--allow-real-model)
fi

(
  cd "${ROOT}"
  exec env \
    COWD_API_TOKEN="${API_TOKEN}" \
    COWD_AUTO_STRATEGY_MAX_TOKENS="${COWD_AUTO_STRATEGY_MAX_TOKENS:-2000000}" \
    COWD_AUTO_STRATEGY_MAX_COST_USD_MILLI="${COWD_AUTO_STRATEGY_MAX_COST_USD_MILLI:-50000}" \
    COWD_EVAL_BINARY_SHA256="${BIN_SHA256}" \
    COWD_EVAL_WORKSPACE_REVISION="${WORKSPACE_REVISION}" \
    COWD_EVAL_FRONTEND_WORKSPACE_REVISION="${FRONTEND_WORKSPACE_REVISION}" \
    COWD_EVAL_BACKEND_SOURCE_ARCHIVE_SHA256="${BACKEND_SOURCE_ARCHIVE_SHA256}" \
    COWD_EVAL_FRONTEND_SOURCE_ARCHIVE_SHA256="${FRONTEND_SOURCE_ARCHIVE_SHA256}" \
    COWD_EVAL_PROVIDER_ACCOUNT_REF="${COWD_EVAL_PROVIDER_ACCOUNT_REF:-anthropic-default}" \
    COWD_EVAL_DIRECT_WORKSPACE="${SCENARIO_ROOT}/direct/workspace" \
    COWD_EVAL_PARALLEL_WORKSPACE="${SCENARIO_ROOT}/parallel_tools/workspace" \
    COWD_EVAL_AUTO_WORKSPACE="${SCENARIO_ROOT}/auto/workspace" \
    COWD_EVAL_DIRECT_PRISTINE="${SCENARIO_ROOT}/direct/pristine" \
    COWD_EVAL_PARALLEL_PRISTINE="${SCENARIO_ROOT}/parallel_tools/pristine" \
    COWD_EVAL_AUTO_PRISTINE="${SCENARIO_ROOT}/auto/pristine" \
    "${EVAL_BIN}" auto-strategy-paired \
      --direct-url "http://127.0.0.1:18652" \
      --parallel-url "http://127.0.0.1:18653" \
      --auto-url "http://127.0.0.1:18654" \
      --provider "${MODEL}" \
      --judge-model "${JUDGE_MODEL}" \
      --corpus "crates/harness-eval/corpora/auto-strategy-v1.json" \
      --rubric "crates/harness-eval/rubrics/auto-strategy-rubric-v1.json" \
      --repetitions 3 \
      --output "${REPORT}" \
      "${allow_real[@]}"
)

[[ "$(git -C "${ROOT}" rev-parse HEAD)" == "${WORKSPACE_REVISION}" ]] || {
  echo "backend revision changed during evaluation" >&2
  exit 1
}
[[ "$(git -C "${EDGE_ROOT}" rev-parse HEAD)" == "${FRONTEND_WORKSPACE_REVISION}" ]] || {
  echo "frontend revision changed during evaluation" >&2
  exit 1
}
require_clean_source "${ROOT}" "backend"
require_clean_source "${EDGE_ROOT}" "frontend"

jq -e '
  .kind == "harness_eval.auto_strategy_paired.v1"
  and .status == "passed"
  and .gate.passed == true
  and .gate.claim_allowed == true
  and .gate.automatic_team_materialization_gate == true
  and .gate.workspace_reset_gate == true
  and .gate.workspace_mutation_gate == true
  and .gate.hard_budget_lease_gate == true
  and .gate.tool_topology_observation_gate == true
  and .gate.baseline_topology_isolation_gate == true
  and .gate.routing_gate == true
  and .gate.judge_isolation_gate == true
' "${REPORT}" >/dev/null
jq -n \
  --arg scenario_id "${SCENARIO_ID}" \
  --arg artifact_dir "${ARTIFACT_DIR}" \
  --arg report "${REPORT}" \
  --arg backend_commit "${WORKSPACE_REVISION}" \
  --arg frontend_commit "${FRONTEND_WORKSPACE_REVISION}" \
  '{
    schema_version: 1,
    producer: "auto-strategy-paired.v1",
    scenario_id: $scenario_id,
    artifact_dir: $artifact_dir,
    report: $report,
    backend_commit: $backend_commit,
    frontend_commit: $frontend_commit
  }' >"${POINTER}"
echo "auto-strategy-paired-report: ${REPORT}"
