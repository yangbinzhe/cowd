#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EDGE_ROOT="${COWD_FRONTEND_REPO:-${ROOT}/../cowd-edge}"
TARGET_ROOT="${CARGO_TARGET_DIR:-${ROOT}/target}"
COWD_BIN="${COWD_BIN:-${TARGET_ROOT}/debug/cowd}"
EVAL_BIN="${COWD_HARNESS_EVAL_BIN:-${TARGET_ROOT}/debug/harness-eval}"
# Real-model evaluation is isolated from user state, but its route must be a
# snapshot of the installed Cowd route. Do not default a model to another
# provider account or copy a literal credential into an artifact: that makes a
# successful interactive configuration and its evaluation exercise different
# products and can leak a secret into the isolated workspace.
SOURCE_CONFIG_HOME="${COWD_AUTO_STRATEGY_SOURCE_CONFIG_HOME:-${COWD_CONFIG_HOME:-${HOME}/.cowd}}"
SOURCE_CONFIG_FILE="${SOURCE_CONFIG_HOME}/config.yaml"
SOURCE_MODELS_FILE="${SOURCE_CONFIG_HOME}/models.yaml"
MODEL="${COWD_AUTO_STRATEGY_MODEL:-}"
JUDGE_MODEL="${COWD_AUTO_STRATEGY_JUDGE_MODEL:-}"
PROVIDER_ID=""
PROVIDER_PROTOCOL=""
PROVIDER_BASE_URL=""
PROVIDER_CREDENTIAL=""
STAGED_CREDENTIAL_REF=""
STAGED_CREDENTIAL_ENV=""
PROVIDER_ACCOUNT_REF=""
MODEL_CONTEXT_WINDOW=""
MODEL_MAX_OUTPUT_TOKENS=""
DIAGNOSTIC_TASK_ID="${COWD_AUTO_STRATEGY_DIAGNOSTIC_TASK_ID:-}"
API_TOKEN="${COWD_API_TOKEN:-auto-strategy-$$_credential}"
SCENARIO_ID="auto-strategy-paired-$$-$(date +%s)"
# Gateway sidecars bind Unix sockets underneath `config/app-runtime`; keep
# this runtime root short enough for every supported Unix socket limit. The
# longer Scenario id remains in artifacts/reports, where it cannot affect IPC.
SCENARIO_ROOT="${COWD_AUTO_STRATEGY_SCENARIO_ROOT:-${TMPDIR:-/tmp}/csp-$$-$(date +%s)}"
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
      for _ in {1..20}; do
        kill -0 "${pid}" 2>/dev/null || break
        sleep 0.1
      done
      if kill -0 "${pid}" 2>/dev/null; then
        kill -KILL "${pid}" 2>/dev/null || true
      fi
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

yaml_model_field() {
  local file="$1"
  local model="$2"
  local field="$3"
  awk -v target="${model}" -v key="${field}" '
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
  ' "${file}"
}

yaml_provider_field() {
  local file="$1"
  local provider="$2"
  local field="$3"
  awk -v target="${provider}" -v key="${field}" '
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
  ' "${file}"
}

resolve_installed_provider_route() {
  [[ -f "${SOURCE_CONFIG_FILE}" && -f "${SOURCE_MODELS_FILE}" ]] || {
    echo "real-model evaluation requires ${SOURCE_CONFIG_FILE} and ${SOURCE_MODELS_FILE}" >&2
    exit 1
  }

  if [[ -z "${MODEL}" ]]; then
    MODEL="$(awk '/^model:[[:space:]]*/ { value = $0; sub(/^model:[[:space:]]*/, "", value); sub(/[[:space:]]+#.*$/, "", value); gsub(/^\"|\"$/, "", value); gsub(/^\047|\047$/, "", value); print value; exit }' "${SOURCE_CONFIG_FILE}")"
  fi
  [[ -n "${MODEL}" ]] || {
    echo "real-model evaluation cannot resolve the installed default model; set COWD_AUTO_STRATEGY_MODEL explicitly" >&2
    exit 1
  }

  PROVIDER_ID="$(yaml_model_field "${SOURCE_MODELS_FILE}" "${MODEL}" provider)"
  MODEL_CONTEXT_WINDOW="$(yaml_model_field "${SOURCE_MODELS_FILE}" "${MODEL}" context_window)"
  MODEL_MAX_OUTPUT_TOKENS="$(yaml_model_field "${SOURCE_MODELS_FILE}" "${MODEL}" max_output_tokens)"
  [[ -n "${PROVIDER_ID}" && -n "${MODEL_CONTEXT_WINDOW}" && -n "${MODEL_MAX_OUTPUT_TOKENS}" ]] || {
    echo "model ${MODEL} must have provider, context_window, and max_output_tokens in ${SOURCE_MODELS_FILE}" >&2
    exit 1
  }

  PROVIDER_PROTOCOL="$(yaml_provider_field "${SOURCE_CONFIG_FILE}" "${PROVIDER_ID}" protocol)"
  PROVIDER_BASE_URL="$(yaml_provider_field "${SOURCE_CONFIG_FILE}" "${PROVIDER_ID}" base_url)"
  PROVIDER_CREDENTIAL="$(yaml_provider_field "${SOURCE_CONFIG_FILE}" "${PROVIDER_ID}" api_key)"
  [[ -n "${PROVIDER_PROTOCOL}" && -n "${PROVIDER_BASE_URL}" && -n "${PROVIDER_CREDENTIAL}" ]] || {
    echo "provider ${PROVIDER_ID} must have protocol, base_url, and api_key in ${SOURCE_CONFIG_FILE}" >&2
    exit 1
  }

  JUDGE_MODEL="${JUDGE_MODEL:-${MODEL}}"
  JUDGE_PROVIDER_ID="$(yaml_model_field "${SOURCE_MODELS_FILE}" "${JUDGE_MODEL}" provider)"
  [[ "$JUDGE_PROVIDER_ID" == "$PROVIDER_ID" ]] || {
    echo "judge model ${JUDGE_MODEL} must use the same configured provider as ${MODEL}; cross-provider paired evaluation is not an implicit fallback" >&2
    exit 1
  }
  case "${PROVIDER_CREDENTIAL}" in
    env:*) STAGED_CREDENTIAL_REF="${PROVIDER_CREDENTIAL}" ;;
    file:*)
      echo "provider ${PROVIDER_ID} uses an unsupported file credential reference; use Cowd's env: credential reference" >&2
      exit 1
      ;;
    *)
      STAGED_CREDENTIAL_ENV="COWD_ISOLATED_EVAL_PROVIDER_API_KEY"
      STAGED_CREDENTIAL_REF="env:${STAGED_CREDENTIAL_ENV}"
      ;;
  esac
  PROVIDER_ACCOUNT_REF="configured:${PROVIDER_ID}"
}

resolve_installed_provider_route
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
  cargo build -p cli -p harness-eval
)
[[ -x "${COWD_BIN}" ]] || { echo "missing cowd binary: ${COWD_BIN}" >&2; exit 1; }
[[ -x "${EVAL_BIN}" ]] || { echo "missing harness-eval binary: ${EVAL_BIN}" >&2; exit 1; }
BIN_SHA256="$(sha256sum "${COWD_BIN}" | awk '{print $1}')"

mkdir -p "${SCENARIO_ROOT}" "${ARTIFACT_DIR}"
EVAL_HOME="${SCENARIO_ROOT}/evaluator-home"
mkdir -p "${EVAL_HOME}/.cowd"
cp "${SOURCE_MODELS_FILE}" "${EVAL_HOME}/.cowd/models.yaml"
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
    echo "  ${PROVIDER_ID}:"
    echo "    base_url: \"${PROVIDER_BASE_URL}\""
    echo "    api_key: \"${STAGED_CREDENTIAL_REF}\""
    echo "    protocol: \"${PROVIDER_PROTOCOL}\""
    echo "    models: [\"${MODEL}\", \"${JUDGE_MODEL}\"]"
    echo "permissions:"
    echo "  default_mode: \"danger-full-access\""
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
  if [[ -f "${EVAL_HOME}/.cowd/models.yaml" ]]; then
    cp "${EVAL_HOME}/.cowd/models.yaml" "${home}/.cowd/models.yaml"
  fi
  (
    cd "${workspace}"
    gateway_env=(
      COWD_CONFIG_HOME="${config_home}"
      COWD_API_TOKEN="${API_TOKEN}"
      COWD_EVAL_HARNESS=1
      COWD_EVAL_CORPUS_ID=auto-strategy-v1
      COWD_EVAL_WORKSPACE_FIXTURE=workspace-auto-strategy-frozen
      COWD_EVAL_STRATEGY_OVERRIDE="${condition}"
      COWD_MODEL_TEMPERATURE=0
      HOME="${home}"
    )
    if [[ -n "${STAGED_CREDENTIAL_ENV}" ]]; then
      gateway_env+=("${STAGED_CREDENTIAL_ENV}=${PROVIDER_CREDENTIAL}")
    fi
    exec env "${gateway_env[@]}" "${COWD_BIN}" gateway run
  ) >"${ARTIFACT_DIR}/${condition}-gateway.log" 2>&1 &
  PIDS+=("$!")
done

# A literal credential from the installed configuration is never retained in
# the script process after the isolated Gateway children have inherited it.
unset PROVIDER_CREDENTIAL

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
    COWD_AUTO_STRATEGY_MAX_TOKENS="${COWD_AUTO_STRATEGY_MAX_TOKENS:-20000000}" \
    COWD_EVAL_BINARY_SHA256="${BIN_SHA256}" \
    COWD_EVAL_WORKSPACE_REVISION="${WORKSPACE_REVISION}" \
    COWD_EVAL_FRONTEND_WORKSPACE_REVISION="${FRONTEND_WORKSPACE_REVISION}" \
    COWD_EVAL_BACKEND_SOURCE_ARCHIVE_SHA256="${BACKEND_SOURCE_ARCHIVE_SHA256}" \
    COWD_EVAL_FRONTEND_SOURCE_ARCHIVE_SHA256="${FRONTEND_SOURCE_ARCHIVE_SHA256}" \
    COWD_EVAL_PROVIDER_ACCOUNT_REF="${PROVIDER_ACCOUNT_REF}" \
    COWD_EVAL_DIRECT_WORKSPACE="${SCENARIO_ROOT}/direct/workspace" \
    COWD_EVAL_PARALLEL_WORKSPACE="${SCENARIO_ROOT}/parallel_tools/workspace" \
    COWD_EVAL_AUTO_WORKSPACE="${SCENARIO_ROOT}/auto/workspace" \
    COWD_EVAL_DIRECT_PRISTINE="${SCENARIO_ROOT}/direct/pristine" \
    COWD_EVAL_PARALLEL_PRISTINE="${SCENARIO_ROOT}/parallel_tools/pristine" \
    COWD_EVAL_AUTO_PRISTINE="${SCENARIO_ROOT}/auto/pristine" \
    HOME="${EVAL_HOME}" \
    "${EVAL_BIN}" auto-strategy-paired \
      --direct-url "http://127.0.0.1:18652" \
      --parallel-url "http://127.0.0.1:18653" \
      --auto-url "http://127.0.0.1:18654" \
      --provider "${MODEL}" \
      --judge-model "${JUDGE_MODEL}" \
      --corpus "crates/harness-eval/corpora/auto-strategy-v1.json" \
      --rubric "crates/harness-eval/rubrics/auto-strategy-rubric-v1.json" \
      --repetitions 3 \
      --poll-interval-ms "${COWD_AUTO_STRATEGY_POLL_INTERVAL_MS:-500}" \
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

if [[ -n "${DIAGNOSTIC_TASK_ID}" ]]; then
  jq -e --arg task_id "${DIAGNOSTIC_TASK_ID}" '
    .kind == "harness_eval.auto_strategy_paired.v1"
    and .status == "diagnostic_passed"
    and .provenance.diagnostic_task_id == $task_id
    and .gate.passed == false
    and .gate.claim_allowed == false
    and .gate.diagnostic_passed == true
    and .gate.all_samples_completed == true
  ' "${REPORT}" >/dev/null
else
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
fi
if [[ -z "${DIAGNOSTIC_TASK_ID}" ]]; then
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
fi
echo "auto-strategy-paired-report: ${REPORT}"
