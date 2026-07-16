#!/usr/bin/env bash
set -euo pipefail

# V543 establishes the evidence producer contract only. V545 activates the
# multi-surface orchestration lane; until then this script must never return a
# successful status that could be mistaken for acceptance evidence.

scenario_id="mfg-surface-acceptance-$$-$(date +%s)"
scenario_root="${TMPDIR:-/tmp}/${scenario_id}"
workspace="${COWD_MFG_TEST_WORKSPACE:-${scenario_root}/workspace}"
config_home="${scenario_root}/config"
test_home="${scenario_root}/home"
artifact_dir="${COWD_MFG_ACCEPTANCE_ARTIFACT_DIR:-${scenario_root}/artifacts}"
gateway_port="${COWD_MFG_GATEWAY_PORT:-18642}"
tmux_prefix="${scenario_id}"
gateway_pid=""

cleanup() {
  if [[ -n "${gateway_pid}" ]] && kill -0 "${gateway_pid}" 2>/dev/null; then
    kill "${gateway_pid}" 2>/dev/null || true
    wait "${gateway_pid}" 2>/dev/null || true
  fi
  tmux -L "${tmux_prefix}" kill-server 2>/dev/null || true
  if [[ "${COWD_MFG_KEEP_FAILED_ARTIFACTS:-0}" != "1" ]]; then
    rm -rf "${scenario_root}"
  fi
}
trap cleanup EXIT INT TERM

mkdir -p "${workspace}" "${config_home}" "${test_home}" "${artifact_dir}"
export COWD_SCENARIO_SKIP_BUILD="${COWD_SCENARIO_SKIP_BUILD:-1}"
export COWD_MFG_TEST_WORKSPACE="${workspace}"
export COWD_MFG_TEST_CONFIG_HOME="${config_home}"
export COWD_MFG_TEST_HOME="${test_home}"
export COWD_CONFIG_HOME="${config_home}"
export HOME="${test_home}"
export COWD_GATEWAY_URL="http://127.0.0.1:${gateway_port}"
export COWD_API_TOKEN="${COWD_API_TOKEN:-}"
export COWD_INTERACTIVE_ARTIFACT_DIR="${artifact_dir}"
export COWD_INTERACTIVE_TMUX_LABEL="${tmux_prefix}"

if [[ "${COWD_MFG_SURFACE_ACCEPTANCE_ACTIVATED:-0}" != "1" ]]; then
  echo "MFG surface acceptance is intentionally not activated in V543." >&2
  echo "V545 must add isolated Gateway startup, fixture seeding, WebUI/TUI observers, live cursor checks, and artifact export." >&2
  exit 64
fi

echo "MFG surface acceptance orchestration is reserved for V545 and is not yet implemented." >&2
exit 65
