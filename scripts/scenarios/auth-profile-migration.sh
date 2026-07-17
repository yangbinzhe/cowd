#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-${ROOT}/target}"
BIN="${COWD_BIN:-${TARGET_ROOT}/debug/cowd}"
AUTH_BROKER_BIN="${COWD_AUTH_BROKER_BIN:-${TARGET_ROOT}/debug/cowd-auth-broker}"
CREDENTIAL="auth-profile-migration-$$-credential"
SCENARIO_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cowd-auth-profile-migration.XXXXXX")"
CONFIG_HOME="${SCENARIO_ROOT}/config"
AUTH_ROOT="${CONFIG_HOME}/auth-broker"
SOCKET="${AUTH_ROOT}/broker.sock"
BROKER_PID=""

cleanup() {
  set +e
  if [[ -n "${BROKER_PID}" ]] && kill -0 "${BROKER_PID}" 2>/dev/null; then
    kill "${BROKER_PID}" 2>/dev/null || true
    wait "${BROKER_PID}" 2>/dev/null || true
  fi
  rm -rf "${SCENARIO_ROOT}"
}
trap cleanup EXIT INT TERM

for command in cargo jq sed; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required for auth profile migration" >&2
    exit 1
  }
done

(
  cd "${ROOT}"
  cargo build -p cli -p auth-broker
  cargo test -p auth-broker --lib v1_state_migrates_to_legacy_profiles_without_new_capabilities
  cargo test -p auth-broker --lib digest_only_state_migrates_to_legacy_profiles_and_removes_legacy_file
)
[[ -x "${BIN}" ]] || { echo "missing cowd binary: ${BIN}" >&2; exit 1; }
[[ -x "${AUTH_BROKER_BIN}" ]] || { echo "missing auth broker binary: ${AUTH_BROKER_BIN}" >&2; exit 1; }

mkdir -p "${AUTH_ROOT}"
printf '%s\n' "${CREDENTIAL}" | "${AUTH_BROKER_BIN}" \
  --root "${AUTH_ROOT}" --socket "${SOCKET}" --credential-stdin \
  >"${SCENARIO_ROOT}/auth-broker.log" 2>&1 &
BROKER_PID=$!
for _ in {1..80}; do
  [[ -S "${SOCKET}" ]] && break
  sleep 0.1
done
[[ -S "${SOCKET}" ]] || { echo "auth broker did not expose its socket" >&2; exit 1; }

profile_show() {
  printf '%s\n' "${CREDENTIAL}" | env COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${SCENARIO_ROOT}/home" \
    "${BIN}" auth profile show
}

set_profile() {
  local core=$1 mfg=$2 current epoch revision confirmation stderr_path
  stderr_path="${SCENARIO_ROOT}/profile-${mfg}.stderr"
  current="$(profile_show)"
  epoch="$(jq -er '.credential_epoch' <<<"${current}")"
  revision="$(jq -er '.profile_revision' <<<"${current}")"
  if printf '%s\n' "${CREDENTIAL}" | env COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${SCENARIO_ROOT}/home" \
    "${BIN}" auth profile set --core "${core}" --mfg "${mfg}" \
      --expected-epoch "${epoch}" --expected-revision "${revision}" --confirm invalid \
      >/dev/null 2>"${stderr_path}"; then
    echo "invalid confirmation unexpectedly succeeded for ${core}/${mfg}" >&2
    return 1
  fi
  confirmation="$(sed -n 's/.*confirmation=\([^[:space:]]*\).*/\1/p' "${stderr_path}" | head -n 1)"
  [[ -n "${confirmation}" ]] || { echo "profile confirmation was not emitted for ${core}/${mfg}" >&2; return 1; }
  printf '%s\n' "${CREDENTIAL}" | env COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${SCENARIO_ROOT}/home" \
    "${BIN}" auth profile set --core "${core}" --mfg "${mfg}" \
      --expected-epoch "${epoch}" --expected-revision "${revision}" --confirm "${confirmation}"
}

initial="$(profile_show)"
jq -e '.core_profile_id == "core_legacy_0_9_530" and .mfg_profile_id == "mfg_viewer" and .profile_revision == 1' \
  <<<"${initial}" >/dev/null
initial_epoch="$(jq -er '.credential_epoch' <<<"${initial}")"
initial_revision="$(jq -er '.profile_revision' <<<"${initial}")"

for mfg in mfg_viewer mfg_operator mfg_reviewer mfg_manager; do
  updated="$(set_profile core_manager "${mfg}")"
  jq -e --arg mfg "${mfg}" '.core_profile_id == "core_manager" and .mfg_profile_id == $mfg and (.profile_revision >= 2)' \
    <<<"${updated}" >/dev/null
done

if printf '%s\n' "${CREDENTIAL}" | env COWD_CONFIG_HOME="${CONFIG_HOME}" HOME="${SCENARIO_ROOT}/home" \
  "${BIN}" auth profile set --core core_manager --mfg mfg_manager \
    --expected-epoch "${initial_epoch}" --expected-revision "${initial_revision}" --confirm invalid \
    >/dev/null 2>"${SCENARIO_ROOT}/stale.stderr"; then
  echo "stale profile update unexpectedly succeeded" >&2
  exit 1
fi
grep -F "stale profile state" "${SCENARIO_ROOT}/stale.stderr" >/dev/null

echo "auth profile migration passed: legacy digest, v1 state, v2 profiles, confirmation and stale-CAS rejection"
