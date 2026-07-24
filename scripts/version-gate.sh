#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="${1:-}"
VERSION="${VERSION#v}"
[[ -n "${VERSION}" ]] || { echo "usage: $0 <version>" >&2; exit 2; }
TARGET_ROOT="${CARGO_TARGET_DIR:-${ROOT}/target}"
BIN="${COWD_BIN:-${TARGET_ROOT}/debug/cowd}"
PORT="${COWD_VERSION_GATE_PORT:-18644}"
BASE_URL="http://127.0.0.1:${PORT}"
TOKEN="version-gate-$$-credential"
SCENARIO_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cowd-version-gate.XXXXXX")"
CONFIG_HOME="${SCENARIO_ROOT}/config"
TEST_HOME="${SCENARIO_ROOT}/home"
WORKSPACE="${SCENARIO_ROOT}/workspace"
GATEWAY_PID=""

cleanup() {
  set +e
  if [[ -n "${GATEWAY_PID}" ]] && kill -0 "${GATEWAY_PID}" 2>/dev/null; then
    kill "${GATEWAY_PID}" 2>/dev/null || true
    wait "${GATEWAY_PID}" 2>/dev/null || true
  fi
  rm -rf "${SCENARIO_ROOT}"
}
trap cleanup EXIT INT TERM

for command in cargo curl jq python3 sed ss; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required for the backend version gate" >&2
    exit 1
  }
done
if ss -ltn | grep -Eq ":${PORT}([[:space:]]|$)"; then
  echo "version gate port ${PORT} is already in use" >&2
  exit 1
fi

METADATA="${SCENARIO_ROOT}/metadata.json"
(
  cd "${ROOT}"
  cargo metadata --offline --no-deps --format-version 1 >"${METADATA}"
)
python3 - "${ROOT}/Cargo.toml" "${ROOT}/Cargo.lock" "${METADATA}" "${VERSION}" <<'PY'
import json
import sys
import tomllib
from pathlib import Path

manifest_path, lock_path, metadata_path = map(Path, sys.argv[1:4])
expected = sys.argv[4]
manifest = tomllib.loads(manifest_path.read_text())
actual = manifest.get("workspace", {}).get("package", {}).get("version")
if actual != expected:
    raise SystemExit(f"workspace manifest version is {actual!r}, expected {expected!r}")
metadata = json.loads(metadata_path.read_text())
members = set(metadata["workspace_members"])
packages = {package["id"]: package for package in metadata["packages"]}
workspace = [packages[member] for member in members]
bad = sorted(f"{package['name']}={package['version']}" for package in workspace if package["version"] != expected)
if bad:
    raise SystemExit("workspace metadata has stale versions: " + ", ".join(bad))
lock = tomllib.loads(lock_path.read_text())
lock_pairs = {(package.get("name"), package.get("version")) for package in lock.get("package", [])}
missing = sorted(package["name"] for package in workspace if (package["name"], expected) not in lock_pairs)
if missing:
    raise SystemExit("Cargo.lock lacks current workspace packages: " + ", ".join(missing))
PY

(
  cd "${ROOT}"
  cargo build -p cli --features full
)
[[ -x "${BIN}" ]] || { echo "missing cowd binary: ${BIN}" >&2; exit 1; }
"${BIN}" --version | grep -F "${VERSION}" >/dev/null || {
  echo "cowd --version does not contain ${VERSION}" >&2
  exit 1
}

mkdir -p "${CONFIG_HOME}" "${TEST_HOME}/.cowd" "${WORKSPACE}/.cowd"
cat >"${CONFIG_HOME}/config.yaml" <<EOF
model: "claude-sonnet-4-6"
providers:
  anthropic:
    base_url: "https://api.anthropic.com/v1"
    api_key: "test-dummy-key-for-version-gate"
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
    --expected-epoch "${epoch}" --expected-revision "${revision}" --confirm "${confirmation}" >/dev/null

OPENAPI="${SCENARIO_ROOT}/openapi.json"
CONTRACT="${SCENARIO_ROOT}/contract.json"
curl -fsS -H "Authorization: Bearer ${TOKEN}" "${BASE_URL}/api/gateway/openapi.json" >"${OPENAPI}"
curl -fsS -H "Authorization: Bearer ${TOKEN}" "${BASE_URL}/api/apps/mfg/contract" >"${CONTRACT}"
jq -e --arg version "${VERSION}" '.openapi == "3.1.0" and .info.version == $version' "${OPENAPI}" >/dev/null
jq -e '([.routes[] | select(.availability == "active")] | length) == 105' "${CONTRACT}" >/dev/null
for stale in 0.9.540 0.9.541 0.9.542 0.9.543 0.9.544 0.9.545 0.9.546 0.9.547; do
  if grep -F "${stale}" "${OPENAPI}" "${CONTRACT}" >/dev/null; then
    echo "Gateway OpenAPI or active contract exposes stale release ${stale}" >&2
    exit 1
  fi
done

echo "backend version gate passed: ${VERSION}"
