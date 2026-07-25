#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE="$(mktemp -d /tmp/cleanup-artifacts-test.XXXXXX)"

cleanup() {
  rm -rf "$FIXTURE"
}
trap cleanup EXIT

mkdir -p \
  "$FIXTURE/cowd-owned" \
  "$FIXTURE/cowd_owned" \
  "$FIXTURE/gateway-foreign" \
  "$FIXTURE/memory-foreign" \
  "$FIXTURE/storage-foreign"

COWD_TMP_ROOT="$FIXTURE" bash "$ROOT/scripts/release/clean-build-artifacts.sh" --tmp >/dev/null

[[ ! -e "$FIXTURE/cowd-owned" ]]
[[ ! -e "$FIXTURE/cowd_owned" ]]
[[ -d "$FIXTURE/gateway-foreign" ]]
[[ -d "$FIXTURE/memory-foreign" ]]
[[ -d "$FIXTURE/storage-foreign" ]]

echo "clean build artifacts ownership test passed"
