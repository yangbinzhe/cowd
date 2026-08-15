#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REFERENCE_ROOT="$ROOT/tests/reference-app"
ARTIFACT_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cowd-reference-app.XXXXXX")"
cleanup() {
  chmod -R u+w "$ARTIFACT_ROOT" 2>/dev/null || true
  rm -rf -- "$ARTIFACT_ROOT"
}
trap cleanup EXIT INT TERM

echo "[reference-app] standalone contracts and Worker lifecycle"
cargo test --locked --offline --manifest-path "$REFERENCE_ROOT/Cargo.toml" --all-targets
node "$REFERENCE_ROOT/scripts/test-webui.mjs"

echo "[reference-app] deterministic signed Bundle"
PACKAGE_JSON="$($REFERENCE_ROOT/scripts/package.sh "$ARTIFACT_ROOT/reference-app")"
PUBLIC_KEY="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["public_key_base64url"])' <<<"$PACKAGE_JSON")"
cargo run --locked --offline --release --manifest-path "$REFERENCE_ROOT/Cargo.toml" \
  --bin reference-app-bundle -- verify --bundle "$ARTIFACT_ROOT/reference-app"
cargo run --locked --offline --release --manifest-path "$REFERENCE_ROOT/Cargo.toml" \
  --bin reference-app-bundle -- discover --apps-root "$ARTIFACT_ROOT"

echo "[reference-app] Gateway catalog, supervisor, invoke, stream and TUI proxy"
COWD_REFERENCE_APP_BUNDLE="$ARTIFACT_ROOT/reference-app" \
COWD_REFERENCE_APP_PUBLIC_KEY_BASE64URL="$PUBLIC_KEY" \
  cargo test --locked -p gateway reference_bundle_gateway_proxy_e2e
