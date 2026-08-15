#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EDGE_ROOT="${COWD_EDGE_ROOT:-${ROOT}/../cowd-edge}"
APP_ROOT="${COWD_APP_ROOT:?set COWD_APP_ROOT to the independently built APP repository}"
APP_BUNDLE="${COWD_APP_BUNDLE:?set COWD_APP_BUNDLE to the signed .cowd-app artifact}"
APP_PUBLIC_KEY="${COWD_APP_PUBLIC_KEY:?set COWD_APP_PUBLIC_KEY to the trusted Ed25519 public key}"
CORE_ARTIFACT="${COWD_CORE_ARTIFACT:?set COWD_CORE_ARTIFACT to the packaged Cowd artifact}"
EDGE_ARTIFACT="${COWD_EDGE_ARTIFACT:?set COWD_EDGE_ARTIFACT to the packaged cowd-edge artifact}"
PROTOCOL_DIGEST="${COWD_APP_PROTOCOL_DIGEST:?set COWD_APP_PROTOCOL_DIGEST to the frozen sha256: protocol digest}"
OUTPUT="${1:-${ROOT}/target/release-evidence/cross-repo-provenance.json}"
EVIDENCE_TMP="$(mktemp -d "${TMPDIR:-/tmp}/cowd-provenance.XXXXXX")"
trap 'rm -rf -- "$EVIDENCE_TMP"' EXIT INT TERM

for repository in "$ROOT" "$EDGE_ROOT" "$APP_ROOT"; do
  git -C "$repository" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
    echo "required release repository is missing: $repository" >&2
    exit 1
  }
  git -C "$repository" diff --quiet
  git -C "$repository" diff --cached --quiet
done
[[ -f "$APP_BUNDLE" ]] || { echo "APP Bundle is missing: $APP_BUNDLE" >&2; exit 1; }

key_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["signature"]["key_id"])' "$APP_BUNDLE/app.json")"
python3 - "$EVIDENCE_TMP/trust.json" "$key_id" "$APP_PUBLIC_KEY" <<'PY'
import json, pathlib, sys
path, key_id, public_key = sys.argv[1:]
pathlib.Path(path).write_text(json.dumps({"schema_version": 1, "keys": [{"key_id": key_id, "public_key_base64url": public_key, "revoked": False}]}) + "\n")
PY
chmod 0600 "$EVIDENCE_TMP/trust.json"
cargo run --locked -q -p xtask -- apps assemble \
  --core "$CORE_ARTIFACT" --edge "$EDGE_ARTIFACT" \
  --trust-store "$EVIDENCE_TMP/trust.json" --protocol-digest "$PROTOCOL_DIGEST" \
  --generation provenance --output "$EVIDENCE_TMP/product" --required-app "$APP_BUNDLE" >/dev/null

core_head="$(git -C "$ROOT" rev-parse HEAD)"
edge_head="$(git -C "$EDGE_ROOT" rev-parse HEAD)"
app_head="$(git -C "$APP_ROOT" rev-parse HEAD)"
bundle_sha="$(sha256sum "$APP_BUNDLE" | awk '{print $1}')"
protocol_sha="$(sha256sum "$ROOT/crates/app-protocol/contracts/v1/contract-manifest.json" | awk '{print $1}')"
assembler_sha="$(sha256sum "$ROOT/crates/xtask/src/apps/assembler.rs" | awk '{print $1}')"

mkdir -p "$(dirname "$OUTPUT")"
python3 - "$OUTPUT" "$core_head" "$edge_head" "$app_head" "$bundle_sha" "$protocol_sha" "$assembler_sha" <<'PY'
import json
import pathlib
import sys

output, core, edge, app, bundle, protocol, assembler = sys.argv[1:]
payload = {
    "kind": "cowd.release.cross_repo_provenance.v2",
    "repositories": {
        "core": {"commit": core},
        "edge": {"commit": edge},
        "app": {"commit": app},
    },
    "artifacts": {
        "app_bundle_sha256": bundle,
        "app_protocol_contract_sha256": protocol,
        "assembler_source_sha256": assembler,
    },
    "checks": {
        "repositories_clean": True,
        "bundle_signature_verified": True,
        "no_cross_repository_source_pin_required": True,
    },
}
path = pathlib.Path(output)
path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY

echo "cross-repository artifact provenance verified: $OUTPUT"
