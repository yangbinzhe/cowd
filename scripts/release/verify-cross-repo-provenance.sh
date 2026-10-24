#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EDGE_ROOT="${COWD_EDGE_ROOT:-${ROOT}/../cowd-edge}"
APP_ROOT="${COWD_APP_ROOT:?set COWD_APP_ROOT to the independently built APP repository}"
APP_BUNDLE="${COWD_APP_BUNDLE:?set COWD_APP_BUNDLE to the signed .cowd-app directory}"
APP_PUBLIC_KEY="${COWD_APP_PUBLIC_KEY:?set COWD_APP_PUBLIC_KEY to the trusted Ed25519 public key}"
CORE_ARTIFACT="${COWD_CORE_ARTIFACT:?set COWD_CORE_ARTIFACT to the packaged Cowd artifact}"
EDGE_ARTIFACT="${COWD_EDGE_ARTIFACT:?set COWD_EDGE_ARTIFACT to the packaged cowd-edge artifact}"
PROTOCOL_DIGEST="${COWD_APP_PROTOCOL_DIGEST:?set COWD_APP_PROTOCOL_DIGEST to the frozen sha256: protocol digest}"
OUTPUT="${1:-${ROOT}/target/release-evidence/cross-repo-provenance.json}"
EVIDENCE_TMP="$(mktemp -d "${TMPDIR:-/tmp}/cowd-provenance.XXXXXX")"
cleanup() {
  local original_status=$? cleanup_status=0
  trap - EXIT INT TERM
  python3 - "$EVIDENCE_TMP" <<'PY' || cleanup_status=$?
import os, pathlib, stat, sys

# Only this invocation's private staging root is traversed. Directory FDs and
# O_NOFOLLOW keep output symlinks from granting access to inputs or repositories.
def empty_directory(fd):
    info = os.fstat(fd)
    if info.st_uid != os.getuid():
        raise RuntimeError("foreign staging directory owner")
    os.fchmod(fd, stat.S_IMODE(info.st_mode) | stat.S_IWUSR | stat.S_IXUSR)
    with os.scandir(fd) as entries:
        for entry in entries:
            info = os.stat(entry.name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                try:
                    empty_directory(child)
                finally:
                    os.close(child)
                os.rmdir(entry.name, dir_fd=fd)
            else:
                os.unlink(entry.name, dir_fd=fd)

try:
    root = pathlib.Path(sys.argv[1])
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        identity = os.fstat(fd)
        empty_directory(fd)
        current = root.lstat()
        if (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino):
            raise RuntimeError("staging root identity changed")
        os.rmdir(root)
    finally:
        os.close(fd)
except Exception:
    sys.exit(1)
PY
  if (( cleanup_status != 0 )); then
    echo "provenance cleanup failed; retained staging path: $EVIDENCE_TMP" >&2
    if (( original_status == 0 )); then original_status=1; fi
  elif (( original_status == 0 )); then
    echo "cross-repository artifact provenance verified: $OUTPUT"
  fi
  exit "$original_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for repository in "$ROOT" "$EDGE_ROOT" "$APP_ROOT"; do
  git -C "$repository" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
    echo "required release repository is missing: $repository" >&2
    exit 1
  }
  git -C "$repository" status --porcelain=v1 --untracked-files=normal >"$EVIDENCE_TMP/repository.status"
  [[ ! -s "$EVIDENCE_TMP/repository.status" ]] || {
    echo "required release repository has tracked or untracked changes: $repository" >&2
    exit 1
  }
done

python3 - "$APP_BUNDLE" "$EVIDENCE_TMP" "$APP_PUBLIC_KEY" <<'PY'
import json, os, pathlib, stat, sys
bundle, staging = map(pathlib.Path, sys.argv[1:3])
if not stat.S_ISDIR(bundle.lstat().st_mode):
    raise SystemExit("APP bundle must be a real non-symlink directory")
manifest = bundle / "app.json"
if not stat.S_ISREG(manifest.lstat().st_mode):
    raise SystemExit("app.json must be a regular non-symlink file")
data = json.loads(manifest.read_text(encoding="utf-8"))
app_id, key_id = data["app_id"], data["signature"]["key_id"]
if not isinstance(app_id, str) or not app_id or not isinstance(key_id, str) or not key_id:
    raise SystemExit("manifest app_id and signature key_id must be nonempty strings")
info = staging.lstat()
if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
    raise SystemExit("provenance staging root is not private")
(staging / "bundle-identity.json").write_text(json.dumps({"app_id": app_id}) + "\n")
(staging / "trust.json").write_text(json.dumps({"schema_version": 1, "keys": [{"key_id": key_id, "public_key_base64url": sys.argv[3], "revoked": False}]}) + "\n")
PY
chmod 0600 "$EVIDENCE_TMP/trust.json"
cargo run --locked -q -p xtask -- apps assemble \
  --core "$CORE_ARTIFACT" --edge "$EDGE_ARTIFACT" \
  --trust-store "$EVIDENCE_TMP/trust.json" --protocol-digest "$PROTOCOL_DIGEST" \
  --generation provenance --output "$EVIDENCE_TMP/product" --required-app "$APP_BUNDLE" >/dev/null

core_head="$(git -C "$ROOT" rev-parse HEAD)"
edge_head="$(git -C "$EDGE_ROOT" rev-parse HEAD)"
app_head="$(git -C "$APP_ROOT" rev-parse HEAD)"
bundle_sha="$(python3 - "$EVIDENCE_TMP" <<'PY'
import json, pathlib, re, stat, sys
staging = pathlib.Path(sys.argv[1])
output = staging / "product"
if not stat.S_ISDIR(output.lstat().st_mode):
    raise SystemExit("assembler output root must be a real directory")
product = output / "provenance"
lock_path = product / "release-lock.json"
if not stat.S_ISDIR(product.lstat().st_mode) or not stat.S_ISREG(lock_path.lstat().st_mode):
    raise SystemExit("assembler release lock must be a regular file in its own real output directory")
lock = json.loads(lock_path.read_text(encoding="utf-8"))
expected = json.loads((staging / "bundle-identity.json").read_text())["app_id"]
if (type(lock.get("schema_version")) is not int or lock["schema_version"] != 1
        or lock.get("kind") != "cowd.product.release-lock-provenance.v1"
        or lock.get("generation") != "provenance"):
    raise SystemExit("assembler release lock identity is invalid")
apps = lock.get("apps")
if not isinstance(apps, list) or len(apps) != 1 or not isinstance(apps[0], dict):
    raise SystemExit("assembler release lock must contain exactly one required app")
app = apps[0]
if app.get("required") is not True or app.get("app_id") != expected:
    raise SystemExit("assembler required app does not match the signed manifest")
digest = app.get("artifact_tree_digest")
if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
    raise SystemExit("assembler artifact tree digest is invalid")
print(digest.removeprefix("sha256:"))
PY
)"
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
        # Existing field: authoritative assembler tree digest, without prefix.
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
