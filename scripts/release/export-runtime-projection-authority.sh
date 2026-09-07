#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  echo "usage: $0 OUTPUT_DIR" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COWD_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_DIR="$1"
if [[ -e "$OUTPUT_DIR" ]]; then
  echo "authority output already exists: $OUTPUT_DIR" >&2
  exit 1
fi
OUTPUT_PARENT="$(dirname "$OUTPUT_DIR")"
mkdir -p "$OUTPUT_PARENT"
OUTPUT_PARENT="$(cd "$OUTPUT_PARENT" && pwd)"
OUTPUT_DIR="$OUTPUT_PARENT/$(basename "$OUTPUT_DIR")"
STAGE="$(mktemp -d "$OUTPUT_PARENT/.runtime-projection-authority.XXXXXX")"
cleanup() {
  if [[ -d "$STAGE" ]]; then
    rm -rf "$STAGE"
  fi
}
trap cleanup EXIT

(
  cd "$COWD_ROOT"
  COWD_EXPORT_RUNTIME_PROJECTION_SCHEMAS="$STAGE" \
    cargo test -p gateway \
      services::core_matrix_catalog::tests::runtime_projection_bridge_schemas_are_exportable_from_core_authority \
      -- --exact
)

python3 - "$STAGE" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
operation_ids = (
    "core.runtime.execution_projection.changes",
    "core.runtime.execution_projection.snapshot",
)
operations = []
for operation_id in operation_ids:
    operation = {"operation_id": operation_id}
    for direction in ("input", "output"):
        name = f"{operation_id}.{direction}.schema.json"
        data = (root / name).read_bytes()
        parsed = json.loads(data)
        canonical = json.dumps(
            parsed, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode()
        if data != canonical:
            raise SystemExit(f"Cowd exporter emitted non-canonical schema: {name}")
        operation[f"{direction}_schema"] = {
            "path": name,
            "digest": "sha256:" + hashlib.sha256(data).hexdigest(),
        }
    operations.append(operation)
artifact = {
    "schema_version": 1,
    "authority": "cowd-core",
    "operations": operations,
}
(root / "runtime-projection-authority.json").write_text(
    json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

mv "$STAGE" "$OUTPUT_DIR"
trap - EXIT
printf 'Cowd Runtime projection authority: %s\n' "$OUTPUT_DIR"
