#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EDGE_ROOT="${COWD_EDGE_ROOT:-${ROOT}/../cowd-edge}"
MFG_ROOT="${COWD_MFG_ROOT:-${ROOT}/../cowd-app-mfg}"
OUTPUT="${1:-${ROOT}/target/release-evidence/cross-repo-provenance.json}"

for repository in "$ROOT" "$EDGE_ROOT" "$MFG_ROOT"; do
  git -C "$repository" rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
    echo "required release repository is missing: $repository" >&2
    exit 1
  }
done

workspace_version() {
  sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' "$1/Cargo.toml" | head -1
}

core_version="$(workspace_version "$ROOT")"
edge_version="$(workspace_version "$EDGE_ROOT")"
mfg_version="$(workspace_version "$MFG_ROOT")"
[[ -n "$core_version" && "$core_version" == "$edge_version" && "$core_version" == "$mfg_version" ]] || {
  echo "release versions diverge: core=$core_version edge=$edge_version mfg=$mfg_version" >&2
  exit 1
}

core_head="$(git -C "$ROOT" rev-parse HEAD)"
edge_head="$(git -C "$EDGE_ROOT" rev-parse HEAD)"
mfg_head="$(git -C "$MFG_ROOT" rev-parse HEAD)"

mapfile -t core_mfg_revisions < <(
  rg -No 'rev = "[0-9a-f]{40}"' \
    "$ROOT/crates/product-apps/Cargo.toml" \
    "$ROOT/crates/gateway/Cargo.toml" \
    | sed -E 's/.*rev = "([0-9a-f]{40})"/\1/' \
    | sort -u
)
[[ "${#core_mfg_revisions[@]}" -eq 1 && "${core_mfg_revisions[0]}" == "$mfg_head" ]] || {
  echo "Core must pin the exact MFG release commit $mfg_head; found: ${core_mfg_revisions[*]:-none}" >&2
  exit 1
}

mapfile -t mfg_core_revisions < <(
  rg -No 'rev = "[0-9a-f]{40}"' "$MFG_ROOT/crates" --glob Cargo.toml \
    | sed -E 's/.*rev = "([0-9a-f]{40})"/\1/' \
    | sort -u
)
[[ "${#mfg_core_revisions[@]}" -eq 1 ]] || {
  echo "MFG foundation dependencies must use one Core revision; found: ${mfg_core_revisions[*]:-none}" >&2
  exit 1
}
mfg_core_revision="${mfg_core_revisions[0]}"
git -C "$ROOT" merge-base --is-ancestor "$mfg_core_revision" "$core_head" || {
  echo "MFG foundation revision $mfg_core_revision is not an ancestor of Core $core_head" >&2
  exit 1
}

for repository in "$ROOT" "$EDGE_ROOT" "$MFG_ROOT"; do
  git -C "$repository" diff --quiet
  git -C "$repository" diff --cached --quiet
done

mkdir -p "$(dirname "$OUTPUT")"
python3 - "$OUTPUT" "$core_version" "$core_head" "$edge_head" "$mfg_head" "$mfg_core_revision" <<'PY'
import json
import pathlib
import sys

output, version, core, edge, mfg, mfg_core = sys.argv[1:]
payload = {
    "kind": "cowd.release.cross_repo_provenance",
    "version": version,
    "repositories": {
        "core": {"commit": core},
        "edge": {"commit": edge},
        "mfg": {"commit": mfg, "core_foundation_commit": mfg_core},
    },
    "checks": {
        "versions_equal": True,
        "core_pins_exact_mfg_commit": True,
        "mfg_uses_single_core_foundation": True,
        "mfg_core_foundation_is_core_ancestor": True,
        "worktrees_clean": True,
    },
}
path = pathlib.Path(output)
path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY

echo "cross-repo provenance verified: $OUTPUT"
