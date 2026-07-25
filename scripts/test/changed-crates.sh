#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

MODE="${1:-}"
BASE="${1:-${COWD_CHANGED_BASE:-HEAD}}"
PACKAGE_MAP="$(mktemp)"
trap 'rm -f "$PACKAGE_MAP"' EXIT

cargo metadata --format-version 1 --no-deps \
  | jq -r --arg root "$ROOT/" '
      .packages[]
      | [.name, (.manifest_path | sub("/Cargo.toml$"; "") | ltrimstr($root))]
      | @tsv
    ' \
  | awk -F '\t' '{ print length($2) "\t" $0 }' \
  | sort -rn \
  | cut -f2- > "$PACKAGE_MAP"

package_for_file() {
  local file="$1"
  local package directory
  case "$file" in
    apps/mfg/*)
      printf '%s\n' cowd-product-apps
      return 0
      ;;
  esac
  while IFS=$'\t' read -r package directory; do
    case "$file" in
      "$directory"|"$directory"/*)
        printf '%s\n' "$package"
        return 0
        ;;
    esac
  done < "$PACKAGE_MAP"
}

if [[ "$MODE" == "--packages-for" ]]; then
  shift
  for file in "$@"; do
    package_for_file "$file" || true
  done | sort -u
  exit 0
fi

changed_files="$({
  git diff --name-only "$BASE" -- 2>/dev/null || true
  git diff --name-only --cached -- 2>/dev/null || true
  git ls-files --others --exclude-standard
} | sort -u)"

echo "[changed] base=$BASE"
if [[ -z "$changed_files" ]]; then
  echo "[changed] no changed files; running quick gate"
  exec "$ROOT/scripts/test/quick.sh"
fi

printf '[changed] %s\n' "$changed_files"

workspace_wide=0
interactive_changed=0
if grep -Eq '^(Cargo\.toml|Cargo\.lock|rust-toolchain|\.cargo/)' <<<"$changed_files"; then
  workspace_wide=1
fi
if grep -Eq '^tests/interactive/' <<<"$changed_files"; then
  interactive_changed=1
fi
packages="$(
  while IFS= read -r file; do
    [[ -n "$file" ]] || continue
    package_for_file "$file" || true
  done <<<"$changed_files" | sort -u
)"

echo "[changed] cargo fmt --all --check"
cargo fmt --all --check

echo "[changed] cargo check --workspace --all-targets"
cargo check --workspace --all-targets

if [[ "$interactive_changed" == "1" ]]; then
  echo "[changed] cargo check --manifest-path tests/interactive/Cargo.toml"
  cargo check --manifest-path tests/interactive/Cargo.toml
fi

if [[ "$workspace_wide" == "1" ]]; then
  echo "[changed] workspace manifest changed; running all workspace targets"
  cargo test --workspace --all-targets
  exit 0
fi

if [[ -z "$packages" ]]; then
  echo "[changed] no package-local changes; running quick gate"
  "$ROOT/scripts/test/quick.sh"
  exit 0
fi

printf '[changed] packages:\n%s\n' "$packages"
while IFS= read -r package; do
  [[ -n "$package" ]] || continue
  echo "[changed] cargo test -p $package --all-targets"
  cargo test -p "$package" --all-targets
done <<<"$packages"
