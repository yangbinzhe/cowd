#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

BASE="${1:-${COWD_CHANGED_BASE:-HEAD}}"

map_crate() {
  local path="$1"
  case "$path" in
    crates/matrix/core/*) echo "matrix-core" ;;
    crates/matrix/repository/*) echo "matrix-repository" ;;
    crates/skill/service/*) echo "skill-service" ;;
    crates/*/*)
      local name
      name="$(printf '%s\n' "$path" | cut -d/ -f2)"
      case "$name" in
        app-mfg|approval|cli|connector|fact-kernel|gateway|harness-contract|harness-eval|mcp|memory|model-protocol|plugins|provider|runtime|session|storage|surface|tools|tui)
          echo "$name"
          ;;
      esac
      ;;
  esac
}

changed_files="$(
  {
    git diff --name-only "$BASE" -- 2>/dev/null || true
    git diff --name-only --cached -- 2>/dev/null || true
    git ls-files --others --exclude-standard
  } | sort -u
)"

packages="$(
  while IFS= read -r file; do
    [[ -n "$file" ]] || continue
    map_crate "$file"
  done <<<"$changed_files" | sort -u
)"

echo "[changed] base=$BASE"
if [[ -z "$changed_files" ]]; then
  echo "[changed] no changed files; running quick gate"
  exec "$ROOT/scripts/test/quick.sh"
fi

echo "[changed] changed files:"
printf '  %s\n' $changed_files

echo "[changed] cargo fmt --all --check"
cargo fmt --all --check

echo "[changed] cargo check --workspace --all-targets"
cargo check --workspace --all-targets

if [[ -z "$packages" ]]; then
  echo "[changed] no crate-local changes detected; quick gate is sufficient"
  "$ROOT/scripts/test/quick.sh"
  exit 0
fi

echo "[changed] packages:"
printf '  %s\n' $packages

for package in $packages; do
  case "$package" in
    tui)
      echo "[changed] cargo test -p tui --lib"
      cargo test -p tui --lib
      ;;
    gateway)
      echo "[changed] cargo test -p gateway --test gateway_runtimehost_architecture"
      cargo test -p gateway --test gateway_runtimehost_architecture
      echo "[changed] cargo test -p gateway --all-targets"
      cargo test -p gateway --all-targets
      ;;
    runtime)
      echo "[changed] cargo test -p runtime --test runtime_module_architecture"
      cargo test -p runtime --test runtime_module_architecture
      echo "[changed] cargo test -p runtime --all-targets"
      cargo test -p runtime --all-targets
      ;;
    memory)
      echo "[changed] cargo test -p memory --test memory_module_architecture"
      cargo test -p memory --test memory_module_architecture
      echo "[changed] cargo test -p memory --all-targets"
      cargo test -p memory --all-targets
      ;;
    *)
      echo "[changed] cargo test -p $package --all-targets"
      cargo test -p "$package" --all-targets
      ;;
  esac
done

