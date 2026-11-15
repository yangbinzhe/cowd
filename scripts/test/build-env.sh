#!/usr/bin/env bash
# Source from both the gate and baseline to keep build artifacts on the data disk.
export COWD_BUILD_ROOT="${COWD_BUILD_ROOT:-/media/yi/Datas/workspace/.cowd-build}"
export RUSTUP_TOOLCHAIN="nightly-2026-11-09-x86_64-unknown-linux-gnu"
export RUSTC_WRAPPER="sccache"
export SCCACHE_DIR="${SCCACHE_DIR:-$COWD_BUILD_ROOT/sccache}"
export SCCACHE_CACHE_SIZE="${SCCACHE_CACHE_SIZE:-25G}"
export CARGO_INCREMENTAL=0
export CARGO_HOME="${CARGO_HOME:-$COWD_BUILD_ROOT/cargo-home}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$COWD_BUILD_ROOT/default-target}"
export COWD_GATE_LOG_DIR="${COWD_GATE_LOG_DIR:-$COWD_BUILD_ROOT/gate-logs}"
export COWD_TEST_PRIVATE_PARENT="${COWD_TEST_PRIVATE_PARENT:-/dev/shm}"
# Inherited interactive shell settings may still point at the root disk.
# Preserve per-leg targets only when they live below the configured build root.
_cowd_build_root="$(realpath -m -- "$COWD_BUILD_ROOT")"
for _cowd_name in CARGO_HOME CARGO_TARGET_DIR SCCACHE_DIR COWD_GATE_LOG_DIR; do
  _cowd_path="$(realpath -m -- "${!_cowd_name}")"
  case "$_cowd_path" in
    "$_cowd_build_root"/*) ;;
    *)
      printf 'warning: %s outside build root; using data-disk default\n' "$_cowd_name" >&2
      case "$_cowd_name" in
        CARGO_HOME) export CARGO_HOME="$COWD_BUILD_ROOT/cargo-home" ;;
        CARGO_TARGET_DIR) export CARGO_TARGET_DIR="$COWD_BUILD_ROOT/default-target" ;;
        SCCACHE_DIR) export SCCACHE_DIR="$COWD_BUILD_ROOT/sccache" ;;
        COWD_GATE_LOG_DIR) export COWD_GATE_LOG_DIR="$COWD_BUILD_ROOT/gate-logs" ;;
      esac ;;
  esac
done
unset _cowd_build_root _cowd_name _cowd_path
mkdir -p "$CARGO_HOME" "$CARGO_TARGET_DIR" "$SCCACHE_DIR" "$COWD_GATE_LOG_DIR"
