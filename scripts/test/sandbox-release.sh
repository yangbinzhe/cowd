#!/usr/bin/env bash
set -euo pipefail

# P7: run the real sandbox (bwrap + inner role) tests against an explicit
# Cowd binary. `cargo test` harnesses cannot carry the inner role, so the
# launcher is injected through COWD_SANDBOX_LAUNCHER_BINARY and verified by
# protocol before use.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
LAUNCHER="${COWD_SANDBOX_LAUNCHER_BINARY:-$TARGET_ROOT/release/cowd}"

if [[ ! -x "$LAUNCHER" ]]; then
  echo "building release launcher (missing: $LAUNCHER)" >&2
  cargo build --release --bin cowd --manifest-path "$ROOT/Cargo.toml"
fi

export COWD_SANDBOX_LAUNCHER_BINARY="$LAUNCHER"
cargo test --manifest-path "$ROOT/Cargo.toml" -p sandbox-launcher --lib -- --test-threads=1
cargo test --manifest-path "$ROOT/Cargo.toml" -p tools --lib -- bash:: sandbox_exec:: -- --test-threads=1

echo "sandbox release tests passed with launcher: $LAUNCHER"
