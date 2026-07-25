#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# `clippy::all` and `clippy::pedantic` evolve with the compiler and are tracked
# separately as advisory hygiene. This gate only blocks release on diagnostics
# that can hide a production crash, unfinished path, debug leak, or process exit.
# Test targets intentionally use assertion-style panics and are covered by the
# full regression lane; applying production panic policy to them makes the gate
# report fixture style instead of executable safety.
exec cargo clippy --workspace --lib --bins -- \
  -D warnings \
  -A clippy::all \
  -A clippy::pedantic \
  -D clippy::unwrap_used \
  -D clippy::expect_used \
  -D clippy::panic \
  -D clippy::todo \
  -D clippy::unimplemented \
  -D clippy::unreachable \
  -D clippy::dbg_macro \
  -D clippy::exit
