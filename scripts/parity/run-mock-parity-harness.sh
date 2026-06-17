#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

cargo test -p cowd-cli --test mock_parity_harness -- --nocapture
