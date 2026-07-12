#!/usr/bin/env bash
set -euo pipefail

# Compare the current candidate with the last V6 baseline through exactly the
# same public Session API. The default controlled mode uses a deterministic
# local OpenAI-compatible fixture so the hard performance gate measures
# Gateway/Runtime work rather than remote model/network jitter. Live-provider
# behavior remains a separately archived capability evaluation in
# v9-terminal-gate. This script deliberately creates and removes an isolated
# historical worktree; it never points a test at an operator's running Gateway
# or writes to the normal COWD home.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${COWD_V9_PERFORMANCE_MODE:-controlled}"
SOURCE_CONFIG_HOME="${COWD_EVAL_CONFIG_HOME:-}"
BASELINE_REF="${COWD_V9_PERFORMANCE_BASELINE_REF:-v0.9.476}"
CANDIDATE_PORT="${COWD_V9_PERFORMANCE_CANDIDATE_PORT:-8766}"
BASELINE_PORT="${COWD_V9_PERFORMANCE_BASELINE_PORT:-8765}"
PROVIDER_PORT="${COWD_V9_PERFORMANCE_PROVIDER_PORT:-8877}"
CANDIDATE_URL="http://127.0.0.1:${CANDIDATE_PORT}"
BASELINE_URL="http://127.0.0.1:${BASELINE_PORT}"
MODEL="${COWD_EVAL_MODEL:-deepseek-v4-flash}"
PAIRS="${COWD_V9_PERFORMANCE_PAIRS:-5}"
ARCHIVE_ROOT="${COWD_V9_PERFORMANCE_ARCHIVE_DIR:-$ROOT/target/v9-performance-artifacts}"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cowd-v9-performance.XXXXXX")"
FIXTURE_CONFIG="$ROOT/scripts/fixtures/v9-performance-config.yaml"
FIXTURE_PROVIDER="$ROOT/scripts/v9-fake-openai-provider.mjs"
BASELINE_WORKTREE="$TEMP_ROOT/baseline-worktree"
CANDIDATE_HOME="$TEMP_ROOT/candidate-home"
BASELINE_HOME="$TEMP_ROOT/baseline-home"
CANDIDATE_LOG="$TEMP_ROOT/candidate.log"
BASELINE_LOG="$TEMP_ROOT/baseline.log"
PROVIDER_LOG="$TEMP_ROOT/provider.log"
CANDIDATE_PID=""
BASELINE_PID=""
PROVIDER_PID=""

case "$MODE" in
  controlled|live) ;;
  *) echo "unsupported COWD_V9_PERFORMANCE_MODE: $MODE (expected controlled or live)" >&2; exit 2 ;;
esac

if [[ "$MODE" == "controlled" ]]; then
  [[ -f "$FIXTURE_CONFIG" ]] || { echo "missing controlled performance fixture config" >&2; exit 2; }
  [[ -f "$FIXTURE_PROVIDER" ]] || { echo "missing controlled performance fixture provider" >&2; exit 2; }
  MODEL="cowd-v9-performance-fixture"
elif [[ -z "$SOURCE_CONFIG_HOME" ]]; then
  echo "set COWD_EVAL_CONFIG_HOME when COWD_V9_PERFORMANCE_MODE=live" >&2
  exit 2
fi

cleanup() {
  for pid in "$CANDIDATE_PID" "$BASELINE_PID" "$PROVIDER_PID"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ -d "$BASELINE_WORKTREE/.git" || -f "$BASELINE_WORKTREE/.git" ]]; then
    git -C "$ROOT" worktree remove --force "$BASELINE_WORKTREE" >/dev/null 2>&1 || true
  fi
  rm -rf "$TEMP_ROOT"
}
trap cleanup EXIT INT TERM

prepare_home() {
  local home="$1"
  local port="$2"
  mkdir -p "$home"
  if [[ "$MODE" == "controlled" ]]; then
    cp "$FIXTURE_CONFIG" "$home/config.yaml"
    sed -i "s|__PERFORMANCE_PROVIDER_URL__|http://127.0.0.1:${PROVIDER_PORT}/v1|" "$home/config.yaml"
    sed -i "s|__PERFORMANCE_STORAGE_ROOT__|${home}/storage|" "$home/config.yaml"
    sed -i "0,/^      port: [0-9][0-9]*/s//      port: ${port}/" "$home/config.yaml"
    return
  fi
  for file in config.yaml models.yaml credentials.json; do
    if [[ -f "$SOURCE_CONFIG_HOME/$file" ]]; then
      cp "$SOURCE_CONFIG_HOME/$file" "$home/$file"
    fi
  done
  for directory in profiles plugins; do
    if [[ -d "$SOURCE_CONFIG_HOME/$directory" ]]; then
      cp -a "$SOURCE_CONFIG_HOME/$directory" "$home/$directory"
    fi
  done
  [[ -f "$home/config.yaml" ]] || { echo "missing $SOURCE_CONFIG_HOME/config.yaml" >&2; exit 1; }
  sed -i "0,/^    port: [0-9][0-9]*/s//    port: ${port}/" "$home/config.yaml"
  sed -i "s|^  store_path: .*|  store_path: ${home}/memory|" "$home/config.yaml"
}

wait_provider() {
  for _ in $(seq 1 80); do
    if curl --fail --silent --show-error "http://127.0.0.1:${PROVIDER_PORT}/not-ready" >/dev/null 2>&1; then
      return 0
    fi
    if grep -q "listening" "$PROVIDER_LOG" 2>/dev/null; then
      return 0
    fi
    sleep 0.05
  done
  cat "$PROVIDER_LOG" >&2
  return 1
}

wait_gateway() {
  local url="$1"
  local log="$2"
  for _ in $(seq 1 120); do
    if curl --fail --silent --show-error "$url/healthz" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  cat "$log" >&2
  return 1
}

prepare_home "$CANDIDATE_HOME" "$CANDIDATE_PORT"
prepare_home "$BASELINE_HOME" "$BASELINE_PORT"
if [[ "$MODE" == "controlled" ]]; then
  node "$FIXTURE_PROVIDER" >"$PROVIDER_LOG" 2>&1 &
  PROVIDER_PID=$!
  wait_provider
fi
if [[ "$MODE" == "controlled" ]]; then
  TOKEN="${COWD_EVAL_GATEWAY_TOKEN:-cowd-v9-performance-fixture-token}"
else
  TOKEN="${COWD_EVAL_GATEWAY_TOKEN:-$(sed -n '/auth:/,/platform_type:/ { s/^[[:space:]]*token:[[:space:]]*//p; }' "$CANDIDATE_HOME/config.yaml" | head -1)}"
fi
[[ -n "$TOKEN" ]] || { echo "missing Gateway API token" >&2; exit 1; }

git -C "$ROOT" rev-parse --verify --quiet "$BASELINE_REF^{commit}" >/dev/null
git -C "$ROOT" worktree add --detach "$BASELINE_WORKTREE" "$BASELINE_REF" >/dev/null

# Keep baseline build artifacts inside the disposable worktree. Candidate uses
# its normal incremental target so the gate measures Runtime work, not a cold
# compilation difference.
cargo build -p cli -p harness-eval
CARGO_TARGET_DIR="$TEMP_ROOT/baseline-target" \
  cargo build --manifest-path "$BASELINE_WORKTREE/Cargo.toml" -p cli

COWD_CONFIG_HOME="$CANDIDATE_HOME" \
  "$ROOT/target/debug/cowd" gateway run --port "$CANDIDATE_PORT" >"$CANDIDATE_LOG" 2>&1 &
CANDIDATE_PID=$!
COWD_CONFIG_HOME="$BASELINE_HOME" \
  "$TEMP_ROOT/baseline-target/debug/cowd" gateway run --port "$BASELINE_PORT" >"$BASELINE_LOG" 2>&1 &
BASELINE_PID=$!
wait_gateway "$CANDIDATE_URL" "$CANDIDATE_LOG"
wait_gateway "$BASELINE_URL" "$BASELINE_LOG"

REPORT_DIR="$ARCHIVE_ROOT/$(date +%s)-${MODEL//[^[:alnum:]._-]/_}"
mkdir -p "$REPORT_DIR"
REPORT_PATH="$REPORT_DIR/paired-performance.json"
COWD_API_TOKEN="$TOKEN" \
  "$ROOT/target/debug/harness-eval" paired-performance \
    --baseline-url "$BASELINE_URL" \
    --candidate-url "$CANDIDATE_URL" \
    --provider "$MODEL" \
    --pairs "$PAIRS" \
    --poll-interval-ms "${COWD_V9_PERFORMANCE_POLL_INTERVAL_MS:-20}" \
    --output "$REPORT_PATH"

node - <<'NODE' "$REPORT_PATH" "$REPORT_DIR/metadata.json" "$ROOT" "$BASELINE_WORKTREE" "$BASELINE_REF" "$MODEL" "$MODE"
const fs = require("fs");
const cp = require("child_process");
const crypto = require("crypto");
const [reportPath, output, root, baseline, baselineRef, model, mode] = process.argv.slice(2);
const rev = (dir) => cp.execFileSync("git", ["-c", `safe.directory=${dir}`, "-C", dir, "rev-parse", "HEAD"], {encoding: "utf8"}).trim();
const workingTreeFingerprint = (dir) => {
  const hash = crypto.createHash("sha256");
  const diff = cp.execFileSync("git", ["-c", `safe.directory=${dir}`, "-C", dir, "diff", "--binary", "HEAD"], {
    encoding: "buffer",
    maxBuffer: 64 * 1024 * 1024,
  });
  hash.update(diff);
  const untracked = cp.execFileSync("git", ["-c", `safe.directory=${dir}`, "-C", dir, "ls-files", "--others", "--exclude-standard", "-z"], {
    encoding: "buffer",
  }).toString("utf8").split("\0").filter(Boolean).sort();
  for (const relative of untracked) {
    hash.update(relative);
    hash.update(fs.readFileSync(require("path").join(dir, relative)));
  }
  return {
    dirty: diff.length > 0 || untracked.length > 0,
    sha256: hash.digest("hex"),
    untracked_files: untracked,
  };
};
const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
report.measurement = {
  ...report.measurement,
  mode,
  hard_gate_scope: mode === "controlled"
    ? "deterministic local provider; Gateway/Runtime public API overhead"
    : "remote provider observation only; external model/network jitter is retained but not attributable to Runtime",
};
fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
fs.writeFileSync(output, JSON.stringify({
  kind: "harness_eval.paired_performance_metadata",
  candidate_commit: rev(root),
  candidate_worktree: workingTreeFingerprint(root),
  baseline_commit: rev(baseline),
  baseline_ref: baselineRef,
  model,
  mode,
  generated_at: new Date().toISOString(),
}, null, 2));
NODE

echo "V9 paired performance gate passed (${MODE}): $REPORT_PATH"
