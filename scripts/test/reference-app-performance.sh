#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REFERENCE_ROOT="$ROOT/tests/reference-app"
ARTIFACT_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cowd-reference-performance.XXXXXX")"
HOST_LOG="$ARTIFACT_ROOT/app-host.log"
REFERENCE_REPORT="$(mktemp /tmp/cowd-reference-transport.XXXXXX.json)"
REPORT="${COWD_PERFORMANCE_REPORT:-/tmp/cowd-reference-performance-$(date +%s).json}"

case "$REPORT" in
  /tmp/*) ;;
  *) echo "COWD_PERFORMANCE_REPORT must be below /tmp" >&2; exit 2 ;;
esac

cleanup() {
  chmod -R u+w "$ARTIFACT_ROOT" 2>/dev/null || true
  rm -rf -- "$ARTIFACT_ROOT"
  rm -f -- "$REFERENCE_REPORT"
}
trap cleanup EXIT INT TERM

echo "[performance] package signed reference Bundle"
PACKAGE_JSON="$($REFERENCE_ROOT/scripts/package.sh "$ARTIFACT_ROOT/reference-app")"
PUBLIC_KEY="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["public_key_base64url"])' <<<"$PACKAGE_JSON")"

echo "[performance] Catalog and Supervisor capacity contracts"
cargo test --locked -p cowd-app-host --lib -- --test-threads=1 --nocapture 2>&1 | tee "$HOST_LOG"

echo "[performance] reference cold, hot UDS/Gateway and stream contracts"
COWD_REFERENCE_APP_BUNDLE="$ARTIFACT_ROOT/reference-app" \
COWD_REFERENCE_APP_PUBLIC_KEY_BASE64URL="$PUBLIC_KEY" \
COWD_PERFORMANCE_REPORT="$REFERENCE_REPORT" \
  cargo test --locked -p gateway --lib \
    api_routes::app_routes::tests::reference_bundle_performance_contract -- \
    --ignored --exact --test-threads=1 --nocapture

python3 - "$HOST_LOG" "$REFERENCE_REPORT" "$REPORT" <<'PY'
import json
import pathlib
import sys

host_log, reference_report, output = map(pathlib.Path, sys.argv[1:])
prefix = "COWD_PERF_JSON "
host = []
for line in host_log.read_text().splitlines():
    marker = line.find(prefix)
    if marker >= 0:
        host.append(json.loads(line[marker + len(prefix):]))

required = {
    "catalog_100",
    "supervisor_singleflight_256",
    "supervisor_idle_reap",
    "supervisor_crash_budget",
    "supervisor_shutdown",
}
observed = {item["case"] for item in host}
missing = required - observed
fairness = sorted(
    item["active_limit"]
    for item in host
    if item["case"] == "supervisor_active_fairness"
)
if missing or fairness != [1, 4, 16]:
    raise SystemExit(f"incomplete AppHost evidence: missing={sorted(missing)} fairness={fairness}")

reference = json.loads(reference_report.read_text())
report = {
    "schema_version": 1,
    "thresholds": {
        "catalog_100_p95_ms": 500,
        "catalog_rss_kib": "32768 + 128 * app_count",
        "reference_cold_p95_ms": 1000,
        "reference_cold_p99_ms": 2000,
        "gateway_hot_p95_overhead": "max(2ms,15%)",
        "gateway_hot_throughput_minimum_ratio": 0.85,
        "gateway_cpu_per_request_maximum_ratio": 1.20,
        "stream_ttfb_overhead_ms": 10,
        "stream_cancel_ms": 1000,
        "idle_shutdown_budget_slack_ms": 1000,
    },
    "app_host": host,
    "reference_transport": reference,
    "passed": True,
}
output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
print(json.dumps({"passed": True, "report": str(output)}, sort_keys=True))
PY
