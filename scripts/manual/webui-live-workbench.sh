#!/usr/bin/env bash
set -euo pipefail

SURFACE_WEBUI_DIR="${COWD_SURFACE_WEBUI_DIR:-}"

if [[ -z "$SURFACE_WEBUI_DIR" ]]; then
  echo "live WebUI workbench scenario moved to cowd-edge; set COWD_SURFACE_WEBUI_DIR to surfaces/webui"
  exit 0
fi

if [[ ! -d "$SURFACE_WEBUI_DIR" ]]; then
  echo "COWD_SURFACE_WEBUI_DIR does not exist: $SURFACE_WEBUI_DIR" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_WEBUI_LIVE_PORT:-18669}"
PROVIDER_PORT="${COWD_WEBUI_LIVE_PROVIDER_PORT:-18670}"
WEB_PORT="${COWD_WEBUI_LIVE_WEB_PORT:-18769}"
BASE_URL="http://127.0.0.1:$PORT"
WEB_URL="http://127.0.0.1:$WEB_PORT"
CHROMIUM="${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-/snap/bin/chromium}"
SESSION="cowd-edge-webui-live-$$"
API_TOKEN="cowd-webui-live-${PPID}-${RANDOM}"
WORKDIR="$(mktemp -d -t cowd-edge-webui-live-workspace-XXXXXX)"
CONFIG_HOME="$(mktemp -d -t cowd-edge-webui-live-config-XXXXXX)"
HOME_DIR="$(mktemp -d -t cowd-edge-webui-live-home-XXXXXX)"
LOG="$WORKDIR/gateway.log"
FAILED=0

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  if [[ "$FAILED" == "1" && "${COWD_WEBUI_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving WebUI live temp dir: $WORKDIR" >&2
    return
  fi
  rm -rf "$WORKDIR" "$CONFIG_HOME" "$HOME_DIR"
}
trap cleanup EXIT
on_error() {
  local status=$?
  FAILED=1
  echo "live WebUI scenario failed with status $status" >&2
  sed -n '1,260p' "$LOG" >&2 || true
  exit "$status"
}
trap on_error ERR

for cmd in tmux ss curl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd is required for live WebUI scenario" >&2
    exit 1
  fi
done

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi
if ss -ltnp | rg -q ":$PROVIDER_PORT\\b"; then
  echo "provider port $PROVIDER_PORT is already in use" >&2
  exit 1
fi
if ss -ltnp | rg -q ":$WEB_PORT\\b"; then
  echo "WebUI test port $WEB_PORT is already in use" >&2
  exit 1
fi

cd "$ROOT"
cargo build -p cli

# The edge WebUI is a separate repository. Build the exact checkout that the
# Gateway will serve and fail closed when its release entry is absent; running
# a Vite dev server or a missing historical spec is not browser evidence.
WEBUI_ROOT="$(cd "$SURFACE_WEBUI_DIR" && pwd)"
if [[ ! -f "$WEBUI_ROOT/package-lock.json" ]]; then
  echo "WebUI package lock is missing: $WEBUI_ROOT/package-lock.json" >&2
  exit 1
fi
(cd "$WEBUI_ROOT" && npm ci --ignore-scripts && npm run build)
[[ -f "$SURFACE_WEBUI_DIR/dist/index.html" ]] || {
  echo "WebUI release build did not produce dist/index.html" >&2
  exit 1
}

cat >"$WORKDIR/mock_provider.py" <<'PY'
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        return

    def do_POST(self):
        if self.path not in ("/chat/completions", "/v1/chat/completions"):
            self.send_error(404)
            return
        size = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(size) or b"{}")
        model = request.get("model", "webui-live-model")
        chunks = [
            {"id": "webui-live", "object": "chat.completion.chunk", "model": model,
             "choices": [{"index": 0, "delta": {"content": "webui live provider"}, "finish_reason": None}]},
            {"id": "webui-live", "object": "chat.completion.chunk", "model": model,
             "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
        ]
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        for chunk in chunks:
            self.wfile.write(("data: " + json.dumps(chunk) + "\n\n").encode())
            self.wfile.flush()
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
PY

mkdir -p "$WORKDIR/.cowd" "$CONFIG_HOME" "$HOME_DIR/.cowd"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "webui-live-model"
providers:
  webui_live:
    base_url: "http://127.0.0.1:$PROVIDER_PORT"
    api_key: "webui-live-provider-key"
    protocol: "completions"
    models:
      - "webui-live-model"
permissions:
  default_mode: "danger-full-access"
memory:
  enabled: false
storage:
  backend: sqlite
gateway:
  enabled: true
  webui_dir: "$SURFACE_WEBUI_DIR/dist"
  sessionReset: "none"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: $PORT
      auth:
        enabled: true
        token: "$API_TOKEN"
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKDIR/.cowd/config.yaml"

tmux new-session -d -s "$SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    (python3 '$WORKDIR/mock_provider.py' '$PROVIDER_PORT' & \
    '$BIN' gateway run) >'$LOG' 2>&1\""

for _ in {1..80}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

# Static WebUI availability is a required surface gate. Runtime readiness may
# legitimately be degraded by optional storage in this isolated browser run,
# but the real Gateway must serve the exact built release entry and manifest.
curl -fsS -H "Authorization: Bearer $API_TOKEN" "$BASE_URL/api/webui/manifest" \
  | rg -q '"kind":"cowd.webui.manifest"'
curl -fsS -H "Authorization: Bearer $API_TOKEN" "$BASE_URL/" \
  | rg -q '<title>Cowd WebUI</title>'

(
  cd "$WEBUI_ROOT"
  # The WebUI contract suite uses deterministic API fixtures for all Session,
  # Team and projection data. It runs against the freshly built browser bundle
  # on an isolated Vite port; the real Gateway entry/manifest are verified by
  # the checks above, so fixture data cannot be mistaken for backend evidence.
  env COWD_E2E_GATEWAY_URL="" \
    COWD_BACKEND_REPO="$ROOT" \
    COWD_PLAN_ROOT="$ROOT" \
    COWD_E2E_WEB_URL="$WEB_URL" \
    PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="$CHROMIUM" \
  npx playwright test --config=playwright.config.js --browser=chromium
)

echo "live WebUI workbench scenario passed"
