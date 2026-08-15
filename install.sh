#!/usr/bin/env bash
# Cowd — AI coding assistant (Rust)
#
# One-click installer for Linux. Builds from source and walks through
# interactive configuration of API endpoint, key, model, and work directory.
#
# Usage:
#   ./install.sh                # debug build + interactive config
#   ./install.sh --release      # optimized release build
#   ./install.sh --no-config    # skip interactive configuration
#   ./install.sh --help
#
# Environment overrides:
#   COWD_BUILD_PROFILE=debug|release
#   COWD_SKIP_CONFIG=1

set -euo pipefail

# ── Pretty printing ──────────────────────────────────────────────────────────

if [ -t 1 ] && command -v tput >/dev/null 2>&1 && [ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]; then
    COLOR_RESET="$(tput sgr0)"
    COLOR_BOLD="$(tput bold)"
    COLOR_DIM="$(tput dim)"
    COLOR_RED="$(tput setaf 1)"
    COLOR_GREEN="$(tput setaf 2)"
    COLOR_YELLOW="$(tput setaf 3)"
    COLOR_BLUE="$(tput setaf 4)"
    COLOR_CYAN="$(tput setaf 6)"
else
    COLOR_RESET="" COLOR_BOLD="" COLOR_DIM=""
    COLOR_RED="" COLOR_GREEN="" COLOR_YELLOW="" COLOR_BLUE="" COLOR_CYAN=""
fi

CURRENT_STEP=0
TOTAL_STEPS=7

step() {
    CURRENT_STEP=$((CURRENT_STEP + 1))
    printf '\n%s[%d/%d]%s %s%s%s\n' \
        "${COLOR_BLUE}" "${CURRENT_STEP}" "${TOTAL_STEPS}" "${COLOR_RESET}" \
        "${COLOR_BOLD}" "$1" "${COLOR_RESET}"
}
info()  { printf '%s  ->%s %s\n' "${COLOR_CYAN}" "${COLOR_RESET}" "$1"; }
ok()    { printf '%s  ok%s %s\n' "${COLOR_GREEN}" "${COLOR_RESET}" "$1"; }
warn()  { printf '%s  warn%s %s\n' "${COLOR_YELLOW}" "${COLOR_RESET}" "$1"; }
error() { printf '%s  error%s %s\n' "${COLOR_RED}" "${COLOR_RESET}" "$1" 1>&2; }

print_banner() {
    printf '%s' "${COLOR_BOLD}"
    cat <<'EOF'
   ______           ____  _
  / ____/___  _____/ __ \(_)___  ____ _
 / /   / __ \/ ___/ / / / / __ \/ __ `/
/ /___/ /_/ / /  / /_/ / / / / / /_/ /
\____/\____/_/  /_____/_/_/ /_/\__, /
                               /____/
EOF
    printf '%s\n' "${COLOR_RESET}"
    printf '%sCowd — AI coding assistant (Rust)%s\n' "${COLOR_DIM}" "${COLOR_RESET}"
}

print_usage() {
    cat <<'EOF'
Usage: ./install.sh [options]

Options:
  --release       Build the optimized release profile
  --debug         Build the debug profile (default)
  --no-config     Skip interactive configuration
  -h, --help      Show this help text and exit

Environment overrides:
  COWD_BUILD_PROFILE   debug | release
  COWD_SKIP_CONFIG     set to 1 to skip configuration
EOF
}

# ── Argument parsing ─────────────────────────────────────────────────────────

BUILD_PROFILE="${COWD_BUILD_PROFILE:-debug}"
SKIP_CONFIG="${COWD_SKIP_CONFIG:-0}"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --release)    BUILD_PROFILE="release" ;;
        --debug)      BUILD_PROFILE="debug" ;;
        --no-config)  SKIP_CONFIG="1" ;;
        -h|--help)    print_usage; exit 0 ;;
        *)            error "unknown argument: $1"; print_usage; exit 2 ;;
    esac
    shift
done

case "${BUILD_PROFILE}" in
    debug|release) ;;
    *) error "invalid build profile: ${BUILD_PROFILE}"; exit 2 ;;
esac

# ── Troubleshooting hints ────────────────────────────────────────────────────

print_troubleshooting() {
    cat <<EOF

${COLOR_BOLD}Troubleshooting${COLOR_RESET}
${COLOR_DIM}---------------${COLOR_RESET}

  ${COLOR_BOLD}1. Rust toolchain missing${COLOR_RESET}
     Install Rust via rustup:
       curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
     Then reload your shell or run:
       source "\$HOME/.cargo/env"

  ${COLOR_BOLD}2. Linux: missing system packages${COLOR_RESET}
     Debian/Ubuntu:
       sudo apt-get update && sudo apt-get install -y \\
         git pkg-config libssl-dev ca-certificates build-essential
     Fedora/RHEL:
       sudo dnf install -y git pkgconf-pkg-config openssl-devel gcc
     Arch:
       sudo pacman -S --needed git pkgconf openssl base-devel

  ${COLOR_BOLD}3. Build fails partway through${COLOR_RESET}
     Try a clean build:
       cargo clean && cargo build --workspace

  ${COLOR_BOLD}4. 'cowd' not found after install${COLOR_RESET}
     The binary lives at:
       target/${BUILD_PROFILE}/cowd
     Add it to your PATH or invoke it with the full path.

EOF
}

trap 'rc=$?; if [ "$rc" -ne 0 ]; then error "installation failed (exit ${rc})"; print_troubleshooting; fi' EXIT

# ── Step 1: detect OS / arch ─────────────────────────────────────────────────

print_banner
step "Detecting host environment"

UNAME_S="$(uname -s 2>/dev/null || echo unknown)"
UNAME_M="$(uname -m 2>/dev/null || echo unknown)"

case "${UNAME_S}" in
    Linux*)  info "platform:      Linux ${UNAME_M}" ;;
    Darwin*) info "platform:      macOS ${UNAME_M}" ;;
    *)       error "Unsupported OS: ${UNAME_S}"; exit 1 ;;
esac

ok "supported platform detected"

# ── Step 2: locate the Rust workspace ────────────────────────────────────────

step "Locating the Rust workspace"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ ! -f "${SCRIPT_DIR}/Cargo.toml" ]; then
    error "Missing Cargo.toml in ${SCRIPT_DIR}"
    exit 1
fi

ok "workspace at ${SCRIPT_DIR}"

# ── Step 3: prerequisite checks ──────────────────────────────────────────────

step "Checking prerequisites"

MISSING=0

if command -v rustc >/dev/null 2>&1; then
    ok "rustc $(rustc --version 2>/dev/null)"
else
    error "rustc not found"; MISSING=1
fi

if command -v cargo >/dev/null 2>&1; then
    ok "cargo $(cargo --version 2>/dev/null)"
else
    error "cargo not found"; MISSING=1
fi

if command -v git >/dev/null 2>&1; then
    ok "git $(git --version 2>/dev/null)"
else
    warn "git not found — some features may degrade"
fi

if [ "${MISSING}" -ne 0 ]; then
    error "Missing required tools. See troubleshooting below."
    exit 1
fi

# ── Step 4: build the workspace ──────────────────────────────────────────────

step "Building the cowd workspace (${BUILD_PROFILE})"

CARGO_FLAGS=("build" "--workspace")
if [ "${BUILD_PROFILE}" = "release" ]; then
    CARGO_FLAGS+=("--release")
fi

info "running: cargo ${CARGO_FLAGS[*]}"
info "this may take a few minutes on the first build"

(
    cd "${SCRIPT_DIR}"
    CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}" cargo "${CARGO_FLAGS[@]}"
)

COWD_BIN="${SCRIPT_DIR}/target/${BUILD_PROFILE}/cowd"

if [ ! -x "${COWD_BIN}" ]; then
    error "Expected binary not found at ${COWD_BIN}"
    exit 1
fi

ok "built ${COWD_BIN}"

# ── Step 5: post-build verification ──────────────────────────────────────────

CONFIG_DIR="${HOME}/.cowd"
APPS_DIR="${CONFIG_DIR}/apps"

step "Verifying the installed binary"

if "${COWD_BIN}" --help >/dev/null 2>&1; then
    ok "cowd --help responded"
else
    warn "cowd --help returned an error — check build output"
fi

info "WebUI assets are external; set gateway.webui_dir in ${CONFIG_DIR}/config.yaml to serve a built cowd-webui dist."

# ── Step 6: interactive configuration ────────────────────────────────────────

step "Interactive configuration"

CONFIG_FILE="${CONFIG_DIR}/config.yaml"

if [ "${SKIP_CONFIG}" = "1" ]; then
    info "configuration skipped (--no-config / COWD_SKIP_CONFIG=1)"
else
    mkdir -p "${CONFIG_DIR}" "${APPS_DIR}"

    # --- API URL ---
    printf '\n%s  API base URL (OpenAI-compatible endpoint)%s\n' "${COLOR_BOLD}" "${COLOR_RESET}"
    printf '  Default: https://api.openai.com/v1\n'
    printf '  > '
    read -r API_URL
    API_URL="${API_URL:-https://api.openai.com/v1}"

    # --- API Key ---
    printf '\n%s  API key (Bearer token)%s\n' "${COLOR_BOLD}" "${COLOR_RESET}"
    printf '  > '
    read -r API_KEY
    API_KEY="${API_KEY:-}"

    # --- Model ---
    printf '\n%s  Default model name%s\n' "${COLOR_BOLD}" "${COLOR_RESET}"
    printf '  Default: gpt-4o\n'
    printf '  > '
    read -r MODEL_NAME
    MODEL_NAME="${MODEL_NAME:-gpt-4o}"

    # --- Work directory ---
    printf '\n%s  Default working directory%s\n' "${COLOR_BOLD}" "${COLOR_RESET}"
    printf '  Default: %s\n' "$(pwd)"
    printf '  > '
    read -r WORK_DIR
    WORK_DIR="${WORK_DIR:-$(pwd)}"

    # --- Write config (idempotent) ---
    if [ -f "${CONFIG_FILE}" ]; then
        info "config file ${CONFIG_FILE} already exists — skipping overwrite"
    else
        cat > "${CONFIG_FILE}" <<YAML
# Cowd configuration — generated by install.sh
# Edit manually or re-run ./install.sh

runtime:
  model: "${MODEL_NAME}"

providers:
  providers:
    default:
      base_url: "${API_URL}"
      api_key: "${API_KEY}"
      models:
        - "${MODEL_NAME}"

gateway:
  enabled: true
  platforms: []

# Signed APP Bundles placed here are discovered and mounted at Gateway startup.
apps:
  directories:
    - "${APPS_DIR}"

memory:
  enabled: true
YAML
        ok "configuration written to ${CONFIG_FILE}"
    fi

    # --- Shell profile ---
    SHELL_RC="${HOME}/.bashrc"
    if [ -f "${HOME}/.zshrc" ] && [ -z "${ZSH_VERSION+x}" ]; then
        SHELL_RC="${HOME}/.zshrc"
    fi
    if [ -n "${ZSH_VERSION+x}" ]; then
        SHELL_RC="${HOME}/.zshrc"
    fi

    COWD_BIN_DIR="$(dirname "${COWD_BIN}")"

    if ! grep -q 'cowd' "${SHELL_RC}" 2>/dev/null; then
        printf '\n# Cowd\nexport PATH="%s:$PATH"\n' "${COWD_BIN_DIR}" >> "${SHELL_RC}"
        info "added ${COWD_BIN_DIR} to PATH in ${SHELL_RC}"
    fi

    # Export env vars for current session
    export COWD_CONFIG_HOME="${CONFIG_DIR}"
    export PATH="${COWD_BIN_DIR}:${PATH}"
fi

# ── Step 7: next steps ───────────────────────────────────────────────────────

step "Next steps"

cat <<EOF
${COLOR_GREEN}Cowd is built and ready.${COLOR_RESET}

  Binary:  ${COLOR_BOLD}${COWD_BIN}${COLOR_RESET}
  Profile: ${BUILD_PROFILE}
  Config:  ${CONFIG_FILE:-not configured}

Try it out:

  ${COLOR_DIM}# start the Gateway and TUI${COLOR_RESET}
  ${COWD_BIN} gateway run
  ${COWD_BIN} tui

  ${COLOR_DIM}# inspect mounted signed APP Bundles${COLOR_RESET}
  ${COWD_BIN} apps list
  ${COWD_BIN} apps doctor

Environment variables:

  COWD_CONFIG_HOME     Config directory (default: ~/.cowd)
  COWD_MODEL           Override default model
  COWD_PERMISSION_MODE Permission mode (default/workspace-write/danger-full-access)

  source ${SHELL_RC:-~/.bashrc}   # reload PATH
EOF

trap - EXIT
