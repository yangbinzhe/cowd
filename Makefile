# Cowd Build System — Auto-limiting OOM protection
#
# Uses /proc/meminfo to dynamically calculate safe parallelism:
# - Build jobs: 80% RAM / 3GB per rustc (safe for linking spikes)
# - Test threads: 80% RAM / 2GB per test binary (capped at 8)
# - C compiler: CC_NUM_JOBS=2 (prevents cc crate from using all 24 cores)
#
# ⚠️  NEVER run `cargo clean` unless absolutely necessary!
#     It destroys SQLite native compilation cache → forces full rebuild (24×2GB=48GB OOM risk).

export CC_NUM_JOBS := 2

SAFE_BUILD_JOBS := $(shell awk '/MemAvailable/{j=int($$2/3000000); if(j<2)print 2; else print j}' /proc/meminfo 2>/dev/null || echo 4)
SAFE_TEST_THREADS := $(shell awk '/MemAvailable/{t=int($$2/2000000); if(t<1)print 1; else if(t>8)print 8; else print t}' /proc/meminfo 2>/dev/null || echo 2)

.PHONY: check test test-all test-config build tui-smoke release-gate clean help

help:
	@echo "Cowd Build Targets (auto-limited OOM prevention):"
	@echo "  Available RAM: $$(awk '/MemAvailable/{printf "%.0f GB", $$2/1048576}' /proc/meminfo)"
	@echo "  Build jobs: $(SAFE_BUILD_JOBS)  |  Test threads: $(SAFE_TEST_THREADS)  |  CC_NUM_JOBS: $(CC_NUM_JOBS)"
	@echo ""
	@echo "  make check       - cargo check --workspace"
	@echo "  make test        - lightweight runtime config + logging smoke tests"
	@echo "  make test-all    - cargo test --workspace"
	@echo "  make build       - cargo build --workspace"
	@echo "  make tui-smoke   - run tmux-backed TUI startup smoke test"
	@echo "  make release-gate - run core Rust/WebUI/E2E release gate"
	@echo "  make clean       - ⚠️  AVOID! Destroys native build caches"

check:
	@echo "=== cargo check --workspace (jobs=$(SAFE_BUILD_JOBS)) ==="
	cargo check --workspace -j $(SAFE_BUILD_JOBS)

test:
	@echo "=== cargo test -p runtime config ($(SAFE_TEST_THREADS) threads) ==="
	cargo test -p runtime config -- --test-threads=$(SAFE_TEST_THREADS)
	@echo "=== cargo test -p cowd-cli logging::tests ($(SAFE_TEST_THREADS) threads) ==="
	cargo test -p cowd-cli logging::tests:: -- --test-threads=$(SAFE_TEST_THREADS)

test-config:
	@echo "=== cargo test -p runtime config ($(SAFE_TEST_THREADS) threads) ==="
	cargo test -p runtime config -- --test-threads=$(SAFE_TEST_THREADS)

test-all:
	@echo "=== cargo test --workspace (jobs=$(SAFE_BUILD_JOBS), threads=$(SAFE_TEST_THREADS)) ==="
	cargo test --workspace -j $(SAFE_BUILD_JOBS) -- --test-threads=$(SAFE_TEST_THREADS)

build:
	@echo "=== cargo build --workspace (jobs=$(SAFE_BUILD_JOBS)) ==="
	cargo build --workspace -j $(SAFE_BUILD_JOBS)

tui-smoke:
	@echo "=== scripts/tui_smoke.sh ==="
	scripts/tui_smoke.sh

release-gate:
	@echo "=== scripts/release_gate.sh ==="
	scripts/release_gate.sh

clean:
	@echo "⚠️  WARNING: cargo clean destroys native build caches (SQLite etc.)"
	@echo "    This forces full recompilation which may cause OOM."
	@echo "    Use only when absolutely necessary."
	@read -p "Continue? [y/N] " yn && case $$yn in [Yy]) cargo clean;; *) echo "Cancelled";; esac
