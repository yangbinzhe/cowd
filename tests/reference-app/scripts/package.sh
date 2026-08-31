#!/bin/sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$script_dir/.." && pwd)
output=${1:?usage: package.sh OUTPUT_DIRECTORY}
target_dir=${CARGO_TARGET_DIR:-"$root/target"}
case "$target_dir" in
    /*) ;;
    *) target_dir="$(pwd)/$target_dir" ;;
esac
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
    cargo build --locked --offline --release --manifest-path "$root/Cargo.toml" --bin reference-app-worker
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY -u http_proxy -u https_proxy -u all_proxy \
    cargo run --locked --offline --release --manifest-path "$root/Cargo.toml" --bin reference-app-bundle -- \
    package --worker "$target_dir/release/reference-app-worker" --output "$output"
