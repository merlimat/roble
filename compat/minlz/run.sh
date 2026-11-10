#!/usr/bin/env bash
# Checks that the Rust `minlz` crate decodes MinLZ blocks produced by Go's
# github.com/minio/minlz, which is what Pebble compresses blocks with when a
# compression profile selects MinLZ — as Oxia's key-encoding conversion does for
# index, filter and metadata blocks on every architecture but arm64
# (DESIGN.md §1, §3).
#
# Fixtures are regenerated on every run rather than committed: Go is the source
# of truth for them, and they come to ~10 MiB.
set -euo pipefail
cd "$(dirname "$0")"

fixtures="$(mktemp -d)"
trap 'rm -rf "$fixtures"' EXIT

(cd ../../harness && go run ./minlz-fixtures "$fixtures")
echo
cargo run --release --quiet -- "$fixtures"
