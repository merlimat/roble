#!/usr/bin/env bash

# Copyright 2026 Matteo Merli <matteo.merli@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
