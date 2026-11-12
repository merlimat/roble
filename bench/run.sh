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

# Runs both engines over the same workload and prints them one after the other.
# See README.md.
#
# Two passes, because they answer different questions:
#
#   1. The whole phase list in one process per engine. Cheap, and the phases
#      share a warm cache in the order they run — which is why `scan-rev` goes
#      before `scan`, so the bias runs against the direction under suspicion.
#   2. Each scan direction in a process of its own over a freshly loaded store,
#      so neither reads a cache the other warmed. This is the comparison
#      DESIGN.md §14 asks for, and the first pass only approximates it.
#
# Everything else is passed straight through, so
#   ./run.sh --shards 8 --batches 2000 --records 500
# works as it does on either engine alone.
set -euo pipefail
cd "$(dirname "$0")"

out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT

args=("$@")

roble() { cargo run --release --quiet -p roble-bench -- "$@"; }
# Pebble's own logging goes to stdout; the lines worth reading are the ones
# the benchmark prints, and the rest is Pebble narrating its compactions.
pebble() { (cd ../harness && go run ./bench "$@" 2>&1 | grep -v '^[0-9]\{4\}/'); }

echo "=== both engines, every phase, one process each ==="
echo
roble  --dir "$out/all-roble"  "${args[@]}"
echo
pebble --dir "$out/all-pebble" "${args[@]}"

echo
echo "=== each scan direction on its own, over a store of its own ==="
echo "(so that neither direction reads a block cache the other warmed)"
for dir in scan-rev scan; do
  echo
  roble  --dir "$out/$dir-roble"  --phases "load,settle,$dir" "${args[@]}"
  pebble --dir "$out/$dir-pebble" --phases "load,settle,$dir" "${args[@]}"
done
