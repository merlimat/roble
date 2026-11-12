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

# Runs every engine over the same workload and prints them one after the other.
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
# ENGINES picks which to run; the default is all of them. roble and Pebble
# need nothing installed. encina is fetched from git. RocksDB needs a C++
# toolchain and libclang, which is why neither is a default cargo feature:
#
#   ENGINES="roble pebble" ./run.sh          # nothing to install
#   ENGINES="roble encina" ./run.sh          # adds a git fetch
#
# Everything else is passed straight through, so
#   ./run.sh --shards 8 --batches 2000 --records 500
# works as it does on any engine alone.
set -euo pipefail
cd "$(dirname "$0")"

ENGINES="${ENGINES:-roble pebble encina rocksdb}"

out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT

args=("$@")

# Pebble's own logging goes to stdout; the lines worth reading are the ones the
# benchmark prints, and the rest is Pebble narrating its compactions.
run_engine() {
  local engine="$1" dir="$2"
  shift 2
  case "$engine" in
    roble)
      cargo run --release --quiet -p roble-bench -- --dir "$dir" "$@"
      ;;
    encina|rocksdb)
      cargo run --release --quiet -p roble-bench --features "$engine" -- \
        --engine "$engine" --dir "$dir" "$@"
      ;;
    pebble)
      (cd ../harness && go run ./bench --dir "$dir" "$@" 2>&1 | grep -v '^[0-9]\{4\}/')
      ;;
    *)
      echo "run.sh: unknown engine $engine" >&2
      exit 2
      ;;
  esac
}

echo "=== every engine, every phase, one process each ==="
for engine in $ENGINES; do
  echo
  run_engine "$engine" "$out/all-$engine" "${args[@]}"
done

echo
echo "=== each scan direction on its own, over a store of its own ==="
echo "(so that neither direction reads a cache the other warmed)"
for dir in scan-rev scan; do
  for engine in $ENGINES; do
    echo
    run_engine "$engine" "$out/$dir-$engine" --phases "load,settle,$dir" "${args[@]}"
  done
done
