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

# Runs every engine over the same workload and prints one table of all of them.
# See README.md.
#
# Four passes, because they answer different questions:
#
#   1. Every phase, every engine, in one table. The phases share a cache and
#      run in order, so whichever of the two scans goes second reads one the
#      first warmed — which is why `scan-rev` goes first, pointing the bias
#      against the direction under suspicion.
#   2. Each scan direction in a process of its own over a freshly loaded store,
#      so neither reads a cache the other warmed. This is the comparison
#      DESIGN.md §14 asks for, and the first pass only approximates it.
#   3. Batch insert against single insert, at the same total number of
#      records, so the per-commit cost can be told from the per-record cost.
#   4. The layout: every table of every level, with its size and key range,
#      which is what a scan number has to be read against.
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

ENGINES="${ENGINES:-roble pebble rocksdb encina}"

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

# Collects every engine's `--tsv` lines and pivots them into one table.
table() {
  local tag="$1"
  shift
  local lines="$out/$tag.tsv"
  : > "$lines"
  for engine in $ENGINES; do
    # Only the tab-separated lines: a build wrapper or a toolchain can put
    # its own chatter on stdout, and the pivot should see measurements only.
    run_engine "$engine" "$out/$tag-$engine" --tsv "$@" \
      | awk -F'\t' 'NF >= 3' >> "$lines"
  done
  python3 pivot.py --metrics rate,p50,cpu,size < "$lines"
}

echo "=== every engine, every phase ==="
echo
table all "${args[@]}"

echo
echo "=== each scan direction on its own, over a store of its own ==="
echo "(so neither direction reads a cache the other warmed)"
for dir in scan-rev scan; do
  echo
  table "$dir" "${args[@]}" --phases "load,settle,$dir"
done

# The same records, written one batch at a time against one record at a time.
# `--records 1` makes every commit its own record, so the load row's rate — it
# is records per second either way — is directly comparable, and the p50 is a
# batch commit in one and a single insert in the other.
#
# The totals are matched by moving the records into the batch count, which
# needs the two numbers this was asked for; without them it is the defaults.
batches=200
records=100
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[$i]}" in
    --batches) batches="${args[$((i + 1))]}" ;;
    --records) records="${args[$((i + 1))]}" ;;
  esac
done
total=$((batches * records))

echo
echo "=== batch insert against single insert, $total records either way ==="
echo
echo "-- batches of $records --"
table batch "${args[@]}" --phases load --batches "$batches" --records "$records"
echo
echo "-- one record per commit --"
table single "${args[@]}" --phases load --batches "$total" --records 1

# The same insert, with the journal and without it. Within one engine this is
# an honest comparison even on macOS, because it is that engine's own sync
# both times; across engines the WAL-on row is not, since Go syncs with
# F_FULLFSYNC there and compio and RocksDB with fsync (README.md).
#
# `--no-wal` is dropped from the passed-through arguments for the first row,
# since there is no flag that turns the journal back on. encina appears in
# both rows with the same number: it commits durably either way and has no
# flag to give it.
without_no_wal=()
for ((i = 0; i < ${#args[@]}; i++)); do
  [ "${args[$i]}" = "--no-wal" ] || without_no_wal+=("${args[$i]}")
done

echo
echo "=== insert with the journal against without it ==="
echo
echo "-- journal on: every commit synced --"
table journal-on "${without_no_wal[@]}" --phases load
echo
echo "-- journal off: a flush is the only durability point, as Oxia runs it --"
table journal-off "${without_no_wal[@]}" --phases load --no-wal

echo
echo "=== the layout each engine settled into ==="
for engine in $ENGINES; do
  echo
  run_engine "$engine" "$out/layout-$engine" "${args[@]}" \
    --phases load,settle --layout
done
