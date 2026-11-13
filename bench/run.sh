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
# ENGINES picks which to run; the default is all of them. roble needs nothing
# installed. encina is fetched from git. RocksDB needs a C++ toolchain and
# libclang, which is why neither is a default cargo feature:
#
#   ENGINES="roble" ./run.sh                 # nothing to install
#   ENGINES="roble encina" ./run.sh          # adds a git fetch
#
# Go Pebble is **not** here any more. It had a driver of its own under
# `harness/bench`, written to this harness's phases and key shape; that is
# gone, and `cmd/pebble`'s own benchmarks do not replace it, because they
# build their own keys. See the README.
#
# KEEP=1 leaves the stores on disk instead of removing them, and BENCH_DIR
# puts them somewhere chosen rather than in a temporary directory — for
# looking at what a run actually wrote:
#
#   KEEP=1 BENCH_DIR=/tmp/bench ./run.sh --phases load,settle
#
# --cache-mb is passed through, so a single engine can be swept across cache
# sizes directly; run.sh runs the reads at two of them (see below).
#   cargo run -p roble-cli -- manifest dump /tmp/bench/all-roble/roble/shard-0000
#
# Everything else is passed straight through, so
#   ./run.sh --shards 8 --batches 2000 --records 500
# works as it does on any engine alone.
set -euo pipefail
cd "$(dirname "$0")"

ENGINES="${ENGINES:-roble rocksdb encina}"

# Where every store of every pass is written. Printed rather than hidden,
# because the thing a run leaves behind is often what you wanted to look at:
# `KEEP=1` leaves it for `ls`, `roble sst dump` and `roble manifest dump`.
#
# One directory per pass per engine under it, named `<pass>-<engine>/<engine>`,
# since each engine makes its store under the `--dir` it is given.
out="${BENCH_DIR:-$(mktemp -d)}"
mkdir -p "$out"
if [ -n "${KEEP:-}" ]; then
  trap 'echo; echo "the stores are under $out"' EXIT
else
  trap 'rm -rf "$out"' EXIT
fi
echo "writing the stores under $out${KEEP:+ (kept)}"

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
    *)
      echo "run.sh: unknown engine $engine" >&2
      exit 2
      ;;
  esac
}

# Collects every engine's `--tsv` lines and pivots them into one table.
#
# The stores go as soon as the table is printed, because otherwise this
# script's disk footprint is every pass at once rather than one pass: nine
# tables times three engines is twenty-seven stores held to the exit trap for
# no reason, and at the dataset size the README asks for -- upwards of half a
# million records a shard, so the load row is not measuring a skiplist -- that
# is tens of gigabytes and the run dies partway with ENOSPC. `KEEP=1` still
# keeps everything; it is the flag that says the stores are the point.
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
  if [ -z "${KEEP:-}" ]; then
    for engine in $ENGINES; do
      rm -rf "$out/$tag-$engine"
    done
  fi
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

# The same records, written in the configured batch against one a tenth the
# size. The load row's rate is records per second either way, so it is
# directly comparable, and the p50 is one commit of each size.
#
# **Both sides batch.** This used to write the small side one record per
# commit, which separated per-commit cost from per-record cost cleanly and was
# untenable at any real dataset size: a copy-on-write B+tree writes a new root
# per commit, so encina spent over an hour on six million of them and 2.2 GiB
# of disk, to measure a ratio two batch sizes give just as well. A hundredfold
# spread between them says what a single insert said.
#
# The totals are matched by moving the records into the batch count, which
# needs the two numbers this was asked for; without them it is the defaults.
batches=200
records=10000
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[$i]}" in
    --batches) batches="${args[$((i + 1))]}" ;;
    --records) records="${args[$((i + 1))]}" ;;
  esac
done
total=$((batches * records))

small=$((records / 10))
[ "$small" -gt 0 ] || small=1
small_batches=$((total / small))

echo
echo "=== a commit of $records records against one of $small, $total records either way ==="
echo
echo "-- batches of $records --"
table batch "${args[@]}" --phases load --batches "$batches" --records "$records"
echo
echo "-- batches of $small --"
table smallbatch "${args[@]}" --phases load --batches "$small_batches" --records "$small"

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

# The same reads with the data inside the cache and far outside it, which is
# how encina's own benchmark frames it. The sizes are picked from the data:
# the store is about 8.4 MiB a shard at the defaults, so 64 MiB holds every
# shard and 4 MiB holds an eighth of one.
#
# What this does and does not reach: roble, Pebble and RocksDB all read
# through the operating system's page cache (§8 — direct I/O is the goal and
# not the state), so the small-cache rows measure the block-cache *miss* path
# — finding, parsing and copying a block again — and not a trip to a disk. A
# genuinely disk-bound comparison needs a working set larger than RAM, which
# is a different run and a much longer one.
echo
echo "=== the reads with the data in cache, and with it out ==="
echo
echo "-- 64 MiB cache: every shard fits --"
table cache-in "${args[@]}" --phases load,settle,scan-rev,scan,get,floor,get-cold --cache-mb 64
echo
echo "-- 4 MiB cache: an eighth of one shard fits --"
table cache-out "${args[@]}" --phases load,settle,scan-rev,scan,get,floor,get-cold --cache-mb 4

# The one pass that reaches a disk. Everything above it keeps the store in the
# page cache, so a small block cache only measures the miss path in RAM — as
# the encina row in the pass above shows, where bypassing the page cache by
# design cost it two hundred times rather than the few times a cache miss
# should.
#
# So: load in one process, drop the page cache, read in another. Dropping it
# needs root, which the benchmark does not have and should not ask for, hence
# the split and the `--reuse` flag. Without a working `drop_caches` the pass
# says so and is skipped rather than reporting numbers that look like disk
# reads and are not.
drop_caches() {
  [ "$(uname -s)" = Linux ] || return 1
  sync
  sudo --non-interactive sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null
}

echo
if drop_caches; then
  echo "=== the reads from a disk: page cache dropped after the load ==="
  echo
  lines="$out/cold-disk.tsv"
  : > "$lines"
  for engine in $ENGINES; do
    run_engine "$engine" "$out/disk-$engine" "${args[@]}" \
      --phases load,settle --cache-mb 4 > /dev/null
    drop_caches || true
    run_engine "$engine" "$out/disk-$engine" "${args[@]}" --reuse --tsv \
      --phases scan,get,floor --cache-mb 4 \
      | awk -F'\t' 'NF >= 3' >> "$lines"
  done
  python3 pivot.py --metrics rate,p50,cpu < "$lines"
else
  echo "=== the reads from a disk: skipped ==="
  echo "(needs Linux and a sudo that can write /proc/sys/vm/drop_caches;"
  echo " without it a small cache only measures the miss path in RAM)"
fi

echo
echo "=== the layout each engine settled into ==="
for engine in $ENGINES; do
  echo
  run_engine "$engine" "$out/layout-$engine" "${args[@]}" \
    --phases load,settle --layout
done
