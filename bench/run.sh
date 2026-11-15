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

# Runs every engine over the same workloads in one standard environment, and
# prints a table per scenario. See README.md, "One core, 512 MiB".
#
# **One environment.** Every engine's process is held to one CPU and 512 MiB
# of memory, page cache included and swap excluded: one shard of a deployment
# that runs a store per core. roble uses one core by design, compaction
# included, so a machine-wide run is roble's one core against RocksDB's
# background threads; and an uncapped run lets the page cache hold any store
# the machine has room for, so nothing is ever read from a device.
#
# **Two scenarios** inside it, differing only in how much data there is:
#
#   in-cache        1,400,000 records, about 200 MiB on disk. It fits the
#                   256 MiB block cache, so once the scans have warmed it the
#                   reads never leave the process.
#   beyond-memory   6,000,000 records, about 850 MiB. More than the 512 MiB the
#                   process may hold, cache and page cache together, so the
#                   reads that miss the block cache go to the device.
#
# Each scenario prints two tables, every phase and YCSB A to F, with each
# engine's run in a process of its own and each YCSB mix over a store loaded
# and settled for it alone. That is all this runs: the passes that compared
# one setting against another — six cores, an uncapped cache, a separate
# memory budget, the WAL, the batch size, each scan direction alone — are
# gone, and the binary still does any of them run by hand.
#
#   ENGINES   which engines (roble rocksdb encina). RocksDB needs a C++
#             toolchain and libclang; encina is fetched from git.
#   CPU       the core to pin to (3). Not 0, which on a VM usually carries
#             the network and console interrupts on top of a block queue.
#   MEM_MB    the memory cap (512), and CACHE_MB the block cache (256).
#   BENCH_DIR where the stores go (a temporary directory). Never a tmpfs:
#             its pages count against the cap, and a store there is memory.
#   KEEP=1    leaves the stores on disk.
#
# Anything else is passed to every run after the standard arguments, so it
# wins over them: `./run.sh --min-seconds 2` for a quick look.
#
# The cap needs Linux, cgroup v2 with the memory controller, and either a
# systemd user session with it delegated or passwordless sudo. Without them
# this refuses rather than print numbers that look standard and are not;
# `STANDARD=0` runs uncapped and unpinned anyway, for a smoke test, and says
# so above every table.
set -euo pipefail
cd "$(dirname "$0")"

ENGINES="${ENGINES:-roble rocksdb encina}"
CPU="${CPU:-3}"
MEM_MB="${MEM_MB:-512}"
CACHE_MB="${CACHE_MB:-256}"

out="${BENCH_DIR:-$(mktemp -d)}"
mkdir -p "$out"
if [ -n "${KEEP:-}" ]; then
  trap 'echo; echo "the stores are under $out"' EXIT
else
  trap 'rm -rf "$out"' EXIT
fi
if [ "$(uname -s)" = Linux ] && [ "$(stat -f -c %T "$out")" = tmpfs ]; then
  echo "run.sh: $out is a tmpfs. Its pages count against the ${MEM_MB} MiB cap, so" >&2
  echo "a store there is memory, not disk; set BENCH_DIR to a disk-backed path." >&2
  exit 2
fi
echo "writing the stores under $out${KEEP:+ (kept)}"

# **The cap, verified rather than assumed.** A `-p MemoryMax=` that systemd
# accepts and an undelegated controller never applies would give numbers that
# look capped and are not, so each way of applying it is tried and the scope
# is asked what limits it actually holds — the cap, no swap, one CPU.
#
# A user scope first, which needs no root. Then a system scope through sudo,
# for a CI runner without a user session, dropping back to this user inside
# it so the stores are not root's.
limit=()
try_limit() {
  local got
  # Single-quoted: the reads run inside the scope, where the answer is.
  # shellcheck disable=SC2016
  got=$("$@" sh -c 'g=/sys/fs/cgroup$(cut -d: -f3 < /proc/self/cgroup);
                    echo "$(cat $g/memory.max) $(cat $g/memory.swap.max) $(nproc)"' \
          2>/dev/null) || return 1
  [ "$got" = "$((MEM_MB * 1024 * 1024)) 0 1" ]
}
if [ "$(uname -s)" = Linux ] && command -v systemd-run > /dev/null && command -v taskset > /dev/null; then
  user_scope=(systemd-run --user --scope --quiet
              -p MemoryMax="${MEM_MB}M" -p MemorySwapMax=0 -- taskset -c "$CPU")
  sudo_scope=(sudo --non-interactive systemd-run --scope --quiet
              -p MemoryMax="${MEM_MB}M" -p MemorySwapMax=0 --
              sudo --non-interactive -u "$(id -un)" env PATH="$PATH" taskset -c "$CPU")
  if try_limit "${user_scope[@]}"; then
    limit=("${user_scope[@]}")
  elif try_limit "${sudo_scope[@]}"; then
    limit=("${sudo_scope[@]}")
  fi
fi
if [ ${#limit[@]} -eq 0 ]; then
  if [ "${STANDARD:-1}" != 0 ]; then
    echo "run.sh: cannot hold a process to one CPU and ${MEM_MB} MiB here (see the" >&2
    echo "header); STANDARD=0 runs uncapped and unpinned, as a smoke test." >&2
    exit 2
  fi
  banner="NOT THE STANDARD: uncapped and unpinned (STANDARD=0)"
else
  banner="one CPU (cpu $CPU), ${MEM_MB} MiB, no swap, ${CACHE_MB} MiB block cache"
fi

# Each engine built once, before anything is capped, and copied aside: the
# feature sets share one output path, and a build is neither part of the
# measurement nor something to fit in 512 MiB.
bin="$out/bin"
mkdir -p "$bin"
target_dir=$(cargo metadata --format-version 1 --no-deps \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
for engine in $ENGINES; do
  case "$engine" in
    roble) cargo build --release --quiet -p roble-bench ;;
    encina|rocksdb) cargo build --release --quiet -p roble-bench --features "$engine" ;;
    *) echo "run.sh: unknown engine $engine" >&2; exit 2 ;;
  esac
  cp "$target_dir/release/roble-bench" "$bin/roble-bench-$engine"
done

# One run of one engine, inside the cap, keeping only its tab-separated lines.
#
# A run the cap kills is a result, not a reason to stop: it comes out as a
# skipped row, which `pivot.py` prints under the table, and the next engine
# runs. `${limit[@]+...}` because bash 3.2, macOS's, calls an empty array
# unbound under `set -u`, and empty is what `STANDARD=0` makes it.
run() {
  local engine="$1" dir="$2"
  shift 2
  local status=0
  ${limit[@]+"${limit[@]}"} "$bin/roble-bench-$engine" --engine "$engine" --dir "$dir" \
    --tsv "$@" > "$dir.tsv" || status=$?
  awk -F'\t' 'NF >= 3' "$dir.tsv"
  if [ "$status" -ne 0 ]; then
    local why="exited with $status"
    if [ ${#limit[@]} -gt 0 ]; then
      why="$why inside the ${MEM_MB} MiB cap"
      [ "$status" -ne 137 ] || why="killed inside the ${MEM_MB} MiB cap: out of memory"
    fi
    printf '%s\trun\tskipped\t%s\n' "$engine" "$why"
  fi
  # The lines are all that is read afterwards, so the store goes now rather
  # than with the others at exit: a scenario holds one store at a time.
  [ -n "${KEEP:-}" ] || rm -rf "$dir" "$dir.tsv"
}

# The phases with a question of their own here. `get-cold` reopened the store
# to read with an empty cache, which the second scenario now asks of every
# phase; and the default run's own YCSB mix is in the table that has all six.
PHASES=load,settle,scan-rev,scan,get,floor,scan-short,checkpoint

scenario() {
  local name="$1" batches="$2"
  shift 2
  local common=(--batches "$batches" --records 10000 --reads 1000
                --cache-mb "$CACHE_MB" --no-wal "${@}")
  # The standard size, or what was passed instead: an override is printed as
  # given rather than a count it may have changed.
  local what="$((batches * 10000)) records"
  [ $# -eq 0 ] || what="with $*"

  echo
  echo "=== $name: every phase, $what — $banner ==="
  echo
  local lines="$out/$name-phases.tsv"
  : > "$lines"
  for engine in $ENGINES; do
    run "$engine" "$out/$name-$engine" --phases "$PHASES" "${common[@]}" >> "$lines"
  done
  python3 pivot.py --metrics rate,p50,cpu,size < "$lines"

  echo
  echo "=== $name: YCSB A to F, $what — $banner ==="
  echo
  lines="$out/$name-ycsb.tsv"
  : > "$lines"
  for engine in $ENGINES; do
    for w in A B C D E F; do
      # Only the mix's own row: each run loads and settles a store of its
      # own, and six load rows an engine would say nothing the table above
      # does not.
      run "$engine" "$out/$name-ycsb-$w-$engine" \
        --phases load,settle,ycsb --workload "$w" "${common[@]}" \
        | awk -F'\t' '$2 ~ /^ycsb-/ || $3 == "skipped"' >> "$lines"
    done
  done
  python3 pivot.py --metrics rate,p50,p99 < "$lines"
}

scenario in-cache 140 "$@"
scenario beyond-memory 600 "$@"
