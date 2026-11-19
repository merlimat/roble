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

# One random history, run through roble and Go Pebble in turn on one
# directory (src/main.rs says how). For each seed, a script of segments is
# generated and each segment is handed to the engine whose turn it is; the
# engine that starts alternates by seed, so each creates databases the other
# opens.
#
# One turn in CRASH (3 by default; 0 for none) is killed with SIGKILL at a
# random moment, and the script cut where it stopped, so that the next turn
# opens what a killed engine left. Never two turns running, and never the
# last, so that the turn after a kill always runs to the end. The killed
# turn kills itself, on a timer it starts as it begins an operation run.sh
# picks, or the open (src/main.rs).
#
#   SEEDS     how many seeds, 10 by default
#   SEED      the first seed, the current time by default; a failure prints
#             the one to replay it with: SEEDS=1 SEED=<it> ./run.sh
#   SEGMENTS  turns per history, 8 by default
#   OPS       operations per turn, 400 by default
#   CRASH     one turn in this many killed, 3 by default
#   DIFFERENTIAL_OUT  where the work goes, a temporary directory by default
#
# A seed kills the same turns at the same operations, but not at the same
# microsecond, so a failure also keeps the directory and the script, cut as
# the turns that ran had it.
set -euo pipefail
cd "$(dirname "$0")"

seeds="${SEEDS:-10}"
first="${SEED:-$(date +%s)}"
segments="${SEGMENTS:-8}"
ops="${OPS:-400}"
crash="${CRASH:-3}"
all_kills=0

# DIFFERENTIAL_OUT puts the work in a directory of the caller's, which CI
# uploads when a run fails.
out="${DIFFERENTIAL_OUT:-$(mktemp -d)}"
mkdir -p "$out"
trap 'rm -rf "$out"' EXIT

fail() {
  echo "$1"
  trap - EXIT
  echo "the directory and the script are kept in $out"
  exit 1
}

cargo build --release --quiet -p differential
roble="$(cd ../.. && pwd)/target/release/differential"
(cd ../../harness && go build -o "$out/pebble-differential" ./differential)
pebble="$out/pebble-differential"

for ((s = first; s < first + seeds; s++)); do
  script="$out/script-$s"
  db="$out/db-$s"
  "$roble" gen "$s" "$segments" "$ops" > "$script"
  # The seed picks the turns killed and where, so a seed kills the same
  # turns at the same operations again; the microsecond a kill lands at, and
  # so what it interrupts, does not replay.
  RANDOM=$s
  killed=-2
  kills=0
  for ((i = 0; i < segments; i++)); do
    if (( (i + s) % 2 == 0 )); then engine=roble; else engine=pebble; fi
    if [ "$engine" = roble ]; then cmd=("$roble" run "$db" "$script" "$i"); else cmd=("$pebble" "$db" "$script" "$i"); fi
    progress="$out/progress"
    failed="seed $s failed in segment $i ($engine)"
    if (( crash > 0 )) && (( i < segments - 1 && killed != i - 1 && RANDOM % crash == 0 )); then
      # A quarter of the kills aim at the open, the rest at an operation
      # picked at random; the turn kills itself that many microseconds after
      # it begins it, wherever it has got to by then. Synced, so that what
      # a turn printed as done is what it wrote.
      if (( RANDOM % 4 == 0 )); then target=open; else target=$((RANDOM % (ops + 1))); fi
      "${cmd[@]}" kill "$target" $((RANDOM % 3000)) > "$progress" 2> "$out/stderr" &
      pid=$!
      rc=0
      wait "$pid" || rc=$?
      # 137 is SIGKILL, which Pebble's turn sends itself; 134 is the abort
      # roble's turn ends itself with.
      if (( rc == 137 || rc == 134 )); then
        done_ops=$(grep -c '^done' "$progress" || true)
        if grep -q '^opened' "$progress"; then
          inside=$(awk -v n=$((i + 1)) -v d="$done_ops" '/^segment$/ { c++; next } c == n { if (j++ == d) print }' "$script")
        else
          inside="open"
        fi
        "$roble" truncate "$script" "$i" "$done_ops"
        killed=$i
        kills=$((kills + 1))
        echo "  segment $i ($engine) killed after $done_ops operations, inside: ${inside:-the close}"
      elif (( rc != 0 )); then
        cat "$out/stderr"
        fail "$failed"
      fi
    else
      if (( kills == 0 )); then failed="$failed; replay with SEEDS=1 SEED=$s"; fi
      "${cmd[@]}" > "$progress" || fail "$failed"
      if (( killed == i - 1 )); then
        how=$(sed -n 's/^resolved //p' "$progress")
        if [ -n "$how" ]; then
          "$roble" resolve "$script" "$killed" "$how"
          echo "  segment $i ($engine) found the write under way $how"
        fi
      fi
    fi
  done
  echo "seed $s: $segments turns of $ops operations, $kills of them killed, roble and Pebble agreeing with the history at every check"
  rm -rf "$db"
  all_kills=$((all_kills + kills))
done

echo
echo "VERDICT: roble and Go Pebble take turns at one database through $seeds random histories, $all_kills turns killed, and each reads what the other wrote or left"
