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
#   SEEDS     how many seeds, 10 by default
#   SEED      the first seed, the current time by default; a failure prints
#             the one to replay it with: SEEDS=1 SEED=<it> ./run.sh
#   SEGMENTS  turns per history, 8 by default
#   OPS       operations per turn, 400 by default
set -euo pipefail
cd "$(dirname "$0")"

seeds="${SEEDS:-10}"
first="${SEED:-$(date +%s)}"
segments="${SEGMENTS:-8}"
ops="${OPS:-400}"

out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT

cargo build --release --quiet -p differential
roble="$(cd ../.. && pwd)/target/release/differential"
(cd ../../harness && go build -o "$out/pebble-differential" ./differential)
pebble="$out/pebble-differential"

for ((s = first; s < first + seeds; s++)); do
  script="$out/script-$s"
  db="$out/db-$s"
  "$roble" gen "$s" "$segments" "$ops" > "$script"
  for ((i = 0; i < segments; i++)); do
    if (( (i + s) % 2 == 0 )); then engine=roble; else engine=pebble; fi
    if [ "$engine" = roble ]; then
      "$roble" run "$db" "$script" "$i" || { echo "seed $s failed in segment $i (roble); replay with SEEDS=1 SEED=$s"; exit 1; }
    else
      "$pebble" "$db" "$script" "$i" || { echo "seed $s failed in segment $i (pebble); replay with SEEDS=1 SEED=$s"; exit 1; }
    fi
  done
  echo "seed $s: $segments turns of $ops operations, roble and Pebble agreeing with the history at every check"
  rm -rf "$db"
done

echo
echo "VERDICT: roble and Go Pebble take turns at one database through $seeds random histories, and each reads what the other wrote"
