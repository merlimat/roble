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

# The M1 gate (DESIGN.md §13): Go Pebble opens and reads what roble wrote.
#
# roble writes a set of tables and dumps each one; Go Pebble opens the same
# files and dumps them in the same canonical format; the two dumps must be
# identical. Tables are regenerated each run rather than committed — roble is
# the source of truth for them, unlike the golden corpus in format/tests.
set -euo pipefail
cd "$(dirname "$0")"

out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT

cargo run --release --quiet -p sst-roundtrip -- "$out"
echo
(cd ../../harness && go run ./verify-sst "$out")
echo

failed=0
for dump in "$out"/*.dump; do
  name="$(basename "$dump" .dump)"
  if ! diff -u "$dump" "$out/$name.godump" > "$out/$name.diff"; then
    echo "$name: FAILED — roble's dump and Go Pebble's differ:"
    head -20 "$out/$name.diff" | sed 's/^/    /'
    failed=$((failed + 1))
  else
    echo "$name: identical"
  fi
done

if [ "$failed" -gt 0 ]; then
  echo
  echo "$failed fixture(s) differ"
  exit 1
fi
echo
echo "VERDICT: Go Pebble reads every table roble wrote, entry for entry"
