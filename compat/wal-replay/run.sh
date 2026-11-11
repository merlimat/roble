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

# Half of the M3 gate (DESIGN.md §13): Go Pebble opens and reads what roble
# wrote.
#
# roble opens a database Pebble built, writes to its log, and says what it
# wrote. Pebble then opens the same directory. Nothing roble wrote has been
# flushed, so every key Pebble finds can only have come out of the log —
# which means Pebble replayed roble's record framing, roble's batch
# representation and roble's sequence numbers.
#
# The other half, roble reading a whole database Pebble wrote, is already
# covered by tests/golden and the probe corpora.
set -euo pipefail
cd "$(dirname "$0")"

out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT

# A database Pebble built, copied so the fixture is left alone.
# Twice over: once with the writes left in the log, once with them flushed
# into a table through a MANIFEST edit roble appended. The first tests
# Pebble replaying roble's log; the second tests Pebble reading roble's
# table and believing roble's MANIFEST.
for mode in log flushed; do
  db="$out/$mode"
  cp -R ../../tests/golden/db-simple "$db"
  chmod -R u+w "$db"

  if [ "$mode" = flushed ]; then
    cargo run --release --quiet -p wal-replay -- "$db" --flush
  else
    cargo run --release --quiet -p wal-replay -- "$db"
  fi
  echo
  (cd ../../harness && go run ./verify-wal "$db" "$mode")
  echo
done
