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
# The third run goes further: roble replaces the MANIFEST altogether, so the
# file Pebble replays is one roble wrote from nothing, naming every table at
# every level.
#
# The fourth hands Pebble a table roble merged out of others, with
# fragmented range deletions in it and a version edit that deletes as well as
# adds.
#
# The fifth hands the picker the wheel until nothing is past its allowance,
# so what Pebble opens is a database roble spread across levels by itself.
#
# The sixth is Oxia's own configuration: the WAL off, so the table and the
# MANIFEST edit are the only things that happened, and the log is retired
# rather than written to.
#
# The eighth starts from an empty directory, so every byte Pebble reads was
# written by roble: the MANIFEST, both markers, the log and the tables. Every
# other run begins from a database Pebble built.
#
# The fifth makes the opposite claim. roble opens a database and writes
# nothing, so the only thing that changed is what roble deleted on the way
# in. Pebble then has to read a directory roble has removed files from.
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
for mode in log flushed rotated compacted settled no-wal swept checkpoint created; do
  db="$out/$mode"
  # The swept run needs a database Pebble left something obsolete in, which
  # db-simple is not: Pebble closed db-unflushed-wal holding a log it had
  # already flushed past, kept back to recycle. The created run needs no
  # fixture at all — it is given a path and nothing else, which is the claim.
  if [ "$mode" = created ]; then
    :
  elif [ "$mode" = swept ]; then
    cp -R ../../tests/golden/db-unflushed-wal "$db"
  else
    cp -R ../../tests/golden/db-simple "$db"
  fi
  if [ -d "$db" ]; then
    chmod -R u+w "$db"
  fi

  case "$mode" in
    flushed) cargo run --release --quiet -p wal-replay -- "$db" --flush ;;
    rotated) cargo run --release --quiet -p wal-replay -- "$db" --rotate ;;
    compacted) cargo run --release --quiet -p wal-replay -- "$db" --compact ;;
    settled) cargo run --release --quiet -p wal-replay -- "$db" --settle ;;
    no-wal)  cargo run --release --quiet -p wal-replay -- "$db" --no-wal ;;
    swept)   cargo run --release --quiet -p wal-replay -- "$db" --swept ;;
    checkpoint) cargo run --release --quiet -p wal-replay -- "$db" --checkpoint ;;
    created) cargo run --release --quiet -p wal-replay -- "$db" --created ;;
    *)       cargo run --release --quiet -p wal-replay -- "$db" ;;
  esac
  echo
  # The checkpoint run hands Pebble the directory roble wrote *out* of the
  # database, not the database.
  verify="$db"
  if [ "$mode" = checkpoint ]; then
    verify="$out/checkpointed"
  fi
  (cd ../../harness && go run ./verify-wal "$verify" "$mode")
  echo
done

# The kinds roble reads and never writes itself: roble compacts db-reads so
# that its own output carries SINGLEDEL, DELSIZED and SETWITHDEL, and Pebble
# has to give the answers it gave over the original, then compact the result
# and give them again.
db="$out/kinds"
cp -R ../../tests/golden/db-reads "$db"
chmod -R u+w "$db"
cargo run --release --quiet -p wal-replay -- "$db" --kinds
echo
# A copy first: the SingleDelete has to meet roble's tables, not the ones
# Pebble's own compaction in the check replaces them with.
cp -R "$db" "$out/kinds-single-delete"
(cd ../../harness && go run ./read-probes check "$db" ../tests/golden)
(cd ../../harness && go run ./read-probes single-delete "$out/kinds-single-delete")
echo

# The other half: a SingleDelete roble itself compacts over the DEL it
# buries, with two older values of the key below. roble writes it as a DEL,
# and Pebble's compaction must not let it cancel one value and uncover the
# other. Created from nothing, since no fixture has two values under a DEL.
db="$out/single-delete"
cargo run --release --quiet -p wal-replay -- "$db" --single-delete
echo
(cd ../../harness && go run ./read-probes stays-deleted "$db" k)
echo

# Pebble's megabyte keys from testdata/large_keys: a block holds one or two,
# every separator and table bound is a megabyte, and the edit naming the
# tables spans dozens of the MANIFEST's blocks.
db="$out/large-keys"
cargo run --release --quiet -p wal-replay -- "$db" --large-keys
echo
(cd ../../harness && go run ./verify-wal "$db" large-keys)
