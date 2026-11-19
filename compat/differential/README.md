# differential

roble and Go Pebble take turns at one database, through a random history.

Every other round trip is a fixed scenario. This one generates a seeded
script of segments and hands each segment to the engine whose turn it is, on
one directory: `differential run` for roble, `harness/differential` for
Pebble. Each opens what the other left — its tables, the layouts its
compactions chose, its MANIFEST, the log it left unflushed — writes, checks
and closes.

A segment is a few hundred operations: puts over a 1000-key alphabet with
values from one byte to past a 32 KiB log block, deletes, range deletions,
flushes, full compactions, snapshots, batches, walks and checks. Both engines
run with memtables and tables small enough that flushes and compactions keep
happening, and Pebble with its own automatic compactions on, as Oxia
configures it otherwise: the format major version of the pin, a ten-bit bloom
filter, L0 uncompressed and Snappy below. Some turns are killed rather than
closed.

## The oracle

The script itself. A runner replays every segment before its own into a
`BTreeMap`, applies its own to the store and the map together, and at every
`check` reads the store both ways end to end and with `Equal`, `Ceiling`,
`Floor`, `Higher` and `Lower` at every key and either side of it (Pebble's
runner has only the seeks: its `Higher` and `Lower` are the same seeks at the
next probe). A `walk` makes an iterator over a range, usually a few hundred
keys with either side now and then open, and makes forty moves with it —
`first`, `last`, `next`, `prev`, and seeks on keys, between them and just
outside the range — and after every move requires the key the map says, so
it turns round in the middle of tables and at their edges, where a full scan
never does. A step from nothing is skipped, since the engines differ there:
Pebble turns round onto the end of the range and roble stays on nothing.

`snap` takes a snapshot, and a turn holds up to three until `unsnap` lets one
go or the turn ends. `snapcheck` and `snapwalk` read one the way `check` and
`walk` read the store, against the map as it was when the snapshot was taken,
with the writes, flushes and compactions since then underneath: newer
versions and range deletions in the same memtable, tables the snapshot's
version does not hold, and, on Pebble, compactions that keep what it can see.

`batch` opens an indexed batch, one at a time, which lives a couple of hundred
operations. While it is open, seven writes in ten go into it (`bput`, `bdel`,
`bdelrange`) and the rest go to the store beside it. `bcheck` and `bwalk` read
through it against the map with the batch's writes laid over what is
committed, since a commit numbers them above everything before; `commit`
applies them to the map, and `abort` drops them.

A disagreement names the engine that read wrongly, the segment and the
operation, and what the other engine wrote before it is the segments before.

## Kills

One turn in three, by either engine, is killed instead of closing. `run.sh`
names an operation at random, or a quarter of the time the open, and up to 3
ms; the turn starts a timer as it begins that one and, when it fires, ends
itself as a kill would (Pebble's sends itself SIGKILL, roble's aborts),
wherever it has got to. A kill turn syncs its writes, since Pebble's unsynced
ones wait in its own buffers and a kill loses them; roble's are the OS's once
written, and syncing runs its other path.

A turn prints `done <i>` after each operation, so the script can be cut where
it stopped (`differential truncate`): the operations done stay, an open batch
is aborted, and a write that was under way becomes `maybe`, since it may or
may not have landed. The next turn, the other engine's, opens what the killed
one left and decides the `maybe` by reading a key on which the two outcomes
disagree. It prints which, `differential resolve` writes that into the script,
and from there the history is one again. Every acknowledged write must be
there, and nothing else.

Over 20 seeds with every eligible turn killed (`CRASH=1`), 80 kills landed
inside roble's flushes (16) and checks (20), Pebble's opens (12), puts,
deletes and flushes, and a few elsewhere; roble's own open is too quick for
most kills aimed at it, which land in its first operations instead. Twenty
writes were under way, and both outcomes came up: 16 had landed and 4 had not.

With roble's flush changed to remove the log it replaces as soon as the new
one is in place, rather than after the edit that retires it, ten seeds pass
with `CRASH=0` and fail with `CRASH=1` (seed 7006: Pebble opens what a killed
roble flush left and finds 233 keys where the history says 234). What a kill
does not reach is a window of microseconds: taking out #204's fix, or
#203's, is not caught, the first because the bug needs a second crash before
the next flush and the second because a kill almost never tears an edit in
half. `tests/failure.rs` fails I/O at every step for those.

No two turns running are killed, so the turn after a kill always finishes and
decides, and the last turn never is. A seed kills the same turns aimed at the
same operations, but the microsecond a kill lands at does not replay, so a
failure keeps the directory and the script, cut as the turns that ran had it.
`CRASH=0` turns kills off.

## What it has found

Kills found a directory neither engine would open: roble, reopening a
directory, carried on writing into the last log where its replay stopped, and
when that file was one Pebble had recycled, the bytes of its earlier life lay
past roble's records. A flush creates a newer log before its edit retires the
old one, and a kill in between left the old log behind a newer one, where
Pebble and roble both read it strictly and took the stale bytes for corruption
(`pebble/record: unexpected EOF`). The open now cuts the log it carries on
writing to where its records end; `tests/failure.rs` crashes such a flush at
every operation.

Its first runs found roble answering `Floor` and a scan's reverse seek with a
range-deleted key, over a layout Pebble writes and roble does not: a
compaction split inside a range deletion, leaving a table that holds only the
deletion's fragment below its first point key (seed 509). And it caught the
`get` that #206 fixed, put back, in 24 of 30 seeds. Both counts are from
before walks: adding them changed the mix of operations, so a seed no longer
makes the history it made then, and seed 509 now passes with that fix
reverted (`tests/range_del.rs` keeps the layout instead).

Walks catch a scan that ignores its lower bound on a seek, or that turns from
reverse to forward onto the key it is on, in the first walk of seed 3000.
Snapshot reads catch, within the first three seeds from 3000, a memtable
range deletion or a scan's point entry seen whatever its number, and a
snapshot's `Equal` taking a version written after it. Reads through a batch
catch, within the same seeds, a batch's `Equal` that ignores its own range
deletion or orders it against its own put the wrong way round, and a scan
that does not see a batch's entries.

## Running it

```sh
./run.sh                                  # ten seeds from the current time
SEEDS=50 SEGMENTS=12 OPS=1000 ./run.sh     # more, and longer
SEEDS=1 SEED=1795046427 ./run.sh           # replay a failure
CRASH=1 ./run.sh                          # kill every turn that may be
```

Needs Go and a Rust toolchain.

## What it cannot catch

A disagreement between the engines on something neither reads back through
the API: a property roble writes and Pebble ignores, say. `compat/sst-roundtrip`
compares tables entry for entry, and the `pebble` tool's checks run there.
