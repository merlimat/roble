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
flushes, full compactions, snapshots, batches, walks and checks. Both engines run with memtables and
tables small enough that flushes and compactions keep happening, and Pebble
with its own automatic compactions on, as Oxia configures it otherwise: the
format major version of the pin, a ten-bit bloom filter, L0 uncompressed and
Snappy below.

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
A disagreement names the engine
that read wrongly, the segment and the operation, and what the other engine
wrote before it is the segments before.

## What it has found

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
```

Needs Go and a Rust toolchain.

## What it cannot catch

A disagreement between the engines on something neither reads back through
the API: a property roble writes and Pebble ignores, say. `compat/sst-roundtrip`
compares tables entry for entry, and the `pebble` tool's checks run there.
