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
flushes, full compactions, walks and checks. Both engines run with memtables and
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
Pebble turns round onto the end of the range and roble stays on nothing. A
disagreement names the engine
that read wrongly, the segment and the operation, and what the other engine
wrote before it is the segments before.

## What it has found

Its first runs found roble answering `Floor` and a scan's reverse seek with a
range-deleted key, over a layout Pebble writes and roble does not: a
compaction split inside a range deletion, leaving a table that holds only the
deletion's fragment below its first point key (seed 509). And it catches the
`get` that #206 fixed, put back, in 24 of 30 seeds.

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
