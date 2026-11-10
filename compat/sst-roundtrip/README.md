# sstable round trip

The M1 gate from DESIGN.md §13: **Go Pebble opens and reads what roble wrote.**

`cargo run -p sst-roundtrip -- <dir>` writes a set of sstables with
`roble-format`'s writer and dumps each one in the canonical format of
`roble_format::dump_table`. `harness/verify-sst` then opens the same files with
Go Pebble — the default bytewise comparer, no merger, no filter policy, the way
Oxia opens them — and dumps them in that same format. The two dumps must be
identical.

## Running it

```sh
./run.sh
```

Needs Go and a Rust toolchain. Tables are regenerated into a temporary
directory each run: roble is the source of truth for them, unlike the golden
corpus in `format/tests/golden`, which is committed because reproducing it
needs a pinned Go Pebble.

## What the fixtures are for

- **simple** — one data block, small enough to read in a hex dump.
- **kinds** — every point kind roble writes, a zero-length value, and a value
  larger than the block size.
- **multiblock** — 400 keys over 29 blocks, so the index has to be right
  rather than incidentally correct.
- **dups** — the same user key at several sequence numbers. This is the only
  fixture that sets `setHasSameKeyPrefix`, and so the `0x20` bit in the value
  prefix and the high bit of a restart offset.
- **prefixes** — Oxia-shaped keys with long shared prefixes, so prefix
  compression and the shared-prefix cap are doing real work.

## What this gate cannot catch

Comparing two dumps catches a disagreement between roble and Go Pebble. It
does **not** catch roble's writer and roble's reader agreeing on a convention
that is wrong in the same way, because Go's reader may then land on the same
bytes by applying the correct rule to mis-written data. That class of bug is
what `roble_reads_back_what_roble_wrote` in `format/tests/golden.rs` is for: it
compares what came back against what went in, not against another dump. Both
tests are needed; neither subsumes the other.
