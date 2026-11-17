# Fuzzing the engine

The engine half of DESIGN.md §12's fuzzing. The format crate's targets
(`format/fuzz`) reach each decoder one file at a time; `open_corrupt` takes
the path the engine actually takes through a whole database. It reads a
table through `OpenTable` rather than `Table::open`, replays the MANIFEST and
the logs, and trusts what each told it about the others. `model` feeds the
store nothing but valid calls and checks that it answers them correctly.

| Target         | Input                                   | What fails it            |
|----------------|-----------------------------------------|--------------------------|
| `open_corrupt` | a golden database, plus damage to its files | a panic, abort or hang   |
| `model`        | options, and a sequence of operations   | any answer a `BTreeMap` would not give, or any error |

The input picks one of the databases in `tests/golden`, read-write or
read-only, and a list of changes to its files: bytes overwritten or flipped,
a file cut short, extended, removed, or renamed to a name the engine would
recognise. The result goes on a `SimFs` and is opened. If the open succeeds,
the store is read every way it can be (both scans, every comparison of `get`,
`dump`). If it is writable it is then written, flushed and compacted, which
reads every table again.

Any error is allowed, at any step. The one thing compared is the undamaged
case: with no changes, every golden database must open, read and write, or
every damaged one would only be refused for the same unrelated reason.

`model` is `tests/model.rs` with the sequence as the input. It starts from a
golden database or an empty one, with the WAL on or off and small tables and
levels so that a short sequence still moves keys between levels, and runs
batches of puts, deletes and range deletions, flushes, compactions, reopens,
`get` under every comparison, bounded scans moved by seeks and steps both
ways, snapshots read after the store has moved on, and checkpoints opened as
stores of their own. Every answer is checked against a `BTreeMap`; a reopen
against what was durable, which with the WAL off is the last flush. Nothing
is allowed to fail: the filesystem is healthy and every call is one a caller
may make.

Checksums are skipped under `cfg(fuzzing)` as for the format targets
(format/fuzz/README.md), so damage reaches the parsers behind them.

## Running

```sh
cargo +nightly fuzz run open_corrupt -- -max_total_time=300 -malloc_limit_mb=256 -timeout=10
cargo +nightly fuzz run model -- -max_total_time=300 -malloc_limit_mb=256 -timeout=10
```

No seeding is needed: both targets read the golden databases themselves,
and an empty input is an undamaged database, or one opened and closed. CI runs it beside the format targets
(`.github/workflows/fuzz.yml`).
