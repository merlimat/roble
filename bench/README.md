# roble benchmark

Compares roble with **RocksDB** and **encina** on the workload Oxia runs:
small records, a store per shard, batches per write, point and ordered
lookups, scans in both directions, short range scans, YCSB's six mixes and a
checkpoint.

**Go Pebble is not here, and that is a loss worth stating plainly.** It had a
driver of its own under `harness/bench`, written to these phases and this key
shape, and it has been removed. `cmd/pebble`'s own benchmarks do not replace
it: `ycsb` builds its keys as `user` plus an FNV hash plus an MVCC suffix,
`scan` builds a hundred thousand `key-` plus `uint32` records and writes while
it reads, and neither key shape is configurable. They measure Pebble on *their*
workload, not on Oxia's. So what ended with the driver is the only
like-for-like comparison against the engine roble exists to replace — `load`,
`settle` and its CPU column, `scan`, `scan-rev`, `get`, `get-cold`,
`checkpoint`, and `floor`, which no `cmd/pebble` benchmark performs at all.
Numbers quoted below that name Pebble are from runs made while the driver
still existed; they are kept as the record of what was measured, not as
something a run today reproduces.

Each run benchmarks one engine in a process of its own, so the CPU time and the
peak memory it reports belong to that engine alone. `run.sh` runs all of them
in one standard environment — one CPU and 512 MiB each — over two scenarios,
one whose data fits the block cache and one whose data does not fit in memory
at all, and prints a table per scenario ("One core, 512 MiB", below).

| engine | what it is here for | how to get it |
|---|---|---|
| roble | the subject | always built |
| RocksDB | the engine Pebble replaced, and the LSM to check a ratio against | `--features rocksdb`, needs a C++ toolchain and libclang |
| encina | a copy-on-write B+tree that owes nobody compatibility — §2 claims Pebble's format is worth its price without naming the price, and this is the price | `--features encina`, fetched from git |

Neither optional engine is a default cargo feature, so an ordinary
`cargo build --workspace` neither clones encina nor needs a C++ compiler.

## Why it exists

§14 of [DESIGN.md](../DESIGN.md) leaves several questions waiting on a
measurement, and names milestones that have since passed. The sharpest is
reverse iteration: Oxia's `KeyRangeScanReverse` is on a serving path, an LSM
pays more for `prev` than for `next`, and the design says so without saying by
how much. `scan-rev` is that number and `scan` is its control.

A ratio from a single pair of numbers is not trustworthy enough to decide
anything. The first run of the reverse comparison had roble at 1.26x and
Pebble at 1.44x, which read as roble being the better of the two. It is not —
the forward rates move by a fifth between runs, and with RocksDB alongside,
all three sat in one band of about 1.1x to 1.2x. One comparison says a number;
two say whether it means anything. That lesson is why RocksDB is still here
now that Pebble is not: with one reference LSM left, a ratio against it says
less than it did, and a surprising one should be met with another run before
it is believed.

## Phases

They always run in this order, whatever order `--phases` names them in.

- **load** — writes `--batches` batches to each shard, each batch `--records`
  consecutive records, through an *indexed* batch because that is what Oxia
  uses: its `PebbleBatch` reads back through itself before committing. With the
  WAL on, each commit syncs.
- **settle** — leaves the database in a shape the read phases can be compared
  over. roble compacts cooperatively on the thread that owns the store (§9),
  in a task this driver spawns beside each shard, so this drives the picker to
  quiescence partly by merging and partly by handing that task the thread; the
  cost is counted here either way. RocksDB compacts on threads of its own, so
  this flushes and waits, and the cost shows up in the CPU column. The CPU
  column is what makes those two comparable. On a `--no-wal` load this row is
  where the compaction the load phase no longer pays for shows up: the load
  row gets faster and this one gets slower by about as much.

  **Leaving `settle` out does not give a reproducible unsettled store**, which
  is what `--no-compactions` is for. Without it the load compacts as it goes,
  paced by a clock, so how far it got before the reads began varies from run
  to run: eight runs over the same 300,000 records read four different
  shapes, from `L0=8 L1=4` to `L1=13`, and neither of the two binaries timed
  read the same shape twice running — so a read rate from one run is not
  comparable with a rate from another. With the flag nothing merges at all,
  so a load leaves L0 holding exactly what the flushes wrote — every table
  its own source in the merge, which is the widest a scan gets and the state
  a store under write load is heading for. Both LSMs turn their own
  compactions off (roble's `disable_automatic_compactions`, together with
  the task this driver would otherwise spawn to run them; RocksDB's
  `disable_auto_compactions`), and both then stop holding writes at the L0
  stop-writes line, since nothing would ever drain it. encina has no
  compactions to stop. `settle` is refused alongside the flag rather than
  given a meaning, because it would get a different one per engine: roble's
  drives the merges itself, and RocksDB's waits for threads the flag has
  stopped. `--reuse` does not help either way: opening a store flushes what the log
  replay put back in the memtable, and without the flag that lets the picker
  run, so a store reads as one shape in the process that wrote it and another
  in the next.
- **scan-rev**, then **scan** — steps every record of every shard, backward and
  then forward. Reverse goes first on purpose: see *Reading the numbers*.
  After `settle` they step over tables. Without it, and with a load that fits
  in one memtable — `--phases load,scan-rev,scan --batches 20` at the default
  64 MiB, where `levels` prints nothing — they step over the memtable instead,
  which is how its step back is measured.
- **get** — `--reads` keys drawn by `--distribution` (below), every one
  present. Oxia's `Get` with
  `ComparisonEqual`, and the one comparison a bloom filter can answer.

  **Each engine reads it the best way it offers, and reports the value's
  length rather than the value.** That is not a detail: every one of them can
  produce a value without handing back an owned copy, and they do it
  differently. roble reads into a `GetBuf` the caller keeps; RocksDB has
  `get_pinned`; encina passes the borrowed value to a closure; Pebble's `Get`
  borrowed from the block it was in and gave back a closer. Asking for an
  owned value charged some of them for an allocation this row is not about
  while others borrowed — which is what it used to do, and it is how roble's
  `get` row came to be not like-for-like with Pebble's until it was fixed. A length still
  proves the value was read, and the phase checks it against `--value-bytes`
  so that an engine cannot win by not producing one.

  The harness builds its probe keys into a buffer it reuses, on both sides,
  for the same reason: a `format!` or a `Sprintf` per probe is the harness
  measuring itself, and leaving it in one language and not the other would
  quietly favour one engine.

- **get-cold** — **one** point lookup per shard, on a store closed and opened
  again. The `get` phase above reports a warm steady state: `--reads` lookups
  in a row, after the scans have walked every block, with the filters and
  indexes cached and the tables open. A shard Oxia has just been handed is in
  none of those states, and the first read of it pays for the footer, the index
  and the filter of whatever table holds the key. Exactly one lookup, because a
  second would be warm — so `--shards 8` gives eight samples and `--shards 1`
  gives one, and the percentile columns collapse onto it. It empties the
  *engine's* caches, not the operating system's, so the blocks may still be in
  the page cache: what it measures is the work above the filesystem.
- **floor** — `--reads` probes drawn by `--distribution` (below), each one
  byte past a stored key so that **no probe exists**. That is deliberate: Oxia's `getFloor` tries an exact
  `Get` first and only seeks when it misses, so probing keys that exist would
  measure the `get` phase over again. What this times is the seek, which no
  filter can help.
- **scan-short** — `--reads` short range scans, each a seek to a key drawn by
  `--distribution` and then up to `--scans` steps. **YCSB E's read, and the
  one the other scan phases cannot stand in for.** `scan` and `scan-rev` walk
  the whole store, so they divide the seek and the per-source setup across
  every record in it and both vanish; Oxia's `KeyRangeScan` reads tens of
  records, where they are most of the cost. Each scan is **one** timed
  operation, as YCSB counts it, and not a sample per step. The phase checks
  the count it got back against what is left between the start key and the end
  of the shard, so a seek that landed in the wrong place cannot pass as a fast
  one.

  No upper bound is given to the iterator, because YCSB gives none and
  `cmd/pebble`'s `scan` does `SeekGE` and then steps a count. A bounded scan
  would let roble open only the tables the bounds reach (§5), so this is the
  pessimistic case for every engine alike.
- **ycsb** — `--reads` operations drawn from one of YCSB's six mixes, chosen
  by `--workload`. **This is the only phase that writes while it reads**, so
  it is the only one where a flush or a compaction can land inside a
  measurement — which is the state Oxia serves in and which every other phase
  here avoids by construction. It runs last, and a `--reuse` of the directory
  afterwards will find more records than `--batches x --records` and fail the
  count checks: that is the phase having worked, not broken.
- **checkpoint** — a checkpoint of every shard, which is how Oxia moves one.

`run.sh` runs these phases — all but `get-cold`, whose question the second
scenario now asks of every phase — with YCSB in a table of its own that has
all six mixes, in one environment over two scenarios ("One core, 512 MiB",
below). It used to add passes of its own: a large commit against a small one,
the journal on against off, the reads at two cache sizes and then under a
separate memory budget, each scan direction in a process of its own, and the
layout. Those are gone from it. Each is still an invocation or two of the
binary — `--phases load` at two `--records`, with and without `--no-wal`,
`--cache-mb`, `--phases load,settle,scan-rev` on its own, `--layout` — and what
they found is kept below, under "Reading the numbers".

An engine that cannot do a phase prints a dash and the reason instead of a
zero, because a zero reads as "it did this and it took no time". encina skips
`scan-rev`, having only a forward `Iter`, and `checkpoint`, having none —
which is part of the comparison rather than a gap in it: an engine that
answers fewer questions can answer them faster, and both of those are Oxia
requirements roble meets.

A `levels` line is printed before the scans: how many tables sit at each level.
A scan's cost is mostly how many iterators it has to merge, so without it a
slow iterator cannot be told from a differently-shaped LSM — and on this
workload that distinction turns out to be the whole story.

## Which keys the read phases ask for

`--distribution` picks how `get` and `floor` choose which record to probe.
The default is **zipf**, because that is YCSB's default and `pebble bench
ycsb`'s, and because uniform is the one shape no real workload has.

- **zipf** — YCSB's skew at theta 0.99. Measured over 100,000 keys, the
  hottest 1% of them take about 62% of the traffic.
- **latest** — YCSB D's, where the most recently written records are the
  hottest.
- **uniform** — every key equally often, which is what the phases did before
  there was a choice. Pass it to reproduce a number measured then.

**The draw is Pebble's arithmetic, deliberately.** The zeta/eta/alpha
constants and the three-branch draw are `randvar.Zipf` line for line, and the
scramble is `cmd/pebble`'s own key hash — an FNV-1 variant, copied rather than
corrected. A roble number and a `pebble bench ycsb` number are comparable only
if the keys they ask for come from the same distribution, and running the same
arithmetic is the cheapest way to be sure of that.

**Zipf is scrambled and `latest` is not.** A raw Zipf draw makes the *lowest*
record numbers the hottest, and record numbers are ordered the way the keys
are — so without a scramble every hot key would land in the first table of the
store, which is a shape no workload has and one that would make every cache
number look far better than it is. Hashing the draw across the keyspace is
what YCSB's `ScrambledZipfianGenerator` is for. `latest` means recent, and
recency *is* the record number, so scrambling it would destroy the only
property it has.

**It matters where the cache binds, and almost nowhere else.** On a 42 MiB
store, 50,000 reads, median of three:

| cache | phase | uniform | zipf | latest |
| --- | --- | --- | --- | --- |
| 64 MiB | `get` | 571,317/s | 1.031x | 1.071x |
| 64 MiB | `floor` | 311,634/s | 1.065x | 1.114x |
| 4 MiB | `get` | 102,070/s | **1.351x** | **2.626x** |
| 4 MiB | `floor` | 70,546/s | **1.301x** | **2.215x** |

At 64 MiB the store fits in the block cache, every read hits whatever it asks
for, and the distribution can only change which CPU cache line is warm. At
4 MiB it decides the hit rate, and `floor`'s p50 falls from 14 us to 3 us. So
the distribution is worth most where the cache cannot hold the store — the
*beyond-memory* scenario now, the reads-in-cache-and-out pass when this was
measured — and that pass was measuring the miss path with the one access
pattern that makes a miss path look like a fair fight.

### Reading over a range deletion

`--delete-range LO:HI` range-deletes records LO to HI of every shard, as
fractions of the records loaded, before `get` and `floor` run. Oxia deletes
ranges, and so would redwood's index, whose keys arrive mostly in timestamp
order with an occasional large range deleted. A range tombstone is read-path
work an LSM keeps paying until a compaction drops it (DESIGN.md §6), so a run
without one measures none of that work.

- The probes are drawn as before, so the share of them that land in the range
  is about its share of the records.
- `get` of a deleted record has to find nothing.
- `floor` of one has to land on the live key below the range.
- An engine that answers either wrongly fails the run, as a wrong value does.

The keys sort by bucket first, so a range of records is one deletion per
bucket. By default the deletions come after `settle`, which leaves the
tombstones in a memtable above every table. `--delete-before-settle` makes
them before it, so that `settle` flushes and compacts them with the records
they delete. Only `get` and `floor` account for the deleted records, so the
other read phases and `ycsb` are refused alongside it.

### How long a scan is

`--scans` takes `[{zipf,uniform}:]min[-max]`, which is `pebble bench ycsb`'s
spelling of the same thing, and defaults to `zipf:1-1000` as Pebble's does.
`--scans 20` fixes every scan at twenty records.

**Scan lengths are not scrambled, where the keys are.** A length is a
magnitude and not a key, and short scans being the common case is the shape
YCSB E has and the shape Oxia has — which is exactly what an unscrambled Zipf
draw gives. The scramble exists only to stop hot *keys* collecting in one
table.

**What a short scan is made of.** Fixed lengths, 20,000 scans, 42 MiB store,
64 MiB cache, median of three:

| records | scans/s | us/scan | us/record |
| ---: | ---: | ---: | ---: |
| 1 | 205,873 | 4.86 | 4.857 |
| 10 | 174,381 | 5.73 | 0.573 |
| 100 | 78,648 | 12.71 | 0.127 |
| 500 | 23,355 | 42.82 | 0.086 |

which is **4.8 us of seek and setup, then 76 ns a record**. A ten-record scan
is 83% setup, a hundred-record scan 38%, and the 300,000-record `scan` phase
0.002% — so the whole-table phases have been measuring the one case where the
setup does not exist. Note also that 76 ns a record is *faster* than `scan`'s
178 ns: short scans over a skewed keyspace return to the same blocks, and
those stay in the CPU's cache where a whole-table walk streams 42 MiB past it
once (§8).

That 4.8 us is worth reading against `floor`, which is 3.2 us for a seek that
answers one key. The difference is the merge roble builds over every table in
the version before the first step — which an Oxia `KeyRangeScan` would not
pay, because it has bounds and `range` opens only the tables they reach.

With the default `zipf:1-1000` spec, 20,000 scans:

| cache | uniform | zipf |
| --- | --- | --- |
| 64 MiB | 65,394/s | 68,321/s |
| 4 MiB | 26,236/s | 28,972/s |

## The YCSB mixes

`--workload` takes A to F. The proportions are YCSB's, from the paper and the
`workload*` files, and the distribution is **the workload's** rather than
`--distribution` — a YCSB workload drawing from somewhere else would not be
that workload.

| | operations | keys | `pebble bench ycsb` |
| --- | --- | --- | --- |
| A | 50% read, 50% update | zipf | same |
| B | 95% read, 5% update | zipf | same |
| C | 100% read | zipf | same |
| D | 95% read, 5% insert | **latest** | **differs** |
| E | 95% scan, 5% insert | zipf | same |
| F | 50% read, 50% read-modify-write | zipf | **differs** |

**D and F are YCSB's definitions and not Pebble's.** `cmd/pebble` has D on the
default distribution, carrying a `TODO(peter): default to skewed-latest`, and
F as 100% inserts, carrying a `TODO(peter): the real workload is
read-modify-write`. Rather than quietly compare two different workloads, those
two are run as YCSB defines them and the phase prints a note saying Pebble has
no row beside it. A, B, C and E are the same thing on both sides.

**What a mix reports.** One row for the workload, as YCSB reports throughput,
plus a per-operation line — because a 50/50 mix's own p50 is two distributions
laid on top of each other and says very little. That line carries p50, p99,
p99.9 and max, and it needs all four: on this phase they span four orders of
magnitude.

**A writing mix drives compaction by writing.** roble compacts cooperatively
on the thread that owns the store (§9) — in the two tasks each store spawns
for itself when it is opened. A write at the line tells the flushing one
there is a memtable to write out and hands it the thread; it writes it out,
and the merge the picker chooses follows. So the phase does not have to yield
to any of it, and L0 stays inside its allowance of four — **1 or 2** through
800,000 operations, with the store around 46 MiB — the two differing only in
where the phase stops in a flush cycle. The level shape is printed anyway,
because the day that stops being true is the day this number is the first to
say so.

**The stall this phase found, and where it went.** This phase is what made
the write stall visible, and the numbers it reported are what the flush line
and the two tasks were built for. 400,000 operations of workload A, one
shard, `--no-wal`, medians of three alternated runs each:

```
before   ycsb-A  400000  400,922/s  p50 1us  p99 12us  max 131,991us
  read   199790  p50 1us  p99 14us  p99.9    20us  max     231us
  update 200210  p50 0us  p99  2us  p99.9     5us  max 131,991us

after    ycsb-A  400000  412,527/s  p50 1us  p99 14us  max   1,305us
  read   199790  p50 1us  p99 14us  p99.9    42us  max   1,305us
  update 200210  p50 0us  p99 13us  p99.9   495us  max   1,251us
```

One write in two hundred thousand used to wait **132 ms**, because it was the
write that found the memtable full, flushed it, and ran a compaction before it
could return. It now waits **1.3 ms**, which is a read's worst case: the flush
happens in a task beside the writes, started an eighth of the budget before
the memtable fills, and the merge happens in another.

**What it cost** is in the other columns, and it is the same work spread
differently rather than less of it. p99.9 on an update goes from 5 us to about
500 us, and a read's worst case from 231 us to 1.3 ms, because a merge paced
at `compaction_unit` entries now hands out its slices to whatever is running —
reads included — where before it arrived as one 130 ms lump on a single write.
The rate is unchanged, and so is the memory: peak RSS is 218 MiB against 217.
`compaction_unit` is the lever on how large a slice is.

At the 64 MiB memtable RocksDB and `cmd/pebble` default to, where a flush is
sixteen times as much to write, the same comparison over 800,000 operations is
139 ms down to **1.7 ms**, with a read's worst case untouched at about 250 us
and the rate equal.

42 MiB store, 50,000 operations, 64 MiB cache, one shard:

| | ops/s | per operation |
| --- | ---: | --- |
| A | 476,987 | read p50 1us p99 15us, update p50 0us p99 2us |
| B | 542,478 | read p50 1us p99 13us, update p50 0us p99 1us |
| C | 593,827 | read p50 0us p99 13us |
| D | 576,719 | read p50 0us p99 14us, insert p50 0us p99 4us |
| E | 68,413 | scan p50 5us p99 85us, insert p50 0us p99 3us |
| F | 381,497 | read p50 1us p99 14us, rmw p50 2us p99 16us |

A and F show a worst case in the milliseconds where the read-only phases show
tens of microseconds — that is a flush landing inside a measurement, and it is
the first time anything here has measured one. E is `scan-short`'s number,
which it should be: 95% of E is that scan. A leaves `L0=2` where the `settle`
before it left `L0=1`.

### Cross-checking against Pebble's own tool

There is no Pebble column here any more, but `cmd/pebble` can still say
whether a roble number is in a plausible band:

```sh
go install github.com/cockroachdb/pebble/v2/cmd/pebble@v2.1.7
pebble bench ycsb --workload B --values 128 --keys zipf <dir>
```

**It is weaker than that sentence suggests, and it was measured rather than
assumed.** Run on a Linux VM against the same 300,000 records and 200,000
operations this harness uses:

| | roble | pebble ycsb |
| --- | ---: | ---: |
| A | 332,197/s | 398,403/s |
| B | 617,766/s | 481,764/s |
| C | 602,702/s | 553,338/s |
| D | 649,683/s | 482,812/s |
| E | 68,555/s | 93,853/s |
| F | 260,979/s | 199,631/s |

Those look comparable and are not, because `cmd/pebble/db.go` hardcodes its
own engine options and exposes only `--cache` and `--disable-wal`:

- `MemTableSize: 64 << 20` — **64 MB**, against the 4 MiB this harness pins on
  every engine precisely because defaults are not comparable. The dataset is
  42 MiB, so it fits in one memtable and the run barely flushes: the reported
  write amplification is 2.2 to 2.7. **A, B, D and F therefore measure a
  skiplist rather than an LSM.**
- `Comparer: cockroachkvs.Comparer` — CockroachDB's MVCC comparer, which is
  the class of comparer roble refuses (§3), with the matching `KeySchema` and
  a `cockroach_merge_operator`.
- `FormatMajorVersion: FormatNewest`, where roble's pin is Pebblev4 (§3).
- `CompactionConcurrencyRange: 1..3` — background compaction on up to three
  threads. `--concurrency` sets the number of *client* workers and does not
  touch it.
- Its keys are `user` plus an FNV hash plus a ten-byte MVCC suffix, built in
  `makeKey` with no flag to change them.
- `Filter utility: 0.0%` in every run above.

So this is Pebble on CockroachDB's workload with CockroachDB's settings, and
the right way to read the table is that **none of those six pairs is a
comparison**. It is worth running to confirm the tool works and that roble is
not off by an order of magnitude from an engine of the same family; it cannot
settle a percentage, a phase, or a direction. The thing that could — a driver
on Oxia's key shape through these phases — is what was removed with
`harness/bench`.

## One table, all the engines

`run.sh` collects every engine's `--tsv` lines and pivots them with
`pivot.py` into one table — a row per operation and metric, a column per
engine — so the comparison is read across a row rather than by holding three
blocks in your head:

```
                           roble     rocksdb      encina
scan       rate/s      2,444,142  10,133,423  48,184,380
           p50 us              0           0           0
           cpu s            0.12        0.03        0.01
floor      rate/s        127,139     488,003   2,392,593
```

The run this was taken from had a fourth column, `pebble`, at 33,848,584 for
`scan` and 474,211 for `floor`. It is left out here because nothing produces
it any more.

A phase an engine cannot do shows `—` in the table and the reason underneath,
never a blank that reads as a zero. Run one engine alone and the aligned
per-run output comes back instead.

## Running

```sh
BENCH_DIR=$HOME/bench-stores ./run.sh            # the standard: three engines, both scenarios
```

`ENGINES` picks which to run, so the ones needing a toolchain can be left out,
and anything passed goes to every run after the standard arguments, so it wins
over them:

```sh
ENGINES="roble" ./run.sh                          # nothing to install
ENGINES="roble encina" ./run.sh                   # adds a git fetch
./run.sh --min-seconds 2                          # a quicker look
STANDARD=0 ./run.sh --batches 20 --min-seconds 1  # a smoke test where the cap cannot apply
```

`BENCH_DIR` matters on Linux, where `/tmp` is often a tmpfs: a tmpfs's pages
count against the cap, so a store there is memory rather than disk, and
`run.sh` refuses one. `STANDARD=0` is for a machine that cannot apply the cap
at all, macOS among them, and prints NOT THE STANDARD above every table it
makes. A full run takes about fifteen minutes on the VM these numbers come
from.

Or one engine at a time:

```sh
cargo run --release -p roble-bench -- --dir /path/to/scratch --no-wal
cargo run --release -p roble-bench --features encina -- --engine encina --dir /path/to/scratch --no-wal
cargo run --release -p roble-bench --features rocksdb -- --engine rocksdb --dir /path/to/scratch --no-wal
```

The store is created at `DIR/<engine>`, which must not exist yet.
`--help` lists the options; both engines take the same ones and the same
`--seed`, and both derive their keys, their values and their lookup order from
it with the same generator, so the two hold the same bytes and probe them in
the same sequence.

## Reading the numbers

**`--no-wal` is Oxia's configuration.** It used to be the only one in which
the load row meant anything on macOS: Go's `os.File.Sync` is
`fcntl(F_FULLFSYNC)` there, which flushes the drive's own write cache, where
compio's and RocksDB's are `fsync`, which does not — so with the WAL on, roble
appeared several times faster at writing than Pebble and was really just
promising less. With the Go driver gone, every engine in the table syncs the
same way and that trap is no longer in it. It is kept here because it is
exactly the trap anyone setting a roble number beside a Go Pebble number
obtained elsewhere will walk into, and because on Linux — which is what Oxia
runs — they are all `fsync` anyway.

**encina's load row is not quite in the same units.** Its `write` is durable
by itself and takes no sync flag, so `--no-wal` asks roble and RocksDB for
less than it asks encina for. That counts against encina on the load row
and nowhere else.

**The phases share a block cache and run in order**, so whichever of the two
scans goes second reads a cache the first one warmed. Reverse runs first
because reverse is the direction under suspicion: a `scan-rev` that keeps up
from there is keeping up from behind. For a comparison with neither direction
favoured, run each in a process of its own over a store of its own
(`--phases load,settle,scan-rev`, then `--phases load,settle,scan`). `run.sh`
did that once and no longer does, so its `scan` row is the one that reads a
cache `scan-rev` warmed.

**Step latencies are sampled**, one step in 64. A scan step costs tens to
hundreds of nanoseconds and reading a clock costs the same order, so timing
every step would charge the faster engine the larger share of the measurement
overhead — enough, on the forward scan, to reverse which engine looks quicker.
The rate comes from the phase's own wall clock, which the sampling leaves
alone.

**Every LSM knob is set the same on both, because their own defaults are not
comparable.** roble took Pebble's defaults (§4, §9), so the settings here are
Pebble's whether or not Pebble is in the table; RocksDB's differ enough to
decide a result rather than inform it, and the benchmark sets them from
roble's own constants — the memtable, the
target file size, the L0 compaction and stop-writes thresholds, and the base
level's budget.

**The open-table budget was the one knob this paragraph did not cover, and it
decided a result.** RocksDB's `max_open_files` defaults to no limit and the
benchmark never set it, so RocksDB held every one of its tables open while
roble kept a default 128 of the 243 its store settled into — reopening one on
nearly every lookup, which is a table's footer, index, filter and tombstones
rather than a file handle. It cost `floor` fifty-five times and was read as a
gap between the engines. roble's default is now no limit too, so the two
match; a deployment that cares about file descriptors sets a budget on both
(§8). `--memtable-mb` is the one of them exposed as a flag, and it
is **64 MiB** — RocksDB's own default, and the size `cmd/pebble` hardcodes for
its YCSB benchmark, so a number from here and a number from that tool are at
least buffering the same amount.

**A memtable that size dictates the dataset.** It was 4 MiB before, for a
reason that has not gone away: left at RocksDB's 64 MiB default, RocksDB
buffered the whole workload and flushed nothing during the load while the
other engines wrote 30 MiB of tables. Standardising on 64 MiB hands that
problem to *every* engine instead of one, so the dataset has to be well past
64 MiB per shard or the load row measures a skiplist and the read phases run
over whatever `settle` was left to write in one go. At `--value-bytes 128` a
record costs about 148 bytes on disk, so a shard needs **upwards of half a
million records** before a 64 MiB memtable is doing anything, and several
million before flushes are routine rather than occasional. The defaults alone
no longer give a meaningful load row; `--batches 20000 --records 300` does.

The target file size was the one that looked decisive and was not. RocksDB's
default is 64 MiB against their 2, which settled a 34 MiB store into a *single*
table where roble leaves eleven (and Pebble, when it was still measured here,
nine) — so a lookup searched one table instead of three. Setting it to 2 MiB gives RocksDB sixteen tables and
changes its point reads by under 1% on the same machine, because its filters
absorb them. Worth aligning anyway, and worth recording that the layout did
not explain the gap it looked certain to explain. `--no-wal` is another: it takes
`disable_wal` on roble and `disable_WAL` on RocksDB's write options, and setting only the sync flag on the last of those left it
writing a journal the others were not — worth 5 MiB of directory and half its
load rate. `--cache-mb` is the third, divided per store for encina, whose
cache is its own.

**`--value-chunk` dials how compressible a value is**, since the default
values are random and give Snappy nothing to find — so without it every
engine's compression setting is inert and the size column says nothing. A
value is one random chunk repeated to fill it: 128 is incompressible, 8 is a
sixteenfold repeat. roble's size does not move across that range, because its
writer only ever writes `NoCompression`; RocksDB's falls by three to four and
a half times.

**Values are pseudo-random by default.** A value made of one repeated byte compresses to
nothing, and with one the size column reported Snappy saving 2.5× — a
measurement of the generator, not of Oxia's data. Real Oxia values are
serialized records, which sit somewhere between the two, so the size column
here is the incompressible end of the range rather than the answer for any
particular shard.

**The read columns are page-cache-bound unless something stops them, which
is what the beyond-memory scenario is for.** Every engine here but encina reads through
the operating system's page cache (§8 — direct I/O is roble's goal and not its
state), and at any dataset this harness can load in reasonable time the store
fits in RAM. So `get` and `floor` measure finding and parsing a block again,
not fetching one. At six million records and a 4 MiB block cache, roble's
`get` reads 18.5x encina's warm; held to a 256 MiB cgroup it reads **1.7x**,
and `floor` goes from 4.5x to **1.01x** — level with a B+tree. The ratio was
mostly the page cache. (At a 64 MiB cache the warm ratios are 8.1x and 1.7x,
because encina's own cache then holds more of its tree.)

**`drop_caches` does not measure that, though not for the reason first given
here.** The cold-disk pass, since removed, ran `scan`, `get` and `floor` in that order, and
the scan reads the whole store back into the page cache in key order, so the
lookups after it run warm: roble's `get` comes out at 17x encina's against
18.5x warm, and `floor` at 4.7x against 4.5x. Dropping the cache moves neither
ratio, in either direction. The `scan` row is the honest one there, because
reading 846 MiB in order keeps meeting blocks nothing has faulted in yet:
roble loses 32% and RocksDB 29% where encina loses 2%.

An earlier version of this paragraph claimed the opposite on both counts —
that random lookups re-fault the store within the first second of a phase, and
that `drop_caches` therefore *widened* the gap, from a warm 2.6x to 16x. The
2.6x was an arithmetic error (it is 307,075 / 37,797 = 8.1x), the 16x was set
beside a column at a different cache size, and random reads re-warm a store
slowly, not at once: with the page cache dropped and only YCSB C after it,
roble ran at 0.71x its warm rate and RocksDB at 0.33x across the whole phase,
p99 73 and 118 us against 8 and 5. That is what a restart onto a cold store
looks like, and it is a different question from a store held to a budget.

So the reads were held to a cgroup budget instead, and now every run is: the
whole process, load included, held to 512 MiB ("One core, 512 MiB"). cgroup v2
charges a page to whichever cgroup faulted it in, so a store the cap cannot
hold has the kernel reclaiming for as long as a phase runs, and systemd's user
session normally has the memory controller delegated, so this needs no root
where `drop_caches` needs one. `run.sh` checks the cap is actually applied
rather than silently ignored, and refuses to run if it is not.

One thing to hold against those numbers: the cap is a chosen number, and the
result is a function of it. 512 MiB says what an engine does when its store
does not fit, not where it lands at a particular deployment's budget. The
budget pass that came first also set `MemoryHigh`, whose throttling stalled
the allocator and pulled every engine's `cpu s` well under 100%; the standard
sets only `MemoryMax`, so reclaim is all that is charged to the process.

**The journal rows, which `run.sh` no longer prints, were the most
platform-dependent numbers here.** Each
engine's own on-against-off ratio is honest about that engine on this machine,
since it is its own sync both times. Across engines it was once not comparable
at all on macOS: Pebble paid **36x** for the journal against roble's **1.7x**,
and almost all of that was `F_FULLFSYNC` against `fsync` rather than anything
about either engine. That pair of numbers is a record of what was measured
while the Go driver existed, not something a run today produces. roble and
RocksDB are the same syscall as each other, so their ratios can be compared —
on Linux, which is what the runner below is for, and where this measurement
belongs anyway.

**The batch-size pass, gone from `run.sh` with the others, was read *within*
an engine, not across engines**: how
much of its throughput depends on the caller batching. With `--no-wal` the
LSMs sync nothing per commit while encina commits durably whatever it is
asked, so the per-commit cost it shows is a different cost from theirs. Run
without `--no-wal` for the comparison where every engine commits durably.

**Both sides of it batch, and that is a change.** It used to write the small
side one record per commit, which separated the two costs as cleanly as
anything could and was untenable at any real dataset size. A copy-on-write
B+tree writes a new root per commit: at 300,000 records encina dropped to
0.006x of its batched rate, and at six million it spent over an hour and
2.2 GiB of disk on that one row before it was stopped. A tenfold spread
between two batch sizes says the same thing in seconds, which is why
`--records` defaults to 10,000 and the pass divided it by ten rather than
going to one. If the single-insert number is what you actually want,
`--records 1` still gives it — at a dataset size you chose on purpose.

The old numbers, for the record: roble kept 0.84x of its batched rate at one
record per commit because a memtable absorbs a single insert, RocksDB 0.24x,
and Pebble 0.94x when it was still measured here.

**`settle` is not comparable row for row**, by construction. roble's does the
compaction work on the thread that owns the store; RocksDB's waits for
background threads; encina has nothing to settle at all. Compare the
`cpu s` column and the `levels` line, not the rate.

**The `levels` line is not enough to explain a scan number, and `--layout` is
the reason this is worth saying.** On this workload roble settles at
`L0=1 L1=13` and RocksDB at a single file, which looks like all the
explanation a scan gap needs. The layout shows it is not: roble's thirteen L1
tables partition the key space with no overlapping pair, exactly as Pebble's
eleven at L6 did when it was measured here, and a run small enough to leave
two tables in each engine kept roble 4.3x behind. The shape is not the cost — the per-step
path is. Which is why `--layout` prints the ranges and not just the counts:
the counts invited the wrong answer and were believed for a day.

**`cpu s`, not `rate/s`, is the column to compare** when the engines schedule
work differently. roble compacts on the thread that owns the store; RocksDB
uses its own; a rate credits the background one with work it still paid for.

## One core, 512 MiB

The `cpu s` column says what a phase cost. It does not say what the rate would
have been had the engines been given the same machine, and on a multi-core box
they are not: roble is single-threaded by design, compaction included (§9), so
one store uses one core and no more, while RocksDB puts flushes and compactions
on background threads and uses several. A rate measured with six cores free is
roble's one core against RocksDB's six.

Which comparison is the right one depends on how the thing is deployed. roble's
first consumer shards, one store per thread, so the question that decides a
design is throughput *per core* — six roble shards on six cores against one
RocksDB on the same six. Memory is the same argument: a node's memory is split
between its shards, so a store gets a share of it and not the machine, and an
uncapped run lets the page cache hold any store the machine has room for — at
which point no read in it ever leaves memory.

So `run.sh` runs every engine's process as

```sh
systemd-run --user --scope -p MemoryMax=512M -p MemorySwapMax=0 -- taskset -c 3 roble-bench ...
```

— one CPU, 512 MiB with the page cache counted against it, and no swap — with
a 256 MiB block cache, over two scenarios that differ only in how much they
load:

| scenario | records | on disk | what it measures |
|---|---|---|---|
| in-cache | 1,400,000 | about 200 MiB | the CPU path: once the scans have warmed the block cache, no read leaves the process |
| beyond-memory | 6,000,000 | about 850 MiB | the miss path: more than the process may hold, cache and page cache together |

Each prints every phase and YCSB A to F, and nothing else does.

**Swap is off because the VM has some.** Without `MemorySwapMax=0` a capped
process pages its own memory out rather than being held to the cap, and the
cap is then a number on paper.

**The cap is checked, not assumed.** `run.sh` asks the scope it would run in
what it actually holds — the cap, no swap, one CPU — and runs nothing if the
answer is wrong. A user scope needs a systemd user session with the memory
controller delegated, which Ubuntu has; a CI runner has no user session, so
`run.sh` falls back to a system scope through passwordless sudo, dropping back
to the invoking user inside it so that the stores are not root's.

Affinity is inherited by every thread and every child, which is what lets the
`taskset` inside the scope pin an engine's background threads as well as its
own. A `--scope` runs in the caller's own context, so a `taskset` outside one
would carry in too; without `--scope` it would not, because the user manager
starts the command and was never pinned — the same prefix around
`systemd-run --user` leaves the command's `Cpus_allowed_list` at `0-5`.

`taskset` rather than a cgroup, although a cgroup is where `AllowedCPUs=` lives:
cgroup v2 delegates `cpu`, `memory` and `pids` to a user slice but not `cpuset`,
so that one needs root. `CPUQuota=100%` needs no root and is a different thing —
one core's worth of CPU *time*, which the threads may spend in parallel and then
be throttled for the rest of the 100 ms period. That throttle arrives as a stall
in `p99` and `max us`, the same columns a flush lands in, so the knob that reads
clean is the cpuset one.

Pick a CPU that is not also serving interrupts. `/proc/interrupts` on the VM
these numbers come from pins one virtio block queue to each of the six CPUs and
puts the network and console queues on CPU 0 as well, which is why `CPU`
defaults to 3 and not 0.

### What one core found

Measured before the memory cap, which is why there is a six-core column at
all. Over 6,000,000 records with `--cache-mb 64 --no-wal`, one run per cell. `@1` is
pinned to CPU 3, `@6` is the whole machine:

```
                   roble@1    roble@6      rdb@1      rdb@6  r/rdb@1  r/rdb@6
load rate/s      1,058,670    887,989    982,406  1,641,511     1.08     0.54
load cpu s            4.62       5.65       5.63       6.60     0.82     0.86
settle cpu s          4.29       5.63       2.58       3.01     1.66     1.87
scan-rev rate/s  4,276,323  4,111,481  7,390,524  6,119,023     0.58     0.67
scan rate/s      9,360,204  8,871,463 10,694,376  8,386,035     0.88     1.06
get rate/s         296,950    273,074    422,038    322,870     0.70     0.85
floor rate/s        75,875     73,680    245,444    144,572     0.31     0.51
get-cold rate/s      4,822      2,764      2,524      1,907     1.91     1.45
ycsb-A rate/s      227,333    228,680    558,071    600,958     0.41     0.38
ycsb-B rate/s      239,271    242,248    464,290    489,395     0.52     0.49
ycsb-C rate/s      284,304    282,889    401,897    404,830     0.71     0.70
ycsb-D rate/s      454,141    456,616    687,450    713,687     0.66     0.64
ycsb-E rate/s       26,384     25,844     51,744     48,625     0.51     0.53
ycsb-F rate/s      152,121    150,858    364,804    388,630     0.42     0.39
```

**roble's two columns are the same column.** Across the six mixes its rate
moves between 0.98x and 1.01x of its pinned rate, so five more cores buy it
nothing — which is what "one store, one thread" predicts, now measured. Read
`@1` and ignore `@6`.

**The load comparison was a core-count artifact.** At six cores RocksDB loads
1.85x faster; on one core each, roble loads 1.08x faster for 0.82x the CPU,
because RocksDB's load is the one phase that really does use the extra cores
(1.67x across the five). This is the comparison to quote for a shard-per-core
deployment, and it is the opposite of the uncapped one.

**The read gaps are not core-count artifacts.** RocksDB's rate on the mixes
moves only 0.94x to 1.08x across the same five cores — in a read-dominated ten
seconds its background threads have little to do — so the cap neither explains
nor closes them. `floor` widens under the cap, from 0.51x to 0.31x, because
pinning is worth 1.70x to RocksDB and 1.03x to roble. A step already missing to
DRAM has no locality left to win, which is what §8 concluded by another route.

Both engines are faster pinned on every read phase, so an unpinned run of
either is measuring thread migration as well as the engine.

Two columns that are not comparisons. `peak rss` is 249 MiB against 203: roble's
Rust allocator is mimalloc while RocksDB's C++ internals go through glibc
malloc, so part of that gap is the allocator. And `get-cold` moved 1.74x between
roble's own two runs — it is the only phase that reaches the device, behind a
qcow2 on host writeback, and one run of each is not enough to call it.

### What the standard found

The first run of it, on the VM, main at `b19065f`, one run per cell. It took
fifteen minutes for three engines and both scenarios.

```
                     in-cache, 1.4M records          beyond-memory, 6M records
                   roble    rocksdb   r/rdb         roble    rocksdb   r/rdb
load rate/s    1,129,487    752,623    1.50     1,000,101    900,577    1.11
settle cpu s        0.66       0.40    1.65          5.61       3.73    1.50
scan-rev       4,552,198  6,561,805    0.69     2,846,074  1,454,758    1.96
scan          11,298,004  9,249,795    1.22     7,683,786  5,642,590    1.36
get              770,055    705,625    1.09        40,828     46,218    0.88
floor            497,134    278,835    1.78        37,755     42,345    0.89
scan-short        70,741     57,017    1.24        13,389     15,751    0.85
ycsb-A           248,336    556,814    0.45        65,913    106,413    0.62
ycsb-B           463,972    834,329    0.56        42,600     54,858    0.78
ycsb-C           832,292    797,309    1.04        42,587     52,416    0.81
ycsb-D           785,141  1,056,555    0.74       104,629    134,450    0.78
ycsb-E            69,892     57,367    1.22         8,987     10,438    0.86
ycsb-F           208,808    410,343    0.51        39,175     36,812    1.06
peak rss MiB         246        261                   306        317
```

**In cache, roble leads every read but the reverse scan, and loses as soon as
writes are mixed in.** YCSB C, all reads, is 1.04x; B is the same reads with
one operation in twenty an update, and it is 0.56x. RocksDB runs B slightly
faster than C — its writes are close to free — where five percent of writes
cost roble nearly half its throughput, so a write interleaved with reads costs
roble many times what it costs RocksDB. A and F, half writes, sit at 0.45x and
0.51x. That is the gap this standard exposes and the old configuration did
not: it is now the largest one.

**Past memory, point reads trail and full scans lead.** `get`, `floor` and
`scan-short` are 0.85x to 0.89x, where every miss goes to the device; the full
scans are 1.36x forward and 1.96x reversed. `settle` still costs roble 1.5x
RocksDB's CPU, the cooperative compaction's bill (DESIGN.md §9).

**The phase table stays well under the cap; the write-heavy mixes did not.**
In the phase table above, peak resident memory is 246 to 317 MiB, and the rest
of the 512 MiB is page cache. YCSB A and F, which write half the time, peaked at
492–510 MiB for roble and 424–441 for RocksDB in later runs, and one capped run
of roble was killed. Two things were in that. 48 MiB was the benchmark's own:
three arrays of up to a million 16-byte latency samples, now a histogram of
36 KiB for each (the latency columns, below). The rest of roble's excess was
its memtable arena doubling to 86 MiB on the way to a 56 MiB flush line, now
allocated at its budget in one step (DESIGN.md §8). With both, in cache, roble
peaks at 392 MiB in A, 321 in B and 361 in F, against RocksDB's 389, 291 and
374; past memory, at 377 in A against RocksDB's 424. Those are medians of
three, because a peak moves between runs of the same build — by up to 20 MiB
for either engine in A, and for roble by up to 50 under the cap. The table
above predates both and is left as it was measured.

encina, the copy-on-write B+tree, reads twice as fast as either LSM in cache
and collapses under writes that commit one at a time: YCSB A runs at 2,361 a
second, against roble's 248,336. Its column is in `run.sh`'s output and left
out here, which is about the two LSMs.

## Columns

- **count** — records for load and the scans, lookups for get and floor,
  shards for settle and checkpoint. **rate/s** is that count per second of
  wall clock.
- **p50 / p99 / max us** — one operation: a sampled step for the scans, a
  lookup for get and floor, one scan for scan-short, one operation for ycsb,
  one shard's checkpoint. In microseconds to a tenth, read from a histogram of
  every operation the phase timed, kept in nanoseconds to two significant
  digits: exact below 256 ns, and at most 0.8% high above.

  **`load` reports `max` alone and a dash for the other two**, because its
  latency is per *commit* where its count and its rate are per *record*. At
  `--records 10000` a p50 of six milliseconds is six milliseconds of batch
  size and nothing else — halve the batch and it halves — so the two columns
  moved with a flag the reader had chosen and looked like measurements.

  `max` stays, and is the reason the row is not simply dropped: a flush or a
  compaction landing inside a write shows up there and nowhere else. It is
  what made roble's write stall visible at all, at 129 ms with a 4 MiB
  memtable and 738 ms at 64 MiB, before any phase wrote while it read.
- **cpu s** — the whole process's CPU time, user and system, background
  threads included. This is the column that makes the engines comparable at
  all, since roble compacts on the thread that owns the store and RocksDB
  compacts on its own.
- **size MiB** — what the store occupies on disk at the end of the phase.

The last line is the process's peak resident memory.

## The allocator

roble is a library, so `#[global_allocator]` is never its choice — it belongs
to whatever binary links it, which for the first consumer is Oxia's data
server. **The benchmark builds with mimalloc by default**, because the
platform's own allocator can decide a result by itself: macOS's libmalloc
keeps large freed regions rather than returning them, and at six million
records that alone put roble's `load,settle` peak RSS near a gigabyte where
the same build under mimalloc, on the same machine, held about a quarter of
it. A benchmark should run under an allocator a deployment would link, and
glibc and mimalloc both qualify where macOS's does not.

```sh
cargo run --release -p roble-bench -- --dir /tmp/b --no-wal                          # mimalloc
cargo run --release -p roble-bench --no-default-features -- --dir /tmp/b --no-wal    # system
cargo run --release -p roble-bench --features count-alloc -- --dir /tmp/b --no-wal   # counted
```

`--no-default-features` puts the system allocator back, which is how the
comparison below was made. `count-alloc` wraps whichever allocator is
underneath — mimalloc by default — and counts, printing a final line of
allocations and bytes asked for, as `<engine>\tallocs\t<n>\t<bytes>` under
`--tsv`. The count is the same under either, since it counts requests rather
than what serving them cost. Only Rust allocations: a `#[global_allocator]`
does not reach RocksDB's C++ `new`, so this measures roble and encina, and
RocksDB runs under its own allocator whatever the feature says.

A phase's own count is the difference between a run with it and a run without
it, which charges nothing to opening the store:

```sh
base=$(... --phases load,settle)
with=$(... --phases load,settle,get)   # (with - base) / --reads
```

### What it measured

On macOS, one shard, `--no-wal`, five paired interleaved repetitions, mimalloc
against the system allocator:

| phase | ratio |
|---|---|
| load | 1.19× |
| scan | 1.13× |
| scan-rev | 1.09× |
| get | 1.46× |
| floor | 1.09× |
| peak RSS | 0.62× |

The mechanism is one number: mimalloc saves a roughly **constant 12 ns per
allocation** here — 11.6 and 12.2 ns/alloc on `get` at two store sizes, 9.6 and
13.2 on `scan`, 7.6 and 10.7 on `load`, 20.0 on `floor` — and each phase's
ratio is that divided by what the phase costs per operation. Nothing
cache-shaped or locality-shaped is needed to explain it.

Which is why the ratio is not a scoreboard to improve: it says how much a
phase allocates, and the cheaper fix is to allocate less. A prediction worth
recording as wrong — that a store with more tables would show a *bigger* `get`
ratio, since allocations per `get` grew with table count — came out backwards,
1.46× at fourteen tables and 1.27× at twenty-six. The per-allocation saving
held; the baseline cost per lookup grew faster than the allocation count, so
the allocator's share of it shrank.

Two things these numbers are not. macOS's libmalloc is the friendliest
possible baseline, and glibc's per-thread tcache is far closer to mimalloc's
fast path, so **expect less on Linux** — which is where Oxia runs, and which
the Benchmark workflow below is for. And this is one shard on one thread,
where mimalloc's per-thread heaps have nothing to do; a run with many shards
is a different measurement.

## On a Linux runner

Linux is what Oxia runs, and several things here can only be measured there.
compio drives io_uring on Linux and kqueue on macOS, which is a different I/O
path entirely; `O_DIRECT` exists only on Linux (§8); the page cache can
actually be dropped; and a cgroup can hold a store to its budget. macOS also
could not give an honest load row with the WAL on while the Go driver was
here, since Go's `os.File.Sync` is `fcntl(F_FULLFSYNC)` there and compio's is
`fsync` — a trap that left with it, but one that returns the moment a roble
number is set beside a Go Pebble number from anywhere else.

The **Benchmark** workflow runs this on `ubuntu-latest` and posts the tables to
the job summary. It is `workflow_dispatch` only and gates nothing: a shared
runner's disk, neighbours and CPU budget are outside this repository's control,
and a benchmark that could fail a pull request for being 8% slower than a
runner last week would be noise with a veto. Read the ratios between engines
within one run, not a rate against another run.
