# roble benchmark

Compares roble with **Go Pebble**, **RocksDB** and **encina** on the workload
Oxia runs: small records, a store per shard, batches per write, point and
ordered lookups, scans in both directions, and a checkpoint.

Each run benchmarks one engine in a process of its own, so the CPU time and the
peak memory it reports belong to that engine alone. `run.sh` runs all of them
and prints them one after the other.

| engine | what it is here for | how to get it |
|---|---|---|
| roble | the subject | always built |
| Pebble | the compatibility target: the same files, the same format version, Oxia's own settings | `harness/bench`, needs Go |
| RocksDB | the engine Pebble replaced, and a second LSM to check a ratio against | `--features rocksdb`, needs a C++ toolchain and libclang |
| encina | a copy-on-write B+tree that owes nobody compatibility — §2 claims Pebble's format is worth its price without naming the price, and this is the price | `--features encina`, fetched from git |

Neither optional engine is a default cargo feature, so an ordinary
`cargo build --workspace` neither clones encina nor needs a C++ compiler.

## Why it exists

§14 of [DESIGN.md](../DESIGN.md) leaves several questions waiting on a
measurement, and names milestones that have since passed. The sharpest is
reverse iteration: Oxia's `KeyRangeScanReverse` is on a serving path, an LSM
pays more for `prev` than for `next`, and the design says so without saying by
how much. `scan-rev` is that number and `scan` is its control.

Two reference LSMs rather than one, because a ratio from a single pair of
numbers is not trustworthy enough to decide anything: the first run of the
reverse comparison had roble at 1.26x and Pebble at 1.44x, which read as roble
being the better of the two. It is not — the forward rates move by a fifth
between runs, and with RocksDB alongside, all three sit in one band of about
1.1x to 1.2x. One comparison says a number; two say whether it means anything.

## Phases

They always run in this order, whatever order `--phases` names them in.

- **load** — writes `--batches` batches to each shard, each batch `--records`
  consecutive records, through an *indexed* batch because that is what Oxia
  uses: its `PebbleBatch` reads back through itself before committing. With the
  WAL on, each commit syncs.
- **settle** — leaves the database in a shape the read phases can be compared
  over. roble compacts cooperatively on the thread that owns the store (§9), so
  this drives the picker to quiescence and the cost is counted here; Pebble
  compacts on threads of its own, so this flushes and waits, and the cost shows
  up in the CPU column.
- **scan-rev**, then **scan** — steps every record of every shard, backward and
  then forward. Reverse goes first on purpose: see *Reading the numbers*.
- **get** — `--reads` random keys, every one present. Oxia's `Get` with
  `ComparisonEqual`, and the one comparison a bloom filter can answer.

  **Each engine reads it the best way it offers, and reports the value's
  length rather than the value.** That is not a detail: every one of them can
  produce a value without handing back an owned copy, and they do it
  differently. roble reads into a `Lookup` the caller keeps; Pebble's `Get`
  borrows the value from the block it is in and gives back a closer; RocksDB
  has `get_pinned`; encina passes the borrowed value to a closure. Asking for
  an owned value charged some of them for an allocation this row is not about
  while others borrowed — which is what it used to do, so **roble's `get` row
  was not like-for-like with Pebble's** until this was fixed. A length still
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
- **floor** — `--reads` random probes, each one byte past a stored key so that
  **no probe exists**. That is deliberate: Oxia's `getFloor` tries an exact
  `Get` first and only seeks when it misses, so probing keys that exist would
  measure the `get` phase over again. What this times is the seek, which no
  filter can help.
- **checkpoint** — a checkpoint of every shard, which is how Oxia moves one.

`run.sh` adds four passes of its own. **Batch against single insert** writes
the same total records in batches and then one record per commit, so that the
per-commit cost can be told from the per-record cost; see the warning about it
below. **Insert with the journal against without it**, which is the WAL's cost per
engine. **The reads in cache and out of it**, at 64 MiB and 4 MiB against 34 MiB
of data — which measures the block-cache *miss* path and not a disk, since all
three LSMs read through the operating system's page cache (§8: direct I/O is
the goal and not the state). A genuinely disk-bound comparison needs a working
set larger than RAM and is a much longer run. **The layout** (`--layout`) prints every table of every level, with its
size and key range, and flags any level below L0 whose tables overlap — which
is what a scan number has to be read against.

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

## One table, all the engines

`run.sh` collects every engine's `--tsv` lines and pivots them with
`pivot.py` into one table — a row per operation and metric, a column per
engine — so the comparison is read across a row rather than by holding four
blocks in your head:

```
                           roble      pebble     rocksdb      encina
scan       rate/s      2,444,142  33,848,584  10,133,423  48,184,380
           p50 us              0           0           0           0
           cpu s            0.12        0.01        0.03        0.01
floor      rate/s        127,139     474,211     488,003   2,392,593
```

A phase an engine cannot do shows `—` in the table and the reason underneath,
never a blank that reads as a zero. Run one engine alone and the aligned
per-run output comes back instead.

## Running

```sh
./run.sh --shards 1 --batches 1000 --records 300 --reads 50000 --no-wal
```

`ENGINES` picks which to run, so the ones needing a toolchain can be left out:

```sh
ENGINES="roble pebble" ./run.sh --no-wal        # nothing to install
ENGINES="roble encina" ./run.sh --no-wal        # adds a git fetch
```

Or one engine at a time:

```sh
cargo run --release -p roble-bench -- --dir /path/to/scratch --no-wal
cargo run --release -p roble-bench --features encina -- --engine encina --dir /path/to/scratch --no-wal
cargo run --release -p roble-bench --features rocksdb -- --engine rocksdb --dir /path/to/scratch --no-wal
cd ../harness && go run ./bench --dir /path/to/scratch --no-wal
```

The store is created at `DIR/roble` or `DIR/pebble`, which must not exist yet.
`--help` lists the options; both engines take the same ones and the same
`--seed`, and both derive their keys, their values and their lookup order from
it with the same generator, so the two hold the same bytes and probe them in
the same sequence.

## Reading the numbers

**`--no-wal` is Oxia's configuration**, and on macOS it is the only one in
which the load row means anything. Go's `os.File.Sync` is
`fcntl(F_FULLFSYNC)` there, which flushes the drive's own write cache;
compio's and RocksDB's are `fsync`, which does not. So with the WAL on, roble
appears several times faster at writing than Pebble and is really just
promising less. On Linux — which is what Oxia runs — they all are `fsync`. The
benchmark prints a note when the configuration is the one that cannot be
compared.

**encina's load row is not quite in the same units.** Its `write` is durable
by itself and takes no sync flag, so `--no-wal` asks roble, Pebble and RocksDB
for less than it asks encina for. That counts against encina on the load row
and nowhere else.

**The phases share a block cache and run in order**, so whichever of the two
scans goes second reads a cache the first one warmed. Reverse runs first
because reverse is the direction under suspicion: a `scan-rev` that keeps up
from there is keeping up from behind. For a comparison with neither direction
favoured, `run.sh` also runs each one in its own process over its own freshly
loaded store, and those are the numbers to quote.

**Step latencies are sampled**, one step in 64. A scan step costs tens to
hundreds of nanoseconds and reading a clock costs the same order, so timing
every step would charge the faster engine the larger share of the measurement
overhead — enough, on the forward scan, to reverse which engine looks quicker.
The rate comes from the phase's own wall clock, which the sampling leaves
alone.

**Every LSM knob is set the same on all three, because their own defaults are
not comparable.** roble took Pebble's defaults (§4, §9), so those two agree
already; RocksDB's differ enough to decide a result rather than inform it, and
the benchmark now sets them from roble's own constants — the memtable, the
target file size, the L0 compaction and stop-writes thresholds, and the base
level's budget. `--memtable-mb` is the one of them exposed as a flag, at 4 MiB:
RocksDB's own default is 64, so left alone it buffered this whole workload and
flushed nothing during the load while the other two wrote 30 MiB of tables.

The target file size was the one that looked decisive and was not. RocksDB's
default is 64 MiB against their 2, which settled a 34 MiB store into a *single*
table where roble leaves eleven and Pebble nine — so a lookup searched one
table instead of three. Setting it to 2 MiB gives RocksDB sixteen tables and
changes its point reads by under 1% on the same machine, because its filters
absorb them. Worth aligning anyway, and worth recording that the layout did
not explain the gap it looked certain to explain. `--no-wal` is another: it takes
`disable_wal` on roble, `DisableWAL` on Pebble and `disable_WAL` on RocksDB's
write options, and setting only the sync flag on the last of those left it
writing a journal the others were not — worth 5 MiB of directory and half its
load rate. `--cache-mb` is the third, divided per store for encina, whose
cache is its own.

**`--value-chunk` dials how compressible a value is**, since the default
values are random and give Snappy nothing to find — so without it every
engine's compression setting is inert and the size column says nothing. A
value is one random chunk repeated to fill it: 128 is incompressible, 8 is a
sixteenfold repeat. roble's size does not move across that range, because its
writer only ever writes `NoCompression`; Pebble's and RocksDB's fall by three
to four and a half times.

**Values are pseudo-random by default.** A value made of one repeated byte compresses to
nothing, and with one the size column reported Snappy saving 2.5× — a
measurement of the generator, not of Oxia's data. Real Oxia values are
serialized records, which sit somewhere between the two, so the size column
here is the incompressible end of the range rather than the answer for any
particular shard.

**The journal rows are the most platform-dependent numbers here, and on macOS
they cannot be compared between engines at all.** Each engine's own on-against-
off ratio is honest about that engine on this machine — it is its own sync both
times — but the ratios are not comparable with each other, because they are not
the same syscall. On this laptop Pebble pays **36x** for the journal and roble
**1.7x**, and almost all of that difference is `F_FULLFSYNC` against `fsync`
rather than anything about either engine. This is the measurement that most
needs the Linux runner below; treat the macOS figures as a demonstration that
the asymmetry is real, not as the WAL's cost.

**The single-insert row is not like for like, and what it shows is expected.**
With `--no-wal` the three LSMs sync nothing per commit while encina commits
each record durably whatever it is asked — so at one record per commit a
copy-on-write B+tree is writing a new root and an fsync behind it, 300,000
times. It drops to 0.006x of its batched rate; that is what the structure
does, not a defect, and it is not a number to set beside three engines that
were not asked to sync. Run without `--no-wal` for the comparison where all
four commit durably. The pass is most useful read *within* an engine: how much
of its throughput depends on the caller batching. roble keeps 0.84x and Pebble
0.94x, because a memtable absorbs a single insert; RocksDB keeps 0.24x.

**`settle` is not comparable row for row**, by construction. roble's does the
compaction work on the thread that owns the store; Pebble's and RocksDB's wait
for background threads; encina has nothing to settle at all. Compare the
`cpu s` column and the `levels` line, not the rate.

**The `levels` line is not enough to explain a scan number, and `--layout` is
the reason this is worth saying.** On this workload roble settles at
`L0=1 L1=13`, Pebble at `L6=11` and RocksDB at a single file, which looks like
all the explanation a scan gap needs. The layout shows it is not: roble's
thirteen L1 tables partition the key space with no overlapping pair, exactly
as Pebble's eleven at L6 do, and a run small enough to leave two tables in
each engine keeps roble 4.3x behind. The shape is not the cost — the per-step
path is. Which is why `--layout` prints the ranges and not just the counts:
the counts invited the wrong answer and were believed for a day.

**`cpu s`, not `rate/s`, is the column to compare** when the engines schedule
work differently. roble compacts on the thread that owns the store; Pebble and
RocksDB use their own; a rate credits the background ones with work they still
paid for.

## Columns

- **count** — records for load and the scans, lookups for get and floor,
  shards for settle and checkpoint. **rate/s** is that count per second of
  wall clock.
- **p50 / p99 / max us** — one operation: a batch commit for load, a sampled
  step for the scans, a lookup for get and floor, one shard's checkpoint.
- **cpu s** — the whole process's CPU time, user and system, background
  threads included. This is the column that makes the two engines comparable
  at all, since roble compacts on the thread that owns the store and Pebble
  compacts on its own.
- **size MiB** — what the store occupies on disk at the end of the phase.

The last line is the process's peak resident memory.

## The allocator

roble is a library, so `#[global_allocator]` is never its choice — it belongs
to whatever binary links it, which for the first consumer is Oxia's data
server. Two features exist to measure what that choice is worth, and they are
mutually exclusive because each installs an allocator:

```sh
cargo run --release -p roble-bench --features mimalloc -- --dir /tmp/b --no-wal
cargo run --release -p roble-bench --features count-alloc -- --dir /tmp/b --no-wal
```

`mimalloc` swaps the process allocator. `count-alloc` wraps the system one and
counts, printing a final line of allocations and bytes asked for — as
`<engine>\tallocs\t<n>\t<bytes>` under `--tsv`. Only Rust allocations: a
`#[global_allocator]` does not reach RocksDB's C++ `new` or Pebble's Go heap,
so this measures roble and encina.

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

macOS cannot give an honest load row with the WAL on: Go's `os.File.Sync` is
`fcntl(F_FULLFSYNC)` there and compio's is `fsync`, so Pebble flushes the
drive's write cache and roble does not. On Linux they are all `fsync`, which
is also what Oxia runs.

The **Benchmark** workflow runs this on `ubuntu-latest` and posts the tables to
the job summary. It is `workflow_dispatch` only and gates nothing: a shared
runner's disk, neighbours and CPU budget are outside this repository's control,
and a benchmark that could fail a pull request for being 8% slower than a
runner last week would be noise with a veto. Read the ratios between engines
within one run, not a rate against another run.
