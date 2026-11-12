# roble benchmark

Compares roble with Go Pebble on the workload Oxia runs: small records, a store
per shard, batches per write, point and ordered lookups, scans in both
directions, and a checkpoint.

Each run benchmarks one engine in a process of its own, so the CPU time and the
peak memory it reports belong to that engine alone. `run.sh` runs both and
prints them one after the other.

## Why it exists

§14 of [DESIGN.md](../DESIGN.md) leaves several questions waiting on a
measurement, and names milestones that have since passed. The sharpest is
reverse iteration: Oxia's `KeyRangeScanReverse` is on a serving path, an LSM
pays more for `prev` than for `next`, and the design says so without saying by
how much. `scan-rev` is that number and `scan` is its control.

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
- **floor** — `--reads` random probes, each one byte past a stored key so that
  **no probe exists**. That is deliberate: Oxia's `getFloor` tries an exact
  `Get` first and only seeks when it misses, so probing keys that exist would
  measure the `get` phase over again. What this times is the seek, which no
  filter can help.
- **checkpoint** — a checkpoint of every shard, which is how Oxia moves one.

A `levels` line is printed before the scans: how many tables sit at each level.
A scan's cost is mostly how many iterators it has to merge, so without it a
slow iterator cannot be told from a differently-shaped LSM — and on this
workload that distinction turns out to be the whole story.

## Running

```sh
./run.sh --shards 1 --batches 1000 --records 300 --reads 50000 --no-wal
```

Or one engine at a time:

```sh
cargo run --release -p roble-bench -- --dir /path/to/scratch --no-wal
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
compio's is `fsync`, which does not. So with the WAL on, roble appears several
times faster at writing and is really just promising less. On Linux — which is
what Oxia runs — both are `fsync`. The benchmark prints a note when the
configuration is the one that cannot be compared.

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

**Values are pseudo-random.** A value made of one repeated byte compresses to
nothing, and with one the size column reported Snappy saving 2.5× — a
measurement of the generator, not of Oxia's data. Real Oxia values are
serialized records, which sit somewhere between the two, so the size column
here is the incompressible end of the range rather than the answer for any
particular shard.

**`settle` is not comparable row for row**, by construction. roble's does the
compaction work and Pebble's waits for it. Compare the `cpu s` column and the
`levels` line, not the rate.

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
