# roble — design

> Status: **M1, M2 and M3 are complete, and M4 has started.** This document fixes
> the compatibility target (§3), the decisions that differ from encina's (§4),
> and the order the work goes in (§13).
>
> **M1, the format crate.** `roble-format` encodes and decodes Pebble's files,
> each structure pinned against Go Pebble rather than against a reading of it:
> sstables both ways, range deletions, the bloom filter bit-for-bit,
> value-block resolution, none/Snappy/Zstd and both checksums, the batch
> representation byte-for-byte, record framing in both wire formats, version
> edits, and the file and marker names. `roble sst dump` and `roble manifest
> dump` print them.
>
> **M2, reading.** The env, filesystem and disk of §10, with a compio
> implementation and an in-memory one that can fail or crash at any single
> operation. `open`, which reads a database directory — its markers, its
> MANIFEST replayed into a version — and refuses one it cannot read whole (§6).
> Seeking inside a table: internal-key ordering, a block cursor that searches
> the restart array and steps either way without borrowing its block, and index
> descent through one or two levels. The table and block caches of §8, shared
> per thread, so a table is opened once and a block fetched once. And over all
> of it, `get` with the five comparisons of §6 and `scan` in both directions,
> bounded, with point deletes and range tombstones applied.
>
> Each is judged against Go Pebble: whole databases under `tests/golden/` that
> Pebble's own reader describes, seek probes and read probes and scan walks
> that Pebble answers and roble has to reproduce exactly.
>
> **M3, writing**, has started with the **memtable**: a skiplist over an arena,
> keyed by internal key, its links indices rather than pointers so the
> structure needs no `unsafe`, its shape a function of a seed so a run replays
> (§9). This is the first thing roble writes, and so the first place §12's
> model-based tests against a `BTreeMap` are worth having: the oracle is only
> interesting because a skiplist reaches the same answer by a different route.
>
> It takes a **batch**: entry `i` of a batch committed at `s` gets `s + i`,
> which is what makes a later write in the same batch win, and range deletions
> go to a list of their own rather than among the point keys. `write` applies
> one, and **`get` and `scan` see what it wrote** — shadowing a table's older
> value, hiding it with a delete, covering it with a range tombstone.
>
> A scan is a snapshot, and cheaply: it records the sequence number it began
> at and ignores anything above. A write during a scan takes a higher number
> and is invisible to it, with nothing copied and nothing reference-counted.
>
> And the **WAL**: `write(batch, sync)` frames the batch into the log, syncs
> it when asked and only then applies it to the memtable, so nothing is
> readable that is not recoverable. `open` replays the logs the version edits
> leave unflushed, taking each batch's own sequence number, cuts the last log
> where the replay stopped, and carries on writing there.
>
> And **flush**: the memtable written out as a table, the table and the log
> that follows it made durable, and then a version edit appended to the
> MANIFEST — after which the table is the database's and the old log is
> retired. **Collecting what the database no longer refers to** closes that
> last step: a flush deletes the log it replaced, and an open deletes the
> table an interrupted flush left behind. Nothing else goes, and a name roble
> cannot parse is never touched.
>
> And **rotation**, which keeps the MANIFEST from being the thing a reopen
> waits on: past 128 MiB a flush starts a fresh one holding a single snapshot
> edit of the whole version, then moves `marker.manifest.*` to it. The marker
> is the one-way door, and it moves last — the new MANIFEST is a file nothing
> refers to until it does, and the old MANIFEST and the old marker are
> obsolete the moment it has.
>
> And **indexed batches**, which Oxia reads back through themselves before
> committing (§6), so this is not optional. A batch has no sequence numbers
> while it is being built — those are handed out at commit — so each of its
> entries stands in at `SEQ_NUM_BATCH | i`, Pebble's `SeqNumBatchBit`: above
> every number a committed entry can hold, and ascending in the order the
> entries were written. The merge then needs no special case at all, and the
> one rule added is that a number carrying that bit is visible to the scan
> reading through it. The index is a memtable like any other, so the batch is
> a merge source like any other.
>
> And **the WAL off**, which is not an absence but a different place for
> durability to come from, and the configuration Oxia runs: a write reaches
> the memtable and nothing else, `flush` is the only promise the store makes,
> and a `sync` is refused rather than ignored. A log already holding records
> is still replayed, because those writes were acknowledged whatever this
> store has since been told.
>
> Six gates hold it up. **A crash replays** to the last synced write with the
> WAL on, and **to the last flush with it off** — both tested, since the
> second is the one Oxia runs. **Go Pebble replays the log roble wrote**,
> when nothing has been flushed, so every key it finds came out of the log.
> **Go Pebble reads the table roble wrote, through the MANIFEST roble
> appended**, when the same writes have been flushed — the MANIFEST's
> `min_unflushed_log_num` has moved past roble's log by then, so Pebble does
> not replay it and the keys can only have come from the table. **That closes
> M3's round trip.** **Go Pebble reads a database through a MANIFEST roble
> wrote from nothing**, after a rotation. **Go Pebble reads what roble wrote
> with the WAL off**, where the table and the MANIFEST edit are the only
> things that happened. And the last guards the deletions: **Go Pebble reads
> a database roble has deleted from**, after roble opens one and does nothing
> but collect.
>
> **M4, compaction**, has started with the **merge**: tables of one level
> compacted into the level below, pulling in every table there whose range
> they meet, keeping the newest version of each key and dropping the rest.
> Three things may go — a superseded version, a key a newer range tombstone
> covers, and a deletion with nothing left below it to hide — and the third
> is the one that resurrects keys when it is wrong, so it asks the version
> rather than reading the level number. Overlapping tombstones are fragmented
> into the shape a range-deletion block holds, and the output is **split at a
> target file size** — which is what puts a range deletion across a boundary,
> clipped so that the table before it ends at the **exclusive sentinel** and
> the table after begins at that very user key. Two tables of one level may
> meet at a key without overlapping, and roble's own overlap check had been
> reading the bound as inclusive and so would have refused a database Pebble
> wrote. **Go Pebble reads tables roble compacted out of others, meeting at
> exclusive bounds.**
>
> And the **picker**: every level scored against what it is allowed to hold —
> L0 by table count, the rest by bytes — and the worst offender compacted into
> the level below, ties going to the level nearest L0. It reads no clock and
> draws no random number, because a seeded run has to replay (§10), and the
> score is a fraction compared by cross-multiplication rather than a float,
> because making the same decision twice is the whole of its job. **Go Pebble
> reads a database roble spread across levels by itself.**
>
> And a compaction is **cooperative in fact**: it merges a slice, hands the
> executor back and resumes, so a merge of a million keys is a million keys'
> worth of work rather than a million keys' worth of waiting for everything
> else on the core. The slice is counted in entries rather than milliseconds,
> because a job that yielded on a timer would stop a seeded run replaying.
>
> And **metrics**, through OpenTelemetry with [nanometer] handles, as encina
> does: a store creates them when it opens and records nothing at all without
> a meter provider. Writes, lookups, scans, flushes, compactions, what each
> level holds and what the directory takes on disk, with the two counters a
> write amplification needs. [METRICS.md](METRICS.md) lists them, and what is
> still owed: the caches' usage, disk I/O, an estimate of the records held,
> and the two write stalls, which arrive with the waiting they count.
>
> [nanometer]: https://docs.rs/nanometer
>
> How each claim is held up: `compat/sst-roundtrip` has Go Pebble read every
> table roble writes; `harness/golden-sst` and `harness/record-files` go the
> other way, the latter with a real WAL and MANIFEST; `harness/batch-repr`
> compares batch bytes. The golden corpus also holds a table roble must
> **refuse**, so the refusals of §3 are exercised rather than asserted. The
> one part with no Go counterpart is the dump commands' own presentation, and
> their tests check only that they add nothing to what is pinned (§13).

roble is an embedded, ordered key-value store for Rust, built on the
[compio](https://github.com/compio-rs/compio) completion-based runtime. It is a
log-structured merge tree whose on-disk files and whose operations are
compatible with [Pebble](https://github.com/cockroachdb/pebble), so that a
Pebble database and a roble database are the same database.

Its first user is a Rust implementation of [Oxia](https://github.com/oxia-db/oxia)'s
data server, where it replaces Pebble behind the `kvstore.KV` interface. roble
takes encina's approach to threads and I/O — one `!Send` store per thread, its
own cache, all I/O through a trait the caller can implement — and applies it to
someone else's file format.

## 1. Workload

Read from `oxia-db/oxia` at `oxiad/dataserver/database/kvstore/`, which is the
contract roble has to meet.

| Aspect | Shape |
|---|---|
| Keys / values | Keys are byte strings: Oxia's `compare.Encoder` encodes a user key so that bytewise order gives its hierarchical, `/`-aware order. Values are protobuf records, mostly a few hundred bytes. |
| Writes | One batch per replicated write, applied atomically: `Put`, `Delete`, `DeleteRange`. A batch is read back through itself before it commits, so batches are indexed (§6). |
| Reads | Point lookups, and lookups under a comparison: floor, ceiling, lower, higher. Forward and reverse range scans. Prefix iteration that also reads the keys outside the prefix, by seeking. |
| Deletes | Point deletes, and `DeleteRange` over key ranges. |
| Shape of a node | Hundreds of shards per process, one store per (namespace, shard), all sharing one block cache. Memtables are 4 MiB, blocks 16 KiB, bloom filters 10 bits per key. |
| Durability | **Oxia disables Pebble's WAL** (`DisableWAL: !UseWAL` in `kv_pebble.go`: "WAL is kept outside the KV store"). Oxia's replication log is the source of truth; the store need only be durable at `Flush()`, and Oxia replays everything after that. `SyncData` decides whether a commit syncs, for every commit alike. roble takes both as one store setting: `Wal::Synced` by default, and `Wal::Off` is what Oxia sets (§4). |
| Snapshots | `db.Checkpoint(dir)`, whose files are streamed to a follower in 1 MiB chunks and written straight into a fresh database directory, which is then opened. |
| Compression | L0 uncompressed, L1 and below Snappy. No merge operator. |
| Key-encoding conversion | Oxia converts a database to the configured key encoding inside `pebble.Open`, writing the new one with 64 KiB blocks and `GoodCompression`, and leaves an `oxia-key-encoding-format` marker in the database directory — a file Pebble knows nothing about, which `Checkpoint` output then also carries (§3, §12). |

Three consequences shape everything below. First, **snapshots move raw files
between implementations**, so file-format compatibility is a correctness
requirement for a mixed Go/Rust cluster, not a migration convenience. Second,
**Oxia's data path runs without a WAL**, so running without one has to be a
first-class configuration rather than a degraded mode — which is not the same as
roble not having a WAL at all (§4). Third, **the files in a live database were
not all written by the current configuration**: a converted database holds
64 KiB blocks and a compression profile that the steady-state options never
produce, which is why §3 separates what roble reads from what it writes.

## 2. Why compatibility, and what it costs

An LSM is not obviously the right structure for this workload — encina's
copy-on-write B+tree would serve it well, and §2 of encina's design says why.
roble is an LSM because *the files must be Pebble's*, and that decides the
structure. The data structure is a consequence of the compatibility
requirement, not a conclusion about the workload.

What compatibility buys:

- **An existing cluster upgrades in place.** A Rust data server opens the
  database a Go one left behind, with no export, no dual write and no downtime.
- **A mixed cluster works.** Oxia moves a shard by checkpointing it and
  streaming the files; a Go leader can seed a Rust follower and the reverse.
- **The way back stays open.** If roble is wrong about something, Go Pebble can
  open the database and serve it.
- **Existing tooling works.** The `pebble` command inspects roble's files, which
  is also how roble's own tests are judged (§12).

What it costs, stated plainly:

- **The format is someone else's, and it moves.** Pebble ratchets format major
  versions; each one may add a structure roble would have to implement.
- **Bug-for-bug fidelity is the bar for reads.** Writing files Go Pebble accepts
  is the easier half. Reading every file Go Pebble might have written — value
  blocks, two-level indexes, obsolete-key bits, four compression algorithms,
  recycled logs — is the harder one, and a gap shows up as a corruption error on
  a follower, far from its cause. Narrowing what roble *writes* (§3) cuts the
  work; it does not cut this.
- **Some of roble's preferences are not available.** It cannot choose a node
  format that suits its data, as encina does, nor skip a bloom filter it doesn't
  want, without writing files Go Pebble reads differently.

The pin in §3 is what makes this bounded rather than open-ended.

## 3. The compatibility target

roble targets exactly what Oxia asks Pebble for, and refuses the rest.

| Item | Value | From |
|---|---|---|
| Pebble | `github.com/cockroachdb/pebble/v2` v2.1.7 | `oxiad/go.mod` |
| Format major version | `FormatVirtualSSTables` | `kv_pebble.go` |
| Max table format | **Pebblev4** — row-oriented blocks, value blocks, `DELSIZED` tombstones, range keys, virtual sstables. What the format permits, not what roble implements: the table below says which of these it declines | `FormatMajorVersion.MaxTableFormat` |
| Comparer | `leveldb.BytewiseComparator` | default; Oxia sets none for new databases |
| Merger | `pebble.concatenate`, unused | no merge operator is configured |
| Block size | 16 KiB | `kv_pebble.go` |
| Filter | `rocksdb.BuiltinBloomFilter`, 10 bits per key | `bloom.FilterPolicy(10)` |
| Compression, steady state | none (L0) and Snappy (L1+) | `kv_pebble.go` |
| Compression, converted databases | none (L0) and `GoodCompression` (L1+), which at this pin means **Zstd data blocks and Snappy index blocks when the converting node is arm64, and all Snappy otherwise** — because `GoodCompression` embeds `fastestCompression`, that is MinLZ off arm64, and Pebble downgrades a MinLZ-using profile to Snappy whole below Pebblev6 (§3). So the file still depends on the writer's CPU, just not in the way the profile reads | `kv_pebble_formats.go` |
| Block size, converted databases | 64 KiB | `kv_pebble_formats.go` |

**Not in the target**, because the pin excludes them: columnar blocks
(Pebblev5), the footer checksum and blob value handles (Pebblev6), footer
attributes (Pebblev7), WAL sync chunks (`FormatWALSyncChunks`, above the pin, so
a log at the pin uses recyclable chunks), WAL failover to a secondary directory,
disaggregated or shared storage, and **MinLZ**, which Pebblev6 introduces and
which Pebble will not write below it (see the compression note below).

roble refuses a database whose format major version is above its pin, and a
table whose format is above Pebblev4, with a clear error rather than a partial
read. **Treat the pin as part of roble's public contract**: if Oxia raises it,
that is a roble release, not a configuration change.

### What roble reads, writes and refuses

The rule: **roble reads what an Oxia database can contain, and writes the
narrowest set of structures Go Pebble accepts.** Where a structure cannot appear
in such a database, roble refuses it rather than carrying a read path for it —
except where reading costs a couple of match arms and refusing would cost a live
shard, which is the case for `SINGLEDEL` and `DELSIZED`.

| Structure | Read | Write |
|---|---|---|
| sstable, Pebblev4 down to Pebblev1 | yes | Pebblev4 only |
| In-place value prefix byte on `SET` (Pebblev3+) | yes | yes |
| Two-level index blocks | yes | **no**; one index block, however long |
| Value blocks and value handles | yes | **no** |
| Range-deletion blocks, v2 (`rocksdb.range_del2`) | yes | yes |
| Range-deletion blocks, v1 (`rocksdb.range_del`) | **error** | never written |
| Bloom filter blocks (full key, bytewise `Split`) | yes | yes |
| The Pebblev4 obsolete bit | yes, honoured | **never set** |
| Block properties | yes, unknown collectors skipped | the minimum Go accepts (M1) |
| `SINGLEDEL`, `DELSIZED` | yes | never created; a compaction carries one Pebble wrote, a `SINGLEDEL` over an older version as `DELETE` |
| MANIFEST version edits | yes | yes |
| `marker.manifest.*`, `marker.format-version.*` | yes | yes |
| `OPTIONS-*`, `LOCK`, file naming, obsolete-file deletion | yes | yes |
| WAL records and batch repr | yes | yes, recyclable chunks |
| Checkpoint directories | yes | yes |
| Files roble does not recognise | left alone, never deleted | never written |
| Range-key blocks (set, unset, delete) | **error** | never written |
| Virtual sstables (a file entry over part of a backing file) | **error** | never written |
| A comparer other than `leveldb.BytewiseComparator` | **error** | never written |
| MERGE keys | error | never written |
| Columnar blocks, blob files | error | never written |
| WAL failover segments (`NNNNNN-NNN.log`) | error | never written |

**Why range keys, virtual sstables and the legacy comparer are refused rather
than read.** Oxia creates none of them, so a file holding one means an
assumption has broken, and a loud error is worth more than a best-effort read.

- **Range keys.** Oxia calls no range-key API anywhere in the repository.
  Refusing them takes the range-key iterator out of the read path entirely
  (§5, §6), which is the largest single saving on this list.
- **Virtual sstables.** Only `Excise` and `IngestAndExcise` create one, and Oxia
  calls neither; the shard split streams a snapshot the child filters by hash
  range, rather than excising (`SHARD_SPLIT_DESIGN.md`). The MANIFEST reader
  still parses backing-table entries far enough to recognise one and refuse it.
- **`oxia-slash-spans`.** Every Oxia from 0.15 on converts the database when it
  opens it, so a Rust server does not meet an unconverted one. roble asserts the
  comparer name and refuses anything else. That also removes a trap: a bloom
  filter is built over `Comparer.Split(key)`, and `oxia-slash-spans` sets `Split`
  to return 0 where bytewise returns `len(key)` — mixing the two gives a silent
  "not found" for a key that is really there.

**Why the write side is narrower.** Writing a structure means getting it right
or corrupting a Go reader; reading one means getting it right or failing loudly.

- **No value blocks.** Pebble sends a value to one when
  `setHasSamePrefix && valueLen > 0`, where the prefix is `key[:Split(key)]` —
  under bytewise, the whole user key. So a value block appears only where the
  same user key repeats within one output table, which takes an open snapshot
  keeping an older version alive. Rare in Oxia, and never necessary to write.
- **The obsolete bit is never set.** It is an optimization that lets a reader
  skip a key; set it wrongly and Go Pebble skips a live one — wrong results, in
  the other implementation, with no corruption error anywhere. Always-zero is
  correct and legal.
- **No two-level index.** Every table gets a single-level index, one block
  however long, which is valid Pebble at any size. Pebble partitions its index
  once it outgrows `IndexBlockSize`, which defaults to the block size, 16 KiB,
  and Oxia sets neither. A deep table does outgrow it: Oxia leaves
  `TargetFileSizes` at Pebble's default, 2 MiB at L0 and doubling per level, so
  an L6 table runs to 128 MiB, and with the bench's keys its index is 0.46 MiB
  uncompressed and 1.3 to 3.8 MiB under Snappy, where Pebble would write 29 to
  238 partitions. Partitions would buy roble nothing, because its table reader
  fetches every one when it opens a table and flattens them into the one index
  the table cache holds (§8). A two-level file would cost it a read per
  partition at open and save none. Partitions pay only for a reader that loads
  them as it needs them: Go Pebble after a rollback, or a roble reader built
  the same way, which is the change to make before writing them.
- **The value prefix byte is block decoding, not a value-block detail.** In a
  Pebblev3+ table every `SET` value is one prefix byte followed by the value,
  value block or not. A Pebblev2 table has no such byte, so the reader branches
  on table format — and that is why reading the older formats stays in, since a
  long-lived database keeps them in its deep levels.

Compression: roble reads and writes none, Snappy and **Zstd**. It writes them as
Pebble's `NoCompression`, `SnappyCompression` and `ZstdCompression` profiles do
at Pebblev4 (`sstable/layout.go`): data and index blocks compressed, the
filter, range-deletion, properties and metaindex blocks never; a block that
compression shrank by less than 12% stored as built (`MinReductionPercent`);
and the profile name, `Snappy` or `ZSTD`, in `rocksdb.compression`. Go Pebble
reads both back entry for entry (`compat/sst-roundtrip`'s `snappy` and `zstd`
fixtures, each a mix of compressed and as-built blocks).
The block indicator is RocksDB's numbering, not Pebble's internal `Algorithm`
enum: zstd is **7** and MinLZ is 8, while 2 to 6 are zlib, bzip2, lz4, lz4hc and
xpress, which Pebble never writes. Reading 2 as zstd — the obvious guess, since
`internal/compression`'s own enum does number Zstd 2 — gets every Zstd block
wrong.

A Zstd block is `uvarint(uncompressed_len)` followed by a standard Zstd frame
(`internal/compression/zstd_nocgo.go`), RocksDB's convention, which Pebble
keeps. A bare frame would decode happily with a stock reader, so this is a
detail that only fails on real files. roble decodes through libzstd (the `zstd`
crate), into a buffer sized from the prefix, as Pebble's own `DecompressInto`
does, and encodes through it too. It used `ruzstd` to decode until it had to
write Zstd: ruzstd's encoder implements only its fastest level, which is not
the Zstd Pebble writes, and a measurement of Zstd against Snappy wants the
real thing on both sides.

**MinLZ is refused, because it cannot occur at the pin.** Pebble checks whether
the table format supports the compression profile and, below Pebblev6, replaces
the *whole profile* with Snappy — not just the MinLZ part
(`sstable/options.go`: `tableFormatSupportsCompressionProfile`, called from
`ensureDefaults`). Measured at Pebblev4, writing with each profile:

| Profile asked for | Data blocks | Index blocks |
|---|---|---|
| `NoCompression` | none | none |
| `SnappyCompression` | snappy | snappy |
| `ZstdCompression` | zstd | zstd |
| `MinLZCompression` | **snappy** | **snappy** |
| `GoodCompression`, arm64 | **zstd** (one snappy, adaptive) | **snappy** |
| `FastCompression`, arm64 | snappy | snappy |

So Oxia's conversion profile (§1) yields Zstd data blocks and Snappy index
blocks when the converting node is arm64, and all-Snappy when it is anything
else, because there `GoodCompression` embeds MinLZ and is therefore downgraded
whole. Either way no MinLZ block reaches a table at this pin, and a table that
holds one was written by something outside the target (§3).

`compat/minlz` stays. It is cheap, it is a genuine cross-implementation check,
and MinLZ becomes live the moment the pin moves to Pebblev6 — at which point the
`minlz` crate is already known good: 12 payloads at both levels, 24 pairs
decoding byte-for-byte, 588 malformed inputs erroring without a panic. What it
does *not* do is gate M2, which is what this document previously claimed.
**roble's default is Oxia's configuration** (§1): L0 uncompressed, L1 and
below Snappy. `Options::compression` sets what a compaction writes, L1 and
below — none, Snappy, or Zstd at a level; a flush writes L0 uncompressed
whatever it says. §14 has the measurements behind the choice.

### The normative reference

Pebble's Go source at the pinned tag is the specification; there is no other.
Each module of the format crate names the Go file and symbol it implements, so a
reader can check it, and so a Pebble upgrade has a list of things to re-read.
The anchors that matter most:

| roble | Pebble at v2.1.7 |
|---|---|
| internal keys, kinds, trailers | `internal/base/internal.go` — `InternalKeyKind*`, `InternalTrailerLen = 8` |
| footer | `sstable/table.go` — `rocksDBFooterLen` (53 bytes), `pebbleDBMagic` |
| table formats | `sstable/format.go` — the (magic, version) tuple per format |
| block trailer | `sstable/block` — `TrailerLen = 5`: one compression byte, four checksum bytes |
| format major versions | `format_major_version.go` — `MaxTableFormat` |
| version edits | `internal/manifest/version_edit.go` |
| markers | `internal/base` and `vfs/atomicfs` |
| batch repr | `batchrepr` — 8-byte sequence number, 4-byte count, then entries |
| log records | `record/record.go` — 32 KiB blocks; three wire formats, `legacyHeaderSize = 7` (the MANIFEST), `recyclableHeaderSize = 11`, the extra four bytes being the log number (the WAL at the pin), and `walSyncHeaderSize = 19` (above the pin, refused) |
| bloom filter | `bloom/bloom.go` — RocksDB's full-file filter: 64-byte lines, then one probe-count byte and a u32 line count. Its hash folds the trailing one to three bytes in **sign-extended**, `uint32(int8(b))`, to match RocksDB; unsigned gives different bits for any key whose tail is ≥ 0x80 |
| metaindex names | `sstable/table.go` — `rocksdb.properties`, `rocksdb.range_del2` (and `rocksdb.range_del` for the refused v1), `pebble.range_key`, `pebble.value_index`, `fullfilter.<policy>`. None of the range ones is spelled the way its feature is |
| WAL file names | `wal/wal.go` — `makeLogFilename`: `NNNNNN.log`, and `NNNNNN-NNN.log` only under failover |
| EOF trailer | `record/log_writer.go` — a closed log ends with an eleven-byte recyclable header whose checksum is **zero** and whose log number is one greater than the file's. A reader must test the log number *before* the checksum, or every closed log looks corrupt at its end |
| unflushed logs | `internal/manifest/version_edit.go` — `MinUnflushedLogNum`, which says what a reopen must replay |

Key kinds roble handles: `DELETE` 0, `SET` 1, `SINGLEDEL` 7, `RANGEDEL` 15,
`SETWITHDEL` 18, `DELSIZED` 23, and the Pebblev4 obsolete bit, 64. Refused:
`MERGE` 2, and the range-key kinds `RANGEKEYDEL` 19, `RANGEKEYUNSET` 20 and
`RANGEKEYSET` 21. `SINGLEDEL` and `DELSIZED` are read but never created: Oxia
creates neither, and reading them costs two match arms where refusing them would
cost a dead shard if Pebble ever produced one on its own. A compaction does
write one it read, when the key has something below the output level for the
deletion to hide, as it would any other deletion; `tests/read.rs` and
`compat/wal-replay --kinds` check that Pebble's answers over those tables, and
after Pebble compacts them, are the ones it gave over its own. And a `SET`
that a flush or a compaction writes over an older `DELETE`, `SINGLEDEL` or
`DELSIZED` of its key, which it drops, is written as `SETWITHDEL`, as
Pebble's `setNext` does. No read can tell the two apart; a later
`SingleDelete` can, when Pebble compacts it next. One that meets a
`SETWITHDEL` acts as a Delete. One that meets a plain `SET` cancels only that
Set, and an older value still in a deeper level comes back (`read-probes
single-delete`). For the same reason a `SINGLEDEL` written over any older
version of its key that the flush or compaction drops is written as a
`DELETE`. Left a `SINGLEDEL`, Pebble would let it cancel the next Set below
and uncover the one under that; Pebble's own `singleDeleteNext` turns one
that meets a deletion or a `SETWITHDEL` into a `DELETE` as well (`read-probes
stays-deleted`). A `SINGLEDEL` with nothing older beside it stays one.

## 4. Decisions

| Topic | Decision |
|---|---|
| Format | Pebble's, pinned at §3. roble defines no format of its own, and has no format version of its own. |
| Durability | **A WAL, synced by default.** A write appends the batch repr to the log as one record, syncs it if the store's `Wal` setting is `Synced`, and then applies it to the memtable; `flush()` writes a table, installs the version edit, and retires the logs that edit covers. The setting is the store's and not each write's, as Oxia configures Pebble: `Wal::Synced` (the default), `Wal::Buffered` (appended, left to the OS to sync), or `Wal::Off`, which skips the log entirely and is what Oxia sets: then `flush()` is the only durability point. A WAL is an integral part of an LSM rather than a feature of this workload, and roble has to read Pebble's logs regardless, so it is in v1 (M3) rather than deferred — but the no-WAL path is tested as a first-class configuration, because it is the one Oxia runs (§1). |
| Threading | One `!Send` store per thread, as encina: a compio runtime's, or whatever runs an env the caller implements. No locks or atomics outside wakers. Oxia already shards; a shard's store is owned by one thread. |
| Memory | **Caches belong to the thread, not the store** (§8): the block cache and the table cache are created per thread and shared by every store on it, within one budget. This differs from encina, where each store has its own, and follows Oxia, where hundreds of shards share one cache. No `mmap`. Direct I/O is the goal and not yet the state: it needs aligned frames under the block cache (§8, §10), so v1 reads through the page cache, as Pebble does. |
| Compaction | Cooperative on the owning thread by default, paced in units of work as encina paces a large merge, so a seeded run replays. Offload to a pool is a capability of the env, not a default (§9). |
| Allocation | No allocation on the read path for an uncompressed block, which is cached as read — every L0 block. A compressed block, L1 and below by default, costs a second buffer per miss: the read, which is dropped, and the block it decompresses into, which the cache keeps. Batches, iterator state and compaction buffers reuse their memory. |
| Keys and values | Byte strings, ordered bytewise. Limits are Pebble's, not roble's. Values are written in place; roble reads the value blocks Go writes but produces none (§3). **No custom comparator at all**: roble asserts the comparer name in the MANIFEST is `leveldb.BytewiseComparator` and refuses the database otherwise, `oxia-slash-spans` included. |
| Snapshots | A snapshot pins a version and a sequence number, as Pebble's does. `checkpoint()` produces a directory Go Pebble opens. |

## 5. Architecture

```text
write(batch) ─► WAL append ─► memtable (arena skiplist) ──► flush ──► new sstable ──► version edit
                 (sync?)             │                                                     │
                 NNNNNN.log          └──► immutable memtables                              ▼
                                                                                   MANIFEST + marker
open ─► replay the logs the MANIFEST leaves unflushed ─► memtables                         │
                                                                                           │
get / scan ─► iterator tree ─► memtables, L0 sublevels, L1..L6 ─┐                          │
                                                                ▼                          ▼
                                                    table cache ─► block cache ─► file I/O
                                                   (open readers)   (per thread, budget)
compaction ◄── picker (scores) ──► merge inputs ──► new sstables ──► version edit
```

- **The WAL is one log file at a time**, `NNNNNN.log`, holding batch reprs in
  record chunks. A write is logged before it is in a memtable, so nothing is
  readable that is not recoverable; a memtable rotation rotates the log, and a
  flush retires every log the version edit's minimum unflushed log number leaves
  behind. With `Wal::Off` none of this exists and `flush()` is the only
  durability point (§4, §6).
- **Memtables are skiplists over an arena**, keyed by internal key (user key,
  sequence number, kind). Range deletions are kept apart from the point keys,
  as in Pebble, so a point read never walks tombstones it doesn't need. Pebble
  gives them a skiplist of their own; roble keeps them in a list sorted by
  start key, which serves that reason at a memtable's scale — a 4 MiB arena
  holds few tombstones, and they are searched rather than walked. A second
  skiplist replaces it the day a workload shows it needs one. **A node is
  one record in the arena** — header, forward links, key — so a hop through
  the list lands in one place, where it used to touch four: the node, its
  links, the next node and that node's key. Searching a memtable is what a
  read pays before it reaches any table, and once a memtable outgrows the CPU's
  caches each of those four places is a trip to memory: at the 240,000 entries
  YCSB B leaves in a 64 MiB memtable, it was a third of every operation. A seek
  is now 1.55x faster at 60,000 entries and 1.23x at 240,000, and YCSB B and D
  in cache 1.19x and 1.26x (`bench/`, one core and 512 MiB). A store that has
  reached `Options::memtable_size` — 4 MiB, Pebble's `MemTableSize` — writes a
  memtable out on the next write, before that write's batch is applied, and the
  arena is released when its table is written. That is **the only thing that
  bounds a store's memory**: nothing else writes a memtable out unless a caller
  asks (§6).

  **The budget is the backstop, and the line sits an eighth below it.** A
  write that pays for a flush waits for a table to be built, written and
  synced, which at 64 MiB is a tenth of a second. So a store with a task
  waiting on `Db::flush_wanted` is told there is a memtable to write out
  at seven eighths of the budget; that task freezes it and writes it out
  while the writes carry on into the fresh one behind it, and the last eighth
  is what they carry on into. The budget itself is unchanged and still counts
  **every** memtable, the frozen one included — a store whose flushes stop
  keeping up fills the reserve too, and then the write that finds it full
  writes a memtable out itself, exactly as before. The swap is bounded
  because the budget is.

  An eighth and not a half. Two memtables sharing the budget evenly halves
  the tables a flush writes, and halving them doubles how often L0 reaches
  the depth the picker compacts it at: a third of a store's write throughput,
  measured, for room nothing uses. The memtable being written out is the one
  that filled; the one in front needs only the writes of a single flush.
  roble cannot do better than a fraction here, because how long a flush takes
  is a question about the clock and §10 does not let a store ask it — and the
  two move together anyway, since a larger budget means a longer flush *and* a
  larger eighth. A store with nobody to hand a flush to never reaches the line
  for any purpose, so it keeps the whole budget for one memtable and writes
  the tables it always did.
- **Versions are immutable, and reference counted.** A version is the set of
  files per level, held as an `Rc<Version>`. A flush or a compaction produces
  a version edit, which is appended to the MANIFEST and then applied — into a
  copy when anything else holds the current version, so a reader holding an
  older version is unaffected, and a scan or a snapshot costs one reference
  count, as an encina iterator does. The sweep below keeps every table a
  held version names: when an edit replaces a version something still holds,
  the store keeps a `Weak` to it (`crate::obsolete::Pinned`), and the sweep
  takes in the tables of each one still alive. That is Pebble's rule — it
  counts the version and collects a file once no live version names it —
  and RocksDB's `SuperVersion`. The count used to be kept per **table**
  instead, by a scan holding an open reader for every table it might read,
  which made every scan open all of them up front (§6).
- **Files are immutable, so there is nothing to lock.** A block is found through
  the table cache (an open reader per file, with its index and filter) and the
  block cache (decoded blocks, keyed by file number and offset). A
  **compaction consults the block cache but does not fill it**: it reads every
  block of its inputs once and never returns, so leaving them behind would
  evict what reads depend on and replace it with bytes nobody will ask for
  again. Pebble draws the same line, reading a compaction's blocks into a
  buffer pool instead of the cache.
- **Reads assemble an iterator tree**: one iterator per memtable and per level,
  merged by internal key, with range-deletion iterators layered over them, and
  the sequence number of the snapshot deciding what is visible.
  This is the part with no encina counterpart and the most room to be slow;
  §6 says what it must not do.

  A scan opens **only the tables the merge reaches**: each level below L0
  is one lazy iterator over the stretch of its tables the scan's bounds
  reach, as Pebble's `levelIter` and RocksDB's `LevelIterator` are; each L0
  table is a level of its own, one table long. A seek lands in the one table
  of a level that can hold its target, and when that table lies wholly past
  where the level is going — or a step runs off the end of one table into
  the next — the level does not open it but waits there, standing in the
  merge with the table's bound from the version, which no entry of the table
  passes. The table is opened only if that bound reaches the top of the
  heap. So making a scan costs a binary search a level and opens nothing,
  and positioning it opens at most one table a level, and none in a level
  whose landing table cannot hold the answer. The version it holds is what
  keeps the tables it has not reached yet on disk, and the price is
  Pebble's: a scan over a handful of keys keeps every table of its version
  from the sweep for as long as the cursor is open. Range deletions come
  from the table each level last opened, as Pebble's come from each level's
  current file: a tombstone lies within its own table's bounds, and since a
  level does not open its next table until the merge reaches that table's
  bound, the table it has open is the one whose tombstones can cover the key
  being decided. Pebble holds a level on its table the same way, by
  interleaving the tombstones' bounds among the point keys.
- **One change at a time to what a store is made of**, and it is enforced
  rather than hoped for (`crate::change`). A flush, a compaction, a MANIFEST
  rotation and the obsolete sweep each take a permit first, and
  `append_version_edit` will not take an edit without one — so a path added
  later cannot forget to ask. The rule is stronger than "one flush and one
  compaction", which is what §5 used to say: each of these reads the version,
  does I/O, and then writes an answer derived from what it read, and two of
  them interleaved at an await produce a database that does not open. Two
  flushes take the same file number and write the same table over each other;
  a sweep that lists the directory between a flush's table and that flush's
  edit collects the table as an interrupted flush's leftovers.

  A **compaction** holds it to decide what it will read and to record what it
  wrote, and **not for the merge in between** (§9). A merge is the longest
  thing a store does, and a permit held across it is one every flush waits
  out — being cooperative inside it does not help, because what a waiter
  wants is the permit and not a slice of the thread. What stands in for the
  permit while a merge runs is a **claim** on the tables it is reading
  (`crate::change::Compacting`, which is Pebble's `compacting` flag on a
  file): a second compaction the picker points at the same tables stands
  down, rather than merging them twice and having its edit delete tables the
  first edit already deleted. Standing down rather than waiting is the point,
  because the caller asking is usually a flush.

  Two of a compaction's reads of the version are what the two holds are for.
  Deciding reads it to choose inputs; numbering the outputs reads it for
  `next_file_num`, which the edit then spends — so those two and the edit are
  one hold, and anything that read the same version in between would have
  read the same numbers. The key ranges of the levels *below* the output are
  read in the first hold and used in the second, which is sound because a
  compaction running meanwhile only moves keys downward: the ranges can be
  read too wide, which keeps a tombstone that could have gone, and never too
  narrow, which would drop one that was still hiding something.

  A **write** does not take the permit, and must not: a write would be
  queueing behind every flush, rotation and sweep the store does, and a write
  is the one operation that never stops. What makes a
  write safe against a flush instead is that the flush **freezes** the
  memtable before its first await, which is Pebble's answer and the machinery
  the memtable write stall will need (§9). Writes from then on go to a fresh
  memtable and, with a WAL, a fresh log, so the table being built holds
  neither of them and the log the edit retires holds neither. A read looks in
  both memtables, and the frozen one stays readable until the edit naming its
  table has landed.

  Freezing *waits*, which is the part that is easy to miss. A write appends
  its record to the log, waits for that, and only then applies it to the
  memtable — the order that keeps anything readable recoverable (§6). A flush
  that froze in between would build its table from a memtable the write had
  not reached yet, and retire the log holding the record: the write returned
  `Ok`, and both copies of it are gone. So there is **one write at a time**,
  `crate::change::Writers`, and a freeze does not swap until no write holds
  it.

  That permit is also what keeps two writes off each other, and that is the
  same fact rather than a second one. A write reads the sequence number it
  will stamp and the offset it will append its record at, waits for the disk,
  and only then applies to the memtable. Two writes overlapping there read
  the same of each, and both go wrong: the second's record lands on top of
  the first's, so `write` returned `Ok` for a record the next open has no
  trace of; and both batches carry the same sequence number, so two entries
  for one key become one, a snapshot cannot tell the two writes apart, and
  the MANIFEST's last sequence number is a write short of what the memtable
  holds. Pebble serializes the same span for the same reason — its WAL append
  happens under `commitPipeline.mu`, with only the memtable apply left
  concurrent. roble has one thread, so there is nothing to gain by splitting
  the two, and a write holds the permit from end to end.

  A freeze is the one part of a flush that a failure does not undo: the store
  is writing to the fresh memtable and the fresh log from then on, whatever
  becomes of the rest. So the new log's file number is spent at the swap
  rather than at the edit, and a flush that fails leaves a memtable frozen for
  the next flush to write out rather than freeze over. One flush writes one
  memtable, which is why a flush does not always leave `unflushed_bytes()` at
  zero.
- **A level's tables may touch but not overlap.** Two tables of one level may
  share a user key at their bounds when the first ends there *exclusively* —
  its largest key being a range deletion's end at the largest sequence number
  there is, which a tombstone excludes. This is Pebble's rule from
  `manifest.CheckOrdering`, and it is the ordinary result of a compaction
  split inside a range deletion, so reading the bound as inclusive refuses
  databases Pebble writes.
- **Obsolete files are deleted only after the edit that retired them is
  durable**: at the end of a flush, and at an open, where the MANIFEST just
  replayed is as durable as it gets. A table goes when the current version
  names it at no level, **no version a scan or a snapshot holds names it,
  and no reader is open for it** — the table cache's
  own reader is let go of first, as Pebble evicts from its table cache at the
  same point, so that a file waits for whoever is reading it rather than for
  cache pressure; a log goes when its number is below
  `min_unflushed_log_num`; a MANIFEST goes when the manifest marker names a
  different one; a marker goes when a higher iteration of a name roble moves
  is there; everything else stays, including every name roble cannot parse. The directory is **not** synced afterwards, as Pebble does not
  either — an unlink a crash undoes brings back a file that is still obsolete,
  and the next sweep finds it again. Getting this wrong either leaks files or
  makes a reader read a deleted one, so it is checked by the invariant checker
  after every step (§12).

## 6. Operations

Each maps to a method of Oxia's `kvstore.KV` or `kvstore.WriteBatch`.

### `write(batch, sync)`

A `WriteBatch` encodes operations into **Pebble's batch repr** — an 8-byte
sequence number, a 4-byte count, then kind/key/value entries — in an arena that
`clear()` empties without releasing. Using Pebble's encoding rather than one of
roble's own is deliberate: Oxia checksums the batch bytes and compares that
checksum across replicas, so a Go batch and a roble batch of the same operations
must be the same bytes.

That encoding pays twice over, because **the WAL record is the batch repr**: the
bytes Oxia checksums are the bytes appended to the log, with no re-encoding
between the batch and the file. A batch hands those bytes back through
`as_bytes()` rather than a checksum of its own: Oxia's `Checksum` is a
*running* CRC-32 Castagnoli that chains one batch onto the next and is masked
only when it is finally read (`oxiad/common/crc`), and that convention belongs
to the replication protocol rather than to a store.

Two details of that contract are measured rather than assumed. Oxia checksums
the **whole** repr, header included (`db.go`: `batch.Checksum(previousChecksum)`),
and it does so **before** `Commit`, so the eight header bytes that hold the
sequence number are still zero at checksum time — Go Pebble's repr for an
empty batch is twelve zero bytes. roble therefore leaves the sequence number
zero until commit stamps it, and stamping it touches nothing but the header.
And which kinds carry a value is not inferable from the kind: `SETWITHDEL`
does not, while `DELSIZED` does (`batchrepr.Reader.Next`).

A write assigns sequence numbers, appends the repr to the log as one record,
syncs the log if the store's `Wal` setting is `Synced`, and then applies the
entries to the memtable — in that order, so nothing is ever readable that a
replay would not recover. One write at a time, because that span is where the
sequence number and the log offset are both chosen and both committed to (§5).
With `Wal::Off` the log step is skipped and durability is `flush()`'s job alone
(§4).

**Durability is the store's, not each write's.** Oxia configures Pebble once
— `UseWAL` and `SyncData` — and makes every commit with those write options,
so a per-write flag would be one argument on every call that Oxia never
varies. It would also have one combination that means nothing: a synced write
with no log to sync, which an earlier version refused at run time, as Pebble
does (`DB.Apply`: "pebble: WAL disabled"). As one `Wal` setting with three
values there is nothing to refuse.

A write that finds the store **at** `Options::memtable_size` writes a memtable
out before applying its own batch, which rotates the log with it (§5). Pebble
asks instead whether the batch *fits*, and so never exceeds the size; roble's
arena has no capacity to run out of, so the batch that crosses the line is
applied whole and the bound is the budget plus one batch. The looser rule needs
no estimate of what a batch will cost the arena, and no second path for a batch
larger than the whole budget. Every memtable counts toward the budget, one a
failed flush left frozen included, so the write after such a flush is the one
that retries it.

Before the batch rather than after, and that is the whole of the ordering: a
flush that fails fails a write that has not happened, so a caller handed the
error can write the same batch again. The other way round the batch would be in
the log and in the memtable and reported as failed, and a caller doing the
obvious thing would write bytes the store already had. The flush is the
caller's to pay for, in the write that asked for the room, and its time is in
`roble.write.duration` because that is what the write cost. It is not a stall —
nothing is waiting, the work is being done. The stalls of §9 are the writes
that wait for somebody else's flush, and are still owed.

roble writes recyclable chunks, as Go does at the pin, but **does not recycle log
files**: a rotation creates a new one. Reading a recycled file is still required,
since Go recycles, and that is what the log number in the chunk header is for.

**Indexed batches.** Oxia reads a batch back through itself (`Get`, `FindLower`,
`KeyRangeScan`, `RangeScan` on the batch) before committing it, so a batch keeps
a skiplist index over its own repr, and its iterators merge the batch with the
store's view. This is Pebble's indexed batch, and it is not optional.

`put_with(key, len, |buf| …)` reserves space in the arena and lets the caller
write the value into it, which is what Oxia's `PutMarshalable` needs to marshal
protobuf once instead of twice.

### `get(key, comparison)`

Five comparisons, as encina: equal, floor, ceiling, lower, higher. Equal is a
point lookup — bloom filter per table, then one block read. **A table no newer than
the best version found so far is not opened at all**: its largest sequence
number bounds everything in it, so nothing there can beat that version or
delete it. A memtable holds only writes no table has taken yet, so a key a
memtable holds is answered without touching a table — what Pebble and RocksDB
get by stopping at the first source that has the key, here from the sequence
numbers rather than from the order the sources are searched in. It is the
whole difference for a key that is read because it was just written: YCSB A
in cache ran 1.75x faster for it, and B 1.33x (`bench/`, one core and 512 MiB).
**Nor is a table searched that a covering range tombstone makes too old.** The
lookup takes the newest tombstone covering the key from the memtables, and
then from each table it opens, newest first and before that table's filter;
a table no newer than it holds only versions it deletes. So a get of a
deleted key reads no data block for it, as RocksDB stops at its
`max_covering_tombstone_seq` and Pebble's `getIter` at the first covering
tombstone. The other four are a
seek: `SeekGE` for ceiling and higher, `SeekLT` for floor and lower, over the
full iterator tree, since the answer may be in any level.

A floor lookup is where an LSM is worst and where Oxia leans hardest: unlike
encina's single descent, it probes every level, and a tombstone may send it
further back. The iterator tree must therefore skip whole blocks by index key
rather than walking entries, and must not read a table whose key range excludes
the seek.

Reads lend their key and value to a closure, as encina's do, so a cached
uncompressed block is read without copying.

**As built, `get` returns the key and value owned rather than lending them.**
The closure exists to avoid copying out of a cached block, and there is no
block cache yet: every read today fetches the bytes afresh, so the closure
would hand out a borrow of something about to be dropped and save nothing. It
arrives with the cache, which is also when it starts to pay.

Neither search can now fail to make progress, and not by checking: a scan's
step always advances at least the iterator whose key it just returned, so the
loop that skips deleted keys ends when the iterators do. The earlier version
did need a check — it re-derived its bound each time round, and a table whose
keys disagreed with the bounds its MANIFEST entry gives it would have spun
forever. Structure is a better guarantee than a check, and this is the kind of
thing worth saying out loud when a guard disappears.

**The four ordered comparisons are a scan, positioned once and read.** They
were their own merge for a while, re-seeking every candidate table once per
key they rejected, so a long run of deleted keys cost a seek per table per
key. A scan steps instead, and skipping deleted keys is its job rather than
`get`'s. Equal keeps its own path, because it is the one comparison a bloom
filter can answer and a scan does not consult filters — it is asking about a
range.

(A table is no longer read whole. The table cache holds an open reader with
its footer, index, filter and tombstones, and data blocks are fetched one at a
time by handle.)

**Building the scan costs a seek a level, not a table.** It used to cost
more than reading it. Bounded only on the side the comparison decides, a
`floor` scan reached every table that starts below the probe — half the
store, around a hundred and twenty tables at six million records — and opened
each, built it an iterator and copied it a bound, because holding a table
open was how a scan kept it from the sweep. A profile on one pinned core put
**52.6%** of a `floor` in building that scan and **32.7%** in the seek that
answered, which touches one table a level.

Pebble and RocksDB avoid the cost the same way, and roble now does too. Each
pins a version by reference count when an iterator is made (`readState`,
`SuperVersion`) and gives every level below L0 a lazy iterator that opens only
the table a seek lands in (`levelIter`, `LevelIterator`), so making any
iterator costs a few allocations however many tables the store holds, and
neither has a path special to `floor`. A roble scan holds an `Rc<Version>`,
the sweep keeps every table a held version names (§5), and a level opens a
table only when the merge reaches it (`crate::scan`).

**Opening only when the merge reaches it is what made `floor`'s window
redundant**, and opening on landing alone did not. #134 had `floor` and
`lower` scan a window just below the probe first, inside one table of every
level, falling back to the half-open scan only when nothing in the window was
visible; that took `floor` from 66,069 to 210,140 a second (3.18x), 0.93x of
RocksDB. Lazy levels that opened every table a seek landed in made building
the half-open scan cheap — 8.3% of a lookup down to 4.0% — but ran `floor` at
0.77x of the window, because the seek grew from 62% to 73%: L1 held nineteen
tables scattered over the keyspace at six million records, against L2's 189,
so a backward seek usually landed in one ending far below the probe and read
and decoded its last block for a key another level beat. The window had
skipped that seek by starting past such a table. A level that waits at the
table instead, with its bound in the heap, skips it for every scan in either
direction, so the four ordered comparisons are one scan each again and the
window is gone.

Measured over the same six-million-record store on one pinned core, five
rounds alternating against the build with the window: `scan-short` went from
25,059 to 44,232 a second (**1.77x**) and YCSB-E from 25,540 to 43,448
(**1.70x**), which is 0.48x to 0.85x and 0.50x to 0.85x of RocksDB measured in
the same session. `floor`, with no window, was **1.03x** of the window in every
round, level with RocksDB. `get`, which builds no scan, and a full scan were
unmoved (1.00x, 1.02x).

**A kept `GetBuf` makes an ordered comparison allocate nothing.** A scan has
more to build than a point lookup — a source a level and a memtable, a heap,
its bounds, the key it seeks to, a block cursor a table it opens — and a
`floor` built one and dropped it on every call, 24 allocations, 30 for a
`ceiling`. A `GetBuf` now keeps one scan parked between calls and rebuilds it
in its own allocations, so once warm the four ordered comparisons allocate
nothing, as `Equal` already did (`tests/allocations.rs`). Parked, it holds
nothing of the store — no version, table or memtable — so keeping the buffer
keeps no file from the sweep.
### `delete_range(start..end)`

A range tombstone, written to the memtable's range-deletion skiplist and then to
a table's range-deletion block — **only** there, never interleaved with point
keys, so a reader that walks data blocks alone sees a table with tombstones as
though it had none. Within the block they are fragmented: sorted by start key,
with equal starts sharing an end and carrying strictly decreasing sequence
numbers, which Pebble enforces when writing and roble enforces in both
directions. Unlike encina, which drops covered subtrees and
leaves nothing behind, every later read in that range checks the tombstone until
compaction drops it. That is the cost the format chooses for us (§2). What a
scan does not pay is a step for each key the tombstone hides: once it decides
one, every level the tombstone wholly hides is sought past the range in one
move, as Pebble's merging iterator does (`crate::scan`). Wholly hidden is the
level order for a tombstone in L1 or below — every deeper level's keys in its
range are older — and the version's sequence numbers for one in a memtable or
L0, whose tables below are checked to be older than it. So a floor or a scan
crossing a deleted range costs a seek a level, where it once stepped and read
through every key of the range. A memtable's keys are still stepped: it
records no sequence numbers to skip them by, and holds a memtable's worth. A
point lookup of a deleted key stops at the tombstone in the same way, reading
no block of the tables it makes too old (`get` above).

### Iterators

Forward and reverse, bounded, over a snapshot. `seek_ge` / `seek_lt` /
`next` / `prev`.

**The snapshot is a sequence number.** A scan takes the highest sequence
number in use when it is made and ignores anything above it, so a write that
happens while it is open is simply not its business. Nothing is copied and
nothing is reference counted: sequence numbers only go up, and a table's
contents are all older than any scan, so the rule has to be applied in exactly
one place — the memtable. An explicit `snapshot()` handle keeps that number
and holds the version with it, which is below. Reverse iteration over an LSM is more expensive than forward,
which Oxia's `KeyRangeScanReverse` uses; it is worth measuring early.

There are two reasons it is dearer, and they compound. Within a block,
prefix compression only runs forwards, so stepping back means rescanning from
the restart point before the current entry. Across blocks, a backward step at
the start of a block has to load the previous one and walk it to its end,
where a forward step loads the next and stops at its first entry.

**The rescan happens once per restart interval, not once per step.** It
decodes every entry from the restart point to the one before the cursor, and
the cursor keeps them, so each later step back within the interval copies a
key it has already assembled; walking to the end of a block keeps the last
interval the same way. Rescanning on every step was two thirds of a reverse
scan over a store in cache, and held it to 0.63x RocksDB's; keeping the
entries doubled it, to 1.39x, and past memory from 0.63x to 1.22x. Pebble's
`blockIter` and RocksDB's `DataBlockIter` keep the same entries.

**The memtable steps back by a link.** A skiplist's links run forwards, so a
step back over the memtable searched from the top for the predecessor of a
copy of the current key: a descent and an allocation per entry, three
quarters of a reverse scan over a memtable of 200,000 entries. Each record
also holds the record before it at level zero, as Pebble's `arenaskl` does,
and a step back follows that. The scan went from 5.0M entries a second to
7.4M, 1.47x, for four bytes a record; forward scans, loads and YCSB A did not
move (`bench/`, one core and 512 MiB).

**A snapshot is the number and the version.** A scan holds a version too
(§5), but it is a cursor, bounded, and a lookup is not something a cursor
does. A snapshot is what a caller says when it means to read the whole store
at one instant: it holds the version, the memtables and the sequence number,
and opens nothing when it is made. Holding the version keeps the sweep off
every table it names; a lookup opens the tables whose bounds contain its key,
and a scan of the snapshot the tables its seeks land in, as a store's scan
does. It used to open every table of the version up front, because holding
the readers was how it kept the files.

That pair — a bounded cursor over one instant, and a lookup for any key at all
over the same instant — is Oxia's `KeyPrefixIterator` with `Lookup`, which it
uses to read a value while scanning an index. The lookup is a point lookup and
not a scan positioned once, so that the bloom filters still rule out the tables
that cannot hold the key; Oxia arrives at the same place from the other side,
seeking a cloned iterator with `SeekPrefixGE`. Filters answer about point keys
only, so a range tombstone is searched for without them.

The prefix itself is not roble's. Oxia's goes through its own key encoder,
which can map one prefix to several ranges, and a plain byte prefix is the
range `[p, p+1)`. roble offers the view and the range; the bounds are the
caller's to compute from its own encoding.

Iterators read ahead, as encina's do, and share a read with another iterator
that wants the same block (§8).

### `flush()`, `snapshot()`, `checkpoint(dir)`

A flush writes in an order chosen so that a crash in the middle of one changes
nothing: the table and its name are made durable, then the log that will follow
it, and only then is the version edit appended to the MANIFEST and synced.
Until that last step the new table is a file nothing refers to and the old log
still holds every write, so a crash leaves the database exactly as it was.
After it, the table is the database's and the rest is bookkeeping a replay
would redo. The one-way door is the MANIFEST write, and it is one record.

A table's **key bounds and sequence-number bounds are not the same thing**. A
range deletion's upper bound is the sentinel — its end key at the largest
sequence number there is — which bounds the keys and is not a sequence number
the table holds. Pebble's own tables show the split: `seq=15..17` beside a
largest key of `#72057594037927935 rangedel`. Folding the sentinel into the
sequence range produces a table claiming a sequence number above anything the
MANIFEST ever committed, which is what §6's version check refuses.

**A flush writes the newest version of each key and leaves the rest out**, as a
compaction does and for the same reason: nothing can ask the table for an older
one. A read of the store as it stands wants the newest, and a scan or snapshot
made before the flush holds the memtable itself rather than the table written
from it, so a flushed table is only ever read at a sequence number past every
entry in it — the snapshot that could sit between two versions is one that
never reads the table. Pebble keeps a version for each snapshot open across
its flush, because its snapshots read the current tables at an old sequence
number; roble's hold what they were made with, which is what lets it keep
none. Under a skewed workload this is most of a memtable: with YCSB's Zipf
draws about two of every three updates land on a key the same memtable already
holds, and writing those cost the flush their encoding, checksums and bytes
and the compaction after it the reading of them back only to drop them.

`flush()` **freezes** the memtable — nothing more is added to it, and writes go
to a fresh one and, with a WAL, a fresh log — then writes it as a table, appends
the version edit carrying the new minimum unflushed log number, and syncs: the
point at which Oxia's data becomes durable. The freeze comes first, before
anything is awaited, so that a write arriving in the middle of a flush is
neither left out of the table nor retired with the log (§5). The minimum the
edit carries is the log the store is writing to when the edit is appended, which
is the one the freeze installed. The logs below that number are then
obsolete, and are deleted like any other obsolete file — including one Pebble
had kept back to recycle, which roble does not do and so has no use for.

Past `Options::max_manifest_bytes` — 128 MiB, Pebble's `MaxManifestFileSize` —
that same flush **rotates** the MANIFEST: a fresh file holding one snapshot edit of the
whole version, and then `marker.manifest.*` moved to name it. The marker moves
last and is the one-way door; roble syncs the directory between creating the new
marker and letting the old one go, so no crash can land with no marker at all.
roble does not rotate at open as Pebble does, which keeps opening a database
something that does not write to it.

`snapshot()` pins a sequence number and the version with it, by holding the
`Rc<Version>`: a table a held version names is one the sweep leaves alone
(§5). `checkpoint(dir)` writes a
directory Go Pebble opens: hard links to the live tables, a fresh MANIFEST
holding one edit that describes the version — the same edit a rotation writes,
and the same claim — and the two markers. Oxia streams that directory, so it
is the single most important interop test (§12), and `compat/wal-replay` hands
Pebble one to open read-only.

**No `OPTIONS-*`,** which this document used to say there would be. roble
writes none and reads nothing from one, and Pebble does not need one: it
writes its own when it opens a directory read-write, and checks a previous one
only if it finds one. **And no logs.** A checkpoint **flushes first**, so
there is nothing for a log to carry — which is what makes it right with the
WAL off, where a log would not be there to carry it anyway. The minimum
unflushed log number the edit records then names a log the checkpoint does not
hold, which is the same thing as a log holding nothing to roble's replay and
to Pebble's.

Links rather than copies, because a table is immutable once written: the
checkpoint and the store share the bytes until one of them deletes its name
(§10, `Fs::link`, which falls back to a copy where a link cannot be made).

### `destroy()` and `disk_usage()`

`destroy()` is Oxia's `KV.Delete`, which it calls when a shard leaves the node:
every file of the database removed, `LOCK` included. It takes the store by
value, which does two things. The lock is held until the last name is gone, so
there is no moment at which the database is half there and unowned — Oxia's
closes first and removes afterwards, which leaves that window open. And it
needs no permit, unlike every other change: an outstanding borrow forbids the
move, and the store's own background tasks are wound down — the flush or merge
in flight finished, both tasks ended — before the first file goes, so there is
no way to ask for a destroy while the store is busy. A `Scan` or a `Snapshot` may outlive it, holding readers and no
borrow; they write nothing, so the removal is none of their business. It does
not flush on the way out, as
Oxia's `Close` does: a memtable written into a table that is removed in the
same call is work for nobody. The table cache's readers go before the names,
because a filesystem reclaims nothing while a file is open, and a destroy that
freed the names and not the space would be only half of one. The directory
itself stays, empty: `Fs` has no way to remove a directory, and a method on it
is a method every embedder has to write (§10).

`disk_usage()` is Oxia's `DiskSpaceUsage`: the bytes of every file in the
directory, which is the measurement `roble.disk.size` reports and the call that
refreshes it. Pebble reaches the number from the other side, adding up the
sizes it tracks, which is a sum of what it believes rather than of what is
there. It walks the directory, so it is a call to make when something asks and
not one to make on a timer — Oxia memoizes Pebble's for five seconds for that
reason.

### Opening and recovery

`open()` reads `marker.format-version.*` and `marker.manifest.*`, replays the
MANIFEST into a version, and then replays, in order, every log the version edits
leave unflushed, applying each record to a fresh memtable. The next sequence
number is the highest the replay saw, or the MANIFEST's last sequence number when
there was nothing to replay — it never goes backwards, or a write Oxia replays
could lose to a tombstone that outlives it.

A torn record at the tail of the last log is the ordinary shape of a crash and
ends the replay; a torn record anywhere else, or a chunk whose log number belongs
to an older file, is corruption. **roble refuses a log it cannot replay rather
than opening without it**: ignoring one silently discards writes that were
already acknowledged. With `Wal::Off` there is nothing to replay and the
database opens as of the last flush.

The MANIFEST ends the same way. A crash between appending an edit and syncing
it can leave the file ending partway through that edit, cut short or with its
unwritten tail read back as zeros, and the edit was never acknowledged: the
version is the one before it, and whatever it would have flushed is still in
the logs. Pebble opens such a MANIFEST too (`versionSet.load` stops at the
first record that does not read). roble then moves to a fresh MANIFEST before
anything can append, as Pebble does on every open, because an edit appended
after the torn bytes is one the next replay would stop short of. Damage
*before* the end is still refused, where Pebble would stop there as well: an
edit that fails to read with written bytes after it was not torn by a crash,
and stopping at it would drop the edits after it — whose tables the sweep
would then delete, with the logs they replaced already gone. The test is that
the written bytes from the failed chunk on, less trailing zeros, fit inside
that chunk. Edits naming long keys run to dozens of the MANIFEST's 32 KiB
blocks, and `tests/large_keys.rs` tears one at every block boundary.

### Reading beside a writer

`open_read_only(env, dir, options)` opens a database to read while another
process holds it open to write, as encina's `ReadOnlyDb` does. It walks the
same steps as `open_with` but three: it takes no lock, creates nothing
(`create_if_missing` is not acted on), and sweeps nothing obsolete. It replays
the MANIFEST and the logs into a memtable of its own, opens every file to read
only, and refuses everything that would change the directory — `write`,
`flush`, `compact`, `compact_once`, `rotate_manifest`, `checkpoint`,
`remove_obsolete_files`, `destroy` — with `Error::ReadOnly`. It reads the
database as of the open and does not follow the writer after it.

**The writer does not know it is there**, so nothing holds back the writer's
deletions for it, and the reader must expect a file it needs to go. The answer
is the simplest honest one: **a reader that needs a file the writer has retired
fails with `Error::Stale`, and the caller opens again**, a bounded number of
times. The new open reads the version that is current, which names files that
are there. Each case:

- **A table, a log or the MANIFEST that has gone** by the time the reader opens
  it is stale rather than an I/O error. The writer deletes only what no version
  it can still read names, so for a store with the lock such a file is damage
  and stays an error; for a reader it is a version that has moved on. A table
  is opened when a read first reaches it, so this can come from a `get` or a
  scan long after the open. Only the open of a file is checked this way: a
  file already open keeps its bytes on a POSIX filesystem, so `FileFs` reads
  it to the end. Where a deleted file stops reading, as in the simulated
  filesystem, such a read fails as the I/O error it is, not as stale.
- **The table check is skipped.** An open with the lock refuses a version that
  names a table the directory does not hold. A reader lists the directory
  before it reads the MANIFEST, so a flush in between names a table the listing
  never saw. The listing stays first all the same, because the logs to replay
  come from it: a newer listing beside an older MANIFEST would replay only the
  newer log and leave a hole without an error. A table that really has gone
  shows up when a read reaches it.
- **A record that does not read** is stale in the MANIFEST, which the writer
  may be appending to, and in a log that is not the last. A flush creates the
  next log before the writes in flight on the old one have finished, so the
  reader can find that older log torn.
- **An older log that grew while the newer ones were read** is stale too. The
  reader can read the older log, miss a write that lands in it afterwards, and
  then see later writes in the newer log, which is a hole. So once the replay
  is done, the reader checks that every log but the last is still the length
  it read. If it is, any write still to land there comes after everything the
  reader saw.

Real damage looks the same as a race to a reader, and is told apart by
persisting: it fails every open, and the error says what it met. So does a
writer that retires files faster than a reader can open the database — one
flushing every millisecond, which retires a log each time, outruns a reader
that has to start a process first — and then the retries run out on a race
rather than on damage. A scan that resumes in a new open after the last key
it returned is not one snapshot: the entries before the retry are as of one
open, and those after it as of the next.

### Creating a database

`open_with(.., Options::new().create_if_missing(true))` writes a database where
there is none, as Pebble's `CreateIfMissing` does. **It is a setting and not a
`create()` of its own because Oxia's call site cannot tell the two cases
apart**: `newKVPebble` builds a path from the namespace and shard and hands it
to `pebble.Open`, whether the shard has been on this node all along or arrived
a moment ago. A caller that cannot distinguish them should not have to, and
keeping the decision inside the open also keeps it under one lock, where two
opens racing to create one directory cannot both write it.

It is off by default, so an `open()` of the wrong path says so rather than
handing back an empty store, and an open that may not create never makes the
directory either.

What gets written is the least a database can be: `MANIFEST-000001` holding one
snapshot edit of an empty version — the same edit a rotation writes, of a
version set with nothing in it — an empty `000002.log`, and iteration one of
both markers. Pebble reaches almost the same place from further away, creating
at `FormatMinSupported` and upgrading, which is why its own directories carry
`marker.format-version.000003.016`; a marker's iteration says only which of
several is current, and `compat/wal-replay --checkpoint` has been making that
claim about a roble-written iteration one since M4.

**The log is there because a store with no log refuses writes.** With the WAL on,
a `write` that could not be recovered is not acknowledged (§4), and nothing but
a flush ever opens a log — so a database created without one could not be
written to until it had been flushed, and could not be flushed until it had
been written to. Pebble solves this on the other side, creating a log in every
open that is not read-only; doing it at creation instead leaves the open's log
handling to the one thing it does today, which is resuming the log a replay
found, and means the directory is complete when the creation returns rather
than when something first opens it.

**The manifest marker goes last**, after the format version, because it is the
marker that makes a directory read as a database: until everything it names is
in place it must not be written. `checkpoint` does the same, for the same
reason — written the other way round, a checkpoint interrupted between the two
left a directory an open read as a database whose format version had never been
recorded, which is the one case that message is for. Both orders leave an
unfinished directory that an open refuses; only one of them describes what is
there.

**A directory holding a `marker.` file is refused rather than written into,
unless it is a creation that did not finish.** A marker means either a database
this would overwrite or a creation that stopped between its two markers. The
second is told apart by holding nothing else: only the creation's own names —
`LOCK`, `MANIFEST-000001`, `000002.log` and `marker.format-version.000001.016`
— with the log empty. Such a directory holds nothing anyone wrote, so a
creation finishes it, rewriting every file whole. Refusing it instead would
leave a directory no open will ever read again: a failure between the
markers, and every later `create_if_missing` open failed. Anything more — a table, a later iteration of
a marker, a log holding a write a database acknowledged before losing its
manifest marker — is refused, and the operator clears the directory.

Two numbers are worth stating. The MANIFEST takes file number 1 and the log 2,
as Pebble's do, and the edit says 3 comes next, so the first flush numbers its
table past the log rather than beside it. And the first key written to a
created database is numbered **10**: Pebble keeps 0 for a key a compaction has
proven to be the last of its kind and 1 to 9 for uses it has not found yet, so
an open clamps the next sequence number up to `base.SeqNumStart` whatever the
MANIFEST says — and roble clamps in the same place, since without it a database
with nothing committed, created by roble or by Pebble, would number its first
key 1. The constant is not importable, so `harness/db-fixture`'s `db-created`
fixture pins it: a database Pebble made and wrote one key to, whose table reads
`seq=10..10`.

What a creation does **not** do is make its own directory entry durable. The
`mkdir_all` and the final `sync_dir` make the database's files durable within
the directory, but nothing syncs the parent that names it, so a crash just
after a creation can take the whole thing. `checkpoint` has the same gap and
Pebble does not (`mkdirAllAndSyncParents`). It is left because the consequence
is the one Oxia already handles — the shard comes back absent and is created
again from the replication log — and closing it means syncing a directory roble
was never handed.

As built, the replay stops at the first record of the **last** log that does
not read, and the store carries on writing from there — first cutting the file
to that point, which drops what was half-written and was never acknowledged. A
record that does not read in an *earlier* log is corruption and refused,
because a later log exists only if that one was finished.

The cut is what makes that last rule safe to apply to a log the store carried
on in. Written over rather than cut, a torn record longer than what replaces
it, or the bytes of a file's earlier life when the log is one Pebble recycled,
stays past the store's last record; the next flush creates a newer log before
its edit retires this one, and a crash between left a log that was finished,
by this rule, and does not read to its end — which Pebble refuses too, since
it replays every log but the newest strictly (`strictWALTail`). Pebble never
meets this, because it never appends to a log it replayed: its open flushes
what it replayed and starts a fresh log. `compat/differential` found it by
killing roble in such a flush.

That rule is looser than §6 states in one way, and the looseness is
deliberate: it tolerates a torn record anywhere in the last log, not only at
its tail. Telling a torn tail from damage in the middle of the same file would
mean deciding what the bytes after it were supposed to be, and stopping at the
last record that reads is the answer that loses nothing either way.

Note that `Wal::Off` leaves an **empty log file** behind rather than no log
file, so the question an opener asks is whether a log holds a record, not
whether one exists — Oxia's own configuration has a log file in the directory,
holding nothing but the eleven-byte EOF trailer. A store writes into that log
rather than starting a new one, which is why no new file number is needed and
no MANIFEST has to be written before a write can be logged.

## 7. The format crate

`roble-format` encodes and decodes Pebble's files. It has no I/O, no async, no
runtime, and no knowledge of an engine, which is what makes it fuzzable and
testable against golden files on its own, and what makes the first milestone
useful before anything else exists.

```text
format/src/
  key.rs            internal keys: user key, sequence number, kind, trailer
  block/mod.rs      row-oriented block builder and in-place reader, including
                    the Pebblev3+ value prefix byte on every SET
  block/restart.rs  restart arrays, prefix compression, seek within a block, and
                    stepping either way. The cursor holds a position and is
                    handed the block per call rather than borrowing it, which
                    is what lets the block cache own the bytes and an iterator
                    hold both (§8)
  value_block.rs    value blocks, value handles, and the value index — read only.
                    The metaindex value is a block handle *plus* three width
                    bytes, without which the index cannot be read
  filter.rs         rocksdb.BuiltinBloomFilter
  props.rs          properties block, metaindex, block properties
  footer.rs         footer, magics and versions per table format
  rangedel.rs       v2 range-deletion blocks
  sst/reader.rs     table reader: footer, metaindex, index, blocks
  sst/writer.rs     table writer
  batch.rs          Pebble batch repr
  manifest.rs       version edits; the new-file tag and the backing-table tags are
                    how a range-key or virtual table announces itself, and both are refused
  record.rs         32 KiB block record framing: legacy (MANIFEST), recyclable
                    (WAL), and the EOF trailer that ends a closed log
  files.rs          file names, NNNNNN.log, marker.manifest.*, marker.format-version.*
```

Blocks are **searched in place**, as encina's nodes are: a reader binary-searches
the restart array and then scans within a restart interval, without decoding the
block into owned values. This is what lets an uncompressed cached block be the
bytes that came off the disk.

## 8. Memory and caches

**Caches belong to the thread.** A `Caches` value is created per runtime thread
and passed to every store opened on it through `Options`, holding:

- a **block cache**, keyed by (file number, offset), with a budget and CLOCK
  eviction, as encina's leaf cache;
- a **table cache**, open readers with their index and filter blocks, bounded by
  count — **unbounded by default**, as RocksDB's `max_open_files` is, because
  a bound smaller than the store is a cliff: a store with more tables than the
  cache reopens one on nearly every lookup, and reopening a table is its
  footer, metaindex, properties, index, filter and tombstones rather than a
  file handle. At 243 tables against a budget of 128 that cost `floor`
  fifty-five times. The budget remains settable and a deployment should set
  it, because an open reader still costs a file descriptor: hundreds of shards
  of a few hundred tables each will pass a default `ulimit -n` of 1024.

Both are keyed by **store and file number**, not by file number alone. File
numbers are per database, so two shards on one thread both have a table
000007; a cache keyed by the number alone hands one shard the other's table,
which is a wrong answer rather than an error. Pebble has the same problem and
solves it the same way, with a per-database cache id.

Eviction is CLOCK rather than LRU: a second-chance bit per entry and a hand
that sweeps, which costs one bit and no reordering on a hit, where an LRU
would write to shared state on every read. A newly inserted entry arrives with
its bit set, so being referenced buys one reprieve from the sweep rather than
immunity. An entry larger than the whole budget is not cached at all, since
evicting everything to make room for one oversized thing is worse than not
caching it.

A node runs hundreds of shards, so a per-store budget — encina's model — would
either waste memory on cold shards or starve hot ones. Dividing one node budget
by the number of threads keeps roble's total within a configured bound, which is
the property encina insists on, while sharing within a thread. The sharing needs
no atomics: it is all within one thread, behind `Rc`.

**Alignment is the cache's problem, not the caller's.** A block is 16 KiB at an
offset that is not aligned, so a read covers the enclosing aligned span and the
block cache keeps the block, not the span. Frames are size-classed rather than
encina's uniform 4 KiB, since blocks vary and a value block or a two-level index
partition may be larger.

**As built, the block cache holds decoded blocks on the heap, and neither
size-classed frames nor span-aligned reads exist yet.** Both serve direct I/O,
and direct I/O is not what the engine does today (§10): a frame pool with no
aligned reads to fill it would be machinery with no caller. They arrive
together, and the cache's shape — one owner of a block's bytes, keyed by store,
table and offset — is what they slot into. What does exist is the budget and
the eviction, which are what bound a node running hundreds of shards.

That span-aligned read is also the only place direct I/O can live. The
structures that find a block are small and unaligned — a 53-byte footer at the
end of the file, a metaindex of a few dozen bytes, a MANIFEST of partial chunks
— and `O_DIRECT` refuses such a request with `EINVAL` instead of serving it. So
`Disk` accepts any offset and any length (§10), and an implementation that reads
in aligned units aligns them itself. roble tried the other order first, setting
`O_DIRECT` on the open and declaring alignment the caller's business, and every
unaligned write failed on Linux while passing on macOS, where `F_NOCACHE`
carries no such contract. Direct I/O returns with the frames, as a disk that
aligns what it is handed, and the cost until then is the page cache's second
copy — which is what Pebble pays today.

**Compression is where "no allocation on the read path" breaks.** A Snappy block
must be decompressed into a buffer, so a cached block is no longer the bytes
that were read, and a miss allocates. Compression is recorded per block, so
writing everything uncompressed is fully format-compatible, and it keeps
in-place search, zero-copy reads and a cache whose budget is exact. **roble
measured the trade** (`bench --compression`, §14) and compresses L1 and below
with Snappy by default, leaving L0 as read, which is how Oxia runs Pebble.
Reading Snappy and Zstd is required regardless, for files Go wrote (§3).

**As built, an uncompressed block is kept in the buffer it was read into** —
which for a while it was not. A miss copied the block out of its read to drop
the five-byte trailer, so the read path paid the very copy and allocation that
the paragraph above credits uncompressed blocks with avoiding: two block-sized
allocations and a 16 KiB copy per miss, found by counting what each kind of
lookup allocates rather than by any timing. Keeping the read buffer took `get`
from 255,830 to 281,456 a second (1.10x) and `floor` from 204,016 to 219,310
(1.075x), on one pinned core over a store several times the cache, where a
miss is the common case. A miss still allocates three times per block: the
buffer itself, which the cache then owns; the `Rc` the cache shares it
through; and the I/O runtime's box for the operation, which is compio's
(`tests/missed_block_memory.rs`).

**Memtables** are arenas of a configured size, 4 MiB to match Oxia. A store's
memory is its memtables plus its share of the thread's caches plus its
compaction buffers, and the budget covers all three. The first of the three is
a bound rather than a hope because a write enforces it: what a store holds
across every memtable it has is `Options::memtable_size` plus one batch (§4,
§6).

**A store's memtable is allocated at its budget, not grown to it.** The arena
grows as a `Vec` does while it is under a megabyte, and the first time it needs
more it goes in one step to all it will hold. Grown by doubling all the way, it
reallocated at every power of two and held the old copy and the new while it
moved: a 64 MiB memtable passed 43 MiB on its way to its 56 MiB flush line,
took an 86 MiB allocation and held 129 MiB for the copy, which made it the
largest thing a store held at its peak in YCSB A — and left a 56 MiB memtable
in an 86 MiB allocation afterwards. The bound above counted keys and values
and was right about them; it was the arena around them that ran to twice it.
Pages are resident only once written, so a budget-sized allocation costs what
the memtable holds; and below a megabyte nothing changes, so an idle store's
memtable — Oxia runs hundreds — is not handed its budget for a few writes.
With the benchmark's own samples made smaller as well, roble's peak in YCSB A
and F in cache went from 47–68 MiB over RocksDB's to within 3 (`bench/`).

**All it will hold is more than the budget**, because the budget counts keys
and values and the arena also holds every record's header and links, about
eighteen bytes a record. Reserved at the budget alone, a 64 MiB store's arena
of 36-byte keys and 128-byte values was full at 58 MiB of keys and values,
before the flush took it, and doubled to 128 MiB for the last few: a 64 MiB
copy every memtable of a load. mimalloc keeps freed memory for a second before
it returns it, so the copies and the arena before were all still resident at
the load's peak, 239 MiB against 134 MiB live — and since a block that large
goes back into whole 32 MiB chunks that any small allocation in between can
claim, one unused 270 KiB allocation per phase took that peak to 265 MiB. So
the arena now reserves the budget plus an eighth, for the batch that crosses
it (§5), at the rate the records of its first megabyte paid for bookkeeping.
The load's peak fell to 141–149 MiB, and YCSB C's in cache to 229 MiB, or 233
with the pad (`bench/`).

### Where a warm scan step goes

A scan step whose block is already in the block cache costs about 28 ns on an
M-series laptop, and taking work out of it has repeatedly measured nothing.
The reason is that the work is not what it is spending. Measured in situ —
each variant against a control that walks the same entries in one
non-resumable loop, so that run-to-run drift divides out — a step is roughly:

| | of a ~28 ns step |
| --- | --- |
| the `async` layering, on a path that never yields | ~0.6 ns |
| cloning the `Rc` the block cache handed out, and re-reading the block's restart count | ~1.7 ns |
| walking `self` → `Rc` → `Vec` → the data to rebuild the block | ~0.4 ns, at the noise floor |
| the cursor's state living in memory rather than in registers | the remaining ~11 ns |

The last row is what an iterator **is**. The control keeps the position, the
assembled prefix-compressed key and the value's range in registers because it
never returns; an iterator has to leave them where the next call will find
them, and so reads and writes them through memory on every step. Pebble's
`rowblk.Iter` is a heap object for the same reason and pays the same. There is
no indirection left to remove: holding the parsed block beside its bytes —
which needs `unsafe`, since the block borrows them — was built and measured at
the noise floor, while growing `TableIter` from 112 to 152 bytes.

**And the step is latency-bound, not throughput-bound.** This is the part that
generalises. Dropping the `Rc` clone measured **1.037 on a 30 MB store, ahead
in 12 of 12 paired runs**, and on an 847 MB store **1.007 forward and 0.978
reverse**. The same change, the same code: a win where the store fits in the
CPU's cache and nothing where it does not, because past that point a step
waits on memory and the instructions removed from it were already hiding under
the wait. Worse, a struct grown to hold the saving then costs more than the
saving is worth, which is where the reverse regression comes from — reverse
touches far more memory per record, so it is the first to feel it.

That is the explanation for a row of null results, each one paired and
alternated, each one reverted:

| | forward scan | note |
| --- | --- | --- |
| lending the value instead of copying it | 1.01 | §6 |
| not copying the key in the merge | 1.0032 over 20 runs | cost `floor` 2.3%; see `OpenTable::value_into` |
| caching the parsed block's shape | 0.947 | grows `TableIter` by 24 bytes |
| rewriting the merge as one stream | 0.921 | |
| dropping the `Rc` clone | 1.007 at 847 MB, 1.037 at 30 MB | 0.978 reverse at 847 MB |
| holding the block beside its bytes (`unsafe`) | noise | grows `TableIter` by 40 bytes |

The three changes that did pay were all asymptotic rather than constant: a
source per level instead of per table (a seek and a compare per table), a
read-ahead window (a syscall per block), and deciding once which tables can
hold a tombstone (a sweep per table per key). **So scan work belongs at the
memory the step waits on — read-ahead, block layout, direct I/O (§10) — and
not at the contents of the loop.** A profile is not precise enough to steer
the latter anyway: at ~90 ns a step spread over a dozen items of 3-15 ns,
`sample`'s per-frame attribution cannot separate them.

**What all of this is measured on, which is not yet Oxia.** A quiesced store,
scanned end to end, with point lookups drawn uniformly, in one process on one
laptop. Every one of those differs from the workload §1 describes. A skewed
distribution keeps a hot working set in the CPU's cache — which is the 30 MB
regime above, where the loop's contents *did* measure, and not the 847 MB one
where they did not. Nothing here mixes reads with writes, scans a short range
rather than a whole table, or runs while a compaction does, and Oxia does all
three. So this section is a conclusion about the benchmark before it is one
about roble, and the standing task is to make the benchmark worth concluding
from: skewed and latest-key distributions, short range scans, read-modify-
write, and a background compaction under the read phases.

## 9. Compaction

As built, `compact(level, tables)` does the merge and `pick()` chooses what to
feed it, with `compact_once()` joining the two. What a merge drops, and why
each is safe, is written out in `src/compact.rs`; what it does not drop yet is
a range tombstone that has nothing left below it, since carrying a spent
tombstone costs a reader a comparison and dropping a live one brings a key
back from the dead. The output is split at `Options::target_file_size` — 2 MiB
for L0 and twice that for each level below, as Pebble's `TargetFileSizes` —
and a tombstone straddling a split is clipped at it, so the tables either side
meet at one user key rather than overlapping across it.

**L0 sublevels.** L0 is the one level whose tables overlap, so a read may have
to look in several of them — but not in all of them. Partition L0 into layers
within which no two tables overlap and a read consults at most one table per
layer: the number of layers is L0's read amplification, and the number of
tables is only an upper bound on it. Ten tables flushed from disjoint key
ranges are one layer and one seek.

`Version::l0_sublevels` computes it by Pebble's rule
(`internal/manifest/l0_sublevels.go`): take the tables oldest first by the
largest sequence number each holds, and put each one a layer above the highest
layer holding a table it overlaps. Two things stay true at once — no two tables
in a layer overlap, and a newer table is always above any older table it
overlaps, so a read walking down the layers meets the newest version of a key
first. Two tables that only *touch*, where the first ends at the sentinel a
range deletion leaves, do not overlap and share a layer (§5).

**Everything that measures L0 measures it in layers**, which is what Pebble's
thresholds are in: `l0_compaction_threshold` and `l0_stop_writes_threshold`
both, and the compaction debt with them, since a debt weighed against an
allowance has to be in the allowance's unit. Ten tables flushed from ten
different key ranges are one layer, so a store that writes in key order is
left alone where counting tables would have called it a backlog of ten.

A bound that does not decode falls back to the table count, which is the upper
bound on the layers. It cannot happen to a database that opened — the ordering
check decodes every bound — and a picker is not where anyone would want to find
that out.

A compaction is paced in units of work (`crate::work`): every
`Options::compaction_unit` entries it hands the executor back and resumes, so
a reader or a writer on the same core waits a slice rather than a merge. The
unit is entries and not time, because §10 keeps a store off the clock, and a
job that yielded on a timer would stop a seeded run replaying.

**The walk of the memtable a flush writes out is paced by the same unit**, and
for the same reason once a flush runs beside the writes rather than inside
one. A 64 MiB memtable is a few hundred thousand entries to read and encode,
and in one piece that is tens of milliseconds in which nothing else on the
core runs — invisible while the write that asked for the flush was waiting on
it anyway, and the whole of what is left once it is not. The rest of a flush
is waiting on a disk, which costs the thread nothing.

**A write hands the thread back too**, once, before it takes the write permit,
whenever something is flushing or merging beside it. That is the same bargain
from the other side, and without it the rest of this section does not happen:
a store with the WAL off awaits nothing that is ever pending on a write — no
log append, no disk — so a writer in a loop polls to completion every time and
the task it just told there was a flush to do never gets a turn. It would be
found out at the budget, where the write waits for the flush it starved, and
at L0's depth, where it waits for the merge. The cost is a trip through the
executor, and it is paid only while something is actually running.

**What runs it.** A flush that wrote a table runs the compaction the picker
chose, before it returns, unless `Options::disable_automatic_compactions`
says otherwise — Pebble's switch of the same name, wanted for the same two
reasons: a caller that would rather compact when it knows it is idle, and a
test that names its own inputs. So a store that is written to keeps itself in
shape without the caller ever naming a table, and the two are timed apart:
`roble.flush.duration` ends where the flush does, and the compaction's time
is its own.

**One compaction per flush**, which is the analogue of Pebble's one
concurrent compaction. A flush adds a single table to L0 and a compaction of
L0 takes all of it, so one per flush more than keeps up with the level a
flush fills; below L0 the levels trade off, because the picker's next answer
is whichever is furthest past its allowance. What a flush deliberately does
*not* do is drain a backlog it did not create, since its caller is waiting on
it — Pebble answers that with the stop-writes thresholds a write stalls on,
and roble owes those. An **idle** store drains its backlog in its own merge
task, which asks the picker again whenever the version changes (§9, "Flushing
and compacting in tasks of their own").

Compaction is the cost encina does not pay, and the only background CPU work in
roble. Leveled, with L0 counted in sublevels, and a **picker** — the component that
decides, each time compaction can run, which level and which of its files to
compact into the level below, by scoring every level's size against the target
size for that level and taking the worst, as Pebble's does. Compatibility
constrains the files a compaction produces, not when it runs or what it picks,
so the picker is roble's own and can be simpler than Pebble's to start.

**Where it runs.** On the owning thread, cooperatively, paced in **units of
work** exactly as encina paces a large merge: a compaction merges a slice, yields
to the executor, and resumes. The reasons are encina's reasons: no locks, no
`Send` requirement, and a seeded run replays, since nothing reads the clock
unless the env says it may (§10). Compaction *scheduling* is part of that: a
picker that consulted a timer or an RNG would break replay, so it consults
neither.

The risk this accepts is that a compaction competes with foreground work on the
same core. For Oxia it is a reasonable bet — many small shards per core, 4 MiB
memtables, so each compaction is small — but it is a bet, and the fallback is
designed in rather than discovered later:

**Offload is a capability of the env.** A compaction's inputs are immutable
files and its output is new files plus a version edit, so the boundary is clean:
an env that offers a compaction executor gets handed a job that owns its own
buffers (not the thread's frames, which are `Rc`), and gets back an edit to
apply. The default executor runs the job cooperatively on the calling thread.
Start there; add the pool when a benchmark asks for it.

**What the bet is worth, measured.** Holding the whole process to one CPU is
the comparison this bet needs, since a rate measured with six cores free is
roble's one core against RocksDB's six (`bench/README.md`, "One core, 512 MiB").
Over 6,000,000 records on one core each, roble loads at **1.08x** RocksDB's
rate for 0.82x its CPU — where with six cores available RocksDB loads 1.85x
faster than roble. The load comparison was a core-count artifact, and
correcting it reverses the result.

The premise holds too. roble gains nothing from five more cores: across YCSB A
to F its rate moves between 0.98x and 1.01x of its pinned rate, which is this
section's "one store, one thread" measured rather than assumed. It is in fact
*faster* pinned — loading 1.19x and getting 1.09x what it does unpinned —
while spending 1.31x the CPU unpinned for identical `settle` work. The extra
CPU for the same output is what a flush landing on a core other than the
store's would cost: it reads a memtable cold in that core's cache and writes
blocks the store's core then has to re-fetch. That is the explanation the
numbers fit, not one they prove.

The bet's other half comes out worse. roble pays **1.66x** RocksDB's CPU to
reach quiescence on one core, and 1.87x on six: a cooperative compaction's cost
arrives later rather than never, and `settle` is where it arrives. And the read
gaps are not core-count artifacts at all — RocksDB's rate on the mixes moves
only 0.94x to 1.08x across the same five cores, so taking them away does not
close them. `floor` widens, from 0.51x at six cores to **0.31x** at one,
because pinning buys RocksDB 1.70x where it buys roble 1.03x. That asymmetry
fits §8's conclusion: a step already missing to DRAM has no cache locality left
to win, and RocksDB's evidently had some.

One run per cell, on the VM of §12's benchmark note. The deltas quoted above
are far outside the spread those runs show, except `get-cold`, which moved
1.74x between roble's own two runs and is not quoted here for that reason.

**Write stalls.** Both of them, and both are a write waiting for work somebody
else is already doing rather than queueing up to repeat it. Pebble's
`maybeInduceWriteStall` is the shape: look, wait for the change underway, look
again.

A write short of room in a memtable makes the room itself — a flush, before its
batch is accepted (§4) — unless a change is already underway, in which case it
waits for that one and asks again, and often finds the room already made. That
is the backstop; the routine case does not reach it, because a store with a
task waiting on `Db::flush_wanted` was told there was a memtable to write
out an eighth of the budget earlier and that task is already writing it (§5).
A store without one has no such task to tell, and this is where it finds out.
A write short of a shallow enough L0 does the same with a merge:
`Options::l0_stop_writes_threshold`, Pebble's and the same twelve, which sits
above the `l0_compaction_threshold` at which the picker merely starts *wanting*
L0 compacted. Between the two a flush runs the merge and nothing waits; at the
upper one a write does.

Reaching that upper line also changes what the picker answers: L0 wins
outright, however much further past its allowance another level is. Pebble
arrives at the same place from the other direction — a deep L0 buys it extra
*concurrent* compactions (`compaction_picker.go`'s `l0ReadAmpCompactions`), so
the L0 merge runs alongside whatever was already going. A store that runs one
compaction at a time has no second one to add, so what it can do is make the
one it has the L0 one.

Neither stall depends on the WAL: a log append is not what a write waits on.
Oxia watches Pebble's `WriteStallBegin` events and counts them, so roble
exports the same two, as `roble.write.stalls` and `roble.write.stall.duration`
split by `roble.stall.reason`. **A write doing the work itself is not stalling**
and is not counted: nothing is waiting on anybody, and its time is in
`roble.write.duration` where the rest of that write's time is.

A store with `disable_automatic_compactions` is not stalled by L0 at all. It
owns L0's depth as well, and a write that waited for a merge nobody was going to
run would wait for ever — which is what Pebble does in that configuration, and a
worse answer than letting the caller have what it asked for.

**Flushing and compacting in tasks of their own.** A write that reaches the
line tells whoever is waiting that there is a memtable to write out (§5); a
flush that has written a table tells whoever is waiting that there is a merge
to run, and runs it where it stands if nobody is. `Db::flush_wanted` and
`Db::compaction_wanted` are the waiting, and the store opened to write spawns
the two tasks that wait, through `Env::spawn`:

```rust
for work in [Flush, Merge] {
    let worker = db.task_handle(); // the same state, counted until it ends
    env.spawn(async move {
        while worker.running() {
            match work {
                Flush => { worker.flush_wanted().await; /* if running: */ let _ = worker.flush().await; }
                Merge => { worker.compaction_wanted().await; /* if running: */ let _ = worker.compact_once().await; }
            }
        }
    });
}
```

Two waits and not one, because the two have to be able to run at the same
time. A merge is the longest thing a store does and holds no permit while it
runs, so a flush can go through the middle of one — but only if something is
there to run it, and one task cannot be in two places. A store told to leave
compaction to its caller (`disable_automatic_compactions`) spawns only the
flush task.

**The tasks hold the store's state, not the caller's handle.** `Env::spawn`
takes a `'static` future, so `Db` is a handle on an `Rc` of the state, and
each task holds a handle of its own on the same `Rc`. That is what makes
shutting down a question with two answers:

- **`Db::close().await`** stops the tasks, waits for the flush or merge in
  flight to finish and both tasks to end, and then releases the directory.
  It is Pebble's `Close`, and the shutdown that lets the directory be opened
  again at once.
- **Dropping the handle** stops the tasks too, but `Drop` cannot wait. If
  neither task is inside a flush or a merge, it releases the directory there
  and then: a task that is only parked does nothing more once woken but see
  the store stopped and end, and what it holds is state no file is open for
  writing in. If one is, the lock goes with the state, when that work has
  finished — so nobody opens the directory while files are still being
  written in it.

A failure in the background has nobody to be returned to. A failed flush is
asked for again after the next write, and one that keeps failing reaches a
write at the budget, which flushes inline and gets the error itself; a failed
merge is not asked for again until the version changes. Both are logged and
counted.

The tasks are not part of the API, and nor are the waits: a test that pins
which side does the work turns the store's own off
(`Options::background_tasks(false)`, hidden) and runs the loop by hand, and
the CLI, which does one thing and exits, opens without them. That keeps §12
intact as well: a failure sweep that counts operations can ask for a store
whose operations do not depend on when a runtime polls a detached task.

Both waits are level-triggered and asked again on every poll, so a task that
starts after the work appeared is told at once rather than waiting for an
event that may not come. The exception in each case is work that **failed**,
which is not wanted again until the thing it would read has changed: for a
compaction, the version, since the picker gives the same answer against the
same one; for a flush, a write, since until then it would write out the same
memtable to the same disk. Either way a task told to go again at once would
spin rather than retry. That is "later" with no clock to say when (§10).

A failure is said twice over, because a compaction often has nobody to return
an error to — a flush swallows the one it scheduled, and a task running one was
told to by a flush that has already returned. `roble.compaction.errors` counts
it by what went wrong, and a `tracing` event at warn carries the store and the
error. The facade only, and without `attributes`: roble emits events and never
opens a span, and it stays silent, and reads no clock, unless the application
installs a subscriber.

## 10. Concurrency and I/O

Reuse encina's shape: a store opens in an `Env`, which opens its files and runs
its tasks, and all I/O goes through a `Disk` whose requests move owned buffers.
A request is any byte range: a disk may not refuse an offset or a length, which
is what rules `O_DIRECT` out of the open and puts it under the block cache
instead (§8). `Db` is `!Send` and `!Sync`, and not `Clone`: one thread's
tasks share it through an `Rc`. `DefaultEnv` uses compio.

**A store owns its filesystem.** `Clone` is on the trait because a store
keeps the `Fs` it was opened in rather than borrowing it, which is what makes
a store a `'static` value: `Env::spawn` takes `'static` futures, and a store
that borrowed its filesystem could be owned by nothing but the stack frame it
was opened on — so a compaction could only ever run inside the call that
started it. Both implementations were already handles, `FileFs` a unit struct
and `SimFs` one `Rc`, and an implementation holding real state is expected to
do the same. This is what a compaction running behind the caller's back needs
and not all of what it needs: a compaction holding the one change permit (§5)
for the length of its merge makes a flush wait for it whichever way round
they are, so the permit has to be narrowed to the edit — with per-table marks
so that two compactions cannot choose the same inputs, as Pebble's
`compacting` flag does — before moving the work off the caller's stack buys
anything.

**What has to change, and why.** encina has one file, created once. An LSM
creates, renames, links and deletes files constantly, and its correctness
depends on directory syncs. So the env grows a filesystem, which is Pebble's
`vfs.FS` in Rust terms:

```rust
pub trait Fs: Clone {
    type Disk: Disk;
    /// Held for as long as this process owns the directory.
    type Lock;

    fn create(&self, path: &Path) -> impl Future<Output = Result<Self::Disk>>;
    fn open(&self, path: &Path, access: Access) -> impl Future<Output = Result<Self::Disk>>;
    fn remove(&self, path: &Path) -> impl Future<Output = Result<()>>;
    fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = Result<()>>;
    /// A hard link, which `checkpoint` wants; falls back to a copy.
    fn link(&self, from: &Path, to: &Path) -> impl Future<Output = Result<()>>;
    fn list(&self, dir: &Path) -> impl Future<Output = Result<Vec<PathBuf>>>;
    fn mkdir_all(&self, dir: &Path) -> impl Future<Output = Result<()>>;
    /// Makes the directory's entries durable, which marker rotation needs.
    fn sync_dir(&self, dir: &Path) -> impl Future<Output = Result<()>>;
    fn disk_usage(&self, dir: &Path) -> impl Future<Output = Result<DiskUsage>>;
    /// Keeps out a second owner; `LOCK` in the database directory.
    fn lock(&self, path: &Path) -> impl Future<Output = Result<Self::Lock>>;
}
```

`Disk` keeps encina's design, with two changes: a read is a byte range rather
than whole pages, since there is no page size, and it keeps the request classes,
so a compaction's reads can be told apart from a lookup's — an LSM has more use
for that than encina does.

Two details settled while writing it. A read that cannot be filled whole fails
with `UnexpectedEof` rather than coming back short: a partly filled buffer
cannot be told from a corrupt block, and the difference decides whether to
refuse a database. And `Fs` is a trait beside `Env` rather than methods on it,
because that is the seam Oxia already injects — it passes a `vfs.FS` and keeps
Pebble's scheduling and clock, so the two must be separable.

Everything else carries over: buffers are owned and returned, a disk that keeps
one cannot make roble touch memory it lends, a read in flight is polled from a
slot without a boxed future, and the store reads no clock — encina's
`real_time()` switch is not carried over, since nothing here paces by time, and
the one read left is the latency a meter provider asks to have timed. One owner per directory, through `LOCK`, and any
number of readers beside it, which take no lock (§6, "Reading beside a
writer").

Oxia injects a `vfs.FS` today, including `vfs.NewCrashableMem` in its tests, so
this trait is the same seam it already relies on.

## 11. API sketch

```rust
let caches = Caches::new(DEFAULT_OPEN_TABLES, 512 << 20);   // open tables, block bytes
let db = open_with(&DefaultEnv, "data/shard-3", Options::new().caches(caches)).await?;

let mut batch = db.indexed_batch();    // readable through itself
batch.put(b"/users/alice", b"...")?;
batch.delete_range(b"/sessions/", b"/sessions0")?;
let seen = batch.get(b"/users/alice", Comparison::Equal).await?;
batch.commit().await?;                 // as durable as the store's Wal setting
db.put(b"/users/bob", b"...").await?;  // one write, no index
db.flush().await?;                     // a table; the logs it covers can go

let last = db.get(b"/users/zzzz", Comparison::Floor).await?;   // Option<Found>, key and value

let snap = db.snapshot()?;             // one instant, every table held open
let mut scan = snap.range(&b"/users/"[..]..&b"/users0"[..])?;
scan.first().await?;
while scan.is_valid() { /* scan.key(), scan.value() */ scan.next().await?; }
let other = snap.get(b"/sessions/7", Comparison::Equal).await?;   // outside those bounds

db.checkpoint("snapshots/3").await?;   // a directory Go Pebble opens
db.close().await?;                     // background work done, directory released

// A shard arriving on this node, where Oxia cannot tell new from returning.
let db = roble::open_with(&env, dir, Options::new().create_if_missing(true)).await?;

// Another process's database, read beside it without its lock (§6).
let ro = roble::open_read_only(&env, dir, Options::new()).await?;
```

The Oxia mapping, so the surface is complete rather than approximately right:

| Oxia `kvstore` | roble |
|---|---|
| `NewWriteBatch`, `Put`, `Delete`, `DeleteRange`, `Commit` | `db.indexed_batch()`, `put`, `delete`, `delete_range`, `commit` |
| `PutMarshalable` | `put_with(key, len, \|buf\| …)` |
| batch `Get`, `FindLower`, `KeyRangeScan`, `RangeScan` | indexed batch iterators (§6) |
| batch `Count`, `Size`, `Checksum` | `count`, `len`, `as_bytes` — the Pebble repr, so a checksum over it matches Go |
| `Get` with `ComparisonType` | `get` with `Comparison` |
| `KeyRangeScan`, `RangeScan`, `KeyRangeScanReverse` | `range`, stepped with `first`/`next`, or `last`/`prev` for reverse |
| `KeyPrefixIterator` with `Lookup` | `snapshot()`, then `range` and `get` on it (§6) |
| `Snapshot`, `SnapshotLoader` | `checkpoint`, and opening a directory of received files |
| `Flush`, `Close`, `Delete`, `DiskSpaceUsage` | `flush`, `close`, `destroy`, `disk_usage` |
| `pebble.Open` of a shard's path, new or returning | `open_with` with `create_if_missing` (§6) |
| `vfs.FS`, `CacheSizeMB` | `Fs` (§10), `Caches` (§8) |
| `SyncData`, `UseWAL` | `Options::wal`: `Wal::Synced`, `Wal::Buffered` or `Wal::Off` (§4) |
| Pebble metrics, write-stall events | OpenTelemetry via nanometer, as encina ([METRICS.md](METRICS.md)) |

## 12. Testing

Compatibility is the thing that can silently be wrong, so it is tested first and
in both directions.

- **Golden corpus.** A Go harness in `harness/`, pinned to the versions Pebble
  v2.1.7 requires, builds databases that
  exercise each structure of §3: two-level indexes, value blocks, range
  deletions, obsolete keys, every compression Oxia can produce — none, Snappy
  and Zstd, the last from the conversion profile — both checksum types, a WAL
  with recycled log files,
  tables at Pebblev2 and Pebblev3 as well as v4, and a checkpoint. It also
  builds the databases **roble must refuse rather than misread** — range keys,
  a virtual sstable, a MERGE key, the `oxia-slash-spans` comparer, a failover
  log segment — because a refusal that is not tested is a refusal that panics or
  half-reads instead. The files are committed. roble's tests read them and
  compare against the harness's dump of the same files.
- **Reads are judged against Pebble; what roble writes is judged against a
  model.** While roble could only read, the operations were Pebble's, so Pebble
  was both the only model available and the stronger one. The memtable is the
  first thing roble writes, so the `BTreeMap` model this section asks for
  finally has something to model — and it is worth having precisely because a
  skiplist reaches the same answers by a different route. Two `BTreeMap`s
  agreeing would say nothing. So `harness/read-probes` builds a database
  from a seeded, deliberately unkind history — a small key alphabet so
  overwrites and deletes pile up on the same keys, range deletions cutting
  across flushed tables, explicit flushes and compactions leaving overlapping
  tables in L0 above a compacted level — and records Pebble's answer to each of
  the five comparisons for every probe key. The BTreeMap model moves to M3.
  Two details that are load-bearing rather than incidental: the generator
  **refuses to write a fixture that collapsed into a single level**, because
  such a fixture tests the merge not at all and nothing else would notice; and
  `SINGLEDEL` and `DELSIZED`, which roble reads and never creates, are written
  by hand on keys the random rounds cannot touch, since both kinds have
  invariants a random mix would break and a kind that is never any key's newest
  entry never has its visibility tested.
- **Seeks are pinned separately from scans.** `harness/seek-probes` runs
  Pebble's own iterator over every table in the corpus and writes down what
  `SeekGE` and `SeekLT` answer for probes derived from that table's keys —
  each key, and the keys immediately either side of it. roble regenerates the
  file from its own seeks and compares the whole text. This exists because the
  full-scan dump cannot catch a seek that lands one block late, or one that is
  right but cannot be stepped away from afterwards. Values are recorded as a
  length and their leading bytes rather than whole: what a seek can get wrong
  about a value is the range it reads it from, and the values themselves are
  already pinned byte for byte by the scan.
- **Pebble's own test cases, where roble claims Pebble's rule.** Where a
  piece of roble is a pure function Pebble also tests in its own test data,
  roble runs Pebble's cases rather than writing its own: L0's sublevel count
  (`tests/pebble/l0_sublevels`, every `define` and `add-l0-files`) and
  range-deletion fragmenting (`format/tests/pebble/fragmenter`, every
  `build`), and reads over hand-built LSMs of range deletions across
  memtables and levels (`tests/pebble/range_del`, its `get` and `iter`). The files are copied unchanged, with Pebble's licence beside them,
  and CI fails if they stop being the files at the pin. Only what roble shares
  is compared: the sublevel count, not Pebble's compaction picking, and the
  fragments, not how the test draws them.
- **Whole-database fixtures.** A file can be pinned on its own; a database
  cannot. Which table sits at which level exists only once a format-version
  marker, a manifest marker and a MANIFEST are put together and the MANIFEST's
  edits are replayed in order, so `harness/db-fixture` writes whole directories
  and dumps what Pebble's own reader finds in them. roble's `open` has to
  produce the same text. The fixtures are generated with compression off, which
  is not for realism but so their table sizes are the same on every
  architecture and the dumps can be compared exactly in CI.
- **Refusals have no Go counterpart.** Pebble will not write a database above
  roble's pin, and cannot be asked to write one with a comparer it does not
  have, so those directories are built by hand in the simulated filesystem.
  What those tests check is that roble refuses *for the stated reason*, not
  merely that it fails — a refusal that fires for the wrong reason is how a
  pin stops being a pin.
- **Round trip, both ways, in CI.** roble writes a database; the Go harness
  opens it, scans it, verifies it with the `pebble` tool, checkpoints it; roble opens
  that checkpoint and compares. Then the reverse. **This is the gate: a change
  that breaks it is a broken change**, including a performance change.
  `compat/wal-replay` runs it in twelve modes, and the strongest is `--created`:
  it is handed a path and nothing else, so when Pebble opens the result every
  byte in the directory — MANIFEST, both markers, the log, the tables — was
  written by roble. Every other mode starts from a database Pebble built, which
  leaves Pebble's own bytes in the parts roble did not touch.
- **Both engines, in turn, on one random history.** Every mode above is a fixed
  scenario. `compat/differential` generates a seeded script of segments — puts
  over a small alphabet with values from a byte to past a log block, deletes,
  range deletions, flushes, full compactions, snapshots, indexed batches, walks
  and checks — and hands each segment to roble or to Go Pebble in turn on one
  directory, so each opens what the other wrote: its tables, the layouts its
  compactions chose (Pebble's automatic ones included), its MANIFEST, and the
  log it left unflushed. The oracle is the script: each runner replays the
  segments before its own into a `BTreeMap` and, at every check, requires both
  scans and every comparison at every key to agree with it, and at every walk,
  an iterator over a range moved forty times — seeking, stepping, turning round
  — to be where the map says after each move. A snapshot is read both ways
  against the map as it stood when it was taken, after the writes, flushes and
  compactions since. A read through an open batch sees the map with the batch's
  writes laid over it, and a commit applies them. One turn in three kills itself
  at a random point instead of closing, and the next, the other engine's, opens
  what it left: the script is cut where the killed turn stopped, and a write it
  had under way is decided by reading a key the two outcomes disagree on, so
  every acknowledged write must be there and nothing else. CI runs ten seeds;
  `SEEDS`, `SEED`, `SEGMENTS` and `OPS` run more or replay one.
- **A number Pebble will not export is pinned by a fixture instead.** The first
  sequence number a key may be given is `base.SeqNumStart`, in one of Pebble's
  internal packages, so it cannot be asserted against the symbol the way
  `db-fixture` asserts the format-version pin. The `db-created` fixture is a
  database Pebble created and wrote one key to, and its dump carries the number
  Pebble gave that key. A Pebble release that moved it lands on a fixture that
  no longer matches, rather than in a roble that had quietly diverged.
- **Format fuzzing.** `roble-format` round-trips blocks, tables, version edits
  and batch reprs under `cargo fuzz`, and decodes arbitrary bytes without
  panicking — the corpus seeded from the golden files.
- **Damage is refused by the checksum, with the checksum on.** The fuzz
  targets turn checksums off and accept any error; the other half is that a
  flipped byte in any block is caught. `format/tests/corruption.rs` damages
  every block of every golden table in turn, across both checksum types and
  all three compressions, and the read has to fail with a checksum mismatch
  naming that block. `tests/corruption.rs` does the same to the blocks of
  two `db-reads` tables under a store, through its table and block caches:
  every `get`, both scans and a compaction must each give the undamaged
  store's answer or that mismatch, something must notice every block, and a
  compaction that fails must leave the version as it was.
- **Keys far larger than a block.** Every other test writes short keys, so a
  block holds many, a separator is short and a MANIFEST edit is one chunk.
  `tests/large_keys.rs` writes Pebble's own `testdata/large_keys` keys, which
  share a megabyte of prefix, and holds them to a `BTreeMap` through flushes,
  compactions that split between them, a MANIFEST rotation, reopens and a
  crash; then tears the two-megabyte edit naming such a table at every block
  boundary, which must open to the edit before. `compat/wal-replay
  --large-keys` hands the same store to Go Pebble.
- **Model-based property tests**, as encina's: random sequences of writes,
  deletes, range deletes, gets under every comparison, scans forward and
  reverse, flushes, compactions and reopens, checked against a `BTreeMap`, with
  a tiny cache and tiny memtables so flush, eviction and compaction happen
  constantly.
- **Invariant checker** after every step: levels non-overlapping above L0,
  sublevels consistent, every file referenced by the current version present on
  disk, every file on disk that roble *recognises* referenced or obsolete (never
  neither — a database directory also holds files that are none of roble's
  business, such as Oxia's `oxia-key-encoding-format` marker, and those are
  never deleted and never counted), sequence numbers monotonic, and the MANIFEST
  replaying to the version in memory.
- **Failure paths.** All I/O goes through `Fs` and `Disk`, so the simulated
  implementations fail or crash at any single operation, as encina's do: each
  scenario runs once to count its operations, then once per operation with an
  error injected there, once with a crash, and once with buffers withheld. After
  a crash, reopening must yield the state as of the last synced write — or as of
  the last `flush()` with `Wal::Off`, which is Oxia's configuration and so
  gets its own pass of every scenario. A half-written table, a torn log record or
  a half-rotated marker must leave the database openable.
- **A real process, killed.** `SimFs` is not what a store runs on, so
  `tests/crash_loop.rs` runs the store on `FileFs` in a child process — with
  its own background flushes and compactions on, small memtables and tables,
  and a MANIFEST rotated every few edits — and kills it with SIGKILL at a
  random moment, sometimes mid-recovery. The parent reopens and requires every
  acknowledged batch, and at most the one in flight, in every round so far.
  A kill keeps the page cache, so this is not a durability test; it is that no
  moment a process can die at leaves a directory that will not open, or one
  that lost or invented a write. CI runs 20 kills; `ROBLE_CRASH_LOOP_ROUNDS`
  runs more, as on the Linux VM, where the I/O goes through io_uring.
- **Overlap paths**, by the same method. Nothing the simulated filesystem does
  blocks — every operation finishes in the call that starts it, which is what
  makes a replay a replay — so two of a store's operations can never interleave
  there by accident, and the one thing a real disk guarantees is that they
  will. `SimFs::pause_at(n)` is one await, put where a test asks for it, and a
  scenario sweeps it over every operation of the first of two operations run
  together, exactly as the failure paths sweep an injected error. What must
  hold at every one of them is §5's rule: the database still opens, and it
  holds what was written to it.
  One await is not everything an overlap test needs. A claim about *being
  woken* cannot be made through a filesystem that never blocks — a future
  polled twice there is a future that has finished — so the two places that
  park a caller, the permit and the count of writes in flight, are driven by
  hand instead: `tests/change.rs` with an executor that polls only what was
  woken, and `crate::change`'s own unit tests with a waker that records
  whether it was called.
  CI measures coverage on every pull request (`cargo llvm-cov`), posts the totals
  as a status, and lists the lines and regions no test runs, such as the error
  branch of a `?`. An uncovered failure path needs a test or a written reason.
- **Benchmarks** on Oxia's shape: small records, a store per shard, batches per
  write, floor lookups, scans both ways, short range scans, YCSB's six mixes and
  a checkpoint. `bench/` runs roble, encina and RocksDB, each engine in a process
  of its own so the CPU time and the memory reported are its alone, and all
  deriving their keys, values and lookup order from one seed with the same
  generator.

  **There is no Go Pebble row.** There was one, from a driver under
  `harness/bench` written to these phases and this key shape, and it has been
  removed. `cmd/pebble`'s own benchmarks do not replace it: `ycsb` builds
  `user` plus a hash plus an MVCC suffix, `scan` builds a hundred thousand
  `key-` plus `uint32` records, and neither key shape is configurable — so they
  measure Pebble on *their* workload, not on Oxia's. What ends with the driver
  is the only like-for-like comparison against the engine roble exists to
  replace, `floor` above all, which no `cmd/pebble` benchmark performs at all. `bench/README.md` says what the phases are and, at more length,
  the three ways a comparison like this misleads a careless reader: a sync
  primitive that differs by platform, a block cache the previous phase warmed,
  and a clock read per step that costs as much as the step. Each of the three
  was caught producing a wrong answer before it was fixed, which is why they
  are written down rather than merely avoided.
  Compressed against uncompressed is a first-class comparison (§8), as is
  cooperative compaction against offloaded (§9), and the WAL on against off (§4).
  `run.sh` runs one environment, one CPU and 512 MiB a process, over a store
  that fits the block cache and one that does not fit in memory at all
  (`bench/README.md`, "One core, 512 MiB"); those comparisons are runs of the
  binary by hand rather than passes of its own.
  **Also against encina**, which is not apples-to-apples and is not meant to be:
  a copy-on-write B+tree with no compaction, no tombstones to step over and a
  format chosen to suit its data. That is the point — the gap between roble and
  encina on the same workload is the price of the compatibility requirement, and
  §2 claims that price is worth paying without ever putting a number on it.

## 13. Milestones

Ordered so that the compatibility risk is resolved before the engine exists.

1. **M1, the format crate.** `roble-format` (§7), plus `roble sst dump` and
   `roble manifest dump`, validated against the golden corpus and the Go
   harness's dumps. No engine. If Pebble's format is going to be a problem, it
   is a problem here, cheaply.
2. **M2, read-only.** Open a Go-written database or checkpoint: markers,
   MANIFEST, version, table cache, block cache, the full iterator tree with
   tombstones, `get` with every comparison, forward and reverse scans, and a
   clean refusal for each structure of §3 that roble declines. Both-way interop
   in CI, minus the writing half.
3. **M3, writes, the WAL and flush.** Memtables, indexed batches, the WAL and its
   replay, flush, version edits, marker rotation, obsolete-file deletion,
   `LOCK`. Two gates: **Go Pebble opens and reads what roble wrote**, closing
   the round trip, and **a crash replays** — to the last synced write with the
   WAL on, to the last flush with it off, both tested (§12).
4. **M4, compaction.** Leveled with L0 sublevels, the picker, cooperative
   execution in units of work, write-stall accounting.
5. **M5, the Oxia surface.** Snapshots, `checkpoint`, the prefix iterator with
   `Lookup`, batch checksums that match Go, metrics, creating and destroying a
   database, and the `KV` mapping of §11 complete. A Rust data server can serve
   a shard.

## 14. Open questions

Stated rather than quietly decided:

- **Compression (§8) is settled: Snappy below L0, L0 uncompressed** — Oxia's
  own configuration, and now roble's default (`Options::compression`). Until
  this, roble could read compressed tables and never write one, and held three
  to four and a half times the disk of Pebble or RocksDB on compressible values
  (`--value-chunk 32` and `8`) for no measurable read gain against a warm
  cache. A store beyond memory is where it cost: the bytes that did not fit
  were bytes read from the device.

  The choice is among what Go Pebble reads at the pin: none, Snappy and Zstd.
  **LZ4 is not one of them** — Pebble's `CompressionIndicator.Algorithm`
  panics on indicator 4 — so a table with one LZ4 block is a table Pebble
  cannot open. MinLZ, the codec Pebble prefers on amd64, becomes writable
  with Pebblev6 (§3). Both were measured anyway, as references.

  CPU per 16 KiB data block, one core of the Linux VM, over 4,000 blocks of a
  settled store whose values are a 32-byte chunk repeated four times
  (decompression includes the buffer it lands in, as a miss pays):

  | codec | stored | compress | decompress |
  |---|---|---|---|
  | none | 1.000 | — | 0.3 us (a copy) |
  | Snappy | 0.340 | 5.5 us | 1.8 us |
  | LZ4, reference | 0.333 | 6.6 us | 1.9 us |
  | MinLZ fastest, reference | 0.331 | 7.8 us | 1.2 us |
  | MinLZ balanced, reference | 0.320 | 15.0 us | 1.3 us |
  | Zstd 1 | 0.295 | 15.9 us | 2.5 us |
  | Zstd 3, Pebble's level | 0.292 | 22.6 us | 3.0 us |
  | Zstd 6 | 0.289 | 44.9 us | 2.9 us |

  LZ4 buys nothing over Snappy here. On incompressible values every codec
  stores the block as built (Pebble's 12% rule) and still pays to find that
  out: Snappy 5.9 us a block, Zstd 3 79 us. MinLZ is the Rust `minlz` crate,
  whose encoder is not Go's, so its row says what MinLZ is like rather than
  what Pebble would write.

  End to end, the beyond-memory scenario (six million records, one core,
  512 MiB, `--value-chunk 32`), roble uncompressed against its new default,
  with RocksDB at Snappy on every level beside them:

  | | roble none | roble Snappy | RocksDB Snappy |
  |---|---|---|---|
  | store after settle | 846.5 MiB | **293.2** | 280.1 |
  | settle cpu | 3.76 s | 3.48 | 1.11 |
  | get | 43,588/s | **83,161** | 92,799 |
  | floor | 40,692/s | **77,973** | 71,556 |
  | scan-short | 14,390/s | **22,973** | 27,661 |

  Snappy's compaction CPU is level with none's, within one round's noise:
  3.48 s against 3.76 here, 4.15 against 3.69 in the earlier round — the CPU
  compression spends is about what writing a third of the bytes saves. Every
  roble settle figure in this item includes the bench's settle loop polling
  the picker while the store's own task merged — some 680,000 times a run,
  every merge the task's. It now only waits for that task
  (`Db::compaction_settled`), as RocksDB's settle waits for its threads,
  which took the Snappy settle from a median of 3.58 s to 2.57 over three
  interleaved rounds, the load unchanged.
  RocksDB's 1.11 s is not the same work either: its threads did most of the
  merging during the load, whose row carries it (4.80 s against roble's
  3.63), and its flushes compress where roble's L0 does not, so roble writes
  1.83 GB across load and settle against RocksDB's 0.87. It
  close to doubles every random read, because a third of the store is what
  has to come from the disk. At `--value-chunk 8` Snappy's 156 MiB fits the
  budget whole and `get` went from 45,952 to 425,180 a second (one round,
  before the fix below). The
  load row's size stays larger than RocksDB's because roble leaves L0
  uncompressed and RocksDB does not.

  **Zstd is an option, not the default.** It stores 13% less than Snappy at
  `--value-chunk 32` (255 against 293 MiB) for 1.4x (level 1) to 2.2x
  (level 3) the settle CPU, and once a store fits the budget its slower
  decompression shows: at `--value-chunk 8`, Zstd 3's `get` was 324,397 a
  second against Snappy's 425,180. On incompressible values it spends 2.3x
  (level 1) and 3.5x (level 3) none's settle CPU for nothing. The Zstd rows
  and that `--value-chunk 8` comparison are one round measured before the fix
  below, when compressed tables were split smaller than their target.

  **The fix:** a compaction split its outputs on the bytes of keys and values
  that went in, which with compression closes every table at a fraction of
  its target. It now splits on `TableWriter::estimated_size` — blocks as
  stored, plus the open block and index — as Pebble's `EstimatedSize` does.
  That moves uncompressed tables too, slightly: the estimate now counts the
  blocks as built, prefix compression and index included, where it counted
  raw keys and values.
  One round per cell throughout; the noise a single round carries under this
  standard (`bench/README.md`) is far below a 1.9x read change but not below
  the few-percent differences in the in-cache scenario, which moved within
  noise for every codec.
- **Cooperative compaction (§9)** is a bet on small shards. The offload path is
  designed but unbuilt; the benchmark decides whether it gets built, and the
  `cpu s` column is where to look — a cooperative compaction's cost lands on
  the thread that owns the store, so it shows up in the phase that drove it
  rather than in a background total.

  The first reading is in §9, from holding both engines to one CPU: roble loads
  at 1.08x RocksDB's rate for 0.82x its CPU, and then pays 1.66x its CPU to
  settle. So the bet pays on the write path and the debt lands in `settle` —
  which is where an offload path would be aimed, and not at the lookups, which
  the same comparison showed are not core-bound at all.
- **What reverse iteration costs** is now measured, and it is not the problem
  this entry expected. `bench/` runs every engine over the same workload, each
  scan direction in a process of its own so that neither reads a cache the
  other warmed. Over 300,000 records, reverse costs roble **1.19x** what
  forward does, Pebble **1.12x** and RocksDB **1.17x**. Three LSMs in one
  narrow band, roble inside it: `prev` is not a design problem.

  Stated as a band rather than a figure because that is what the runs support.
  An earlier run of the same comparison gave 1.26x for roble and 1.44x for
  Pebble, which would have read as roble being the better of the two; the
  forward rates move by a fifth between runs, so the ratio is good to about a
  tenth and no finer. Having two reference LSMs rather than one is what makes
  the band trustworthy where a single pair of numbers was not.

  What the same run exposed instead is that **roble is the slowest of the
  three on every read path** — scans 4x to 6x, `get` about 3x, `floor` about
  3.5x.

  The first reading of that blamed the picker, from the `levels` line alone:
  roble settles at `L0=1 L1=13` where Pebble reaches `L6=11` and RocksDB
  `L0=1`, so it looked like a scan merging fourteen iterators against one
  reading a level it could walk. `--layout` says otherwise. roble's thirteen
  L1 tables **partition the key space** — no overlapping pair, the same shape
  as Pebble's eleven at L6 — and a run small enough to leave two tables in
  each engine keeps the gap at 4.3x (roble 3.05 M/s, Pebble 13.2 M/s). So it
  is not the level shape and not the table count: **the cost is per step**.

  The step itself was where to look, and two of its costs are now gone. A
  step copied the user key out of **every** source before it could touch that
  source mutably — one allocation per source per step, so a store settled
  into fourteen tables made fourteen of them for every record returned — and
  it allocated the key it was about to hand out. Scoping the first borrow to
  the trailer's scalars costs nothing, and the second reuses the buffer the
  previous key vacated. Over 300,000 records: forward 1.94 to 3.11 M/s,
  reverse 1.61 to 2.27 M/s, CPU down a third. Against Pebble the gap goes
  from 6.3x to 3.9x forward.

  What was left was structural rather than wasteful, and it is now a heap.
  `least` compared every source's key to find the smallest, and
  `take_newest_visible` then walked every source again to step it past that
  key — a comparison per source, then a trailer decode and a user-key
  comparison per source — so a step cost about `2N` for `N` sources. The
  merge is Pebble's `mergingIterHeap` instead: **one heap that flips its
  comparator** rather than a `least` and a `greatest`, which is Pebble's
  `reverse`, with the second walk driven by it, because every version of a
  user key reaches the top of the heap before anything else does. A step
  costs about three whatever `N` is.

  **The forty this entry predicted had already gone when it was written.** It
  counted a settled store's fourteen tables as fourteen sources, which they
  stopped being when a source became a level: L1's tables are disjoint and
  are read as one run. So a settled store merges one to three sources,
  depending on where `settle` happens to stop, and paid two to six operations
  a record. The store that pays the `2N` is one with compactions still owed,
  and `bench/ --no-compactions` now holds that shape still — `L0=12`, every
  run — where leaving `settle` out did not. Over 300,000 records, eight pairs
  alternating the two binaries, each run loading a store of its own:

  | | sources | per record | `scan` | `scan-rev` | `scan-short` | `floor` | `get` |
  |---|---|---|---|---|---|---|---|
  | `L0=12` | 13 | 25.8 → 3.0 | 3.19x | 1.78x | 2.22x | 1.02x | 1.02x |
  | settled | 1–3 | 2–6 → 2–3 | 1.07x | 1.06x | 1.04x | 0.99x | 1.02x |

  Each ratio is the median over the eight pairs of the heap's rate against
  the linear scans', and `get` is the control: a point lookup never builds a scan, so what it does is the noise.
  The settled row sits close to that noise, which is §8's rule again — at one
  to three sources the merge was never most of a step, and a step waits on
  memory. `floor` does not move in either shape, and should not: it builds a
  scan per lookup and steps it once, so it pays a heapify that costs about
  what the linear walk did and never takes a second step to recover it.

  So the row that matters is the unsettled one, since Oxia serves from a
  store that is taking writes. And the gap to RocksDB and encina this entry
  opened with was measured over a settled store, where this moves a scan by
  some six per cent: whatever that gap is, it is not the merge.

- **What Pebble compatibility costs** has a number now, which §2 claimed was
  worth paying without ever naming it. Against encina on the same workload,
  roble reads 7x to 19x slower — `scan` 2.0 M/s against 50.8 M/s, `get` 283
  k/s against 1.92 M/s, `floor` 121 k/s against 2.32 M/s — and holds twice
  the resident memory, while writing about twice as fast with the WAL off.

  The gap is not all compatibility, and the honest reading is smaller than the
  headline. encina has no reverse iteration at all, so it cannot serve
  `KeyRangeScanReverse`, and no checkpoint, so it cannot serve a shard move
  either; both are Oxia requirements that roble meets and encina does not, and
  an engine that answers fewer questions can answer them faster. And a B+tree
  reads a page where an LSM merges levels, which is the structural difference
  §2 was weighing, not a cost of Pebble's file format. What the number does
  establish is the *scale* of what is being traded, which the design had left
  to the imagination.

  The write side runs the other way, and the benchmark's batch-against-single
  pass is where it shows. Written 300,000 records one commit at a time rather
  than in batches of 300, roble holds 0.84x of its batched rate and Pebble
  0.94x — a memtable absorbs a single insert — while RocksDB drops to 0.24x
  and encina to 0.006x, 2,237 records a second, with 36 seconds of CPU and a
  file grown from 44 MiB to 152 MiB. **That is a copy-on-write B+tree
  committing each record durably**, which is what it is for: a new root per
  commit and an fsync behind it. It is the expected shape rather than a
  finding, and it is not a like-for-like row either, since `--no-wal` asks the
  three LSMs for no sync at all while encina syncs regardless. What the pass
  is good for is the comparison *within* an engine — how much of its
  throughput depends on the caller batching — and there roble's answer is the
  flattest of the four.
- **A future Oxia that excises.** Virtual sstables are refused (§3) because
  nothing in Oxia creates one today — not the data path, and not the shard
  split, which streams a filtered snapshot. If Oxia adopts `Excise` or
  `IngestAndExcise`, roble needs them on both sides, and that is a release
  rather than a patch. Worth watching rather than deciding now.
- **Whether a table count explains a read cost: twice now, no.** `bench/`
  sets every LSM knob from roble's own constants, because RocksDB's defaults
  differ enough to decide a result — its 64 MiB target file size settled a
  34 MiB store into one table where roble leaves eleven. That looked like the
  reason its lookups led. Aligned to 2 MiB it has sixteen tables and its point
  reads move by under 1%: the filters absorb them. The scan gap went the same
  way, surviving a run with the table counts matched. So a shape that *should*
  cost more does not, twice, and the thing left both times is roble's cost per
  operation rather than the structure it operates over.

- **How closely the picker should follow Pebble's.** Compatibility does not
  require it, and this was filed as a question about noise. It still is: the
  three settle into visibly different shapes for the same 300,000 records —
  roble into one L0 table over thirteen at L1, Pebble into eleven at L6,
  RocksDB into a single file — and `bench/ --layout` prints all three so the
  difference can be seen rather than inferred. But roble's L1 partitions the
  key space as Pebble's L6 does, and the scan gap survives matching the table
  count, so the shape is not what is costing the reads. It remains a question
  about what Go Pebble will do with a shard roble shaped, not about speed.
- **What a cold read costs**, which the benchmark asked rather than answered.
  `bench/`'s `get-cold` phase closes every store, opens it again and does one
  point lookup, which is the state a shard Oxia has just been handed is in.
  roble's first read takes about 110 microseconds against Pebble's 28 and
  RocksDB's 4, where all three answer a warm `get` in about one. Four samples
  a run, so it is an order of magnitude and not a figure — but the order of
  magnitude is the point: roble consults a bloom filter per table whose bounds
  take the key in (§6), and on a shard it settles into fourteen tables that is
  fourteen footers, indexes and filters to read where Pebble has eleven and
  RocksDB one. Worth measuring properly before deciding anything, since the
  first read after a shard moves is a latency Oxia's clients see.

- **Whether roble should recycle log files**, as Pebble does. Not recycling is
  simpler and keeps `ReuseForWrite` out of `Fs` (§10); recycling saves a file
  creation and its directory sync per rotation, which with 4 MiB memtables is
  often. Reading a recycled log is required either way, so this is a performance
  question to settle in M3, not a compatibility one.
- **Publishing `roble-format` separately** is deferred rather than open. It is
  useful on its own — tooling, other engines, reading backups — and worth doing;
  it also pins roble's compatibility surface in public, and nothing is complete
  enough to publish until M1 lands. Revisit then.

## Non-goals (for now)

- Access to one store from multiple threads.
- A format of roble's own, or any format option Pebble does not define.
- Columnar blocks, blob files, or any format major version above the pin (§3).
- Merge operators, and any comparator but `leveldb.BytewiseComparator` (§3).
- Range keys and virtual sstables, in either direction (§3).
- Disaggregated or shared storage, and remote object stores.
- Transactions beyond atomic batches.
- Matching Pebble's compaction heuristics, as opposed to its files.
