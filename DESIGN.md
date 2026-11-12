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
> leave unflushed, taking each batch's own sequence number, and carries on
> writing where the replay stopped.
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
| Durability | **Oxia disables Pebble's WAL** (`DisableWAL: !UseWAL` in `kv_pebble.go`: "WAL is kept outside the KV store"). Oxia's replication log is the source of truth; the store need only be durable at `Flush()`, and Oxia replays everything after that. `SyncData` decides whether a commit syncs. roble works both ways: the WAL is on by default, and `disable_wal` is what Oxia sets (§4). |
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
| Two-level index blocks | yes | yes, when the index outgrows a block |
| Value blocks and value handles | yes | **no** |
| Range-deletion blocks, v2 (`rocksdb.range_del2`) | yes | yes |
| Range-deletion blocks, v1 (`rocksdb.range_del`) | **error** | never written |
| Bloom filter blocks (full key, bytewise `Split`) | yes | yes |
| The Pebblev4 obsolete bit | yes, honoured | **never set** |
| Block properties | yes, unknown collectors skipped | the minimum Go accepts (M1) |
| `SINGLEDEL`, `DELSIZED` | yes | never written |
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
- **Two-level indexes are written**, which is not a choice. Oxia leaves
  `TargetFileSizes` at Pebble's default — 2 MiB at L0, doubling per level — so a
  deep file runs to ~128 MiB and its index is many blocks long.
- **The value prefix byte is block decoding, not a value-block detail.** In a
  Pebblev3+ table every `SET` value is one prefix byte followed by the value,
  value block or not. A Pebblev2 table has no such byte, so the reader branches
  on table format — and that is why reading the older formats stays in, since a
  long-lived database keeps them in its deep levels.

Compression: roble reads none, Snappy and **Zstd**, and writes none and Snappy.
The block indicator is RocksDB's numbering, not Pebble's internal `Algorithm`
enum: zstd is **7** and MinLZ is 8, while 2 to 6 are zlib, bzip2, lz4, lz4hc and
xpress, which Pebble never writes. Reading 2 as zstd — the obvious guess, since
`internal/compression`'s own enum does number Zstd 2 — gets every Zstd block
wrong.

A Zstd block is `uvarint(uncompressed_len)` followed by a standard Zstd frame
(`internal/compression/zstd_nocgo.go`), RocksDB's convention, which Pebble
keeps. A bare frame would decode happily with a stock reader, so this is a
detail that only fails on real files. roble decodes through `ruzstd`, decode
only, into a buffer sized from the prefix, as Pebble's own `DecompressInto`
does; swapping it for the libzstd binding is one function if a benchmark asks.

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
**Uncompressed is roble's default**, which is a format-legal choice and the one
that keeps a cached block byte-for-byte its on-disk bytes (§8).

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
`RANGEKEYSET` 21. `SINGLEDEL` and `DELSIZED` are read but never written: Oxia
creates neither, and reading them costs two match arms where refusing them would
cost a dead shard if Pebble ever produced one on its own.

## 4. Decisions

| Topic | Decision |
|---|---|
| Format | Pebble's, pinned at §3. roble defines no format of its own, and has no format version of its own. |
| Durability | **A WAL, on by default.** `write(batch, sync)` appends the batch repr to the log as one record, syncs it if asked, and then applies it to the memtable; `flush()` writes a table, installs the version edit, and retires the logs that edit covers. `disable_wal` skips the log entirely, which is what Oxia sets: then `flush()` is the only durability point. A WAL is an integral part of an LSM rather than a feature of this workload, and roble has to read Pebble's logs regardless, so it is in v1 (M3) rather than deferred — but the no-WAL path is tested as a first-class configuration, because it is the one Oxia runs (§1). |
| Threading | One `!Send` store per thread, as encina: a compio runtime's, or whatever runs an env the caller implements. No locks or atomics outside wakers. Oxia already shards; a shard's store is owned by one thread. |
| Memory | **Caches belong to the thread, not the store** (§8): the block cache and the table cache are created per thread and shared by every store on it, within one budget. This differs from encina, where each store has its own, and follows Oxia, where hundreds of shards share one cache. No `mmap`. Direct I/O is the goal and not yet the state: it needs aligned frames under the block cache (§8, §10), so v1 reads through the page cache, as Pebble does. |
| Compaction | Cooperative on the owning thread by default, paced in units of work as encina paces a large merge, so a seeded run replays. Offload to a pool is a capability of the env, not a default (§9). |
| Allocation | No allocation on the read path for an uncompressed block, which is cached as read. A compressed block costs one buffer per miss, which is why uncompressed is the default. Batches, iterator state and compaction buffers reuse their memory. |
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
  behind. With `disable_wal` none of this exists and `flush()` is the only
  durability point (§4, §6).
- **Memtables are skiplists over an arena**, keyed by internal key (user key,
  sequence number, kind). Range deletions are kept apart from the point keys,
  as in Pebble, so a point read never walks tombstones it doesn't need. Pebble
  gives them a skiplist of their own; roble keeps them in a list sorted by
  start key, which serves that reason at a memtable's scale — a 4 MiB arena
  holds few tombstones, and they are searched rather than walked. A second
  skiplist replaces it the day a workload shows it needs one. A
  full memtable becomes immutable and queues for flush; the arena is released
  when its table is written.
- **Versions are immutable, and what they name is reference counted.** A
  version is the set of files per level, with L0 as sublevels. A flush or a
  compaction produces a version edit, which is appended to the MANIFEST and
  then applied; a reader holding an older version is unaffected, so a
  snapshot costs one reference count, as an encina iterator does. The count
  is kept per **table** rather than per version, because that is where roble
  already holds the references: a scan owns an `Rc<OpenTable>` for every
  table of the version it began on, so keeping those tables alive is keeping
  that version alive, and the sweep below asks whether a table has a reader
  rather than whether a version has a holder. Pebble counts the version and
  collects a file once no live version names it, which is the same guarantee
  with a second structure to keep in step with the first.
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
  edit collects the table as an interrupted flush's leftovers. Both are
  cooperative within their permit, so a write never waits on a compaction's
  merge for more than a slice of work (§9).

  A **write** does not take the permit, and so is not yet safe against a
  flush: a write that lands after a flush has read the memtable and before it
  swaps in a fresh one belongs to a memtable the flush is about to drop,
  whose log its edit retires. Serializing writes against flushes would close
  it and would be the wrong fix — Pebble's is to make the memtable immutable
  at the *start* of a flush and send writes to a new one, which is the
  machinery the memtable write stall needs anyway (§9). Until then, one task
  per store.
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
  names it at no level **and no reader is open for it** — the table cache's
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
between the batch and the file.

Two details of that contract are measured rather than assumed. Oxia checksums
the **whole** repr, header included (`db.go`: `batch.Checksum(previousChecksum)`),
and it does so **before** `Commit`, so the eight header bytes that hold the
sequence number are still zero at checksum time — Go Pebble's repr for an
empty batch is twelve zero bytes. roble therefore leaves the sequence number
zero until commit stamps it, and stamping it touches nothing but the header.
And which kinds carry a value is not inferable from the kind: `SETWITHDEL`
does not, while `DELSIZED` does (`batchrepr.Reader.Next`).

`write()` assigns sequence numbers, appends the repr to the log as one record,
syncs the log if `sync` is set, and then applies the entries to the memtable — in
that order, so nothing is ever readable that a replay would not recover. With
`disable_wal` the log step is skipped and durability is `flush()`'s job alone
(§4). A `sync` is then **refused** rather than ignored, which is what Pebble
does (`DB.Apply`: "pebble: WAL disabled") and the correction this document
needed: a caller that asked for durability and got `Ok` would have none, and
finding out would mean losing the write. A batch whose entries would exceed the memtable
rotates it first, which rotates the log with it.

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
point lookup — bloom filter per table, then one block read. The other four are a
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

### `delete_range(start..end)`

A range tombstone, written to the memtable's range-deletion skiplist and then to
a table's range-deletion block — **only** there, never interleaved with point
keys, so a reader that walks data blocks alone sees a table with tombstones as
though it had none. Within the block they are fragmented: sorted by start key,
with equal starts sharing an end and carrying strictly decreasing sequence
numbers, which Pebble enforces when writing and roble enforces in both
directions. Unlike encina, which drops covered subtrees and
leaves nothing behind, every later read in that range checks the tombstone until
compaction drops it. That is the cost the format chooses for us (§2).

### Iterators

Forward and reverse, bounded, over a snapshot. `seek_ge` / `seek_lt` /
`next` / `prev`.

**The snapshot is a sequence number.** A scan takes the highest sequence
number in use when it is made and ignores anything above it, so a write that
happens while it is open is simply not its business. Nothing is copied and
nothing is reference counted: sequence numbers only go up, and a table's
contents are all older than any scan, so the rule has to be applied in exactly
one place — the memtable. An explicit `snapshot()` handle is the same
mechanism with the number kept for longer (§4). Reverse iteration over an LSM is more expensive than forward,
which Oxia's `KeyRangeScanReverse` uses; it is worth measuring early.

There are two reasons it is dearer, and they compound. Within a block,
prefix compression only runs forwards, so stepping back means rescanning from
the restart point before the current entry. Across blocks, a backward step at
the start of a block has to load the previous one and walk it to its end,
where a forward step loads the next and stops at its first entry.

A prefix iterator additionally serves `Lookup` for keys outside its prefix,
which Oxia uses to read internal keys while scanning: it keeps a second iterator
over the same snapshot rather than moving the first.

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

`flush()` makes the memtable an immutable one, writes it as a table, appends the
version edit — carrying the new minimum unflushed log number — and syncs: the
point at which Oxia's data becomes durable. The logs below that number are then
obsolete, and are deleted like any other obsolete file — including one Pebble
had kept back to recycle, which roble does not do and so has no use for.

Past `Options::max_manifest_bytes` — 128 MiB, Pebble's `MaxManifestFileSize` —
that same flush **rotates** the MANIFEST: a fresh file holding one snapshot edit of the
whole version, and then `marker.manifest.*` moved to name it. The marker moves
last and is the one-way door; roble syncs the directory between creating the new
marker and letting the old one go, so no crash can land with no marker at all.
roble does not rotate at open as Pebble does, which keeps opening a database
something that does not write to it.

`snapshot()` pins a version and a sequence number. `checkpoint(dir)` writes a
directory Go Pebble opens: hard links to the live tables, a fresh MANIFEST
holding one edit that describes the version, the markers, and an `OPTIONS-*`.
Oxia streams that directory, so it is the single most important interop test
(§12).

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
already acknowledged. With `disable_wal` there is nothing to replay and the
database opens as of the last flush.

As built, the replay stops at the first record of the **last** log that does
not read, and the store carries on writing from there — overwriting what was
half-written, which was never acknowledged. A record that does not read in an
*earlier* log is corruption and refused, because a later log exists only if
that one was finished.

That rule is looser than §6 states in one way, and the looseness is
deliberate: it tolerates a torn record anywhere in the last log, not only at
its tail. Telling a torn tail from damage in the middle of the same file would
mean deciding what the bytes after it were supposed to be, and stopping at the
last record that reads is the answer that loses nothing either way.

Note that `disable_wal` leaves an **empty log file** behind rather than no log
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
  count, since an open reader costs file descriptors as well as memory.

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
defaults to uncompressed and measures the trade**: Oxia already runs L0
uncompressed, and its values are small records with shared structure, so the
disk-space cost may be smaller than it looks. Reading Snappy and Zstd is
required regardless, for files Go wrote (§3).

**Memtables** are arenas of a configured size, 4 MiB to match Oxia. A store's
memory is its memtables plus its share of the thread's caches plus its
compaction buffers, and the budget covers all three.

## 9. Compaction

As built, `compact(level, tables)` does the merge and `pick()` chooses what to
feed it, with `compact_once()` joining the two. What a merge drops, and why
each is safe, is written out in `src/compact.rs`; what it does not drop yet is
a range tombstone that has nothing left below it, since carrying a spent
tombstone costs a reader a comparison and dropping a live one brings a key
back from the dead. The output is split at `Options::target_file_size` — 2 MiB
for L0 and twice that for each level below, as Pebble's `TargetFileSizes` —
and a tombstone straddling a split is clipped at it, so the tables either side
meet at one user key rather than overlapping across it. L0 sublevels and the
write-stall counters are still owed.

A compaction is paced in units of work (`crate::work`): every
`Options::compaction_unit` entries it hands the executor back and resumes, so
a reader or a writer on the same core waits a slice rather than a merge. The
unit is entries and not time, because §10 lets a store read the clock only
where the env allows it, and a job that yielded on a timer would stop a seeded
run replaying.

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
and roble owes those. And what still has no trigger at all is an **idle**
store draining a backlog: nothing runs a compaction behind the caller's back,
which needs a store that can be handed to a task of its own — an ownership
question rather than a pacing one.

Compaction is the cost encina does not pay, and the only background CPU work in
roble. Leveled, with L0 as sublevels, and a **picker** — the component that
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

**Write stalls.** A write that cannot find room in a memtable waits for a flush,
and too many L0 files make a write wait for a compaction. Neither depends on the
WAL: a log append is not what a write waits on. Oxia watches Pebble's
`WriteStallBegin` events and counts
them, so roble exports the same two: memtable stalls and L0 stalls.

## 10. Concurrency and I/O

Reuse encina's shape: a store opens in an `Env`, which opens its files and runs
its tasks, and all I/O goes through a `Disk` whose requests move owned buffers.
A request is any byte range: a disk may not refuse an offset or a length, which
is what rules `O_DIRECT` out of the open and puts it under the block cache
instead (§8). `Db` is `!Send` and `!Sync`, a cheap `Clone` handle shared by one
thread's tasks. `DefaultEnv` uses compio.

**What has to change, and why.** encina has one file, created once. An LSM
creates, renames, links and deletes files constantly, and its correctness
depends on directory syncs. So the env grows a filesystem, which is Pebble's
`vfs.FS` in Rust terms:

```rust
pub trait Fs {
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
slot without a boxed future, and `real_time()` decides whether the store may
read the clock at all. One owner per directory, through `LOCK`.

Oxia injects a `vfs.FS` today, including `vfs.NewCrashableMem` in its tests, so
this trait is the same seam it already relies on.

## 11. API sketch

```rust
let caches = Caches::new(CacheOptions::default().block_cache_bytes(512 << 20));
let db = open_with(&DefaultEnv, "data/shard-3", Options::new().caches(caches)).await?;

let mut batch = db.batch();            // indexed: readable through itself
batch.put(b"/users/alice", b"...")?;
batch.delete_range(b"/sessions/", b"/sessions0")?;
let seen = batch.get(b"/users/alice", Comparison::Equal).await?;
batch.commit(false).await?;            // appended to the WAL, not synced
db.flush().await?;                     // a table; the logs it covers can go

let last = db.get(b"/users/zzzz", Comparison::Floor, |k, _v| k.to_vec()).await?;

let snap = db.snapshot();
let mut iter = snap.range(&b"/users/"[..]..&b"/users0"[..]);
while let Some((k, v)) = iter.next().await? { /* ... */ }

db.checkpoint("snapshots/3").await?;   // a directory Go Pebble opens
```

The Oxia mapping, so the surface is complete rather than approximately right:

| Oxia `kvstore` | roble |
|---|---|
| `NewWriteBatch`, `Put`, `Delete`, `DeleteRange`, `Commit` | `db.batch()`, `put`, `delete`, `delete_range`, `commit` |
| `PutMarshalable` | `put_with(key, len, \|buf\| …)` |
| batch `Get`, `FindLower`, `KeyRangeScan`, `RangeScan` | indexed batch iterators (§6) |
| batch `Count`, `Size`, `Checksum` | over the Pebble batch repr, so checksums match Go |
| `Get` with `ComparisonType` | `get` with `Comparison` |
| `KeyRangeScan`, `RangeScan`, `KeyRangeScanReverse` | `range`, `keys`, `range_rev` |
| `KeyPrefixIterator` with `Lookup` | `prefix` iterator (§6) |
| `Snapshot`, `SnapshotLoader` | `checkpoint`, and opening a directory of received files |
| `Flush`, `Delete`, `DiskSpaceUsage` | `flush`, `destroy`, `disk_usage` |
| `vfs.FS`, `CacheSizeMB` | `Fs` (§10), `Caches` (§8) |
| `SyncData`, `UseWAL` | `Sync` on commit, and `disable_wal` in `Options` (§4) |
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
  `SINGLEDEL` and `DELSIZED`, which roble reads and never writes, are written
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
- **Format fuzzing.** `roble-format` round-trips blocks, tables, version edits
  and batch reprs under `cargo fuzz`, and decodes arbitrary bytes without
  panicking — the corpus seeded from the golden files.
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
  the last `flush()` with `disable_wal`, which is Oxia's configuration and so
  gets its own pass of every scenario. A half-written table, a torn log record or
  a half-rotated marker must leave the database openable.
- **Overlap paths**, by the same method. Nothing the simulated filesystem does
  blocks — every operation finishes in the call that starts it, which is what
  makes a replay a replay — so two of a store's operations can never interleave
  there by accident, and the one thing a real disk guarantees is that they
  will. `SimFs::pause_at(n)` is one await, put where a test asks for it, and a
  scenario sweeps it over every operation of the first of two operations run
  together, exactly as the failure paths sweep an injected error. What must
  hold at every one of them is §5's rule: the database still opens, and it
  holds what was written to it.
  CI measures coverage on every pull request (`cargo llvm-cov`), posts the totals
  as a status, and lists the lines and regions no test runs, such as the error
  branch of a `?`. An uncovered failure path needs a test or a written reason.
- **Benchmarks** against Go Pebble on Oxia's shape: small records, hundreds of
  shards, batches per write, floor lookups, reverse scans, and a checkpoint under
  load. Compressed against uncompressed is a first-class comparison (§8), as is
  cooperative compaction against offloaded (§9), and the WAL on against off (§4).
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
   `Lookup`, batch checksums that match Go, metrics, and the `KV` mapping of §11
   complete. A Rust data server can serve a shard.

## 14. Open questions

Stated rather than quietly decided:

- **Uncompressed by default (§8)** needs the measurement before it is a default.
  If Snappy's space saving on Oxia's values is large, the zero-copy read path is
  worth less than it looks.
- **Cooperative compaction (§9)** is a bet on small shards. The offload path is
  designed but unbuilt; the benchmark in M4 decides whether it gets built.
- **What reverse iteration costs** is unmeasured, and it is not an optional
  feature: `KeyRangeScanReverse` is on an Oxia serving path, and an LSM pays more
  for `prev` than for `next`. So it is measured in M2 against Go Pebble over the
  same database, and the result is a **gate on M2**, not a number reported in
  M5 — if roble is materially slower there, that is a design problem to fix while
  the iterator tree is still being built.
- **A future Oxia that excises.** Virtual sstables are refused (§3) because
  nothing in Oxia creates one today — not the data path, and not the shard
  split, which streams a filtered snapshot. If Oxia adopts `Excise` or
  `IngestAndExcise`, roble needs them on both sides, and that is a release
  rather than a patch. Worth watching rather than deciding now.
- **How closely the picker should follow Pebble's.** Compatibility does not
  require it. Diverging risks a shard whose shape Go Pebble then compacts
  differently, which is legal but makes performance comparisons noisy.
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
