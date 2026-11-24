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
flushes, full compactions, snapshots, batches, checkpoints, walks, held
iterators and checks.
Both engines
run with memtables and tables small enough that flushes and compactions keep
happening, and Pebble with its own automatic compactions on, as Oxia
configures it otherwise: the format major version of the pin, a ten-bit bloom
filter, L0 uncompressed and Snappy below. Every other roble turn flushes and
compacts inline, inside the writes that need it, and the rest in the store's
own tasks, as a store is opened by default, so that a flush or a merge runs
beside the turn's reads, and a kill in such a turn may find one under way.
Some turns are killed rather than closed.

Half the turns write with the WAL off, as Oxia runs Pebble: roble's with
`Wal::Off`, Pebble's with `DisableWAL`. Their segment starts `segment
wal-off` and ends with a `flush`, since with the WAL off nothing else makes a
write last, and Oxia flushes before it closes. A turn with the WAL off opens
what one with it on left, its log included, and the reverse, so each engine
replays logs the other wrote and has to retire them once it has flushed what
they held.

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

A walk's iterator lives inside one operation. `iter` makes one that is held,
up to three in a turn, and `itermove` moves one a few steps at a time from
wherever it was left, against the map as it was when the iterator was made,
with the turn's writes, flushes and compactions landing between its moves
until `uniter` closes it: an iterator reads the store as of its making, and
holds what it needs to, the memtables and the tables of its version, for as
long as it lives.

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

`biter` makes a held iterator through the open batch, up to two at a time
beside the store's three, and `itermove` moves it while the batch goes on
taking writes, against the map with the batch's writes laid over it as both
stood when the iterator was made. An iterator reads a batch as the batch was
at its making, as it reads the store: Pebble's does until `SetOptions`
refreshes it, which the runner never calls, and roble's has no such call.
Most are made a while after their batch, so that the batch holds writes from
before the making as well as after, and `uniter` closes them before the
batch's `commit` or `abort`.

`checkpoint <id>` writes a checkpoint into a directory beside the store, named
by an id no other checkpoint of the script has. `cpcheck <id>` opens it as a
store, checks it against the map as it was when it was taken, writes the few
puts the line carries into it, and closes it: whoever receives a checkpoint
opens it and goes on writing, so a checkpoint has a history of its own from
there, and the next `cpcheck` checks those puts too. The turn after the one
that took a checkpoint opens it first, so the other engine reads it as its
taker wrote it, Pebble's with the logs it copies in and roble's with none;
later `cpcheck`s, by either engine, open what the last one left. A kill can
spoil that order, by cutting the next turn before its first `cpcheck`s, and
the last turn's checkpoints are never read. Pebble's runner takes its checkpoints with `WithFlushedWAL`,
since its writes are not synced and a checkpoint without it may miss them.
With the WAL off a checkpoint is taken flushed, by roble's own `checkpoint`
and by Pebble's runner calling `Flush` first, as Oxia does before it takes
one: Pebble's checkpoint copies only tables then. A `cpcheck` in such a turn
flushes the checkpoint's store before it closes it, for its puts to last.

Beside the map, each engine checks the order its store keeps: what lies above
is newer than what it overlaps below. Compactions rely on that order, and so do
both engines' scans, which pass over the keys a range deletion covers in the
levels below it without reading their numbers. roble's runner calls
`Db::check_levels` as it opens the store, at every `check`, in every checkpoint
it opens, and before it closes. Pebble's runs Pebble's own `CheckLevels` on
every version it installs (`DebugCheck`), its open's included, so what roble
left is checked as Pebble opens it. roble's check is Pebble's, with one rule
more: a range deletion above a newer key it covers, which hides that key. It
also holds one write found in two places to be a fault, where Pebble's finds
that only when its heap gives the deeper copy first.

A disagreement names the engine that read wrongly, the segment and the
operation, and what the other engine wrote before it is the segments before.

## Kills

One turn in three, by either engine, is killed instead of closing. `run.sh`
names an operation at random, or a quarter of the time the open, and up to 3
ms; the turn starts a timer as it begins that one and, when it fires, ends
itself as a kill would (Pebble's sends itself SIGKILL, roble's aborts),
wherever it has got to. A kill turn syncs its writes, since Pebble's unsynced
ones wait in its own buffers and a kill loses them; roble's are the OS's once
written, and syncing runs its other path. With the WAL off there is nothing to
sync, and Pebble refuses a synced write.

A turn prints `done <i>` after each operation, so the script can be cut where
it stopped (`differential truncate`): the operations done stay, an open batch
is aborted, and a write that was under way becomes `maybe`, since it may or
may not have landed. With the WAL off, so does every write after the last
`flush` or `checkpoint` the turn finished: a kill loses the memtable, and what
lasts is the writes up to wherever the last flush to finish, the script's or
the store's own, cut them off. The next turn, the other engine's, opens what
the killed one left, reads it whole, and finds how many of the `maybe`s, taken
in order, make what it holds. It prints that, `differential resolve` writes it
into the script, and from there the history is one again. With the WAL on,
every acknowledged write must be there, and nothing else. With it off, every
write the last finished flush covered must be there, then some first ones of
the rest, in order, and nothing else. A `cpcheck` under way when the turn died may or may
not have written its puts into the checkpoint, and nothing decides which, so
its checkpoint is not read again; a `cpcheck` of a checkpoint whose turn died
before taking it says so and does nothing.

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

Each new kind of operation changes the mix, so a seed named in this README
makes the history it made when it was written only until the next one; the planted bugs
are the ones to put back and run again.

Turns with the WAL off catch #226's bug put back. There a flush with the WAL
off left the log the open had replayed unretired, so every later open
replayed it again, and a key deleted since came back once a compaction had
dropped its tombstone. With that fix taken out, 2 of the ten seeds from 5000
fail with `CRASH=0` (5004 and 5009), each in a Pebble turn that finds one key
more than the history. The differential before WAL-off turns, whose roble
never ran without a WAL, passes all ten. The level checks find the same two
sooner, by the write that came back rather than by the key it brought back:
5004 a turn earlier, in roble's, which opens a checkpoint whose log such a
flush left live and finds a put in its memtable above itself in L0; and 5009
as Pebble opens the store, where its own check finds a put in L0 and in L1.

A roble runner that says a flush is done without flushing, for all but its
turn's last, checks the kill path. It passes the ten seeds from 6000 with
`CRASH=0`, since the last flush writes everything out. It fails 5 of them
with `CRASH=1`: a kill after such a flush leaves the store without writes the
turn had counted sure, and the next turn finds the store holds none of the
maps the writes in doubt could have left. The same in Pebble's runner, so
that roble's turn is the one to decide, passes them all with `CRASH=0` and
fails 4 with `CRASH=1`.

Checkpoints found roble refusing a log Pebble wrote: `corrupt: internal key
kind 3`, on the first seed. Kind 3 is Pebble's `LogData`, which a batch can
carry to the log and which Pebble's checkpoint writes itself, alone in a
batch, to sync the log first. Pebble steps over it when it applies a batch,
and roble's batch reader now does too. Checkpoints also catch, on the first
seed from 8000, a checkpoint that skips its flush (Pebble opens it and finds
none of the keys still in roble's memtable) or records a last sequence number
below its tables'. One whose next file number is too low is not caught, and
cannot be: both engines take the number past every file in the directory as
they open it.

Held iterators catch, on the first seed from 9000, a scan that reads at the
store's latest sequence number rather than the one it was made at; twelve
seeds from 9000 generated with walks in their place pass, since a walk takes
no writes between its moves. They also catch the sweep removing a table a
live scan still holds, which snapshots catch too.

Held iterators through the batch catch a scan over a batch that sees what
the batch was given after the scan was made, as roble's did before #235;
`bwalk` never did, since it walks a scan as soon as it makes it. With that
fix taken out, all ten seeds from 1000 fail with `CRASH=0`, each in a roble
turn at a move of an iterator `biter` made 10 to 133 batch writes before:
onto a key past one a later `bdelrange` hid, onto a value a later `bput`
replaced, or onto a key the batch was given later. With the fix, all ten
pass.

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

Turns with the store's tasks on catch a read that skips the memtable a flush
has frozen and is writing out, in the first seed from 4000. Inline, no read
runs while a flush is under way, and the same twelve seeds pass with every
turn inline. Half of roble's kills land in turns with the tasks on (10 of 20
over ten seeds with `CRASH=1`); whether a flush or a merge of theirs was
under way at the moment is not something the turn can tell.

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
