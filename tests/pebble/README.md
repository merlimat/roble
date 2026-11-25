# Pebble's test data

`l0_sublevels` is Pebble's own, copied unchanged from
`internal/manifest/testdata/l0_sublevels` at `github.com/cockroachdb/pebble/v2`
v2.1.7, the pin of DESIGN.md §3. It is Pebble's and is under Pebble's licence,
which is in `LICENSE` beside it.

`tests/version.rs` reads every `define` and `add-l0-files` in it and requires
roble's `Version::l0_sublevels` to give the count Pebble prints. The other
commands test Pebble's compaction picking, which roble does not share (§9),
and are skipped. CI checks that the copy is still the file at the pin.

`range_del` is Pebble's `testdata/range_del`, copied unchanged the same way.
`tests/range_del.rs` builds each `define`'s LSM — memtables as a log it
replays, tables written at their levels — and asks `get` and `iter` what Pebble
answered. Every read there is at a sequence number, so each is asked of the
LSM less its entries numbered at or above it, which is the same answer read at
the newest one. Skipped: defines holding a MERGE, which roble refuses; two
whose layers hold older sequence numbers than the layers below them, which no
store writes and Pebble's own comment calls unnatural; and the reads after a
`compact`, which are of Pebble's compaction; `tests/range_del.rs` writes out
by hand each state Pebble prints after a `compact`, and makes those reads of
it.

`level_checker` is Pebble's `testdata/level_checker`, copied unchanged the
same way. `tests/version.rs` writes each `define`'s tables at their levels and
requires `Db::check_levels` to find a fault where Pebble's `CheckLevels` does,
naming each key and range deletion Pebble's names, at the same level.
Skipped: defines that turn off the writer's order checks or write range
deletions unfragmented, whose faults are in a table's own order, which the
table reader refuses; defines holding a MERGE; and one keyed for Pebble's test
comparer, under which `a@9` sorts before `a@6`.

`version_edit_apply` is Pebble's `internal/manifest/testdata/version_edit_apply`,
copied unchanged the same way. `tests/version.rs` builds each `define`'s
version and applies each `apply`'s edits to it, and requires the version
Pebble prints, or a refusal where Pebble refuses: each level below L0 the same
tables in the same order, and L0 the same tables in as many sublevels.
Skipped: the cases of virtual tables and blob files, which arrive above the
pin.

`tombstone_elision` is Pebble's `internal/compact/testdata/tombstone_elision`,
copied unchanged the same way. For each point deletion an `elide` asks about,
`tests/version.rs` lays out the `define`'s tables below the compaction's
output, compacts a table holding the deletion out of the start level, and
requires roble to drop it where Pebble does. The one exception is a key at a
table's exclusive sentinel, which roble's rule reads as inside the table and so
keeps the deletion. Skipped: the range deletions, which roble never drops.
