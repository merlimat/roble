# Pebble's test data

`l0_sublevels` is Pebble's own, copied unchanged from
`internal/manifest/testdata/l0_sublevels` at `github.com/cockroachdb/pebble/v2`
v2.1.7, the pin of DESIGN.md §3. It is Pebble's and is under Pebble's licence,
which is in `LICENSE` beside it.

`tests/version.rs` reads every `define` and `add-l0-files` in it and requires
roble's `Version::l0_sublevels` to give the count Pebble prints. The other
commands test Pebble's compaction picking, which roble does not share (§9),
and are skipped. CI checks that the copy is still the file at the pin.
