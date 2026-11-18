# Pebble's test data

`fragmenter` is Pebble's own, copied unchanged from
`internal/keyspan/testdata/fragmenter` at `github.com/cockroachdb/pebble/v2`
v2.1.7, the pin of DESIGN.md §3. It is Pebble's and is under Pebble's licence,
which is in `LICENSE` beside it.

`tests/fragment.rs` runs every `build` in it through
`roble_format::rangedel::fragment` and requires the fragments Pebble prints.
The `get` commands test Pebble's snapshot visibility over the result, which
the format crate has no counterpart for, and are skipped. CI checks that the
copy is still the file at the pin.
