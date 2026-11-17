# Fuzzing roble-format

The format fuzzing of DESIGN.md §12, under `cargo fuzz`. It is a crate of its
own, outside the workspace, because it needs nightly and the sanitizer flags
`cargo fuzz` builds with.

| Target           | Input                          | What fails it                                         |
|------------------|--------------------------------|-------------------------------------------------------|
| `decode_records` | a MANIFEST or WAL, as on disk  | a panic; an edit that changes through encode, decode  |
| `decode_block`   | one fetched block              | a panic, in any block reader or cursor move           |
| `decode_table`   | a whole sstable                | a panic, opening, dumping, seeking or filtering       |
| `roundtrip`      | structured (`arbitrary`)       | a table, batch, framed stream or fragmenting that does not give back what went in |
| `block_cursor`   | structured (`arbitrary`)       | the restart cursor off the entry a sorted `Vec` says  |

For the three decode targets, any answer is allowed: a decoder handed garbage
may return an error or a wrong value. A panic, an abort, a hang or an
allocation larger than the input could justify is never allowed, since any of
them takes down the process the store is embedded in.

## Checksums

Every block and every record chunk carries a CRC, so a mutated input almost
always stops at the checksum and never reaches the parser behind it. Under
`cfg(fuzzing)`, which `cargo fuzz` sets, `decode_fetched` and `RecordReader`
compute the checksum but do not refuse a mismatch. No other build sets the
flag.

## Running

```sh
python3 format/fuzz/seed.py          # seed corpora from the golden files
cd format
cargo +nightly fuzz run decode_table -- -max_total_time=300 -malloc_limit_mb=256
```

CI (`.github/workflows/fuzz.yml`) runs every target for a minute on a pull
request that touches the format crate, and for ten minutes nightly. It keeps
the corpus main's runs grow in the Actions cache, and uploads a failing input
as an artifact of the run.

`-malloc_limit_mb` is what turns "a length read from the file sized an
allocation" into a crash rather than a slow run. A crash leaves its input in
`format/fuzz/artifacts/<target>/`; `cargo +nightly fuzz run <target> <file>`
replays it, and `cargo +nightly fuzz fmt <target> <file>` prints a structured
one readably.
