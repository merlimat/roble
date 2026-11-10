# MinLZ decode compatibility

Checks that the Rust `minlz` crate decodes MinLZ blocks produced by Go's
`github.com/minio/minlz`, which is what Pebble compresses with when a
compression profile selects MinLZ.

**This is not needed at roble's current pin, and that is a correction.** An
earlier version of this file — and of DESIGN.md §3 — claimed that Oxia's
key-encoding conversion writes MinLZ index blocks on any node that is not
arm64, and so that refusing MinLZ would refuse the in-place upgrade roble
exists to make possible. That was wrong. Pebble checks whether the table format
supports the compression profile and, below Pebblev6, swaps the *entire*
profile for Snappy rather than only the MinLZ part
(`sstable/options.go`: `tableFormatSupportsCompressionProfile`). Measured at
Pebblev4, `MinLZCompression` produces Snappy blocks and `GoodCompression`
produces Zstd data blocks with Snappy index blocks on arm64, or all Snappy
elsewhere. No MinLZ block reaches a table at this pin, and `roble-format`
refuses one.

The check stays for two reasons: it is cheap, and MinLZ goes live the moment
the pin moves to Pebblev6, at which point the crate is already known good
rather than a question to answer under time pressure.

## What this checks

Pebble compresses a block with `minlz.Encode(dst, src, level)` — the block
format, not the stream — and reads it back with `DecodedLen` and `Decode`
(`internal/compression/minlz.go`). Above `minlz.MaxBlockSize` (8 MiB) it falls
back to Snappy *and tags the block Snappy*, so an oversized block needs no
special case.

`harness/minlz-fixtures` compresses 12 payloads with `github.com/minio/minlz` at
the version Pebble v2.1.7 pins, at both `LevelFastest` and `LevelBalanced`, and
round-trips each through Go first so that a harness bug cannot pass as a crate
bug. The payloads cover empty, one byte, 16 KiB and 64 KiB blocks shaped like
index blocks (shared key prefixes plus varint handles), filter blocks and
protobuf-ish records, incompressible blocks that encode as literals, 1 MiB, and
exactly `MaxBlockSize`.

This crate then decodes all 24 pairs through the three calls roble needs —
`decompressed_len` (Pebble's `DecodedLen`), `decompress`, and `decompress_into`
an exactly-sized buffer (Pebble's `DecompressInto`, and roble's frame path) —
and asserts `MAX_BLOCK_SIZE` agrees with Go's. It then feeds 588 truncated and
bit-flipped inputs through the same calls and requires an error rather than a
panic from each: a corrupt block has to surface as a read error, not abort a
data server.

## Running it

```sh
./run.sh
```

Needs Go and a Rust toolchain. Fixtures are regenerated into a temporary
directory each run.
