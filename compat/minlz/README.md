# MinLZ decode compatibility

Settles one question from DESIGN.md §3: **can roble read the MinLZ blocks that
Oxia's databases contain?**

It can, and it has to. Oxia's key-encoding conversion (`kv_pebble_formats.go`)
writes L1 and below with Pebble's `GoodCompression` profile, whose `OtherBlocks`
setting — index, filter and metadata blocks — is `fastestCompression`: Snappy on
arm64, **MinLZ everywhere else**, resolved at package init from `runtime.GOARCH`.
So a database converted on an x86 node holds MinLZ blocks for as long as those
files live, which for a deep file may be forever. Refusing MinLZ would refuse
the in-place upgrade roble exists to make possible (§2).

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
