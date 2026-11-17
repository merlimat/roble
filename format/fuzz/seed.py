#!/usr/bin/env python3
# Copyright 2026 Matteo Merli <matteo.merli@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Seeds the decode targets' corpora from the golden files Pebble wrote.

Run from anywhere; writes format/fuzz/corpus/<target>/. The round-trip and
cursor targets take structured input and need no seeds.
"""

import struct
from pathlib import Path

FUZZ = Path(__file__).resolve().parent
ROOT = FUZZ.parent.parent
GOLDEN = ROOT / "format" / "tests" / "golden"
DB = ROOT / "tests" / "golden" / "db-created"
CORPUS = FUZZ / "corpus"

FOOTER_LEN = 53
TRAILER_LEN = 5


def put(target, name, data):
    d = CORPUS / target
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_bytes(data)


def uvarint(b, i):
    v = shift = 0
    while True:
        c = b[i]
        i += 1
        v |= (c & 0x7F) << shift
        if c < 0x80:
            return v, i
        shift += 7


def handle(b, i=0):
    off, i = uvarint(b, i)
    n, i = uvarint(b, i)
    return (off, n), i


def entries(block):
    """(key, value) of every entry of an uncompressed row block."""
    n = struct.unpack_from("<I", block, len(block) - 4)[0]
    end = len(block) - 4 - 4 * n
    i, key = 0, b""
    while i < end:
        shared, i = uvarint(block, i)
        fresh, i = uvarint(block, i)
        vlen, i = uvarint(block, i)
        key = key[:shared] + block[i : i + fresh]
        i += fresh
        yield key, block[i : i + vlen]
        i += vlen


def blocks(sst):
    """Every block of a table, trailer included, as the engine fetches it."""
    footer = sst[-FOOTER_LEN:]
    meta, i = handle(footer, 1)
    index, _ = handle(footer, i)
    seen, todo = set(), [(meta, "meta"), (index, "index")]
    while todo:
        (off, n), kind = todo.pop()
        if (off, n) in seen or off + n + TRAILER_LEN > len(sst):
            continue
        seen.add((off, n))
        fetched = sst[off : off + n + TRAILER_LEN]
        yield fetched
        # Only an uncompressed block is walked for the handles inside it.
        if kind not in ("meta", "index") or fetched[-TRAILER_LEN] != 0:
            continue
        for key, value in entries(fetched[:-TRAILER_LEN]):
            if kind == "index":
                # A data block, or an index partition when the index has two
                # levels; either way a block worth a seed.
                todo.append((handle(value)[0], "data"))
            elif key.startswith((b"rocksdb.properties", b"pebble.value_index",
                                 b"rocksdb.range_del2", b"fullfilter.")):
                todo.append((handle(value)[0], "leaf"))


def main():
    tables = sorted(GOLDEN.glob("*.sst")) + sorted(DB.glob("*.sst"))
    for sst in tables:
        data = sst.read_bytes()
        put("decode_table", sst.name, data)
        for n, fetched in enumerate(blocks(data)):
            # No cursor operations, then a few: [count, ops..., block].
            put("decode_block", f"{sst.stem}-{n}", b"\x00" + fetched)
            put("decode_block", f"{sst.stem}-{n}-ops", b"\x06\x00\x02\x03\x01\x24\x2d" + fetched)

    # [log number, file]. The WAL is cut to two blocks: past one boundary
    # is what matters, and a short seed runs many more times a second.
    log = lambda n: struct.pack("<I", n)
    put("decode_records", "wal", log(2) + (GOLDEN / "records-wal.log").read_bytes()[: 64 << 10])
    put("decode_records", "manifest", log(1) + (GOLDEN / "records-manifest").read_bytes())
    put("decode_records", "db-manifest", log(1) + (DB / "MANIFEST-000001").read_bytes())
    put("decode_records", "db-log", log(2) + (DB / "000002.log").read_bytes())
    for line in (GOLDEN / "batches.txt").read_text().splitlines():
        if line.startswith("batch "):
            name = line.split()[1]
        elif line.startswith("repr "):
            put("decode_records", f"batch-{name}", bytes.fromhex(line.split()[1]))

    for target in sorted(CORPUS.iterdir()):
        print(f"{target.name}: {len(list(target.iterdir()))} seeds")


if __name__ == "__main__":
    main()
