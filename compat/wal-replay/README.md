# wal-replay

Half of the M3 gate (DESIGN.md §13): **Go Pebble opens and reads what roble
wrote.**

roble opens a database Pebble built, writes several batches to its log —
including one large enough to span a 32 KiB block, so a record has to be split
into chunks — and records what it wrote. Pebble then opens the same directory.

## What this catches

Nothing roble writes here is flushed, so every key Pebble finds can only have
come out of the log. Pebble finding them means it agreed with roble about:

- the **record framing**: recyclable chunks, their log number, their checksums,
  and where a chunk may sit relative to a 32 KiB block boundary;
- the **batch representation**: the header's sequence number and count, and
  each entry's kind, key and value;
- the **sequence numbers**: a delete roble wrote has to bury a value a table
  holds, which only works if Pebble reads roble's numbering the way roble
  meant it.

## What it cannot catch

That roble's log is byte-identical to the one Pebble would have written. It is
not meant to be: a log is a stream of records, and two writers can frame the
same records differently and both be right — different batch boundaries, for
one. What is pinned here is that Pebble **reads** what roble wrote, which is
the property that matters.

The other direction — roble reading a whole database Pebble wrote — is covered
by `tests/golden` and the probe corpora.
