# roble-format

Encodes and decodes [Pebble](https://github.com/cockroachdb/pebble)'s on-disk
formats: sstables, the write-ahead log and MANIFEST records, batches, version
edits and internal keys. Bytes in and bytes out, with no I/O, no async and no
engine.

It was written for [roble](https://github.com/merlimat/roble), the embedded
key-value store whose files are Pebble's, and is pinned to Pebble v2.1.7:
anything Pebble writes above that pin is refused rather than half-read. The
[crate docs](https://docs.rs/roble-format) say what each module reads and
writes.

## License

Apache-2.0, as [LICENSE](LICENSE) states.
