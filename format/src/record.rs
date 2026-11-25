// Copyright 2026 Matteo Merli <matteo.merli@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Record framing (`record/record.go` and `record/log_writer.go`), which the
//! MANIFEST and the WAL are both built from.
//!
//! A stream is 32 KiB blocks of tightly packed chunks. **A chunk never crosses
//! a block boundary**, the last block may be short, and unused bytes at the
//! end of a block must be zero. One record becomes one full chunk, or a first
//! chunk, zero or more middle chunks and a last chunk.
//!
//! ```text
//!   legacy       CRC(4) | Size(2) | Type(1) |                  | Payload
//!   recyclable   CRC(4) | Size(2) | Type(1) | LogNum(4)         | Payload
//!   walSync      CRC(4) | Size(2) | Type(1) | LogNum(4) | Off(8)| Payload
//! ```
//!
//! Three things here are not guessable:
//!
//! 1. **The CRC covers the type, the log number and the payload — not the
//!    size.** Starting it at the wrong field gives a checksum that is wrong
//!    for every chunk, which is at least loud.
//! 2. **`Size` is the payload length**, not the chunk length.
//! 3. **The wire format is per chunk, read from the type byte**, not a
//!    property of the file: types 1 to 4 are legacy, 5 to 8 recyclable, 9 to
//!    12 walSync. The MANIFEST uses legacy and the WAL uses recyclable at
//!    roble's pin, but a reader must dispatch on the byte it finds.
//!
//! The log number is what makes a recycled file safe to read: a reused file
//! still holds chunks from its previous life, and they are recognised by
//! carrying a different log number, so that none is read as this log's. One
//! ends the stream as a torn record does, not cleanly. Only Pebble's EOF
//! trailer, a chunk whose log number is exactly one more than the expected
//! one, ends a stream cleanly, and only a log its writer closed has one.

use crate::error::{Error, Result};

/// `blockSize`: the framing's fixed block.
pub const BLOCK_SIZE: usize = 32 * 1024;

/// `legacyHeaderSize`.
pub const LEGACY_HEADER_LEN: usize = 7;

/// `recyclableHeaderSize`: the legacy header plus a four-byte log number.
pub const RECYCLABLE_HEADER_LEN: usize = 11;

/// `walSyncHeaderSize`: recyclable plus an eight-byte sync offset. Above
/// roble's pin, so it is refused rather than read (DESIGN.md §3).
pub const WAL_SYNC_HEADER_LEN: usize = 19;

const FULL: u8 = 1;
const FIRST: u8 = 2;
const MIDDLE: u8 = 3;
const LAST: u8 = 4;
const RECYCLABLE_FULL: u8 = 5;
const RECYCLABLE_FIRST: u8 = 6;
const RECYCLABLE_MIDDLE: u8 = 7;
const RECYCLABLE_LAST: u8 = 8;
const WAL_SYNC_FULL: u8 = 9;
const WAL_SYNC_LAST: u8 = 12;

/// Which chunk layout a type byte selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireFormat {
    /// Types 1 to 4: no log number. What the MANIFEST uses.
    Legacy,
    /// Types 5 to 8: a four-byte log number. What the WAL uses at the pin.
    Recyclable,
}

impl WireFormat {
    /// The header length for this layout.
    pub fn header_len(self) -> usize {
        match self {
            WireFormat::Legacy => LEGACY_HEADER_LEN,
            WireFormat::Recyclable => RECYCLABLE_HEADER_LEN,
        }
    }

    fn chunk_type(self, first: bool, last: bool) -> u8 {
        match (self, first, last) {
            (WireFormat::Legacy, true, true) => FULL,
            (WireFormat::Legacy, true, false) => FIRST,
            (WireFormat::Legacy, false, false) => MIDDLE,
            (WireFormat::Legacy, false, true) => LAST,
            (WireFormat::Recyclable, true, true) => RECYCLABLE_FULL,
            (WireFormat::Recyclable, true, false) => RECYCLABLE_FIRST,
            (WireFormat::Recyclable, false, false) => RECYCLABLE_MIDDLE,
            (WireFormat::Recyclable, false, true) => RECYCLABLE_LAST,
        }
    }
}

/// Where a chunk sits within its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    Full,
    First,
    Middle,
    Last,
}

/// Decodes a chunk type byte into its layout and position.
fn classify(t: u8) -> Result<(WireFormat, Position)> {
    Ok(match t {
        FULL => (WireFormat::Legacy, Position::Full),
        FIRST => (WireFormat::Legacy, Position::First),
        MIDDLE => (WireFormat::Legacy, Position::Middle),
        LAST => (WireFormat::Legacy, Position::Last),
        RECYCLABLE_FULL => (WireFormat::Recyclable, Position::Full),
        RECYCLABLE_FIRST => (WireFormat::Recyclable, Position::First),
        RECYCLABLE_MIDDLE => (WireFormat::Recyclable, Position::Middle),
        RECYCLABLE_LAST => (WireFormat::Recyclable, Position::Last),
        WAL_SYNC_FULL..=WAL_SYNC_LAST => {
            return Error::unsupported(
                "walSync chunks arrive with FormatWALSyncChunks, which is above roble's pin",
            );
        }
        other => return Error::corrupt(format!("record chunk type {other}")),
    })
}

/// Frames records into 32 KiB blocks.
#[derive(Debug)]
pub struct RecordWriter {
    out: Vec<u8>,
    format: WireFormat,
    log_num: u32,
    /// Where the first byte of `out` lands in the file.
    ///
    /// Blocks are aligned to the **file**, not to this buffer, so a writer
    /// that carries on part-way through a log has to know how far in it is.
    /// Zero for a writer that starts a file.
    base: usize,
}

impl RecordWriter {
    /// A writer for the MANIFEST's legacy framing.
    pub fn legacy() -> Self {
        RecordWriter {
            out: Vec::new(),
            format: WireFormat::Legacy,
            log_num: 0,
            base: 0,
        }
    }

    /// A writer for the WAL's recyclable framing, stamping `log_num`.
    pub fn recyclable(log_num: u32) -> Self {
        RecordWriter {
            out: Vec::new(),
            format: WireFormat::Recyclable,
            log_num,
            base: 0,
        }
    }

    /// A legacy writer that carries on at `at` bytes into the file.
    ///
    /// For appending to a MANIFEST, whose records are legacy-framed but
    /// blocked the same way: a chunk never crosses a 32 KiB boundary, so a
    /// writer that picks up part-way through has to know where it is.
    pub fn legacy_resuming(at: usize) -> Self {
        RecordWriter {
            out: Vec::new(),
            format: WireFormat::Legacy,
            log_num: 0,
            base: at,
        }
    }

    /// A recyclable writer that carries on at `at` bytes into the log.
    ///
    /// This is how a store reopens a log it is going to keep writing to: the
    /// records already there are left alone, and the next chunk is framed
    /// against the block boundaries of the **file**. Starting a fresh writer
    /// and appending its bytes would align every chunk to the wrong place,
    /// which no checksum would catch — the chunks would be individually valid
    /// and in the wrong positions.
    pub fn resuming(log_num: u32, at: usize) -> Self {
        RecordWriter {
            out: Vec::new(),
            format: WireFormat::Recyclable,
            log_num,
            base: at,
        }
    }

    /// How far into the file the writer has reached.
    pub fn position(&self) -> usize {
        self.base + self.out.len()
    }

    /// The bytes written so far.
    pub fn as_bytes(&self) -> &[u8] {
        &self.out
    }

    /// Consumes the writer, returning the framed stream.
    pub fn finish(self) -> Vec<u8> {
        self.out
    }

    /// Writes Pebble's EOF trailer, which is how a closed log ends.
    ///
    /// It is an eleven-byte recyclable header with a **zero** checksum, a zero
    /// payload length, and a log number one greater than the file's own. The
    /// zero checksum is why a reader has to test the log number before
    /// validating the checksum: validated first, the trailer looks like
    /// corruption rather than like the end of the log.
    pub fn write_eof_trailer(&mut self) -> Result<()> {
        if self.format != WireFormat::Recyclable {
            return Error::unsupported("only a recyclable log has an EOF trailer");
        }
        let used = self.position() % BLOCK_SIZE;
        if BLOCK_SIZE - used < RECYCLABLE_HEADER_LEN {
            self.out.resize(self.out.len() + (BLOCK_SIZE - used), 0);
        }
        let at = self.out.len();
        self.out.resize(at + RECYCLABLE_HEADER_LEN, 0);
        self.out[at + 6] = RECYCLABLE_FULL;
        self.out[at + 7..at + 11].copy_from_slice(&self.log_num.wrapping_add(1).to_le_bytes());
        Ok(())
    }

    /// Frames one record, splitting it across blocks as needed.
    pub fn write_record(&mut self, payload: &[u8]) -> Result<()> {
        let header_len = self.format.header_len();
        // A writer that carries on in a file can start where no writer
        // stops: at the end of a record too close to its block's end for a
        // header, as a reopen carries on in a log once it has cut it back to
        // its last record, padding and all. The record goes to the next
        // block, behind zeros, as the trailer does.
        let used = self.position() % BLOCK_SIZE;
        if BLOCK_SIZE - used < header_len {
            self.out.resize(self.out.len() + (BLOCK_SIZE - used), 0);
        }
        let mut rest = payload;
        let mut first = true;
        loop {
            // A chunk never crosses a block boundary. No check is needed here
            // that the block has room for a header: the padding above and the
            // eager padding at the end of this loop guarantee it, so a branch
            // for it would be unreachable — and an unreachable branch is one
            // no test can cover.
            let used = self.position() % BLOCK_SIZE;
            debug_assert!(
                BLOCK_SIZE - used >= header_len,
                "a block was left with {} bytes, too few for a {header_len}-byte header",
                BLOCK_SIZE - used
            );
            let room = BLOCK_SIZE - used - header_len;
            let last = rest.len() <= room;
            let n = rest.len().min(room);

            let at = self.out.len();
            self.out.resize(at + header_len, 0);
            self.out[at + 6] = self.format.chunk_type(first, last);
            if self.format == WireFormat::Recyclable {
                self.out[at + 7..at + 11].copy_from_slice(&self.log_num.to_le_bytes());
            }
            self.out.extend_from_slice(&rest[..n]);

            let size = u16::try_from(n)
                .map_err(|_| Error::Corrupt(format!("chunk payload of {n} bytes")))?;
            self.out[at + 4..at + 6].copy_from_slice(&size.to_le_bytes());
            // The CRC starts at the type byte, not at the size.
            let crc = crate::checksum::crc32c(&self.out[at + 6..]);
            self.out[at..at + 4].copy_from_slice(&crc.to_le_bytes());

            // Pebble zero-fills eagerly: once a block cannot hold another
            // header it is padded and closed.
            let used = self.position() % BLOCK_SIZE;
            if used != 0 && BLOCK_SIZE - used < header_len {
                self.out.resize(self.out.len() + (BLOCK_SIZE - used), 0);
            }

            rest = &rest[n..];
            first = false;
            if last {
                return Ok(());
            }
        }
    }
}

/// The records, up to a clean end of stream.
///
/// A clean end is where a writer stops: the end of the stream after a record
/// or a full block, a zero-filled block tail, or Pebble's EOF trailer.
/// Anything else is an error, as it is to Pebble's reader: a torn record, a
/// chunk of another log, or fewer bytes than a header ending a block the
/// stream does not fill. Whether that is where a crash stopped the writer or
/// corruption is the caller's to say (DESIGN.md §6). Fused: after an error
/// or the end it yields nothing more.
impl Iterator for RecordReader<'_> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.next_inner() {
            Ok(Some(rec)) => Some(Ok(rec)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

impl std::iter::FusedIterator for RecordReader<'_> {}

/// Reads a framed stream.
#[derive(Debug)]
pub struct RecordReader<'a> {
    buf: &'a [u8],
    pos: usize,
    log_num: u32,
    done: bool,
}

impl<'a> RecordReader<'a> {
    /// Reads `buf`, expecting `log_num` in any recyclable chunk.
    ///
    /// For a MANIFEST, whose chunks are legacy and carry no log number, the
    /// value is unused; pass the file number anyway so that a file which turns
    /// out to be recyclable is checked rather than waved through.
    pub fn new(buf: &'a [u8], log_num: u32) -> Self {
        RecordReader {
            buf,
            pos: 0,
            log_num,
            done: false,
        }
    }

    /// How many bytes have been consumed, which is where a torn tail begins.
    pub fn offset(&self) -> usize {
        self.pos
    }

    fn next_inner(&mut self) -> Result<Option<Vec<u8>>> {
        let mut payload: Vec<u8> = Vec::new();
        let mut want_first = true;
        loop {
            let block_start = (self.pos / BLOCK_SIZE) * BLOCK_SIZE;
            let block_end = (block_start + BLOCK_SIZE).min(self.buf.len());
            if self.pos + LEGACY_HEADER_LEN > block_end {
                if self.pos >= self.buf.len() {
                    return if want_first {
                        Ok(None)
                    } else {
                        Error::corrupt("stream ends in the middle of a record")
                    };
                }
                // Not even a legacy header fits. At the end of a full block
                // that is its padding, whatever it holds, and the stream goes
                // on in the next one. In a block the stream does not fill,
                // its last, no writer stops there: Pebble's reader takes the
                // bytes for a chunk that does not read (`r.end != r.n`).
                if block_end - block_start < BLOCK_SIZE {
                    return Error::corrupt(format!(
                        "{} bytes at the end of the stream, too few for a chunk header",
                        block_end - self.pos
                    ));
                }
                self.pos = block_end;
                continue;
            }

            let h = &self.buf[self.pos..];
            let crc = u32::from_le_bytes(h[0..4].try_into().expect("4 bytes"));
            let size = u16::from_le_bytes(h[4..6].try_into().expect("2 bytes")) as usize;
            let type_byte = h[6];

            if crc == 0 && size == 0 && type_byte == 0 {
                // A zeroed header is a block tail. Pebble tolerates it only
                // where a header could not have fitted; past that it is a sign
                // of corruption rather than of padding.
                let remaining = block_end - self.pos;
                if remaining < RECYCLABLE_HEADER_LEN {
                    self.pos = block_end;
                } else if remaining < WAL_SYNC_HEADER_LEN {
                    if self.buf[self.pos..block_end].iter().any(|&b| b != 0) {
                        return Error::corrupt("zeroed chunk header followed by data");
                    }
                    self.pos = block_end;
                } else {
                    return Error::corrupt(format!(
                        "zeroed chunk header with {remaining} bytes left in the block"
                    ));
                }
                if self.pos >= self.buf.len() {
                    return if want_first {
                        Ok(None)
                    } else {
                        Error::corrupt("stream ends in the middle of a record")
                    };
                }
                continue;
            }

            let (format, position) = classify(type_byte)?;
            let header_len = format.header_len();
            if self.pos + header_len > block_end {
                return Error::corrupt(format!(
                    "chunk header of {header_len} bytes runs past its block"
                ));
            }

            if format == WireFormat::Recyclable {
                // The log number is tested before the checksum on purpose:
                // Pebble's EOF trailer carries a zero checksum, so validating
                // first would report corruption at the end of every closed log.
                // And before the length, as Pebble's `nextChunk` tests it: the
                // trailer is known by its number alone, so a chunk stamped as
                // the trailer ends the log whatever its length says.
                let log_num = u32::from_le_bytes(h[7..11].try_into().expect("4 bytes"));
                if log_num != self.log_num {
                    // One more than expected, where a record starts, is the
                    // EOF trailer, the `+ 1` wrapping as the writer's does.
                    // Anything else is not this log's: a chunk of the file's
                    // earlier life, or a damaged one. The stream ends there,
                    // but not cleanly, as Pebble's reader ends it: "treat
                    // this chunk as invalid in order to prevent reading of a
                    // partial record".
                    if log_num == self.log_num.wrapping_add(1) && want_first {
                        return Ok(None);
                    }
                    return Error::corrupt(format!(
                        "chunk from log {log_num} inside log {}",
                        self.log_num
                    ));
                }
            }

            if self.pos + header_len + size > block_end {
                return Error::corrupt(format!(
                    "chunk of {size} bytes runs past its block, which a chunk never does"
                ));
            }

            let covered = &self.buf[self.pos + 6..self.pos + header_len + size];
            let computed = crate::checksum::crc32c(covered);
            // A fuzzer that has to forge a CRC never gets past it (format/fuzz).
            if computed != crc && !cfg!(fuzzing) {
                return Err(Error::ChecksumMismatch {
                    offset: self.pos as u64,
                    stored: crc,
                    computed,
                });
            }

            match (want_first, position) {
                (true, Position::Full | Position::First) => {}
                (false, Position::Middle | Position::Last) => {}
                (true, _) => {
                    return Error::corrupt("record starts with a middle or last chunk");
                }
                (false, _) => {
                    return Error::corrupt("record continues with a full or first chunk");
                }
            }

            payload
                .extend_from_slice(&self.buf[self.pos + header_len..self.pos + header_len + size]);
            self.pos += header_len + size;
            want_first = false;

            if matches!(position, Position::Full | Position::Last) {
                return Ok(Some(payload));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(format: WireFormat, log_num: u32, records: &[Vec<u8>]) {
        let mut w = match format {
            WireFormat::Legacy => RecordWriter::legacy(),
            WireFormat::Recyclable => RecordWriter::recyclable(log_num),
        };
        for r in records {
            w.write_record(r).expect("write");
        }
        let bytes = w.finish();
        let rd = RecordReader::new(&bytes, log_num);
        let mut got: Vec<Vec<u8>> = Vec::new();
        for r in rd {
            got.push(r.expect("record"));
        }
        assert_eq!(got, records, "{format:?}");
    }

    fn sizes() -> Vec<Vec<u8>> {
        // Around the block boundary in both directions, plus multi-block.
        // The first two each leave a recyclable writer a block tail to pad:
        // five bytes, too few for even a legacy header, and then nine, enough
        // for one but too few for a recyclable one.
        let interesting = [
            BLOCK_SIZE - RECYCLABLE_HEADER_LEN - 5,
            BLOCK_SIZE - RECYCLABLE_HEADER_LEN - 9,
            0usize,
            1,
            100,
            BLOCK_SIZE - RECYCLABLE_HEADER_LEN - 1,
            BLOCK_SIZE - RECYCLABLE_HEADER_LEN,
            BLOCK_SIZE - RECYCLABLE_HEADER_LEN + 1,
            BLOCK_SIZE,
            BLOCK_SIZE + 1,
            3 * BLOCK_SIZE + 17,
        ];
        interesting
            .iter()
            .enumerate()
            .map(|(i, &n)| vec![b'a' + (i as u8 % 26); n])
            .collect()
    }

    #[test]
    fn records_round_trip_in_both_framings() {
        round_trip(WireFormat::Legacy, 0, &sizes());
        round_trip(WireFormat::Recyclable, 7, &sizes());
    }

    #[test]
    fn no_chunk_crosses_a_block_boundary() {
        let mut w = RecordWriter::recyclable(3);
        for r in sizes() {
            w.write_record(&r).expect("write");
        }
        let bytes = w.finish();
        // Walk the chunks and check each one lies inside a single block.
        let mut pos = 0usize;
        while pos < bytes.len() {
            let block_end = ((pos / BLOCK_SIZE) + 1) * BLOCK_SIZE;
            if pos + LEGACY_HEADER_LEN > block_end.min(bytes.len()) {
                pos = block_end;
                continue;
            }
            let type_byte = bytes[pos + 6];
            if type_byte == 0 {
                pos = block_end;
                continue;
            }
            let size = u16::from_le_bytes(bytes[pos + 4..pos + 6].try_into().unwrap()) as usize;
            let (format, _) = classify(type_byte).expect("type");
            let end = pos + format.header_len() + size;
            assert!(
                end <= block_end,
                "a chunk at {pos} ends at {end}, past its block boundary {block_end}"
            );
            pos = end;
        }
    }

    #[test]
    fn a_payload_that_exactly_fills_a_block_is_one_full_chunk() {
        // The boundary between "fits" and "does not": with the test written as
        // `<` rather than `<=`, this payload would be split into a first and a
        // last chunk, which is a different stream of bytes for the same record.
        for (format, header_len) in [
            (WireFormat::Legacy, LEGACY_HEADER_LEN),
            (WireFormat::Recyclable, RECYCLABLE_HEADER_LEN),
        ] {
            let exact = vec![b'z'; BLOCK_SIZE - header_len];
            let mut w = match format {
                WireFormat::Legacy => RecordWriter::legacy(),
                WireFormat::Recyclable => RecordWriter::recyclable(4),
            };
            w.write_record(&exact).expect("write");
            let bytes = w.finish();
            assert_eq!(
                bytes.len(),
                BLOCK_SIZE,
                "{format:?}: an exact fit should leave no padding"
            );
            let (_, position) = classify(bytes[6]).expect("type");
            assert_eq!(
                position,
                Position::Full,
                "{format:?}: an exact fit must be a single full chunk, not a split one"
            );

            // One byte more must split, and still round-trip.
            let over = vec![b'z'; BLOCK_SIZE - header_len + 1];
            let mut w = match format {
                WireFormat::Legacy => RecordWriter::legacy(),
                WireFormat::Recyclable => RecordWriter::recyclable(4),
            };
            w.write_record(&over).expect("write");
            let bytes = w.finish();
            let (_, position) = classify(bytes[6]).expect("type");
            assert_eq!(
                position,
                Position::First,
                "{format:?}: one byte over must split"
            );
            let mut rd = RecordReader::new(&bytes, 4);
            assert_eq!(rd.next().expect("a record").expect("ok"), over);
        }
    }

    #[test]
    fn a_block_tail_too_small_for_a_header_is_zero_filled() {
        // Pebble's reader treats non-zero bytes in a block's tail as
        // corruption rather than as padding, so leaving them unzeroed produces
        // a file Go rejects. The condition needs constructing: it only arises
        // when a chunk happens to end within a header's length of a block
        // edge, which neither the round-trip fixtures nor a real WAL did.
        for (format, header_len) in [
            (WireFormat::Legacy, LEGACY_HEADER_LEN),
            (WireFormat::Recyclable, RECYCLABLE_HEADER_LEN),
        ] {
            for leftover in 1..header_len {
                let mut w = match format {
                    WireFormat::Legacy => RecordWriter::legacy(),
                    WireFormat::Recyclable => RecordWriter::recyclable(5),
                };
                // End the first chunk exactly `leftover` bytes before the
                // block boundary.
                let first = vec![b'p'; BLOCK_SIZE - header_len - leftover];
                w.write_record(&first).expect("write");
                w.write_record(b"second").expect("write");
                let bytes = w.finish();

                let tail = &bytes[BLOCK_SIZE - leftover..BLOCK_SIZE];
                assert!(
                    tail.iter().all(|&b| b == 0),
                    "{format:?}, {leftover} bytes left: tail is {tail:02x?}, not padding"
                );
                // And the second record must start in the next block.
                let (_, position) = classify(bytes[BLOCK_SIZE + 6]).expect("type");
                assert_eq!(position, Position::Full, "{format:?}, {leftover}");

                let mut rd = RecordReader::new(&bytes, 5);
                assert_eq!(rd.next().expect("first").expect("ok"), first);
                assert_eq!(rd.next().expect("second").expect("ok"), b"second");
                assert!(rd.next().is_none());
            }
        }
    }

    #[test]
    fn a_torn_tail_ends_the_stream_but_a_torn_middle_does_not() {
        let mut w = RecordWriter::recyclable(1);
        w.write_record(b"first").expect("write");
        w.write_record(&[b'x'; 100]).expect("write");
        let bytes = w.finish();

        // Truncating inside the second record must not silently yield one
        // record and a clean end: the first record is fine, the remainder is
        // either absent or an error, never wrong data.
        for cut in 1..bytes.len() {
            let rd = RecordReader::new(&bytes[..cut], 1);
            let mut records = Vec::new();
            let mut errored = false;
            for r in rd {
                match r {
                    Ok(rec) => records.push(rec),
                    Err(_) => errored = true,
                }
            }
            for rec in &records {
                assert!(
                    rec.as_slice() == b"first" || rec.as_slice() == [b'x'; 100].as_slice(),
                    "truncating at {cut} invented a record of {} bytes",
                    rec.len()
                );
            }
            let _ = errored;
        }
    }

    #[test]
    fn a_chunk_stamped_as_the_trailer_ends_the_log_whatever_its_length() {
        // Log 5's one record, never closed, and then a header stamped with
        // 6, the trailer's number, claiming more bytes than the stream has.
        // Pebble's reader tests the number first and ends the log cleanly
        // there, and so does this one.
        let mut w = RecordWriter::recyclable(5);
        w.write_record(b"r1").expect("write");
        let mut bytes = w.finish();
        let mut stamped = vec![0u8; RECYCLABLE_HEADER_LEN];
        stamped[4..6].copy_from_slice(&1000u16.to_le_bytes());
        stamped[6] = RECYCLABLE_FULL;
        stamped[7..11].copy_from_slice(&6u32.to_le_bytes());
        bytes.extend_from_slice(&stamped);
        let records: Vec<Vec<u8>> = RecordReader::new(&bytes, 5)
            .collect::<Result<_>>()
            .expect("the log ends at the trailer");
        assert_eq!(records, [b"r1".to_vec()]);

        // Cut short of its eleven bytes the header is not read at all: the
        // log number is not there to be tested, and the stream ends
        // uncleanly, as Pebble's does at a header its block cannot hold.
        let cut = bytes.len() - 2;
        let mut rd = RecordReader::new(&bytes[..cut], 5);
        assert_eq!(rd.next().expect("r1").expect("r1"), b"r1");
        let err = rd
            .next()
            .expect("the cut header")
            .expect_err("a header cut short");
        assert!(
            format!("{err}").contains("chunk header of 11 bytes runs past its block"),
            "got {err:?}"
        );

        // Inside a record it is no trailer, whatever its length: every chunk
        // of a record is its own log's.
        let mut inside = chunk(RECYCLABLE_FIRST, Some(5), b"ab");
        inside.extend_from_slice(&stamped);
        let err = first_error(&inside, 5);
        assert!(
            format!("{err}").contains("chunk from log 6 inside log 5"),
            "got {err:?}"
        );
    }

    #[test]
    fn a_chunk_from_another_log_does_not_read_as_data() {
        let mut w = RecordWriter::recyclable(9);
        w.write_record(b"mine").expect("write");
        let bytes = w.finish();
        // Reading it as log 10 must not yield "mine", nor end the log cleanly:
        // 9 is not 10's trailer either.
        let err = first_error(&bytes, 10);
        assert!(
            format!("{err}").contains("chunk from log 9 inside log 10"),
            "got {err:?}"
        );
    }

    #[test]
    fn walsync_chunks_are_refused_by_name() {
        for t in WAL_SYNC_FULL..=WAL_SYNC_LAST {
            let err = classify(t).expect_err("walSync must be refused");
            assert!(
                format!("{err}").contains("walSync"),
                "the refusal should name the format: {err}"
            );
        }
    }
    /// Frames one chunk by hand, with a checksum that matches it.
    ///
    /// The reader validates the checksum before it looks at how chunks follow
    /// one another, so a test about chunk order has to get the checksum right
    /// or it tests the checksum instead.
    fn chunk(type_byte: u8, log_num: Option<u32>, payload: &[u8]) -> Vec<u8> {
        let header_len = match log_num {
            Some(_) => RECYCLABLE_HEADER_LEN,
            None => LEGACY_HEADER_LEN,
        };
        let mut out = vec![0u8; header_len];
        out[4..6].copy_from_slice(&(payload.len() as u16).to_le_bytes());
        out[6] = type_byte;
        if let Some(n) = log_num {
            out[7..11].copy_from_slice(&n.to_le_bytes());
        }
        out.extend_from_slice(payload);
        // Covers the type byte onwards, as the reader computes it.
        let crc = crate::checksum::crc32c(&out[6..]);
        out[0..4].copy_from_slice(&crc.to_le_bytes());
        out
    }

    fn first_error(bytes: &[u8], log_num: u32) -> Error {
        RecordReader::new(bytes, log_num)
            .next()
            .expect("a chunk to read")
            .expect_err("bytes that do not read as a record")
    }

    #[test]
    fn a_chunk_type_pebble_does_not_define_is_refused() {
        // 13 is past walSync, which has a refusal of its own. The type is read
        // before the checksum, so this does not need a good one.
        let err = first_error(&chunk(13, None, b"x"), 0);
        assert!(
            format!("{err}").contains("record chunk type 13"),
            "got {err:?}"
        );
    }

    #[test]
    fn chunks_that_do_not_follow_one_another_are_refused() {
        // A record cannot begin in the middle of itself.
        let err = first_error(&chunk(MIDDLE, None, b"x"), 0);
        assert!(
            format!("{err}").contains("starts with a middle or last chunk"),
            "got {err:?}"
        );

        // Nor continue with something that begins one.
        let mut bytes = chunk(FIRST, None, b"a");
        bytes.extend_from_slice(&chunk(FULL, None, b"b"));
        let err = first_error(&bytes, 0);
        assert!(
            format!("{err}").contains("continues with a full or first chunk"),
            "got {err:?}"
        );
    }

    #[test]
    fn a_record_that_the_stream_ends_in_the_middle_of_is_refused() {
        // A FIRST chunk and nothing after it. The difference from a torn tail
        // is that this is not the tail of a log being replayed — a reader
        // asked for a record and the bytes stop inside one.
        let err = first_error(&chunk(FIRST, None, b"abc"), 0);
        assert!(
            format!("{err}").contains("ends in the middle of a record"),
            "got {err:?}"
        );
    }

    #[test]
    fn a_zeroed_header_is_padding_only_where_a_header_could_not_have_fitted() {
        // Pebble zero-fills the tail of a block when the next record will not
        // start in it. Past the point where a header could have fitted, a
        // zeroed header is a sign of corruption rather than of padding, and
        // the two cases are told apart by how much of the block is left.
        let mut bytes = chunk(FIRST, None, b"a");
        // A zeroed header with the whole rest of the block behind it.
        bytes.resize(BLOCK_SIZE, 0);
        let err = first_error(&bytes, 0);
        assert!(
            format!("{err}").contains("zeroed chunk header with"),
            "got {err:?}"
        );

        // Padding proper: too little left for even a recyclable header, so the
        // reader steps to the next block without complaint — and then finds
        // the stream has ended inside a record, which is the other half of
        // what makes this padding rather than a tail.
        // Nine bytes: enough that the zeroed header is read as a header, too
        // few to be a recyclable one. Fewer than seven would not be read at
        // all and would take the branch above this one instead.
        let fill = BLOCK_SIZE - LEGACY_HEADER_LEN - 9;
        let mut bytes = chunk(FIRST, None, &vec![b'a'; fill]);
        bytes.resize(BLOCK_SIZE, 0);
        let err = first_error(&bytes, 0);
        assert!(
            format!("{err}").contains("ends in the middle of a record"),
            "got {err:?}"
        );

        // And a zeroed header with too little behind it to be a walSync
        // header but enough to be a recyclable one, which Pebble tolerates as
        // padding only while the padding is actually zero.
        let fill = BLOCK_SIZE - LEGACY_HEADER_LEN - 15;
        let mut bytes = chunk(FIRST, None, &vec![b'a'; fill]);
        assert_eq!(
            bytes.len(),
            BLOCK_SIZE - 15,
            "the chunk ends 15 bytes short"
        );
        bytes.resize(BLOCK_SIZE, 0);
        *bytes.last_mut().expect("a byte") = 0xff;
        let err = first_error(&bytes, 0);
        assert!(
            format!("{err}").contains("zeroed chunk header followed by data"),
            "got {err:?}"
        );

        // The same fifteen bytes all zero, after a whole record, are padding,
        // and where the stream ends with them it ends cleanly.
        let payload = vec![b'a'; fill];
        let mut bytes = chunk(FULL, None, &payload);
        bytes.resize(BLOCK_SIZE, 0);
        let got: Vec<Vec<u8>> = RecordReader::new(&bytes, 0)
            .collect::<Result<_>>()
            .expect("a clean end");
        assert_eq!(got, [payload]);
    }

    #[test]
    fn a_chunk_from_another_log_interrupting_a_record_is_refused() {
        // A recycled log's tail. Reaching it mid-record is the case that
        // cannot be waved through: at the start of a record it is the end of
        // this log's contents, but inside one it means the record was lost.
        let mut bytes = chunk(RECYCLABLE_FIRST, Some(7), b"a");
        bytes.extend_from_slice(&chunk(RECYCLABLE_MIDDLE, Some(9), b"b"));
        let err = first_error(&bytes, 7);
        assert!(
            format!("{err}").contains("chunk from log 9 inside log 7"),
            "got {err:?}"
        );
    }

    #[test]
    fn only_a_recyclable_log_has_an_eof_trailer() {
        // The trailer is a recyclable header with a zero checksum, so there is
        // nothing to write it into in a legacy log.
        let mut w = RecordWriter::legacy();
        w.write_record(b"x").expect("write");
        let err = w
            .write_eof_trailer()
            .expect_err("a legacy log has no trailer");
        assert!(
            format!("{err}").contains("only a recyclable log"),
            "got {err:?}"
        );
    }
}

#[cfg(test)]
mod resume_tests {
    use super::*;

    /// Records chosen to straddle a block boundary, which is the only place
    /// the base offset can matter.
    fn payloads() -> Vec<Vec<u8>> {
        vec![
            vec![b'a'; 100],
            vec![b'b'; BLOCK_SIZE - 200],
            vec![b'c'; BLOCK_SIZE + 500],
            vec![b'd'; 7],
        ]
    }

    #[test]
    fn resuming_frames_the_same_bytes_as_writing_straight_through() {
        let payloads = payloads();
        let mut whole = RecordWriter::recyclable(9);
        for p in &payloads {
            whole.write_record(p).expect("write");
        }
        let whole = whole.finish();

        // The same records, but the writer is put down and picked up again
        // after each one.
        let mut joined: Vec<u8> = Vec::new();
        for p in &payloads {
            let mut w = RecordWriter::resuming(9, joined.len());
            w.write_record(p).expect("write");
            joined.extend_from_slice(w.as_bytes());
        }

        assert_eq!(
            joined, whole,
            "a resumed writer must frame against the file, not its own buffer"
        );

        // And the result still reads back as the records that went in.
        let reader = RecordReader::new(&joined, 9);
        let mut back = Vec::new();
        for r in reader {
            back.push(r.expect("record"));
        }
        assert_eq!(back, payloads);
    }

    #[test]
    fn a_legacy_writer_resumes_the_same_way() {
        // The MANIFEST is framed in the same 32 KiB blocks, so appending to
        // one has the same requirement as appending to a log.
        let payloads = payloads();
        let mut whole = RecordWriter::legacy();
        for p in &payloads {
            whole.write_record(p).expect("write");
        }
        let whole = whole.finish();

        let mut joined: Vec<u8> = Vec::new();
        for p in &payloads {
            let mut w = RecordWriter::legacy_resuming(joined.len());
            w.write_record(p).expect("write");
            joined.extend_from_slice(w.as_bytes());
        }
        assert_eq!(joined, whole);

        let reader = RecordReader::new(&joined, 0);
        let mut back = Vec::new();
        for r in reader {
            back.push(r.expect("record"));
        }
        assert_eq!(back, payloads);
    }

    #[test]
    fn an_eof_trailer_lands_in_the_same_place_either_way() {
        let payload = vec![b'x'; BLOCK_SIZE - 20];
        let mut whole = RecordWriter::recyclable(3);
        whole.write_record(&payload).expect("write");
        whole.write_eof_trailer().expect("trailer");
        let whole = whole.finish();

        let mut first = RecordWriter::recyclable(3);
        first.write_record(&payload).expect("write");
        let mut joined = first.finish();
        let mut second = RecordWriter::resuming(3, joined.len());
        second.write_eof_trailer().expect("trailer");
        joined.extend_from_slice(second.as_bytes());

        assert_eq!(joined, whole);
    }

    /// A resumed writer can be handed an offset no writer stops at, one that
    /// leaves its block too little room for a header. The trailer then goes
    /// where a reader looks for the next header, at the start of the next
    /// block, behind zeros.
    #[test]
    fn an_eof_trailer_with_no_room_in_its_block_starts_the_next() {
        let short = RECYCLABLE_HEADER_LEN - 1;
        let mut w = RecordWriter::resuming(3, BLOCK_SIZE - short);
        w.write_eof_trailer().expect("trailer");
        assert_eq!(w.position(), BLOCK_SIZE + RECYCLABLE_HEADER_LEN);
        let bytes = w.as_bytes();
        assert!(bytes[..short].iter().all(|&b| b == 0), "padded");
        assert_eq!(bytes[short + 6], RECYCLABLE_FULL, "then the trailer");
    }

    /// The same for a record, which a reopen writes there when a log's last
    /// record ends that close to its block's end: it carries on where the
    /// record ends, its padding cut away with whatever follows it.
    #[test]
    fn a_record_resumed_with_no_room_in_its_block_starts_the_next() {
        for (format, header_len) in [
            (WireFormat::Legacy, LEGACY_HEADER_LEN),
            (WireFormat::Recyclable, RECYCLABLE_HEADER_LEN),
        ] {
            for short in 1..header_len {
                let at = BLOCK_SIZE - short;
                let earlier = vec![b'p'; at - header_len];
                let (mut first, mut next) = match format {
                    WireFormat::Legacy => {
                        (RecordWriter::legacy(), RecordWriter::legacy_resuming(at))
                    }
                    WireFormat::Recyclable => {
                        (RecordWriter::recyclable(3), RecordWriter::resuming(3, at))
                    }
                };
                first.write_record(&earlier).expect("write");
                next.write_record(b"next").expect("write");
                assert_eq!(
                    next.position(),
                    BLOCK_SIZE + header_len + 4,
                    "{format:?}, {short} bytes left: the record starts the next block"
                );
                let resumed = next.as_bytes();
                assert!(
                    resumed[..short].iter().all(|&b| b == 0),
                    "{format:?}, {short} bytes left: padded"
                );

                let mut file = first.finish();
                file.truncate(at);
                file.extend_from_slice(resumed);
                let back: Vec<Vec<u8>> = RecordReader::new(&file, 3)
                    .collect::<Result<_>>()
                    .expect("reads back");
                assert_eq!(back, [earlier, b"next".to_vec()], "{format:?}, {short}");
            }
        }
    }
}
