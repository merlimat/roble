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
//! carrying a different log number rather than by any end marker. A chunk
//! whose log number is exactly one more than the expected one is Pebble's EOF
//! trailer and ends the stream cleanly.

use crate::checksum::ChecksumType;
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
pub enum WireFormat {
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
        let mut rest = payload;
        let mut first = true;
        loop {
            // A chunk never crosses a block boundary. No check is needed here
            // that the block has room for a header: the eager padding at the
            // end of this loop guarantees it, so a branch for it would be
            // unreachable — and an unreachable branch is one no test can
            // cover.
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
            let crc = ChecksumType::Crc32c.compute_raw(&self.out[at + 6..]);
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

    /// Reads the next record, or `None` at a clean end of stream.
    ///
    /// A clean end is a zero-filled block tail or Pebble's EOF trailer, which
    /// is what a crash leaves behind. Anything else is an error: a torn record
    /// in the middle of a file is corruption, not an ending (DESIGN.md §6).
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<Vec<u8>>> {
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

    fn next_inner(&mut self) -> Result<Option<Vec<u8>>> {
        let mut payload: Vec<u8> = Vec::new();
        let mut want_first = true;
        loop {
            let block_start = (self.pos / BLOCK_SIZE) * BLOCK_SIZE;
            let block_end = (block_start + BLOCK_SIZE).min(self.buf.len());
            if self.pos + LEGACY_HEADER_LEN > block_end {
                // Not even a legacy header fits: the rest of the block is pad.
                if self.pos >= self.buf.len() {
                    return if want_first {
                        Ok(None)
                    } else {
                        Error::corrupt("stream ends in the middle of a record")
                    };
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
            if self.pos + header_len + size > block_end {
                return Error::corrupt(format!(
                    "chunk of {size} bytes runs past its block, which a chunk never does"
                ));
            }

            if format == WireFormat::Recyclable {
                // The log number is tested before the checksum on purpose:
                // Pebble's EOF trailer carries a zero checksum, so validating
                // first would report corruption at the end of every closed log.
                let log_num = u32::from_le_bytes(h[7..11].try_into().expect("4 bytes"));
                if log_num != self.log_num {
                    // One more than expected is the EOF trailer; anything else
                    // is a chunk left over from the file's previous life.
                    if log_num == self.log_num.wrapping_add(1) && want_first {
                        return Ok(None);
                    }
                    if want_first {
                        return Ok(None);
                    }
                    return Error::corrupt(format!(
                        "chunk from log {log_num} inside log {}: a recycled tail \
                         interrupting a record",
                        self.log_num
                    ));
                }
            }

            let covered = &self.buf[self.pos + 6..self.pos + header_len + size];
            let computed = ChecksumType::Crc32c.compute_raw(covered);
            if computed != crc {
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
        let mut rd = RecordReader::new(&bytes, log_num);
        let mut got: Vec<Vec<u8>> = Vec::new();
        while let Some(r) = rd.next() {
            got.push(r.expect("record"));
        }
        assert_eq!(got, records, "{format:?}");
    }

    fn sizes() -> Vec<Vec<u8>> {
        // Around the block boundary in both directions, plus multi-block.
        let interesting = [
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
            let mut rd = RecordReader::new(&bytes[..cut], 1);
            let mut records = Vec::new();
            let mut errored = false;
            while let Some(r) = rd.next() {
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
    fn a_chunk_from_another_log_does_not_read_as_data() {
        let mut w = RecordWriter::recyclable(9);
        w.write_record(b"mine").expect("write");
        let bytes = w.finish();
        // Reading it as log 10 must not yield "mine".
        let mut rd = RecordReader::new(&bytes, 10);
        match rd.next() {
            None => {}
            Some(Ok(r)) => panic!("read {r:?} from a different log"),
            Some(Err(_)) => {}
        }
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
        let mut reader = RecordReader::new(&joined, 9);
        let mut back = Vec::new();
        while let Some(r) = reader.next() {
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
}
