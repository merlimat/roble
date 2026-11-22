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

//! Where a log ends: the cases Pebble's `record/record_test.go` builds at
//! v2.1.7, and the ones beside them that it does not.
//!
//! Pebble's reader ends a log in one of two ways, and its replay acts on the
//! difference (`open.go`, `replayWAL`):
//!
//! - **Clean**, `io.EOF`: the log ends here, whichever log it is.
//! - **Unclean**, `ErrUnexpectedEOF`: the torn tail of the newest log, where
//!   the replay stops, or corruption in any log before it, which the replay
//!   refuses (`strictWALTail`). Every chunk that does not read ends up here
//!   at roble's pin: Pebble's read-ahead turns it into corruption only on the
//!   word of a walSync chunk, which a log at the pin does not have.
//!
//! Each test builds the bytes by hand and expects what Pebble's
//! `record.Reader` answers for the same bytes; where `record_test.go` has the
//! case, the test says which. A record that does not read is roble's error,
//! so an error here is Pebble's unclean end.
//!
//! `TestStaleReader` has nothing to port. It checks that Go's per-record
//! `io.Reader` stops working once `Next` moves on, and roble's reader hands
//! each record out whole.

use roble_format::record::{
    BLOCK_SIZE, LEGACY_HEADER_LEN, RECYCLABLE_HEADER_LEN, RecordReader, RecordWriter,
    WAL_SYNC_HEADER_LEN,
};

/// How a read ends.
#[derive(Debug, PartialEq, Eq)]
enum End {
    Clean,
    Unclean,
}

/// The records a read of `bytes` as log `log_num` yields, and how it ends.
///
/// The number is cut to its low 32 bits as the engine cuts it, and as
/// Pebble's `NewReader` does (`uint32(logNum)`).
fn read(bytes: &[u8], log_num: u64) -> (Vec<String>, End) {
    let mut records = Vec::new();
    for record in RecordReader::new(bytes, log_num as u32) {
        match record {
            Ok(record) => records.push(describe(&record)),
            Err(_) => return (records, End::Unclean),
        }
    }
    (records, End::Clean)
}

/// A record as the assertions compare it: a long one, which here is always
/// one byte repeated, by its length and that byte, so that a failure prints
/// a line rather than 32 KiB.
fn describe(record: &[u8]) -> String {
    match record {
        [first, ..] if record.len() > 16 && record.iter().all(|b| b == first) => {
            format!("{} x {:?}", record.len(), char::from(*first))
        }
        _ => String::from_utf8_lossy(record).into_owned(),
    }
}

/// A log of `records` as a writer that never closed it leaves it.
fn unclosed(log_num: u64, records: &[&[u8]]) -> Vec<u8> {
    let mut w = RecordWriter::recyclable(log_num as u32);
    for r in records {
        w.write_record(r).expect("frame a record");
    }
    w.finish()
}

/// The same log closed: Pebble's EOF trailer after its records.
fn closed(log_num: u64, records: &[&[u8]]) -> Vec<u8> {
    let mut w = RecordWriter::recyclable(log_num as u32);
    for r in records {
        w.write_record(r).expect("frame a record");
    }
    w.write_eof_trailer().expect("the trailer");
    w.finish()
}

fn recs(records: &[&[u8]]) -> Vec<String> {
    records.iter().map(|r| describe(r)).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

// ------------------------------------------------- log numbers past u32::MAX

/// `TestRecycleLogNumberOverflow`. A file closed as log 2^32 - 1 is recycled
/// as log 2^32, whose low 32 bits are zero, and so is the first log's
/// trailer: 2^32 - 1 + 1 wraps. The second log is the shorter, so the last
/// four bytes of the first one's trailer stay past its own.
#[test]
fn a_log_numbered_past_u32_max_reads_back_over_the_one_before_it() {
    let mut file = closed(u64::from(u32::MAX), &[b"aaaaa"]);
    let second = closed(1 << 32, &[b"a"]);
    file[..second.len()].copy_from_slice(&second);
    // Byte for byte what Pebble's `LogWriter` leaves in the test's buffer.
    assert_eq!(
        file,
        unhex("5c1d8e030100050000000061000000000000050100000000000000")
    );
    assert_eq!(read(&file, 1 << 32), (recs(&[b"a"]), End::Clean));
}

/// The trailer of a log whose low 32 bits are all ones carries zero, the
/// `+ 1` having wrapped, and it still ends the log, ahead of the stale
/// bytes past it. The Pebble test above never reads such a trailer: the log
/// it reads is the one whose trailer carries one.
#[test]
fn the_trailer_ends_a_log_whose_number_wraps_when_one_is_added() {
    let n = 0x1_FFFF_FFFF;
    let mut file = closed(n, &[b"x", b"y"]);
    assert_eq!(
        file[file.len() - 4..],
        [0; 4],
        "the trailer's number wrapped"
    );
    file.extend_from_slice(b"zzzz");
    assert_eq!(read(&file, n), (recs(&[b"x", b"y"]), End::Clean));
}

/// `TestInvalidLogNum`. Read under its own number, a closed log ends at its
/// trailer. Read under the next one, its first chunk is from another log and
/// is not that log's trailer either, and Pebble's reader ends there,
/// uncleanly: "treat this chunk as invalid in order to prevent reading of a
/// partial record" (`nextChunk`).
#[test]
fn a_log_read_under_another_number_ends_uncleanly() {
    let records: Vec<Vec<u8>> = (0..10).map(|i| format!("{i:04}\n").into_bytes()).collect();
    let refs: Vec<&[u8]> = records.iter().map(Vec::as_slice).collect();
    let file = closed(1, &refs);
    assert_eq!(read(&file, 1), (recs(&refs), End::Clean));
    assert_eq!(read(&file, 2), (vec![], End::Unclean));
}

/// A chunk from another log where this log's next record would start: what
/// a recycled file holds past the records of a log whose writer never closed
/// it, or what a damaged log number looks like. It is not read as this
/// log's, and it ends the log uncleanly, past `u32::MAX` as below it. Only a
/// chunk carrying the trailer's number, one more than this log's, ends the
/// log cleanly, and it does with the `+ 1` wrapped too.
#[test]
fn a_chunk_from_another_log_is_not_read_and_ends_the_log_uncleanly() {
    for (log, other) in [
        (5, 4),
        (5, 7),
        (0x1_0000_0005, 0x1_0000_0004),
        (0x1_0000_0005, 4),
    ] {
        let file = [unclosed(log, &[b"r1", b"r2"]), unclosed(other, &[b"stale"])].concat();
        assert_eq!(
            read(&file, log),
            (recs(&[b"r1", b"r2"]), End::Unclean),
            "log {log:#x} before a chunk of log {other:#x}"
        );
    }

    // Between two of the log's own records, it hides the second.
    let file = [
        unclosed(5, &[b"r1"]),
        unclosed(4, &[b"stale"]),
        unclosed(5, &[b"r2"]),
    ]
    .concat();
    assert_eq!(read(&file, 5), (recs(&[b"r1"]), End::Unclean));

    let n = 0x1_FFFF_FFFF;
    let file = [unclosed(n, &[b"x"]), unclosed(n + 1, &[b"stale"])].concat();
    assert_eq!(read(&file, n), (recs(&[b"x"]), End::Clean));
}

// ------------------------------------------- a tail shorter than a header

/// `TestRecycleLogWithPartialBlock`. Log 2 closed over a longer log 1 leaves
/// the last four bytes of log 1's trailer past its own, and its trailer ends
/// the read before them. Killed before it was closed, log 2 leaves log 1's
/// bytes straight after its record, and those do not read as a chunk.
#[test]
fn a_recycled_log_ends_at_its_trailer_before_the_older_ones_bytes() {
    let mut file = closed(1, &[b"aaaaa"]);
    let second = closed(2, &[b"a"]);
    file[..second.len()].copy_from_slice(&second);
    // Byte for byte what Pebble's `LogWriter` leaves in the test's buffer.
    assert_eq!(
        file,
        unhex("99bd3e820100050200000061000000000000050300000002000000")
    );
    assert_eq!(read(&file, 2), (recs(&[b"a"]), End::Clean));

    let mut file = closed(1, &[b"aaaaa"]);
    let second = unclosed(2, &[b"a"]);
    file[..second.len()].copy_from_slice(&second);
    assert_eq!(read(&file, 2), (recs(&[b"a"]), End::Unclean));
}

/// Fewer bytes after a log's last record than any header needs, and then the
/// end of the file, in a block the file does not fill. Pebble's reader does
/// not read a header there, and finds the block not used up (`r.end != r.n`
/// in `nextChunk`): the log ends uncleanly, whatever the bytes are. Seven
/// zeros or more are a zeroed header instead, the padding a block's tail
/// gets, up to eighteen: nineteen would fit a walSync header and are not
/// padding.
#[test]
fn bytes_too_few_for_a_header_end_a_short_last_block_uncleanly() {
    let log = unclosed(2, &[b"a"]);
    for n in 1..LEGACY_HEADER_LEN {
        for fill in [0, b'z'] {
            let file = [log.clone(), vec![fill; n]].concat();
            assert_eq!(
                read(&file, 2),
                (recs(&[b"a"]), End::Unclean),
                "{n} bytes of {fill:#04x}"
            );
        }
    }
    for n in LEGACY_HEADER_LEN..WAL_SYNC_HEADER_LEN {
        let file = [log.clone(), vec![0; n]].concat();
        assert_eq!(read(&file, 2), (recs(&[b"a"]), End::Clean), "{n} zeros");
    }
    let file = [log, vec![0; WAL_SYNC_HEADER_LEN]].concat();
    assert_eq!(read(&file, 2), (recs(&[b"a"]), End::Unclean), "19 zeros");
}

/// The same few bytes at the end of a full block are its padding, whatever
/// they hold: Pebble's reader reads the next block without looking at them,
/// and the log goes on there or ends with the file.
#[test]
fn bytes_too_few_for_a_header_at_the_end_of_a_full_block_are_padding() {
    let big = vec![b'p'; BLOCK_SIZE - RECYCLABLE_HEADER_LEN - 4];
    let mut file = unclosed(2, &[&big, b"b"]);
    file[BLOCK_SIZE - 4..BLOCK_SIZE].copy_from_slice(b"zzzz");
    assert_eq!(read(&file, 2), (recs(&[&big, b"b"]), End::Clean));

    file.truncate(BLOCK_SIZE);
    assert_eq!(read(&file, 2), (recs(&[&big]), End::Clean));
}

/// A record that ends five bytes short of a block boundary, which the writer
/// pads with five zeros, cut at four places:
///
/// - Before the padding: the file ends where a record does, which is clean.
/// - Two zeros into it: too few bytes for a header in a short block, which is
///   not.
/// - With all five: a full block, which is clean.
/// - With all five, then a chunk of another log in the next block, which is
///   what a recycled file holds there: not clean.
#[test]
fn a_record_that_ends_short_of_a_block_boundary() {
    let p = vec![b'q'; BLOCK_SIZE - RECYCLABLE_HEADER_LEN - 5];
    let log = unclosed(2, &[&p]);
    assert_eq!(
        log.len(),
        BLOCK_SIZE,
        "the writer pads the block's last five bytes"
    );

    assert_eq!(read(&log[..BLOCK_SIZE - 5], 2), (recs(&[&p]), End::Clean));
    assert_eq!(read(&log[..BLOCK_SIZE - 3], 2), (recs(&[&p]), End::Unclean));
    assert_eq!(read(&log, 2), (recs(&[&p]), End::Clean));
    let file = [log, unclosed(1, &[b"stale"])].concat();
    assert_eq!(read(&file, 2), (recs(&[&p]), End::Unclean));
}

/// `TestTruncatedLog`: a record that spans two blocks, read from the first
/// alone. The file ends at a block boundary, inside the record.
#[test]
fn a_record_cut_off_at_a_block_boundary_ends_the_log_uncleanly() {
    let file = closed(1, &[&vec![b's'; BLOCK_SIZE + 100]]);
    assert_eq!(read(&file[..BLOCK_SIZE], 1), (vec![], End::Unclean));
}

/// `TestRecycleLogWithPartialRecord`: log 2's record spans two blocks, and
/// the second block is the one log 1 left, as if the crash came between the
/// two. Log 1's chunk is not read as the end of log 2's record.
#[test]
fn a_record_finished_by_another_logs_chunk_ends_the_log_uncleanly() {
    let size = BLOCK_SIZE * 3 / 2;
    let first = closed(1, &[&vec![b'a'; size]]);
    let mut second = closed(2, &[&vec![b'b'; size]]);
    second[BLOCK_SIZE..].copy_from_slice(&first[BLOCK_SIZE..]);
    assert_eq!(read(&second, 2), (vec![], End::Unclean));
}
