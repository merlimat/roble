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

//! What roble writes, read back: tables, batches, framed records, and range
//! tombstones through fragmentation.
//!
//! Unlike the decode targets, these compare. A table must scan, seek and
//! filter exactly as the entries put into it say; a batch and a framed
//! stream must give back what went in; fragmenting tombstones must leave
//! every key covered at exactly the sequence numbers it was covered at.

#![no_main]

use std::cmp::Ordering;
use std::collections::BTreeSet;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use roble_format::batch::{self, Batch};
use roble_format::block::Value;
use roble_format::filter::Filter;
use roble_format::key::{self, SEQ_NUM_MAX, search_key};
use roble_format::rangedel::{self, Tombstone};
use roble_format::record::{RecordReader, RecordWriter};
use roble_format::sst::{Table, TableWriter, WriterOptions};
use roble_format::{InternalKey, Kind, dump_table};

#[derive(Arbitrary, Debug)]
enum Case {
    Table(TableCase),
    Batch(u64, Vec<BatchOp>),
    Records(RecordsCase),
    Fragment(Vec<(Vec<u8>, Vec<u8>, u32)>),
}

#[derive(Arbitrary, Debug)]
struct TableCase {
    block_size: u16,
    filter: bool,
    /// User key, sequence number, a kind chosen from [`POINT_KINDS`], value.
    points: Vec<(Vec<u8>, u32, u8, Vec<u8>)>,
    tombstones: Vec<(Vec<u8>, Vec<u8>, u32)>,
    probes: Vec<Vec<u8>>,
}

/// The point kinds the writer takes and the reader serves.
const POINT_KINDS: [Kind; 5] = [
    Kind::Set,
    Kind::Delete,
    Kind::SetWithDelete,
    Kind::SingleDelete,
    Kind::DeleteSized,
];

#[derive(Arbitrary, Debug)]
enum BatchOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    SingleDelete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
    DeleteSized(Vec<u8>, u32),
}

#[derive(Arbitrary, Debug)]
struct RecordsCase {
    recyclable: bool,
    log_num: u32,
    /// Each record is the bytes given, then `len` copies of `fill`, so a
    /// small input can still make records that cross 32 KiB blocks.
    records: Vec<(Vec<u8>, u8, u16)>,
    eof: bool,
}

/// Tombstones as an engine could have them: each at a sequence number of its
/// own, since every entry of every batch is given one.
fn tombstones(raw: &[(Vec<u8>, Vec<u8>, u32)]) -> Vec<Tombstone> {
    let mut seen = BTreeSet::new();
    raw.iter()
        .filter(|(s, e, seq)| s < e && seen.insert(*seq))
        .map(|(s, e, seq)| Tombstone {
            start: s.clone(),
            end: e.clone(),
            seq_num: u64::from(*seq),
        })
        .collect()
}

fn fragment(all: &[Tombstone]) -> Vec<Tombstone> {
    rangedel::fragment(&all.iter().collect::<Vec<_>>())
}

fn table(case: TableCase) {
    // Sorted as a table holds them: user key ascending, then newest first.
    // A user key at a sequence number it already has is dropped.
    let mut points: Vec<(Vec<u8>, u64, Kind, Vec<u8>)> = case
        .points
        .into_iter()
        .map(|(k, seq, kind, v)| {
            let kind = POINT_KINDS[kind as usize % POINT_KINDS.len()];
            (k, u64::from(seq), kind, v)
        })
        .collect();
    points.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    points.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
    let fragments = fragment(&tombstones(&case.tombstones));

    let mut w = TableWriter::new(
        WriterOptions::default()
            .block_size(usize::from(case.block_size))
            .filter_bits_per_key(case.filter.then_some(10)),
    )
    .expect("a writer");
    for (k, seq, kind, v) in &points {
        let ikey = InternalKey {
            user_key: k,
            seq_num: *seq,
            kind: *kind,
            obsolete: false,
        };
        w.add(ikey, v).expect("an ordered point is written");
    }
    for t in &fragments {
        w.add_range_deletion(&t.start, &t.end, t.seq_num)
            .expect("a fragment is written");
    }
    let bytes = w.finish().expect("a table is finished");

    dump_table(&bytes).expect("a written table dumps");
    let table = Table::open(&bytes).expect("a written table opens");
    // Pebble's count, which includes the tombstones.
    assert_eq!(
        table.properties().num_entries().expect("num_entries"),
        Some((points.len() + fragments.len()) as u64)
    );
    assert_eq!(table.range_deletions().expect("tombstones"), fragments);

    let mut got = Vec::new();
    table
        .for_each_entry(|k, v| {
            let Value::InPlace(v) = v else {
                panic!("roble writes only in-place values, read {}", v.kind_name());
            };
            got.push((k.user_key.to_vec(), k.seq_num, k.kind, v.to_vec()));
            Ok(())
        })
        .expect("a written table scans");
    assert_eq!(got, points, "a scan disagrees with what was written");

    // Seeks, against the entries as encoded internal keys: every stored key
    // (the equality case a separator can meet) and a search key per probe.
    let encoded: Vec<Vec<u8>> = points
        .iter()
        .map(|(k, seq, kind, _)| key::encode(k, *seq, *kind))
        .collect();
    let targets = encoded.iter().cloned().chain(
        case.probes
            .iter()
            .chain(points.iter().map(|p| &p.0))
            .map(|k| search_key(k)),
    );
    for target in targets {
        let cmp = |e: &Vec<u8>| key::compare(e, &target).expect("internal keys");
        let ge = encoded.iter().position(|e| cmp(e) != Ordering::Less);
        let lt = encoded.iter().rposition(|e| cmp(e) == Ordering::Less);
        for (want, is_ge) in [(ge, true), (lt, false)] {
            let mut seen = None;
            let f = |k: InternalKey<'_>, _: Value<'_>| {
                seen = Some(key::encode(k.user_key, k.seq_num, k.kind));
                Ok(())
            };
            let found = if is_ge {
                table.seek_ge(&target, f)
            } else {
                table.seek_lt(&target, f)
            }
            .expect("a written table seeks");
            assert_eq!(found, want.is_some());
            assert_eq!(
                seen.as_ref(),
                want.map(|i| &encoded[i]),
                "{} {:?}",
                if is_ge { "seek_ge" } else { "seek_lt" },
                target
            );
        }
    }

    if case.filter {
        if let Some(block) = table.filter_block().expect("filter block") {
            let filter = Filter::from_block(block.as_slice())
                .expect("a written filter decodes")
                .expect("a written filter is one roble reads");
            for (k, ..) in &points {
                assert!(filter.may_contain(k), "a filter denies a key it holds");
            }
        } else {
            assert!(points.is_empty(), "a filter was asked for and not written");
        }
    }
}

fn batch(seq: u64, ops: Vec<BatchOp>) {
    let mut b = Batch::new();
    let mut want: Vec<(Kind, Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    for op in ops {
        let (r, entry) = match op {
            BatchOp::Put(k, v) => (b.put(&k, &v), (Kind::Set, k, Some(v))),
            BatchOp::Delete(k) => (b.delete(&k), (Kind::Delete, k, None)),
            BatchOp::SingleDelete(k) => (b.single_delete(&k), (Kind::SingleDelete, k, None)),
            BatchOp::DeleteRange(s, e) => (b.delete_range(&s, &e), (Kind::RangeDelete, s, Some(e))),
            // The value is the size as a varint, which is the batch's
            // business; only that there is one is checked.
            BatchOp::DeleteSized(k, n) => {
                (b.delete_sized(&k, n), (Kind::DeleteSized, k, Some(vec![])))
            }
        };
        if r.is_err() {
            return;
        }
        want.push(entry);
    }
    let seq = seq & SEQ_NUM_MAX;
    b.set_seq_num(seq);
    assert_eq!(b.count() as usize, want.len());
    assert_eq!(batch::seq_num_of(b.as_bytes()).expect("seq num"), seq);

    let mut r = Batch::read(b.as_bytes()).expect("a batch reads");
    for (kind, k, v) in want {
        let e = r
            .next()
            .expect("an entry per op")
            .expect("a written entry reads");
        assert_eq!((e.kind, e.key), (kind, &k[..]));
        match (kind, v) {
            (Kind::DeleteSized, _) => assert!(e.value.is_some()),
            (_, v) => assert_eq!(e.value, v.as_deref()),
        }
    }
    assert!(
        r.next().is_none(),
        "a batch reads more entries than it was given"
    );
}

fn records(case: RecordsCase) {
    // Held to a few MiB, which is past a hundred blocks and keeps a run fast.
    let mut total = 0;
    let payloads: Vec<Vec<u8>> = case
        .records
        .into_iter()
        .map(|(head, fill, len)| {
            let mut p = head;
            p.resize(p.len() + usize::from(len), fill);
            p
        })
        .take_while(|p| {
            total += p.len();
            total <= 4 << 20
        })
        .collect();
    let mut w = if case.recyclable {
        RecordWriter::recyclable(case.log_num)
    } else {
        RecordWriter::legacy()
    };
    for p in &payloads {
        w.write_record(p).expect("a record is framed");
    }
    if case.eof && case.recyclable {
        w.write_eof_trailer().expect("an EOF trailer");
    }
    let bytes = w.finish();

    let mut r = RecordReader::new(&bytes, case.log_num);
    for (i, p) in payloads.iter().enumerate() {
        let got = r
            .next()
            .unwrap_or_else(|| panic!("record {i} of {} is missing", payloads.len()))
            .unwrap_or_else(|e| panic!("record {i} does not read: {e}"));
        assert_eq!(&got, p, "record {i} changed");
    }
    match r.next() {
        None => {}
        Some(Ok(extra)) => panic!("a record nobody wrote, {} bytes", extra.len()),
        Some(Err(e)) => panic!("a framed stream ends in an error: {e}"),
    }
}

/// Every key a tombstone bounds, and one just past each, is covered by the
/// fragments at exactly the sequence numbers the tombstones covered it at.
fn fragments(raw: Vec<(Vec<u8>, Vec<u8>, u32)>) {
    let all = tombstones(&raw);
    let frags = fragment(&all);
    let mut b = rangedel::Builder::new();
    for t in &frags {
        b.add(&t.start, &t.end, t.seq_num)
            .expect("fragments are fragmented");
    }
    let covering = |ts: &[Tombstone], k: &[u8]| -> BTreeSet<u64> {
        ts.iter()
            .filter(|t| &t.start[..] <= k && k < &t.end[..])
            .map(|t| t.seq_num)
            .collect()
    };
    for t in &all {
        for k in [&t.start, &t.end] {
            let mut past = k.clone();
            past.push(0);
            for k in [k, &past] {
                assert_eq!(covering(&frags, k), covering(&all, k), "covering {k:?}");
            }
        }
    }
}

fuzz_target!(|case: Case| match case {
    Case::Table(c) => table(c),
    Case::Batch(seq, ops) => batch(seq, ops),
    Case::Records(c) => records(c),
    Case::Fragment(raw) => fragments(raw),
});
