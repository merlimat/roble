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

//! The memtable against a model, which is what §12 asked for.
//!
//! Until now roble could only read, so the operations were Pebble's and
//! Pebble was both the only model available and the stronger one. A memtable
//! is the first thing roble **writes**, so a model can be driven by the same
//! operations — and a `BTreeMap` is a worthwhile oracle precisely because a
//! skiplist is a different structure reached by a different algorithm. The
//! two agreeing says something; two `BTreeMap`s agreeing would not.
//!
//! The operations are generated from a fixed seed, so a failure is
//! reproducible.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use roble::memtable::Memtable;
use roble_format::key::{self, Kind, TRAILER_LEN};

/// An encoded internal key ordered as Pebble orders them, so that a
/// `BTreeMap` of these is the memtable's contents in the memtable's order.
#[derive(PartialEq, Eq, Clone, Debug)]
struct Key(Vec<u8>);

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        let (a_user, a_trailer) = self.0.split_at(self.0.len() - TRAILER_LEN);
        let (b_user, b_trailer) = other.0.split_at(other.0.len() - TRAILER_LEN);
        let a = u64::from_le_bytes(a_trailer.try_into().unwrap());
        let b = u64::from_le_bytes(b_trailer.try_into().unwrap());
        // User key ascending, trailer descending.
        a_user.cmp(b_user).then_with(|| b.cmp(&a))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A small, reproducible generator: the same seed gives the same run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Builds a memtable and the model beside it from the same operations.
fn build(seed: u64, ops: usize) -> (Memtable, BTreeMap<Key, Vec<u8>>) {
    let mut rng = Rng(seed);
    let mut table = Memtable::new(seed);
    let mut model = BTreeMap::new();

    // A small key alphabet, so the same user key gets many versions and the
    // trailer ordering is exercised rather than assumed.
    let users: Vec<Vec<u8>> = (0..12u8)
        .map(|i| format!("k{}", i % 7).into_bytes())
        .chain((0..4u8).map(|i| vec![b'z', i]))
        .collect();

    for i in 0..ops {
        let user = &users[rng.below(users.len() as u64) as usize];
        // Sequence numbers mostly ascend, but sometimes repeat an earlier one
        // so that an exact duplicate internal key happens and replacement is
        // exercised.
        let seq = if rng.below(8) == 0 {
            rng.below(16)
        } else {
            i as u64
        };
        let kind = match rng.below(4) {
            0 => Kind::Delete,
            1 => Kind::SetWithDelete,
            _ => Kind::Set,
        };
        let encoded = key::encode(user, seq, kind);
        let value = format!("v{i}").into_bytes();

        table.insert(&encoded, &value).expect("insert");
        model.insert(Key(encoded), value);
    }
    (table, model)
}

/// Every entry of the memtable, forwards.
fn forward(table: &Memtable) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = table.cursor();
    it.first(table);
    let mut out = Vec::new();
    while it.is_valid() {
        out.push((it.key(table).to_vec(), it.value(table).to_vec()));
        it.next(table);
    }
    out
}

/// Every entry of the memtable, backwards.
fn backward(table: &Memtable) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = table.cursor();
    it.last(table);
    let mut out = Vec::new();
    while it.is_valid() {
        out.push((it.key(table).to_vec(), it.value(table).to_vec()));
        it.prev(table);
    }
    out
}

fn model_entries(model: &BTreeMap<Key, Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .iter()
        .map(|(k, v)| (k.0.clone(), v.clone()))
        .collect()
}

#[test]
fn a_memtable_holds_what_the_model_holds() {
    for seed in [1u64, 2, 99, 12345] {
        let (table, model) = build(seed, 400);
        let want = model_entries(&model);

        assert_eq!(table.len(), model.len(), "seed {seed}: count");
        assert_eq!(forward(&table), want, "seed {seed}: forward");

        let mut reversed = want.clone();
        reversed.reverse();
        assert_eq!(backward(&table), reversed, "seed {seed}: reverse");
    }
}

#[test]
fn seeks_land_where_the_model_says() {
    for seed in [7u64, 4242] {
        let (table, model) = build(seed, 300);
        let entries = model_entries(&model);

        // Probes on, around and between the keys that are there, with
        // trailers above and below every stored one.
        let mut probes: Vec<Vec<u8>> = Vec::new();
        for user in ["k0", "k3", "k6", "z\u{0}", "a", "zz", ""] {
            for seq in [0u64, 5, 50, 1_000] {
                probes.push(key::encode(user.as_bytes(), seq, Kind::Set));
            }
            probes.push(key::search_key(user.as_bytes()));
        }
        for (k, _) in entries.iter().take(40) {
            probes.push(k.clone());
        }

        for probe in &probes {
            let want_ge = entries
                .iter()
                .position(|(k, _)| Key(k.clone()) >= Key(probe.clone()));
            let mut it = table.cursor();
            it.seek_ge(&table, probe).expect("seek_ge");
            match want_ge {
                Some(i) => {
                    assert!(it.is_valid(), "seed {seed}: seek_ge should land");
                    assert_eq!(it.key(&table), &entries[i].0[..], "seed {seed}: seek_ge");
                    assert_eq!(
                        it.value(&table),
                        &entries[i].1[..],
                        "seed {seed}: seek_ge value"
                    );
                }
                None => assert!(!it.is_valid(), "seed {seed}: seek_ge past the end"),
            }

            let want_lt = entries
                .iter()
                .rposition(|(k, _)| Key(k.clone()) < Key(probe.clone()));
            let mut it = table.cursor();
            it.seek_lt(&table, probe).expect("seek_lt");
            match want_lt {
                Some(i) => {
                    assert!(it.is_valid(), "seed {seed}: seek_lt should land");
                    assert_eq!(it.key(&table), &entries[i].0[..], "seed {seed}: seek_lt");
                }
                None => assert!(!it.is_valid(), "seed {seed}: seek_lt before the start"),
            }
        }
    }
}

#[test]
fn a_seek_can_be_stepped_away_from_in_either_direction() {
    let (table, model) = build(31, 200);
    let entries = model_entries(&model);

    for (i, (k, _)) in entries.iter().enumerate() {
        let mut it = table.cursor();
        it.seek_ge(&table, k).expect("seek_ge");
        assert_eq!(it.key(&table), &k[..]);

        it.next(&table);
        match entries.get(i + 1) {
            Some((n, _)) => assert_eq!(it.key(&table), &n[..], "forward from {i}"),
            None => assert!(!it.is_valid(), "past the last entry"),
        }

        let mut it = table.cursor();
        it.seek_ge(&table, k).expect("seek_ge");
        it.prev(&table);
        match i.checked_sub(1).and_then(|j| entries.get(j)) {
            Some((p, _)) => assert_eq!(it.key(&table), &p[..], "backward from {i}"),
            None => assert!(!it.is_valid(), "before the first entry"),
        }
    }
}

#[test]
fn a_step_back_retraces_a_step_forward_as_keys_go_in_between() {
    // A step back follows the link each node keeps to the one before it, and
    // an insert has to keep that right for two nodes: the new one, and the
    // one after it, which used to point back at the new one's predecessor.
    // So the keys go in scattered — each lands between two already there, or
    // before the first, or after the last — and after every insert the walk
    // backwards has to be the walk forwards reversed.
    //
    // A cursor left on the key after the new one is stepped back as well,
    // which is what a scan open across a write does (`MemSource` in
    // scan.rs): it has to land on the new key and not skip it.
    const N: u64 = 211;
    let mut table = Memtable::new(3);
    for i in 0..N {
        // A stride coprime with N, so every key goes in once.
        let n = i * 89 % N;
        let k = key::encode(format!("k{n:03}").as_bytes(), n, Kind::Set);

        let mut after = table.cursor();
        after.seek_ge(&table, &k).expect("seek_ge");
        let was_after = after.is_valid().then(|| after.key(&table).to_vec());
        table.insert(&k, b"v").expect("insert");
        if let Some(was_after) = was_after {
            after.prev(&table);
            assert!(
                after.is_valid() && after.key(&table) == &k[..],
                "stepping back from {:?} skipped k{n:03}, just inserted before it",
                String::from_utf8_lossy(&was_after)
            );
        }

        let forwards = forward(&table);
        let mut backwards = backward(&table);
        backwards.reverse();
        assert_eq!(forwards.len() as u64, i + 1, "after k{n:03}: forwards");
        assert_eq!(backwards, forwards, "after inserting k{n:03}");
    }
}

#[test]
fn inserting_a_key_twice_replaces_its_value() {
    // Pebble gives every write its own sequence number, so this never happens
    // there — which is exactly why the behaviour has to be decided and stated
    // rather than left to whatever the structure does.
    let mut table = Memtable::new(5);
    let k = key::encode(b"dup", 9, Kind::Set);
    table.insert(&k, b"first").expect("insert");
    table.insert(&k, b"second").expect("insert");

    assert_eq!(table.len(), 1, "a replacement is not a second entry");
    let entries = forward(&table);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1, b"second");
    assert!(
        table.bytes() >= k.len() + b"first".len() + b"second".len(),
        "the arena still counts the bytes it was given"
    );
}

#[test]
fn an_empty_memtable_has_nowhere_to_go() {
    let table = Memtable::new(1);
    assert!(table.is_empty());
    assert_eq!(table.bytes(), 0);

    let mut it = table.cursor();
    it.first(&table);
    assert!(!it.is_valid());
    it.last(&table);
    assert!(!it.is_valid());
    it.seek_ge(&table, &key::search_key(b"anything"))
        .expect("seek_ge");
    assert!(!it.is_valid());
    it.seek_lt(&table, &key::search_key(b"anything"))
        .expect("seek_lt");
    assert!(!it.is_valid());
    // Stepping an invalid iterator stays invalid rather than panicking.
    it.next(&table);
    it.prev(&table);
    assert!(!it.is_valid());
}

#[test]
fn a_key_that_is_not_an_internal_key_is_refused() {
    let mut table = Memtable::new(1);
    assert!(table.insert(b"short", b"v").is_err(), "no trailer");
    let mut it = table.cursor();
    assert!(
        it.seek_ge(&table, b"short").is_err(),
        "no trailer to compare against"
    );
    assert!(it.seek_lt(&table, b"short").is_err());
}

// ------------------------------------------------------------ applying a batch

use roble_format::batch::Batch;

/// The first entry the memtable holds for `user`, which is its newest version.
fn newest(table: &Memtable, user: &[u8]) -> Option<(u64, Kind, Vec<u8>)> {
    let mut it = table.cursor();
    it.seek_ge(table, &key::search_key(user)).expect("seek");
    if !it.is_valid() {
        return None;
    }
    let k = roble_format::InternalKey::decode(it.key(table)).expect("decode");
    if k.user_key != user {
        return None;
    }
    Some((k.seq_num, k.kind, it.value(table).to_vec()))
}

#[test]
fn a_batch_lands_in_the_memtable_with_ascending_sequence_numbers() {
    let mut b = Batch::new();
    b.put(b"alpha", b"one").expect("put");
    b.delete(b"bravo").expect("delete");
    b.put(b"charlie", b"three").expect("put");

    let mut table = Memtable::new(1);
    let next = table.apply(b.as_bytes(), 100).expect("apply");

    assert_eq!(next, 103, "three entries from 100 leaves the next at 103");
    assert_eq!(table.len(), 3);
    assert_eq!(
        newest(&table, b"alpha"),
        Some((100, Kind::Set, b"one".to_vec()))
    );
    assert_eq!(
        newest(&table, b"bravo"),
        Some((101, Kind::Delete, Vec::new())),
        "a delete carries no value"
    );
    assert_eq!(
        newest(&table, b"charlie"),
        Some((102, Kind::Set, b"three".to_vec()))
    );
}

#[test]
fn the_later_write_in_a_batch_wins() {
    // Entry i gets base + i, and the memtable orders newest first, so the
    // second put is the one a read meets. This is the whole reason the
    // numbering is per entry rather than per batch.
    let mut b = Batch::new();
    b.put(b"k", b"first").expect("put");
    b.put(b"k", b"second").expect("put");
    b.delete(b"gone").expect("delete");
    b.put(b"gone", b"back").expect("put");

    let mut table = Memtable::new(1);
    table.apply(b.as_bytes(), 1).expect("apply");

    assert_eq!(
        newest(&table, b"k"),
        Some((2, Kind::Set, b"second".to_vec()))
    );
    assert_eq!(
        newest(&table, b"gone"),
        Some((4, Kind::Set, b"back".to_vec())),
        "the put after the delete is the newer entry"
    );
    assert_eq!(table.len(), 4, "every version is kept, not just the winner");
}

#[test]
fn range_deletions_are_kept_apart_from_the_point_keys() {
    // §6: a reader that walks data blocks alone must see a table with
    // tombstones as though it had none. The same holds in a memtable.
    let mut b = Batch::new();
    b.put(b"a", b"v").expect("put");
    b.delete_range(b"m", b"p").expect("delete_range");
    b.put(b"z", b"v").expect("put");

    let mut table = Memtable::new(1);
    table.apply(b.as_bytes(), 7).expect("apply");

    assert_eq!(table.len(), 2, "the tombstone is not a point key");
    assert_eq!(table.tombstones().len(), 1);
    let t = &table.tombstones()[0];
    assert_eq!(t.start, b"m");
    assert_eq!(t.end, b"p");
    assert_eq!(t.seq_num, 8, "it took the sequence number of its position");

    // Walking the point keys meets no tombstone.
    let mut it = table.cursor();
    it.first(&table);
    let mut kinds = Vec::new();
    while it.is_valid() {
        kinds.push(
            roble_format::InternalKey::decode(it.key(&table))
                .expect("decode")
                .kind,
        );
        it.next(&table);
    }
    assert_eq!(kinds, vec![Kind::Set, Kind::Set]);
}

#[test]
fn tombstones_are_kept_sorted_by_where_they_start() {
    let mut b = Batch::new();
    for (start, end) in [
        (&b"m"[..], &b"p"[..]),
        (b"a", b"c"),
        (b"x", b"z"),
        (b"d", b"f"),
    ] {
        b.delete_range(start, end).expect("delete_range");
    }
    let mut table = Memtable::new(1);
    table.apply(b.as_bytes(), 1).expect("apply");

    let starts: Vec<&[u8]> = table
        .tombstones()
        .iter()
        .map(|t| t.start.as_slice())
        .collect();
    assert_eq!(starts, [&b"a"[..], b"d", b"m", b"x"]);
}

#[test]
fn a_batch_roble_cannot_serve_is_refused() {
    // MERGE is the one Oxia configures no merger for; roble refuses it on the
    // way in rather than storing something it cannot later resolve.
    let mut b = Batch::new();
    b.put(b"k", b"v").expect("put");
    // The format crate has no `merge`, so the repr is built by hand: kind 2,
    // then key and value, with the count raised to match.
    let mut repr = b.as_bytes().to_vec();
    repr.push(Kind::Merge.to_byte());
    repr.push(1);
    repr.push(b'm');
    repr.push(1);
    repr.push(b'v');
    let count = u32::from_le_bytes(repr[8..12].try_into().unwrap()) + 1;
    repr[8..12].copy_from_slice(&count.to_le_bytes());

    let mut table = Memtable::new(1);
    match table.apply(&repr, 1) {
        Err(roble::Error::Unsupported(m)) => assert!(m.contains("merge"), "got: {m}"),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[test]
fn a_truncated_batch_is_refused() {
    let mut table = Memtable::new(1);
    assert!(table.apply(b"short", 1).is_err(), "no header");

    let mut b = Batch::new();
    b.put(b"k", b"v").expect("put");
    let repr = b.as_bytes();
    assert!(
        table.apply(&repr[..repr.len() - 1], 1).is_err(),
        "an entry cut short"
    );
}

/// Builds a batch repr by hand with one range deletion, so the bounds can be
/// anything — the format crate's `delete_range` will not build a bad one.
fn hand_built_range(start: &[u8], end: &[u8]) -> Vec<u8> {
    let mut repr = vec![0u8; 12];
    repr[8..12].copy_from_slice(&1u32.to_le_bytes());
    repr.push(Kind::RangeDelete.to_byte());
    repr.push(start.len() as u8);
    repr.extend_from_slice(start);
    repr.push(end.len() as u8);
    repr.extend_from_slice(end);
    repr
}

#[test]
fn an_empty_range_deletion_is_dropped_rather_than_refused() {
    // Pebble's DeleteRange(a, a) is legal and covers nothing, and roble
    // applies batches that came out of Pebble's own WAL — so refusing one
    // would mean refusing a database Pebble wrote.
    let mut table = Memtable::new(1);
    let next = table
        .apply(&hand_built_range(b"m", b"m"), 5)
        .expect("an empty range is not an error");

    assert_eq!(next, 6, "it still consumed its sequence number");
    assert!(
        table.tombstones().is_empty(),
        "a tombstone covering nothing has nothing to say"
    );
}

#[test]
fn a_reversed_range_deletion_is_refused() {
    let mut table = Memtable::new(1);
    match table.apply(&hand_built_range(b"m", b"a"), 1) {
        Err(roble::Error::Corrupt(m)) => {
            assert!(m.contains("ends before it starts"), "got: {m}")
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[test]
fn the_arena_counts_a_tombstone_from_where_it_is_held() {
    let mut table = Memtable::new(1);
    let before = table.bytes();
    table
        .apply(&hand_built_range(b"aaa", b"zzzz"), 1)
        .expect("apply");
    assert_eq!(
        table.bytes() - before,
        b"aaa".len() + b"zzzz".len(),
        "the budget has to see a tombstone's bytes, wherever they live"
    );
}
