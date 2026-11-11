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
    let mut it = table.iter();
    it.first();
    let mut out = Vec::new();
    while it.is_valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next();
    }
    out
}

/// Every entry of the memtable, backwards.
fn backward(table: &Memtable) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = table.iter();
    it.last();
    let mut out = Vec::new();
    while it.is_valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.prev();
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
            let mut it = table.iter();
            it.seek_ge(probe).expect("seek_ge");
            match want_ge {
                Some(i) => {
                    assert!(it.is_valid(), "seed {seed}: seek_ge should land");
                    assert_eq!(it.key(), &entries[i].0[..], "seed {seed}: seek_ge");
                    assert_eq!(it.value(), &entries[i].1[..], "seed {seed}: seek_ge value");
                }
                None => assert!(!it.is_valid(), "seed {seed}: seek_ge past the end"),
            }

            let want_lt = entries
                .iter()
                .rposition(|(k, _)| Key(k.clone()) < Key(probe.clone()));
            let mut it = table.iter();
            it.seek_lt(probe).expect("seek_lt");
            match want_lt {
                Some(i) => {
                    assert!(it.is_valid(), "seed {seed}: seek_lt should land");
                    assert_eq!(it.key(), &entries[i].0[..], "seed {seed}: seek_lt");
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
        let mut it = table.iter();
        it.seek_ge(k).expect("seek_ge");
        assert_eq!(it.key(), &k[..]);

        it.next();
        match entries.get(i + 1) {
            Some((n, _)) => assert_eq!(it.key(), &n[..], "forward from {i}"),
            None => assert!(!it.is_valid(), "past the last entry"),
        }

        let mut it = table.iter();
        it.seek_ge(k).expect("seek_ge");
        it.prev();
        match i.checked_sub(1).and_then(|j| entries.get(j)) {
            Some((p, _)) => assert_eq!(it.key(), &p[..], "backward from {i}"),
            None => assert!(!it.is_valid(), "before the first entry"),
        }
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

    let mut it = table.iter();
    it.first();
    assert!(!it.is_valid());
    it.last();
    assert!(!it.is_valid());
    it.seek_ge(&key::search_key(b"anything")).expect("seek_ge");
    assert!(!it.is_valid());
    it.seek_lt(&key::search_key(b"anything")).expect("seek_lt");
    assert!(!it.is_valid());
    // Stepping an invalid iterator stays invalid rather than panicking.
    it.next();
    it.prev();
    assert!(!it.is_valid());
}

#[test]
fn a_key_that_is_not_an_internal_key_is_refused() {
    let mut table = Memtable::new(1);
    assert!(table.insert(b"short", b"v").is_err(), "no trailer");
    let mut it = table.iter();
    assert!(
        it.seek_ge(b"short").is_err(),
        "no trailer to compare against"
    );
    assert!(it.seek_lt(b"short").is_err());
}
