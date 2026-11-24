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

//! The level invariant: what lies above in a store is newer than what it
//! overlaps below (`Db::check_levels`).
//!
//! A store keeps what was written later above what was written earlier: the
//! memtables over the tables, L0 over L1, and each level over the ones below.
//! A lookup compares sequence numbers and does not lean on that, but three
//! things do. A compaction drops a deletion once no level below its output
//! holds the key (`crate::compact`). A scan passes over the keys of a deeper
//! level inside a range deletion from L1 or below without reading their
//! numbers (`crate::scan`). And Pebble, reading the same files, takes a range
//! deletion to delete everything under it in the levels below.
//!
//! The layers, from the top: the memtable writes go to, the one a flush is
//! writing out, each L0 table in the order Pebble stacks them (largest
//! sequence number first, then smallest, then file number, which puts any
//! two that overlap in the order of Pebble's sublevels), and then L1 to L6,
//! a layer each, since a level's tables do not overlap. A table's range
//! deletions count only inside its bounds, as reads take them.
//!
//! Four rules, each of them strict. One sequence number is one write, which
//! lies in one place, so the same number above and below is a write that
//! came back a second time, as a log replayed twice brings back its records.
//! 1. A key is newer than every version of itself below it.
//! 2. A key is newer than every range deletion below it that covers it.
//! 3. A range deletion is newer than every one below it over the same keys.
//! 4. A range deletion is newer than every key below it that it covers.
//!
//! The first three are Pebble's `DB.CheckLevels` (`level_checker.go`),
//! checked in its order, so that both report a store's first fault alike.
//! Pebble states its rule strictly too, though its check finds the same
//! number twice only when its heap happens to give the deeper one first.
//! It leaves out the fourth, which its merging iterator relies on as roble's
//! scan does: an older range deletion above a key hides it.

use std::cmp::Reverse;
use std::fmt;
use std::rc::Rc;

use roble_format::InternalKey;
use roble_format::key::Kind;
use roble_format::manifest::NUM_LEVELS;

use crate::env::Fs;
use crate::error::{Error, Result};
use crate::open::Db;
use crate::table::Caching;
use crate::version::Table;

/// Where something was found.
#[derive(Clone, Copy)]
enum Place {
    Memtable,
    Table { level: u32, file_num: u64 },
}

impl fmt::Display for Place {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Place::Memtable => write!(f, "a memtable"),
            Place::Table { level, file_num } => write!(f, "L{level} table {file_num}"),
        }
    }
}

/// A point key, and how far down it was found: layer 0 is the top.
struct Point {
    user: Vec<u8>,
    seq: u64,
    kind: Kind,
    layer: usize,
    place: Place,
}

/// A range deletion, inside the bounds of the table that holds it.
struct Span {
    start: Vec<u8>,
    end: Vec<u8>,
    seq: u64,
    layer: usize,
    place: Place,
}

/// A point key or a range deletion, by its index in the check's lists.
#[derive(Clone, Copy)]
enum Item {
    Point(usize),
    Span(usize),
}

impl<F: Fs> Db<F> {
    /// Checks that what lies above in the store is newer than what it
    /// overlaps below: Pebble's `DB.CheckLevels`, and the one rule more that
    /// `crate::check` gives.
    ///
    /// For tests and tools. It reads every key the store holds, and keeps
    /// them all in memory while it checks.
    #[doc(hidden)]
    pub async fn check_levels(&self) -> Result<()> {
        // The memtables and the version together, with no await between
        // them, so that they are one moment's: a flush landing while the
        // tables are read moves a memtable into a table this version does not
        // name. Holding the version keeps its tables from the sweep.
        let memtables: Vec<_> = self.memtables_each().collect();
        let version = Rc::clone(&self.version_set().version);

        let mut points = Vec::new();
        let mut spans = Vec::new();
        let mut layer = 0;
        for m in &memtables {
            let m = m.borrow();
            let mut cursor = m.cursor();
            cursor.first(&m);
            while cursor.is_valid() {
                let k = InternalKey::decode(cursor.key(&m))?;
                points.push(Point {
                    user: k.user_key.to_vec(),
                    seq: k.seq_num,
                    kind: k.kind,
                    layer,
                    place: Place::Memtable,
                });
                cursor.next(&m);
            }
            for t in m.tombstones() {
                spans.push(Span {
                    start: t.start.clone(),
                    end: t.end.clone(),
                    seq: t.seq_num,
                    layer,
                    place: Place::Memtable,
                });
            }
            layer += 1;
        }
        let mut l0: Vec<&Table> = version.level(0).iter().collect();
        l0.sort_by_key(|t| Reverse((t.largest_seq_num, t.smallest_seq_num, t.file_num)));
        for t in l0 {
            self.read_for_check(0, t, layer, &mut points, &mut spans)
                .await?;
            layer += 1;
        }
        for level in 1..NUM_LEVELS {
            for t in version.level(level) {
                self.read_for_check(level, t, layer, &mut points, &mut spans)
                    .await?;
            }
            layer += 1;
        }
        check(points, spans)
    }

    /// Every point key and range deletion of one table, into the lists.
    async fn read_for_check(
        &self,
        level: u32,
        meta: &Table,
        layer: usize,
        points: &mut Vec<Point>,
        spans: &mut Vec<Span>,
    ) -> Result<()> {
        let place = Place::Table {
            level,
            file_num: meta.file_num,
        };
        let table = self.open_table(meta.file_num).await?;
        let mut it = table.iter_with(Caching::Discard);
        it.first().await?;
        while it.is_valid() {
            let k = InternalKey::decode(it.key())?;
            points.push(Point {
                user: k.user_key.to_vec(),
                seq: k.seq_num,
                kind: k.kind,
                layer,
                place,
            });
            it.next().await?;
        }
        // Cut to the table's bounds, as a read takes them: a lookup or a
        // level of a scan consults only the table whose bounds hold the key,
        // and Pebble truncates a table's range deletions to them. A table
        // written by roble or by Pebble's v2 writer never holds one past its
        // bounds; an older writer's could.
        let lower = meta.smallest_user_key()?;
        let mut upper = meta.largest_user_key()?.to_vec();
        if !meta.largest_is_exclusive()? {
            upper.push(0);
        }
        for t in &table.tombstones {
            let start = t.start.as_slice().max(lower);
            let end = t.end.as_slice().min(upper.as_slice());
            if start < end {
                spans.push(Span {
                    start: start.to_vec(),
                    end: end.to_vec(),
                    seq: t.seq_num,
                    layer,
                    place,
                });
            }
        }
        Ok(())
    }
}

/// The four rules of `crate::check`, over everything a store holds.
fn check(mut points: Vec<Point>, spans: Vec<Span>) -> Result<()> {
    points.sort_by(|a, b| a.user.cmp(&b.user).then(b.seq.cmp(&a.seq)));

    // Every range deletion's ends, which cut the keys into pieces each
    // covered by the same range deletions throughout.
    let mut cuts: Vec<&[u8]> = spans
        .iter()
        .flat_map(|s| [s.start.as_slice(), s.end.as_slice()])
        .collect();
    cuts.sort_unstable();
    cuts.dedup();
    let mut pieces: Vec<Vec<Item>> = vec![Vec::new(); cuts.len().saturating_sub(1)];
    for (i, s) in spans.iter().enumerate() {
        let from = cuts.partition_point(|c| *c < s.start.as_slice());
        let to = cuts.partition_point(|c| *c < s.end.as_slice());
        for piece in &mut pieces[from..to] {
            piece.push(Item::Span(i));
        }
    }

    // Newest first, and of two with one number the deeper first, so that
    // the one above comes after it and is found at fault.
    let order =
        |a: &Item, b: &Item| seq_layer(&points, &spans, *b).cmp(&seq_layer(&points, &spans, *a));

    // Each key's versions with the range deletions over it, in that order.
    let mut keys: Vec<Vec<Item>> = Vec::new();
    let mut first = 0;
    for versions in points.chunk_by(|a, b| a.user == b.user) {
        let user = versions[0].user.as_slice();
        let n = cuts.partition_point(|c| *c <= user);
        let over: &[Item] = match n.checked_sub(1).and_then(|i| pieces.get(i)) {
            Some(piece) => piece,
            None => &[],
        };
        let mut key: Vec<Item> = (first..first + versions.len())
            .map(Item::Point)
            .chain(over.iter().copied())
            .collect();
        key.sort_by(order);
        keys.push(key);
        first += versions.len();
    }

    let show = |item: Item, piece: Option<usize>| -> String {
        match item {
            Item::Point(i) => {
                let p = &points[i];
                format!(
                    "{}#{},{} in {}",
                    p.user.escape_ascii(),
                    p.seq,
                    p.kind.name().to_uppercase(),
                    p.place
                )
            }
            Item::Span(i) => {
                let s = &spans[i];
                let (start, end) = match piece {
                    Some(at) => (cuts[at], cuts[at + 1]),
                    None => (s.start.as_slice(), s.end.as_slice()),
                };
                format!(
                    "{}-{}:{{(#{},RANGEDEL)}} in {}",
                    start.escape_ascii(),
                    end.escape_ascii(),
                    s.seq,
                    s.place
                )
            }
        }
    };
    // The first item of `items`, in that order, with one before it that is
    // deeper, so no older than it, of the kinds `rule` looks at; and that one.
    let above = |items: &[Item], rule: &dyn Fn(Item, Item) -> bool| -> Option<(Item, Item)> {
        items.iter().enumerate().find_map(|(j, &top)| {
            let (_, layer) = seq_layer(&points, &spans, top);
            items[..j]
                .iter()
                .find(|&&deeper| seq_layer(&points, &spans, deeper).1 > layer && rule(top, deeper))
                .map(|&deeper| (top, deeper))
        })
    };
    let fault = |top: Item, deeper: Item, piece: Option<usize>| {
        let how = if seq_layer(&points, &spans, top).0 == seq_layer(&points, &spans, deeper).0 {
            "numbered the same"
        } else {
            "newer"
        };
        Err(Error::Corrupt(format!(
            "{} is above {}, which is {how}",
            show(top, piece),
            show(deeper, piece)
        )))
    };

    // Rules 1 and 2: a key above a version of itself, or a range deletion
    // over it, that is no older.
    for key in &keys {
        if let Some((top, deeper)) = above(key, &|top, _| matches!(top, Item::Point(_))) {
            return fault(top, deeper, None);
        }
    }
    // Rule 3: a range deletion above one no older, piece by piece.
    for (at, piece) in pieces.iter().enumerate() {
        let mut piece = piece.clone();
        piece.sort_by(order);
        if let Some((top, deeper)) = above(&piece, &|_, _| true) {
            return fault(top, deeper, Some(at));
        }
    }
    // Rule 4: a range deletion above a key it covers that is no older.
    for key in &keys {
        if let Some((top, deeper)) = above(key, &|top, deeper| {
            matches!((top, deeper), (Item::Span(_), Item::Point(_)))
        }) {
            return fault(top, deeper, None);
        }
    }
    Ok(())
}

/// An item's sequence number and layer.
fn seq_layer(points: &[Point], spans: &[Span], item: Item) -> (u64, usize) {
    match item {
        Item::Point(i) => (points[i].seq, points[i].layer),
        Item::Span(i) => (spans[i].seq, spans[i].layer),
    }
}
