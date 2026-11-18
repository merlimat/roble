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

//! A batch that can be read back through itself before it commits (§6).
//!
//! Oxia builds one batch per replicated write and reads it back through
//! itself — `Get`, `FindLower`, `KeyRangeScan`, `RangeScan` on the batch —
//! before committing it, so this is Pebble's indexed batch and §6 says it is
//! not optional.
//!
//! **The problem it solves is ordering without numbers.** A batch's entries
//! have no sequence numbers while it is being built: those are handed out
//! when it commits. But a read through the batch has to put its entries above
//! everything in the store, and its later entries above its earlier ones. So
//! each entry stands in at [`key::SEQ_NUM_BATCH`] `| i`, which is above every
//! number a committed entry can hold and ascends in the order the entries
//! were written. The merge then needs no special case for the batch at all:
//! the ordinary "highest sequence number wins" gives the right answer, and
//! the only rule added anywhere is that a number with that bit is visible to
//! the scan reading through it (`crate::scan`).
//!
//! The stand-ins never leave this index. The repr carries no sequence numbers
//! until `commit` stamps its header, so nothing with the bit set reaches a
//! log, a table or a MANIFEST.
//!
//! **The index is a memtable**, the same arena skiplist a store keeps, rather
//! than Pebble's skiplist of offsets into the repr. The cost is that a
//! batch's keys and values are held twice over, once in the repr and once in
//! the arena; what it buys is that a batch is a merge source like any other,
//! with the cursor, the ordering and the tombstone list already written and
//! already tested. Oxia's batches are one replicated write each, so the
//! doubling is bounded by what the caller was going to hold anyway.

use std::cell::RefCell;
use std::ops::RangeBounds;
use std::rc::Rc;

use roble_format::batch::Batch;
use roble_format::key::{self, InternalKey, Kind};

use crate::env::Fs;
use crate::error::Result;
use crate::memtable::Memtable;
use crate::open::Db;
use crate::read::{Comparison, Found, successor};
use crate::scan::Scan;

/// A batch of writes, readable through itself.
///
/// Built by [`Db::indexed_batch`], read with [`IndexedBatch::get`] and
/// [`IndexedBatch::range`], and applied to the store by
/// [`IndexedBatch::commit`].
pub struct IndexedBatch<'a, F: Fs = crate::FileFs> {
    db: &'a Db<F>,
    repr: Batch,
    index: Rc<RefCell<Memtable>>,
}

impl<'a, F: Fs> IndexedBatch<'a, F> {
    pub(crate) fn new(db: &'a Db<F>) -> Self {
        IndexedBatch {
            db,
            repr: Batch::new(),
            // Seeded from where the store has got to, so two batches on one
            // store get skiplists of different shapes and a run still
            // replays (§9).
            index: Rc::new(RefCell::new(Memtable::new(
                db.last_seq_num().wrapping_add(1),
            ))),
        }
    }

    /// The stand-in number the next entry takes.
    ///
    /// Pebble uses the entry's byte offset in the repr; roble uses its index,
    /// which orders the entries the same way. Neither number means anything
    /// outside the batch, and neither survives the commit.
    fn next_seq(&self) -> u64 {
        key::SEQ_NUM_BATCH | self.repr.count() as u64
    }

    /// Adds a key and value.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let seq = self.next_seq();
        self.repr.put(key, value)?;
        self.index
            .borrow_mut()
            .insert(&key::encode(key, seq, Kind::Set), value)
    }

    /// Adds a key, letting `fill` write the value straight into the batch.
    ///
    /// What Oxia's `PutMarshalable` needs, so a protobuf is marshalled once
    /// rather than into a buffer that is then copied.
    pub fn put_with<G>(&mut self, key: &[u8], value_len: usize, fill: G) -> Result<()>
    where
        G: FnOnce(&mut [u8]),
    {
        let seq = self.next_seq();
        self.repr.put_with(key, value_len, fill)?;
        // The index needs the bytes `fill` wrote, and the value is the last
        // thing the entry appended, so it is the tail of the repr. Copying it
        // out is what the index's second copy costs; the caller still
        // marshals only once, which is the whole point of `put_with`.
        let repr = self.repr.as_bytes();
        let value = repr[repr.len() - value_len..].to_vec();
        self.index
            .borrow_mut()
            .insert(&key::encode(key, seq, Kind::Set), &value)
    }

    /// Deletes a key.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let seq = self.next_seq();
        self.repr.delete(key)?;
        self.index
            .borrow_mut()
            .insert(&key::encode(key, seq, Kind::Delete), &[])
    }

    /// Deletes every key in `start..end`.
    pub fn delete_range(&mut self, start: &[u8], end: &[u8]) -> Result<()> {
        let seq = self.next_seq();
        self.repr.delete_range(start, end)?;
        self.index.borrow_mut().add_range(start, end, seq)
    }

    /// How many entries it holds.
    pub fn count(&self) -> u32 {
        self.repr.count()
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.repr.is_empty()
    }

    /// How many bytes its representation takes.
    pub fn len(&self) -> usize {
        self.repr.len()
    }

    /// Its representation: the bytes a commit appends to the log.
    ///
    /// These are the bytes Oxia checksums and compares across replicas, so
    /// they are byte-identical to the ones Go Pebble builds for the same
    /// operations — a claim the golden fixture of `format/tests/batch_repr.rs`
    /// holds the format crate to, and which this batch inherits by encoding
    /// through it and nothing else.
    ///
    /// The eight header bytes that hold the sequence number are still zero.
    /// That is what a checksum here covers, and it is what Oxia relies on,
    /// taking the checksum before committing; since [`commit`](Self::commit)
    /// takes the batch, these are the only bytes it can ever hand out. The
    /// stand-in numbers its index orders itself by never reach them.
    pub fn as_bytes(&self) -> &[u8] {
        self.repr.as_bytes()
    }

    /// Reads the key `how` says, through the batch and the store together.
    ///
    /// `Equal` has a path of its own; the four ordered comparisons go through
    /// a scan, because the answer may be any key and only a merge knows which.
    ///
    /// This used to send `Equal` through the scan as well, on the grounds
    /// that a bloom filter knows nothing of a batch not yet written, so
    /// consulting one would answer half the question twice. Asking the batch
    /// **first** removes that objection: its index is exact, and an entry in
    /// it is above every committed sequence number, so if the batch has the
    /// key it has the whole answer and the store is never consulted.
    ///
    /// What the scan cost was the filter. A key that is **absent** is what a
    /// filter answers without reading a block at all, and on a settled store
    /// of fourteen tables an absent key costs about 318 ns through the point
    /// path against 4,192 ns through the merge — which is the case that
    /// matters, since Oxia reads a record's version metadata before every
    /// put and an insert is a key that is not there yet.
    pub async fn get(&self, key: &[u8], how: Comparison) -> Result<Option<Found>> {
        if how == Comparison::Equal {
            return self.equal(key).await;
        }
        let mut scan = self.range(..)?;
        match how {
            Comparison::Ceiling => scan.seek_ge(key).await?,
            Comparison::Higher => scan.seek_ge(&successor(key)).await?,
            // The greatest key at or below `key` is the greatest key strictly
            // below the one after it.
            Comparison::Floor => scan.seek_lt(&successor(key)).await?,
            Comparison::Lower => scan.seek_lt(key).await?,
            Comparison::Equal => unreachable!("Equal has its own path above"),
        }
        if !scan.is_valid() {
            return Ok(None);
        }
        Ok(Some(Found {
            key: scan.key().to_vec(),
            value: scan.value().to_vec(),
        }))
    }

    /// A point lookup: the batch, and the store only if the batch is silent.
    async fn equal(&self, key: &[u8]) -> Result<Option<Found>> {
        match self.says(key)? {
            Some(answer) => Ok(answer),
            None => self.db.get(key, Comparison::Equal).await,
        }
    }

    /// What this batch says about `key`, or `None` if it says nothing.
    ///
    /// `Some(None)` is a batch that **deletes** it, which is an answer and
    /// not a silence: a deletion here shadows whatever the store holds,
    /// because every stand-in number is above every committed one.
    fn says(&self, key: &[u8]) -> Result<Option<Option<Found>>> {
        let index = self.index.borrow();
        let target = key::search_key(key);

        // The newest entry the batch holds for this key. Trailers sort
        // descending, so the search key lands on the highest stand-in
        // number — the last `put` or `delete` of it.
        let mut newest = None;
        let mut cursor = index.cursor();
        cursor.seek_ge(&index, &target)?;
        if cursor.is_valid() {
            let k = InternalKey::decode(cursor.key(&index))?;
            if k.user_key == key {
                newest = Some((k.seq_num, k.kind));
            }
        }

        // And the newest range deletion of this batch's own that covers it.
        // A comparison rather than a short circuit, because a `put` after a
        // `delete_range` puts the key back.
        let covering = index
            .tombstones()
            .iter()
            .filter(|t| t.start.as_slice() <= key && key < t.end.as_slice())
            .map(|t| t.seq_num)
            .max();

        Ok(match (newest, covering) {
            (None, None) => None,
            (Some((seq, _)), Some(range)) if range > seq => Some(None),
            (None, Some(_)) => Some(None),
            (Some((_, kind)), _) if crate::table::is_deletion(kind) => Some(None),
            (Some(_), _) => Some(Some(Found {
                key: key.to_vec(),
                value: cursor.value(&index).to_vec(),
            })),
        })
    }

    /// A scan over the keys in `range`, reading the batch and the store
    /// together, as [`Db::range`] makes one over the store alone.
    pub fn range<'k>(&self, range: impl RangeBounds<&'k [u8]>) -> Result<Scan<F>> {
        let (lower, upper) = crate::scan::bounds(&range);
        self.db.scan_with(
            vec![Rc::clone(&self.index)],
            lower.as_deref(),
            upper.as_deref(),
        )
    }

    /// Applies the batch to the store, as durable when this returns as the
    /// store's [`Wal`](crate::Wal) setting makes every write.
    ///
    /// Takes the batch, because the stand-in numbers in its index are only
    /// meaningful before a commit: afterwards its entries are the store's,
    /// with sequence numbers of their own, and reading them through this
    /// index again would find every one of them twice.
    pub async fn commit(mut self) -> Result<()> {
        self.db.write(&mut self.repr).await
    }
}

/// How much it holds, not the entries: a batch can be a whole replicated
/// write, and its repr is what [`IndexedBatch::as_bytes`] is for.
impl<F: Fs> std::fmt::Debug for IndexedBatch<'_, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexedBatch")
            .field("count", &self.count())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<F: Fs> Db<F> {
    /// A batch that can be read back through itself before it commits.
    ///
    /// Every write in it is indexed as it is added, so that [`IndexedBatch::get`]
    /// and [`IndexedBatch::range`] see it alongside the store. A single write
    /// that is never read back is cheaper through [`Db::put`],
    /// [`Db::delete`] or [`Db::delete_range`].
    pub fn indexed_batch(&self) -> IndexedBatch<'_, F> {
        IndexedBatch::new(self)
    }
}
