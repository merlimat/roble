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
use std::rc::Rc;

use roble_format::batch::Batch;
use roble_format::key::{self, Kind};

use crate::env::Fs;
use crate::error::Result;
use crate::memtable::Memtable;
use crate::open::Opened;
use crate::read::{Comparison, Found, successor};
use crate::scan::Scan;

/// A batch of writes, readable through itself.
///
/// Built by [`Opened::batch`], read with [`IndexedBatch::get`] and
/// [`IndexedBatch::scan`], and applied to the store by
/// [`IndexedBatch::commit`].
pub struct IndexedBatch<'a, F: Fs> {
    db: &'a Opened<F>,
    repr: Batch,
    index: Rc<RefCell<Memtable>>,
}

impl<'a, F: Fs> IndexedBatch<'a, F> {
    pub(crate) fn new(db: &'a Opened<F>) -> Self {
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
    /// Every comparison goes through a scan, including `Equal`. The point
    /// lookup a store does for `Equal` consults each table's bloom filter
    /// first, and a filter can only answer about what a table holds — it
    /// knows nothing of a batch that has not been written yet, so taking that
    /// path here would mean answering half the question twice.
    pub async fn get(&self, key: &[u8], how: Comparison) -> Result<Option<Found>> {
        let mut scan = self.scan().await?;
        match how {
            Comparison::Equal | Comparison::Ceiling => scan.seek_ge(key).await?,
            Comparison::Higher => scan.seek_ge(&successor(key)).await?,
            // The greatest key at or below `key` is the greatest key strictly
            // below the one after it.
            Comparison::Floor => scan.seek_lt(&successor(key)).await?,
            Comparison::Lower => scan.seek_lt(key).await?,
        }
        if !scan.is_valid() {
            return Ok(None);
        }
        if how == Comparison::Equal && scan.key() != key {
            return Ok(None);
        }
        Ok(Some(Found {
            key: scan.key().to_vec(),
            value: scan.value().to_vec(),
        }))
    }

    /// A scan over the batch and the store together.
    pub async fn scan(&self) -> Result<Scan<F::Disk>> {
        self.scan_range(None, None).await
    }

    /// The same, bounded below by `lower` (included) and above by `upper`
    /// (excluded).
    pub async fn scan_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<Scan<F::Disk>> {
        self.db
            .scan_with(vec![Rc::clone(&self.index)], lower, upper)
            .await
    }

    /// Applies the batch to the store, syncing the log if asked.
    ///
    /// Takes the batch, because the stand-in numbers in its index are only
    /// meaningful before a commit: afterwards its entries are the store's,
    /// with sequence numbers of their own, and reading them through this
    /// index again would find every one of them twice.
    pub async fn commit(mut self, sync: bool) -> Result<()> {
        self.db.write(&mut self.repr, sync).await
    }
}

impl<F: Fs> Opened<F> {
    /// A batch that can be read back through itself before it commits.
    pub fn batch(&self) -> IndexedBatch<'_, F> {
        IndexedBatch::new(self)
    }
}
