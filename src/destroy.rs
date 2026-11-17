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

//! Removing a database: Oxia's `KV.Delete` (§6, §11).
//!
//! Oxia deletes a shard's store when the shard leaves the node, and its
//! `Delete` is `Close()` followed by `os.RemoveAll`. The order here is the
//! whole content of this module, and it is chosen so that there is never a
//! moment at which the database is both half gone and unowned.
//!
//! **No permit, because ownership has already said it.** A flush or a
//! compaction still running while the files went would write a table into a
//! directory with no MANIFEST left to name it, and then fail in the middle of
//! its own edit — which is what §5's one-change permit exists to prevent. A
//! destroy needs none of it: every other operation on a store takes `&self`
//! and this one takes `self`, so an outstanding borrow anywhere forbids the
//! move, and a store shared into a task of its own lives behind an `Rc` that
//! has to be sole-owned before `self` can be had back (§10,
//! `tests/open.rs`). There is no way to ask for a destroy while the store is
//! busy.
//!
//! What may still be alive is a [`Scan`](crate::Scan) or a
//! [`Snapshot`](crate::Snapshot): those hold open readers and no borrow of
//! the store. They read tables they opened already and write nothing, so they
//! have no quarrel with the removal — their files keep their bytes until the
//! last reader goes, exactly as under the obsolete sweep.
//!
//! **The lock last.** `destroy` takes the store by value, so the directory's
//! lock is held until the last file is gone and released on the way out. For
//! as long as anything is left there is still an owner, so nothing can open
//! the remains. Oxia's closes first and removes afterwards, which leaves that
//! window open; roble does not need to, because the two halves are one call.
//!
//! **No flush.** Oxia's `Delete` goes through `Close`, which flushes on its
//! way out — a memtable written into a table that is removed a moment later.
//! roble does not copy that: the point of a destroy is that none of it
//! survives.
//!
//! **The readers go before the names.** A reader is an open file, and a
//! filesystem reclaims nothing while one is open, so a destroy that left the
//! table cache holding readers would free the names and not the space.
//! `crate::obsolete` lets go at exactly this point and for this reason.
//!
//! **The directory itself stays**, empty. roble was handed a path, and `Fs`
//! has no way to remove a directory — Pebble's `vfs.FS` has one, but a method
//! on roble's is a method every embedder has to write (§10). An `open` makes
//! what it needs again, and what matters is that every file the database
//! consisted of, `LOCK` included, is gone.

use crate::env::Fs;
use crate::error::Result;
use crate::open::Opened;

impl<F: Fs> Opened<F> {
    /// Removes every file of this database, consuming the store.
    ///
    /// Oxia's `KV.Delete`. Returns the names it removed, as
    /// [`Opened::remove_obsolete_files`] does.
    ///
    /// The database's directory is left behind, empty: see the module's
    /// documentation for that and for the order of what follows. A
    /// subdirectory, which roble never makes, is left behind with it rather
    /// than failing the call on a `remove` that was never going to work.
    ///
    /// A table some [`Scan`](crate::Scan) or [`Snapshot`](crate::Snapshot) is
    /// still reading keeps its bytes until that reader is dropped, exactly as
    /// it does when the obsolete sweep retires one. The name goes either way.
    pub async fn destroy(self) -> Result<Vec<String>> {
        self.writable()?;
        let names: Vec<String> = self
            .fs()
            .list(&self.dir)
            .await?
            .iter()
            .filter(|e| !e.is_dir)
            .map(|e| e.name.to_string_lossy().into_owned())
            .collect();

        for file_num in self.readers().open() {
            self.forget_reader(file_num);
        }

        let mut removed = Vec::with_capacity(names.len());
        for name in names {
            self.fs().remove(&self.dir.join(&name)).await?;
            removed.push(name);
        }
        // Synced, which the obsolete sweep does not bother with. An obsolete
        // file that comes back after a crash is collected by the next sweep;
        // a *database* that comes back is a shard the node was told to forget,
        // holding data it would then open and serve. The simulated filesystem
        // forgets a removed name outright, so no test can tell this line from
        // its absence — stated rather than tested (§12).
        self.fs().sync_dir(&self.dir).await?;
        Ok(removed)
    }
}
