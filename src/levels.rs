//! The shape of a store's tables, for a caller to look at (`Db::levels`).
//!
//! An owned copy of what the version names, with the user keys decoded out
//! of the internal keys a version keeps, so nothing in it reaches back into
//! the store: it holds no borrow and no table, and the store goes on flushing
//! and compacting while a caller reads it.

use std::rc::Rc;

use crate::env::Fs;
use crate::error::Result;
use crate::open::Db;

/// One level of the store, as [`Db::levels`] found it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct LevelInfo {
    /// Which level: L0, where flushes land, to L6 at the bottom.
    pub level: u32,
    /// How many layers of non-overlapping tables the level forms, which is
    /// how many of its tables a read may have to consult (§9).
    ///
    /// L0's tables are whole flushed memtables and may overlap, so for L0 it
    /// is anywhere from one to the number of tables. Every other level is one
    /// layer when it holds a table, and any level is none when it holds none.
    pub sublevels: usize,
    /// Its tables: L0's in file-number order, every other level's in key
    /// order.
    pub tables: Vec<TableInfo>,
}

/// One table, as [`Db::levels`] found it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TableInfo {
    /// The number in its file name: table 7 is `000007.sst`.
    pub file_num: u64,
    /// Its size on disk, in bytes.
    pub size: u64,
    /// The least user key it holds.
    pub smallest: Vec<u8>,
    /// The greatest user key it holds. When the table ends with a range
    /// deletion, the end of that deletion, which the table does not include.
    pub largest: Vec<u8>,
    /// The lowest sequence number of anything in it.
    pub smallest_seq_num: u64,
    /// The highest sequence number of anything in it.
    pub largest_seq_num: u64,
}

impl<F: Fs> Db<F> {
    /// Every level's tables, L0 first, as they are at this call.
    ///
    /// What `roble db tables` and `roble db stats` print, and what a caller
    /// watching read amplification or a level's size reads. An owned copy:
    /// it keeps no table from the obsolete sweep, and later flushes and
    /// compactions do not change it.
    ///
    /// Fails only on a table bound in the MANIFEST that is not an internal
    /// key. Opening a store checks the bounds of every level below L0, where
    /// the tables may not overlap, and not L0's.
    pub fn levels(&self) -> Result<Vec<LevelInfo>> {
        let version = Rc::clone(&self.version_set().version);
        version
            .levels()
            .map(|(level, tables)| {
                let sublevels = if level == 0 {
                    version.l0_sublevels()?
                } else {
                    usize::from(!tables.is_empty())
                };
                let tables = tables
                    .iter()
                    .map(|t| {
                        Ok(TableInfo {
                            file_num: t.file_num,
                            size: t.size,
                            smallest: t.smallest_user_key()?.to_vec(),
                            largest: t.largest_user_key()?.to_vec(),
                            smallest_seq_num: t.smallest_seq_num,
                            largest_seq_num: t.largest_seq_num,
                        })
                    })
                    .collect::<Result<_>>()?;
                Ok(LevelInfo {
                    level,
                    sublevels,
                    tables,
                })
            })
            .collect()
    }
}
