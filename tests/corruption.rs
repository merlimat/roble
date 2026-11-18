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

//! A damaged table makes the store refuse, never answer wrongly.
//!
//! `format/tests/corruption.rs` shows the format crate refusing a flipped
//! byte in any block of a table read whole. A store reads a table one block
//! at a time, through a table cache and a block cache, and decides which
//! blocks to read from an index and a filter. So this flips one byte inside
//! each block of two of `db-reads`'s tables in turn — one deep in L6 under
//! everything, one in L0 holding range deletions — and asks the store every
//! question it can be asked: `get` under every comparison for every probe,
//! scans end to end both ways, and, of the L0 table, a compaction.
//!
//! Every answer has to be the one the undamaged store gives, which
//! `tests/read.rs` holds to Pebble's, or a checksum mismatch. Nothing else:
//! not a different answer, not another error. And each damaged block has to
//! be caught by something, so a block the store never verified would fail
//! here rather than look like one it did. A compaction that fails has to
//! leave the version as it was.

use std::path::{Path, PathBuf};

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
use roble_format::block::Block;
use roble_format::sst::{Table, index_entries_of};

/// `db-reads` on a fresh simulated filesystem, with `damaged` in place of the
/// file of that name.
async fn load(damaged: Option<(&str, &[u8])>) -> (SimEnv, PathBuf) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-reads");
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let fs: &SimFs = env.sim();
    fs.mkdir_all(&dir).await.expect("mkdir");
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        let bytes = match damaged {
            Some((n, b)) if n == name => b.to_vec(),
            _ => std::fs::read(entry.path()).expect("read file"),
        };
        let disk = fs.create(&dir.join(&name)).await.expect("create");
        let (result, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
        result.expect("write");
        disk.sync(()).await.expect("sync");
        disk.close().await.expect("close");
    }
    fs.sync_dir(&dir).await.expect("sync dir");
    (env, dir)
}

async fn open(env: &SimEnv, dir: &Path) -> roble::Result<roble::Db<SimFs>> {
    roble::open_with(env, dir, Options::new().disable_automatic_compactions(true)).await
}

/// The offset of the block a checksum mismatch names, or `None` for any
/// other error.
fn checksum_offset(e: &roble::Error) -> Option<u64> {
    let roble::Error::Format(f) = e else {
        return None;
    };
    match std::error::Error::source(f)?.downcast_ref::<roble_format::Error>()? {
        roble_format::Error::ChecksumMismatch { offset, .. } => Some(*offset),
        _ => None,
    }
}

/// Every block of a table, by name, as `format/tests/corruption.rs` lists
/// them.
fn blocks(bytes: &[u8]) -> Vec<(String, roble_format::BlockHandle)> {
    let table = Table::open(bytes).expect("the undamaged table opens");
    let footer = table.footer();
    let mut out = vec![
        ("the metaindex".to_string(), footer.metaindex),
        ("the index".to_string(), footer.index),
    ];
    let meta = table.metaindex();
    for (name, handle) in [
        ("the properties", meta.properties),
        ("the range deletions", meta.range_del),
        ("the filter", meta.filter),
    ] {
        if let Some(h) = handle {
            out.push((name.to_string(), h));
        }
    }
    if table.has_two_level_index() {
        let top = table.decode_block(footer.index).expect("the index");
        let top = Block::new(top.as_slice()).expect("the index block");
        for (i, e) in index_entries_of(&top)
            .expect("partitions")
            .into_iter()
            .enumerate()
        {
            out.push((format!("index partition {i}"), e.handle));
        }
    }
    for (i, h) in table
        .data_block_handles()
        .expect("data blocks")
        .into_iter()
        .enumerate()
    {
        out.push((format!("data block {i}"), h));
    }
    out
}

/// One answer: what the store said, spelled so two can be compared, or the
/// offset a checksum mismatch named. Any other error fails the test there.
#[derive(Debug, PartialEq)]
enum Answer {
    Said(String),
    /// What a scan gave before it ended, which is all of it or, when the
    /// scan was refused, as far as it got.
    Entries(Vec<(Vec<u8>, Vec<u8>)>),
    Refused(u64),
}

fn answer<T: std::fmt::Debug>(what: &str, r: roble::Result<T>) -> Answer {
    match r {
        Ok(v) => Answer::Said(format!("{v:?}")),
        Err(e) => Answer::Refused(
            checksum_offset(&e)
                .unwrap_or_else(|| panic!("{what}: refused, but not by a checksum: {e}")),
        ),
    }
}

/// A whole scan in one direction: the entries it gave, and how it ended.
async fn scan(db: &roble::Db<SimFs>, forward: bool) -> (Vec<(Vec<u8>, Vec<u8>)>, Answer) {
    let mut out = Vec::new();
    let mut s = match db.range(..) {
        Ok(s) => s,
        Err(e) => return (out, answer("a scan", Err::<(), _>(e))),
    };
    let mut step = if forward {
        s.first().await
    } else {
        s.last().await
    };
    loop {
        if let Err(e) = step {
            return (out, answer("a scan", Err::<(), _>(e)));
        }
        if !s.is_valid() {
            return (out, Answer::Said("end".to_string()));
        }
        out.push((s.key().to_vec(), s.value().to_vec()));
        step = if forward {
            s.next().await
        } else {
            s.prev().await
        };
    }
}

/// Every answer the store gives, in one list, so a damaged store's can be
/// laid against an undamaged one's.
async fn every_answer(db: &roble::Db<SimFs>, probes: &[Vec<u8>]) -> Vec<(String, Answer)> {
    let mut out = Vec::new();
    for probe in probes {
        for how in [
            Comparison::Equal,
            Comparison::Ceiling,
            Comparison::Higher,
            Comparison::Floor,
            Comparison::Lower,
        ] {
            let what = format!("{how:?} of {probe:02x?}");
            let got = answer(&what, db.get(probe, how).await);
            out.push((what, got));
        }
    }
    for forward in [true, false] {
        let (entries, end) = scan(db, forward).await;
        let what = if forward {
            "a forward scan"
        } else {
            "a reverse scan"
        };
        out.push((format!("{what}, its entries"), Answer::Entries(entries)));
        out.push((format!("{what}, its end"), end));
    }
    out
}

/// Lays a damaged store's answers against the undamaged ones: each is the
/// same, or a checksum mismatch naming `block`. A scan that was refused may
/// have stopped short, but what it gave first is what the undamaged scan
/// gave first. Says how many were refused.
fn judge(when: &str, got: &[(String, Answer)], want: &[(String, Answer)], block: u64) -> usize {
    assert_eq!(got.len(), want.len());
    let mut refused = 0;
    for (i, ((what, g), (_, w))) in got.iter().zip(want).enumerate() {
        match (g, w) {
            (Answer::Refused(offset), _) => {
                assert_eq!(
                    *offset, block,
                    "{when}: {what} was refused for another block"
                );
                refused += 1;
            }
            (Answer::Entries(g), Answer::Entries(w))
                if matches!(got.get(i + 1), Some((_, Answer::Refused(_)))) =>
            {
                assert!(
                    w.starts_with(g),
                    "{when}: {what} gave entries the undamaged store does not"
                );
            }
            _ => assert_eq!(g, w, "{when}: {what} was answered wrongly"),
        }
    }
    refused
}

#[compio::test]
async fn a_damaged_block_is_refused_and_never_misread() {
    let probes: Vec<Vec<u8>> = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-reads.probes"),
    )
    .expect("the probes")
    .lines()
    .filter_map(|l| l.strip_prefix("probe "))
    .map(|h| {
        (0..h.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&h[i..i + 2], 16).expect("hex"))
            .collect()
    })
    .collect();

    let (env, dir) = load(None).await;
    let db = open(&env, &dir).await.expect("open the undamaged store");
    let want = every_answer(&db, &probes).await;
    let l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    drop(db);

    // The deepest table, which every read that gets past L0 lands in, and
    // an L0 table holding range deletions, which a compaction reads.
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-reads");
    let mut damaged = 0;
    for (name, in_l0) in [("000019.sst", false), ("000021.sst", true)] {
        let bytes = std::fs::read(src.join(name)).expect("the table");
        for (block, handle) in blocks(&bytes) {
            let when = format!("{name}, {block} damaged");
            let mut copy = bytes.clone();
            copy[(handle.offset + handle.length / 2) as usize] ^= 0x10;
            let (env, dir) = load(Some((name, &copy))).await;

            // An open reads no table, so it has nothing to refuse.
            let db = open(&env, &dir)
                .await
                .unwrap_or_else(|e| panic!("{when}: open: {e}"));
            let got = every_answer(&db, &probes).await;
            let mut refused = judge(&when, &got, &want, handle.offset);

            if in_l0 {
                let before = db.versions().version.table_file_nums();
                match db.compact(0, &l0).await {
                    Ok(_) => {
                        // A block the merge had no need of, the filter's:
                        // what it wrote is undamaged, so every answer is.
                        let after = every_answer(&db, &probes).await;
                        assert_eq!(
                            after, want,
                            "{when}: the compacted store answers differently"
                        );
                    }
                    Err(e) => {
                        assert_eq!(
                            checksum_offset(&e),
                            Some(handle.offset),
                            "{when}: the compaction failed, but not on the damaged block: {e}"
                        );
                        refused += 1;
                        assert_eq!(
                            db.versions().version.table_file_nums(),
                            before,
                            "{when}: a compaction that failed changed the version"
                        );
                        let again = every_answer(&db, &probes).await;
                        judge(
                            &format!("{when}, after the failed compaction"),
                            &again,
                            &want,
                            handle.offset,
                        );
                    }
                }
            }
            assert!(
                refused > 0,
                "{when}: nothing the store was asked noticed the damage"
            );
            damaged += 1;
        }
    }
    assert!(damaged > 10, "only {damaged} blocks were damaged");
}
