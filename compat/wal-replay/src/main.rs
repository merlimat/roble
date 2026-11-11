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

//! Writes to a database's log with roble, and says what it wrote.
//!
//! Go Pebble then opens the same directory. Nothing roble wrote has been
//! flushed, so every key Pebble reports can only have come out of the log —
//! which means Pebble replaying roble's framing, roble's batch
//! representation, and roble's sequence numbers.
//!
//! `--no-wal` opens the same database in Oxia's configuration, where a write
//! reaches the memtable and nothing else: what Pebble reads back then came
//! through a table and a MANIFEST edit alone, with the log untouched.
//!
//! `--rotate` flushes and then replaces the MANIFEST, so the file Pebble
//! replays is one roble wrote from nothing — every table at every level,
//! described by roble's snapshot edit and named by roble's marker.
//!
//! `--swept` makes the opposite claim, about what roble *removed*: it opens a
//! database and writes nothing at all, leaving only the obsolete files roble
//! collected on the way in. Pebble then has to open a directory roble has
//! deleted from and still find every key.

use std::path::PathBuf;

#[compio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let Some(dir) = args.next().map(PathBuf::from) else {
        eprintln!(
            "usage: wal-replay <database-dir> [--flush|--rotate|--compact|--settle|--no-wal|--swept]"
        );
        std::process::exit(2);
    };
    // With `--flush` the same writes go through a table and a MANIFEST edit
    // instead of staying in the log, so what Pebble reads back tests the
    // other half of the round trip. With `--swept` roble writes nothing and
    // the only change to the directory is what its open deleted.
    let mode = args.next();
    let (flush, rotate, compact, settle, no_wal, swept) = match mode.as_deref() {
        None => (false, false, false, false, false, false),
        Some("--flush") => (true, false, false, false, false, false),
        // `--rotate` goes further than `--flush`: the MANIFEST Pebble reads
        // from afterwards is not one Pebble ever wrote a byte of.
        Some("--rotate") => (true, true, false, false, false, false),
        // `--compact` goes further still: the table Pebble reads is one
        // roble merged out of others, with fragmented range deletions and a
        // version edit that deletes as well as adds.
        Some("--compact") => (true, false, true, false, false, false),
        // `--settle` hands the picker the wheel until nothing is past its
        // allowance, so what Pebble opens is a database roble spread across
        // levels by itself.
        Some("--settle") => (true, false, false, true, false, false),
        // `--no-wal` is Oxia's own configuration: nothing reaches the log, so
        // the flush is the only thing that happened.
        Some("--no-wal") => (true, false, false, false, true, false),
        Some("--swept") => (false, false, false, false, false, true),
        Some(other) => fail(&format!("unknown mode {other}")),
    };

    let before = names_in(&dir);
    let mut options = roble::Options::new().disable_wal(no_wal);
    if settle {
        // Allowances a test-sized database can actually cross, so the picker
        // has something to do.
        options = options.l0_compaction_threshold(2).lbase_max_bytes(4096);
    }
    let db = roble::open_with(&roble::DefaultEnv, &dir, options)
        .await
        .unwrap_or_else(|e| fail(&format!("open {}: {e}", dir.display())));

    if swept {
        swept_run(&db, &dir, &before).await;
        return;
    }

    // Several batches, so the log holds more than one record, and one large
    // enough to span a 32 KiB block — the case where a chunk has to be split
    // and the second chunk's position depends on the first.
    //
    // Written through *indexed* batches, so what Pebble reads back is the
    // representation one of those produces. The index itself never reaches
    // the disk, which is the point: Go Pebble is what says the repr beneath
    // it is unchanged by having an index over it.
    let mut wrote: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    let mut batch = db.batch();
    for i in 0..8 {
        let k = format!("wal-{i:03}").into_bytes();
        let v = format!("value-{i}").into_bytes();
        batch
            .put(&k, &v)
            .unwrap_or_else(|e| fail(&format!("put: {e}")));
        wrote.push((k, v));
    }
    // Read back through itself before it commits, which is what Oxia does
    // and what an index is for.
    let seen = batch
        .get(b"wal-000", roble::Comparison::Equal)
        .await
        .unwrap_or_else(|e| fail(&format!("get through the batch: {e}")));
    if seen.map(|f| f.value).as_deref() != Some(b"value-0".as_slice()) {
        fail("the batch could not read back its own first key");
    }
    batch
        .commit(!no_wal)
        .await
        .unwrap_or_else(|e| fail(&format!("commit: {e}")));

    let mut batch = db.batch();
    let big_key = b"wal-big".to_vec();
    let big_value = vec![b'x'; 40 * 1024];
    batch
        .put(&big_key, &big_value)
        .unwrap_or_else(|e| fail(&format!("put: {e}")));
    wrote.push((big_key, big_value));
    batch
        .commit(!no_wal)
        .await
        .unwrap_or_else(|e| fail(&format!("commit: {e}")));

    // A delete of a key the tables hold, so Pebble has to apply roble's
    // tombstone and not merely add keys.
    let mut batch = db.batch();
    batch
        .delete(b"alpha")
        .unwrap_or_else(|e| fail(&format!("delete: {e}")));
    if batch
        .get(b"alpha", roble::Comparison::Equal)
        .await
        .unwrap_or_else(|e| fail(&format!("get through the batch: {e}")))
        .is_some()
    {
        fail("the batch did not read back its own delete");
    }
    batch
        .commit(!no_wal)
        .await
        .unwrap_or_else(|e| fail(&format!("commit: {e}")));

    if flush {
        let table = db
            .flush()
            .await
            .unwrap_or_else(|e| fail(&format!("flush: {e}")))
            .unwrap_or_else(|| fail("flush wrote nothing"));
        println!("roble flushed the memtable to table {table:06}");
    }

    if compact {
        // A second table over the same keys, so the merge has versions to
        // drop, plus a range deletion so the output carries a
        // range-deletion block.
        let mut batch = db.batch();
        for i in 0..8 {
            let k = format!("wal-{i:03}").into_bytes();
            batch
                .put(&k, b"compacted")
                .unwrap_or_else(|e| fail(&format!("put: {e}")));
            wrote.retain(|(key, _)| key != &k);
            wrote.push((k, b"compacted".to_vec()));
        }
        batch
            .delete_range(b"wal-100", b"wal-200")
            .unwrap_or_else(|e| fail(&format!("delete_range: {e}")));
        batch
            .commit(true)
            .await
            .unwrap_or_else(|e| fail(&format!("commit: {e}")));
        db.flush()
            .await
            .unwrap_or_else(|e| fail(&format!("flush: {e}")))
            .unwrap_or_else(|| fail("flush wrote nothing"));

        let l0: Vec<u64> = db
            .versions()
            .version
            .level(0)
            .iter()
            .map(|t| t.file_num)
            .collect();
        let out = db
            .compact(0, &l0)
            .await
            .unwrap_or_else(|e| fail(&format!("compact: {e}")))
            .unwrap_or_else(|| fail("the compaction wrote nothing"));
        println!("roble compacted {l0:?} into table {out:06} at L1");
    }

    if settle {
        // More rounds, so there is enough to spread over several levels.
        for round in 0..6 {
            let mut batch = db.batch();
            for i in 0..16 {
                let k = format!("settle-{:03}", (round * 7 + i) % 40).into_bytes();
                let v = format!("round-{round}").into_bytes();
                batch
                    .put(&k, &v)
                    .unwrap_or_else(|e| fail(&format!("put: {e}")));
                wrote.retain(|(key, _)| key != &k);
                wrote.push((k, v));
            }
            batch
                .commit(true)
                .await
                .unwrap_or_else(|e| fail(&format!("commit: {e}")));
            db.flush()
                .await
                .unwrap_or_else(|e| fail(&format!("flush: {e}")));

            let mut ran = 0;
            while db
                .compact_once()
                .await
                .unwrap_or_else(|e| fail(&format!("compact_once: {e}")))
                .is_some()
                || db.pick().is_some()
            {
                ran += 1;
                if ran > 50 {
                    fail("the picker never settled");
                }
            }
        }
        let spread: Vec<(u32, usize)> = db
            .versions()
            .version
            .levels()
            .map(|(l, t)| (l, t.len()))
            .filter(|(_, n)| *n > 0)
            .collect();
        if spread.len() < 2 {
            fail("the picker left everything in one level, so this proves nothing");
        }
        println!("roble compacted until it settled; levels are {spread:?}");
    }

    if no_wal {
        let logs = names_in(&dir)
            .into_iter()
            .filter(|n| n.ends_with(".log"))
            .collect::<Vec<_>>();
        println!("roble wrote with the WAL off; the logs it left alone are {logs:?}");
    }

    if rotate {
        let manifest = db
            .rotate_manifest()
            .await
            .unwrap_or_else(|e| fail(&format!("rotate: {e}")));
        db.remove_obsolete_files()
            .await
            .unwrap_or_else(|e| fail(&format!("sweep: {e}")));
        println!("roble rotated the MANIFEST to {manifest}, written from nothing");
    }

    // What Pebble should find, in the format its reader prints.
    let mut out = String::new();
    for (k, v) in &wrote {
        out.push_str(&format!("{} {}\n", hex(k), hex(v)));
    }
    out.push_str(&format!("deleted {}\n", hex(b"alpha")));
    std::fs::write(dir.join("roble-wrote.txt"), out)
        .unwrap_or_else(|e| fail(&format!("write the expectation: {e}")));

    println!(
        "roble wrote {} keys and one delete to {} ({})",
        wrote.len(),
        dir.display(),
        if flush { "flushed" } else { "log only" }
    );
}

/// Opens and deletes, and nothing else.
///
/// What this hands Pebble is a directory roble has removed files from: a log
/// the MANIFEST has moved past, which Pebble had kept to recycle. The
/// expectation is every key roble can still read, so Pebble reading the same
/// set is the claim — that collecting those files took nothing with them.
async fn swept_run<F: roble::Fs>(db: &roble::Opened<'_, F>, dir: &PathBuf, before: &[String]) {
    let after = names_in(dir);
    let gone: Vec<&String> = before.iter().filter(|n| !after.contains(n)).collect();
    if gone.is_empty() {
        fail("roble deleted nothing, so there is nothing for Pebble to miss");
    }

    let mut scan = db
        .scan()
        .await
        .unwrap_or_else(|e| fail(&format!("scan: {e}")));
    scan.first()
        .await
        .unwrap_or_else(|e| fail(&format!("first: {e}")));
    let mut out = String::new();
    let mut keys = 0;
    while scan.is_valid() {
        out.push_str(&format!("{} {}\n", hex(scan.key()), hex(scan.value())));
        keys += 1;
        scan.next()
            .await
            .unwrap_or_else(|e| fail(&format!("next: {e}")));
    }
    if keys == 0 {
        fail("the database holds no keys, so this proves nothing");
    }
    std::fs::write(dir.join("roble-wrote.txt"), out)
        .unwrap_or_else(|e| fail(&format!("write the expectation: {e}")));

    println!(
        "roble opened {} and collected {gone:?}, leaving {keys} keys",
        dir.display()
    );
}

/// What the directory holds, by name.
fn names_in(dir: &PathBuf) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| fail(&format!("read {}: {e}", dir.display())))
        .map(|e| {
            e.unwrap_or_else(|e| fail(&format!("entry: {e}")))
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn fail(message: &str) -> ! {
    eprintln!("wal-replay: {message}");
    std::process::exit(1);
}
