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
//! `--rotate` flushes and then replaces the MANIFEST, so the file Pebble
//! replays is one roble wrote from nothing — every table at every level,
//! described by roble's snapshot edit and named by roble's marker.
//!
//! `--swept` makes the opposite claim, about what roble *removed*: it opens a
//! database and writes nothing at all, leaving only the obsolete files roble
//! collected on the way in. Pebble then has to open a directory roble has
//! deleted from and still find every key.

use std::path::PathBuf;

use roble_format::batch::Batch;

#[compio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let Some(dir) = args.next().map(PathBuf::from) else {
        eprintln!("usage: wal-replay <database-dir> [--flush|--rotate|--swept]");
        std::process::exit(2);
    };
    // With `--flush` the same writes go through a table and a MANIFEST edit
    // instead of staying in the log, so what Pebble reads back tests the
    // other half of the round trip. With `--swept` roble writes nothing and
    // the only change to the directory is what its open deleted.
    let mode = args.next();
    let (flush, rotate, swept) = match mode.as_deref() {
        None => (false, false, false),
        Some("--flush") => (true, false, false),
        // `--rotate` goes further than `--flush`: the MANIFEST Pebble reads
        // from afterwards is not one Pebble ever wrote a byte of.
        Some("--rotate") => (true, true, false),
        Some("--swept") => (false, false, true),
        Some(other) => fail(&format!("unknown mode {other}")),
    };

    let before = names_in(&dir);
    let db = roble::open(&roble::DefaultEnv, &dir)
        .await
        .unwrap_or_else(|e| fail(&format!("open {}: {e}", dir.display())));

    if swept {
        swept_run(&db, &dir, &before).await;
        return;
    }

    // Several batches, so the log holds more than one record, and one large
    // enough to span a 32 KiB block — the case where a chunk has to be split
    // and the second chunk's position depends on the first.
    let mut wrote: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    let mut batch = Batch::new();
    for i in 0..8 {
        let k = format!("wal-{i:03}").into_bytes();
        let v = format!("value-{i}").into_bytes();
        batch
            .put(&k, &v)
            .unwrap_or_else(|e| fail(&format!("put: {e}")));
        wrote.push((k, v));
    }
    db.write(&mut batch, true)
        .await
        .unwrap_or_else(|e| fail(&format!("write: {e}")));

    let mut batch = Batch::new();
    let big_key = b"wal-big".to_vec();
    let big_value = vec![b'x'; 40 * 1024];
    batch
        .put(&big_key, &big_value)
        .unwrap_or_else(|e| fail(&format!("put: {e}")));
    wrote.push((big_key, big_value));
    db.write(&mut batch, true)
        .await
        .unwrap_or_else(|e| fail(&format!("write: {e}")));

    // A delete of a key the tables hold, so Pebble has to apply roble's
    // tombstone and not merely add keys.
    let mut batch = Batch::new();
    batch
        .delete(b"alpha")
        .unwrap_or_else(|e| fail(&format!("delete: {e}")));
    db.write(&mut batch, true)
        .await
        .unwrap_or_else(|e| fail(&format!("write: {e}")));

    if flush {
        let table = db
            .flush()
            .await
            .unwrap_or_else(|e| fail(&format!("flush: {e}")))
            .unwrap_or_else(|| fail("flush wrote nothing"));
        println!("roble flushed the memtable to table {table:06}");
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
