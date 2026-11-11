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

use std::path::PathBuf;

use roble_format::batch::Batch;

#[compio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let Some(dir) = args.next().map(PathBuf::from) else {
        eprintln!("usage: wal-replay <database-dir>");
        std::process::exit(2);
    };

    let db = roble::open(&roble::DefaultEnv, &dir)
        .await
        .unwrap_or_else(|e| fail(&format!("open {}: {e}", dir.display())));

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

    // What Pebble should find, in the format its reader prints.
    let mut out = String::new();
    for (k, v) in &wrote {
        out.push_str(&format!("{} {}\n", hex(k), hex(v)));
    }
    out.push_str(&format!("deleted {}\n", hex(b"alpha")));
    std::fs::write(dir.join("roble-wrote.txt"), out)
        .unwrap_or_else(|e| fail(&format!("write the expectation: {e}")));

    println!(
        "roble wrote {} keys and one delete to the log of {}",
        wrote.len(),
        dir.display()
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn fail(message: &str) -> ! {
    eprintln!("wal-replay: {message}");
    std::process::exit(1);
}
