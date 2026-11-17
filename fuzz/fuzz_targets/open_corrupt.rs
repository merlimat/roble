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

//! A database Pebble wrote, damaged, then opened and used.
//!
//! One of the golden databases has its files overwritten, flipped, cut
//! short, extended, removed or renamed, is put on a simulated filesystem, and
//! is opened. If the open succeeds the store is read every way it can be —
//! both scans, every comparison, the dump — and, opened to write, written,
//! flushed and compacted, which reads every table again on the way.
//!
//! Any error is allowed, at any step: these are files no writer produced.
//! A panic, an abort or a hang is not, since any of them takes down the
//! process the store is embedded in rather than failing the one call.
//!
//! The format crate's fuzz targets reach the same decoders one file at a
//! time; this is the path the engine actually takes, which reads a table
//! through `OpenTable` rather than `Table::open`, replays the MANIFEST and
//! the logs, and trusts what each told it about the others.

#![no_main]

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use roble::sim::SimEnv;
use roble::{Comparison, Db, Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

#[derive(Arbitrary, Debug)]
struct Case {
    fixture: u8,
    read_only: bool,
    mutations: Vec<Mutation>,
}

/// One change to one file, picked by index into the database's listing.
#[derive(Arbitrary, Debug)]
enum Mutation {
    Overwrite { file: u8, at: u32, bytes: Vec<u8> },
    Flip { file: u8, at: u32, mask: u8 },
    Truncate { file: u8, at: u32 },
    Append { file: u8, bytes: Vec<u8> },
    Remove { file: u8 },
    Rename { file: u8, to: Name },
}

/// A name the engine would recognise, so that a rename makes a file into
/// something it reads rather than something it ignores.
#[derive(Arbitrary, Debug)]
enum Name {
    Table(u32),
    Log(u32),
    Manifest(u32),
    Options(u32),
    ManifestMarker(u32, u32),
    FormatMarker(u32, u16),
    Raw(String),
}

impl Name {
    fn render(&self) -> Option<String> {
        let name = match self {
            Name::Table(n) => format!("{n:06}.sst"),
            Name::Log(n) => format!("{n:06}.log"),
            Name::Manifest(n) => format!("MANIFEST-{n:06}"),
            Name::Options(n) => format!("OPTIONS-{n:06}"),
            Name::ManifestMarker(i, n) => format!("marker.manifest.{i:06}.MANIFEST-{n:06}"),
            Name::FormatMarker(i, v) => format!("marker.format-version.{i:06}.{v:03}"),
            Name::Raw(s) => s.clone(),
        };
        // A file name, not a path: the store lists one directory.
        let ok = !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\0']);
        ok.then_some(name)
    }
}

type Files = Vec<(String, Vec<u8>)>;

/// The golden databases, read once.
fn fixtures() -> &'static [Files] {
    static FIXTURES: OnceLock<Vec<Files>> = OnceLock::new();
    FIXTURES.get_or_init(|| {
        let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/golden");
        let mut dbs: Vec<PathBuf> = std::fs::read_dir(&golden)
            .expect("tests/golden")
            .map(|e| e.expect("entry").path())
            .filter(|p| p.is_dir())
            .collect();
        dbs.sort();
        dbs.iter()
            .map(|db| {
                let mut files: Files = std::fs::read_dir(db)
                    .expect("a golden database")
                    .map(|e| {
                        let e = e.expect("entry");
                        let name = e.file_name().into_string().expect("UTF-8 name");
                        (name, std::fs::read(e.path()).expect("read"))
                    })
                    .collect();
                files.sort();
                files
            })
            .collect()
    })
}

fn mutate(files: &mut Files, m: &Mutation) {
    if files.is_empty() {
        return;
    }
    let n = files.len();
    let pick = |file: u8| usize::from(file) % n;
    let at = |at: u32, len: usize| at as usize % (len + 1);
    match m {
        Mutation::Overwrite {
            file,
            at: pos,
            bytes,
        } => {
            let data = &mut files[pick(*file)].1;
            let start = at(*pos, data.len());
            let end = (start + bytes.len()).min(data.len());
            data[start..end].copy_from_slice(&bytes[..end - start]);
        }
        Mutation::Flip {
            file,
            at: pos,
            mask,
        } => {
            let data = &mut files[pick(*file)].1;
            if !data.is_empty() {
                let i = *pos as usize % data.len();
                data[i] ^= mask;
            }
        }
        Mutation::Truncate { file, at: pos } => {
            let data = &mut files[pick(*file)].1;
            let len = at(*pos, data.len());
            data.truncate(len);
        }
        Mutation::Append { file, bytes } => files[pick(*file)].1.extend_from_slice(bytes),
        Mutation::Remove { file } => {
            files.remove(pick(*file));
        }
        Mutation::Rename { file, to } => {
            if let Some(name) = to.render() {
                let (_, bytes) = files.remove(pick(*file));
                // Onto another file's name replaces it, as a rename does.
                files.retain(|(n, _)| *n != name);
                files.push((name, bytes));
            }
        }
    }
}

/// Reads the whole store every way there is, stopping at the first error.
async fn read<F: Fs>(db: &Db<F>) -> roble::Result<()> {
    let mut keys = Vec::new();
    let mut scan = db.range(..)?;
    scan.first().await?;
    while scan.is_valid() {
        keys.push(scan.key().to_vec());
        scan.next().await?;
    }
    scan.last().await?;
    while scan.is_valid() {
        scan.prev().await?;
    }
    drop(scan);

    keys.truncate(32);
    keys.extend([b"".to_vec(), b"/oxia/".to_vec(), b"\xff\xff".to_vec()]);
    for key in &keys {
        for how in [
            Comparison::Equal,
            Comparison::Floor,
            Comparison::Ceiling,
            Comparison::Lower,
            Comparison::Higher,
        ] {
            db.get(key, how).await?;
        }
    }
    db.dump()?;
    Ok(())
}

/// Writes, flushes and compacts until there is nothing left to compact.
async fn write<F: Fs>(db: &Db<F>) -> roble::Result<()> {
    let mut batch = Batch::new();
    batch.put(b"/oxia/fuzz", b"value")?;
    batch.delete_range(b"/oxia/a", b"/oxia/m")?;
    db.write_raw(&mut batch, true).await?;
    db.flush().await?;
    // Bounded, so a picker that keeps finding work is a slow input rather
    // than a hang — a store that never runs out is its own bug, but not
    // the one this looks for.
    for _ in 0..16 {
        if db.compact_once().await?.is_empty() {
            break;
        }
    }
    read(db).await
}

async fn run(case: Case) {
    let all = fixtures();
    let pristine = case.mutations.is_empty();
    let mut files = all[usize::from(case.fixture) % all.len()].clone();
    for m in &case.mutations {
        mutate(&mut files, m);
    }

    let env = SimEnv::new();
    let fs = env.sim();
    let dir = PathBuf::from("/db");
    fs.mkdir_all(&dir).await.expect("mkdir");
    for (name, bytes) in files {
        let disk = fs.create(&dir.join(name)).await.expect("create");
        let (r, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
        r.expect("write");
        disk.sync(()).await.expect("sync");
        disk.close().await.expect("close");
    }
    fs.sync_dir(&dir).await.expect("sync dir");

    let options = Options::new().disable_automatic_compactions(true);
    let opened = if case.read_only {
        roble::open_read_only(&env, &dir, options).await
    } else {
        roble::open_with(&env, &dir, options).await
    };
    // An undamaged database must work, or every damaged one is only being
    // refused for the same unrelated reason.
    if pristine {
        let db = opened.expect("a golden database opens");
        read(&db).await.expect("a golden database reads");
        if !case.read_only {
            write(&db).await.expect("a golden database writes");
        }
        return;
    }
    let Ok(db) = opened else {
        return;
    };
    if read(&db).await.is_ok() && !case.read_only {
        let _ = write(&db).await;
    }
}

thread_local! {
    static RUNTIME: compio::runtime::Runtime =
        compio::runtime::Runtime::new().expect("a compio runtime");
}

fuzz_target!(|case: Case| {
    RUNTIME.with(|rt| rt.block_on(run(case)));
});
