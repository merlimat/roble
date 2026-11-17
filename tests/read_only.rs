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

//! A store opened read-only, beside a writer that holds the directory
//! (DESIGN.md §6, "Reading beside a writer").
//!
//! The reader runs on [`ReaderFs`], which shares the writer's simulated
//! filesystem and panics on anything that would change it. So every test here
//! also checks the claim that a reader writes nothing — not by comparing the
//! directory afterwards, which a file created and removed again would pass,
//! but by refusing the call that would have done it.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use roble::sim::{SimDisk, SimEnv, SimFs, SimLock};
use roble::{Access, Comparison, DirEntry, Disk, DiskUsage, Env, Error, Fs, Options, WriteBuf};
use roble_format::batch::Batch;
use roble_format::files::{self, FileType};

/// A filesystem a reader may read and nothing more.
///
/// The writer in these tests opens through it too, with `reader` off, so
/// that one instantiation of the engine's generic code runs both the
/// writer's paths and the reader's. The coverage gate counts the best
/// instantiation of each function rather than their union (DESIGN.md §12),
/// so a reader on a type of its own would leave every line only it runs
/// counted as missed.
///
/// It can also play the writer's part at the moment a reader opens a file: a
/// name in `gone` is removed just before the reader opens it, and `grow`
/// appends to one file just before the reader opens another. Those are the
/// races a reader beside a live writer meets, placed where a test wants them.
#[derive(Clone)]
struct ReaderFs {
    sim: SimFs,
    /// Whether to refuse every change; off for the writer.
    reader: bool,
    gone: Rc<RefCell<Vec<PathBuf>>>,
    grow: Rc<RefCell<Option<(PathBuf, PathBuf)>>>,
}

impl ReaderFs {
    fn new(sim: &SimFs) -> Self {
        ReaderFs {
            sim: sim.clone(),
            reader: true,
            gone: Default::default(),
            grow: Default::default(),
        }
    }

    /// The same filesystem, for the writer.
    fn writing(sim: &SimFs) -> Self {
        ReaderFs {
            reader: false,
            ..ReaderFs::new(sim)
        }
    }

    /// Fails the test if a reader is about to change the directory.
    fn refuse(&self, what: &str, path: &Path) {
        assert!(!self.reader, "a reader {what} {}", path.display());
    }
}

impl Fs for ReaderFs {
    type Disk = SimDisk;
    type Lock = SimLock;

    async fn create(&self, path: &Path) -> roble::Result<SimDisk> {
        self.refuse("created", path);
        self.sim.create(path).await
    }

    async fn open(&self, path: &Path, access: Access) -> roble::Result<SimDisk> {
        if matches!(access, Access::ReadWrite) {
            self.refuse("opened to write", path);
        }
        if self.gone.borrow().iter().any(|p| p == path) {
            let _ = self.sim.remove(path).await;
        }
        let grow = self.grow.borrow().clone();
        if let Some((when, target)) = grow
            && when == path
        {
            let disk = self.sim.open(&target, Access::ReadWrite).await?;
            let at = disk.len().await?;
            let (result, _) = disk.write(WriteBuf::new(vec![0; 7]), at, ()).await;
            result?;
        }
        self.sim.open(path, access).await
    }

    async fn remove(&self, path: &Path) -> roble::Result<()> {
        self.refuse("removed", path);
        self.sim.remove(path).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> roble::Result<()> {
        self.refuse("renamed", from);
        self.sim.rename(from, to).await
    }

    async fn link(&self, from: &Path, to: &Path) -> roble::Result<()> {
        self.refuse("linked", from);
        self.sim.link(from, to).await
    }

    async fn list(&self, dir: &Path) -> roble::Result<Vec<DirEntry>> {
        self.sim.list(dir).await
    }

    async fn mkdir_all(&self, dir: &Path) -> roble::Result<()> {
        self.refuse("made", dir);
        self.sim.mkdir_all(dir).await
    }

    async fn sync_dir(&self, dir: &Path) -> roble::Result<()> {
        self.refuse("synced", dir);
        self.sim.sync_dir(dir).await
    }

    async fn disk_usage(&self, dir: &Path) -> roble::Result<DiskUsage> {
        self.sim.disk_usage(dir).await
    }

    async fn lock(&self, path: &Path) -> roble::Result<SimLock> {
        self.refuse("locked", path);
        self.sim.lock(path).await
    }
}

struct ReaderEnv(ReaderFs);

impl Env for ReaderEnv {
    type Fs = ReaderFs;

    fn fs(&self) -> &ReaderFs {
        &self.0
    }

    fn spawn(&self, task: impl Future<Output = ()> + 'static) {
        compio::runtime::spawn(task).detach();
    }
}

fn dir() -> PathBuf {
    PathBuf::from("/db")
}

async fn writer(env: &SimEnv) -> roble::Opened<ReaderFs> {
    let options = Options::default()
        .create_if_missing(true)
        .disable_automatic_compactions(true);
    roble::open_with(&ReaderEnv(ReaderFs::writing(env.sim())), &dir(), options)
        .await
        .expect("open the writer")
}

/// What the writer's own open makes of the directory, without creating one.
async fn reopen(env: &SimEnv) -> roble::Result<roble::Opened<ReaderFs>> {
    roble::open(&ReaderEnv(ReaderFs::writing(env.sim())), &dir()).await
}

async fn reader(env: &ReaderEnv) -> roble::Result<roble::Opened<ReaderFs>> {
    roble::open_read_only(env, &dir(), Options::default()).await
}

async fn put(db: &roble::Opened<ReaderFs>, key: &[u8], value: &[u8]) {
    let mut batch = Batch::new();
    batch.put(key, value).expect("put");
    db.write(&mut batch, true).await.expect("write");
}

async fn get<F: Fs>(db: &roble::Opened<F>, key: &[u8]) -> roble::Result<Option<Vec<u8>>> {
    Ok(db
        .get(key, Comparison::Equal)
        .await?
        .map(|found| found.value.to_vec()))
}

/// The names of `dir`'s files of one type.
async fn named(sim: &SimFs, file_type: FileType) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = sim
        .list(&dir())
        .await
        .expect("list")
        .into_iter()
        .filter(|e| {
            files::parse_filename(&e.name.to_string_lossy()).map(|p| p.file_type) == Some(file_type)
        })
        .map(|e| dir().join(e.name))
        .collect();
    out.sort();
    out
}

#[compio::test]
async fn a_reader_opens_beside_the_writer_and_reads_what_it_wrote() {
    let env = SimEnv::new();
    let db = writer(&env).await;
    put(&db, b"a", b"1").await;
    put(&db, b"b", b"2").await;
    db.flush().await.expect("flush");
    // In the log and no table: the reader has to replay it.
    put(&db, b"c", b"3").await;

    let env = ReaderEnv(ReaderFs::new(env.sim()));
    let ro = reader(&env).await.expect("open beside the writer");
    assert_eq!(get(&ro, b"a").await.expect("get"), Some(b"1".to_vec()));
    assert_eq!(get(&ro, b"c").await.expect("get"), Some(b"3".to_vec()));

    let mut scan = ro.scan().await.expect("scan");
    scan.first().await.expect("first");
    let mut seen = Vec::new();
    while scan.is_valid() {
        seen.push((scan.key().to_vec(), scan.value().to_vec()));
        scan.next().await.expect("next");
    }
    assert_eq!(
        seen,
        [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")].map(|(k, v)| (k.to_vec(), v.to_vec()))
    );

    // What `stats` asks for, which is read from the version and the directory.
    ro.disk_usage().await.expect("disk usage");
    assert!(ro.current_log().is_some());

    // And the writer is undisturbed by it.
    put(&db, b"d", b"4").await;
    db.flush().await.expect("flush after the reader");
}

#[compio::test]
async fn a_reader_refuses_everything_that_would_change_the_directory() {
    let env = SimEnv::new();
    let db = writer(&env).await;
    put(&db, b"a", b"1").await;
    drop(db);

    let env = ReaderEnv(ReaderFs::new(env.sim()));
    let ro = reader(&env).await.expect("open");

    let mut batch = Batch::new();
    batch.put(b"b", b"2").expect("put");
    assert!(matches!(
        ro.write(&mut batch, false).await,
        Err(Error::ReadOnly)
    ));
    assert!(matches!(ro.flush().await, Err(Error::ReadOnly)));
    assert!(matches!(ro.compact_once().await, Err(Error::ReadOnly)));
    assert!(matches!(ro.compact(0, &[]).await, Err(Error::ReadOnly)));
    assert!(matches!(ro.rotate_manifest().await, Err(Error::ReadOnly)));
    assert!(matches!(
        ro.remove_obsolete_files().await,
        Err(Error::ReadOnly)
    ));
    assert!(matches!(
        ro.checkpoint(Path::new("/elsewhere")).await,
        Err(Error::ReadOnly)
    ));
    assert!(matches!(ro.destroy().await, Err(Error::ReadOnly)));
}

/// What a reader that opened before a compaction finds once the compaction
/// has deleted the tables it read the version of.
#[compio::test]
async fn a_table_the_writer_retires_is_stale_to_a_reader() {
    let sim_env = SimEnv::new();
    let db = writer(&sim_env).await;
    put(&db, b"a", b"1").await;
    db.flush().await.expect("flush");
    put(&db, b"b", b"2").await;
    db.flush().await.expect("flush");

    let env = ReaderEnv(ReaderFs::new(sim_env.sim()));
    let ro = reader(&env).await.expect("open");

    let l0: Vec<u64> = db
        .versions()
        .version
        .levels()
        .find(|(level, _)| *level == 0)
        .map(|(_, tables)| tables.iter().map(|t| t.file_num).collect())
        .expect("L0");
    assert_eq!(l0.len(), 2);
    db.compact(0, &l0).await.expect("compact");
    assert!(
        named(sim_env.sim(), FileType::Table).await.len() == 1,
        "the compaction's inputs are swept"
    );

    assert!(matches!(get(&ro, b"a").await, Err(Error::Stale(_))));

    // Opening again reads the database as it is now.
    let ro = reader(&env).await.expect("reopen");
    assert_eq!(get(&ro, b"a").await.expect("get"), Some(b"1".to_vec()));
    assert_eq!(get(&ro, b"b").await.expect("get"), Some(b"2".to_vec()));
}

/// A log the writer deletes between the reader's listing and its replay.
#[compio::test]
async fn a_log_the_writer_retires_while_a_reader_opens_is_stale() {
    let sim_env = SimEnv::new();
    let db = writer(&sim_env).await;
    put(&db, b"a", b"1").await;
    drop(db);

    let fs = ReaderFs::new(sim_env.sim());
    *fs.gone.borrow_mut() = named(sim_env.sim(), FileType::Log).await;
    let env = ReaderEnv(fs);
    assert!(matches!(reader(&env).await, Err(Error::Stale(_))));
}

/// The MANIFEST the marker named, replaced and deleted by a rotation between
/// the reader's listing and its read of it.
#[compio::test]
async fn a_manifest_the_writer_rotates_away_while_a_reader_opens_is_stale() {
    let sim_env = SimEnv::new();
    let db = writer(&sim_env).await;
    put(&db, b"a", b"1").await;
    drop(db);

    let fs = ReaderFs::new(sim_env.sim());
    *fs.gone.borrow_mut() = named(sim_env.sim(), FileType::Manifest).await;
    let env = ReaderEnv(fs);
    assert!(matches!(reader(&env).await, Err(Error::Stale(_))));
}

/// Two unflushed logs, as a flush leaves while writes are still in flight on
/// the old one: it creates the next log before it swaps the memtable.
///
/// Returns the env and the two logs, older first.
async fn two_logs() -> (SimEnv, PathBuf, PathBuf) {
    let sim_env = SimEnv::new();
    let db = writer(&sim_env).await;
    put(&db, b"a", b"1").await;
    drop(db);
    let older = named(sim_env.sim(), FileType::Log).await.remove(0);
    let num = files::parse_filename(&older.file_name().unwrap().to_string_lossy())
        .unwrap()
        .file_num;
    let newer = dir().join(files::log_filename(num + 10));
    sim_env.sim().create(&newer).await.expect("create");
    (sim_env, older, newer)
}

#[compio::test]
async fn an_earlier_log_that_grows_while_a_reader_replays_is_stale() {
    let (sim_env, older, newer) = two_logs().await;

    // Without the race, the reader reads both and has the write.
    let env = ReaderEnv(ReaderFs::new(sim_env.sim()));
    let ro = reader(&env).await.expect("open");
    assert_eq!(get(&ro, b"a").await.expect("get"), Some(b"1".to_vec()));

    // With it: the older log grows while the newer one is being read.
    let fs = ReaderFs::new(sim_env.sim());
    *fs.grow.borrow_mut() = Some((newer, older));
    let env = ReaderEnv(fs);
    assert!(matches!(reader(&env).await, Err(Error::Stale(_))));
}

/// A torn record in a log that is not the last is corruption to the writer
/// and stale to a reader, which may have caught the writer mid-append.
#[compio::test]
async fn a_torn_earlier_log_is_corrupt_to_the_writer_and_stale_to_a_reader() {
    let (sim_env, older, _) = two_logs().await;
    let disk = sim_env
        .sim()
        .open(&older, Access::ReadWrite)
        .await
        .expect("open");
    let at = disk.len().await.expect("len");
    // A chunk header that claims more bytes than follow it.
    let torn = vec![0xde, 0xad, 0xbe, 0xef, 0xff, 0x00, 1, b'x'];
    let (result, _) = disk.write(WriteBuf::new(torn), at, ()).await;
    result.expect("write");

    let env = ReaderEnv(ReaderFs::new(sim_env.sim()));
    assert!(matches!(reader(&env).await, Err(Error::Stale(_))));
    assert!(matches!(reopen(&sim_env).await, Err(Error::Corrupt(_))));
}

/// A reader opens a directory whose lock the writer holds, and leaves the
/// lock as it found it.
#[compio::test]
async fn a_reader_does_not_take_the_lock() {
    let sim_env = SimEnv::new();
    let db = writer(&sim_env).await;
    let env = ReaderEnv(ReaderFs::new(sim_env.sim()));
    let ro = reader(&env).await.expect("open beside the writer");
    drop(ro);
    drop(db);
    // And the lock it never took is not one it kept either.
    let _db = writer(&sim_env).await;
}

/// A torn record at the end of the MANIFEST is corruption to the writer and
/// stale to a reader, which may have caught the writer appending an edit.
#[compio::test]
async fn a_torn_manifest_is_corrupt_to_the_writer_and_stale_to_a_reader() {
    let sim_env = SimEnv::new();
    let db = writer(&sim_env).await;
    put(&db, b"a", b"1").await;
    drop(db);
    let manifest = named(sim_env.sim(), FileType::Manifest).await.remove(0);
    let disk = sim_env
        .sim()
        .open(&manifest, Access::ReadWrite)
        .await
        .expect("open");
    let at = disk.len().await.expect("len");
    // A chunk header that claims more bytes than follow it.
    let torn = vec![0xde, 0xad, 0xbe, 0xef, 0xff, 0x00, 1, b'x'];
    let (result, _) = disk.write(WriteBuf::new(torn), at, ()).await;
    result.expect("write");

    let env = ReaderEnv(ReaderFs::new(sim_env.sim()));
    assert!(matches!(reader(&env).await, Err(Error::Stale(_))));
    assert!(matches!(reopen(&sim_env).await, Err(Error::Corrupt(_))));
}

/// A reader does not check the version's tables against its listing, which a
/// flush between the two would fail; a table that has really gone is stale
/// when a read reaches it, and only then.
#[compio::test]
async fn a_missing_table_is_stale_when_a_read_reaches_it() {
    let sim_env = SimEnv::new();
    let db = writer(&sim_env).await;
    put(&db, b"a", b"1").await;
    db.flush().await.expect("flush");
    put(&db, b"b", b"2").await;
    drop(db);
    let table = named(sim_env.sim(), FileType::Table).await.remove(0);
    sim_env.sim().remove(&table).await.expect("remove");

    let env = ReaderEnv(ReaderFs::new(sim_env.sim()));
    let ro = reader(&env).await.expect("open without the table");
    // In the log, so no table is needed to read it.
    assert_eq!(get(&ro, b"b").await.expect("get"), Some(b"2".to_vec()));
    assert!(matches!(get(&ro, b"a").await, Err(Error::Stale(_))));
    // The writer refuses the same directory at once.
    assert!(matches!(reopen(&sim_env).await, Err(Error::Corrupt(_))));
}
