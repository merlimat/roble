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

//! One suite, run against both filesystems.
//!
//! The point is not coverage of either implementation but agreement between
//! them: a trait with one implementation is a description of that
//! implementation, and the simulated filesystem only earns its place in §12's
//! failure testing if it behaves like the real one where it claims to.
//!
//! So every case below is written once and run twice.

// The cases are generic over the filesystem, so a request's class is written
// `Default::default()` rather than `()`. Both implementations happen to use
// `()`, which clippy then reads as passing a unit value to a function.
#![allow(clippy::unit_arg)]

use std::path::{Path, PathBuf};

use roble::sim::SimFs;
use roble::{Access, Disk, Fs, ReadBuf, WriteBuf};

/// Runs `case` against both filesystems, with a directory each.
///
/// `FileFs` gets a real temporary directory; `SimFs` gets a path that exists
/// only in its own map, which is the point — a scenario must not depend on
/// anything outside the trait.
macro_rules! both {
    ($name:ident, |$fs:ident, $dir:ident| $body:block) => {
        mod $name {
            use super::*;

            #[compio::test]
            async fn file_fs() {
                let tmp = tempfile::tempdir().expect("temp dir");
                let owned = roble::FileFs;
                let $fs = &owned;
                let $dir: PathBuf = tmp.path().to_path_buf();
                $body
            }

            #[compio::test]
            async fn sim_fs() {
                let owned = SimFs::new();
                let $fs = &owned;
                let $dir: PathBuf = PathBuf::from("/db");
                $fs.mkdir_all(&$dir).await.expect("mkdir");
                $body
            }
        }
    };
}

async fn write_file<F: Fs>(fs: &F, path: &Path, bytes: &[u8]) {
    let disk = fs.create(path).await.expect("create");
    let (result, _) = disk
        .write(WriteBuf::new(bytes.to_vec()), 0, Default::default())
        .await;
    result.expect("write");
    disk.sync(Default::default()).await.expect("sync");
    disk.close().await.expect("close");
}

async fn read_file<F: Fs>(fs: &F, path: &Path) -> Vec<u8> {
    let disk = fs.open(path, Access::ReadOnly).await.expect("open");
    let len = disk.len().await.expect("len") as usize;
    let (result, buf) = disk
        .read(ReadBuf::with_len(len), 0, Default::default())
        .await;
    result.expect("read");
    buf.into_vec()
}

both!(a_file_reads_back_what_was_written, |fs, dir| {
    let path = dir.join("000001.sst");
    write_file(fs, &path, b"the quick brown fox").await;
    assert_eq!(read_file(fs, &path).await, b"the quick brown fox");

    let disk = fs.open(&path, Access::ReadOnly).await.expect("open");
    assert_eq!(disk.len().await.expect("len"), 19);
});

both!(
    a_read_past_the_end_fails_rather_than_coming_back_short,
    |fs, dir| {
        // A partly filled buffer cannot be told from a corrupt block, and the
        // difference decides whether to refuse a database.
        let path = dir.join("short.sst");
        write_file(fs, &path, b"0123456789").await;
        let disk = fs.open(&path, Access::ReadOnly).await.expect("open");

        let (result, _) = disk
            .read(ReadBuf::with_len(11), 0, Default::default())
            .await;
        assert!(result.is_err(), "reading 11 of 10 bytes must fail");

        let (result, _) = disk.read(ReadBuf::with_len(4), 8, Default::default()).await;
        assert!(result.is_err(), "a read crossing the end must fail");

        // Exactly to the end is fine.
        let (result, buf) = disk.read(ReadBuf::with_len(2), 8, Default::default()).await;
        result.expect("a read ending at the end is fine");
        assert_eq!(buf.as_slice(), b"89");
    }
);

both!(a_write_at_an_offset_grows_the_file, |fs, dir| {
    let path = dir.join("grow.sst");
    let disk = fs.create(&path).await.expect("create");
    let (result, _) = disk
        .write(WriteBuf::new(b"tail".to_vec()), 8, Default::default())
        .await;
    result.expect("write past the end");
    assert_eq!(disk.len().await.expect("len"), 12);
    disk.sync(Default::default()).await.expect("sync");
    assert_eq!(&read_file(fs, &path).await[8..], b"tail");
});

both!(rename_replaces_and_is_visible_at_once, |fs, dir| {
    // Marker rotation depends on this: a reader must see one name or the
    // other, never neither.
    let from = dir.join("marker.new");
    let to = dir.join("marker");
    write_file(fs, &to, b"old").await;
    write_file(fs, &from, b"new").await;
    fs.rename(&from, &to).await.expect("rename");
    assert_eq!(read_file(fs, &to).await, b"new");
    assert!(fs.open(&from, Access::ReadOnly).await.is_err());
});

both!(link_shares_the_bytes_without_copying_them, |fs, dir| {
    // checkpoint() hard-links the live tables, so a snapshot costs no bytes.
    let from = dir.join("000002.sst");
    let to = dir.join("checkpoint-000002.sst");
    write_file(fs, &from, b"table bytes").await;
    fs.link(&from, &to).await.expect("link");
    assert_eq!(read_file(fs, &to).await, b"table bytes");
});

both!(listing_names_the_files_in_order, |fs, dir| {
    for name in ["000003.sst", "000001.sst", "MANIFEST-000002"] {
        write_file(fs, &dir.join(name), b"x").await;
    }
    let listed: Vec<String> = fs
        .list(&dir)
        .await
        .expect("list")
        .into_iter()
        .map(|e| e.name.to_string_lossy().into_owned())
        .collect();
    assert_eq!(listed, ["000001.sst", "000003.sst", "MANIFEST-000002"]);
});

both!(disk_usage_counts_the_bytes, |fs, dir| {
    write_file(fs, &dir.join("a.sst"), &[0u8; 100]).await;
    write_file(fs, &dir.join("b.sst"), &[0u8; 200]).await;
    let usage = fs.disk_usage(&dir).await.expect("disk usage");
    assert_eq!(usage.used_bytes, 300);
});

both!(removing_a_missing_file_is_an_error, |fs, dir| {
    assert!(fs.remove(&dir.join("never-existed")).await.is_err());
});

both!(the_lock_keeps_out_a_second_owner, |fs, dir| {
    // Two owners of one database corrupt it, so this is a refusal.
    let path = dir.join("LOCK");
    let held = fs.lock(&path).await.expect("first lock");
    let second = fs.lock(&path).await;
    assert!(
        matches!(second, Err(roble::Error::Locked)),
        "a second owner must be refused with Locked"
    );
    drop(held);
    // Released, so the next owner may have it.
    let again = fs.lock(&path).await;
    assert!(again.is_ok(), "the lock should be free once dropped");
});

/// Only the simulated filesystem can crash, so this one is not in `both!`.
#[compio::test]
async fn a_crash_loses_what_was_never_synced() {
    let fs = SimFs::new();
    let dir = PathBuf::from("/db");
    fs.mkdir_all(&dir).await.expect("mkdir");

    // Synced contents and a synced name: survives.
    let kept = dir.join("kept.sst");
    write_file(&fs, &kept, b"durable").await;
    fs.sync_dir(&dir).await.expect("sync dir");

    // Written but never synced: the bytes go.
    let partial = dir.join("partial.sst");
    let disk = fs.create(&partial).await.expect("create");
    let (result, _) = disk.write(WriteBuf::new(b"unsynced".to_vec()), 0, ()).await;
    result.expect("write");
    fs.sync_dir(&dir).await.expect("sync dir");

    // Contents synced but the name never was: the file goes entirely. This is
    // the case a store gets wrong by syncing a table and forgetting its
    // directory.
    let nameless = dir.join("nameless.sst");
    let disk2 = fs.create(&nameless).await.expect("create");
    let (result, _) = disk2.write(WriteBuf::new(b"orphan".to_vec()), 0, ()).await;
    result.expect("write");
    disk2.sync(()).await.expect("sync");

    fs.crash();

    assert_eq!(fs.durable(&kept).as_deref(), Some(&b"durable"[..]));
    assert_eq!(
        fs.durable(&partial).as_deref(),
        Some(&b""[..]),
        "unsynced bytes should be gone, the file still named"
    );
    assert!(
        fs.durable(&nameless).is_none(),
        "a file whose name was never synced should be gone"
    );
}

/// The failure injection §12 relies on: every operation of a scenario, failed
/// one at a time, and never a panic.
#[compio::test]
async fn any_single_operation_can_be_made_to_fail() {
    async fn scenario(fs: &SimFs) -> roble::Result<()> {
        let dir = PathBuf::from("/db");
        fs.mkdir_all(&dir).await?;
        let path = dir.join("000001.sst");
        let disk = fs.create(&path).await?;
        let (result, _) = disk.write(WriteBuf::new(b"bytes".to_vec()), 0, ()).await;
        result?;
        disk.sync(()).await?;
        fs.sync_dir(&dir).await?;
        let reopened = fs.open(&path, Access::ReadOnly).await?;
        let (result, _) = reopened.read(ReadBuf::with_len(5), 0, ()).await;
        result?;
        fs.rename(&path, &dir.join("000002.sst")).await?;
        fs.sync_dir(&dir).await?;
        Ok(())
    }

    // Once to count the operations.
    let counting = SimFs::new();
    scenario(&counting).await.expect("the scenario should pass");
    let total = counting.ops();
    assert!(
        total >= 8,
        "expected a scenario of several operations, got {total}"
    );

    // Then once per operation, with that one failing.
    for n in 1..=total {
        let fs = SimFs::new();
        fs.fail_at(n);
        match scenario(&fs).await {
            Err(_) => {}
            Ok(()) => panic!("operation {n} of {total} was supposed to fail"),
        }
    }
}
