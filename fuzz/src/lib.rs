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

//! What the engine's fuzz targets share: the golden databases, and putting
//! one on a simulated filesystem.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use roble::sim::SimFs;
use roble::{Disk, Fs, WriteBuf};

/// A database's files, by name.
pub type Files = Vec<(String, Vec<u8>)>;

/// The databases in `tests/golden`, read once and in name order.
pub fn fixtures() -> &'static [Files] {
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

/// Writes `files` into `dir`, every byte and every name durable.
pub async fn install(fs: &SimFs, dir: &Path, files: &Files) {
    fs.mkdir_all(dir).await.expect("mkdir");
    for (name, bytes) in files {
        let disk = fs.create(&dir.join(name)).await.expect("create");
        let (r, _) = disk.write(WriteBuf::new(bytes.clone()), 0, ()).await;
        r.expect("write");
        disk.sync(()).await.expect("sync");
        disk.close().await.expect("close");
    }
    fs.sync_dir(dir).await.expect("sync dir");
}
