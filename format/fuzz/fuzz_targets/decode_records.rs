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

//! A MANIFEST or a WAL as it comes off the disk, decoded the way open does.
//!
//! The first four bytes are the log number the reader expects; the rest is
//! the file. Every framed record is decoded both as a version edit and as a
//! batch, since a fuzzer cannot tell which file it is writing. An edit that
//! decodes must survive encode and decode unchanged.

#![no_main]

use libfuzzer_sys::fuzz_target;
use roble_format::batch::{self, Batch};
use roble_format::manifest::VersionEdit;
use roble_format::record::RecordReader;

fn edit(bytes: &[u8]) {
    if let Ok(e) = VersionEdit::decode(bytes) {
        let again = VersionEdit::decode(&e.encode()).expect("an encoded edit decodes");
        assert_eq!(again, e, "an edit changed through encode and decode");
    }
}

fn batch(bytes: &[u8]) {
    let _ = batch::seq_num_of(bytes);
    if let Ok(mut r) = Batch::read(bytes) {
        while let Some(Ok(_)) = r.next() {}
    }
}

fuzz_target!(|data: &[u8]| {
    edit(data);
    batch(data);

    let Some((log_num, file)) = data.split_first_chunk::<4>() else {
        return;
    };
    let mut reader = RecordReader::new(file, u32::from_le_bytes(*log_num));
    while let Some(Ok(record)) = reader.next() {
        assert!(reader.offset() <= file.len());
        edit(&record);
        batch(&record);
    }
});
