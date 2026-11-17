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

//! A whole sstable: open it, dump it, seek it, probe its filter.
//!
//! Seeded from the golden tables, so mutations start from tables Pebble
//! wrote. Any answer is allowed; a panic, abort, hang or an allocation the
//! input cannot justify is not.

#![no_main]

use libfuzzer_sys::fuzz_target;
use roble_format::filter::Filter;
use roble_format::key::search_key;
use roble_format::sst::Table;
use roble_format::{Footer, dump_table};

fuzz_target!(|data: &[u8]| {
    let _ = Footer::decode(data, data.len() as u64);
    let _ = dump_table(data);

    let Ok(table) = Table::open(data) else {
        return;
    };
    let _ = table.index_entries();
    let probe = &data[..data.len().min(8)];
    for user_key in [&b""[..], b"a", b"\xff\xff", probe] {
        let target = search_key(user_key);
        let _ = table.seek_ge(&target, |_, _| Ok(()));
        let _ = table.seek_lt(&target, |_, _| Ok(()));
    }
    if let Ok(Some(block)) = table.filter_block()
        && let Ok(Some(filter)) = Filter::from_block(block.as_slice())
    {
        let _ = filter.may_contain(probe);
    }
});
