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

//! A canonical textual dump of a table, which both roble and the Go harness
//! produce so that the two can be diffed (DESIGN.md §12).
//!
//! The format is deliberately dull and line-oriented, so that a mismatch
//! points at one line rather than at a blob:
//!
//! ```text
//!   format pebblev4
//!   checksum crc32c
//!   datablocks 3
//!   entries 42
//!   tombstones 2
//!   entry 000000000000000a set 2f61 76616c7565
//!   tombstone 0000000000000005 2f61 2f62
//! ```
//!
//! `entries` counts point entries only. Pebble's own `rocksdb.num.entries`
//! includes range deletions, which is a different number and deliberately not
//! the one dumped here.
//!
//! Sequence numbers are 16 hex digits, keys and values hex, kinds the names in
//! [`crate::Kind::name`]. This is the output `roble sst dump` will print.

use std::fmt::Write as _;

use crate::block::Value;
use crate::error::{Error, Result};
use crate::sst::Table;

/// Dumps a table's structure and every point entry.
///
/// Cross-checks the index walk against `rocksdb.num.data.blocks`: a reader
/// that stopped early would otherwise produce a well-formed, shorter dump, and
/// a dump that disagrees with the table's own properties is not worth printing.
pub fn dump_table(bytes: &[u8]) -> Result<String> {
    let table = Table::open(bytes)?;
    let footer = *table.footer();
    let data_blocks = table.data_block_handles()?.len();

    if let Some(claimed) = table.properties().num_data_blocks()?
        && claimed as usize != data_blocks
    {
        // Untested, with a reason rather than a test (§12). Every writer
        // counts the blocks it wrote, so the two disagreeing needs a table
        // whose properties were edited after its checksum was computed, or
        // the block check fires first. It is here because a dump is read as
        // evidence, and a dump that quietly disagreed with the file's own
        // account of itself would be evidence of the wrong thing.
        return Error::corrupt(format!(
            "properties claim {claimed} data blocks, the index walk found {data_blocks}"
        ));
    }

    let tombstones = table.range_deletions()?;

    // Decoded once rather than per value: a table with value blocks has few
    // of them, but the index would otherwise be re-read for every entry.
    let value_blocks = table.value_blocks()?;

    let mut entries: Vec<String> = Vec::new();
    table.for_each_entry(|key, value| {
        let resolved;
        let bytes = match value {
            Value::InPlace(v) => v,
            Value::ValueHandle(h) => {
                resolved = table.resolve_value(&value_blocks, h)?;
                &resolved
            }
            // Untested, with a reason (§12), and the reason is that an
            // earlier refusal gets there first: a blob handle arrives with
            // Pebblev6, and the footer refuses anything above v4 before a
            // block is read. The backstop stays for the day the pin moves.
            other => {
                return Error::unsupported(format!(
                    "{} values arrive above roble's pin and cannot be dumped",
                    other.kind_name()
                ));
            }
        };
        let mut line = String::new();
        write!(
            line,
            "entry {:016x} {} {} {}",
            key.seq_num,
            key.kind.name(),
            hex(key.user_key),
            hex(bytes)
        )
        .expect("writing to a String cannot fail");
        entries.push(line);
        Ok(())
    })?;

    let mut out = String::new();
    writeln!(out, "format {}", footer.format.name()).expect("String");
    writeln!(out, "checksum {}", footer.checksum.name()).expect("String");
    writeln!(out, "datablocks {data_blocks}").expect("String");
    writeln!(out, "entries {}", entries.len()).expect("String");
    writeln!(out, "tombstones {}", tombstones.len()).expect("String");
    for e in entries {
        writeln!(out, "{e}").expect("String");
    }
    for t in &tombstones {
        writeln!(
            out,
            "tombstone {:016x} {} {}",
            t.seq_num,
            hex(&t.start),
            hex(&t.end)
        )
        .expect("String");
    }
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").expect("writing to a String cannot fail");
    }
    s
}
