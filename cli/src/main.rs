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

//! `roble`: inspects Pebble's files (DESIGN.md §13).
//!
//! Deliberately a thin wrapper. Everything structural comes from
//! `roble-format`, whose output is pinned byte for byte against Go Pebble; the
//! only thing this crate decides is presentation, and presentation is the one
//! part of M1 with no Go counterpart to check against. Keeping the split sharp
//! is what stops an unverified layer from growing logic of its own: if a
//! question can be answered by the format crate, this file must not answer it
//! itself.
//!
//! The argument handling is hand-rolled rather than taken from a crate. Two
//! subcommands and one flag do not earn a dependency tree, and the format
//! crate's own dependencies are deliberately few.

use std::process::ExitCode;

use roble_format::block::Compression;
use roble_format::manifest::VersionEdit;
use roble_format::record::RecordReader;
use roble_format::sst::Table;

const USAGE: &str = "\
roble — inspects Pebble's files

usage:
    roble sst dump [--layout] <file.sst>
    roble manifest dump <MANIFEST-file>

    sst dump          every point entry and range tombstone in a table
      --layout        also the table's structure: blocks, compression, properties
    manifest dump     every version edit in a MANIFEST

Both read Pebble's own formats, pinned at the version DESIGN.md §3 names, and
refuse — rather than partly read — anything above that pin.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(output) => {
            print!("{output}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("roble: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<String, String> {
    let as_str: Vec<&str> = args.iter().map(String::as_str).collect();
    match as_str.as_slice() {
        [] | ["-h"] | ["--help"] | ["help"] => Ok(USAGE.to_string()),
        ["sst", "dump", rest @ ..] => sst_dump(rest),
        ["manifest", "dump", rest @ ..] => manifest_dump(rest),
        [other, ..] => Err(format!("unknown command {other:?}\n\n{USAGE}")),
    }
}

fn sst_dump(args: &[&str]) -> Result<String, String> {
    let (layout, path) = match args {
        ["--layout", path] | [path, "--layout"] => (true, *path),
        [path] => (false, *path),
        _ => {
            return Err(format!(
                "usage: roble sst dump [--layout] <file.sst>\n\n{USAGE}"
            ));
        }
    };
    let bytes = read(path)?;

    let mut out = String::new();
    if layout {
        out.push_str(&describe_layout(&bytes, path)?);
        out.push('\n');
    }
    // The dump itself is the format crate's, unchanged: it is the part that is
    // pinned against Go Pebble, and reformatting it here would put an
    // unverified layer between the file and the output.
    out.push_str(&roble_format::dump_table(&bytes).map_err(|e| format!("{path}: {e}"))?);
    Ok(out)
}

/// The structural summary `--layout` adds.
///
/// Every number here is read through `roble-format`; this function chooses the
/// words and nothing else.
fn describe_layout(bytes: &[u8], path: &str) -> Result<String, String> {
    use std::fmt::Write as _;

    let table = Table::open(bytes).map_err(|e| format!("{path}: {e}"))?;
    let footer = *table.footer();
    let handles = table
        .data_block_handles()
        .map_err(|e| format!("{path}: index: {e}"))?;

    let mut counts: Vec<(Compression, usize)> = Vec::new();
    for h in &handles {
        let c = table
            .block_compression(*h)
            .map_err(|e| format!("{path}: block at {}: {e}", h.offset))?;
        match counts.iter_mut().find(|(k, _)| *k == c) {
            Some((_, n)) => *n += 1,
            None => counts.push((c, 1)),
        }
    }

    let mut out = String::new();
    writeln!(out, "file {path}").unwrap();
    writeln!(out, "size {}", bytes.len()).unwrap();
    writeln!(out, "table-format {}", footer.format.name()).unwrap();
    writeln!(out, "checksum {}", footer.checksum.name()).unwrap();
    writeln!(
        out,
        "index {}",
        if table.has_two_level_index() {
            "two-level"
        } else {
            "single-level"
        }
    )
    .unwrap();
    write!(out, "data-blocks {}", handles.len()).unwrap();
    for (c, n) in &counts {
        write!(out, " {}={n}", c.name()).unwrap();
    }
    out.push('\n');

    let meta = table.metaindex();
    let mut present: Vec<&str> = Vec::new();
    if meta.properties.is_some() {
        present.push("properties");
    }
    if meta.filter.is_some() {
        present.push("filter");
    }
    if meta.range_del.is_some() {
        present.push("range-del");
    }
    if meta.value_index.is_some() {
        present.push("value-index");
    }
    writeln!(out, "metaindex {}", present.join(" ")).unwrap();

    let value_blocks = table
        .value_blocks()
        .map_err(|e| format!("{path}: value index: {e}"))?;
    if !value_blocks.is_empty() {
        writeln!(out, "value-blocks {}", value_blocks.len()).unwrap();
    }

    for (name, value) in table.properties().iter() {
        // Properties are name/value pairs whose types vary by name, so they
        // are shown as the format crate holds them rather than guessed at.
        writeln!(
            out,
            "property {} {}",
            String::from_utf8_lossy(name),
            printable(value)
        )
        .unwrap();
    }
    Ok(out)
}

/// Shows a property value as text when it is text, and as hex when it is not.
fn printable(value: &[u8]) -> String {
    match std::str::from_utf8(value) {
        Ok(s) if s.chars().all(|c| !c.is_control()) && !s.is_empty() => s.to_string(),
        _ => value.iter().map(|b| format!("{b:02x}")).collect(),
    }
}

fn manifest_dump(args: &[&str]) -> Result<String, String> {
    use std::fmt::Write as _;

    let [path] = args else {
        return Err(format!(
            "usage: roble manifest dump <MANIFEST-file>\n\n{USAGE}"
        ));
    };
    let bytes = read(path)?;

    let mut out = String::new();
    writeln!(out, "file {path}").unwrap();
    writeln!(out, "size {}", bytes.len()).unwrap();

    // A MANIFEST's records are legacy-framed and carry no log number.
    let mut reader = RecordReader::new(&bytes, 0);
    let mut edits = Vec::new();
    while let Some(record) = reader.next() {
        let record = record.map_err(|e| format!("{path}: record {}: {e}", edits.len()))?;
        let edit = VersionEdit::decode(&record)
            .map_err(|e| format!("{path}: version edit {}: {e}", edits.len()))?;
        edits.push(edit);
    }
    writeln!(out, "edits {}", edits.len()).unwrap();

    for (i, edit) in edits.iter().enumerate() {
        writeln!(out, "edit {i}").unwrap();
        if let Some(name) = &edit.comparer_name {
            writeln!(out, "  comparer {name}").unwrap();
        }
        if let Some(n) = edit.min_unflushed_log_num {
            writeln!(out, "  min-unflushed-log {n}").unwrap();
        }
        if let Some(n) = edit.obsolete_prev_log_num {
            writeln!(out, "  prev-log {n}").unwrap();
        }
        if let Some(n) = edit.next_file_num {
            writeln!(out, "  next-file-num {n}").unwrap();
        }
        if let Some(n) = edit.last_seq_num {
            writeln!(out, "  last-seq-num {n}").unwrap();
        }
        for (level, file_num) in &edit.deleted_tables {
            writeln!(out, "  deleted L{level} {file_num}").unwrap();
        }
        for t in &edit.new_tables {
            write!(
                out,
                "  added L{} {} size={} seq={}..{}",
                t.level, t.file_num, t.size, t.smallest_seq_num, t.largest_seq_num
            )
            .unwrap();
            if t.creation_time != 0 {
                write!(out, " created={}", t.creation_time).unwrap();
            }
            if t.marked_for_compaction {
                write!(out, " marked-for-compaction").unwrap();
            }
            out.push('\n');
            writeln!(out, "    smallest {}", internal_key(&t.smallest)).unwrap();
            writeln!(out, "    largest  {}", internal_key(&t.largest)).unwrap();
        }
    }
    Ok(out)
}

/// Shows an encoded internal key as the format crate reads it.
fn internal_key(encoded: &[u8]) -> String {
    match roble_format::InternalKey::decode(encoded) {
        Ok(k) => format!("{} #{} {}", printable(k.user_key), k.seq_num, k.kind.name()),
        // A bound roble cannot decode is shown rather than hidden: it is the
        // interesting case when something is wrong.
        Err(e) => format!("<undecodable: {e}> {}", printable(encoded)),
    }
}

fn read(path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("{path}: {e}"))
}
