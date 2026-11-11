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

//! Tests for the `roble` command.
//!
//! These are weaker than the rest of the suite, and deliberately so: the
//! output format is roble's own, so there is no Go counterpart to diff against
//! (DESIGN.md §13). What *can* be checked is that the command adds nothing of
//! its own to the parts that are pinned — its table dump must be exactly what
//! `roble_format::dump_table` produced — and that it fails cleanly where the
//! format crate refuses a file.
//!
//! So the assertions here are about the seam, not about the prose.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn golden(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../format/tests/golden")
        .join(name)
}

fn roble(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_roble"))
        .args(args)
        .output()
        .expect("run roble")
}

fn stdout(out: &Output) -> String {
    String::from_utf8(out.stdout.clone()).expect("stdout is UTF-8")
}

fn stderr(out: &Output) -> String {
    String::from_utf8(out.stderr.clone()).expect("stderr is UTF-8")
}

/// The command must print the pinned dump unchanged.
///
/// This is the whole point of the thin-wrapper decision: the bytes that are
/// verified against Go Pebble reach the terminal without an unverified layer
/// reformatting, truncating or reordering them.
#[test]
fn sst_dump_prints_exactly_what_the_format_crate_produced() {
    for name in ["simple", "kinds", "multiblock", "rangedel", "valueblocks"] {
        let path = golden(&format!("{name}.sst"));
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("{name}: {e}"));
        let expected = roble_format::dump_table(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));

        let out = roble(&["sst", "dump", path.to_str().expect("path")]);
        assert!(out.status.success(), "{name}: {}", stderr(&out));
        assert_eq!(
            stdout(&out),
            expected,
            "{name}: the command changed the dump"
        );
    }
}

/// `--layout` may only add, never alter.
#[test]
fn the_layout_flag_prepends_and_leaves_the_dump_alone() {
    let path = golden("valueblocks.sst");
    let bytes = std::fs::read(&path).expect("read");
    let dump = roble_format::dump_table(&bytes).expect("dump");

    for args in [
        vec!["sst", "dump", "--layout", path.to_str().unwrap()],
        // The flag is accepted on either side of the path, and short.
        vec!["sst", "dump", path.to_str().unwrap(), "--layout"],
        vec!["sst", "dump", "-l", path.to_str().unwrap()],
    ] {
        let out = roble(&args);
        assert!(out.status.success(), "{}", stderr(&out));
        let text = stdout(&out);
        assert!(
            text.ends_with(&dump),
            "the layout flag altered the dump instead of preceding it"
        );
        // And it really said something structural.
        for expected in ["table-format pebblev4", "data-blocks 6", "value-blocks 6"] {
            assert!(text.contains(expected), "layout is missing {expected:?}");
        }
    }
}

/// A file the format crate refuses must fail cleanly, naming the reason.
///
/// The fixture holds range keys — legal Pebble that Oxia never writes — so §3
/// has roble decline the table rather than read past a structure it does not
/// understand.
#[test]
fn a_refused_table_fails_cleanly_rather_than_panicking() {
    let path = golden("refuse-rangekeys.sst");
    let out = roble(&["sst", "dump", path.to_str().expect("path")]);
    assert!(!out.status.success(), "a refused table must not exit zero");
    assert!(stdout(&out).is_empty(), "nothing should be printed");
    let err = stderr(&out);
    assert!(
        err.contains("range-key") || err.contains("range key"),
        "the error should name what was refused, got {err:?}"
    );
    assert!(!err.contains("panic"), "a refusal is not a panic: {err:?}");
}

#[test]
fn manifest_dump_reports_every_edit_the_decoder_finds() {
    use roble_format::manifest::VersionEdit;
    use roble_format::record::RecordReader;

    let path = golden("records-manifest");
    let bytes = std::fs::read(&path).expect("read");

    // Count edits with the pinned decoder, independently of the command.
    let mut reader = RecordReader::new(&bytes, 0);
    let mut edits = Vec::new();
    while let Some(rec) = reader.next() {
        edits.push(VersionEdit::decode(&rec.expect("record")).expect("edit"));
    }
    assert!(edits.len() > 1, "the fixture should hold several edits");

    let out = roble(&["manifest", "dump", path.to_str().expect("path")]);
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);

    assert!(
        text.contains(&format!("edits {}", edits.len())),
        "the command disagrees with the decoder about how many edits there are"
    );
    for i in 0..edits.len() {
        assert!(text.contains(&format!("edit {i}\n")), "edit {i} is missing");
    }
    // Spot-check that the fields are reported, not just counted.
    let added: usize = edits.iter().map(|e| e.new_tables.len()).sum();
    let deleted: usize = edits.iter().map(|e| e.deleted_tables.len()).sum();
    assert_eq!(
        text.matches("  added L").count(),
        added,
        "added tables are under-reported"
    );
    assert_eq!(
        text.matches("  deleted L").count(),
        deleted,
        "deleted tables are under-reported"
    );
    assert!(text.contains("comparer leveldb.BytewiseComparator"));
}

/// Argument handling follows encina's: clap with derive, help on stdout, and
/// errors on stderr with exit status 2.
#[test]
fn usage_and_bad_arguments_behave() {
    for args in [vec!["--help"], vec!["-h"], vec!["help"]] {
        let out = roble(&args);
        assert!(out.status.success(), "{args:?} should succeed");
        let text = stdout(&out);
        assert!(text.contains("Usage: roble"), "{args:?}: {text}");
        for command in ["sst", "manifest", "completions"] {
            assert!(text.contains(command), "{args:?} omits {command}");
        }
        // The after_help prose, which says what a refusal means.
        assert!(
            text.contains("Exit status"),
            "{args:?} omits the exit status"
        );
    }

    // clap reports a missing or wrong argument itself, on stderr, with status 2.
    for args in [
        vec![],
        vec!["nonsense"],
        vec!["sst"],
        vec!["sst", "dump"],
        vec!["sst", "dump", "a", "b"],
        vec!["manifest", "dump"],
        vec!["sst", "dump", "--nonsense", "x"],
    ] {
        let out = roble(&args);
        assert_eq!(
            out.status.code(),
            Some(2),
            "{args:?} should exit 2, said {:?}",
            out.status.code()
        );
        assert!(
            !stderr(&out).is_empty(),
            "{args:?} should explain itself on stderr"
        );
    }
}

/// A refusal exits 2, as every other error does.
#[test]
fn errors_exit_with_status_two() {
    let out = roble(&[
        "sst",
        "dump",
        golden("refuse-rangekeys.sst").to_str().unwrap(),
    ]);
    assert_eq!(out.status.code(), Some(2));
    let out = roble(&["sst", "dump", "/nonexistent/table.sst"]);
    assert_eq!(out.status.code(), Some(2));
}

/// Completions are generated for the shells encina supports.
#[test]
fn completions_are_generated() {
    for shell in ["bash", "zsh", "fish"] {
        let out = roble(&["completions", shell]);
        assert!(out.status.success(), "{shell}: {}", stderr(&out));
        let text = stdout(&out);
        assert!(!text.is_empty(), "{shell}: nothing generated");
        assert!(
            text.contains("roble"),
            "{shell}: does not mention the command"
        );
    }
    // An unknown shell is clap's error, not a panic.
    let out = roble(&["completions", "tcsh"]);
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn a_missing_file_is_reported_with_its_name() {
    let out = roble(&["sst", "dump", "/nonexistent/table.sst"]);
    assert!(!out.status.success());
    let err = stderr(&out);
    assert!(err.contains("/nonexistent/table.sst"), "got {err:?}");
}

/// A MANIFEST path handed to `sst dump`, and the reverse, must fail rather than
/// produce confident nonsense.
#[test]
fn the_wrong_kind_of_file_is_refused() {
    let out = roble(&["sst", "dump", golden("records-manifest").to_str().unwrap()]);
    assert!(!out.status.success(), "a MANIFEST is not a table");
    assert!(!stderr(&out).is_empty());

    let out = roble(&["manifest", "dump", golden("simple.sst").to_str().unwrap()]);
    assert!(!out.status.success(), "a table is not a MANIFEST");
    assert!(!stderr(&out).is_empty());
}

/// The layout names what each table actually holds.
///
/// `--layout` is the one place the command says something the dump does not,
/// so what it says has to be true of the file rather than of the last fixture
/// anyone looked at. Each of these is a structure only some tables have, and
/// `valueblocks` alone — the fixture the test above uses — has none of the
/// first three.
#[test]
fn the_layout_names_what_each_table_holds() {
    for (fixture, expected) in [
        // A bloom filter, which only this fixture is written with.
        ("bloom", "filter"),
        // Range deletions, which live in a metaindex block of their own.
        ("rangedel", "range-del"),
        // An index too big for one block, so the index is two levels.
        ("multiblock", "two-level"),
        // And the one the test above already covers, kept so that a change
        // making them all say the same thing would show up here.
        ("valueblocks", "value-index"),
    ] {
        let path = golden(&format!("{fixture}.sst"));
        let out = roble(&["sst", "dump", "--layout", path.to_str().unwrap()]);
        assert!(out.status.success(), "{}: {}", fixture, stderr(&out));
        assert!(
            stdout(&out).contains(expected),
            "{fixture}'s layout should mention {expected:?}"
        );
    }

    // The negative half: a table without a filter must not claim one, or
    // "filter" would mean nothing wherever it appears.
    let path = golden("simple.sst");
    let out = roble(&["sst", "dump", "--layout", path.to_str().unwrap()]);
    let text = stdout(&out);
    let metaindex = text
        .lines()
        .find(|l| l.starts_with("metaindex "))
        .expect("a metaindex line");
    assert!(
        !metaindex.contains("filter"),
        "simple.sst has no filter, got {metaindex:?}"
    );
}

/// A reader that goes away stops the dump quietly.
///
/// `roble sst dump big.sst | head` is how a dump is read more often than it is
/// read whole, and the pipe closes while the command is still writing. Rust
/// leaves SIGPIPE ignored, so the write comes back as an error rather than
/// killing the process, and it is the command's job to tell that case apart
/// from a real failure: a broken pipe is success and anything else is not.
///
/// `snappy` is used because its dump is a quarter of a megabyte, well past
/// any pipe buffer — a fixture small enough to fit would be written in one go
/// and never meet the closed reader.
#[test]
fn a_reader_that_goes_away_is_not_an_error() {
    use std::process::Stdio;

    let path = golden("snappy.sst");
    let mut child = Command::new(env!("CARGO_BIN_EXE_roble"))
        .args(["sst", "dump", path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run roble");

    // Close the read end while the command is still writing.
    drop(child.stdout.take().expect("stdout is piped"));

    let out = child.wait_with_output().expect("wait for roble");
    assert!(
        out.status.success(),
        "a closed reader should be a quiet stop, got {} and {:?}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stderr.is_empty(),
        "nothing should be said about it, got {:?}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Writes a MANIFEST of framed records into a temporary file.
fn a_manifest(name: &str, records: &[Vec<u8>]) -> PathBuf {
    use roble_format::record::RecordWriter;

    let mut writer = RecordWriter::legacy();
    for record in records {
        writer.write_record(record).expect("frame the record");
    }
    let path = std::env::temp_dir().join(format!("roble-cli-{name}-{}", std::process::id()));
    std::fs::write(&path, writer.finish()).expect("write the manifest");
    path
}

/// A record that frames correctly but is not a version edit is reported with
/// its number.
///
/// The number is the point. A MANIFEST is a sequence, and "this file is bad"
/// is not something anyone can act on — "version edit 3" says which edit to
/// look at, and that the three before it decoded.
#[test]
fn a_record_that_is_not_a_version_edit_is_reported_with_its_number() {
    let good = roble_format::manifest::VersionEdit {
        comparer_name: Some("leveldb.BytewiseComparator".to_string()),
        next_file_num: Some(10),
        ..Default::default()
    }
    .encode();
    // Tag 99 is not a tag any Pebble writes.
    let path = a_manifest("bad-edit", &[good.clone(), good, vec![99, 1, 2, 3]]);

    let out = roble(&["manifest", "dump", path.to_str().unwrap()]);
    let _ = std::fs::remove_file(&path);

    assert_eq!(out.status.code(), Some(2), "{}", stdout(&out));
    let err = stderr(&out);
    assert!(
        err.contains("version edit 2"),
        "the refusal should say which edit, got {err:?}"
    );
}

/// The rare fields of an edit reach the dump.
///
/// A dump exists to show what is in a file, so the fields that appear once in
/// a database's life are exactly the ones worth pinning: they are the fields
/// nobody will notice the absence of. The bound here is deliberately not a
/// decodable internal key, because a dump that hid an unreadable bound would
/// hide the thing someone opened the dump to find.
#[test]
fn the_dump_shows_the_fields_an_edit_rarely_carries() {
    use roble_format::manifest::{NewTable, VersionEdit};

    let edit = VersionEdit {
        comparer_name: Some("leveldb.BytewiseComparator".to_string()),
        next_file_num: Some(10),
        last_seq_num: Some(20),
        obsolete_prev_log_num: Some(7),
        new_tables: vec![NewTable {
            level: 0,
            file_num: 4,
            size: 100,
            // Too short to hold the eight-byte trailer an internal key ends
            // with, so the dump has to say so rather than drop it.
            smallest: b"short".to_vec(),
            largest: b"short".to_vec(),
            smallest_seq_num: 1,
            largest_seq_num: 2,
            marked_for_compaction: true,
            creation_time: 0,
        }],
        ..Default::default()
    };
    let path = a_manifest("rare-fields", &[edit.encode()]);

    let out = roble(&["manifest", "dump", path.to_str().unwrap()]);
    let _ = std::fs::remove_file(&path);

    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    for expected in ["prev-log 7", "marked-for-compaction", "<undecodable:"] {
        assert!(text.contains(expected), "the dump should show {expected:?}");
    }
}
