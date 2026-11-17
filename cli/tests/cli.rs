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
        for command in [
            "sst",
            "manifest",
            "completions",
            "put",
            "delete-range",
            "scan",
            "stats",
            "tables",
        ] {
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
        vec!["put", r"\x0g", "v"],
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

/// A copy of a database fixture in a directory of its own: opening one takes
/// its lock and may sweep it, which the checked-in fixture must not see.
fn a_copy_of(fixture: &str) -> tempfile::TempDir {
    let from = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/golden")
        .join(fixture);
    let dir = tempfile::tempdir().expect("tempdir");
    for entry in std::fs::read_dir(&from).expect("fixture") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), dir.path().join(entry.file_name())).expect("copy");
    }
    dir
}

fn on(dir: &Path, args: &[&str]) -> Output {
    let mut all = vec!["-d", dir.to_str().expect("path")];
    all.extend_from_slice(args);
    roble(&all)
}

/// What one command writes, the next one reads: the commands share nothing
/// but the directory, so every write here has to be in it before the
/// process that made it exits.
#[test]
fn puts_and_deletes_are_read_back_by_the_next_command() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // A directory that is not there yet, which `put` creates.
    let dir = tmp.path().join("db");

    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        let out = on(&dir, &["put", key, value]);
        assert!(out.status.success(), "put {key}: {}", stderr(&out));
    }
    let out = on(&dir, &["get", "b"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&out), "2\n");

    let out = on(&dir, &["scan", "-s", "b", "-e", "d"]);
    assert_eq!(stdout(&out), "b\t2\nc\t3\n");

    let out = on(&dir, &["delete", "a"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let out = on(&dir, &["delete-range", "-s", "b", "-e", "d"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let out = on(&dir, &["scan"]);
    assert_eq!(stdout(&out), "d\t4\n");
}

/// Keys and values are bytes: an escape in an argument is the byte it names,
/// and a byte that is not text is printed escaped the same way.
#[test]
fn bytes_that_are_not_text_go_in_and_come_out_escaped() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let out = on(dir, &["put", r"k\x00\xff", r"back\\slash\x09"]);
    assert!(out.status.success(), "{}", stderr(&out));

    let out = on(dir, &["scan"]);
    assert_eq!(stdout(&out), "k\\x00\\xff\tback\\\\slash\\x09\n");

    let out = on(dir, &["get", "-x", r"k\x00\xff"]);
    assert_eq!(
        stdout(&out),
        "00000000  62 61 63 6b 5c 73 6c 61  73 68 09                 |back\\slash.|\n"
    );
}

#[test]
fn get_exits_one_for_a_key_that_is_not_there() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = on(tmp.path(), &["put", "k", "v"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let out = on(tmp.path(), &["get", "nope"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stdout(&out).is_empty());
    assert!(stderr(&out).contains("not found"), "{}", stderr(&out));
}

/// Only `put` creates a database. The others refuse a directory that holds
/// none, and leave it as they found it, without so much as a `LOCK`.
#[test]
fn only_put_creates_a_database() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let missing = tmp.path().join("missing");
    for args in [
        vec!["get", "k"],
        vec!["scan"],
        vec!["delete", "k"],
        vec!["delete-range", "-s", "a", "-e", "b"],
        vec!["stats"],
        vec!["tables"],
    ] {
        let out = on(&missing, &args);
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        assert!(
            stderr(&out).contains("missing"),
            "{args:?}: {}",
            stderr(&out)
        );
        assert!(!missing.exists(), "{args:?} created the directory");

        let out = on(tmp.path(), &args);
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        assert!(
            stderr(&out).contains("no database"),
            "{args:?}: {}",
            stderr(&out)
        );
        assert_eq!(
            std::fs::read_dir(tmp.path()).expect("list").count(),
            0,
            "{args:?} left something behind"
        );
    }
}

/// Every file in `dir`, by name, with its bytes.
fn contents(dir: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .expect("list")
        .map(|e| {
            let e = e.expect("entry");
            let bytes = std::fs::read(e.path()).expect("read");
            (e.file_name().to_string_lossy().into_owned(), bytes)
        })
        .collect()
}

/// While another process holds the directory, the commands that read open
/// it read-only and work, see what is still only in the log, and leave every
/// file as it was. The commands that write fail at once rather than wait.
/// Only where roble locks.
#[cfg(unix)]
#[test]
fn a_database_open_elsewhere_is_read_and_not_written() {
    let db = a_copy_of("db-reads");
    // Written to the log and never flushed: a read has to replay it.
    let out = on(db.path(), &["put", "unflushed", "yes"]);
    assert!(out.status.success(), "{}", stderr(&out));

    let lock = std::fs::File::open(db.path().join("LOCK")).expect("open LOCK");
    lock.lock().expect("lock");
    let before = contents(db.path());

    let out = on(db.path(), &["get", "unflushed"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&out), "yes\n");
    let out = on(db.path(), &["scan"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        stdout(&out).contains("unflushed\tyes\n"),
        "{}",
        stdout(&out)
    );
    for args in [&["stats"][..], &["tables"]] {
        let out = on(db.path(), args);
        assert!(out.status.success(), "{args:?}: {}", stderr(&out));
    }
    assert_eq!(contents(db.path()), before, "a read changed the directory");

    for args in [
        &["put", "k", "v"][..],
        &["delete", "k"],
        &["delete-range", "-s", "a", "-e", "z"],
    ] {
        let out = on(db.path(), args);
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        assert!(stderr(&out).contains("already open"), "{}", stderr(&out));
    }
    assert_eq!(contents(db.path()), before);
}

/// `tables` lists what the MANIFEST names, and `stats` adds the same tables
/// up. The fixture is a database Pebble wrote, with tables in L0 and L6.
#[test]
fn stats_and_tables_describe_the_same_layout() {
    let db = a_copy_of("db-reads");

    let out = on(db.path(), &["tables"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let tables = stdout(&out);
    let mut rows = tables.lines().map(|l| l.split('\t').collect::<Vec<_>>());
    let header = rows.next().expect("a header");
    assert_eq!(header.len(), 13, "{header:?}");
    let lines: Vec<Vec<&str>> = rows.collect();
    assert!(!lines.is_empty());
    let column = |name: &str| header.iter().position(|h| *h == name).unwrap();
    for fields in &lines {
        assert_eq!(fields.len(), header.len(), "{fields:?}");
        assert_eq!(fields[column("format")], "pebblev4", "{fields:?}");
        assert_eq!(fields[column("compression")], "NoCompression", "{fields:?}");
        assert!(fields[1].ends_with(".sst"), "{fields:?}");
        assert!(
            db.path().join(fields[1]).exists(),
            "{} is not in the directory",
            fields[1]
        );
    }
    let in_level = |level: &str| lines.iter().filter(|f| f[0] == level).count();
    let sum = |name: &str| -> u64 {
        lines
            .iter()
            .map(|f| f[column(name)].parse::<u64>().unwrap())
            .sum()
    };
    let bytes = sum("size");

    let out = on(db.path(), &["stats"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let stats = stdout(&out);
    let field = |name: &str| {
        stats
            .lines()
            .find_map(|l| l.strip_prefix(name))
            .unwrap_or_else(|| panic!("stats omits {name}"))
            .trim()
            .to_string()
    };
    assert_eq!(field("tables "), lines.len().to_string());
    assert_eq!(field("table bytes "), bytes.to_string());
    assert_eq!(field("table entries "), sum("entries").to_string());
    assert_eq!(field("format version "), "16");
    for level in 0..7 {
        let row = stats
            .lines()
            .find(|l| l.split_whitespace().next() == Some(&level.to_string()))
            .unwrap_or_else(|| panic!("stats omits level {level}"));
        let count: usize = row.split_whitespace().nth(1).unwrap().parse().unwrap();
        assert_eq!(count, in_level(&format!("L{level}")), "level {level}");
    }
}

/// Without -d, a command opens the database in the current directory.
#[test]
fn the_directory_is_the_current_one_by_default() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_roble"))
            .args(args)
            .current_dir(tmp.path())
            .output()
            .expect("run roble")
    };
    let out = run(&["put", "k", "v"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&on(tmp.path(), &["get", "k"])), "v\n");
    assert_eq!(stdout(&run(&["get", "k"])), "v\n");
}

/// `scan -x` dumps each key and value across as many lines as it takes, and a
/// reader that goes away part-way, as `head` does, is a quiet stop.
#[test]
fn a_scan_piped_into_a_reader_that_goes_away_is_not_an_error() {
    use std::io::Read as _;
    use std::process::Stdio;

    let tmp = tempfile::tempdir().expect("tempdir");
    // Each value dumps to about four times its size, so these overflow any
    // pipe's buffer long before the scan is done.
    let value = "v".repeat(60_000);
    for key in ["a", "b", "c"] {
        let out = on(tmp.path(), &["put", key, &value]);
        assert!(out.status.success(), "{}", stderr(&out));
    }

    let mut child = Command::new(env!("CARGO_BIN_EXE_roble"))
        .args(["-d", tmp.path().to_str().unwrap(), "scan", "-x"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run roble");
    // Read the first lines, then go away while the command is still writing.
    let mut head = [0; 256];
    child
        .stdout
        .as_mut()
        .expect("stdout is piped")
        .read_exact(&mut head)
        .expect("read");
    drop(child.stdout.take());
    let out = child.wait_with_output().expect("wait for roble");

    let head = String::from_utf8_lossy(&head);
    assert!(
        head.starts_with(
            "key   00000000  61                                                |a|\n\
             value 00000000  76 76 76 76 76 76 76 76  76 76 76 76 76 76 76 76  |vvvvvvvvvvvvvvvv|\n\
             \x20     00000010  76"
        ),
        "{head:?}"
    );
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(out.stderr.is_empty(), "{}", stderr(&out));
}

/// A database with nothing in its tables: every level holds none of the
/// bytes, and there is no compaction to run.
#[test]
fn stats_on_an_empty_database() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = on(tmp.path(), &["put", "k", "v"]);
    assert!(out.status.success(), "{}", stderr(&out));

    let out = on(tmp.path(), &["stats"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    for expected in [
        "tables                                 0",
        "next compaction                     none",
    ] {
        assert!(text.contains(expected), "{expected:?} in {text}");
    }
    assert!(
        text.contains(
            "\n    0          0                0     0.0%            0            0        -\n"
        ),
        "{text}"
    );
    // The write is in the log and the memtable, and in no table yet.
    assert!(
        !text.contains("unflushed bytes                        0\n"),
        "{text}"
    );
    assert_eq!(stdout(&on(tmp.path(), &["tables"])).lines().count(), 1);
}

/// A database with no log to write to says so, rather than naming one.
#[test]
fn stats_without_a_log() {
    let db = a_copy_of("db-simple");
    std::fs::remove_file(db.path().join("000002.log")).expect("remove the log");
    let out = on(db.path(), &["stats"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(
        stdout(&out).contains("wal                                 none"),
        "{}",
        stdout(&out)
    );
}

/// The commands that read keep working while a writer in another thread
/// flushes and compacts under them, deleting the tables and logs they read:
/// a read that finds a file gone opens again, and a scan picks up after the
/// last key it printed, so no key is printed twice or out of order.
#[test]
fn reads_carry_on_beside_a_writer_that_compacts() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let db = a_copy_of("db-reads");
    let dir = db.path().to_path_buf();
    let keys_of = |out: &Output| -> Vec<String> {
        stdout(out)
            .lines()
            .map(|l| l.split('\t').next().unwrap().to_string())
            .collect()
    };
    // What the fixture holds, which the writer below adds to and never
    // touches: its keys all sort before "x". Compared as printed, since some
    // hold bytes that print escaped and so sort differently as text.
    let fixture = keys_of(&on(&dir, &["scan"]));
    assert!(fixture.iter().all(|k| k.as_str() < "x"), "{fixture:?}");
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (dir, stop) = (dir.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            let runtime = compio::runtime::Runtime::new().expect("runtime");
            runtime.block_on(async {
                let options = roble::Options::default().l0_compaction_threshold(2);
                let db = roble::open_with(&roble::DefaultEnv, &dir, options)
                    .await
                    .expect("open the writer");
                let mut round = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    let mut batch = roble_format::batch::Batch::new();
                    for i in 0..50 {
                        let key = format!("x{:03}", (round * 7 + i) % 200);
                        batch.put(key.as_bytes(), b"v").expect("put");
                    }
                    db.write(&mut batch, false).await.expect("write");
                    db.flush().await.expect("flush");
                    db.compact_once().await.expect("compact");
                    round += 1;
                    // A flush a few milliseconds apart: still many to every
                    // read, and slow enough that a loaded machine opens a
                    // database between two of them. A writer that retires
                    // files faster than a reader can open one keeps every
                    // read stale, which the retries cannot outlast (§6).
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                round
            })
        })
    };

    for _ in 0..20 {
        let out = on(&dir, &["scan"]);
        assert!(out.status.success(), "{}", stderr(&out));
        let keys = keys_of(&out);
        let (old, new) = keys.split_at(fixture.len().min(keys.len()));
        assert_eq!(old, fixture, "the fixture's keys, once each");
        assert!(
            new.windows(2).all(|w| w[0] < w[1]),
            "the writer's keys, in order and once each: {new:?}"
        );
        for args in [&["stats"][..], &["tables"], &["get", "x000"]] {
            let out = on(&dir, args);
            // `get` may run before the writer has put the key.
            assert!(out.status.code() != Some(2), "{args:?}: {}", stderr(&out));
        }
    }
    stop.store(true, Ordering::Relaxed);
    let rounds = writer.join().expect("the writer");
    assert!(rounds > 0, "the writer never wrote");
}
