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
