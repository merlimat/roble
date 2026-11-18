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

//! Reading, judged against Pebble.
//!
//! `harness/read-probes` builds a database with a history messy enough to be
//! worth reading — a small key alphabet so overwrites and deletes pile up,
//! range deletions cutting across flushed tables, four overlapping tables in
//! L0 above one in L6 — and writes down what Pebble answers to each of the
//! five comparisons for every probe key. This asks roble the same questions
//! and compares the whole text.
//!
//! Pebble is the oracle rather than a model written beside roble. §12 asks for
//! model-based tests against a `BTreeMap`; those are worth writing in M3, when
//! roble can write its own databases and the model can be driven by the same
//! operations. Until then the operations are Pebble's, and Pebble is both the
//! only available model and the stronger one, since it is the thing roble has
//! to agree with.

use std::path::{Path, PathBuf};

use roble::{Comparison, Found};

fn fixture(name: &str) -> (tempfile::TempDir, PathBuf) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    let tmp = tempfile::tempdir().expect("temp dir");
    let dst = tmp.path().join(name);
    std::fs::create_dir(&dst).expect("create");
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        std::fs::copy(entry.path(), dst.join(entry.file_name())).expect("copy");
    }
    (tmp, dst)
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd-length hex {s:?}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A key-and-value answer, as the harness spells it.
fn spell(found: &Option<Found>) -> String {
    match found {
        None => "none".to_string(),
        Some(f) => format!("{} {}", hex(&f.key), hex(&f.value)),
    }
}

/// Pebble's answers to the probes, and the probe keys they were asked of.
fn pebble_probes() -> (String, Vec<Vec<u8>>) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-reads.probes");
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{}: {e}; regenerate with 'cd harness && go run ./read-probes ../tests/golden'",
            path.display()
        )
    });
    let probes: Vec<Vec<u8>> = expected
        .lines()
        .filter_map(|l| l.strip_prefix("probe "))
        .map(unhex)
        .collect();
    assert!(!probes.is_empty(), "no probes in {}", path.display());
    (expected, probes)
}

/// roble's answers to the probes, spelled as the harness spells Pebble's.
async fn answers<F: roble::Fs>(db: &roble::Db<F>, probes: &[Vec<u8>]) -> String {
    let mut out = String::new();
    for probe in probes {
        out.push_str(&format!("probe {}\n", hex(probe)));

        let equal = db.get(probe, Comparison::Equal).await.expect("equal");
        // Pebble's Get answers with a value alone, the key being the probe.
        out.push_str(&match &equal {
            None => "  equal none\n".to_string(),
            Some(f) => {
                assert_eq!(
                    &f.key, probe,
                    "an Equal read must answer about the key asked"
                );
                format!("  equal {}\n", hex(&f.value))
            }
        });

        for (name, how) in [
            ("ceiling", Comparison::Ceiling),
            ("higher", Comparison::Higher),
            ("floor", Comparison::Floor),
            ("lower", Comparison::Lower),
        ] {
            let found = db.get(probe, how).await.expect(name);
            out.push_str(&format!("  {name} {}\n", spell(&found)));
        }
    }
    out
}

/// Fails with the first line where roble's answers leave Pebble's.
fn assert_answers_match(out: &str, expected: &str, when: &str) {
    if out != expected {
        let (a, b) = (
            out.lines().collect::<Vec<_>>(),
            expected.lines().collect::<Vec<_>>(),
        );
        let at = a
            .iter()
            .zip(&b)
            .position(|(x, y)| x != y)
            .unwrap_or(a.len().min(b.len()));
        let from = at.saturating_sub(6);
        panic!(
            "{when}: reads differ at line {}:\n  roble:  {:?}\n  pebble: {:?}\ncontext:\n{}",
            at + 1,
            a.get(at),
            b.get(at),
            b[from..(at + 2).min(b.len())].join("\n")
        );
    }
}

#[compio::test]
async fn roble_reads_what_go_pebble_reads() {
    let (expected, probes) = pebble_probes();

    let (_tmp, db_dir) = fixture("db-reads");
    let db = roble::open(&roble::DefaultEnv, &db_dir)
        .await
        .expect("open the database Go wrote");

    // The shape the harness asserted when it built this: if a later change
    // flattens it, the comparison below would still pass while testing much
    // less, so it is checked here too.
    assert!(
        db.versions().version.level(0).len() >= 2,
        "L0 should hold overlapping tables, holds {}",
        db.versions().version.level(0).len()
    );
    assert!(
        db.versions()
            .version
            .levels()
            .filter(|(_, t)| !t.is_empty())
            .count()
            >= 2,
        "the database should span more than one level"
    );

    let out = answers(&db, &probes).await;
    assert_answers_match(&out, &expected, "the database as Go wrote it");
}

/// The tables of `level` whose user keys run from `smallest` to `largest`,
/// oldest first.
fn tables_spanning<F: roble::Fs>(
    db: &roble::Db<F>,
    level: u32,
    smallest: &[u8],
    largest: &[u8],
) -> Vec<u64> {
    let mut found: Vec<u64> = db
        .versions()
        .version
        .level(level)
        .iter()
        .filter(|t| {
            t.smallest_user_key().expect("smallest") == smallest
                && t.largest_user_key().expect("largest") == largest
        })
        .map(|t| t.file_num)
        .collect();
    found.sort_unstable();
    found
}

/// Every entry of the tables, as the format crate's dump prints them.
fn dumps(dir: &Path, tables: &[u64]) -> String {
    tables
        .iter()
        .map(|n| {
            let bytes = std::fs::read(dir.join(format!("{n:06}.sst"))).expect("read a table");
            roble::roble_format::dump_table(&bytes).expect("dump a table")
        })
        .collect()
}

/// A compaction of what Pebble wrote changes none of Pebble's answers, at any
/// step, including the step where roble writes the kinds it only reads.
///
/// `SINGLEDEL` and `DELSIZED` are §3's "read, never written", and roble's
/// own writes never produce them. A compaction does copy them, though: it
/// keeps each key's newest entry, and a deletion that still has something
/// below it to hide stays (`crate::compact`). Every other compaction test
/// starts from a fixture with neither kind in it, and compacting `db-reads`
/// all at once would not reach the case either, because each kind's Set
/// would be in the same merge and both would go. So the Sets go down first:
/// the oldest table, holding `s0`'s Set, into L2 together with the `u` keys'
/// table, so that one L2 table spans `t0` as well. Then the tables holding
/// `SINGLEDEL s0`, `DELSIZED t0` and `SETWITHDEL t1` go to L1 over it, and
/// each deletion has something below it to hide.
///
/// Then everything settles into L6, where the deletions have nothing left to
/// hide and go. Pebble's answers are checked after every step. The
/// directory at the step that writes the kinds is the one
/// `compat/wal-replay --kinds` hands Pebble to read and compact.
#[compio::test]
async fn compacting_what_go_pebble_wrote_changes_none_of_its_answers() {
    let (expected, probes) = pebble_probes();
    let (_tmp, dir) = fixture("db-reads");
    let db = roble::open_with(
        &roble::DefaultEnv,
        &dir,
        roble::Options::new()
            .disable_automatic_compactions(true)
            .background_tasks(false),
    )
    .await
    .expect("open the database Go wrote");

    // The scripted tables, found by what they span rather than by number.
    let s = tables_spanning(&db, 0, b"s0", b"s1");
    let t = tables_spanning(&db, 0, b"s1", b"t1");
    let u = tables_spanning(&db, 0, b"u0", b"u2");
    let ([sets, singledels], [kinds], [u_sets, _u_cut]) = (&s[..], &t[..], &u[..]) else {
        panic!("the fixture no longer has the tables this test is built around: {s:?} {t:?} {u:?}");
    };
    assert!(
        dumps(&dir, &[*singledels]).contains("singledel 7330")
            && dumps(&dir, &[*kinds]).contains("delsized 7430"),
        "the fixture's scripted tables should hold SINGLEDEL s0 and DELSIZED t0"
    );

    let low = [
        db.compact(0, &[*sets]).await.expect("compact the Sets"),
        db.compact(0, &[*u_sets]).await.expect("compact the u keys"),
    ]
    .concat();
    assert_answers_match(
        &answers(&db, &probes).await,
        &expected,
        "the oldest tables moved to L1",
    );
    let below = db.compact(1, &low).await.expect("compact into L2");
    assert_eq!(below.len(), 1, "one L2 table should span s0 to u2");
    assert_answers_match(
        &answers(&db, &probes).await,
        &expected,
        "the Sets settled into L2",
    );

    let out = db
        .compact(0, &[*singledels, *kinds])
        .await
        .expect("compact the deletions");
    let written = dumps(&dir, &out);
    for (entry, kind) in [
        ("singledel 7330", "SINGLEDEL"),
        ("delsized 7430", "DELSIZED"),
        ("setwithdel 7431", "SETWITHDEL"),
        // s1's Set buried its SINGLEDEL, with the first Set still in L2.
        ("setwithdel 7331", "SETWITHDEL over a buried SINGLEDEL"),
    ] {
        assert!(
            written.contains(entry),
            "roble's output should carry {kind} over what lies below it:\n{written}"
        );
    }
    assert_answers_match(
        &answers(&db, &probes).await,
        &expected,
        "SINGLEDEL and DELSIZED written by roble",
    );

    // Everything down a level at a time, until it all sits in L6.
    for level in 0..6 {
        let tables: Vec<u64> = db
            .versions()
            .version
            .level(level)
            .iter()
            .map(|t| t.file_num)
            .collect();
        if tables.is_empty() {
            continue;
        }
        db.compact(level, &tables).await.expect("compact a level");
        assert_answers_match(
            &answers(&db, &probes).await,
            &expected,
            &format!("L{level} compacted into L{}", level + 1),
        );
    }
    let deepest: Vec<u32> = db
        .versions()
        .version
        .levels()
        .filter(|(_, t)| !t.is_empty())
        .map(|(l, _)| l)
        .collect();
    assert_eq!(deepest, [6], "everything should have settled into L6");
    let settled: Vec<u64> = db
        .versions()
        .version
        .level(6)
        .iter()
        .map(|t| t.file_num)
        .collect();
    let settled = dumps(&dir, &settled);
    assert!(
        !settled.contains("singledel") && !settled.contains("delsized"),
        "a deletion with nothing below it should be gone from L6:\n{settled}"
    );

    // And a reopen reads what the MANIFEST roble appended to says.
    drop(db);
    let db = roble::open(&roble::DefaultEnv, &dir).await.expect("reopen");
    assert_answers_match(&answers(&db, &probes).await, &expected, "after a reopen");
}

/// `get_into` answers what `get` answers, through one `GetBuf` reused for
/// every probe and every comparison.
///
/// The reuse is the whole point of the method, and it is also where it can
/// quietly break. A buffer not cleared would show up as an answer with the
/// previous probe's bytes still on the front; a miss that left the buffer
/// filled would show up as the *next* miss looking like a hit. Every call
/// after the first answer arrives at a buffer that still holds the last one,
/// and the counts at the end are what says both cases were reached.
///
/// Pebble's probe set is the oracle again, and asking `get` and `get_into`
/// the same question is deliberate: `get` is compared with Pebble line by
/// line in the test above, so agreeing with `get` is agreeing with Pebble
/// without a second copy of the expected text to keep current.
#[compio::test]
async fn get_into_answers_what_get_answers_through_one_reused_lookup() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/db-reads.probes");
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{}: {e}; regenerate with 'cd harness && go run ./read-probes ../tests/golden'",
            path.display()
        )
    });
    let probes: Vec<Vec<u8>> = expected
        .lines()
        .filter_map(|l| l.strip_prefix("probe "))
        .map(unhex)
        .collect();
    assert!(!probes.is_empty(), "no probes in {}", path.display());

    let (_tmp, db_dir) = fixture("db-reads");
    let db = roble::open(&roble::DefaultEnv, &db_dir)
        .await
        .expect("open the database Go wrote");

    let mut buf = roble::GetBuf::new();
    let (mut hits, mut misses, mut after_an_answer) = (0usize, 0usize, 0usize);
    for probe in &probes {
        for (name, how) in [
            ("equal", Comparison::Equal),
            ("ceiling", Comparison::Ceiling),
            ("higher", Comparison::Higher),
            ("floor", Comparison::Floor),
            ("lower", Comparison::Lower),
        ] {
            let owned = db.get(probe, how).await.expect(name);
            let dirty = !buf.key().is_empty() || !buf.value().is_empty();

            let found = db.get_into(probe, how, &mut buf).await.expect(name);

            if dirty {
                after_an_answer += 1;
            }
            assert_eq!(
                found,
                owned.is_some(),
                "{name} of {}: get_into said {found}, get said {}",
                hex(probe),
                owned.is_some()
            );
            match &owned {
                Some(f) => {
                    assert_eq!(
                        (buf.key(), buf.value()),
                        (f.key.as_slice(), f.value.as_slice()),
                        "{name} of {}: get_into filled {} / {}, get answered {} / {}",
                        hex(probe),
                        hex(buf.key()),
                        hex(buf.value()),
                        hex(&f.key),
                        hex(&f.value)
                    );
                    hits += 1;
                }
                None => {
                    assert!(
                        buf.key().is_empty() && buf.value().is_empty(),
                        "{name} of {}: no answer, but the buffer kept {} / {}",
                        hex(probe),
                        hex(buf.key()),
                        hex(buf.value())
                    );
                    misses += 1;
                }
            }
        }
    }
    // Without all three, part of the contract above would go untested while
    // the test still passed, which is the way a test like this rots.
    assert!(
        hits > 0 && misses > 0 && after_an_answer > 0,
        "the fixture should reach answers, absences and a reused buffer: \
         {hits} hits, {misses} misses, {after_an_answer} calls onto a full buffer"
    );
}

/// `take` hands the answer over and leaves the working space empty.
#[compio::test]
async fn take_empties_the_lookup() {
    let (_tmp, db_dir) = fixture("db-reads");
    let db = roble::open(&roble::DefaultEnv, &db_dir)
        .await
        .expect("open the database Go wrote");

    // Whatever the fixture's first key is, reached without assuming one.
    let mut scan = db.range(..).expect("scan");
    scan.seek_ge(b"").await.expect("seek");
    assert!(scan.is_valid(), "the fixture should hold at least one key");
    let (key, value) = (scan.key().to_vec(), scan.value().to_vec());
    drop(scan);

    let mut buf = roble::GetBuf::new();
    assert!(
        db.get_into(&key, Comparison::Equal, &mut buf)
            .await
            .expect("get_into"),
        "the key a scan just handed back should be readable"
    );
    let taken = buf.take();
    assert_eq!(taken.key, key);
    assert_eq!(taken.value, value);
    assert!(
        buf.key().is_empty() && buf.value().is_empty(),
        "take should leave the working space empty, kept {} / {}",
        hex(buf.key()),
        hex(buf.value())
    );
}
