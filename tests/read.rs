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

#[compio::test]
async fn roble_reads_what_go_pebble_reads() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let path = dir.join("db-reads.probes");
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "{}: {e}; regenerate with 'cd harness && go run ./read-probes ../tests/golden'",
            path.display()
        )
    });

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

    let probes: Vec<Vec<u8>> = expected
        .lines()
        .filter_map(|l| l.strip_prefix("probe "))
        .map(unhex)
        .collect();
    assert!(!probes.is_empty(), "no probes in {}", path.display());

    let mut out = String::new();
    for probe in &probes {
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
            "reads differ at line {}:\n  roble:  {:?}\n  pebble: {:?}\ncontext:\n{}",
            at + 1,
            a.get(at),
            b.get(at),
            b[from..(at + 2).min(b.len())].join("\n")
        );
    }
}

/// `get_into` answers what `get` answers, through one `Lookup` reused for
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

    let mut buf = roble::Lookup::new();
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
    let mut scan = db.scan().await.expect("scan");
    scan.seek_ge(b"").await.expect("seek");
    assert!(scan.is_valid(), "the fixture should hold at least one key");
    let (key, value) = (scan.key().to_vec(), scan.value().to_vec());
    drop(scan);

    let mut buf = roble::Lookup::new();
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
