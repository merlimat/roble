//! `Db::levels`: the public, owned view of which table is where.
//!
//! Checked against the version it copies, which roble's own tests read
//! through the hidden `Db::versions`, so that the view says what the store
//! holds rather than what this file expects it to.

use std::path::PathBuf;

use roble::Options;
use roble::sim::SimEnv;

/// Writes `keys` and flushes them into one L0 table.
async fn flushed(db: &roble::Db<roble::sim::SimFs>, keys: &[&[u8]]) {
    let mut batch = db.indexed_batch();
    for key in keys {
        batch.put(key, b"value").expect("put");
    }
    batch
        .commit(roble::Durability::NoSync)
        .await
        .expect("commit");
    db.flush().await.expect("flush");
}

#[compio::test]
async fn levels_shows_every_level_with_its_tables_user_keys_and_sublevels() {
    let env = SimEnv::new();
    let db = roble::open_with(
        &env,
        PathBuf::from("/db"),
        Options::new()
            .create_if_missing(true)
            .disable_automatic_compactions(true)
            // So the three flushes below are enough for the picker to take L0.
            .l0_compaction_threshold(2),
    )
    .await
    .expect("open");

    let empty = db.levels().expect("levels");
    assert_eq!(empty.len(), 7, "every level, L0 to L6");
    assert!(
        empty
            .iter()
            .enumerate()
            .all(|(i, l)| l.level == i as u32 && l.sublevels == 0 && l.tables.is_empty()),
        "an empty store has empty levels of no sublevels: {empty:?}"
    );

    // Two flushes over the same keys overlap, so L0 is two sublevels deep;
    // a third over keys of their own sits beside the first.
    flushed(&db, &[b"b", b"d"]).await;
    flushed(&db, &[b"c", b"e"]).await;
    flushed(&db, &[b"x", b"z"]).await;

    let levels = db.levels().expect("levels");
    let l0 = &levels[0];
    assert_eq!(l0.tables.len(), 3);
    assert_eq!(l0.sublevels, 2, "b..d and c..e overlap, x..z does not");
    let bounds: Vec<(&[u8], &[u8])> = l0
        .tables
        .iter()
        .map(|t| (t.smallest.as_slice(), t.largest.as_slice()))
        .collect();
    assert_eq!(
        bounds,
        [(&b"b"[..], &b"d"[..]), (b"c", b"e"), (b"x", b"z")],
        "user keys, in file-number order"
    );

    let versions = db.versions();
    for (level, tables) in versions.version.levels() {
        let shown = &levels[level as usize];
        assert_eq!(
            shown
                .tables
                .iter()
                .map(|t| (t.file_num, t.size, t.smallest_seq_num, t.largest_seq_num))
                .collect::<Vec<_>>(),
            tables
                .iter()
                .map(|t| (t.file_num, t.size, t.smallest_seq_num, t.largest_seq_num))
                .collect::<Vec<_>>(),
            "L{level} should show the version's tables"
        );
    }

    // Compacted, L0 empties into one layer of L1.
    while !db.compact_once().await.expect("compact").is_empty() {}
    let after = db.levels().expect("levels");
    assert_eq!(after[0].sublevels, 0);
    let below: Vec<_> = after[1..].iter().filter(|l| !l.tables.is_empty()).collect();
    assert_eq!(
        below.len(),
        1,
        "one level below L0 holds the output: {after:?}"
    );
    assert_eq!(below[0].sublevels, 1);

    // And what was taken before is a copy, which the compaction left alone.
    assert_eq!(levels[0].tables.len(), 3);
}
