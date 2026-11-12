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

//! What a store exports, collected through a real OpenTelemetry reader.
//!
//! Not through roble's own idea of what it recorded: the point of these is
//! that the numbers come back out of a provider the way an application's
//! would, with the attributes they were given.
//!
//! **Histograms are not checked here.** nanometer exports those through its
//! `Producer`, which an application registers with its metric reader and
//! which needs an OTel SDK with `MetricProducer` support — a patched one, at
//! this version. So `roble.write.size`, `roble.write.entries` and
//! `roble.flush.size` have no assertion below, and a mutation of what they
//! record survives: that is a gap in what can be collected, not one in what
//! is recorded, and it closes when the SDK does.
//!
//! nanometer's registry is process-wide and keyed by (scope, metric name),
//! and a metric is registered with the provider of the *first* store to
//! create it. So these tests share one provider — a second would be handed
//! nothing — and tell their series apart by an attribute of their own.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{
    InstrumentKind, ManualReader, Pipeline, SdkMeterProvider, Temporality,
};

use roble::cache::DEFAULT_OPEN_TABLES;
use roble::sim::{SimEnv, SimFs};
use roble::{Caches, Comparison, Disk, Fs, Options, WriteBuf};
use roble_format::batch::Batch;

/// A reader the test keeps a handle to, so it can collect on demand.
#[derive(Clone, Debug)]
struct Reader(Arc<ManualReader>);

type Sdk = opentelemetry_sdk::error::OTelSdkResult;

impl MetricReader for Reader {
    fn register_pipeline(&self, p: std::sync::Weak<Pipeline>) {
        self.0.register_pipeline(p);
    }
    fn collect(&self, rm: &mut ResourceMetrics) -> Sdk {
        self.0.collect(rm)
    }
    fn force_flush(&self) -> Sdk {
        self.0.force_flush()
    }
    fn shutdown_with_timeout(&self, t: std::time::Duration) -> Sdk {
        self.0.shutdown_with_timeout(t)
    }
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.0.temporality(kind)
    }
}

/// Every data point roble exported, as (metric, attributes, value).
///
/// One extraction for both shapes a sum comes in — counters are unsigned,
/// up-down counters signed — so the two readings below cannot drift. A
/// histogram would be neither, and none reaches this reader: nanometer
/// exports those through its `Producer`, which these tests do not register.
fn points(reader: &Reader) -> Vec<(String, Vec<KeyValue>, i64)> {
    let mut rm = ResourceMetrics::default();
    reader.collect(&mut rm).expect("collect");
    let mut out = Vec::new();
    for scope in rm.scope_metrics() {
        if scope.scope().name() != "roble" {
            continue;
        }
        for metric in scope.metrics() {
            let name = metric.name().to_string();
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    out.extend(sum.data_points().map(|p| {
                        (
                            name.clone(),
                            p.attributes().cloned().collect::<Vec<_>>(),
                            p.value() as i64,
                        )
                    }))
                }
                AggregatedMetrics::I64(MetricData::Sum(sum)) => out.extend(
                    sum.data_points()
                        .map(|p| (name.clone(), p.attributes().cloned().collect(), p.value())),
                ),
                data => panic!("{name} is neither kind of sum: {data:?}"),
            }
        }
    }
    out
}

/// Every metric whose data points carry `tag`, summed, by name.
fn collected(reader: &Reader, tag: &KeyValue) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    for (name, attributes, value) in points(reader) {
        if attributes.contains(tag) {
            *out.entry(name).or_default() += value;
        }
    }
    out
}

/// One metric's data points carrying `tag`, by the value of their `key`
/// attribute.
///
/// Summing across an attribute hides exactly what it is there to say — which
/// kind of operation, which direction, which level — so anything with one is
/// read through this.
fn by_attr(reader: &Reader, tag: &KeyValue, name: &str, key: &str) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    for (metric, attributes, value) in points(reader) {
        if metric != name || !attributes.contains(tag) {
            continue;
        }
        let at = attributes
            .iter()
            .find(|a| a.key.as_str() == key)
            .map(|a| a.value.to_string())
            .expect("the attribute the metric is split by");
        // Summed, not replaced: several series can share the value this is
        // grouping by. `roble.cache.lookups` has four, and grouping them by
        // result alone puts the block cache's hits and the table cache's
        // into one number.
        *out.entry(at).or_default() += value;
    }
    out
}

/// One cache's hits and misses, which is two attributes deep: grouping by
/// the result alone adds the block cache's to the table cache's, and then a
/// hit counted against the wrong cache looks like no change at all.
fn lookups(reader: &Reader, tag: &KeyValue, cache: &str) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    for (metric, attributes, value) in points(reader) {
        if metric != "roble.cache.lookups" || !attributes.contains(tag) {
            continue;
        }
        let of = |key: &str| {
            attributes
                .iter()
                .find(|a| a.key.as_str() == key)
                .map(|a| a.value.to_string())
                .expect("the attribute")
        };
        if of("roble.cache") == cache {
            out.insert(of("roble.cache.result"), value);
        }
    }
    out
}

async fn load(name: &str) -> (SimEnv, PathBuf) {
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    let env = SimEnv::new();
    let dir = PathBuf::from("/db");
    let fs: &SimFs = env.sim();
    fs.mkdir_all(&dir).await.expect("mkdir");
    for entry in std::fs::read_dir(&src).expect("read fixture") {
        let entry = entry.expect("entry");
        let bytes = std::fs::read(entry.path()).expect("read file");
        let disk = fs
            .create(&dir.join(entry.file_name()))
            .await
            .expect("create");
        let (result, _) = disk.write(WriteBuf::new(bytes), 0, ()).await;
        result.expect("write");
        disk.sync(()).await.expect("sync");
        disk.close().await.expect("close");
    }
    fs.sync_dir(&dir).await.expect("sync dir");
    (env, dir)
}

/// The one provider every test shares, and the reader they collect from.
static METRICS: LazyLock<(SdkMeterProvider, Reader)> = LazyLock::new(|| {
    let reader = Reader(Arc::new(ManualReader::builder().build()));
    let provider = SdkMeterProvider::builder()
        .with_reader(reader.clone())
        .build();
    (provider, reader)
});

/// Options that export, with an attribute that tells this test's store apart
/// from every other in the process.
fn metered(case: &'static str) -> (Options<SimFs>, KeyValue) {
    let tag = KeyValue::new("case", case);
    let options = Options::new()
        .meter_provider(&METRICS.0)
        .metric_attributes(std::slice::from_ref(&tag));
    (options, tag)
}

/// The reader the shared provider writes into.
fn reader() -> &'static Reader {
    &METRICS.1
}

#[compio::test]
async fn a_store_without_a_provider_records_nothing() {
    // The default, and what every other test in this repository runs with.
    let (_options, tag) = metered("silent");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open(&env, &dir).await.expect("open");

    let mut batch = Batch::new();
    batch.put(b"a", b"b").expect("put");
    db.write(&mut batch, true).await.expect("write");
    db.flush().await.expect("flush").expect("a table");

    assert!(
        collected(reader(), &tag).is_empty(),
        "a store with no meter provider exported something"
    );
}

#[compio::test]
async fn writes_are_counted_by_what_they_did() {
    let (options, tag) = metered("writes");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options).await.expect("open");

    let mut batch = Batch::new();
    batch.put(b"k1", b"v").expect("put");
    batch.put(b"k2", b"v").expect("put");
    batch.put(b"k3", b"v").expect("put");
    batch.delete(b"k1").expect("delete");
    batch.delete_range(b"k2", b"k3").expect("delete_range");
    db.write(&mut batch, true).await.expect("write");

    let seen = collected(reader(), &tag);
    assert_eq!(
        seen.get("roble.write.operations").copied(),
        Some(5),
        "five entries went in: {seen:?}"
    );
    // And by kind, which is the whole reason the attribute is there.
    let kinds = by_attr(
        reader(),
        &tag,
        "roble.write.operations",
        "roble.operation.type",
    );
    assert_eq!(kinds.get("put").copied(), Some(3), "{kinds:?}");
    assert_eq!(kinds.get("delete").copied(), Some(1), "{kinds:?}");
    assert_eq!(kinds.get("delete_range").copied(), Some(1), "{kinds:?}");
    assert!(
        seen["roble.memtable.size"] > 0,
        "the memtable holds the batch now: {seen:?}"
    );
}

#[compio::test]
async fn a_scan_counts_the_keys_it_steps_onto() {
    let (options, tag) = metered("scans");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options).await.expect("open");

    let mut walked = 0;
    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    while scan.is_valid() {
        walked += 1;
        scan.next().await.expect("next");
    }
    assert!(walked > 0, "the fixture should hold keys");

    let seen = collected(reader(), &tag);
    assert_eq!(seen.get("roble.scans").copied(), Some(1));
    assert_eq!(
        seen.get("roble.scan.entries").copied(),
        Some(walked),
        "every key the scan landed on should have been counted: {seen:?}"
    );
    assert!(
        seen["roble.read.bytes"] > 0,
        "and the bytes it handed back with them: {seen:?}"
    );

    // A `get` under one of the ordered comparisons is a scan too, positioned
    // once, so it counts one entry and not a walk.
    db.get(b"a0", Comparison::Ceiling).await.expect("get");
    let after = collected(reader(), &tag);
    assert_eq!(after.get("roble.scans").copied(), Some(2));
    assert_eq!(after.get("roble.scan.entries").copied(), Some(walked + 1));
}

#[compio::test]
async fn a_flush_and_a_compaction_report_what_they_moved() {
    let (options, tag) = metered("flush-compact");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options).await.expect("open");

    let before = by_attr(reader(), &tag, "roble.level.tables", "roble.level");
    assert_eq!(
        before.get("0").copied(),
        Some(1),
        "the fixture arrives with one L0 table: {before:?}"
    );

    let mut batch = Batch::new();
    for i in 0..8 {
        batch
            .put(format!("m{i:02}").as_bytes(), b"value")
            .expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");

    let after_flush = by_attr(reader(), &tag, "roble.level.tables", "roble.level");
    assert_eq!(
        after_flush.get("0").copied(),
        Some(2),
        "the flush added an L0 table: {after_flush:?}"
    );
    assert_eq!(
        collected(reader(), &tag)["roble.memtable.size"],
        0,
        "and emptied the memtable"
    );

    let outputs = db.compact(0, &[table]).await.expect("compact");
    assert_eq!(outputs.len(), 1);

    let seen = collected(reader(), &tag);
    assert_eq!(
        seen.get("roble.compaction.tables").copied(),
        Some(2),
        "one table read and one written: {seen:?}"
    );
    let moved = by_attr(
        reader(),
        &tag,
        "roble.compaction.tables",
        "disk.io.direction",
    );
    assert_eq!(
        moved.get("read").copied(),
        Some(1),
        "one table read: {moved:?}"
    );
    assert_eq!(
        moved.get("write").copied(),
        Some(1),
        "and one written: {moved:?}"
    );
    let bytes = by_attr(reader(), &tag, "roble.compaction.io", "disk.io.direction");
    assert!(bytes["read"] > 0 && bytes["write"] > 0, "{bytes:?}");

    let levels = by_attr(reader(), &tag, "roble.level.tables", "roble.level");
    assert_eq!(
        levels.get("0").copied(),
        Some(1),
        "L0 is back to the fixture's"
    );
    assert_eq!(levels.get("1").copied(), Some(1), "and the output is in L1");
    let sizes = by_attr(reader(), &tag, "roble.level.size", "roble.level");
    assert_eq!(
        sizes.get("1").copied(),
        Some(db.versions().version.level(1)[0].size as i64),
        "L1's bytes are the table's bytes, not a count of them: {sizes:?}"
    );
}

#[compio::test]
async fn the_bytes_a_write_amplification_needs_are_both_there() {
    // Pebble's write amplification is every byte written over the bytes that
    // flushes wrote. Both halves have to be counters, not histograms, or the
    // ratio cannot be taken.
    let (options, tag) = metered("amplification");
    let (env, dir) = load("db-twin-a").await;
    // The compactions here are the test's own, named table by table, so the
    // flushes are left to do nothing but flush.
    let db = roble::open_with(&env, &dir, options.disable_automatic_compactions(true))
        .await
        .expect("open");

    let value = vec![b'v'; 200];
    let mut tables = Vec::new();
    for round in 0..3 {
        let mut batch = Batch::new();
        for i in 0..20 {
            batch
                .put(format!("w{:03}", round * 10 + i).as_bytes(), &value)
                .expect("put");
        }
        db.write(&mut batch, true).await.expect("write");
        tables.push(db.flush().await.expect("flush").expect("a table"));
    }
    db.compact(0, &tables).await.expect("compact");

    let seen = collected(reader(), &tag);
    let flushed = seen["roble.flush.bytes"];
    let written = seen["roble.compaction.io"];
    assert!(flushed > 0, "flushes should have written bytes: {seen:?}");
    assert!(
        written > flushed,
        "a compaction that read three tables and wrote one should move more bytes \
         than the flushes did, since it both read and wrote: {seen:?}"
    );

    // And the directory on disk is reported. Not the bytes flushes wrote,
    // which is a running total and counts tables the compaction has since
    // swept away: what is there now, which is the version's tables plus the
    // MANIFEST, the log and the markers beside them.
    assert!(
        seen["roble.disk.size"] > seen["roble.level.size"],
        "the directory holds more than its tables: {seen:?}"
    );
}

#[compio::test]
async fn what_oxia_graphs_from_pebble_is_there() {
    // Oxia's data server exports a fixed set of Pebble's metrics per shard
    // (`kv_pebble.go`). These are the ones roble can answer today, under its
    // own names; the correspondence is written out in METRICS.md.
    let (options, tag) = metered("oxia");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options.disable_automatic_compactions(true))
        .await
        .expect("open");

    let value = vec![b'v'; 200];
    let mut tables = Vec::new();
    for round in 0..3 {
        let mut batch = Batch::new();
        for i in 0..20 {
            batch
                .put(format!("o{:03}", round * 10 + i).as_bytes(), &value)
                .expect("put");
        }
        db.write(&mut batch, true).await.expect("write");
        tables.push(db.flush().await.expect("flush").expect("a table"));
    }
    db.get(b"o000", Comparison::Equal).await.expect("get");
    db.compact(0, &tables).await.expect("compact");

    let seen = collected(reader(), &tag);
    for name in [
        "roble.flushes",          // pebble_flush_total
        "roble.flush.bytes",      // pebble_flush
        "roble.compactions",      // pebble_compactions_total
        "roble.memtable.size",    // pebble_memtable_size
        "roble.disk.size",        // pebble_disk_space
        "roble.level.tables",     // pebble_num_files_total
        "roble.read.bytes",       // kv_read
        "roble.write.bytes",      // kv_write
        "roble.write.operations", // kv_write_ops
    ] {
        assert!(seen.contains_key(name), "{name} is missing: {seen:?}");
    }
    assert_eq!(seen["roble.flushes"], 3);
    assert_eq!(seen["roble.compactions"], 1);
    assert!(seen["roble.read.bytes"] >= 200, "{seen:?}");
    // 60 keys of 200-byte values, so the batches are well past 12 KiB, and
    // the counter has to carry the bytes rather than merely exist.
    assert!(
        seen["roble.write.bytes"] > 12_000,
        "the bytes a write carried should be counted: {seen:?}"
    );

    // The debt: how far the levels are past what they may hold. Zero here,
    // because nothing is.
    assert_eq!(
        seen.get("roble.compaction.debt").copied(),
        Some(0),
        "nothing is past its allowance: {seen:?}"
    );
    assert_eq!(db.compaction_debt(), 0);
}

#[compio::test]
async fn the_debt_is_what_the_levels_hold_above_their_allowances() {
    let (options, tag) = metered("debt");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(
        &env,
        &dir,
        options
            .l0_compaction_threshold(1)
            .lbase_max_bytes(1)
            .disable_automatic_compactions(true),
    )
    .await
    .expect("open");

    let value = vec![b'v'; 200];
    let mut batch = Batch::new();
    for i in 0..20 {
        batch
            .put(format!("d{i:03}").as_bytes(), &value)
            .expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");

    // Two L0 tables against an allowance of one: one table's worth of debt.
    let debt = collected(reader(), &tag)["roble.compaction.debt"];
    assert!(debt > 0, "L0 is past its allowance, so there is debt");

    db.compact(0, &[table]).await.expect("compact");
    let after = collected(reader(), &tag)["roble.compaction.debt"];
    assert_ne!(after, debt, "compacting should have moved the debt");
    assert_eq!(after as u64, db.compaction_debt());
}

#[compio::test]
async fn a_failed_write_is_counted_by_what_went_wrong() {
    let (options, tag) = metered("errors");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options.disable_wal(true))
        .await
        .expect("open");

    let mut batch = Batch::new();
    batch.put(b"durable", b"please").expect("put");
    db.write(&mut batch, true)
        .await
        .expect_err("a synced write with the WAL off is refused");

    let kinds = by_attr(reader(), &tag, "roble.write.errors", "error.type");
    assert_eq!(
        kinds.get("unsupported").copied(),
        Some(1),
        "the refusal should be classified, not merely counted: {kinds:?}"
    );
    // The other kinds exist at zero — a handle is created for each when the
    // store opens — which is what makes a dashboard's series appear before
    // anything has gone wrong.
    assert_eq!(kinds.get("io").copied(), Some(0), "{kinds:?}");
    assert_eq!(kinds.get("corrupt").copied(), Some(0), "{kinds:?}");
}

#[compio::test]
async fn a_failed_lookup_is_counted_by_what_went_wrong() {
    let (options, tag) = metered("read-errors");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options).await.expect("open");

    // The next filesystem operation fails, and a lookup that has to open a
    // table makes one.
    env.sim().fail_at(1);
    db.get(b"a0", Comparison::Equal)
        .await
        .expect_err("the read should have failed");
    env.sim().stop_failing();

    let kinds = by_attr(reader(), &tag, "roble.read.errors", "error.type");
    assert_eq!(
        kinds.get("io").copied(),
        Some(1),
        "a read that failed on the disk is an io error: {kinds:?}"
    );
}

#[compio::test]
async fn open_scans_are_counted_until_they_are_dropped() {
    let (options, tag) = metered("open-scans");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options).await.expect("open");

    let a = db.scan().await.expect("scan");
    let b = db.scan().await.expect("scan");
    assert_eq!(
        collected(reader(), &tag).get("roble.scans.open").copied(),
        Some(2),
        "two scans are open"
    );
    drop(a);
    assert_eq!(
        collected(reader(), &tag).get("roble.scans.open").copied(),
        Some(1),
        "one went away"
    );
    drop(b);
    assert_eq!(
        collected(reader(), &tag).get("roble.scans.open").copied(),
        Some(0),
        "and a store that goes on reporting scans nobody holds is how a leak hides"
    );
}

#[compio::test]
async fn every_read_write_and_sync_is_counted() {
    let (options, tag) = metered("disk-io");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options).await.expect("open");

    // A lookup opens a table and reads blocks out of it.
    db.get(b"a0", Comparison::Equal).await.expect("get");
    let read = by_attr(reader(), &tag, "roble.disk.io", "disk.io.direction");
    let ops = by_attr(reader(), &tag, "roble.disk.operations", "disk.io.direction");
    assert!(read["read"] > 0, "a lookup read bytes: {read:?}");
    assert!(ops["read"] > 1, "in more than one request: {ops:?}");

    // A write goes to the log; a flush writes a table and a MANIFEST record.
    let mut batch = Batch::new();
    batch.put(b"io", b"counted").expect("put");
    db.write(&mut batch, true).await.expect("write");
    let after_write = by_attr(reader(), &tag, "roble.disk.io", "disk.io.direction");
    assert!(
        after_write["write"] > 0,
        "the log append should have been counted: {after_write:?}"
    );

    db.flush().await.expect("flush").expect("a table");
    let after_flush = by_attr(reader(), &tag, "roble.disk.io", "disk.io.direction");
    assert!(
        after_flush["write"] > after_write["write"],
        "and the table and the version edit too: {after_flush:?}"
    );
    let ops = by_attr(reader(), &tag, "roble.disk.operations", "disk.io.direction");
    assert!(
        ops["write"] >= 3,
        "the log, the table and the edit: {ops:?}"
    );

    // And a compaction writes its outputs through the same counters.
    let in_l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    let outputs = db.compact(0, &in_l0).await.expect("compact");
    let written: u64 = db
        .versions()
        .version
        .level(1)
        .iter()
        .filter(|t| outputs.contains(&t.file_num))
        .map(|t| t.size)
        .sum();
    assert!(written > 0, "the compaction wrote a table");
    let after_compaction = by_attr(reader(), &tag, "roble.disk.io", "disk.io.direction");
    assert!(
        after_compaction["write"] - after_flush["write"] >= written as i64,
        "a compaction's outputs should be counted, not just its version edit: \
         {after_compaction:?} against {after_flush:?}, for {written} bytes of table"
    );
}

#[compio::test]
async fn the_caches_report_what_they_hold_and_the_store_what_it_looked_for() {
    // The budget and what is held belong to the `Caches`, which every store
    // on a thread shares; who looked, and whether they found it, belongs to
    // the store that looked.
    let cache_tag = KeyValue::new("case", "caches");
    let caches: Caches<SimFs> = Caches::new(DEFAULT_OPEN_TABLES, 64 << 10)
        .meter_provider(&METRICS.0, std::slice::from_ref(&cache_tag));

    let (options, tag) = metered("cache-lookups");
    let (env, dir) = load("db-reads").await;
    let db = roble::open_with(&env, &dir, options.caches(caches.clone()))
        .await
        .expect("open");

    let limits = by_attr(reader(), &cache_tag, "roble.cache.limit", "roble.cache");
    assert_eq!(
        limits.get("block").copied(),
        Some(64 << 10),
        "the block budget is the caches' own: {limits:?}"
    );
    assert_eq!(
        limits.get("table").copied(),
        Some(DEFAULT_OPEN_TABLES as i64)
    );

    // Two reads of the same key: the first misses in both caches, the second
    // finds what the first left behind.
    db.get(b"a0", Comparison::Equal).await.expect("get");
    let before: Vec<BTreeMap<String, i64>> = ["block", "table"]
        .iter()
        .map(|cache| lookups(reader(), &tag, cache))
        .collect();
    for (cache, counts) in ["block", "table"].iter().zip(&before) {
        assert!(
            counts["miss"] > 0,
            "the first read missed in the {cache} cache: {counts:?}"
        );
    }
    assert_eq!(
        before[0].get("hit").copied(),
        Some(0),
        "and the block cache was empty: {:?}",
        before[0]
    );

    db.get(b"a0", Comparison::Equal).await.expect("get");
    for (cache, was) in ["block", "table"].iter().zip(&before) {
        let now = lookups(reader(), &tag, cache);
        assert!(
            now["hit"] > was["hit"],
            "the second read should have hit what the first left in the {cache} cache: \
             {now:?} against {was:?}"
        );
    }

    let used = by_attr(reader(), &cache_tag, "roble.cache.usage", "roble.cache");
    assert!(
        used["block"] > 0,
        "the block cache holds what the reads fetched: {used:?}"
    );
    assert!(used["table"] > 0, "and a reader per table opened: {used:?}");
    assert!(
        used["block"] <= (64 << 10),
        "never more than its budget: {used:?}"
    );
}

#[compio::test]
async fn a_compaction_does_not_fill_the_block_cache() {
    // The thing this is for: a compaction reads every block of its inputs
    // once and never comes back, so the blocks the foreground is reading have
    // to still be there afterwards.
    let cache_tag = KeyValue::new("case", "no-pollution");
    // Room for a few blocks and nothing like the table written below, so a
    // compaction that filled the cache would certainly push the foreground's
    // blocks out.
    let caches: Caches<SimFs> = Caches::new(DEFAULT_OPEN_TABLES, 16 << 10)
        .meter_provider(&METRICS.0, std::slice::from_ref(&cache_tag));

    let (options, tag) = metered("no-pollution-store");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options.caches(caches))
        .await
        .expect("open");

    // Something worth compacting: 200 keys of 200 bytes, well past the cache.
    let value = vec![b'v'; 200];
    let mut batch = Batch::new();
    for i in 0..200 {
        batch
            .put(format!("p{i:04}").as_bytes(), &value)
            .expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");

    // The foreground reads a key of the *fixture's* table, which this
    // compaction does not touch — a key of the compacted table would be in a
    // different file afterwards, and missing for that reason rather than for
    // having been evicted.
    db.get(b"a0", Comparison::Equal).await.expect("get");
    db.get(b"a0", Comparison::Equal).await.expect("get");
    let before = lookups(reader(), &tag, "block");
    assert!(before["hit"] > 0, "the second read hit: {before:?}");
    let held = by_attr(reader(), &cache_tag, "roble.cache.usage", "roble.cache")["block"];
    assert!(held > 0, "and the cache holds it");

    db.compact(0, &[table]).await.expect("compact");

    // Not one of the compaction's block reads was counted as a lookup, and
    // not one was left behind.
    let after = lookups(reader(), &tag, "block");
    assert_eq!(
        after, before,
        "a compaction's block reads are not the foreground's lookups: {after:?}"
    );
    let still = by_attr(reader(), &cache_tag, "roble.cache.usage", "roble.cache")["block"];
    assert!(
        still <= held,
        "a compaction left {} bytes more in the cache than it found",
        still - held
    );
    let evicted = by_attr(reader(), &cache_tag, "roble.cache.evictions", "roble.cache");
    assert_eq!(
        evicted.get("block").copied(),
        Some(0),
        "and evicted nothing to make room for them: {evicted:?}"
    );

    // And the foreground's key is still a hit, which is the whole point:
    // 40 KiB of compaction went past a 16 KiB cache without disturbing it.
    db.get(b"a0", Comparison::Equal).await.expect("get");
    let last = lookups(reader(), &tag, "block");
    assert!(
        last["hit"] > after["hit"],
        "the foreground's block should have survived the compaction: {last:?} \
         against {after:?}"
    );
}

#[compio::test]
async fn a_compaction_still_takes_what_the_cache_already_has() {
    // Discarding is about what a compaction *leaves*, not what it takes: a
    // block already in the cache is free, and reading it from there costs the
    // foreground nothing. So a compaction over blocks the foreground just
    // read should go to the disk less than one over cold blocks.
    let (options, tag) = metered("warm-compaction");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options).await.expect("open");

    let value = vec![b'v'; 200];
    let mut batch = Batch::new();
    for i in 0..40 {
        batch
            .put(format!("w{i:04}").as_bytes(), &value)
            .expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    let table = db.flush().await.expect("flush").expect("a table");

    // Walk the whole table, so every one of its blocks is in the cache.
    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    while scan.is_valid() {
        scan.next().await.expect("next");
    }
    drop(scan);

    let before = by_attr(reader(), &tag, "roble.disk.io", "disk.io.direction")["read"];
    db.compact(0, &[table]).await.expect("compact");
    let after = by_attr(reader(), &tag, "roble.disk.io", "disk.io.direction")["read"];

    let from_disk = after - before;
    assert!(
        from_disk < 40 * 200,
        "the compaction read {from_disk} bytes from the disk for a table whose \
         blocks were all in the cache"
    );
}

#[compio::test]
async fn a_small_cache_reports_what_it_evicted() {
    let cache_tag = KeyValue::new("case", "evictions");
    // Room for a handful of the fixture's 128-byte blocks, so a walk over
    // the whole database has to push some of them out again.
    let caches: Caches<SimFs> = Caches::new(DEFAULT_OPEN_TABLES, 1024)
        .meter_provider(&METRICS.0, std::slice::from_ref(&cache_tag));

    let (options, _) = metered("eviction-store");
    let (env, dir) = load("db-reads").await;
    let db = roble::open_with(&env, &dir, options.caches(caches))
        .await
        .expect("open");

    let mut scan = db.scan().await.expect("scan");
    scan.first().await.expect("first");
    while scan.is_valid() {
        scan.next().await.expect("next");
    }
    drop(scan);

    let evicted = by_attr(reader(), &cache_tag, "roble.cache.evictions", "roble.cache");
    assert!(
        evicted.get("block").copied().unwrap_or(0) > 0,
        "a 4 KiB block cache walked over a whole database evicts: {evicted:?}"
    );
}

#[compio::test]
async fn the_records_estimate_climbs_to_what_the_store_holds() {
    let (options, tag) = metered("records");
    let (env, dir) = load("db-twin-a").await;
    let db = roble::open_with(&env, &dir, options).await.expect("open");

    // Nothing opened yet, so nothing is known: the estimate starts at what
    // the memtable holds, which is nothing.
    assert_eq!(
        collected(reader(), &tag)
            .get("roble.records.estimate")
            .copied(),
        Some(0),
        "no table has been read, so none has been counted"
    );

    // Writing is exact at once.
    let mut batch = Batch::new();
    for i in 0..9 {
        batch.put(format!("r{i}").as_bytes(), b"v").expect("put");
    }
    db.write(&mut batch, true).await.expect("write");
    assert_eq!(
        collected(reader(), &tag)
            .get("roble.records.estimate")
            .copied(),
        Some(9),
        "the memtable's entries count straight away"
    );

    // A flush knows its own output, so the estimate does not dip.
    db.flush().await.expect("flush").expect("a table");
    assert_eq!(
        collected(reader(), &tag)
            .get("roble.records.estimate")
            .copied(),
        Some(9),
        "the memtable emptied into a table the store wrote and counted"
    );

    // And reading a table the store never wrote teaches it that one too.
    db.get(b"a0", Comparison::Equal).await.expect("get");
    let both = collected(reader(), &tag)["roble.records.estimate"];
    assert!(
        both > 9,
        "the fixture's own table should have been counted once it was read: {both}"
    );

    // A compaction merges the two into one. The table that left the version
    // has to stop being counted, or every compaction would inflate the
    // estimate by what it rewrote.
    let in_l0: Vec<u64> = db
        .versions()
        .version
        .level(0)
        .iter()
        .map(|t| t.file_num)
        .collect();
    assert_eq!(in_l0.len(), 2);
    db.compact(0, &in_l0).await.expect("compact");
    let after = collected(reader(), &tag)["roble.records.estimate"];
    assert_eq!(
        after, both,
        "the merge dropped no key, so the estimate should not have moved: {after} \
         against {both}"
    );
}

#[compio::test]
async fn a_store_takes_its_amounts_back_when_it_closes() {
    // An up-down counter is a store's share of a series. A store that went
    // away still holding its levels would leave a process reporting tables
    // that no longer exist.
    let (options, tag) = metered("closing");
    let (env, dir) = load("db-twin-a").await;
    {
        let db = roble::open_with(&env, &dir, options).await.expect("open");
        let mut batch = Batch::new();
        batch.put(b"held", b"value").expect("put");
        db.write(&mut batch, true).await.expect("write");
        let levels = by_attr(reader(), &tag, "roble.level.tables", "roble.level");
        assert_eq!(levels.get("0").copied(), Some(1));
        assert!(collected(reader(), &tag)["roble.memtable.size"] > 0);
    }

    let seen = collected(reader(), &tag);
    assert_eq!(
        seen.get("roble.memtable.size").copied(),
        Some(0),
        "the closed store still holds memtable bytes: {seen:?}"
    );
    let levels = by_attr(reader(), &tag, "roble.level.tables", "roble.level");
    assert_eq!(
        levels.get("0").copied(),
        Some(0),
        "and still holds its tables: {levels:?}"
    );
}

#[compio::test]
async fn the_store_names_itself_and_the_caller_may_rename_it() {
    let (_options, _) = metered("naming");
    let (env, dir) = load("db-twin-a").await;

    let options: Options<SimFs> = Options::new().meter_provider(&METRICS.0);
    let db = roble::open_with(&env, &dir, options).await.expect("open");
    let mut batch = Batch::new();
    batch.put(b"a", b"b").expect("put");
    db.write(&mut batch, true).await.expect("write");
    drop(db);

    let by_path = KeyValue::new("roble.store", "/db");
    assert!(
        collected(reader(), &by_path).contains_key("roble.write.operations"),
        "a store should name itself by the directory it opened"
    );

    // An attribute with the same key replaces it, since the last value wins.
    let renamed = KeyValue::new("roble.store", "shard-7");
    let options: Options<SimFs> = Options::new()
        .meter_provider(&METRICS.0)
        .metric_attributes(std::slice::from_ref(&renamed));
    let db = roble::open_with(&env, &dir, options).await.expect("open");
    let mut batch = Batch::new();
    batch.put(b"c", b"d").expect("put");
    db.write(&mut batch, true).await.expect("write");

    assert!(
        collected(reader(), &renamed).contains_key("roble.write.operations"),
        "the caller's own name for the store should be the one exported"
    );
}
