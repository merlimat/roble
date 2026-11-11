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

use roble::sim::{SimEnv, SimFs};
use roble::{Comparison, Disk, Fs, Options, WriteBuf};
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
        out.insert(at, value);
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
    let db = roble::open_with(&env, &dir, options).await.expect("open");

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
