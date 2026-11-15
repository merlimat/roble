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

//! One phase's line: how much it did, how fast, and what it cost.

use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

/// How a phase's line is printed.
///
/// `--tsv` exists so `run.sh` can put every engine in one table: the aligned
/// output is for reading one run, and a table across four of them has to be
/// assembled from something a script can split.
#[derive(Clone, Copy)]
pub struct Output {
    pub tsv: bool,
    pub engine: &'static str,
}

/// A phase's accumulated measurements.
pub struct Report {
    name: &'static str,
    out: Output,
    /// What the phase did: records, lookups or shards, depending on which.
    pub count: u64,
    started: Instant,
    cpu_at_start: Duration,
    latencies: Latencies,
    only_max: bool,
}

/// Every latency a phase measured, in nanoseconds, in fixed memory.
///
/// A histogram rather than the samples, for what the samples cost. A phase
/// that runs for ten seconds instead of a tenth of one would keep a hundred
/// times as many, and YCSB keeps one set for the mix and one per kind of
/// operation: as `Duration`s, A held 48 MiB of the benchmark's own inside
/// every engine's 512 MiB (`run.sh`), on a row whose last column is the
/// process's peak resident memory. Capping them was worse than it looked —
/// the cap kept the *first* million, so a long phase's quantiles described
/// its start, before compaction had anything to catch up with.
///
/// This keeps them all, in 36 KiB however many there are. Nanoseconds,
/// because a lookup the cache answers takes under a microsecond, which whole
/// microseconds printed as 0. Two significant digits, so a quantile is exact
/// below 256 ns and within 0.8% above, and it is the top of its bucket, so it
/// errs high, never low.
///
/// Not three, which is within 0.1% and takes 270 KiB, because a histogram
/// that size moved roble's peak by 28 MiB: allocated as each phase starts, it
/// shifts where mimalloc puts what roble allocates after it, and an unused
/// buffer of the same size moved the peak by exactly as much. One a seventh
/// the size did not.
pub struct Latencies(Histogram<u64>);

impl Latencies {
    /// The longest latency told apart from a longer one: an hour. Past it a
    /// latency is counted as an hour rather than refused.
    const HIGHEST_NS: u64 = 3_600 * 1_000_000_000;
    /// Significant digits; see the type.
    const DIGITS: u8 = 2;

    pub fn saw(&mut self, took: Duration) {
        let ns = u64::try_from(took.as_nanos()).unwrap_or(u64::MAX);
        self.0.saturating_record(ns);
    }

    /// The latency at quantile `q`, in microseconds to a tenth of one.
    ///
    /// `0.0` when nothing was measured, as a column of samples gave `0`.
    pub fn us(&self, q: f64) -> String {
        format!("{:.1}", self.0.value_at_quantile(q) as f64 / 1_000.0)
    }
}

impl Default for Latencies {
    fn default() -> Self {
        Latencies(
            Histogram::new_with_bounds(1, Self::HIGHEST_NS, Self::DIGITS)
                .expect("an hour in nanoseconds, to two digits, is a valid histogram"),
        )
    }
}

impl Report {
    /// One step in this many is timed, in the scan phases.
    ///
    /// A scan step costs tens to hundreds of nanoseconds and reading a clock
    /// costs the same order, so timing every step would charge the faster
    /// engine the larger share of the measurement overhead — enough, on the
    /// forward scan, to reverse which engine looks quicker. The rate comes
    /// from the phase's own wall clock, which sampling leaves alone.
    pub const SAMPLE: u64 = 64;

    pub fn header() -> String {
        format!(
            "{:<11} {:>10} {:>12} {:>9} {:>9} {:>9} {:>8} {:>9}",
            "phase", "count", "rate/s", "p50 us", "p99 us", "max us", "cpu s", "size MiB"
        )
    }

    pub fn new(name: &'static str, out: Output) -> Self {
        Report {
            name,
            out,
            count: 0,
            started: Instant::now(),
            cpu_at_start: crate::cpu_time(),
            latencies: Latencies::default(),
            only_max: false,
        }
    }

    /// Reports `max` alone, for a phase whose p50 and p99 describe the
    /// caller rather than the engine.
    ///
    /// `load`'s latency is **per commit** where its count and its rate are
    /// per record, so at `--records 10000` a p50 of six milliseconds is six
    /// milliseconds of batch size and nothing else — halve the batch and it
    /// halves. Two columns that move with a flag the reader chose are worse
    /// than no columns, because they look like measurements.
    ///
    /// `max` stays, and is the reason this is not simply dropped: a flush or
    /// a compaction landing inside a write shows up there and nowhere else.
    /// It is what made roble's write stall visible at all — 129 ms at a
    /// 4 MiB memtable, 738 ms at 64 MiB — before any phase wrote while it
    /// read.
    pub fn max_only(mut self) -> Self {
        self.only_max = true;
        self
    }

    /// Whether step `n` is one of the sampled ones.
    pub fn sampling(&self, n: u64) -> bool {
        n.is_multiple_of(Self::SAMPLE)
    }

    pub fn saw(&mut self, took: Duration) {
        self.latencies.saw(took);
    }

    /// Counts a set of latencies kept elsewhere into this phase's.
    ///
    /// For YCSB, which keeps one set per kind of operation: adding them up
    /// once at the end gives the mix's, where recording each operation into
    /// both would time the engine with two histograms' worth of work around
    /// every call — a few nanoseconds each, which on an operation that takes
    /// a microsecond is a percent or two of the rate.
    pub fn absorb(&mut self, latencies: &Latencies) {
        self.latencies
            .0
            .add(&latencies.0)
            .expect("every phase's histogram has the same bounds");
    }

    /// Prints the line, and says what the phase could not be asked to do.
    pub fn finish(self, size_bytes: u64) {
        let wall = self.started.elapsed();
        let cpu = crate::cpu_time().saturating_sub(self.cpu_at_start);
        // A dash rather than a number the reader would take for one, and
        // the same dash the aligned output and the TSV both carry so that
        // `pivot.py` sees one thing.
        let quantile = |q: f64| -> String {
            if self.only_max && q < 1.0 {
                "—".to_string()
            } else {
                self.latencies.us(q)
            }
        };
        let rate = self.count as f64 / wall.as_secs_f64().max(f64::MIN_POSITIVE);
        let size_mib = size_bytes as f64 / (1024.0 * 1024.0);
        if self.out.tsv {
            println!(
                "{}\t{}\t{}\t{:.0}\t{}\t{}\t{}\t{:.2}\t{:.1}",
                self.out.engine,
                self.name,
                self.count,
                rate,
                quantile(0.50),
                quantile(0.99),
                quantile(1.0),
                cpu.as_secs_f64(),
                size_mib,
            );
            return;
        }
        println!(
            "{:<11} {:>10} {:>12.0} {:>9} {:>9} {:>9} {:>8.2} {:>9.1}",
            self.name,
            self.count,
            rate,
            quantile(0.50),
            quantile(0.99),
            quantile(1.0),
            cpu.as_secs_f64(),
            size_mib,
        );
    }

    /// Says a phase was skipped, and why, in place of its line.
    ///
    /// A dash rather than a zero: a zero would read as "it did this and it
    /// took no time", which is the one thing it does not mean.
    pub fn skipped(name: &str, why: &str, out: Output) {
        if out.tsv {
            // A phase that did not happen, which a reader of the table has to
            // be able to tell from one that happened instantly.
            println!("{}\t{}\tskipped\t{}", out.engine, name, why);
            return;
        }
        println!("{name:<11} {:>10} — {why}", "-");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(ns: impl IntoIterator<Item = u64>) -> Latencies {
        let mut l = Latencies::default();
        for n in ns {
            l.saw(Duration::from_nanos(n));
        }
        l
    }

    /// `us(q)` as a number, checked to be `want` or at most 0.8% above it.
    fn near(l: &Latencies, q: f64, want: f64) {
        let got: f64 = l.us(q).parse().unwrap();
        assert!(
            got >= want && got <= want * 1.008,
            "q {q}: {got}, not {want}"
        );
    }

    #[test]
    fn a_quantile_is_the_latency_at_its_rank() {
        // One to a hundred microseconds: the median is the fiftieth.
        let l = of((1..=100).map(|us| us * 1_000));
        near(&l, 0.50, 50.0);
        near(&l, 0.99, 99.0);
        near(&l, 1.0, 100.0);
        assert_eq!(Latencies::default().us(0.50), "0.0");
    }

    #[test]
    fn a_phase_absorbing_two_sets_reads_as_one() {
        // YCSB's mix line, from its per-operation sets.
        let out = Output {
            tsv: true,
            engine: "test",
        };
        let mut report = Report::new("ycsb-A", out);
        report.absorb(&of((1..=50).map(|us| us * 1_000)));
        report.absorb(&of((51..=100).map(|us| us * 1_000)));
        near(&report.latencies, 0.50, 50.0);
        near(&report.latencies, 1.0, 100.0);
    }

    #[test]
    fn a_latency_under_a_microsecond_or_two_keeps_its_tenths() {
        // Whole microseconds printed these as 0, 1 and 1.
        assert_eq!(of([800]).us(0.50), "0.8");
        assert_eq!(of([1_400]).us(0.50), "1.4");
        assert_eq!(of([1_900]).us(0.50), "1.9");
    }

    #[test]
    fn a_latency_past_an_hour_counts_as_an_hour() {
        // Neither refused, which would panic the phase, nor wrapped, which
        // would print a stall as a fast operation.
        let mut l = Latencies::default();
        l.saw(Duration::MAX);
        let max: f64 = l.us(1.0).parse().unwrap();
        assert!(max >= 3_600e6, "{max}");
    }
}
