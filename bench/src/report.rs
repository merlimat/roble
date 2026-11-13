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
    latencies: Vec<Duration>,
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
            latencies: Vec::new(),
        }
    }

    /// Whether step `n` is one of the sampled ones.
    pub fn sampling(&self, n: u64) -> bool {
        n.is_multiple_of(Self::SAMPLE)
    }

    /// How many latency samples a phase keeps.
    ///
    /// A phase that runs for ten seconds instead of a tenth of one keeps a
    /// hundred times the samples, and a scan sampling one step in 64 at forty
    /// million steps a second would reach seven million of them — over a
    /// hundred megabytes, on a row whose last column is the process's peak
    /// resident memory. The cap keeps that column measuring the engine.
    const MAX_SAMPLES: usize = 1_000_000;

    pub fn saw(&mut self, took: Duration) {
        if self.latencies.len() < Self::MAX_SAMPLES {
            self.latencies.push(took);
        }
    }

    /// Prints the line, and says what the phase could not be asked to do.
    pub fn finish(mut self, size_bytes: u64) {
        let wall = self.started.elapsed();
        let cpu = crate::cpu_time().saturating_sub(self.cpu_at_start);
        self.latencies.sort_unstable();
        let us = |q: f64| -> u128 {
            if self.latencies.is_empty() {
                return 0;
            }
            let i = ((self.latencies.len() - 1) as f64 * q).round() as usize;
            self.latencies[i].as_micros()
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
                us(0.50),
                us(0.99),
                us(1.0),
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
            us(0.50),
            us(0.99),
            us(1.0),
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
