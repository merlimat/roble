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

/// A phase's accumulated measurements.
pub struct Report {
    name: &'static str,
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

    pub fn new(name: &'static str) -> Self {
        Report {
            name,
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

    pub fn saw(&mut self, took: Duration) {
        self.latencies.push(took);
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
        println!(
            "{:<11} {:>10} {:>12.0} {:>9} {:>9} {:>9} {:>8.2} {:>9.1}",
            self.name,
            self.count,
            self.count as f64 / wall.as_secs_f64().max(f64::MIN_POSITIVE),
            us(0.50),
            us(0.99),
            us(1.0),
            cpu.as_secs_f64(),
            size_bytes as f64 / (1024.0 * 1024.0),
        );
    }

    /// Says a phase was skipped, and why, in place of its line.
    ///
    /// A dash rather than a zero: a zero would read as "it did this and it
    /// took no time", which is the one thing it does not mean.
    pub fn skipped(name: &str, why: &str) {
        println!("{name:<11} {:>10} — {why}", "-");
    }
}
