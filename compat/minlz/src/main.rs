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

//! Decodes fixtures produced by Go's `github.com/minio/minlz` (the version
//! Pebble v2.1.7 depends on) using the Rust `minlz` crate's MinLZ codec,
//! exercising the three calls roble needs: `decompressed_len`, `decompress`
//! and `decompress_into`.
use minlz::minlz::{MAX_BLOCK_SIZE, decompress, decompress_into, decompressed_len};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;

const FIXTURES: &[&str] = &[
    "empty",
    "one_byte",
    "tiny_hello",
    "repetitive_16k",
    "index_block_like_16k",
    "bloom_like_16k",
    "random_16k",
    "repetitive_64k",
    "records_64k",
    "random_64k",
    "compressible_1m",
    "max_block_size",
];

const LEVELS: &[&str] = &["fastest", "balanced"];

fn main() {
    let dir = std::env::args()
        .nth(1)
        .expect("usage: rust-decode <fixtures-dir>");
    let dir = Path::new(&dir);

    assert_eq!(
        MAX_BLOCK_SIZE,
        8 << 20,
        "MAX_BLOCK_SIZE disagrees with Go's minlz.MaxBlockSize"
    );
    println!("crate MAX_BLOCK_SIZE = {} (Go: 8388608)\n", MAX_BLOCK_SIZE);

    println!(
        "{:<22} {:<9} {:>9} {:>9}  result",
        "fixture", "level", "raw", "encoded"
    );
    let mut failures = 0usize;
    let mut checked = 0usize;

    for name in FIXTURES {
        let raw = std::fs::read(dir.join(format!("{name}.raw"))).expect("read raw");
        for level in LEVELS {
            let enc = std::fs::read(dir.join(format!("{name}.{level}.minlz"))).expect("read enc");
            let mut problems: Vec<String> = Vec::new();

            // 1. decompressed_len must agree with the original length.
            match decompressed_len(&enc) {
                Ok(n) if n == raw.len() => {}
                Ok(n) => problems.push(format!("decompressed_len {n} != {}", raw.len())),
                Err(e) => problems.push(format!("decompressed_len: {e}")),
            }

            // 2. decompress must reproduce the original bytes exactly.
            match decompress(&enc) {
                Ok(out) if out == raw => {}
                Ok(out) => problems.push(format!(
                    "decompress mismatch: got {} bytes, want {}{}",
                    out.len(),
                    raw.len(),
                    first_diff(&out, &raw)
                )),
                Err(e) => problems.push(format!("decompress: {e}")),
            }

            // 3. decompress_into an exactly-sized buffer: roble's frame path.
            let mut buf = vec![0u8; raw.len()];
            match decompress_into(&mut buf, &enc) {
                Ok(n) if n == raw.len() && buf == raw => {}
                Ok(n) => problems.push(format!(
                    "decompress_into wrote {n}, buffer {}",
                    if buf == raw { "matches" } else { "differs" }
                )),
                Err(e) => problems.push(format!("decompress_into: {e}")),
            }

            checked += 1;
            if problems.is_empty() {
                println!(
                    "{:<22} {:<9} {:>9} {:>9}  ok",
                    name,
                    level,
                    raw.len(),
                    enc.len()
                );
            } else {
                failures += 1;
                println!(
                    "{:<22} {:<9} {:>9} {:>9}  FAIL: {}",
                    name,
                    level,
                    raw.len(),
                    enc.len(),
                    problems.join("; ")
                );
            }
        }
    }

    // Malformed input must error, never panic: a corrupt block has to surface as
    // an error on roble's read path, not abort the process.
    println!("\nmalformed-input pass (truncations and single-byte flips):");
    let mut panics = 0usize;
    let mut cases = 0usize;
    for name in [
        "index_block_like_16k",
        "repetitive_64k",
        "random_16k",
        "tiny_hello",
    ] {
        let enc = std::fs::read(dir.join(format!("{name}.fastest.minlz"))).expect("read enc");
        for cut in 0..enc.len().min(64) {
            cases += 1;
            if run_panics(&enc[..cut]) {
                panics += 1;
            }
        }
        for i in 0..enc.len().min(64) {
            for xor in [0x01u8, 0x80, 0xff] {
                let mut m = enc.clone();
                m[i] ^= xor;
                cases += 1;
                if run_panics(&m) {
                    panics += 1;
                }
            }
        }
    }
    println!("  {cases} malformed inputs, {panics} panics");

    println!(
        "\n{checked} fixture/level pairs checked, {failures} failures, {panics} panics on malformed input"
    );
    if failures > 0 || panics > 0 {
        std::process::exit(1);
    }
    println!("VERDICT: the Rust crate decodes Go minio/minlz block output byte-for-byte");
}

// Returns true if any of the three calls panicked on this input.
fn run_panics(src: &[u8]) -> bool {
    catch_unwind(AssertUnwindSafe(|| {
        let _ = decompressed_len(src);
        if let Ok(n) = decompressed_len(src)
            && n <= MAX_BLOCK_SIZE
        {
            let mut buf = vec![0u8; n];
            let _ = decompress_into(&mut buf, src);
        }
        let _ = decompress(src);
    }))
    .is_err()
}

fn first_diff(a: &[u8], b: &[u8]) -> String {
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        if x != y {
            return format!(", first differing byte at {i}: {x:#04x} != {y:#04x}");
        }
    }
    String::new()
}
