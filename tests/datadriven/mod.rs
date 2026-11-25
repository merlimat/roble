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

//! Pebble's test data is written in the format of cockroachdb/datadriven:
//! a command, its input lines, `----`, and the output under it. This reads
//! it, for each test that runs one of Pebble's files.

/// One command of a datadriven file: the command line, its input lines, and
/// the output written under it.
pub struct Case {
    pub line: usize,
    pub command: String,
    pub input: Vec<String>,
    pub expected: String,
}

/// Splits a file in the format of cockroachdb/datadriven, the one Pebble's
/// test data is written in. An output runs to the first blank line, or,
/// when it opens with a second `----`, to a closing `----` pair.
pub fn cases(text: &str) -> Vec<Case> {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i].trim_end();
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }
        let at = i + 1;
        let command = line.to_string();
        i += 1;
        let mut input = Vec::new();
        while i < lines.len() && lines[i] != "----" {
            input.push(lines[i].to_string());
            i += 1;
        }
        i += 1;
        let mut expected = Vec::new();
        if lines.get(i) == Some(&"----") {
            i += 1;
            while i + 1 < lines.len() && !(lines[i] == "----" && lines[i + 1] == "----") {
                expected.push(lines[i]);
                i += 1;
            }
            i += 2;
        } else {
            while i < lines.len() && !lines[i].trim().is_empty() {
                expected.push(lines[i]);
                i += 1;
            }
        }
        out.push(Case {
            line: at,
            command,
            input,
            expected: expected.join("\n"),
        });
    }
    out
}
