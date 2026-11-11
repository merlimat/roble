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

//! `roble`: inspects Pebble's files from the command line.
//!
//! Deliberately a thin wrapper. Everything structural comes from
//! `roble-format`, whose output is pinned byte for byte against Go Pebble; the
//! only thing this crate decides is presentation, and presentation is the one
//! part of M1 with no Go counterpart to check against (DESIGN.md §13). Keeping
//! the split sharp is what stops an unverified layer from growing logic of its
//! own: if a question can be answered by the format crate, this file must not
//! answer it itself.

use std::fmt::Write as _;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use roble_format::block::Compression;
use roble_format::manifest::VersionEdit;
use roble_format::record::RecordReader;
use roble_format::sst::Table;

const AFTER_HELP: &str = r"Both commands read Pebble's own formats, pinned at the version DESIGN.md §3
names, and refuse — rather than partly read — anything above that pin. A refusal
names what it refused: a table with range keys or a virtual sstable is valid
Pebble that Oxia never writes, so roble declines it instead of reading past a
structure it does not understand.

Keys and values print as hex, because they are bytes and a dump is compared
rather than read.

Exit status: 0 on success, 2 on errors.";

/// Inspects the files of a Pebble or roble database.
#[derive(Parser)]
#[command(version, after_help = AFTER_HELP)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Reads a table file
    Sst {
        #[command(subcommand)]
        command: SstCommand,
    },
    /// Reads a MANIFEST file
    Manifest {
        #[command(subcommand)]
        command: ManifestCommand,
    },
    /// Prints a script that completes roble's commands and options in a shell
    ///
    /// For bash, save it where bash-completion looks for it:
    ///
    ///     roble completions bash > ~/.local/share/bash-completion/completions/roble
    ///
    /// For zsh, save it as _roble in a directory of $fpath, and for fish, as
    /// ~/.config/fish/completions/roble.fish.
    #[command(override_usage = "roble completions <SHELL>")]
    Completions { shell: Shell },
}

#[derive(Subcommand)]
enum SstCommand {
    /// Prints every point entry and range tombstone in a table
    Dump {
        /// Also print the table's structure: blocks, compression, properties
        #[arg(short = 'l', long)]
        layout: bool,
        /// The table file
        #[arg(value_name = "PATH")]
        path: PathBuf,
    },
}

#[derive(Subcommand)]
enum ManifestCommand {
    /// Prints every version edit in a MANIFEST
    Dump {
        /// The MANIFEST file
        #[arg(value_name = "PATH")]
        path: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Sst {
            command: SstCommand::Dump { layout, path },
        } => sst_dump(&path, layout).and_then(write_out),
        Command::Manifest {
            command: ManifestCommand::Dump { path },
        } => manifest_dump(&path).and_then(write_out),
        Command::Completions { shell } => completions(shell),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        // The reader went away, as in `roble sst dump big.sst | head`: a dump
        // is piped more often than it is read whole, so stop quietly.
        Err(Error::Io(e)) if e.kind() == io::ErrorKind::BrokenPipe => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("roble: {e}");
            ExitCode::from(2)
        }
    }
}

/// What the command can fail with.
enum Error {
    /// Reading or writing a file.
    Io(io::Error),
    /// Anything the format crate or this command refused.
    Message(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Untested, with a reason rather than a test (§12). An `Error::Io`
            // only arises writing the output — everything read is reported
            // with its path as a `Message` — and the one output failure a
            // test can arrange is a closed reader, which `main` answers
            // above without printing. Failing a write some other way means
            // an output whose behaviour differs between platforms, and a
            // test that passes on one and not the other is worse than this
            // line being unrun.
            Error::Io(e) => write!(f, "{e}"),
            Error::Message(m) => write!(f, "{m}"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

type Result<T> = std::result::Result<T, Error>;

fn write_out(text: String) -> Result<()> {
    // Buffered, so that a dump of a large table is one write per block rather
    // than one per line — and so a broken pipe surfaces here rather than
    // part-way through a line.
    let mut out = BufWriter::new(io::stdout().lock());
    out.write_all(text.as_bytes())?;
    out.flush()?;
    Ok(())
}

fn completions(shell: Shell) -> Result<()> {
    let mut command = Cli::command();
    let name = command.get_name().to_string();
    clap_complete::generate(shell, &mut command, name, &mut io::stdout());
    Ok(())
}

fn sst_dump(path: &Path, layout: bool) -> Result<String> {
    let bytes = read(path)?;
    let mut out = String::new();
    if layout {
        out.push_str(&describe_layout(&bytes, path)?);
        out.push('\n');
    }
    // The dump itself is the format crate's, unchanged: it is the part pinned
    // against Go Pebble, and reformatting it here would put an unverified
    // layer between the file and the output.
    out.push_str(&roble_format::dump_table(&bytes).map_err(|e| at(path, &e))?);
    Ok(out)
}

/// The structural summary `--layout` adds.
///
/// Every number here is read through `roble-format`; this function chooses the
/// words and nothing else.
fn describe_layout(bytes: &[u8], path: &Path) -> Result<String> {
    let table = Table::open(bytes).map_err(|e| at(path, &e))?;
    let footer = *table.footer();
    let handles = table.data_block_handles().map_err(|e| at(path, &e))?;

    let mut counts: Vec<(Compression, usize)> = Vec::new();
    for h in &handles {
        let c = table.block_compression(*h).map_err(|e| at(path, &e))?;
        match counts.iter_mut().find(|(k, _)| *k == c) {
            Some((_, n)) => *n += 1,
            None => counts.push((c, 1)),
        }
    }

    let mut out = String::new();
    let _ = writeln!(out, "file {}", path.display());
    let _ = writeln!(out, "size {}", bytes.len());
    let _ = writeln!(out, "table-format {}", footer.format.name());
    let _ = writeln!(out, "checksum {}", footer.checksum.name());
    let _ = writeln!(
        out,
        "index {}",
        if table.has_two_level_index() {
            "two-level"
        } else {
            "single-level"
        }
    );
    let _ = write!(out, "data-blocks {}", handles.len());
    for (c, n) in &counts {
        let _ = write!(out, " {}={n}", c.name());
    }
    out.push('\n');

    let meta = table.metaindex();
    let mut present: Vec<&str> = Vec::new();
    if meta.properties.is_some() {
        present.push("properties");
    }
    if meta.filter.is_some() {
        present.push("filter");
    }
    if meta.range_del.is_some() {
        present.push("range-del");
    }
    if meta.value_index.is_some() {
        present.push("value-index");
    }
    let _ = writeln!(out, "metaindex {}", present.join(" "));

    let value_blocks = table.value_blocks().map_err(|e| at(path, &e))?;
    if !value_blocks.is_empty() {
        let _ = writeln!(out, "value-blocks {}", value_blocks.len());
    }

    for (name, value) in table.properties().iter() {
        // Properties are name/value pairs whose types vary by name, so they
        // are shown as the format crate holds them rather than guessed at.
        let _ = writeln!(
            out,
            "property {} {}",
            String::from_utf8_lossy(name),
            printable(value)
        );
    }
    Ok(out)
}

fn manifest_dump(path: &Path) -> Result<String> {
    let bytes = read(path)?;
    let mut out = String::new();
    let _ = writeln!(out, "file {}", path.display());
    let _ = writeln!(out, "size {}", bytes.len());

    // A MANIFEST's records are legacy-framed and carry no log number.
    let mut reader = RecordReader::new(&bytes, 0);
    let mut edits = Vec::new();
    while let Some(record) = reader.next() {
        let record = record.map_err(|e| {
            Error::Message(format!("{}: record {}: {e}", path.display(), edits.len()))
        })?;
        let edit = VersionEdit::decode(&record).map_err(|e| {
            Error::Message(format!(
                "{}: version edit {}: {e}",
                path.display(),
                edits.len()
            ))
        })?;
        edits.push(edit);
    }
    let _ = writeln!(out, "edits {}", edits.len());

    for (i, edit) in edits.iter().enumerate() {
        let _ = writeln!(out, "edit {i}");
        if let Some(name) = &edit.comparer_name {
            let _ = writeln!(out, "  comparer {name}");
        }
        if let Some(n) = edit.min_unflushed_log_num {
            let _ = writeln!(out, "  min-unflushed-log {n}");
        }
        if let Some(n) = edit.obsolete_prev_log_num {
            let _ = writeln!(out, "  prev-log {n}");
        }
        if let Some(n) = edit.next_file_num {
            let _ = writeln!(out, "  next-file-num {n}");
        }
        if let Some(n) = edit.last_seq_num {
            let _ = writeln!(out, "  last-seq-num {n}");
        }
        for (level, file_num) in &edit.deleted_tables {
            let _ = writeln!(out, "  deleted L{level} {file_num}");
        }
        for t in &edit.new_tables {
            let _ = write!(
                out,
                "  added L{} {} size={} seq={}..{}",
                t.level, t.file_num, t.size, t.smallest_seq_num, t.largest_seq_num
            );
            if t.creation_time != 0 {
                let _ = write!(out, " created={}", t.creation_time);
            }
            if t.marked_for_compaction {
                let _ = write!(out, " marked-for-compaction");
            }
            out.push('\n');
            let _ = writeln!(out, "    smallest {}", internal_key(&t.smallest));
            let _ = writeln!(out, "    largest  {}", internal_key(&t.largest));
        }
    }
    Ok(out)
}

/// Shows an encoded internal key as the format crate reads it.
fn internal_key(encoded: &[u8]) -> String {
    match roble_format::InternalKey::decode(encoded) {
        Ok(k) => format!("{} #{} {}", printable(k.user_key), k.seq_num, k.kind.name()),
        // A bound roble cannot decode is shown rather than hidden: it is the
        // interesting case when something is wrong.
        Err(e) => format!("<undecodable: {e}> {}", printable(encoded)),
    }
}

/// Shows a value as text when it is text, and as hex when it is not.
fn printable(value: &[u8]) -> String {
    match std::str::from_utf8(value) {
        Ok(s) if !s.is_empty() && s.chars().all(|c| !c.is_control()) => s.to_string(),
        _ => value.iter().map(|b| format!("{b:02x}")).collect(),
    }
}

fn at(path: &Path, e: &roble_format::Error) -> Error {
    Error::Message(format!("{}: {e}", path.display()))
}

fn read(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| Error::Message(format!("{}: {e}", path.display())))
}
