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

//! The commands that open a database: they put, get, scan and delete its
//! entries, and say how its tables are laid out across the levels.
//!
//! The same commands encina's command offers, with `tables` in place of its
//! `pages`. Every answer comes from the engine; this file reads it and chooses
//! the words.

use std::io::{self, BufWriter, Write};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::Path;
use std::process::ExitCode;

use roble::{Comparison, Db, DefaultEnv, FileFs, Options};
use roble_format::batch::Batch;
use roble_format::files::{self, MARKER_MANIFEST};

use crate::{Error, Result};

/// How many times in a row a read opens the database again because the
/// process writing it retired a file the read needed, as encina's command
/// does. A scan that prints something has made progress, and starts counting
/// again.
const MAX_STALE: u32 = 10;

/// What a database command does.
pub enum DbCommand {
    Put {
        key: Box<[u8]>,
        value: Box<[u8]>,
    },
    Delete {
        key: Box<[u8]>,
    },
    DeleteRange {
        start: Box<[u8]>,
        end: Box<[u8]>,
    },
    Get {
        key: Box<[u8]>,
        hexdump: bool,
    },
    Scan {
        start: Option<Box<[u8]>>,
        end: Option<Box<[u8]>>,
        hexdump: bool,
    },
    Stats,
    Tables,
}

/// Runs `command` against the database in `dir`.
pub fn run(dir: &Path, command: DbCommand) -> Result<ExitCode> {
    let runtime = compio::runtime::Runtime::new()?;
    runtime.block_on(run_async(dir, command))
}

async fn run_async(dir: &Path, command: DbCommand) -> Result<ExitCode> {
    let mut out = BufWriter::new(io::stdout().lock());
    match command {
        DbCommand::Put { key, value } => {
            let db = open(dir, true).await?;
            let mut batch = Batch::new();
            batch.put(&key, &value).at(dir)?;
            write(&db, dir, &mut batch).await?;
        }
        DbCommand::Delete { key } => {
            let db = open(dir, false).await?;
            let mut batch = Batch::new();
            batch.delete(&key).at(dir)?;
            write(&db, dir, &mut batch).await?;
        }
        DbCommand::DeleteRange { start, end } => {
            let db = open(dir, false).await?;
            let mut batch = Batch::new();
            batch.delete_range(&start, &end).at(dir)?;
            write(&db, dir, &mut batch).await?;
        }
        DbCommand::Get { key, hexdump } => {
            let found = reading(dir, async |db| {
                db.get(&key, Comparison::Equal).await.at(dir)
            })
            .await?;
            let Some(found) = found else {
                eprintln!("roble: key not found");
                return Ok(ExitCode::FAILURE);
            };
            if hexdump {
                write_hexdump(&mut out, "", &found.value)?;
            } else {
                write_text(&mut out, &found.value)?;
                out.write_all(b"\n")?;
            }
        }
        DbCommand::Scan {
            start,
            end,
            hexdump,
        } => {
            // Where a scan that went stale starts again: the least key after
            // the last one it printed, so that nothing is printed twice.
            let mut from: Option<Vec<u8>> = None;
            let mut stale = 0;
            loop {
                let scanned: Result<()> = async {
                    let db = open_read_only(dir).await?;
                    let bounds = (
                        start.as_deref().map_or(Unbounded, Included),
                        end.as_deref().map_or(Unbounded, Excluded),
                    );
                    let mut scan = db.range(bounds).at(dir)?;
                    match &from {
                        Some(key) => scan.seek_ge(key).await,
                        None => scan.first().await,
                    }
                    .at(dir)?;
                    while scan.is_valid() {
                        let (key, value) = (scan.key(), scan.value());
                        if hexdump {
                            write_hexdump(&mut out, "key   ", key)?;
                            write_hexdump(&mut out, "value ", value)?;
                        } else {
                            write_text(&mut out, key)?;
                            out.write_all(b"\t")?;
                            write_text(&mut out, value)?;
                            out.write_all(b"\n")?;
                        }
                        let mut next = key.to_vec();
                        next.push(0);
                        from = Some(next);
                        stale = 0;
                        scan.next().await.at(dir)?;
                    }
                    Ok(())
                }
                .await;
                match scanned {
                    Err(Error::Stale(_)) if stale < MAX_STALE => stale += 1,
                    scanned => break scanned?,
                }
            }
        }
        DbCommand::Stats => {
            // Into a buffer, so that a read that goes stale and is tried
            // again prints its answer once.
            let text = reading(dir, async |db| {
                let mut text = Vec::new();
                stats(db, dir, &mut text).await?;
                Ok(text)
            })
            .await?;
            out.write_all(&text)?;
        }
        DbCommand::Tables => {
            let text = reading(dir, async |db| {
                let mut text = Vec::new();
                tables(db, dir, &mut text)?;
                Ok(text)
            })
            .await?;
            out.write_all(&text)?;
        }
    }
    out.flush()?;
    Ok(ExitCode::SUCCESS)
}

/// Prints the table `tables` gives: a row for each table the version names.
///
/// Into a buffer, which a write cannot fail on, so what is written is not
/// checked here. The buffer goes to the output once the read is done.
fn tables(db: &Db<FileFs>, dir: &Path, out: &mut Vec<u8>) -> Result<()> {
    let versions = db.versions();
    let _ = writeln!(
        out,
        "level\tfile\tsize\tseqs\tformat\tcompression\tentries\tdeletions\t\
         range-dels\traw\tratio\tsmallest\tlargest"
    );
    for (level, tables) in versions.version.levels() {
        for t in tables {
            let file = files::table_filename(t.file_num);
            let p = properties_of(&dir.join(&file))?;
            let _ = write!(
                out,
                "L{level}\t{file}\t{}\t{}..{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t",
                t.size,
                t.smallest_seq_num,
                t.largest_seq_num,
                p.format,
                p.compression,
                shown(p.entries),
                shown(p.deletions),
                shown(p.range_deletions),
                shown(p.raw()),
                p.ratio()
            );
            let _ = write_text(out, t.smallest_user_key().at(dir)?);
            let _ = out.write_all(b"\t");
            let _ = write_text(out, t.largest_user_key().at(dir)?);
            let _ = out.write_all(b"\n");
        }
    }
    Ok(())
}

/// Prints the summary `stats` gives, then the table of levels, into a buffer
/// as [`tables`] does.
async fn stats(db: &Db<FileFs>, dir: &Path, out: &mut Vec<u8>) -> Result<()> {
    let disk = db.disk_usage().await.at(dir)?;
    let (sublevels, levels, debt) = {
        let versions = db.versions();
        let version = &versions.version;
        let sublevels = version.l0_sublevels().at(dir)?;
        let mut levels: Vec<Level> = Vec::new();
        for (level, tables) in version.levels() {
            let mut row = Level {
                level,
                tables: tables.len(),
                ..Level::default()
            };
            for t in tables {
                let p = properties_of(&dir.join(files::table_filename(t.file_num)))?;
                row.bytes += t.size;
                row.entries += p.entries.unwrap_or(0);
                row.deletions += p.deletions.unwrap_or(0);
                row.raw += p.raw().unwrap_or(0);
                row.data += p.data.unwrap_or(0);
            }
            levels.push(row);
        }
        (sublevels, levels, db.compaction_debt())
    };
    let table_count: usize = levels.iter().map(|l| l.tables).sum();
    let table_bytes: u64 = levels.iter().map(|l| l.bytes).sum();
    let entries: u64 = levels.iter().map(|l| l.entries).sum();
    let wal = db
        .current_log()
        .map_or_else(|| "none".to_string(), files::log_filename);
    let next = match db.pick() {
        Some(p) => format!("L{}, {} tables, {:.2}x", p.level, p.tables.len(), p.score()),
        None => "none".to_string(),
    };

    let _ = writeln!(out, "format version  {:>24}", db.format_version());
    let _ = writeln!(out, "manifest        {:>24}", db.manifest_filename());
    let _ = writeln!(out, "wal             {wal:>24}");
    let _ = writeln!(out, "unflushed bytes {:>24}", db.unflushed_bytes());
    let _ = writeln!(out, "disk usage      {disk:>24}");
    let _ = writeln!(out, "tables          {table_count:>24}");
    let _ = writeln!(out, "table bytes     {table_bytes:>24}");
    let _ = writeln!(out, "table entries   {entries:>24}");
    let _ = writeln!(out, "l0 sublevels    {sublevels:>24}");
    let _ = writeln!(out, "compaction debt {debt:>24}");
    let _ = writeln!(out, "next compaction {next:>24}");

    let _ = writeln!(
        out,
        "\nlevel     tables            bytes    share      entries    deletions    ratio"
    );
    for l in levels {
        let share = if table_bytes == 0 {
            0.0
        } else {
            100.0 * l.bytes as f64 / table_bytes as f64
        };
        let ratio = ratio(Some(l.raw), Some(l.data));
        let _ = writeln!(
            out,
            "{:>5} {:>10} {:>16} {share:>7.1}% {:>12} {:>12} {ratio:>8}",
            l.level, l.tables, l.bytes, l.entries, l.deletions
        );
    }
    Ok(())
}

/// One level's row of `stats`, added up over its tables.
#[derive(Default)]
struct Level {
    level: u32,
    tables: usize,
    bytes: u64,
    entries: u64,
    deletions: u64,
    raw: u64,
    data: u64,
}

/// What a table's footer and properties block say about it.
struct TableProperties {
    format: &'static str,
    /// The compression the writer recorded, which is not a promise about
    /// every block: Pebble stores a block that would not shrink as it is.
    compression: String,
    entries: Option<u64>,
    deletions: Option<u64>,
    range_deletions: Option<u64>,
    raw_keys: Option<u64>,
    raw_values: Option<u64>,
    /// The bytes its data blocks take, as stored.
    data: Option<u64>,
}

impl TableProperties {
    /// The bytes of its keys and values, before the blocks hold them.
    fn raw(&self) -> Option<u64> {
        Some(self.raw_keys? + self.raw_values?)
    }

    fn ratio(&self) -> String {
        ratio(self.raw(), self.data)
    }
}

/// How many times smaller the data blocks are than the keys and values they
/// hold: what compression, and the sharing of key prefixes, saved.
fn ratio(raw: Option<u64>, data: Option<u64>) -> String {
    match (raw, data) {
        (Some(raw), Some(data)) if data > 0 => format!("{:.2}x", raw as f64 / data as f64),
        _ => "-".to_string(),
    }
}

fn shown(n: Option<u64>) -> String {
    n.map_or_else(|| "-".to_string(), |n| n.to_string())
}

/// Reads a table's footer and properties block, and nothing else of it.
///
/// Three reads, wherever the table is large or small: the footer at its end,
/// the metaindex it points at, and the properties block the metaindex names.
/// Every structure is decoded by the format crate; this function only reads
/// the bytes it is told to.
fn properties_of(path: &Path) -> Result<TableProperties> {
    use roble_format::block::{Block, decode_fetched};
    use roble_format::sst::{Metaindex, Properties};
    use roble_format::{BlockHandle, ChecksumType, FOOTER_LEN, Footer};
    use std::io::{Read, Seek, SeekFrom};

    // A table the version names and the directory no longer holds is one the
    // process writing the database retired after this one read the version.
    let file = std::fs::File::open(path).map_err(|e| match e.kind() {
        io::ErrorKind::NotFound => Error::Stale(format!(
            "{}: stale: {e}: the process writing the database retired it",
            path.display()
        )),
        _ => Error::Message(format!("{}: {e}", path.display())),
    })?;
    let read_at = |offset: u64, len: u64| -> Result<Vec<u8>> {
        let mut buf = vec![0; len as usize];
        let mut file = &file;
        file.seek(SeekFrom::Start(offset))
            .and_then(|_| file.read_exact(&mut buf))
            .at(path)?;
        Ok(buf)
    };
    let block = |handle: BlockHandle, checksum: ChecksumType| -> Result<Vec<u8>> {
        let fetched = read_at(handle.offset, handle.span_len())?;
        Ok(decode_fetched(&fetched, handle.offset, checksum)
            .at(path)?
            .as_slice()
            .to_vec())
    };

    let size = file.metadata().at(path)?.len();
    let tail_len = (FOOTER_LEN as u64).min(size);
    let tail = read_at(size - tail_len, tail_len)?;
    let footer = Footer::decode(&tail, size).at(path)?;
    let meta = block(footer.metaindex, footer.checksum)?;
    let metaindex = Metaindex::parse(&Block::new(&meta).at(path)?).at(path)?;
    // Untested, with a reason (§12): every writer that exists emits a
    // properties block, so a metaindex without one is a table built by hand.
    // The engine refuses such a table when it opens one too, and is untested
    // there for the same reason.
    let Some(handle) = metaindex.properties else {
        return Err(Error::Message(format!(
            "{}: the table has no properties block",
            path.display()
        )));
    };
    let bytes = block(handle, footer.checksum)?;
    let p = Properties::decode(&Block::new(&bytes).at(path)?).at(path)?;
    let number = |name: &str| p.uvarint(name).at(path);
    Ok(TableProperties {
        format: footer.format.name(),
        compression: p.string("rocksdb.compression").unwrap_or("-").to_string(),
        entries: p.num_entries().at(path)?,
        deletions: number("rocksdb.deleted.keys")?,
        range_deletions: number("rocksdb.num.range-deletions")?,
        raw_keys: number("rocksdb.raw.key.size")?,
        raw_values: number("rocksdb.raw.value.size")?,
        data: number("rocksdb.data.size")?,
    })
}

/// Opens the database in `dir` to write, writing a new one there if
/// `create` is set.
///
/// Without `create`, a directory that holds no database is refused before
/// it is opened: opening takes the directory's lock, which would leave a
/// `LOCK` file behind in a directory that was never a database.
async fn open(dir: &Path, create: bool) -> Result<Db<FileFs>> {
    if !create {
        a_database_in(dir)?;
    }
    let options = Options::<FileFs>::default().create_if_missing(create);
    roble::open_with(&DefaultEnv, dir, options).await.at(dir)
}

/// Opens the database in `dir` to read, beside any process that has it open
/// to write. It takes no lock and changes nothing in the directory.
async fn open_read_only(dir: &Path) -> Result<Db<FileFs>> {
    a_database_in(dir)?;
    roble::open_read_only(&DefaultEnv, dir, Options::default())
        .await
        .at(dir)
}

/// Refuses a directory that holds no database, in the words the command
/// uses for it whether it reads or writes.
fn a_database_in(dir: &Path) -> Result<()> {
    let listing = std::fs::read_dir(dir).at(dir)?;
    let names: Vec<String> = listing
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    let marker =
        files::current_marker(names.iter().map(String::as_str), MARKER_MANIFEST).at(dir)?;
    if marker.is_none() {
        return Err(Error::Message(format!(
            "{}: no database here",
            dir.display()
        )));
    }
    Ok(())
}

/// Runs `read` against the database in `dir`, opened read-only. When the
/// process writing the database retires a file the read needs, opens it
/// again and reads again, up to [`MAX_STALE`] times.
async fn reading<T>(dir: &Path, mut read: impl AsyncFnMut(&Db<FileFs>) -> Result<T>) -> Result<T> {
    let mut stale = 0;
    loop {
        let result = match open_read_only(dir).await {
            Ok(db) => read(&db).await,
            Err(e) => Err(e),
        };
        match result {
            Err(Error::Stale(_)) if stale < MAX_STALE => stale += 1,
            result => return result,
        }
    }
}

/// Writes `batch` and waits for it to be synced: the process exits straight
/// after, so anything less would leave the write to the machine staying up.
async fn write(db: &Db<FileFs>, dir: &Path, batch: &mut Batch) -> Result<()> {
    db.write(batch, true).await.at(dir)
}

/// Names the directory or file an error is about: every message this
/// command prints starts with the path it concerns.
///
/// A method rather than a `map_err` at each call, so that what turns an error
/// into a message is written once for each kind of error there is.
trait At<T> {
    fn at(self, path: &Path) -> Result<T>;
}

impl<T> At<T> for roble::Result<T> {
    fn at(self, path: &Path) -> Result<T> {
        self.map_err(|e| {
            let m = format!("{}: {e}", path.display());
            match e {
                roble::Error::Stale(_) => Error::Stale(m),
                _ => Error::Message(m),
            }
        })
    }
}

impl<T> At<T> for roble_format::Result<T> {
    fn at(self, path: &Path) -> Result<T> {
        self.map_err(|e| Error::Message(format!("{}: {e}", path.display())))
    }
}

impl<T> At<T> for io::Result<T> {
    fn at(self, path: &Path) -> Result<T> {
        self.map_err(|e| Error::Message(format!("{}: {e}", path.display())))
    }
}

/// Decodes an argument, in which `\xNN` stands for the byte with hex value
/// `NN`, and `\\` for a backslash. As encina's command reads them.
pub fn unescape(arg: &str) -> std::result::Result<Box<[u8]>, String> {
    let invalid = || r"use \xNN for a byte, or \\ for a backslash".to_string();
    let hex = |d: &u8| (*d as char).to_digit(16);
    let mut bytes = Vec::with_capacity(arg.len());
    let mut rest = arg.as_bytes();
    while let [b, tail @ ..] = rest {
        rest = match (b, tail) {
            (b'\\', [b'\\', tail @ ..]) => {
                bytes.push(b'\\');
                tail
            }
            (b'\\', [b'x', hi, lo, tail @ ..]) => match (hex(hi), hex(lo)) {
                (Some(hi), Some(lo)) => {
                    bytes.push((hi * 16 + lo) as u8);
                    tail
                }
                _ => return Err(invalid()),
            },
            (b'\\', _) => return Err(invalid()),
            (b, tail) => {
                bytes.push(*b);
                tail
            }
        };
    }
    Ok(bytes.into_boxed_slice())
}

/// Writes `bytes` as [`unescape`] reads them: UTF-8 text as is, and
/// backslashes, control characters and bytes that aren't UTF-8 escaped.
fn write_text(out: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    for chunk in bytes.utf8_chunks() {
        for c in chunk.valid().chars() {
            if c == '\\' {
                out.write_all(br"\\")?;
            } else if c.is_control() {
                for b in c.encode_utf8(&mut [0; 4]).bytes() {
                    write!(out, r"\x{b:02x}")?;
                }
            } else {
                write!(out, "{c}")?;
            }
        }
        for b in chunk.invalid() {
            write!(out, r"\x{b:02x}")?;
        }
    }
    Ok(())
}

/// Writes `bytes` in the layout of `hexdump -C`: lines of up to 16 bytes, each
/// with its offset, the bytes in hex, and the bytes as ASCII. `label` starts
/// the first line, and as many spaces the others.
fn write_hexdump(out: &mut impl Write, label: &str, bytes: &[u8]) -> io::Result<()> {
    let width = label.len();
    // No bytes still take a line.
    for offset in (0..bytes.len().max(1)).step_by(16) {
        let line = &bytes[offset..bytes.len().min(offset + 16)];
        let label = if offset == 0 { label } else { "" };
        write!(out, "{label:width$}{offset:08x} ")?;
        for i in 0..16 {
            if i % 8 == 0 {
                out.write_all(b" ")?;
            }
            match line.get(i) {
                Some(b) => write!(out, "{b:02x} ")?,
                None => out.write_all(b"   ")?,
            }
        }
        out.write_all(b" |")?;
        for &b in line {
            let shown = if b == b' ' || b.is_ascii_graphic() {
                b
            } else {
                b'.'
            };
            out.write_all(&[shown])?;
        }
        out.write_all(b"|\n")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn golden(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../format/tests/golden")
            .join(name)
    }

    /// What the properties say, read from the end of the file alone, is what
    /// the format crate finds reading the file whole.
    #[test]
    fn properties_match_a_whole_read_of_the_table() {
        for (name, compression) in [
            ("snappy.sst", "Snappy"),
            ("zstd.sst", "ZSTD"),
            ("rangedel.sst", "NoCompression"),
            ("valueblocks.sst", "NoCompression"),
        ] {
            let path = golden(name);
            let p = properties_of(&path).unwrap_or_else(|_| panic!("{name}"));
            let bytes = std::fs::read(&path).expect("read");
            let table = roble_format::sst::Table::open(&bytes).expect("open");
            let whole = table.properties();
            assert_eq!(p.compression, compression, "{name}");
            assert_eq!(p.format, table.footer().format.name(), "{name}");
            assert_eq!(p.entries, whole.num_entries().unwrap(), "{name}");
            for (got, property) in [
                (p.deletions, "rocksdb.deleted.keys"),
                (p.range_deletions, "rocksdb.num.range-deletions"),
                (p.raw_keys, "rocksdb.raw.key.size"),
                (p.raw_values, "rocksdb.raw.value.size"),
                (p.data, "rocksdb.data.size"),
            ] {
                assert_eq!(got, whole.uvarint(property).unwrap(), "{name} {property}");
            }
        }
    }

    /// A table that has gone is stale, since the process writing the
    /// database retires tables; anything else that stops it opening is not.
    #[test]
    fn a_table_that_cannot_be_opened_says_why() {
        let gone = properties_of(Path::new("/nonexistent/000001.sst"));
        assert!(
            matches!(gone, Err(Error::Stale(_))),
            "a missing table is stale"
        );
        // A path no file system takes.
        let refused = properties_of(Path::new("table\0.sst"));
        assert!(
            matches!(&refused, Err(Error::Message(m)) if m.contains("table")),
            "the refusal names the table"
        );
    }

    #[test]
    fn a_ratio_needs_both_sizes() {
        assert_eq!(ratio(Some(300), Some(100)), "3.00x");
        assert_eq!(ratio(None, Some(100)), "-");
        assert_eq!(ratio(Some(300), None), "-");
        assert_eq!(ratio(Some(300), Some(0)), "-");
    }
}
