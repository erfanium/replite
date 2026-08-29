//! Row-level binlog: protobuf message types and segmented storage.
//!
//! Design (mirrors MySQL binlog semantics for a one-way server -> device
//! replication):
//! - INSERT events carry the full after-image (all columns).
//! - UPDATE events carry only the after-image. Primary keys are never updated
//!   (guaranteed by the schema contract), so the applier can upsert.
//! - DELETE events carry only the primary key values.
//!
//! Storage: append-only segment files named `{lsn_of_first_record:010}.seg`
//! under `<namespace>/binlog/`. Each record is `[varint length][Transaction
//! protobuf]`. LSNs are monotonically increasing u64s assigned in commit
//! order, one per committed transaction that changed something.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use prost::Message;

// ---------------------------------------------------------------------------
// Message types (protobuf, prost derive)
// ---------------------------------------------------------------------------

/// A SQL value. Same oneof shape as Hrana values, so clients that already
/// speak Hrana can reuse their value codecs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Value {
    #[prost(oneof = "value::Value", tags = "1, 2, 3, 4, 5")]
    pub value: Option<value::Value>,
}

pub mod value {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Empty {}

    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(message, tag = "1")]
        Null(Empty),
        #[prost(sint64, tag = "2")]
        Integer(i64),
        #[prost(double, tag = "3")]
        Float(f64),
        #[prost(string, tag = "4")]
        Text(String),
        #[prost(bytes, tag = "5")]
        Blob(Vec<u8>),
    }
}

impl Value {
    pub fn null() -> Self {
        Value {
            value: Some(value::Value::Null(value::Empty {})),
        }
    }

    pub fn integer(v: i64) -> Self {
        Value {
            value: Some(value::Value::Integer(v)),
        }
    }

    pub fn float(v: f64) -> Self {
        Value {
            value: Some(value::Value::Float(v)),
        }
    }

    pub fn text(v: impl Into<String>) -> Self {
        Value {
            value: Some(value::Value::Text(v.into())),
        }
    }

    pub fn blob(v: Vec<u8>) -> Self {
        Value {
            value: Some(value::Value::Blob(v)),
        }
    }
}

/// Row mutation opcode, same values as SQLite's `SQLITE_INSERT` / `SQLITE_UPDATE`
/// / `SQLITE_DELETE` hook opcodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
#[repr(i32)]
pub enum Op {
    Insert = 0,
    Update = 1,
    Delete = 2,
}

/// One changed row.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RowEvent {
    #[prost(string, tag = "1")]
    pub table: String,
    #[prost(enumeration = "Op", tag = "2")]
    pub op: i32,
    /// Column names that identify the row (declared PK, or `["rowid"]` for
    /// rowid tables without a declared PK).
    #[prost(string, repeated, tag = "3")]
    pub pk_columns: Vec<String>,
    /// PK values, one per `pk_columns` entry (DELETE events only need these).
    #[prost(message, repeated, tag = "4")]
    pub pk_values: Vec<Value>,
    /// Column names of the after-image (INSERT/UPDATE). Omitted for DELETE.
    #[prost(string, repeated, tag = "5")]
    pub columns: Vec<String>,
    /// After-image values, aligned with `columns`. Omitted for DELETE.
    #[prost(message, repeated, tag = "6")]
    pub values: Vec<Value>,
}

/// Schema change: verbatim SQL statements, to be replayed as-is by the applier.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DdlEvent {
    #[prost(string, repeated, tag = "1")]
    pub statements: Vec<String>,
}

/// One committed transaction.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transaction {
    #[prost(uint64, tag = "1")]
    pub lsn: u64,
    #[prost(int64, tag = "2")]
    pub commit_ts_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub rows: Vec<RowEvent>,
    #[prost(message, repeated, tag = "4")]
    pub ddl: Vec<DdlEvent>,
}

// ---------------------------------------------------------------------------
// Segment storage
// ---------------------------------------------------------------------------

fn lsn_to_name(lsn: u64) -> String {
    format!("{lsn:010}.seg")
}

fn name_to_lsn(name: &str) -> Option<u64> {
    name.strip_suffix(".seg")?.parse().ok()
}

/// Append-only binlog storage for one namespace.
pub struct Binlog {
    dir: PathBuf,
    segment: Option<File>,
    /// LSN of the last record written (0 if empty).
    current_lsn: u64,
    /// LSN of the first record in the oldest retained segment.
    min_lsn: u64,
    max_segment_bytes: u64,
    max_total_bytes: u64,
    /// Cache of known segment files (for GC decisions).
    segments: Vec<(u64, u64)>, // (start_lsn, file_size)
}

impl Binlog {
    /// Open (or create) the binlog directory and recover state from disk.
    pub fn open(dir: PathBuf, max_segment_bytes: u64, max_total_bytes: u64) -> Result<Self> {
        fs::create_dir_all(&dir)?;
        let mut segments = Self::list_segments(&dir)?;
        let (current_lsn, min_lsn) = Self::scan_state(&dir, &mut segments)?;
        Ok(Binlog {
            dir,
            segment: None,
            current_lsn,
            min_lsn,
            max_segment_bytes,
            max_total_bytes,
            segments,
        })
    }

    fn list_segments(dir: &Path) -> Result<Vec<(u64, u64)>> {
        let mut segments = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(lsn) = name_to_lsn(&name) {
                let size = entry.metadata()?.len();
                segments.push((lsn, size));
            }
        }
        segments.sort_by_key(|(lsn, _)| *lsn);
        Ok(segments)
    }

    /// Recover current_lsn/min_lsn from disk. Truncates a torn (partially
    /// written) trailing record so the log is always readable.
    fn scan_state(dir: &Path, segments: &mut Vec<(u64, u64)>) -> Result<(u64, u64)> {
        let mut current_lsn = 0u64;
        for i in 0..segments.len() {
            let start_lsn = segments[i].0;
            let path = dir.join(lsn_to_name(start_lsn));
            let mut f = File::open(&path)?;
            let mut consumed = 0u64;
            loop {
                let len = match read_varint(&mut f) {
                    Ok(Some(len)) => len,
                    Ok(None) => break,
                    Err(_) => {
                        // Torn record: truncate the file to the last good record.
                        tracing::warn!("binlog: truncating torn record in {:?}", path);
                        f.set_len(consumed)?;
                        break;
                    }
                };
                if len > 64 * 1024 * 1024 {
                    bail!("binlog: implausible record length {len} in {path:?}");
                }
                let mut buf = vec![0u8; len as usize];
                if let Err(e) = f.read_exact(&mut buf) {
                    tracing::warn!("binlog: truncating torn record in {:?}: {e}", path);
                    f.set_len(consumed)?;
                    break;
                }
                let tx = Transaction::decode(&*buf)
                    .with_context(|| format!("binlog: corrupt record in {path:?}"))?;
                if tx.lsn > current_lsn {
                    current_lsn = tx.lsn;
                }
                consumed += varint_len(len) as u64 + len as u64;
            }
            segments[i].1 = f.metadata()?.len();
        }
        let min_lsn = segments.first().map(|(lsn, _)| *lsn).unwrap_or(0);
        Ok((current_lsn, min_lsn))
    }

    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    pub fn min_lsn(&self) -> u64 {
        self.min_lsn
    }

    /// Append one transaction, assigning it the next LSN. Returns the LSN.
    pub fn append(&mut self, mut tx: Transaction) -> Result<u64> {
        if tx.rows.is_empty() && tx.ddl.is_empty() {
            return Ok(self.current_lsn);
        }
        let lsn = self.current_lsn + 1;
        tx.lsn = lsn;
        tracing::debug!(
            lsn,
            commit_ts_ms = tx.commit_ts_ms,
            row_count = tx.rows.len(),
            ddl_count = tx.ddl.len(),
            "binlog append"
        );
        for row in &tx.rows {
            let op = Op::try_from(row.op).unwrap_or(Op::Insert);
            tracing::debug!(
                lsn,
                table = %row.table,
                op = ?op,
                pk = ?row.pk_values,
                "binlog row"
            );
        }
        for ddl in &tx.ddl {
            tracing::debug!(lsn, statements = ?ddl.statements, "binlog ddl");
        }
        let bytes = tx.encode_to_vec();
        if self.segment.is_none() || {
            let size = self
                .segment
                .as_ref()
                .map(|f| f.metadata().map(|m| m.len()).unwrap_or(0))
                .unwrap_or(0);
            size + bytes.len() as u64 + 16 > self.max_segment_bytes
        } {
            self.rotate()?;
        }
        let f = self.segment.as_mut().unwrap();
        write_varint(&mut *f, bytes.len() as u64)?;
        f.write_all(&bytes)?;
        f.sync_data()?;
        self.current_lsn = lsn;
        self.maybe_gc();
        Ok(lsn)
    }

    fn rotate(&mut self) -> Result<()> {
        if let Some(mut f) = self.segment.take() {
            f.flush()?;
            f.sync_data()?;
            let start = self.segments_start_of_last();
            let size = f.metadata()?.len();
            if let Some(slot) = self.segments.iter_mut().find(|(lsn, _)| *lsn == start) {
                slot.1 = size;
            }
        }
        let next_start = self.current_lsn + 1;
        let path = self.dir.join(lsn_to_name(next_start));
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("binlog: cannot open segment {path:?}"))?;
        self.segments.push((next_start, 0));
        self.segment = Some(f);
        Ok(())
    }

    fn segments_start_of_last(&self) -> u64 {
        self.segments.last().map(|(lsn, _)| *lsn).unwrap_or(0)
    }

    /// Delete oldest segments until total size is under the limit.
    fn maybe_gc(&mut self) {
        loop {
            let total: u64 = self.segments.iter().map(|(_, s)| *s).sum();
            if total <= self.max_total_bytes || self.segments.len() <= 1 {
                break;
            }
            let (start, size) = self.segments.remove(0);
            let path = self.dir.join(lsn_to_name(start));
            match fs::remove_file(&path) {
                Ok(()) => {
                    tracing::info!("binlog: gc removed segment {path:?} ({size} bytes)");
                    self.min_lsn = self
                        .segments
                        .first()
                        .map(|(lsn, _)| *lsn)
                        .unwrap_or(self.current_lsn);
                }
                Err(e) => {
                    tracing::warn!("binlog: gc failed to remove {path:?}: {e}");
                    break;
                }
            }
        }
    }

    /// Read all transactions with lsn > `since`, in order. The caller must
    /// have already checked `since >= self.min_lsn`.
    pub fn read_since(&self, since: u64) -> Result<Vec<Transaction>> {
        self.iter_since(since)?.collect()
    }

    /// Open a lazy iterator over transactions with lsn > `since`, in order.
    /// All segment file handles are opened up-front, so a later GC deleting
    /// segments does not invalidate the stream; records are decoded one at a
    /// time as the caller consumes them (no full buffering).
    ///
    /// The caller must hold the binlog lock while calling this and have
    /// checked `since >= self.min_lsn`.
    pub fn iter_since(&self, since: u64) -> Result<BinlogIter> {
        let mut files = Vec::with_capacity(self.segments.len());
        for (start_lsn, _) in &self.segments {
            let path = self.dir.join(lsn_to_name(*start_lsn));
            files.push(File::open(&path)?);
        }
        Ok(BinlogIter {
            files: files.into_iter(),
            file: None,
            since,
        })
    }
}

/// Lazy reader over binlog segment files: yields one decoded `Transaction`
/// per record, skipping those with `lsn <= since`.
pub struct BinlogIter {
    files: std::vec::IntoIter<File>,
    file: Option<File>,
    since: u64,
}

impl Iterator for BinlogIter {
    type Item = Result<Transaction>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.file.is_none() {
                self.file = Some(self.files.next()?);
            }
            let file = self.file.as_mut()?;
            let len = match read_varint(file) {
                Ok(Some(len)) => len,
                Ok(None) => {
                    // clean EOF of this segment: move to the next one
                    self.file = None;
                    continue;
                }
                Err(e) => return Some(Err(e)),
            };
            let mut buf = vec![0u8; len as usize];
            if let Err(e) = file.read_exact(&mut buf) {
                return Some(Err(e.into()));
            }
            let tx = match Transaction::decode(&*buf) {
                Ok(tx) => tx,
                Err(e) => return Some(Err(e.into())),
            };
            if tx.lsn > self.since {
                return Some(Ok(tx));
            }
        }
    }
}

fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

fn write_varint<W: Write>(w: &mut W, mut v: u64) -> Result<()> {
    let mut buf = [0u8; 10];
    let mut i = 0;
    while v >= 0x80 {
        buf[i] = (v as u8) | 0x80;
        v >>= 7;
        i += 1;
    }
    buf[i] = v as u8;
    w.write_all(&buf[..=i])?;
    Ok(())
}

fn read_varint<R: Read>(r: &mut R) -> Result<Option<u64>> {
    let mut v = 0u64;
    let mut shift = 0u32;
    for _ in 0..10 {
        let mut byte = [0u8; 1];
        match r.read_exact(&mut byte) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                if shift == 0 {
                    return Ok(None);
                }
                return Err(e.into());
            }
            Err(e) => return Err(e.into()),
        }
        v |= ((byte[0] & 0x7f) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(Some(v));
        }
        shift += 7;
    }
    bail!("binlog: varint too long")
}

/// Column metadata cache used by the capture layer: maps a table to the
/// column names and PK membership, keyed by schema version.
#[derive(Default)]
pub struct SchemaCache {
    /// (schema_version, table) -> (columns, is_pk)
    entries: HashMap<(i64, String), (Vec<String>, Vec<bool>)>,
    schema_version: i64,
}

impl SchemaCache {
    pub fn with_schema_version(schema_version: i64) -> Self {
        SchemaCache {
            entries: HashMap::new(),
            schema_version,
        }
    }

    /// Drop cached schemas when the schema version changed.
    pub fn refresh(&mut self, schema_version: i64) {
        if schema_version != self.schema_version {
            self.entries.clear();
            self.schema_version = schema_version;
        }
    }

    pub fn get(
        &mut self,
        schema_version: i64,
        table: &str,
        fetch: impl FnOnce(&str) -> (Vec<String>, Vec<bool>),
    ) -> (Vec<String>, Vec<bool>) {
        self.refresh(schema_version);
        if let Some(v) = self.entries.get(&(schema_version, table.to_string())) {
            return v.clone();
        }
        let fetched = fetch(table);
        self.entries
            .insert((schema_version, table.to_string()), fetched.clone());
        fetched
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(lsn: u64) -> Transaction {
        Transaction {
            lsn,
            commit_ts_ms: 0,
            rows: vec![RowEvent {
                table: "t".into(),
                op: Op::Insert as i32,
                pk_columns: vec!["id".into()],
                pk_values: vec![Value::integer(1)],
                columns: vec!["id".into(), "v".into()],
                values: vec![Value::integer(1), Value::text("x")],
            }],
            ddl: vec![],
        }
    }

    #[test]
    fn append_read_roundtrip() {
        let dir = std::env::temp_dir().join(format!("binlog-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut b = Binlog::open(dir.clone(), 1024, 10_000).unwrap();
        assert_eq!(b.append(tx(0)).unwrap(), 1);
        assert_eq!(b.append(tx(0)).unwrap(), 2);
        assert_eq!(b.current_lsn(), 2);
        let txs = b.read_since(0).unwrap();
        assert_eq!(txs.len(), 2);
        assert_eq!(txs[0].lsn, 1);
        assert_eq!(txs[1].lsn, 2);
        drop(b);
        // Reopen and recover
        let b2 = Binlog::open(dir.clone(), 1024, 10_000).unwrap();
        assert_eq!(b2.current_lsn(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotation_and_min_lsn() {
        let dir = std::env::temp_dir().join(format!("binlog-rot-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut b = Binlog::open(dir.clone(), 200, 10_000).unwrap();
        for _ in 0..50 {
            b.append(tx(0)).unwrap();
        }
        assert!(b.current_lsn() >= 50);
        let txs = b.read_since(0).unwrap();
        assert_eq!(txs.len(), 50);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn value_roundtrip() {
        let v = Value {
            value: Some(value::Value::Blob(vec![1, 2, 3])),
        };
        let bytes = v.encode_to_vec();
        let v2 = Value::decode(&*bytes).unwrap();
        assert_eq!(v, v2);
    }
}
