//! binlog-inspect: dump a replite binlog in human-readable form.
//!
//! Usage:
//!   binlog-inspect <binlog-dir>     directory of `*.seg` segment files
//!   binlog-inspect <file.seg>       a single segment file
//!
//! Read-only: unlike `Binlog::open`, it never truncates torn records; it
//! reports them as errors.

use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail};
use prost::Message;

use replite::binlog::{Op, Transaction, Value};

const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let path = match args.len() {
        2 if !matches!(args[1].as_str(), "-h" | "--help") => &args[1],
        _ => {
            eprintln!("usage: binlog-inspect <binlog-dir | file.seg>");
            std::process::exit(2);
        }
    };

    let path = Path::new(path);
    if path.is_dir() {
        let mut files: Vec<_> = fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "seg"))
            .collect();
        files.sort();
        let mut total = 0usize;
        for f in files {
            println!("== {} ==", f.display());
            total += dump_file(&f)?;
            println!();
        }
        println!("total: {total} records");
    } else {
        dump_file(path)?;
    }
    Ok(())
}

/// Dump every record in one segment file.
fn dump_file(path: &Path) -> Result<usize> {
    let mut f = File::open(path)
        .with_context(|| format!("binlog-inspect: cannot open {}", path.display()))?;
    let mut count = 0usize;
    loop {
        let len = match read_varint(&mut f)? {
            Some(len) => len,
            None => break,
        };
        if len > MAX_RECORD_BYTES {
            bail!(
                "binlog-inspect: implausible record length {len} in {}",
                path.display()
            );
        }
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf).with_context(|| {
            format!(
                "binlog-inspect: torn record after {} records in {}",
                count,
                path.display()
            )
        })?;
        let tx = Transaction::decode(&*buf).with_context(|| {
            format!(
                "binlog-inspect: corrupt record after {} records in {}",
                count,
                path.display()
            )
        })?;
        print_tx(&tx);
        count += 1;
    }
    Ok(count)
}

fn print_tx(tx: &Transaction) {
    println!(
        "txn lsn={} commit={} rows={} ddl={}",
        tx.lsn,
        fmt_utc(tx.commit_ts_ms),
        tx.rows.len(),
        tx.ddl.len(),
    );
    for row in &tx.rows {
        let op = Op::try_from(row.op).unwrap_or(Op::Insert);
        let pk: Vec<String> = if !row.pk_values.is_empty() {
            row.pk_values.iter().map(fmt_value).collect()
        } else if op != Op::Delete {
            row.pk_columns
                .iter()
                .filter_map(|pc| {
                    row.columns
                        .iter()
                        .position(|c| c == pc)
                        .map(|i| fmt_value(&row.values[i]))
                })
                .collect()
        } else {
            Vec::new()
        };
        let pk = if pk.is_empty() {
            String::new()
        } else {
            format!("pk=[{}] ", pk.join(", "))
        };
        match op {
            Op::Delete => {
                println!("  {:<6} table=\"{}\" {pk}", op_label(op), row.table);
            }
            Op::Insert | Op::Update => {
                let cols: Vec<String> = row
                    .columns
                    .iter()
                    .zip(&row.values)
                    .map(|(c, v)| format!("{c}={}", fmt_value(v)))
                    .collect();
                println!(
                    "  {:<6} table=\"{}\" {pk}{}",
                    op_label(op),
                    row.table,
                    cols.join(", "),
                );
            }
        }
    }
    for ddl in &tx.ddl {
        for stmt in &ddl.statements {
            println!("  DDL    {stmt}");
        }
    }
}

fn op_label(op: Op) -> &'static str {
    match op {
        Op::Insert => "INSERT",
        Op::Update => "UPDATE",
        Op::Delete => "DELETE",
    }
}

fn fmt_value(v: &Value) -> String {
    use replite::binlog::value::Value as V;
    match &v.value {
        None | Some(V::Null(_)) => "NULL".to_string(),
        Some(V::Integer(i)) => i.to_string(),
        Some(V::Float(f)) => format!("{f}"),
        Some(V::Text(t)) => format!("\"{}\"", escape(t)),
        Some(V::Blob(b)) => {
            let hex = b.iter().map(|x| format!("{x:02x}")).collect::<String>();
            if b.len() > 32 {
                format!("X'{}…'", &hex[..64])
            } else {
                format!("X'{hex}'")
            }
        }
    }
}

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn read_varint<R: Read>(r: &mut R) -> Result<Option<u64>> {
    let mut v = 0u64;
    for i in 0..10 {
        let mut b = [0u8; 1];
        match r.read_exact(&mut b) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && i == 0 => return Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                bail!("binlog-inspect: torn varint")
            }
            Err(e) => return Err(e.into()),
        }
        v |= ((b[0] & 0x7f) as u64) << (7 * i);
        if b[0] & 0x80 == 0 {
            return Ok(Some(v));
        }
    }
    bail!("binlog-inspect: varint too long")
}

fn fmt_utc(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{millis:03}Z")
}

/// Howard Hinnant's `civil_from_days` — days since 1970-01-01 to (y, m, d).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m as u32, d as u32)
}
