//! Statement execution with binlog capture integration.
//!
//! Every statement runs under the namespace's connection mutex. The capture
//! hooks buffer raw row events; after each statement we resolve schemas,
//! finalize committed capture units and append them to the binlog — all
//! before the Hrana response is sent, so a successful commit always has its
//! binlog record durable.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::binlog::{self, Transaction};
use crate::capture::{Capture, finalize_unit};
use crate::error::StmtError;
use crate::hrana::proto::{Col, NamedArg, Row, StmtResult, Value};
use rusqlite::Connection;
use rusqlite::types::ToSqlOutput;
use rusqlite::types::{ToSql, Value as SqliteValue, ValueRef};

/// Everything belonging to one connection that touches SQLite.
///
/// The namespace's main handle and every baton'd stream handle each own a
/// connection + capture (the hooks are per-connection); the binlog is SHARED
/// between all of them (one append stream per namespace, serialized by the
/// mutex), so changes committed on any connection land in the same LSN order.
pub struct DbHandle {
    pub conn: Connection,
    /// The SQLite hooks capture a raw pointer to this cell, so the DbHandle
    /// must live in a stable allocation (Box) once the hooks are installed.
    pub capture: RefCell<Capture>,
    pub binlog: Arc<Mutex<binlog::Binlog>>,
    /// Stored SQL (`store_sql`). Lives for the lifetime of the stream; the
    /// namespace's main handle clears it at the start of every baton-less
    /// request (fresh-connection semantics).
    pub sqls: RefCell<HashMap<i32, String>>,
}

impl DbHandle {
    pub fn schema_version(&self) -> i64 {
        self.conn
            .query_row("PRAGMA schema_version", [], |r| r.get(0))
            .unwrap_or(0)
    }
}

/// Arguments as received from the Hrana wire protocol.
pub struct StmtArgs {
    pub args: Vec<Value>,
    pub named_args: Vec<NamedArg>,
}

impl StmtArgs {
    pub fn empty() -> Self {
        StmtArgs {
            args: Vec::new(),
            named_args: Vec::new(),
        }
    }
}

/// Execute a single statement (Hrana `execute` request semantics: exactly one
/// statement; more than one is an error).
pub fn run_stmt(
    handle: &DbHandle,
    sql: &str,
    args: &StmtArgs,
    want_rows: bool,
) -> Result<StmtResult, StmtError> {
    {
        let mut capture = handle.capture.borrow_mut();
        if capture.unit.is_none() {
            // Read the schema version only when we are about to open a new
            // capture unit, and only outside a transaction: a PRAGMA read
            // inside the client's open transaction leaves the connection in a
            // read transaction (a WAL read mark), which blocks its later
            // read->write upgrade with an immediate SQLITE_BUSY once another
            // connection has committed newer frames. Inside a transaction the
            // version is read again at commit (autocommit, safe) for DDL
            // detection.
            let version = if handle.conn.is_autocommit() {
                handle.schema_version()
            } else {
                0
            };
            capture.begin_unit(version);
        }
        capture.record_statement(sql);
    }

    let start = std::time::Instant::now();
    let result = run_stmt_inner(handle, sql, args, want_rows);
    match result {
        Ok(mut r) => {
            r.query_duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            after_success(handle);
            Ok(r)
        }
        Err(e) => {
            let autocommit = handle.conn.is_autocommit();
            handle.capture.borrow_mut().rollback_statement(autocommit);
            Err(e)
        }
    }
}

/// Execute an arbitrary sequence of statements (Hrana `sequence` request
/// semantics: statements run in order; on error, remaining statements are
/// skipped and the error is returned).
pub fn run_sequence(handle: &DbHandle, sql: &str) -> Result<(), StmtError> {
    let stmts = split_statements(&handle.conn, sql)?;
    if stmts.is_empty() {
        return Ok(());
    }
    for stmt in stmts {
        run_stmt(handle, &stmt, &StmtArgs::empty(), false)?;
    }
    Ok(())
}

/// Split a SQL string into its constituent statements (needed by
/// `sequence`, which runs multiple statements). Uses `sqlite3_prepare_v2`
/// directly: the tail pointer gives exact byte offsets, comments and
/// trailing semicolons are handled by the parser.
fn split_statements(conn: &Connection, sql: &str) -> Result<Vec<String>, StmtError> {
    use std::os::raw::c_char;
    use std::ptr;

    let mut out = Vec::new();
    let mut rest = sql;
    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            break;
        }
        let mut stmt: *mut rusqlite::ffi::sqlite3_stmt = ptr::null_mut();
        let mut tail: *const c_char = ptr::null();
        // Pass the explicit byte length, never -1: `rest` is a subslice of a
        // larger string and has no NUL terminator, so a read-until-NUL scan
        // would walk past the string into allocator garbage (libsql-server
        // does the same: `prepare_stmt` passes `sql.len()`).
        let rc = unsafe {
            rusqlite::ffi::sqlite3_prepare_v2(
                conn.handle(),
                rest.as_ptr() as *const c_char,
                rest.len() as i32,
                &mut stmt,
                &mut tail,
            )
        };
        if rc != rusqlite::ffi::SQLITE_OK {
            return Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(rc), None).into());
        }
        if stmt.is_null() {
            break; // only comments / empty
        }
        // With an explicit length the tail stays inside the string; clamp
        // defensively anyway (mirrors libsql-server's tail bounds check).
        let consumed =
            (unsafe { tail.offset_from(rest.as_ptr() as *const c_char) } as usize).min(rest.len());
        let stmt_sql = if consumed > 0 {
            rest[..consumed].to_string()
        } else {
            // Fallback: sqlite3_sql() text
            let s = unsafe { rusqlite::ffi::sqlite3_sql(stmt) };
            if s.is_null() {
                unsafe { rusqlite::ffi::sqlite3_finalize(stmt) };
                break;
            }
            unsafe { std::ffi::CStr::from_ptr(s) }
                .to_string_lossy()
                .into_owned()
        };
        unsafe { rusqlite::ffi::sqlite3_finalize(stmt) };
        let stmt_sql = stmt_sql.trim().to_string();
        if !stmt_sql.is_empty() {
            out.push(stmt_sql);
        }
        if consumed == 0 {
            break; // no progress -> stop (defensive; consumed==0 already returns the sql3_sql fallback)
        }
        rest = &rest[consumed..];
    }
    Ok(out)
}

fn run_stmt_inner(
    handle: &DbHandle,
    sql: &str,
    args: &StmtArgs,
    want_rows: bool,
) -> Result<StmtResult, StmtError> {
    tracing::debug!(sql, "statement");
    if sql.trim_start().to_ascii_uppercase().starts_with("ATTACH") {
        return Err(StmtError::new(
            "SQLITE_MISUSE",
            "ATTACH DATABASE is not supported",
        ));
    }

    let mut stmt = handle.conn.prepare(sql).map_err(|e| match e {
        rusqlite::Error::MultipleStatement => StmtError::new(
            "SQLITE_MISUSE",
            "SQL string contains more than one statement",
        ),
        e => StmtError::from(e),
    })?;

    bind_args(&mut stmt, args)?;

    let cols: Vec<Col> = column_info(&stmt);
    let mut rows: Vec<Row> = Vec::new();

    if want_rows {
        let mut query = stmt.raw_query();
        while let Some(row) = query.next()? {
            let mut values = Vec::with_capacity(cols.len());
            for i in 0..cols.len() {
                values.push(value_ref_to_hrana(row.get_ref(i)?));
            }
            rows.push(Row { values });
        }
    } else {
        let mut query = stmt.raw_query();
        while query.next()?.is_some() {}
    }

    let changed = handle.conn.changes();
    let last_insert_rowid = if changed > 0 {
        Some(handle.conn.last_insert_rowid())
    } else {
        None
    };

    Ok(StmtResult {
        cols,
        rows,
        affected_row_count: changed,
        last_insert_rowid,
        replication_index: None,
        rows_read: 0,
        rows_written: changed,
        query_duration_ms: 0.0,
    })
}

fn column_info(stmt: &rusqlite::Statement) -> Vec<Col> {
    stmt.columns()
        .iter()
        .map(|c| Col {
            name: Some(c.name().to_string()),
            decltype: c.decl_type().map(|d| d.to_string()),
        })
        .collect()
}

/// Bind Hrana values onto the statement, mirroring libsql-server semantics:
/// positional args bind to `?`/unnamed params in order; named args bind by
/// name (prefix stripped). A param left without a value is an error.
fn bind_args(stmt: &mut rusqlite::Statement, args: &StmtArgs) -> Result<(), StmtError> {
    if !args.args.is_empty() && !args.named_args.is_empty() {
        return Err(StmtError::new(
            "SQLITE_MISUSE",
            "Specifying both positional and named arguments is not supported",
        ));
    }

    let param_count = stmt.parameter_count();
    if args.args.len() > param_count {
        return Err(StmtError::new(
            "SQLITE_MISUSE",
            format!(
                "too many parameters, expected {param_count} found {}",
                args.args.len()
            ),
        ));
    }

    let named: HashMap<String, &Value> = args
        .named_args
        .iter()
        .map(|a| (a.name.clone(), &a.value))
        .collect();

    for index in 1..=param_count {
        let maybe_value = match stmt.parameter_name(index) {
            Some(name) => {
                let mut chars = name.chars();
                match chars.next() {
                    Some('?') => {
                        let pos: usize = chars.as_str().parse().map_err(|_| {
                            StmtError::new(
                                "SQLITE_MISUSE",
                                format!("invalid parameter {name}: expected a numerical position after `?`"),
                            )
                        })?;
                        if pos == 0 || pos > args.args.len() {
                            None
                        } else {
                            Some(&args.args[pos - 1])
                        }
                    }
                    _ => named
                        .get(name)
                        .copied()
                        .or_else(|| named.get(chars.as_str()).copied()),
                }
            }
            None => args.args.get(index - 1),
        };

        match maybe_value {
            Some(value) => stmt.raw_bind_parameter(index, HranaValue(value))?,
            None => {
                return Err(StmtError::new(
                    "SQLITE_MISUSE",
                    format!(
                        "value for parameter {} not found",
                        stmt.parameter_name(index).unwrap_or(&index.to_string())
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Adapt a Hrana value to a `rusqlite::ToSql`.
struct HranaValue<'a>(&'a Value);

impl ToSql for HranaValue<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let v = match &self.0 {
            Value::None | Value::Null => SqliteValue::Null,
            Value::Integer { value } => SqliteValue::Integer(*value),
            Value::Float { value } => SqliteValue::Real(*value),
            Value::Text { value } => SqliteValue::Text(value.to_string()),
            Value::Blob { value } => SqliteValue::Blob(value.to_vec()),
        };
        Ok(ToSqlOutput::Owned(v))
    }
}

fn value_ref_to_hrana(v: ValueRef) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Integer { value: i },
        ValueRef::Real(f) => Value::Float { value: f },
        ValueRef::Text(b) => Value::Text {
            value: String::from_utf8_lossy(b).into(),
        },
        ValueRef::Blob(b) => Value::Blob {
            value: b.to_vec().into(),
        },
    }
}

/// Post-statement bookkeeping: refresh schema version, finalize any committed
/// capture unit into the binlog, close empty units.
///
/// All reads (PRAGMA schema_version / table_info) happen only when the
/// connection is back in autocommit: a read inside the client's open
/// transaction would leave a WAL read mark on the connection, which makes its
/// later read->write upgrade fail with an immediate SQLITE_BUSY once another
/// connection committed newer WAL frames (see `run_stmt`).
fn after_success(handle: &DbHandle) {
    let mut capture = handle.capture.borrow_mut();

    if handle.conn.is_autocommit() {
        let schema_version = handle.schema_version();
        if let Some(unit) = capture.unit.as_mut() {
            unit.latest_schema_version = schema_version;
        }

        if capture.commit_pending {
            match finalize_unit(&handle.conn, &mut capture) {
                Ok(Some(res)) => {
                    let tx = Transaction {
                        lsn: 0, // assigned by the binlog
                        commit_ts_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0),
                        rows: res.rows,
                        ddl: res.ddl,
                    };
                    if let Ok(mut binlog) = handle.binlog.lock() {
                        if let Err(e) = binlog.append(tx) {
                            tracing::error!("binlog append failed: {e}");
                        }
                    } else {
                        tracing::error!("binlog lock poisoned");
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!("binlog finalize failed: {e}");
                }
            }
            return;
        }

        // No commit happened (autocommit, no explicit transaction): close empty
        // units so read-only statements don't keep a capture unit open across
        // statements. Inside an explicit transaction the unit stays open (it was
        // opened at BEGIN with the pre-transaction schema version, which the
        // commit-time finalize needs for DDL detection).
        if let Some(unit) = capture.unit.as_ref()
            && unit.rows.is_empty()
            && unit.stmts.is_empty()
        {
            capture.unit = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binlog::Binlog;
    use crate::capture::{Capture, install_hooks};

    fn fresh_db() -> (Box<DbHandle>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open(dir.path().join("test.sqlite")).unwrap();
        let capture = RefCell::new(Capture::new(0));
        let binlog = Arc::new(Mutex::new(
            Binlog::open(dir.path().join("binlog"), 1024, 10_000).unwrap(),
        ));
        let handle = Box::new(DbHandle {
            conn,
            capture,
            binlog,
            sqls: RefCell::new(HashMap::new()),
        });
        install_hooks(&handle.conn, &handle.capture);
        (handle, dir)
    }

    fn read_txs(handle: &DbHandle) -> Vec<crate::binlog::Transaction> {
        handle.binlog.lock().unwrap().read_since(0).unwrap()
    }

    #[test]
    fn insert_updates_binlog() {
        let (handle, _dir) = fresh_db();
        run_stmt(
            &handle,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        run_stmt(
            &handle,
            "INSERT INTO t (id, v) VALUES (1, 'hello')",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        run_stmt(
            &handle,
            "INSERT INTO t (id, v) VALUES (2, 'world')",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        run_stmt(
            &handle,
            "UPDATE t SET v = 'hello2' WHERE id = 1",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        run_stmt(
            &handle,
            "DELETE FROM t WHERE id = 2",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();

        let txs = read_txs(&handle);
        assert_eq!(txs.len(), 5, "ddl + 2 inserts + update + delete");
        // tx1 = DDL
        assert_eq!(txs[0].ddl.len(), 1);
        assert_eq!(txs[0].ddl[0].statements.len(), 1);
        assert_eq!(txs[0].rows.len(), 0);
        // tx2 = insert
        let ev = &txs[1].rows[0];
        assert_eq!(ev.table, "t");
        assert_eq!(ev.op, crate::binlog::Op::Insert as i32);
        assert_eq!(ev.columns, vec!["id", "v"]);
        assert_eq!(ev.pk_columns, vec!["id"]);
        assert_eq!(txs[2].rows[0].values.len(), 2);
        // tx4 = update: after-image only, values updated
        let upd = &txs[3].rows[0];
        assert_eq!(upd.op, crate::binlog::Op::Update as i32);
        assert_eq!(upd.pk_columns, vec!["id"]);
        assert_eq!(upd.pk_values.len(), 0, "no pk values for update");
        assert_eq!(upd.values[1], crate::binlog::Value::text("hello2"));
        // tx5 = delete: pk only
        let del = &txs[4].rows[0];
        assert_eq!(del.op, crate::binlog::Op::Delete as i32);
        assert_eq!(del.pk_values, vec![crate::binlog::Value::integer(2)]);
        assert!(del.columns.is_empty());
        assert!(del.values.is_empty());
    }

    #[test]
    fn without_rowid_table() {
        let (handle, _dir) = fresh_db();
        run_stmt(
            &handle,
            "CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY (a, b)) WITHOUT ROWID",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        run_stmt(
            &handle,
            "INSERT INTO t VALUES ('x', 'y')",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        run_stmt(
            &handle,
            "DELETE FROM t WHERE a = 'x'",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        let txs = read_txs(&handle);
        let del = &txs[2].rows[0];
        assert_eq!(del.op, crate::binlog::Op::Delete as i32);
        assert_eq!(del.pk_columns, vec!["a", "b"]);
        assert_eq!(
            del.pk_values,
            vec![
                crate::binlog::Value::text("x"),
                crate::binlog::Value::text("y")
            ]
        );
    }

    #[test]
    fn rollback_drops_events() {
        let (handle, _dir) = fresh_db();
        run_stmt(
            &handle,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        run_stmt(&handle, "BEGIN", &StmtArgs::empty(), false).unwrap();
        run_stmt(
            &handle,
            "INSERT INTO t (id, v) VALUES (1, 'x')",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        run_stmt(&handle, "ROLLBACK", &StmtArgs::empty(), false).unwrap();
        let txs = read_txs(&handle);
        assert_eq!(txs.len(), 1, "only the DDL transaction");
    }

    #[test]
    fn failed_statement_truncates_partial_events() {
        let (handle, _dir) = fresh_db();
        run_stmt(
            &handle,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        // Multi-row UPDATE where one row violates NOT NULL: the whole
        // statement rolls back, and no events must leak into the binlog.
        run_stmt(
            &handle,
            "INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        let err =
            run_stmt(&handle, "UPDATE t SET v = NULL", &StmtArgs::empty(), false).unwrap_err();
        assert!(err.message.contains("NOT NULL"), "got: {err:?}");
        let txs = read_txs(&handle);
        assert_eq!(txs.len(), 2, "DDL + insert; failed update must not appear");
        // A subsequent good statement must not contain the failed events.
        run_stmt(
            &handle,
            "INSERT INTO t (id, v) VALUES (3, 'c')",
            &StmtArgs::empty(),
            false,
        )
        .unwrap();
        let txs = read_txs(&handle);
        assert_eq!(txs.len(), 3);
        assert_eq!(txs[2].rows.len(), 1);
    }

    #[test]
    fn named_and_positional_args() {
        let (handle, _dir) = fresh_db();
        let r = run_stmt(
            &handle,
            "SELECT :a + ?1 AS total",
            &StmtArgs {
                args: vec![Value::integer(40)],
                named_args: vec![crate::hrana::proto::NamedArg {
                    name: "a".into(),
                    value: Value::integer(2),
                }],
            },
            true,
        )
        .unwrap_err();
        assert!(r.message.contains("both positional and named"));
    }
}
