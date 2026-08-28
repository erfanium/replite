//! Row-change capture via SQLite's C hooks (`sqlite3_preupdate_hook`,
//! `sqlite3_commit_hook`, `sqlite3_rollback_hook`).
//!
//! Captured data model:
//! - INSERT  -> full after-image (all columns).
//! - UPDATE  -> full after-image only (PKs are never updated by contract).
//! - DELETE  -> primary key values only.
//!
//! The hooks only do cheap in-memory work: they never run SQL against the
//! connection (SQLite forbids running new statements from inside the
//! preupdate hook's call path). Column/PK metadata is resolved *after* each
//! statement, from normal executor context, using a schema cache that is
//! invalidated when `PRAGMA schema_version` changes.
//!
//! Transactions are finalized (column resolution + segment append) after
//! their commit, in the executor, before the response is sent to the client.
//! The commit hook only records that a commit happened; the rollback hook
//! drops the pending capture unit.

use std::cell::RefCell;
use std::os::raw::{c_char, c_int, c_void};

use rusqlite::ffi::{self, sqlite3, sqlite3_value};

use crate::binlog::{self, DdlEvent, Op, RowEvent, SchemaCache, Value};
use crate::capture::ffi_ext::PreupdateValue;
use crate::error::AppError;

/// A row change as seen by the preupdate hook, before schema resolution.
pub struct RawEvent {
    pub table: String,
    pub op: Op,
    /// rowid of the row (rowid tables). `None` for WITHOUT ROWID tables.
    pub rowid: Option<i64>,
    /// Full after-image (INSERT/UPDATE).
    pub new_row: Option<Vec<Value>>,
    /// Full before-image (DELETE, needed for PK extraction).
    pub old_row: Option<Vec<Value>>,
}

/// One open capture unit = one SQLite transaction (explicit or implicit).
pub struct CaptureUnit {
    pub rows: Vec<RawEvent>,
    /// SQL executed in this unit, for DDL replication.
    pub stmts: Vec<String>,
    pub schema_version_at_start: i64,
    pub latest_schema_version: i64,
    /// rows.len() at the last statement start, for rollback-on-error.
    snapshot: usize,
    /// True when a savepoint rollback occurred mid-unit: the buffer is no
    /// longer trustworthy. Dropped at commit, and a divergence warning is
    /// logged (documented limitation: savepoints are not replicated).
    pub poisoned: bool,
}

impl CaptureUnit {
    fn begin(schema_version: i64) -> Self {
        CaptureUnit {
            rows: Vec::new(),
            stmts: Vec::new(),
            schema_version_at_start: schema_version,
            latest_schema_version: schema_version,
            snapshot: 0,
            poisoned: false,
        }
    }

    /// Call before executing a statement: remember the buffer position so a
    /// failed statement's partial events can be rolled back.
    pub fn mark_statement_start(&mut self) {
        self.snapshot = self.rows.len();
    }

    /// A statement failed. If we're in autocommit the whole unit is gone;
    /// otherwise only the failed statement's events are removed.
    pub fn rollback_statement(&mut self, autocommit: bool) {
        if autocommit {
            self.rows.clear();
            self.stmts.clear();
        } else {
            self.rows.truncate(self.snapshot);
        }
    }
}

/// Connection-scoped capture state. Lives inside the namespace's mutex.
pub struct Capture {
    pub unit: Option<CaptureUnit>,
    /// Set by the commit hook; consumed by the executor after the statement.
    pub commit_pending: bool,
    pub schema: SchemaCache,
}

impl Capture {
    pub fn new(schema_version: i64) -> Self {
        Capture {
            unit: None,
            commit_pending: false,
            schema: SchemaCache::with_schema_version(schema_version),
        }
    }

    pub fn begin_unit(&mut self, schema_version: i64) {
        if self.unit.is_none() {
            self.unit = Some(CaptureUnit::begin(schema_version));
        }
    }

    pub fn record_statement(&mut self, sql: &str) {
        if let Some(unit) = self.unit.as_mut() {
            unit.mark_statement_start();
            if !is_txn_control(sql) {
                unit.stmts.push(sql.to_string());
            }
        }
    }

    pub fn rollback_statement(&mut self, autocommit: bool) {
        if let Some(unit) = self.unit.as_mut() {
            unit.rollback_statement(autocommit);
            if autocommit {
                self.unit = None;
            }
        }
    }

    pub fn drop_unit(&mut self) {
        self.unit = None;
        self.commit_pending = false;
    }
}

/// Transaction-control statements that must not be replayed verbatim on the
/// replica (the applier manages transactions itself).
fn is_txn_control(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    let first = words.next().unwrap_or("").to_ascii_uppercase();
    match first.as_str() {
        "BEGIN" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" => true,
        "PRAGMA" => true,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// FFI hooks
// ---------------------------------------------------------------------------

unsafe extern "C" fn preupdate_cb(
    p_arg: *mut c_void,
    _db: *mut sqlite3,
    op: c_int,
    _z_db: *const c_char,
    z_table: *const c_char,
    i_pk: i64,
    _i_key2: i64,
) {
    let capture = unsafe { &*(p_arg as *const RefCell<Capture>) };
    if z_table.is_null() {
        return;
    }
    let table = unsafe { std::ffi::CStr::from_ptr(z_table) }
        .to_string_lossy()
        .into_owned();
    let mut capture = capture.borrow_mut();
    let unit = match capture.unit.as_mut() {
        Some(u) => u,
        None => return,
    };
    let count = unsafe { ffi::sqlite3_preupdate_count(_db) };
    if count <= 0 {
        return;
    }
    let read_value = |kind: PreupdateValue| -> Vec<Value> {
        (0..count)
            .map(|i| unsafe {
                let mut v: *mut sqlite3_value = std::ptr::null_mut();
                match kind {
                    PreupdateValue::Old => ffi::sqlite3_preupdate_old(_db, i, &mut v),
                    PreupdateValue::New => ffi::sqlite3_preupdate_new(_db, i, &mut v),
                };
                sqlite_value_to_value(v)
            })
            .collect()
    };
    let raw = match op {
        ffi::SQLITE_INSERT => RawEvent {
            table,
            op: Op::Insert,
            rowid: Some(i_pk),
            new_row: Some(read_value(PreupdateValue::New)),
            old_row: None,
        },
        ffi::SQLITE_UPDATE => RawEvent {
            table,
            op: Op::Update,
            rowid: Some(i_pk),
            new_row: Some(read_value(PreupdateValue::New)),
            old_row: None,
        },
        ffi::SQLITE_DELETE => RawEvent {
            table,
            op: Op::Delete,
            rowid: Some(i_pk),
            new_row: None,
            old_row: Some(read_value(PreupdateValue::Old)),
        },
        _ => return,
    };
    unit.rows.push(raw);
}

unsafe extern "C" fn commit_cb(p_arg: *mut c_void) -> c_int {
    let capture = unsafe { &*(p_arg as *const RefCell<Capture>) };
    capture.borrow_mut().commit_pending = true;
    0
}

unsafe extern "C" fn rollback_cb(p_arg: *mut c_void) {
    // Fires on transaction rollback (including savepoint rollbacks). We
    // can't distinguish the two from the callback, so any rollback drops the
    // pending capture unit: a full rollback discards uncommitted events
    // (correct), and a savepoint rollback sacrifices the whole transaction's
    // binlog record (documented limitation — savepoints are unsupported).
    let capture = unsafe { &*(p_arg as *const RefCell<Capture>) };
    let mut capture = capture.borrow_mut();
    if capture.unit.is_some() {
        tracing::warn!("binlog: rollback during an open capture unit; dropping the unit");
        capture.unit = None;
    }
}

#[allow(non_upper_case_globals)]
mod ffi_ext {
    use rusqlite::ffi::sqlite3;
    use std::os::raw::{c_char, c_int, c_void};

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum PreupdateValue {
        Old,
        New,
    }

    pub type PreupdateCallback = Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut sqlite3,
            c_int,
            *const c_char,
            *const c_char,
            i64,
            i64,
        ),
    >;
    pub type CommitCallback = Option<unsafe extern "C" fn(*mut c_void) -> c_int>;
    pub type RollbackCallback = Option<unsafe extern "C" fn(*mut c_void)>;

    unsafe extern "C" {
        pub fn sqlite3_preupdate_hook(
            db: *mut sqlite3,
            cb: PreupdateCallback,
            p_arg: *mut c_void,
        ) -> *mut c_void;
        pub fn sqlite3_commit_hook(
            db: *mut sqlite3,
            cb: CommitCallback,
            p_arg: *mut c_void,
        ) -> *mut c_void;
        pub fn sqlite3_rollback_hook(
            db: *mut sqlite3,
            cb: RollbackCallback,
            p_arg: *mut c_void,
        ) -> *mut c_void;
    }
}

/// Install the three capture hooks on a connection.
pub fn install_hooks(conn: &rusqlite::Connection, capture: &RefCell<Capture>) {
    let db = unsafe { conn.handle() };
    let ptr = capture as *const RefCell<Capture> as *mut c_void;
    unsafe {
        ffi_ext::sqlite3_preupdate_hook(db, Some(preupdate_cb), ptr);
        ffi_ext::sqlite3_commit_hook(db, Some(commit_cb), ptr);
        ffi_ext::sqlite3_rollback_hook(db, Some(rollback_cb), ptr);
    }
}

/// Convert a `sqlite3_value*` (may be null) to a binlog `Value`.
pub unsafe fn sqlite_value_to_value(v: *mut sqlite3_value) -> Value {
    if v.is_null() {
        return Value::null();
    }
    match unsafe { ffi::sqlite3_value_type(v) } {
        ffi::SQLITE_INTEGER => Value::integer(unsafe { ffi::sqlite3_value_int64(v) }),
        ffi::SQLITE_FLOAT => Value::float(unsafe { ffi::sqlite3_value_double(v) }),
        ffi::SQLITE_TEXT => {
            let bytes = unsafe {
                let p = ffi::sqlite3_value_text(v);
                if p.is_null() {
                    &[]
                } else {
                    let len = ffi::sqlite3_value_bytes(v) as usize;
                    std::slice::from_raw_parts(p as *const u8, len)
                }
            };
            Value::text(String::from_utf8_lossy(bytes).into_owned())
        }
        ffi::SQLITE_BLOB => {
            let bytes = unsafe {
                let p = ffi::sqlite3_value_blob(v);
                if p.is_null() {
                    &[]
                } else {
                    let len = ffi::sqlite3_value_bytes(v) as usize;
                    std::slice::from_raw_parts(p as *const u8, len)
                }
            };
            Value::blob(bytes.to_vec())
        }
        _ => Value::null(),
    }
}

// ---------------------------------------------------------------------------
// Finalization: raw events -> proto events, plus DDL detection
// ---------------------------------------------------------------------------

pub struct FinalizeResult {
    pub rows: Vec<RowEvent>,
    pub ddl: Vec<DdlEvent>,
}

/// Resolve raw events into proto row events, querying `PRAGMA table_info` as
/// needed. Called from executor context (never from inside a hook).
///
/// `conn` must be the connection that captured the events.
pub fn finalize_unit(
    conn: &rusqlite::Connection,
    capture: &mut Capture,
) -> Result<Option<FinalizeResult>, AppError> {
    let unit = match capture.unit.take() {
        Some(u) => u,
        None => return Ok(None),
    };
    capture.commit_pending = false;

    if unit.poisoned {
        return Ok(None);
    }

    let schema_version = unit.latest_schema_version;
    let mut rows = Vec::with_capacity(unit.rows.len());
    for raw in &unit.rows {
        let (columns, _, pk_indices) =
            table_schema(conn, &mut capture.schema, schema_version, &raw.table)?;
        let pk_columns: Vec<String> = if !pk_indices.is_empty() {
            pk_indices.iter().map(|&i| columns[i].clone()).collect()
        } else if raw.rowid.is_some() {
            vec!["rowid".to_string()]
        } else {
            // WITHOUT ROWID without a declared PK: cannot happen (SQLite
            // requires a PK for WITHOUT ROWID). Defensive fallback: whole row.
            columns.clone()
        };

        let (pk_values, event_columns, values) = match raw.op {
            Op::Insert | Op::Update => {
                let new_row = raw.new_row.clone().unwrap_or_default();
                (Vec::new(), columns.clone(), new_row)
            }
            Op::Delete => {
                let old_row = raw.old_row.clone().unwrap_or_default();
                let pk_values: Vec<Value> = if !pk_indices.is_empty() {
                    pk_indices.iter().map(|&i| old_row[i].clone()).collect()
                } else if let Some(rowid) = raw.rowid {
                    vec![Value::integer(rowid)]
                } else {
                    old_row.clone()
                };
                (pk_values, Vec::new(), Vec::new())
            }
        };

        rows.push(RowEvent {
            table: raw.table.clone(),
            op: raw.op as i32,
            pk_columns: pk_columns.clone(),
            pk_values,
            columns: event_columns,
            values,
        });
    }

    let schema_changed = unit.schema_version_at_start != schema_version;
    let ddl = if schema_changed && !unit.stmts.is_empty() {
        vec![DdlEvent {
            statements: unit.stmts.clone(),
        }]
    } else {
        Vec::new()
    };

    if rows.is_empty() && ddl.is_empty() {
        return Ok(None);
    }
    Ok(Some(FinalizeResult { rows, ddl }))
}

/// Look up (columns, is_pk, pk_indices) for a table, fetching via
/// `PRAGMA table_info` on cache miss.
fn table_schema(
    conn: &rusqlite::Connection,
    cache: &mut SchemaCache,
    schema_version: i64,
    table: &str,
) -> Result<(Vec<String>, Vec<bool>, Vec<usize>), AppError> {
    let (columns, is_pk) = cache.get(schema_version, table, |t| {
        let mut columns = Vec::new();
        let mut is_pk = Vec::new();
        let quoted = t.replace('"', "\"\"");
        if let Ok(mut stmt) = conn.prepare(&format!("PRAGMA table_info(\"{quoted}\")"))
            && let Ok(mut rows) = stmt.query([])
        {
            while let Ok(Some(row)) = rows.next() {
                let name: String = row.get(1).unwrap_or_default();
                let pk: i64 = row.get(5).unwrap_or(0);
                columns.push(name);
                is_pk.push(pk > 0);
            }
        }
        (columns, is_pk)
    });
    let pk_indices: Vec<usize> = is_pk
        .iter()
        .enumerate()
        .filter(|(_, pk)| **pk)
        .map(|(i, _)| i)
        .collect();
    Ok((columns, is_pk, pk_indices))
}

/// For tests and debugging: dump pending raw events of the open unit.
#[allow(dead_code)]
pub fn unit_stats(capture: &Capture) -> (usize, usize) {
    match &capture.unit {
        Some(u) => (u.rows.len(), u.stmts.len()),
        None => (0, 0),
    }
}

/// Convert a binlog value into a map-friendly debug string.
#[allow(dead_code)]
pub fn value_debug(v: &Value) -> String {
    match &v.value {
        Some(binlog::value::Value::Null(_)) => "NULL".into(),
        Some(binlog::value::Value::Integer(i)) => i.to_string(),
        Some(binlog::value::Value::Float(f)) => f.to_string(),
        Some(binlog::value::Value::Text(t)) => t.clone(),
        Some(binlog::value::Value::Blob(b)) => format!("<blob {} bytes>", b.len()),
        None => "UNKNOWN".into(),
    }
}
