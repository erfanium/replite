//! Sync endpoints for the replication protocol (server -> device):
//! - `GET /sync/v1/namespaces/{ns}/info`    — LSN watermarks
//! - `GET /sync/v1/namespaces/{ns}/binlog?since=N` — change stream as SSE
//! - `GET /sync/v1/namespaces/{ns}/clone`   — physical snapshot (raw db file)
//!
//! The binlog is a Server-Sent Events stream: one event per committed
//! transaction, `data:` being `{"lsn": <u64>, "statements": [<sql>, ...]}`.
//! Statements are ready to apply: DDL is replayed verbatim, row changes are
//! materialized as statements with literal values (never the original DML,
//! which may be non-deterministic). Each event's `id` is the transaction's
//! LSN. A mid-stream read failure emits an `event: error` and closes; a
//! clean EOF means the stream is complete. The clone response carries
//! `X-LSN` (the binlog position the snapshot corresponds to) and a
//! `Content-Length`, so clients can report progress as a percentage of bytes
//! received.

use std::convert::Infallible;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::response::sse::{Event, Sse};
use futures_util::StreamExt;

use crate::binlog::{Op, RowEvent, Transaction, Value, value};
use crate::error::AppError;
use crate::http::AppState;
use crate::namespace::{Namespace, NamespaceName};
use crate::pipeline::rollback_stray_tx;

async fn resolve(state: &AppState, name: NamespaceName) -> Result<Arc<Namespace>, AppError> {
    state
        .namespaces
        .get(&name)
        .await
        .ok_or_else(|| AppError::not_found(format!("namespace {name} does not exist")))
}

#[derive(serde::Deserialize)]
pub struct BinlogQuery {
    /// Fetch transactions with lsn > since.
    #[serde(default)]
    pub since: u64,
}

#[derive(serde::Serialize)]
pub struct InfoResponse {
    /// LSN of the last committed transaction in the binlog.
    pub current_lsn: u64,
    /// LSN of the first retained transaction. Clients with a local LSN below
    /// this must re-clone.
    pub min_lsn: u64,
    /// PRAGMA schema_version of the primary database.
    pub schema_version: i64,
}

pub async fn handle_info(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<InfoResponse>, AppError> {
    let name = NamespaceName::from_string(ns)?;
    let ns = resolve(&state, name).await?;
    let schema_version = {
        let handle = ns.handle.lock().await;
        handle.schema_version()
    };
    let binlog = ns.binlog.lock().unwrap();
    Ok(Json(InfoResponse {
        current_lsn: binlog.current_lsn(),
        min_lsn: binlog.min_lsn(),
        schema_version,
    }))
}

/// One committed transaction as served on the wire: a ready-to-apply
/// statement list, keyed by its LSN.
#[derive(serde::Serialize)]
pub struct TxPayload {
    pub lsn: u64,
    pub statements: Vec<String>,
}

pub async fn handle_binlog(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Query(query): Query<BinlogQuery>,
) -> Result<Response, AppError> {
    let name = NamespaceName::from_string(ns)?;
    let ns = resolve(&state, name).await?;

    // Segment handles are opened up-front under the lock (GC can then delete
    // segments mid-stream without invalidating them); the lock is not held
    // for the response itself.
    let (iter, current_lsn, min_lsn) = {
        let binlog = ns.binlog.lock().unwrap();
        if query.since < binlog.min_lsn() {
            return Err(AppError::conflict(
                "BINLOG_LAG",
                format!(
                    "requested LSN {} is older than the oldest retained binlog \
                     record ({}); the client must re-clone the database",
                    query.since,
                    binlog.min_lsn()
                ),
            ));
        }
        (binlog.iter_since(query.since)?, binlog.current_lsn(), binlog.min_lsn())
    };

    // One SSE event per transaction; a mid-stream read error is surfaced as
    // an `error` event so the client can tell failure from clean EOF.
    let stream = futures_util::stream::iter(iter).map(|res| {
        let event = match res {
            Ok(tx) => {
                let payload = TxPayload {
                    lsn: tx.lsn,
                    statements: tx_statements(&tx),
                };
                match Event::default().id(tx.lsn.to_string()).json_data(payload) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!("binlog: failed to serialize event: {e}");
                        error_event("BINLOG_SERIALIZE", &e.to_string())
                    }
                }
            }
            Err(e) => {
                tracing::error!("binlog: stream read failed: {e}");
                error_event("BINLOG_READ", &e.to_string())
            }
        };
        Ok::<Event, Infallible>(event)
    });

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-current-lsn",
        HeaderValue::from_str(&current_lsn.to_string()).unwrap(),
    );
    headers.insert(
        "x-min-lsn",
        HeaderValue::from_str(&min_lsn.to_string()).unwrap(),
    );
    headers.insert(
        "x-namespace",
        HeaderValue::from_str(ns.name.as_str()).unwrap(),
    );

    Ok((headers, Sse::new(stream)).into_response())
}

fn error_event(code: &str, message: &str) -> Event {
    Event::default()
        .event("error")
        .json_data(serde_json::json!({ "code": code, "message": message }))
        .unwrap_or_default()
}

pub async fn handle_clone(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Response, AppError> {
    let name = NamespaceName::from_string(ns)?;
    let ns = resolve(&state, name).await?;
    let handle = ns.handle.lock().await;

    // A fresh snapshot must not include any in-flight transaction.
    rollback_stray_tx(&handle);
    // Checkpoint so the db file alone contains the full snapshot.
    handle
        .conn
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(AppError::from)?;

    let lsn = handle.binlog.lock().unwrap().current_lsn();
    let bytes = std::fs::read(ns.db_path()).map_err(|e| AppError::internal(e.to_string()))?;
    drop(handle);

    let mut headers = HeaderMap::new();
    headers.insert("x-lsn", HeaderValue::from_str(&lsn.to_string()).unwrap());
    headers.insert(
        "x-namespace",
        HeaderValue::from_str(ns.name.as_str()).unwrap(),
    );
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.sqlite3"),
    );

    // Content-Length is set automatically by axum for byte bodies; it lets
    // clients compute clone progress (percent = bytes_received / length).
    Ok((StatusCode::OK, headers, bytes).into_response())
}

/// Render one committed transaction as a ready-to-apply statement list: DDL
/// statements replayed verbatim, row changes materialized as statements with
/// literal values. The caller applies the list atomically; no BEGIN/COMMIT
/// are included (the transaction boundary is the list itself).
fn tx_statements(tx: &Transaction) -> Vec<String> {
    let mut out = Vec::new();
    for ddl in &tx.ddl {
        for stmt in &ddl.statements {
            let stmt = stmt.trim_end();
            if stmt.ends_with(';') {
                out.push(stmt.to_string());
            } else {
                out.push(format!("{stmt};"));
            }
        }
    }
    for row in &tx.rows {
        out.push(row_sql(row));
    }
    out
}

fn row_sql(row: &RowEvent) -> String {
    let table = quote_ident(&row.table);
    match Op::try_from(row.op) {
        Ok(Op::Insert) => {
            // Upsert on the PK so re-applying a stream suffix is idempotent
            // (a client crash between apply and LSN persist re-applies the
            // same transactions).
            let cols = row.columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
            let vals = row.values.iter().map(sql_literal).collect::<Vec<_>>().join(", ");
            let assignable: Vec<&String> = row
                .columns
                .iter()
                .filter(|c| !row.pk_columns.contains(c))
                .collect();
            let on_conflict = if assignable.is_empty() {
                "DO NOTHING".to_string()
            } else {
                let set = assignable
                    .iter()
                    .map(|c| format!("{} = excluded.{}", quote_ident(c), quote_ident(c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("DO UPDATE SET {set}")
            };
            format!(
                "INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT({}) {on_conflict};",
                row.pk_columns.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", "),
            )
        }
        Ok(Op::Update) => {
            // UPDATE events carry only the after-image; PK columns are part of
            // it (PKs are never updated), so the WHERE clause is derived from
            // the PK column values in the full row.
            let sets = row
                .columns
                .iter()
                .zip(&row.values)
                .map(|(c, v)| format!("{} = {}", quote_ident(c), sql_literal(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("UPDATE {table} SET {sets} WHERE {};", row_where(row))
        }
        Ok(Op::Delete) => format!("DELETE FROM {table} WHERE {};", row_where(row)),
        Err(_) => panic!("unknown row opcode {}", row.op),
    }
}

/// WHERE clause targeting a row by its PK. For DELETE the PK values are stored
/// directly; for UPDATE they are looked up among the after-image columns.
fn row_where(row: &RowEvent) -> String {
    let conditions = row
        .pk_columns
        .iter()
        .map(|pk| {
            let value = if !row.pk_values.is_empty() {
                row.pk_values[row.pk_columns.iter().position(|c| c == pk).unwrap()].clone()
            } else {
                let idx = row
                    .columns
                    .iter()
                    .position(|c| c == pk)
                    .expect("PK column missing from after-image");
                row.values[idx].clone()
            };
            format!("{} = {}", quote_ident(pk), sql_literal(&value))
        })
        .collect::<Vec<_>>();
    conditions.join(" AND ")
}

/// Render a binlog value as a deterministic SQL literal.
fn sql_literal(v: &Value) -> String {
    match &v.value {
        None => "NULL".into(),
        Some(value::Value::Null(_)) => "NULL".into(),
        Some(value::Value::Integer(i)) => i.to_string(),
        Some(value::Value::Float(f)) => {
            // SQLite has no hexfloat literals, so round-trip through the
            // shortest decimal that reproduces the exact f64. Force an
            // integer-looking value to be a REAL literal (trailing ".0") and
            // spell out the non-finite cases (SQLite stores NaN as NULL).
            if f.is_nan() {
                "NULL".into()
            } else if *f == f64::INFINITY {
                "9e999".into()
            } else if *f == f64::NEG_INFINITY {
                "-9e999".into()
            } else if *f == 0.0 && f.is_sign_negative() {
                "-0.0".into()
            } else {
                let s = f.to_string();
                if s.contains('.') || s.contains('e') || s.contains('E') {
                    s
                } else {
                    format!("{s}.0")
                }
            }
        }
        Some(value::Value::Text(t)) => format!("'{}'", t.replace('\'', "''")),
        Some(value::Value::Blob(b)) => {
            let mut hex = String::with_capacity(b.len() * 2);
            for byte in b {
                hex.push_str(&format!("{byte:02x}"));
            }
            format!("X'{hex}'")
        }
    }
}

/// Quote an identifier; embedded double quotes are doubled.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binlog::{DdlEvent, RowEvent};
    use rusqlite::Connection;

    fn tx(rows: Vec<RowEvent>, ddl: Vec<DdlEvent>) -> Transaction {
        Transaction {
            lsn: 0,
            commit_ts_ms: 0,
            rows,
            ddl,
        }
    }

    fn insert_row(id: i64, v: Value) -> RowEvent {
        RowEvent {
            table: "t".into(),
            op: Op::Insert as i32,
            pk_columns: vec!["id".into()],
            pk_values: vec![],
            columns: vec!["id".into(), "v".into()],
            values: vec![Value::integer(id), v],
        }
    }

    /// Render the stream as per-tx statement lists and apply each
    /// transaction atomically to a fresh in-memory db; return it.
    fn apply(stream: &[Transaction], schema: &str) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(schema).unwrap();
        for tx in stream {
            let statements = tx_statements(tx);
            if statements.is_empty() {
                continue;
            }
            conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            for stmt in statements {
                conn.execute_batch(&stmt).unwrap();
            }
            conn.execute_batch("COMMIT").unwrap();
        }
        conn
    }

    fn dump_t(conn: &Connection) -> Vec<(i64, rusqlite::types::Value)> {
        let mut stmt = conn
            .prepare("SELECT id, v FROM t ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[test]
    fn renders_and_applies_dml() {
        let stream = [
            tx(
                vec![
                    insert_row(1, Value::text("alice's \"quoted\"")),
                    insert_row(2, Value::blob(vec![0xde, 0xad, 0xbe, 0xef])),
                    insert_row(3, Value::null()),
                    insert_row(4, Value::float(1.0)),
                ],
                vec![],
            ),
            tx(
                vec![RowEvent {
                    table: "t".into(),
                    op: Op::Update as i32,
                    pk_columns: vec!["id".into()],
                    pk_values: vec![],
                    columns: vec!["id".into(), "v".into()],
                    values: vec![Value::integer(1), Value::text("updated")],
                }],
                vec![],
            ),
            tx(
                vec![RowEvent {
                    table: "t".into(),
                    op: Op::Delete as i32,
                    pk_columns: vec!["id".into()],
                    pk_values: vec![Value::integer(2)],
                    columns: vec![],
                    values: vec![],
                }],
                vec![],
            ),
        ];
        let conn = apply(
            &stream,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v);",
        );
        let mut stmt = conn
            .prepare("SELECT id, typeof(v), v FROM t ORDER BY id")
            .unwrap();
        let rows: Vec<(i64, String, rusqlite::types::Value)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        use rusqlite::types::Value as RValue;
        assert_eq!(
            rows,
            vec![
                (1, "text".into(), RValue::Text("updated".into())),
                (3, "null".into(), RValue::Null),
                // 1.0 stays a REAL (not an integer) on the replica.
                (4, "real".into(), RValue::Real(1.0)),
            ]
        );

        // Re-applying the whole stream must be idempotent (upserts): the
        // client may crash between apply and LSN persist.
        for tx in &stream {
            let statements = tx_statements(tx);
            conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            for stmt in statements {
                conn.execute_batch(&stmt).unwrap();
            }
            conn.execute_batch("COMMIT").unwrap();
        }
        assert_eq!(dump_t(&conn).len(), 3, "no phantom rows on re-apply");
    }

    #[test]
    fn renders_ddl_verbatim() {
        let conn = apply(
            &[tx(
                vec![],
                vec![DdlEvent {
                    statements: vec!["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)".into()],
                }],
            )],
            "",
        );
        let n: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 't'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn literal_roundtrips() {
        assert_eq!(sql_literal(&Value::text("it's")), "'it''s'");
        assert_eq!(sql_literal(&Value::blob(vec![0x00, 0xff])), "X'00ff'");
        assert_eq!(sql_literal(&Value::float(1.0)), "1.0");
        assert_eq!(sql_literal(&Value::float(-0.0)), "-0.0");
        assert_eq!(sql_literal(&Value::float(0.1)), "0.1");
        assert_eq!(sql_literal(&Value::float(f64::INFINITY)), "9e999");
        assert_eq!(sql_literal(&Value::null()), "NULL");
        assert_eq!(sql_literal(&Value::integer(-7)), "-7");
        assert_eq!(quote_ident("weird\"name"), "\"weird\"\"name\"");
    }

    #[test]
    fn rowid_fallback_and_multi_pk() {
        let conn = apply(
            &[tx(
                vec![
                    RowEvent {
                        table: "x".into(),
                        op: Op::Delete as i32,
                        pk_columns: vec!["rowid".into()],
                        pk_values: vec![Value::integer(7)],
                        columns: vec![],
                        values: vec![],
                    },
                    RowEvent {
                        table: "y".into(),
                        op: Op::Update as i32,
                        pk_columns: vec!["a".into(), "b".into()],
                        pk_values: vec![],
                        columns: vec!["a".into(), "b".into(), "c".into()],
                        values: vec![
                            Value::integer(1),
                            Value::text("k"),
                            Value::integer(99),
                        ],
                    },
                ],
                vec![],
            )],
            "CREATE TABLE x (v); INSERT INTO x (v) VALUES (0); \
             CREATE TABLE y (a, b, c, PRIMARY KEY (a, b)); \
             INSERT INTO y (a, b, c) VALUES (1, 'k', 0);",
        );
        let n: i64 = conn
            .query_row("SELECT count(*) FROM x WHERE rowid = 7", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        let c: i64 = conn
            .query_row("SELECT c FROM y WHERE a = 1 AND b = 'k'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(c, 99);
    }
}
