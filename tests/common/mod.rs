//! Shared test harness for the integration tests:
//! - `TestServer`: the real axum router over a temp-dir namespace store,
//!   driven through `tower::oneshot` (real HTTP, no sockets).
//! - Hrana request builders (`execute`, `seq`, `close`, `batch`).
//! - The standalone binlog applier (`apply_binlog`) — intentionally uses
//!   only the documented wire format, the same algorithm a mobile client
//!   implements, with no server code.
//! - `compare_dbs` / `assert_dbs_equal`: schema (`sqlite_master`) + row
//!   comparison between primary and replica.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode, header};
use http_body_util::BodyExt;
use prost::Message as _;
use rusqlite::Connection;
use replite::binlog::value;
use replite::binlog::{Op, Transaction, Value as BValue};
use replite::hrana::proto::{BatchStep, PipelineReqBody, StreamRequest};
use replite::http::{AppState, build_router};
use replite::namespace::NamespaceManager;
use tower::ServiceExt;

pub struct TestServer {
    pub app: axum::Router,
    pub dir: PathBuf,
    _dir: tempfile::TempDir,
}

impl TestServer {
    pub fn new() -> Self {
        Self::with_limits(1024 * 1024, 16 * 1024 * 1024)
    }

    pub fn with_limits(max_segment_bytes: u64, max_binlog_bytes: u64) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let namespaces = Arc::new(
            NamespaceManager::open(
                dir.path().to_path_buf(),
                max_segment_bytes,
                max_binlog_bytes,
            )
            .unwrap(),
        );
        let streams = replite::stream::StreamRegistry::new();
        let app = build_router(AppState {
            namespaces,
            streams,
        });
        let dir_path = dir.path().to_path_buf();
        TestServer {
            app,
            dir: dir_path,
            _dir: dir,
        }
    }

    pub async fn call(
        &self,
        method: &str,
        uri: &str,
        body: Option<Vec<u8>>,
    ) -> (StatusCode, Vec<u8>, HeaderMap) {
        let req = {
            let mut builder = Request::builder().method(method).uri(uri);
            if body.is_some() {
                builder = builder.header(header::CONTENT_TYPE, "application/json");
            }
            builder.body(Body::from(body.unwrap_or_default())).unwrap()
        };
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, bytes, headers)
    }

    pub async fn post_json(
        &self,
        uri: &str,
        json: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let (status, bytes, _) = self
            .call("POST", uri, Some(serde_json::to_vec(&json).unwrap()))
            .await;
        let body = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, body)
    }

    pub async fn pipeline_json(
        &self,
        ns: &str,
        req: &PipelineReqBody,
    ) -> (StatusCode, serde_json::Value) {
        let uri = "/v2/pipeline".to_string();
        let json = serde_json::to_value(req).unwrap();
        let (status, body) = self.post_json_with_ns(&uri, ns, json).await;
        (status, body)
    }

    async fn post_json_with_ns(
        &self,
        uri: &str,
        ns: &str,
        json: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let mut b = Request::builder().method("POST").uri(uri);
        b = b.header(header::CONTENT_TYPE, "application/json");
        b = b.header("x-namespace", ns);
        let resp = self
            .app
            .clone()
            .oneshot(
                b.body(Body::from(serde_json::to_vec(&json).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        let body = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, body)
    }

    /// Execute SQL via the protobuf pipeline endpoint (exactly what
    /// `@libsql/client` 0.17 does).
    pub async fn pipeline_protobuf(&self, ns: &str, req: &PipelineReqBody) -> (StatusCode, Vec<u8>) {
        let mut b = Request::builder()
            .method("POST")
            .uri("/v3-protobuf/pipeline");
        b = b.header(header::CONTENT_TYPE, "application/x-protobuf");
        b = b.header("x-namespace", ns);
        let resp = self
            .app
            .clone()
            .oneshot(b.body(Body::from(req.encode_to_vec())).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, bytes)
    }

    pub async fn fetch_binlog(&self, ns: &str, since: u64) -> (StatusCode, Vec<u8>, HeaderMap) {
        self.call(
            "GET",
            &format!("/sync/v1/namespaces/{ns}/binlog?since={since}"),
            None,
        )
        .await
    }

    pub async fn clone_db(&self, ns: &str) -> (StatusCode, Vec<u8>, HeaderMap) {
        self.call("GET", &format!("/sync/v1/namespaces/{ns}/clone"), None)
            .await
    }

    pub fn primary_path(&self, ns: &str) -> PathBuf {
        self.dir.join(ns).join("db.sqlite")
    }

    pub async fn info(&self, ns: &str) -> serde_json::Value {
        let (_, bytes, _) = self
            .call("GET", &format!("/sync/v1/namespaces/{ns}/info"), None)
            .await;
        serde_json::from_slice(&bytes).unwrap()
    }
}

// ---------------------------------------------------------------------------
// Hrana request builders
// ---------------------------------------------------------------------------

pub fn execute(sql: &str, want_rows: bool) -> StreamRequest {
    StreamRequest::Execute(replite::hrana::proto::ExecuteStreamReq {
        stmt: replite::hrana::proto::Stmt::new(sql, want_rows),
    })
}

pub fn seq(sql: &str) -> StreamRequest {
    StreamRequest::Sequence(replite::hrana::proto::SequenceStreamReq {
        sql: Some(sql.to_string()),
        sql_id: None,
        replication_index: None,
    })
}

pub fn close() -> StreamRequest {
    StreamRequest::Close(Default::default())
}

pub fn batch(steps: Vec<BatchStep>) -> StreamRequest {
    StreamRequest::Batch(replite::hrana::proto::BatchStreamReq {
        batch: replite::hrana::proto::Batch {
            steps,
            replication_index: None,
        },
    })
}

// ---------------------------------------------------------------------------
// Binlog applier (the algorithm the mobile client implements)
// ---------------------------------------------------------------------------

/// Read a varint from a byte slice; returns (value, bytes_consumed).
pub fn read_varint(buf: &[u8]) -> (u64, usize) {
    let mut v = 0u64;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return (v, i + 1);
        }
        shift += 7;
    }
    panic!("truncated varint")
}

/// Apply a binlog body to a fresh replica. Returns the last applied LSN.
pub fn apply_binlog(conn: &Connection, body: &[u8]) -> u64 {
    let mut pos = 0usize;
    let mut last_lsn = 0u64;
    while pos < body.len() {
        let (len, n) = read_varint(&body[pos..]);
        pos += n;
        let tx = Transaction::decode(&body[pos..pos + len as usize]).unwrap();
        pos += len as usize;
        apply_tx(conn, &tx);
        last_lsn = tx.lsn;
    }
    last_lsn
}

fn apply_tx(conn: &Connection, tx: &Transaction) {
    conn.pragma_update(None, "foreign_keys", "OFF").unwrap();

    // DDL first, verbatim.
    for ddl in &tx.ddl {
        for stmt in &ddl.statements {
            conn.execute_batch(stmt)
                .unwrap_or_else(|e| panic!("failed to apply DDL {stmt:?} for lsn {}: {e}", tx.lsn));
        }
    }

    // Row events, all inside one transaction (like the source transaction).
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    for ev in &tx.rows {
        match Op::try_from(ev.op).unwrap() {
            Op::Insert | Op::Update => upsert(conn, ev),
            Op::Delete => delete(conn, ev),
        }
    }
    conn.execute_batch("COMMIT").unwrap();
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn upsert(conn: &Connection, ev: &replite::binlog::RowEvent) {
    let cols: Vec<&str> = ev.columns.iter().map(|c| c.as_str()).collect();
    let col_list: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect();
    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("?{i}")).collect();
    let conflict: String = ev
        .pk_columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let set_clause: Vec<String> = cols
        .iter()
        .map(|c| format!("{} = excluded.{}", quote_ident(c), quote_ident(c)))
        .collect();
    let sql = format!(
        "INSERT INTO {t} ({cols}) VALUES ({ph}) \
         ON CONFLICT({conflict}) DO UPDATE SET {set}",
        t = quote_ident(&ev.table),
        cols = col_list.join(", "),
        ph = placeholders.join(", "),
        conflict = conflict,
        set = set_clause.join(", "),
    );
    let values: Vec<rusqlite::types::Value> = ev.values.iter().map(bvalue_to_owned).collect();
    conn.execute(&sql, rusqlite::params_from_iter(values))
        .unwrap_or_else(|e| panic!("upsert into {} failed: {e}; sql={sql}", ev.table));
}

fn delete(conn: &Connection, ev: &replite::binlog::RowEvent) {
    let cond: Vec<String> = ev
        .pk_columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{} = ?{}", quote_ident(c), i + 1))
        .collect();
    let sql = format!(
        "DELETE FROM {t} WHERE {cond}",
        t = quote_ident(&ev.table),
        cond = cond.join(" AND "),
    );
    let values: Vec<rusqlite::types::Value> = ev.pk_values.iter().map(bvalue_to_owned).collect();
    conn.execute(&sql, rusqlite::params_from_iter(values))
        .unwrap();
}

fn bvalue_to_owned(v: &BValue) -> rusqlite::types::Value {
    match &v.value {
        Some(value::Value::Null(_)) => rusqlite::types::Value::Null,
        Some(value::Value::Integer(i)) => rusqlite::types::Value::Integer(*i),
        Some(value::Value::Float(f)) => rusqlite::types::Value::Real(*f),
        Some(value::Value::Text(t)) => rusqlite::types::Value::Text(t.clone()),
        Some(value::Value::Blob(b)) => rusqlite::types::Value::Blob(b.clone()),
        None => rusqlite::types::Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Row dump + comparison helpers
// ---------------------------------------------------------------------------

/// Dump every user table's content as sorted string rows, for comparison.
pub fn dump_db(conn: &Connection) -> BTreeMap<String, Vec<Vec<String>>> {
    let mut out = BTreeMap::new();
    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' \
             AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .unwrap();
    let tables: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    drop(stmt);
    for t in tables {
        let stmt = conn
            .prepare(&format!("SELECT * FROM {}", quote_ident(&t)))
            .unwrap();
        let ncols = stmt.column_count();
        let order: Vec<String> = (1..=ncols).map(|i| i.to_string()).collect();
        let sql = format!(
            "SELECT * FROM {} ORDER BY {}",
            quote_ident(&t),
            order.join(", ")
        );
        drop(stmt);
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut rows = Vec::new();
        let mut query = stmt.query([]).unwrap();
        while let Some(row) = query.next().unwrap() {
            let mut vals = Vec::new();
            for i in 0..row.as_ref().column_count() {
                vals.push(match row.get_ref(i).unwrap() {
                    rusqlite::types::ValueRef::Null => "NULL".to_string(),
                    rusqlite::types::ValueRef::Integer(v) => format!("i{v}"),
                    rusqlite::types::ValueRef::Real(v) => format!("r{v}"),
                    rusqlite::types::ValueRef::Text(v) => match std::str::from_utf8(v) {
                        // Lossless rendering: non-UTF-8 TEXT is hex-encoded
                        // with a marker, so the comparator can see through
                        // `String::from_utf8_lossy`-style mangling.
                        Ok(s) => format!("s{s}"),
                        Err(_) => format!("s!{}", hex(v)),
                    },
                    rusqlite::types::ValueRef::Blob(v) => format!("b{}", hex(v)),
                });
            }
            rows.push(vals);
        }
        rows.sort();
        out.insert(t, rows);
    }
    out
}

/// Dump the schema (tables, indexes, views, triggers) as a sorted list of
/// (type, name, tbl_name, sql) tuples. `sql` is NULL for auto-indexes.
pub fn dump_schema(conn: &Connection) -> Vec<(String, String, String, String)> {
    let mut stmt = conn
        .prepare(
            "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_master \
             WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name, tbl_name",
        )
        .unwrap();
    stmt.query_map([], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn format_seq_diff(label: &str, a: &[(String, String, String, String)], b: &[(String, String, String, String)]) -> String {
    let mut out = String::new();
    out.push_str(&format!("{label}: primary has {} entries, replica has {}\n", a.len(), b.len()));
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        if x != y {
            out.push_str(&format!("  [{i}] primary={x:?}\n  [{i}] replica={y:?}\n"));
        }
    }
    out
}

/// Compare primary vs replica; returns a human-readable diff, or None if the
/// two databases are identical (schema + rows).
pub fn compare_dbs(primary: &Connection, replica: &Connection) -> Option<String> {
    let mut out = String::new();

    let ps = dump_schema(primary);
    let rs = dump_schema(replica);
    if ps != rs {
        out.push_str("schema mismatch:\n");
        out.push_str(&format_seq_diff("sqlite_master", &ps, &rs));
    }

    let p = dump_db(primary);
    let r = dump_db(replica);
    if p != r {
        out.push_str("row mismatch:\n");
        let tables: BTreeSet<&String> = p.keys().chain(r.keys()).collect();
        for t in tables {
            let a = p.get(t);
            let b = r.get(t);
            if a != b {
                out.push_str(&format!(
                    "  table {t}: primary has {} rows, replica has {} rows\n",
                    a.map(|v| v.len()).unwrap_or(0),
                    b.map(|v| v.len()).unwrap_or(0),
                ));
                if let (Some(a), Some(b)) = (a, b) {
                    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
                        if x != y {
                            out.push_str(&format!(
                                "    row[{i}]: primary={x:?} replica={y:?}\n"
                            ));
                        }
                    }
                }
            }
        }
    }

    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

pub fn assert_dbs_equal(primary: &Connection, replica: &Connection) {
    if let Some(diff) = compare_dbs(primary, replica) {
        panic!("primary and replica diverged:\n{diff}");
    }
}

pub fn base64_blob(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes)
}

pub fn blob_bytes(b64: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(b64)
        .unwrap()
}

// ---------------------------------------------------------------------------
// Differential scenario engine
// ---------------------------------------------------------------------------
//
// Shared by the random property test (`differential_convergence.rs`) and the
// exact-scenario runner (`exact_scenarios.rs`): run statements on a primary,
// apply the binlog to a fresh replica, and compare the two databases.

pub mod differential {
    use axum::http::StatusCode;
    use rusqlite::Connection;
    use replite::hrana::proto::PipelineReqBody;

    use super::{TestServer, apply_binlog, compare_dbs, seq};

    /// Run the statements on a fresh primary, apply the binlog to a fresh
    /// replica, and return a diff description if the two diverged
    /// (None = converged).
    pub async fn check(stmts: &[String]) -> Option<String> {
        let server = TestServer::new();
        let ns = "diff";
        let (status, _) = server
            .post_json(&format!("/v1/namespaces/{ns}/create"), serde_json::json!({}))
            .await;
        assert_eq!(status, StatusCode::OK, "namespace creation failed");

        for s in stmts {
            let req = PipelineReqBody {
                baton: None,
                requests: vec![seq(s)],
            };
            let (status, body) = server.pipeline_json(ns, &req).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "pipeline request failed for {s:?}: {body}"
            );
            // Per-request errors (a duplicate-PK INSERT, etc.) are reported as
            // results, not HTTP failures — and must simply not advance the LSN.
        }

        let (status, binlog, _) = server.fetch_binlog(ns, 0).await;
        assert_eq!(status, StatusCode::OK, "binlog fetch failed");

        let replica = Connection::open_in_memory().unwrap();
        apply_binlog(&replica, &binlog);

        let primary = Connection::open(server.primary_path(ns)).unwrap();
        compare_dbs(&primary, &replica)
    }

    /// Reduce a failing sequence to a minimal reproducer: drop statements
    /// from the tail while the divergence persists.
    pub async fn shrink(original: &[String]) -> Vec<String> {
        let mut best = original.to_vec();
        loop {
            let mut changed = false;
            for cut in (1..best.len()).rev() {
                let candidate = best[..cut].to_vec();
                if check(&candidate).await.is_some() {
                    best = candidate;
                    changed = true;
                    break;
                }
            }
            if !changed {
                break;
            }
        }
        best
    }

    /// Render a statement list one-per-line, for panics and reproducers.
    pub fn display(stmts: &[String]) -> String {
        stmts
            .iter()
            .map(|s| format!("    {s}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}
