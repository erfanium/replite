//! Sync endpoints for the replication protocol (server -> device):
//! - `GET /sync/v1/namespaces/{ns}/info`    — LSN watermarks
//! - `GET /sync/v1/namespaces/{ns}/binlog?since=N` — row-level change stream
//! - `GET /sync/v1/namespaces/{ns}/clone`   — physical snapshot (raw db file)
//!
//! The binlog body is a stream of records: `[varint length][Transaction
//! protobuf]`, the same framing the server uses on disk. The clone response
//! carries `X-LSN` (the binlog position the snapshot corresponds to) and a
//! `Content-Length`, so clients can report progress as a percentage of bytes
//! received.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::error::AppError;
use crate::http::AppState;
use crate::namespace::{Namespace, NamespaceName};
use crate::pipeline::rollback_stray_tx;
use prost::Message as _;

pub const BINLOG_CONTENT_TYPE: &str = "application/x-sqlite-replication-binlog";

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

pub async fn handle_binlog(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Query(query): Query<BinlogQuery>,
) -> Result<Response, AppError> {
    let name = NamespaceName::from_string(ns)?;
    let ns = resolve(&state, name).await?;
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

    let txs = binlog.read_since(query.since)?;
    let current_lsn = binlog.current_lsn();
    let min_lsn = binlog.min_lsn();
    drop(binlog);

    let mut body = Vec::new();
    for tx in &txs {
        let bytes = tx.encode_to_vec();
        write_varint(&mut body, bytes.len() as u64);
        body.extend_from_slice(&bytes);
    }

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
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(BINLOG_CONTENT_TYPE),
    );

    Ok((headers, body).into_response())
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

fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    let mut tmp = [0u8; 10];
    let mut i = 0;
    while v >= 0x80 {
        tmp[i] = (v as u8) | 0x80;
        v >>= 7;
        i += 1;
    }
    tmp[i] = v as u8;
    buf.extend_from_slice(&tmp[..=i]);
}
