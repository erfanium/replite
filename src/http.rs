//! HTTP router: libsql-server-compatible admin + Hrana routes, plus the new
//! sync endpoints.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::executor::{StmtArgs, run_sequence, run_stmt};
use crate::hrana::proto::PipelineReqBody;
use crate::namespace::{NamespaceManager, NamespaceName};
use crate::pipeline;
use crate::stream::{StreamGuard, StreamRegistry};
use crate::sync;
use prost::Message as _;

#[derive(Clone)]
pub struct AppState {
    pub namespaces: Arc<NamespaceManager>,
    pub streams: Arc<StreamRegistry>,
}

pub fn build_router(state: AppState) -> Router {
    let user = Router::<AppState>::new()
        .route("/", post(handle_legacy_query))
        .route("/health", get(handle_health))
        .route("/version", get(handle_version))
        .route("/v2", get(handle_index_v2))
        .route("/v3", get(handle_index_v3))
        .route("/v3-protobuf", get(handle_index_v3))
        .route("/v2/pipeline", post(handle_pipeline_json))
        .route("/v3/pipeline", post(handle_pipeline_json))
        .route("/v3-protobuf/pipeline", post(handle_pipeline_protobuf));

    let admin = Router::<AppState>::new()
        .route(
            "/v1/namespaces/{namespace}/create",
            post(handle_create_namespace),
        )
        .route(
            "/v1/namespaces/{namespace}/config",
            get(handle_get_config).post(handle_post_config),
        )
        .route(
            "/v1/namespaces/{namespace}/checkpoint",
            post(handle_checkpoint),
        )
        .route(
            "/v1/namespaces/{namespace}/replication",
            get(handle_replication),
        )
        .route(
            "/v1/namespaces/{namespace}",
            delete(handle_delete_namespace),
        );

    let sync = Router::<AppState>::new()
        .route(
            "/sync/v1/namespaces/{namespace}/info",
            get(sync::handle_info),
        )
        .route(
            "/sync/v1/namespaces/{namespace}/binlog",
            get(sync::handle_binlog),
        )
        .route(
            "/sync/v1/namespaces/{namespace}/clone",
            get(sync::handle_clone),
        );

    user.merge(admin).merge(sync).with_state(state)
}

// ---------------------------------------------------------------------------
// Namespace resolution (libsql-server fork compatibility)
// ---------------------------------------------------------------------------

/// Namespace from the `x-namespace` header; requests without the header go to
/// the default namespace (auto-created on demand, like libsql-server's
/// default namespace). Explicit namespaces must exist.
fn namespace_from_request(headers: &HeaderMap) -> Result<NamespaceName, AppError> {
    if let Some(value) = headers.get("x-namespace") {
        let raw = value.to_str().map_err(|_| {
            AppError::bad_request(
                "INVALID_NAMESPACE",
                "x-namespace header must be valid UTF-8",
            )
        })?;
        NamespaceName::from_string(raw.to_string())
            .map_err(|e| AppError::bad_request("INVALID_NAMESPACE", e.to_string()))
    } else {
        Ok(NamespaceName::default())
    }
}

async fn resolve_user_namespace(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Arc<crate::namespace::Namespace>, AppError> {
    let name = namespace_from_request(headers)?;
    if headers.get("x-namespace").is_some() {
        state
            .namespaces
            .get(&name)
            .await
            .ok_or_else(|| AppError::not_found(format!("namespace {name} does not exist")))
    } else {
        // default namespace: auto-create
        state
            .namespaces
            .create(&name)
            .await
            .map_err(|e| AppError::internal(e.to_string()))
    }
}

fn path_namespace(raw: String) -> Result<NamespaceName, AppError> {
    NamespaceName::from_string(raw)
        .map_err(|e| AppError::bad_request("INVALID_NAMESPACE", e.to_string()))
}

// ---------------------------------------------------------------------------
// Hrana pipeline endpoints
// ---------------------------------------------------------------------------

async fn handle_pipeline_json(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Result<Response, AppError> {
    let req: PipelineReqBody = serde_json::from_str(&body)
        .map_err(|e| AppError::bad_request("BAD_REQUEST", format!("cannot parse pipeline: {e}")))?;
    let resp = run_pipeline(&state, &headers, &req).await?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&resp).unwrap(),
    )
        .into_response())
}

async fn handle_pipeline_protobuf(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let req = PipelineReqBody::decode(body.as_ref())
        .map_err(|e| AppError::bad_request("BAD_REQUEST", format!("cannot parse pipeline: {e}")))?;
    let resp = run_pipeline(&state, &headers, &req).await?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-protobuf")],
        resp.encode_to_vec(),
    )
        .into_response())
}

/// Acquire the request's stream (creating one for baton-less requests),
/// execute the pipeline against it and release the stream, returning the
/// next baton (or none when the stream was closed).
async fn run_pipeline(
    state: &AppState,
    headers: &HeaderMap,
    req: &PipelineReqBody,
) -> Result<crate::hrana::proto::PipelineRespBody, AppError> {
    let fresh = req.baton.is_none();
    let guard: StreamGuard = if fresh {
        let ns = resolve_user_namespace(state, headers).await?;
        state.streams.acquire(None, || ns.open_stream_handle())?
    } else {
        state
            .streams
            .acquire(req.baton.as_deref(), || unreachable!())?
    };

    let (mut resp, closed) = pipeline::handle_pipeline(guard.handle(), req, fresh);
    resp.baton = guard.release(closed);
    Ok(resp)
}

async fn handle_index_v2() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "version": 2 }))
}

async fn handle_index_v3() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "version": 3 }))
}

// ---------------------------------------------------------------------------
// Legacy v1 endpoint (POST /)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct LegacyQuery {
    #[serde(default)]
    statements: Vec<LegacyStatement>,
}

#[derive(Deserialize)]
struct LegacyStatement {
    q: String,
    #[serde(default = "default_params")]
    params: LegacyParams,
}

fn default_params() -> LegacyParams {
    LegacyParams::Named(HashMap::new())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum LegacyParams {
    Positional(Vec<serde_json::Value>),
    Named(HashMap<String, serde_json::Value>),
}

#[derive(Serialize)]
struct LegacyResult {
    results: LegacyRows,
    success: bool,
}

#[derive(Serialize)]
struct LegacyRows {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

async fn handle_legacy_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(query): Json<LegacyQuery>,
) -> Result<Response, AppError> {
    let ns = resolve_user_namespace(&state, &headers).await?;
    let handle = ns.handle.lock().await;

    let mut results = Vec::with_capacity(query.statements.len());
    let mut err: Option<AppError> = None;

    let multi = query.statements.len() > 1;
    if multi {
        let _ = run_sequence(&handle, "BEGIN IMMEDIATE");
    }

    for stmt in &query.statements {
        let args = match &stmt.params {
            LegacyParams::Positional(values) => StmtArgs {
                args: values.iter().map(legacy_value_to_hrana).collect(),
                named_args: Vec::new(),
            },
            LegacyParams::Named(values) => StmtArgs {
                args: Vec::new(),
                named_args: values
                    .iter()
                    .map(|(name, v)| crate::hrana::proto::NamedArg {
                        name: name.clone(),
                        value: legacy_value_to_hrana(v),
                    })
                    .collect(),
            },
        };
        match run_stmt(&handle, &stmt.q, &args, true) {
            Ok(result) => {
                let rows: Vec<Vec<serde_json::Value>> = result
                    .rows
                    .iter()
                    .map(|row| row.values.iter().map(hrana_value_to_json).collect())
                    .collect();
                let columns = result
                    .cols
                    .iter()
                    .map(|c| c.name.clone().unwrap_or_default())
                    .collect();
                results.push(LegacyResult {
                    results: LegacyRows { columns, rows },
                    success: true,
                });
            }
            Err(e) => {
                err = Some(AppError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    e.code,
                    e.message,
                ));
                break;
            }
        }
    }

    if multi {
        if err.is_some() {
            let _ = run_sequence(&handle, "ROLLBACK");
        } else {
            let _ = run_sequence(&handle, "COMMIT");
        }
    }
    drop(handle);

    if let Some(err) = err {
        return Err(err);
    }

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&serde_json::json!({ "results": results })).unwrap(),
    )
        .into_response())
}

fn legacy_value_to_hrana(v: &serde_json::Value) -> crate::hrana::proto::Value {
    match v {
        serde_json::Value::Null => crate::hrana::proto::Value::null(),
        serde_json::Value::Bool(b) => crate::hrana::proto::Value::integer(*b as i64),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                crate::hrana::proto::Value::integer(i)
            } else {
                crate::hrana::proto::Value::float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => crate::hrana::proto::Value::text(s.clone()),
        serde_json::Value::Object(map) => {
            if let Some(b64) = map.get("base64").and_then(|v| v.as_str()) {
                match STANDARD_NO_PAD.decode(b64) {
                    Ok(bytes) => crate::hrana::proto::Value::blob(bytes),
                    Err(_) => crate::hrana::proto::Value::null(),
                }
            } else {
                crate::hrana::proto::Value::null()
            }
        }
        serde_json::Value::Array(_) => crate::hrana::proto::Value::null(),
    }
}

fn hrana_value_to_json(v: &crate::hrana::proto::Value) -> serde_json::Value {
    use crate::hrana::proto::Value as V;
    match v {
        V::None | V::Null => serde_json::Value::Null,
        V::Integer { value } => serde_json::Value::Number((*value).into()),
        V::Float { value } => serde_json::json!(value),
        V::Text { value } => serde_json::Value::String(value.to_string()),
        V::Blob { value } => serde_json::json!({ "base64": STANDARD_NO_PAD.encode(value) }),
    }
}

// ---------------------------------------------------------------------------
// Health / version
// ---------------------------------------------------------------------------

#[axum::debug_handler]
async fn handle_health() -> StatusCode {
    StatusCode::OK
}

async fn handle_version() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "minimum_supported_libsql_version": "0.6.0",
    }))
}

// ---------------------------------------------------------------------------
// Admin routes (/v1/namespaces/*)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct NamespaceConfig {
    block_reads: bool,
    block_writes: bool,
    allow_attach: bool,
}

#[derive(Deserialize)]
struct NamespaceConfigUpdate {
    #[serde(default)]
    #[allow(dead_code)]
    block_reads: Option<bool>,
    #[serde(default)]
    #[allow(dead_code)]
    block_writes: Option<bool>,
    #[serde(default)]
    #[allow(dead_code)]
    allow_attach: Option<bool>,
}

async fn handle_create_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Response, AppError> {
    let name = path_namespace(ns)?;
    state
        .namespaces
        .create(&name)
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok((StatusCode::OK, "{}").into_response())
}

async fn handle_get_config(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Json<NamespaceConfig>, AppError> {
    let name = path_namespace(ns)?;
    state
        .namespaces
        .get(&name)
        .await
        .ok_or_else(|| AppError::not_found(format!("namespace {name} does not exist")))?;
    Ok(Json(NamespaceConfig {
        block_reads: false,
        block_writes: false,
        allow_attach: false,
    }))
}

async fn handle_post_config(
    State(state): State<AppState>,
    Path(ns): Path<String>,
    Json(_update): Json<NamespaceConfigUpdate>,
) -> Result<Json<NamespaceConfig>, AppError> {
    let name = path_namespace(ns)?;
    state
        .namespaces
        .get(&name)
        .await
        .ok_or_else(|| AppError::not_found(format!("namespace {name} does not exist")))?;
    Ok(Json(NamespaceConfig {
        block_reads: false,
        block_writes: false,
        allow_attach: false,
    }))
}

async fn handle_checkpoint(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Response, AppError> {
    let name = path_namespace(ns)?;
    let ns = state
        .namespaces
        .get(&name)
        .await
        .ok_or_else(|| AppError::not_found(format!("namespace {name} does not exist")))?;
    let handle = ns.handle.lock().await;
    handle
        .conn
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(AppError::from)?;
    Ok((StatusCode::OK, "{}").into_response())
}

async fn handle_replication() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "primary": {
            "url": null,
        },
        "replicas": [],
    }))
}

async fn handle_delete_namespace(
    State(state): State<AppState>,
    Path(ns): Path<String>,
) -> Result<Response, AppError> {
    let name = path_namespace(ns)?;
    match state.namespaces.delete(&name).await {
        Ok(true) => Ok((StatusCode::OK, "{}").into_response()),
        Ok(false) => Err(AppError::not_found(format!(
            "namespace {name} does not exist"
        ))),
        Err(e) => Err(AppError::internal(e.to_string())),
    }
}
