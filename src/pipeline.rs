//! Hrana pipeline execution against a namespace.

use std::collections::HashMap;

use crate::error::StmtError;
use crate::executor::{self, DbHandle, StmtArgs};
use crate::hrana::proto::{
    Batch, BatchCond, BatchResult, DescribeParam, DescribeResult, Error, ExecuteStreamResp,
    StmtResult,
};
use crate::hrana::proto::{
    PipelineReqBody, PipelineRespBody, StreamRequest, StreamResponse, StreamResult,
};

const MAX_STORED_SQL: usize = 5000;
const MAX_SQL_COUNT: usize = 50;

/// Execute one pipeline request body against a stream (connection).
///
/// [fresh] is true for baton-less requests, which behave like a fresh
/// connection: any transaction left open by an earlier abandoned request is
/// rolled back and the stored-SQL cache is cleared. Baton'd requests
/// (streams) keep their transaction and SQL state across requests.
///
/// Returns the response body and whether the stream was closed (the client
/// sent a `close` request — the caller then drops the stream instead of
/// returning a baton).
pub fn handle_pipeline(
    handle: &DbHandle,
    req: &PipelineReqBody,
    fresh: bool,
) -> (PipelineRespBody, bool) {
    if fresh {
        rollback_stray_tx(handle);
        handle.sqls.borrow_mut().clear();
    }

    let mut closed = false;
    let mut sqls = handle.sqls.borrow_mut();
    let mut results = Vec::with_capacity(req.requests.len());
    for request in &req.requests {
        let result = match request {
            StreamRequest::None => StreamResult::Error {
                error: Error {
                    message: "empty stream request".into(),
                    code: "EMPTY_STREAM_REQUEST".into(),
                },
            },
            StreamRequest::Close(_) => {
                closed = true;
                StreamResult::Ok {
                    response: StreamResponse::Close(Default::default()),
                }
            }
            StreamRequest::Execute(r) => match resolve_stmt(&r.stmt.sql, r.stmt.sql_id, &sqls) {
                Ok(sql) => match executor::run_stmt(
                    handle,
                    &sql,
                    &StmtArgs {
                        args: r.stmt.args.clone(),
                        named_args: r.stmt.named_args.clone(),
                    },
                    r.stmt.want_rows.unwrap_or(true),
                ) {
                    Ok(result) => StreamResult::Ok {
                        response: StreamResponse::Execute(ExecuteStreamResp { result }),
                    },
                    Err(e) => StreamResult::Error {
                        error: err_to_proto(&e),
                    },
                },
                Err(e) => StreamResult::Error {
                    error: err_to_proto(&e),
                },
            },
            StreamRequest::Batch(r) => match run_batch(handle, &r.batch, &mut sqls) {
                Ok(result) => StreamResult::Ok {
                    response: StreamResponse::Batch(crate::hrana::proto::BatchStreamResp {
                        result,
                    }),
                },
                Err(e) => StreamResult::Error {
                    error: err_to_proto(&e),
                },
            },
            StreamRequest::Sequence(r) => match resolve_stmt(&r.sql, r.sql_id, &sqls) {
                Ok(sql) => match executor::run_sequence(handle, &sql) {
                    Ok(()) => StreamResult::Ok {
                        response: StreamResponse::Sequence(Default::default()),
                    },
                    Err(e) => StreamResult::Error {
                        error: err_to_proto(&e),
                    },
                },
                Err(e) => StreamResult::Error {
                    error: err_to_proto(&e),
                },
            },
            StreamRequest::Describe(r) => match resolve_stmt(&r.sql, r.sql_id, &sqls) {
                Ok(sql) => match describe(handle, &sql) {
                    Ok(result) => StreamResult::Ok {
                        response: StreamResponse::Describe(
                            crate::hrana::proto::DescribeStreamResp { result },
                        ),
                    },
                    Err(e) => StreamResult::Error {
                        error: err_to_proto(&e),
                    },
                },
                Err(e) => StreamResult::Error {
                    error: err_to_proto(&e),
                },
            },
            StreamRequest::StoreSql(r) => {
                if sqls.contains_key(&r.sql_id) {
                    StreamResult::Error {
                        error: Error {
                            message: format!("SQL with id {} already stored", r.sql_id),
                            code: "SQL_STORE_EXISTS".into(),
                        },
                    }
                } else if sqls.len() >= MAX_SQL_COUNT {
                    StreamResult::Error {
                        error: Error {
                            message: format!(
                                "The server already stores {count} SQL texts, it cannot store more",
                                count = sqls.len()
                            ),
                            code: "SQL_STORE_TOO_MANY".into(),
                        },
                    }
                } else if r.sql.len() > MAX_STORED_SQL {
                    StreamResult::Error {
                        error: Error {
                            message: "The statement is too large to be stored".to_string(),
                            code: "SQL_STORE_TOO_LARGE".into(),
                        },
                    }
                } else {
                    sqls.insert(r.sql_id, r.sql.clone());
                    StreamResult::Ok {
                        response: StreamResponse::StoreSql(Default::default()),
                    }
                }
            }
            StreamRequest::CloseSql(r) => {
                sqls.remove(&r.sql_id);
                StreamResult::Ok {
                    response: StreamResponse::CloseSql(Default::default()),
                }
            }
            StreamRequest::GetAutocommit(_) => StreamResult::Ok {
                response: StreamResponse::GetAutocommit(
                    crate::hrana::proto::GetAutocommitStreamResp {
                        is_autocommit: handle.conn.is_autocommit(),
                    },
                ),
            },
        };
        results.push(result);
    }
    drop(sqls);

    (
        PipelineRespBody {
            baton: None, // set by the caller (stream layer)
            base_url: None,
            results,
        },
        closed,
    )
}

/// Execute a batch: each step's condition is evaluated against the outcomes
/// of previous steps (mirroring libsql-server's program VM).
fn run_batch(
    handle: &DbHandle,
    batch: &Batch,
    sqls: &mut HashMap<i32, String>,
) -> Result<BatchResult, StmtError> {
    let mut step_results: Vec<Option<StmtResult>> = Vec::with_capacity(batch.steps.len());
    let mut step_errors: Vec<Option<Error>> = Vec::with_capacity(batch.steps.len());
    let mut outcomes: Vec<bool> = Vec::with_capacity(batch.steps.len());

    for (i, step) in batch.steps.iter().enumerate() {
        let enabled = match &step.condition {
            None => true,
            Some(cond) => eval_cond(cond, &outcomes, handle.conn.is_autocommit()),
        };
        if !enabled {
            step_results.push(None);
            step_errors.push(None);
            outcomes.push(false);
            continue;
        }
        let sql = resolve_stmt(&step.stmt.sql, step.stmt.sql_id, sqls)?;
        let args = StmtArgs {
            args: step.stmt.args.clone(),
            named_args: step.stmt.named_args.clone(),
        };
        match executor::run_stmt(handle, &sql, &args, step.stmt.want_rows.unwrap_or(true)) {
            Ok(result) => {
                step_results.push(Some(result));
                step_errors.push(None);
                outcomes.push(true);
            }
            Err(e) => {
                tracing::debug!("batch step {i} failed: {}", e.message);
                step_results.push(None);
                step_errors.push(Some(err_to_proto(&e)));
                outcomes.push(false);
            }
        }
    }

    Ok(BatchResult {
        step_results,
        step_errors,
        replication_index: None,
    })
}

fn eval_cond(cond: &BatchCond, outcomes: &[bool], autocommit: bool) -> bool {
    match cond {
        BatchCond::None => true,
        BatchCond::Ok { step } => outcomes.get(*step as usize).copied().unwrap_or(false),
        BatchCond::Error { step } => !outcomes.get(*step as usize).copied().unwrap_or(true),
        BatchCond::Not { cond } => !eval_cond(cond, outcomes, autocommit),
        BatchCond::And(list) => list
            .conds
            .iter()
            .all(|c| eval_cond(c, outcomes, autocommit)),
        BatchCond::Or(list) => list
            .conds
            .iter()
            .any(|c| eval_cond(c, outcomes, autocommit)),
        BatchCond::IsAutocommit {} => autocommit,
    }
}

/// Describe a statement: parameter names, result columns, readonly/explain.
fn describe(handle: &DbHandle, sql: &str) -> Result<DescribeResult, StmtError> {
    let stmt = handle.conn.prepare(sql).map_err(|e| match e {
        rusqlite::Error::MultipleStatement => StmtError::new(
            "SQLITE_MISUSE",
            "SQL string contains more than one statement",
        ),
        e => StmtError::from(e),
    })?;

    let params = (1..=stmt.parameter_count())
        .map(|i| DescribeParam {
            name: stmt.parameter_name(i).map(|s| s.to_string()),
        })
        .collect();

    let cols = (0..stmt.column_count())
        .map(|i| crate::hrana::proto::DescribeCol {
            name: stmt.columns()[i].name().to_string(),
            decltype: stmt.columns()[i].decl_type().map(|d| d.to_string()),
        })
        .collect();

    let is_readonly = stmt.readonly();
    let is_explain = {
        let s = sql.trim_start().to_ascii_uppercase();
        s.starts_with("EXPLAIN")
    };

    Ok(DescribeResult {
        params,
        cols,
        is_explain,
        is_readonly,
    })
}

fn resolve_stmt(
    sql: &Option<String>,
    sql_id: Option<i32>,
    sqls: &HashMap<i32, String>,
) -> Result<String, StmtError> {
    match (sql, sql_id) {
        (Some(sql), _) => Ok(sql.clone()),
        (None, Some(id)) => sqls
            .get(&id)
            .cloned()
            .ok_or_else(|| StmtError::new("SQL_STORE_MISSING", "stored SQL not found")),
        (None, None) => Err(StmtError::new(
            "SQLITE_MISUSE",
            "statement has neither sql nor sql_id",
        )),
    }
}

fn err_to_proto(e: &StmtError) -> Error {
    Error {
        message: e.message.clone(),
        code: e.code.clone(),
    }
}

/// A pipeline without a baton behaves like a fresh connection: any
/// transaction left open by an earlier, abandoned request is rolled back.
pub fn rollback_stray_tx(handle: &DbHandle) {
    if !handle.conn.is_autocommit() {
        tracing::debug!("rolling back stray transaction left by a previous request");
        let _ = handle.conn.execute_batch("ROLLBACK");
        handle.capture.borrow_mut().drop_unit();
    }
}
