use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Top-level server error. Rendered as an HTTP error response.
///
/// The JSON body carries both `error` (libsql-server admin API convention,
/// parsed by the ragham backend) and `message`/`code` (Hrana convention,
/// parsed by @libsql/client) so both consumers are compatible.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct AppError {
    pub status: StatusCode,
    pub message: String,
    pub code: String,
}

impl AppError {
    pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            code: code.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message)
    }

    pub fn bad_request(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", message)
    }

    pub fn conflict(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(e: rusqlite::Error) -> Self {
        let code = hrana_code(&e);
        let message = match &e {
            rusqlite::Error::SqliteFailure(_, msg) => msg.clone().unwrap_or_else(|| e.to_string()),
            _ => e.to_string(),
        };
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, code, message)
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            e.to_string(),
        )
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "error": self.message,
            "message": self.message,
            "code": self.code,
        });
        (self.status, Json(body)).into_response()
    }
}

/// Map a rusqlite error to an Hrana-style error code string
/// (e.g. "SQLITE_CONSTRAINT"), matching what clients surface on `err.code`.
pub fn hrana_code(e: &rusqlite::Error) -> String {
    if let Some(ffi_code) = e.sqlite_error_code() {
        return format!("SQLITE_{}", ffi_code_name(ffi_code));
    }
    match e {
        rusqlite::Error::QueryReturnedNoRows => "SQLITE_QUERY_RETURNED_NO_ROWS".to_string(),
        _ => "SQLITE_ERROR".to_string(),
    }
}

fn ffi_code_name(code: rusqlite::ffi::ErrorCode) -> &'static str {
    use rusqlite::ffi::ErrorCode::*;
    match code {
        Unknown => "UNKNOWN",
        InternalMalfunction => "INTERNAL",
        PermissionDenied => "PERM",
        OperationAborted => "ABORT",
        DatabaseBusy => "BUSY",
        DatabaseLocked => "LOCKED",
        OutOfMemory => "NOMEM",
        ReadOnly => "READONLY",
        OperationInterrupted => "INTERRUPT",
        SystemIoFailure => "IOERR",
        DatabaseCorrupt => "CORRUPT",
        NotFound => "NOTFOUND",
        DiskFull => "FULL",
        CannotOpen => "CANTOPEN",
        FileLockingProtocolFailed => "PROTOCOL",
        SchemaChanged => "SCHEMA",
        TooBig => "TOOBIG",
        ConstraintViolation => "CONSTRAINT",
        TypeMismatch => "MISMATCH",
        ApiMisuse => "MISUSE",
        NoLargeFileSupport => "NOLFS",
        AuthorizationForStatementDenied => "AUTH",
        ParameterOutOfRange => "RANGE",
        NotADatabase => "NOTADB",
        _ => "UNKNOWN",
    }
}

/// Error produced while executing one Hrana statement. Returned inside the
/// pipeline response as a per-request error result (HTTP 200), which is what
/// @libsql/client expects for statement-level failures.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct StmtError {
    pub code: String,
    pub message: String,
}

impl StmtError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl From<rusqlite::Error> for StmtError {
    fn from(e: rusqlite::Error) -> Self {
        StmtError::new(
            hrana_code(&e),
            match &e {
                rusqlite::Error::SqliteFailure(_, msg) => {
                    msg.clone().unwrap_or_else(|| e.to_string())
                }
                _ => e.to_string(),
            },
        )
    }
}
