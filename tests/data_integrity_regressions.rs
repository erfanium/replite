//! Regression tests for the known data-integrity divergences (ISSUES.md).
//!
//! Each test drives the full pipeline (Hrana write -> binlog -> standalone
//! applier, the same algorithm a mobile client implements) and asserts that
//! the primary and replica converge. They intentionally FAIL against the
//! current code — one red test per ISSUES.md entry. When an issue is fixed,
//! its test turns green and becomes the guard against regression.

mod common;

use common::differential::{check, display};

/// Assert that a scenario converges (primary == replica after binlog apply).
async fn expect_converge(name: &str, stmts: &[&str]) {
    let stmts: Vec<String> = stmts.iter().map(|s| s.to_string()).collect();
    match check(&stmts).await {
        None => {}
        Some(diff) => panic!(
            "scenario `{name}` diverged:\n{}\ndiff:\n{diff}",
            display(&stmts),
        ),
    }
}

// ---------------------------------------------------------------------------
// ISSUES.md #1: SAVEPOINT / ROLLBACK TO
// ---------------------------------------------------------------------------

/// `ROLLBACK TO` does not fire the rollback hook, so the capture unit keeps
/// phantom after-images of rows the primary rolled back and the replica
/// diverges.
#[tokio::test]
async fn savepoint_rollback_to() {
    let stmts = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
        "BEGIN; INSERT INTO t VALUES (2, 'b'); SAVEPOINT s; UPDATE t SET v = 'c' WHERE id = 2; ROLLBACK TO s; COMMIT;",
    ];
    expect_converge("savepoint_rollback_to", &stmts).await;
}

// ---------------------------------------------------------------------------
// ISSUES.md #2: UPDATE of a primary-key column
// ---------------------------------------------------------------------------

/// Contract: "PKs are never updated". An UPDATE of a PK column is replicated
/// as an after-image upsert, leaving the old row behind on the replica.
#[tokio::test]
async fn update_primary_key() {
    let stmts = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
        "UPDATE t SET id = 2 WHERE id = 1",
    ];
    expect_converge("update_primary_key", &stmts).await;
}

// ---------------------------------------------------------------------------
// ISSUES.md #3: non-UTF-8 TEXT
// ---------------------------------------------------------------------------

/// `sqlite_value_to_text` converts TEXT through `String::from_utf8_lossy`, so
/// non-UTF-8 text (storable via CAST from a blob) is silently mangled in the
/// binlog.
#[tokio::test]
async fn non_utf8_text() {
    let stmts = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, CAST(X'fffe' AS TEXT))",
    ];
    expect_converge("non_utf8_text", &stmts).await;
}

// ---------------------------------------------------------------------------
// ISSUES.md #4: DDL events include the transaction's DML statements
// ---------------------------------------------------------------------------

/// A transaction that mixes DML and DDL records every statement (DML
/// included) verbatim in the DdlEvent. The applier replays them AND applies
/// the row events, so the DML runs twice. For a rowid table without a
/// declared PK the replayed INSERT takes a fresh rowid and the row event's
/// upsert cannot target the original one: the replica gains a phantom row.
#[tokio::test]
async fn ddl_event_includes_dml() {
    let stmts = [
        "CREATE TABLE t (v TEXT)",
        "INSERT INTO t VALUES ('a')",
        "BEGIN; INSERT INTO t VALUES ('b'); CREATE INDEX idx_t_v ON t(v); COMMIT;",
    ];
    expect_converge("ddl_event_includes_dml", &stmts).await;
}

// ---------------------------------------------------------------------------
// ISSUES.md #5: row events for a table dropped in the same transaction
// ---------------------------------------------------------------------------

/// Dropping a table inside the same transaction that wrote to it makes
/// `PRAGMA table_info` fail at finalize time: the row event is emitted with
/// empty `columns`, and the applier either errors or writes a malformed row.
/// The committed transaction's record is effectively unusable.
#[tokio::test]
async fn drop_table_in_same_transaction() {
    let stmts = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
        "BEGIN; INSERT INTO t VALUES (2, 'b'); DROP TABLE t; COMMIT;",
    ];
    expect_converge("drop_table_in_same_transaction", &stmts).await;
}
