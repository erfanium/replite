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
// Built-in scenarios
// ---------------------------------------------------------------------------

/// CRUD on a rowid table plus a CREATE INDEX.
#[tokio::test]
async fn basic_crud_and_schema() {
    let stmts = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL, n REAL DEFAULT 0)",
        "INSERT INTO t (id, v, n) VALUES (1, 'alpha', 1.5), (2, 'beta', -2.25)",
        "UPDATE t SET v = 'alpha2', n = 10.0 WHERE id = 1",
        "INSERT INTO t (id, v, n) VALUES (3, 'gamma', NULL)",
        "CREATE INDEX idx_t_v ON t (v)",
        "DELETE FROM t WHERE id = 2",
        "UPDATE t SET n = NULL WHERE id = 3",
    ];
    expect_converge("basic_crud_and_schema", &stmts).await;
}

/// ALTER TABLE ADD COLUMN between DML — after-images must include the new
/// column, and the DDL must replay verbatim.
#[tokio::test]
async fn alter_add_column_mid_stream() {
    let stmts = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')",
        "ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0",
        "UPDATE t SET extra = 42 WHERE id = 1",
        "INSERT INTO t (id, v, extra) VALUES (3, 'c', 7)",
        "ALTER TABLE t ADD COLUMN note TEXT DEFAULT ''",
        "UPDATE t SET note = 'hi' WHERE id = 2",
        "DELETE FROM t WHERE id = 3",
    ];
    expect_converge("alter_add_column_mid_stream", &stmts).await;
}

/// One explicit BEGIN...COMMIT block must be one binlog record.
#[tokio::test]
async fn explicit_transaction_block() {
    let stmts = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "BEGIN; INSERT INTO t (id, v) VALUES (1, 'a'); INSERT INTO t (id, v) VALUES (2, 'b'); UPDATE t SET v = 'a2' WHERE id = 1; COMMIT;",
        "INSERT INTO t (id, v) VALUES (3, 'c')",
        "BEGIN; DELETE FROM t WHERE id = 2; COMMIT;",
    ];
    expect_converge("explicit_transaction_block", &stmts).await;
}

/// WITHOUT ROWID composite-PK lifecycle: DELETE must carry both PK values.
#[tokio::test]
async fn without_rowid_composite() {
    let stmts = [
        "CREATE TABLE t (a TEXT, b TEXT, v INTEGER, PRIMARY KEY (a, b)) WITHOUT ROWID",
        "INSERT INTO t (a, b, v) VALUES ('x', 'y', 1), ('x', 'z', 2), ('w', 'y', 3)",
        "UPDATE t SET v = 10 WHERE a = 'x' AND b = 'y'",
        "DELETE FROM t WHERE a = 'w' AND b = 'y'",
        "INSERT INTO t (a, b, v) VALUES ('x', 'z', 20)",
        "DELETE FROM t WHERE a = 'x' AND b = 'z'",
        "INSERT INTO t (a, b, v) VALUES ('x', 'y', 100)",
    ];
    expect_converge("without_rowid_composite", &stmts).await;
}

/// Blob, NULL and REAL round-trips, including an empty blob.
#[tokio::test]
async fn blobs_nulls_reals() {
    let stmts = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, b BLOB, n REAL, v TEXT)",
        "INSERT INTO t (id, b, n, v) VALUES (1, X'deadbeef', 0.5, NULL)",
        "INSERT INTO t (id, b, n, v) VALUES (2, X'', -0.0, 'x')",
        "INSERT INTO t (id, b, n, v) VALUES (3, NULL, 1e300, 'y')",
        "UPDATE t SET b = X'00ff00', v = NULL WHERE id = 1",
        "DELETE FROM t WHERE id = 2",
    ];
    expect_converge("blobs_nulls_reals", &stmts).await;
}

/// Interleaved writes to two tables keep independent event streams in order.
#[tokio::test]
async fn two_tables_interleaved() {
    let stmts = [
        "CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)",
        "CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO a (id, v) VALUES (1, 'a1')",
        "INSERT INTO b (id, v) VALUES (1, 'b1')",
        "INSERT INTO a (id, v) VALUES (2, 'a2')",
        "UPDATE b SET v = 'b1u' WHERE id = 1",
        "DELETE FROM a WHERE id = 1",
        "INSERT INTO b (id, v) VALUES (2, 'b2')",
        "CREATE INDEX idx_b_v ON b (v)",
        "UPDATE a SET v = 'a2u' WHERE id = 2",
    ];
    expect_converge("two_tables_interleaved", &stmts).await;
}

// Data-integrity regression scenarios moved to tests/data_integrity_regressions.rs.

