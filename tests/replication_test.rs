//! End-to-end replication tests: write through the Hrana API (JSON and
//! protobuf wire formats), fetch the binlog, apply it on a replica with the
//! same algorithm a mobile client would use, and verify convergence.
//!
//! The applier is intentionally standalone (no server code): it only
//! consumes the documented wire format, proving the binlog is self-contained.
//! It lives in `common` together with the in-process `TestServer` harness.

mod common;

use axum::http::{StatusCode, header};
use common::*;
use prost::Message as _;
use replite::hrana::proto::{BatchStep, NamedArg, PipelineReqBody, StreamRequest, Value as HValue};

use rusqlite::Connection;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replication_roundtrip_json() {
    let server = TestServer::new();
    let ns = "user123";

    // Admin API: create namespace.
    let (status, _) = server
        .post_json(
            &format!("/v1/namespaces/{ns}/create"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    // Write schema + data through Hrana v2 (JSON).
    for sql in [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL DEFAULT 0)",
        "CREATE TABLE orders (order_id TEXT, user_id INTEGER, amount INTEGER, PRIMARY KEY (order_id, user_id)) WITHOUT ROWID",
        "INSERT INTO users (id, name, score) VALUES (1, 'alice', 9.5), (2, 'bob', 7.25)",
        "INSERT INTO orders VALUES ('o1', 1, 100), ('o2', 1, 50), ('o3', 2, 25)",
        "UPDATE users SET score = 10.0 WHERE id = 1",
        "DELETE FROM users WHERE id = 2",
    ] {
        let req = PipelineReqBody {
            baton: None,
            requests: vec![execute(sql, false), close()],
        };
        let (status, body) = server.pipeline_json(ns, &req).await;
        assert_eq!(status, StatusCode::OK, "failed: {sql}; body={body}");
    }

    // Fetch the binlog (an SSE stream).
    let (status, body, headers) = server.fetch_binlog(ns, 0).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).unwrap().to_str().unwrap(),
        "text/event-stream",
        "binlog is served as SSE"
    );
    assert_eq!(
        headers.get("x-current-lsn").unwrap().to_str().unwrap(),
        "6",
        "six transactions: 5 dml + 1 ddl (create statements)"
    );

    // Apply on a fresh replica db. The body is plain SQL; the LSNs live in
    // the response headers.
    let replica = Connection::open_in_memory().unwrap();
    apply_binlog(&replica, &body);
    assert_eq!(
        headers.get("x-current-lsn").unwrap().to_str().unwrap(),
        "6",
        "six transactions: 5 dml + 1 ddl (create statements)"
    );

    // Verify the replica matches the primary.
    let primary = Connection::open(server.primary_path(ns)).unwrap();
    assert_dbs_equal(&primary, &replica);

    // Verify specific row contents on the replica.
    let n: i64 = replica
        .query_row("SELECT count(*) FROM users", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1, "bob was deleted");
    let (name, score): (String, f64) = replica
        .query_row("SELECT name, score FROM users WHERE id = 1", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(name, "alice");
    assert_eq!(score, 10.0);
    let orders: i64 = replica
        .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
        .unwrap();
    assert_eq!(orders, 3, "WITHOUT ROWID table replicated");
}

#[tokio::test]
async fn replication_roundtrip_protobuf() {
    // Same flow as the JSON test but through /v3-protobuf/pipeline, the exact
    // endpoint and encoding @libsql/client 0.17.4 uses.
    let server = TestServer::new();
    let ns = "proto_ns";
    let (status, _) = server
        .post_json(
            &format!("/v1/namespaces/{ns}/create"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let sql = "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, b BLOB)";
    let req = PipelineReqBody {
        baton: None,
        requests: vec![execute(sql, false), close()],
    };
    let (status, bytes) = server.pipeline_protobuf(ns, &req).await;
    assert_eq!(status, StatusCode::OK, "protobuf pipeline rejected");
    let resp = replite::hrana::proto::PipelineRespBody::decode(&bytes[..]).unwrap();
    assert_eq!(resp.results.len(), 2);
    assert!(
        matches!(
            resp.results[0],
            replite::hrana::proto::StreamResult::Ok { .. }
        ),
        "execute failed: {:?}",
        resp.results[0]
    );

    // Insert with args (sint64 + text + blob) via protobuf.
    let req = PipelineReqBody {
        baton: None,
        requests: vec![
            StreamRequest::Execute(replite::hrana::proto::ExecuteStreamReq {
                stmt: replite::hrana::proto::Stmt {
                    sql: Some("INSERT INTO t (id, v, b) VALUES (?1, ?2, ?3)".into()),
                    sql_id: None,
                    args: vec![
                        HValue::integer(1),
                        HValue::text("hello"),
                        HValue::blob(vec![0xde, 0xad, 0xbe, 0xef]),
                    ],
                    named_args: vec![],
                    want_rows: Some(false),
                    replication_index: None,
                },
            }),
            close(),
        ],
    };
    let (status, bytes) = server.pipeline_protobuf(ns, &req).await;
    assert_eq!(status, StatusCode::OK);
    let resp = replite::hrana::proto::PipelineRespBody::decode(&bytes[..]).unwrap();
    assert!(
        matches!(
            resp.results[0],
            replite::hrana::proto::StreamResult::Ok { .. }
        ),
        "insert failed: {:?}",
        resp.results[0]
    );

    // Read back via protobuf to prove the response codec round-trips too.
    let req = PipelineReqBody {
        baton: None,
        requests: vec![execute("SELECT id, v, b FROM t", true), close()],
    };
    let (_, bytes) = server.pipeline_protobuf(ns, &req).await;
    let resp = replite::hrana::proto::PipelineRespBody::decode(&bytes[..]).unwrap();
    let result = match &resp.results[0] {
        replite::hrana::proto::StreamResult::Ok {
            response: replite::hrana::proto::StreamResponse::Execute(er),
        } => er,
        other => panic!("unexpected result: {other:?}"),
    };
    assert_eq!(result.result.rows.len(), 1);
    let row = &result.result.rows[0].values;
    assert_eq!(row[0], HValue::integer(1));
    assert_eq!(row[1], HValue::text("hello"));
    assert_eq!(row[2], HValue::blob(vec![0xde, 0xad, 0xbe, 0xef]));

    // Binlog should contain: DDL + INSERT, with the blob intact.
    let (_, body, _) = server.fetch_binlog(ns, 0).await;
    let replica = Connection::open_in_memory().unwrap();
    apply_binlog(&replica, &body);
    let blob: Vec<u8> = replica
        .query_row("SELECT b FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(blob, vec![0xde, 0xad, 0xbe, 0xef]);
}

#[tokio::test]
async fn incremental_sync() {
    let server = TestServer::new();
    let ns = "incr";
    let (status, _) = server
        .post_json(
            &format!("/v1/namespaces/{ns}/create"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
        "INSERT INTO t VALUES (2, 'b')",
    ] {
        let req = PipelineReqBody {
            baton: None,
            requests: vec![execute(sql, false), close()],
        };
        let (status, _) = server.pipeline_json(ns, &req).await;
        assert_eq!(status, StatusCode::OK);
    }

    let (_, body, headers) = server.fetch_binlog(ns, 0).await;
    let replica = Connection::open_in_memory().unwrap();
    apply_binlog(&replica, &body);
    let last = headers.get("x-current-lsn").unwrap().to_str().unwrap();
    assert_eq!(last, "3");

    // Write more on the primary, then sync only the delta.
    for sql in [
        "UPDATE t SET v = 'a2' WHERE id = 1",
        "DELETE FROM t WHERE id = 2",
        "INSERT INTO t VALUES (3, 'c')",
    ] {
        let req = PipelineReqBody {
            baton: None,
            requests: vec![execute(sql, false), close()],
        };
        let (status, _) = server.pipeline_json(ns, &req).await;
        assert_eq!(status, StatusCode::OK);
    }

    let last: u64 = last.parse().unwrap();
    let (_, body, headers) = server.fetch_binlog(ns, last).await;
    assert_eq!(headers.get("x-current-lsn").unwrap().to_str().unwrap(), "6");
    apply_binlog(&replica, &body);

    let primary = Connection::open(server.primary_path(ns)).unwrap();
    assert_dbs_equal(&primary, &replica);
}

#[tokio::test]
async fn transactional_batch_is_single_record() {
    let server = TestServer::new();
    let ns = "txn";
    let (status, _) = server
        .post_json(
            &format!("/v1/namespaces/{ns}/create"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let req = PipelineReqBody {
        baton: None,
        requests: vec![
            execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", false),
            close(),
        ],
    };
    let (status, _) = server.pipeline_json(ns, &req).await;
    assert_eq!(status, StatusCode::OK);

    // The exact shape @libsql/client sends for batch("write"):
    // BEGIN, statements (conditional on previous ok), COMMIT, ROLLBACK.
    let steps = vec![
        BatchStep {
            condition: None,
            stmt: replite::hrana::proto::Stmt::new("BEGIN", false),
        },
        BatchStep {
            condition: Some(replite::hrana::proto::BatchCond::Ok { step: 0 }),
            stmt: replite::hrana::proto::Stmt::new("INSERT INTO t VALUES (1, 'x')", false),
        },
        BatchStep {
            condition: Some(replite::hrana::proto::BatchCond::Ok { step: 1 }),
            stmt: replite::hrana::proto::Stmt::new("INSERT INTO t VALUES (2, 'y')", false),
        },
        BatchStep {
            condition: Some(replite::hrana::proto::BatchCond::Ok { step: 2 }),
            stmt: replite::hrana::proto::Stmt::new("COMMIT", false),
        },
        BatchStep {
            condition: Some(replite::hrana::proto::BatchCond::Not {
                cond: Box::new(replite::hrana::proto::BatchCond::Ok { step: 3 }),
            }),
            stmt: replite::hrana::proto::Stmt::new("ROLLBACK", false),
        },
    ];
    let req = PipelineReqBody {
        baton: None,
        requests: vec![batch(steps), close()],
    };
    let (status, body) = server.pipeline_json(ns, &req).await;
    assert_eq!(status, StatusCode::OK, "batch failed: {body}");
    assert_eq!(
        body["results"][0]["type"], "ok",
        "batch result was an error: {body}"
    );

    // The two inserts must be ONE binlog transaction with TWO statements:
    // the SSE stream carries one event per transaction, one statement per
    // row change.
    let (_, binlog, _) = server.fetch_binlog(ns, 0).await;
    let events = parse_sse(&binlog);
    assert_eq!(events.len(), 2, "DDL + one transaction");
    assert_eq!(events[0].lsn, 1, "DDL is the first transaction");
    assert_eq!(events[1].lsn, 2, "the batch is the second transaction");
    let total_rows = events
        .iter()
        .map(|tx| {
            tx.statements
                .iter()
                .filter(|s| s.starts_with("INSERT INTO"))
                .count()
        })
        .sum::<usize>();
    assert_eq!(total_rows, 2, "both inserts in one transaction");

    // Failed batch: BEGIN ok, INSERT violates constraint, COMMIT must be
    // skipped (condition), ROLLBACK runs. Nothing may hit the binlog.
    let steps = vec![
        BatchStep {
            condition: None,
            stmt: replite::hrana::proto::Stmt::new("BEGIN", false),
        },
        BatchStep {
            condition: Some(replite::hrana::proto::BatchCond::Ok { step: 0 }),
            stmt: replite::hrana::proto::Stmt::new("INSERT INTO t VALUES (1, 'dupe')", false),
        },
        BatchStep {
            condition: Some(replite::hrana::proto::BatchCond::Ok { step: 1 }),
            stmt: replite::hrana::proto::Stmt::new("COMMIT", false),
        },
        BatchStep {
            condition: Some(replite::hrana::proto::BatchCond::Not {
                cond: Box::new(replite::hrana::proto::BatchCond::Ok { step: 2 }),
            }),
            stmt: replite::hrana::proto::Stmt::new("ROLLBACK", false),
        },
    ];
    let req = PipelineReqBody {
        baton: None,
        requests: vec![batch(steps), close()],
    };
    let (status, body) = server.pipeline_json(ns, &req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"][0]["type"], "ok", "batch: {body}");
    let step_err = &body["results"][0]["response"]["result"]["step_errors"];
    assert!(
        step_err.as_array().unwrap().iter().any(|e| e.is_object()),
        "expected a step error: {body}"
    );
    // Just verify current_lsn did not advance.
    let info = server.info(ns).await;
    assert_eq!(info["current_lsn"], 2, "failed tx must not advance the LSN");
}

#[tokio::test]
async fn clone_snapshot() {
    let server = TestServer::new();
    let ns = "clone_ns";
    let (status, _) = server
        .post_json(
            &format!("/v1/namespaces/{ns}/create"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    ] {
        let req = PipelineReqBody {
            baton: None,
            requests: vec![execute(sql, false), close()],
        };
        let (status, _) = server.pipeline_json(ns, &req).await;
        assert_eq!(status, StatusCode::OK);
    }

    let (status, body, headers) = server.clone_db(ns).await;
    assert_eq!(status, StatusCode::OK);
    let lsn: u64 = headers["x-lsn"].to_str().unwrap().parse().unwrap();
    assert_eq!(lsn, 2);
    assert_eq!(
        headers[header::CONTENT_LENGTH].to_str().unwrap(),
        body.len().to_string(),
        "Content-Length lets clients report clone progress"
    );

    // The clone is a plain SQLite file: open it directly.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("clone.sqlite");
    std::fs::write(&path, &body).unwrap();
    let replica = Connection::open(&path).unwrap();
    let n: i64 = replica
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 3);
    let v: String = replica
        .query_row("SELECT v FROM t WHERE id = 3", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "c");

    // Writes after the clone appear in the binlog at lsn > X-LSN.
    let req = PipelineReqBody {
        baton: None,
        requests: vec![execute("INSERT INTO t VALUES (4, 'd')", false), close()],
    };
    let (status, _) = server.pipeline_json(ns, &req).await;
    assert_eq!(status, StatusCode::OK);
    let (status, delta, headers) = server.fetch_binlog(ns, lsn).await;
    assert_eq!(status, StatusCode::OK);
    apply_binlog(&replica, &delta);
    assert_eq!(
        headers.get("x-current-lsn").unwrap().to_str().unwrap(),
        "3"
    );
    let n: i64 = replica
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 4);
}

#[tokio::test]
async fn binlog_lag_requires_clone() {
    // Tiny retention: enough writes will evict old segments, advancing
    // min_lsn past 0 and making an old client fall behind.
    let server = TestServer::with_limits(4096, 16 * 1024);
    let ns = "laggy";
    let (status, _) = server
        .post_json(
            &format!("/v1/namespaces/{ns}/create"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let req = PipelineReqBody {
        baton: None,
        requests: vec![
            execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, payload BLOB)",
                false,
            ),
            close(),
        ],
    };
    let (status, _) = server.pipeline_json(ns, &req).await;
    assert_eq!(status, StatusCode::OK);

    // ~2KB blobs: each insert is a ~3KB binlog record; 20 of them is ~60KB
    // against a 16KB retention, forcing GC of the oldest segments.
    let blob = base64_blob(&vec![0x42u8; 2048]);
    for i in 0..20u64 {
        let req = PipelineReqBody {
            baton: None,
            requests: vec![
                StreamRequest::Execute(replite::hrana::proto::ExecuteStreamReq {
                    stmt: replite::hrana::proto::Stmt {
                        sql: Some(format!("INSERT INTO t (id, payload) VALUES ({i}, ?1)")),
                        sql_id: None,
                        args: vec![HValue::blob(blob_bytes(&blob))],
                        named_args: vec![],
                        want_rows: Some(false),
                        replication_index: None,
                    },
                }),
                close(),
            ],
        };
        let (status, body) = server.pipeline_json(ns, &req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["results"][0]["type"], "ok",
            "insert {i} failed: {body}"
        );
    }

    let info = server.info(ns).await;
    let min_lsn = info["min_lsn"].as_u64().unwrap();
    assert!(min_lsn > 0, "GC should have evicted old segments: {info}");

    // A client whose last-known LSN is older than min_lsn must re-clone.
    let (status, body, _headers) = server.fetch_binlog(ns, 0).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let err: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(err["code"], "BINLOG_LAG");
    assert!(err["message"].as_str().unwrap().contains("re-clone"));
    assert!(
        err["message"]
            .as_str()
            .unwrap()
            .contains(&min_lsn.to_string()),
        "error should surface min_lsn: {err}"
    );

    // A client at or above min_lsn still syncs fine.
    let (status, _, _) = server.fetch_binlog(ns, min_lsn).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn ddl_replication() {
    let server = TestServer::new();
    let ns = "ddl_ns";
    let (status, _) = server
        .post_json(
            &format!("/v1/namespaces/{ns}/create"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO t VALUES (1, 'a')",
        "ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 0",
        "UPDATE t SET extra = 42 WHERE id = 1",
        "CREATE INDEX idx_t_v ON t (v)",
    ] {
        let req = PipelineReqBody {
            baton: None,
            requests: vec![execute(sql, false), close()],
        };
        let (status, _) = server.pipeline_json(ns, &req).await;
        assert_eq!(status, StatusCode::OK);
    }

    let (_, body, _) = server.fetch_binlog(ns, 0).await;
    let replica = Connection::open_in_memory().unwrap();
    apply_binlog(&replica, &body);

    // Schema (including the index) must be replicated verbatim.
    let index: i64 = replica
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_t_v'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(index, 1);
    let extra: i64 = replica
        .query_row("SELECT extra FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(extra, 42);

    // The ALTER + UPDATE must both be present: ALTER is a DDL event, the
    // UPDATE's after-image includes the new column.
    let primary = Connection::open(server.primary_path(ns)).unwrap();
    assert_dbs_equal(&primary, &replica);
}

#[tokio::test]
async fn json_pipeline_with_named_args_and_error_codes() {
    let server = TestServer::new();
    let ns = "named";
    let (status, _) = server
        .post_json(
            &format!("/v1/namespaces/{ns}/create"),
            serde_json::json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let req = PipelineReqBody {
        baton: None,
        requests: vec![
            execute(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)",
                false,
            ),
            close(),
        ],
    };
    let (status, _) = server.pipeline_json(ns, &req).await;
    assert_eq!(status, StatusCode::OK);

    // Named args (JSON: integer as string).
    let req = PipelineReqBody {
        baton: None,
        requests: vec![
            StreamRequest::Execute(replite::hrana::proto::ExecuteStreamReq {
                stmt: replite::hrana::proto::Stmt {
                    sql: Some("INSERT INTO t (id, v) VALUES (:id, :v)".into()),
                    sql_id: None,
                    args: vec![],
                    named_args: vec![
                        NamedArg {
                            name: "id".into(),
                            value: HValue::integer(7),
                        },
                        NamedArg {
                            name: "v".into(),
                            value: HValue::text("seven"),
                        },
                    ],
                    want_rows: Some(false),
                    replication_index: None,
                },
            }),
            close(),
        ],
    };
    let (status, body) = server.pipeline_json(ns, &req).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["type"], "ok", "{body}");

    // Constraint violation surfaces as a per-request error with code.
    let req = PipelineReqBody {
        baton: None,
        requests: vec![
            execute("INSERT INTO t (id, v) VALUES (8, NULL)", false),
            close(),
        ],
    };
    let (status, body) = server.pipeline_json(ns, &req).await;
    assert_eq!(status, StatusCode::OK, "per-request errors are HTTP 200");
    assert_eq!(body["results"][0]["type"], "error");
    assert_eq!(body["results"][0]["error"]["code"], "SQLITE_CONSTRAINT");

    // And the failed insert must not appear in the binlog.
    let (_, binlog, _) = server.fetch_binlog(ns, 0).await;
    let replica = Connection::open_in_memory().unwrap();
    apply_binlog(&replica, &binlog);
    let n: i64 = replica
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
}

// ---------------------------------------------------------------------------
// Baton streams (the @libsql/client `session.transaction()` flow)
// ---------------------------------------------------------------------------

/// Mirrors `@libsql/client`'s HttpTransaction over HTTP: BEGIN, then more
/// statements, then ROLLBACK/COMMIT — each as a separate pipeline request
/// carrying the baton from the previous response. This is how the backend's
/// `syncUser` writes (engine.ts `tenantDb.transaction(...)`), and it was
/// broken by the stateless server ("cannot rollback - no transaction is
/// active").
#[tokio::test]
async fn baton_transaction_rollback_and_commit() {
    let server = TestServer::new();
    let ns = "baton_test";
    let (status, _, _) = server
        .call(
            "POST",
            &format!("/v1/namespaces/{ns}/create"),
            Some(b"{}".to_vec()),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    // SETUP: schema via a regular baton-less batch (as ensureTenant does).
    let (status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: None,
                requests: vec![seq("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")],
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK, "schema setup failed: {body}");

    // REQUEST 1: BEGIN (baton-less -> fresh stream) -> server returns a baton
    let (status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: None,
                requests: vec![execute("BEGIN", false)],
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"][0]["type"], "ok");
    let baton = body["baton"]
        .as_str()
        .expect("stream must return a baton")
        .to_string();

    // REQUEST 2: upserts inside the same transaction (baton carried over).
    let (status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: Some(baton.clone()),
                requests: vec![
                    execute("INSERT INTO t (id, v) VALUES (1, 'a')", false),
                    execute("INSERT INTO t (id, v) VALUES (2, 'b')", false),
                    seq("INSERT INTO t (id, v) VALUES (3, 'c'); INSERT INTO t (id, v) VALUES (4, 'd')"),
                ],
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upserts failed: {body}");
    let results = body["results"].as_array().unwrap();
    for r in results {
        assert_eq!(r["type"], "ok", "upsert failed: {r}");
    }
    let baton2 = body["baton"].as_str().expect("still open").to_string();
    assert_ne!(baton, baton2);

    // REQUEST 3: ROLLBACK (the exact failing request: it must find the
    // transaction still open and not error with "no transaction is active").
    let (status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: Some(baton2.clone()),
                requests: vec![execute("ROLLBACK", false)],
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK, "rollback failed: {body}");
    assert_eq!(body["results"][0]["type"], "ok", "rollback result: {body}");

    // Nothing was committed: no rows, no binlog records.
    let (_, binlog, _) = server.fetch_binlog(ns, 0).await;
    let replica = Connection::open_in_memory().unwrap();
    apply_binlog(&replica, &binlog);
    let n: i64 = replica
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0, "rolled-back inserts must not be visible");

    // COMMIT flow on a new stream: BEGIN + inserts + COMMIT, then close.
    let (_status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: None,
                requests: vec![execute("BEGIN", false)],
            },
        )
        .await;
    let baton = body["baton"].as_str().unwrap().to_string();
    let (status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: Some(baton.clone()),
                requests: vec![execute("INSERT INTO t (id, v) VALUES (10, 'x')", false)],
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let baton = body["baton"].as_str().unwrap().to_string();
    let (status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: Some(baton.clone()),
                requests: vec![execute("COMMIT", false), close()],
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK, "commit failed: {body}");
    assert_eq!(body["results"][0]["type"], "ok", "commit result: {body}");
    assert!(
        body["baton"].is_null(),
        "closed stream must not return a baton, got {body}"
    );

    // The committed transaction is in the binlog (one record, one row event).
    let (_, binlog, _) = server.fetch_binlog(ns, 0).await;
    let replica = Connection::open_in_memory().unwrap();
    apply_binlog(&replica, &binlog);
    let rows: Vec<(i64, String)> = replica
        .prepare("SELECT id, v FROM t ORDER BY id")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, vec![(10, "x".to_string())]);
}

/// A baton-less request must not disturb an open stream's transaction.
#[tokio::test]
async fn baton_stream_survives_concurrent_batch() {
    let server = TestServer::new();
    let ns = "baton_concurrent";
    let (status, _, _) = server
        .call(
            "POST",
            &format!("/v1/namespaces/{ns}/create"),
            Some(b"{}".to_vec()),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: None,
                requests: vec![seq("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")],
            },
        )
        .await;

    // stream: BEGIN
    let (_, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: None,
                requests: vec![execute("BEGIN", false)],
            },
        )
        .await;
    let baton = body["baton"].as_str().unwrap().to_string();

    // baton-less batch commits a row on its own connection
    let (status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: None,
                requests: vec![seq("INSERT INTO t (id, v) VALUES (1, 'other')")],
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // stream inserts + COMMIT — must still work (its transaction was not
    // rolled back by the stray-tx cleanup of the baton-less request)
    let (status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: Some(baton.clone()),
                requests: vec![execute("INSERT INTO t (id, v) VALUES (2, 'stream')", false)],
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["type"], "ok", "{body}");
    let baton = body["baton"].as_str().unwrap().to_string();
    let (status, body) = server
        .pipeline_json(
            ns,
            &PipelineReqBody {
                baton: Some(baton.clone()),
                requests: vec![execute("COMMIT", false), close()],
            },
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (_, binlog, _) = server.fetch_binlog(ns, 0).await;
    let replica = Connection::open_in_memory().unwrap();
    apply_binlog(&replica, &binlog);
    let rows: Vec<i64> = replica
        .prepare("SELECT id FROM t ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, vec![1, 2]);
}
