# Sync Protocol (server → replica)

This document specifies the replication protocol implemented by
`replite`. It is the contract any read replica (mobile apps via vanilla
`androidx.sqlite`, edge nodes, other servers) must implement.

Replication is **one-way**: the server is the single writer. Replicas are read
replicas. There is no conflict resolution.

```
┌────────────┐  Hrana over HTTP   ┌────────────────────┐
│  backend   │ ─────────────────▶ │       replite       │──▶ SQLite (primary)
│ (libsql)   │  admin API         │  -server           │──▶ binlog segments
└────────────┘                    └─────────┬──────────┘
                                           │ sync endpoints
┌────────────┐        clone / binlog       ▼
│  replica  │ ◀──────────────────────── (plain HTTP, SSE/binary)
└────────────┘
```

---

## 1. Sync algorithm (what the client does)

```
loop:
  info = GET /sync/v1/namespaces/{ns}/info
  if local_db missing or local_lsn < info.min_lsn:
      clone()                       # physical snapshot, see §4
      continue                      # then loop and start applying binlog
  else:
      delta = GET /sync/v1/namespaces/{ns}/binlog?since=local_lsn   # SSE stream
      if delta == 409 BINLOG_LAG:   # fell behind retention window
          clone()                   # start over
          continue
      local_lsn = delta.x-current-lsn   # logical apply, see §3
```

- `local_lsn` is the LSN of the last transaction applied locally (0 = empty).
  After a successful apply, take it from the response's `x-current-lsn`
  header (the stream contains every transaction up to it) and persist it
  (e.g. in a `sync_state` table or SharedPreferences) so interrupted syncs
  resume exactly where they stopped.
- Applying is idempotent by construction (see §3), so a crash between "apply
  succeeded" and "persist local_lsn" is harmless — the transaction is simply
  re-applied.

## 2. Namespace & admin endpoints

Namespaces are created by the backend (not by replicas) through the
libsql-server-compatible admin API:

| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/v1/namespaces/{ns}/create` | `{}` | 200 `{}` (or 409 if exists) |
| GET | `/v1/namespaces/{ns}/config` | — | 200 `{"block_reads":false,"block_writes":false,"allow_attach":false}` / 404 |
| POST | `/v1/namespaces/{ns}/checkpoint` | — | 200 `{}` |
| DELETE | `/v1/namespaces/{ns}` | — | 200 `{}` / 404 |

The server is authless but the routes are compatible with the ragham
backend, which sends `Authorization: Bearer <key>`; the header is accepted
and ignored.

SQL writes go through the standard Hrana protocol so existing libsql clients
keep working:

| Endpoint | Encoding | Used by |
|---|---|---|
| `POST /v2/pipeline` | JSON | older `@libsql/client` |
| `POST /v3/pipeline` | JSON | — |
| `POST /v3-protobuf/pipeline` | protobuf | `@libsql/client` ≥ 0.14 (current) |
| `POST /` | JSON | legacy clients |

All user routes select the namespace via the `x-namespace` request header.
Requests without the header use the `default` namespace.

## 3. Binlog format (SSE)

`GET /sync/v1/namespaces/{ns}/binlog?since=<lsn>` streams every committed
transaction with `lsn > since`, in commit order, as a **Server-Sent Events**
response (`Content-Type: text/event-stream`). The server does not buffer the
stream: each transaction is read from the segment files and emitted as soon
as it is decoded.

Response headers (available immediately, before the first event):

| Header | Meaning |
|---|---|
| `x-current-lsn` | LSN of the last transaction in the stream |
| `x-min-lsn` | oldest retained transaction (see §5) |
| `x-namespace` | namespace the stream belongs to |

One event per committed transaction:

```
id: 7
data: {"lsn":7,"statements":["ALTER TABLE \"t\" ADD COLUMN \"extra\" INTEGER DEFAULT 0;","UPDATE \"t\" SET \"extra\" = 42 WHERE \"id\" = 1;"]}

```

- `id:` is the transaction's LSN (also inside `data.lsn`).
- `data:` is a single JSON object: `{"lsn": <u64>, "statements": [<sql>, ...]}`.
- A clean EOF after the last event means the stream is complete. An empty
  stream (nothing newer than `since`) is headers followed immediately by EOF.
- If reading the segments fails mid-stream the server emits
  `event: error` with `data: {"code":"BINLOG_READ","message":"..."}` and
  then closes; clients must treat that as a failure and retry (do not
  persist `x-current-lsn`).
- If `since < min_lsn` the server answers **409 Conflict** with
  `{"code":"BINLOG_LAG","message":"... re-clone ..."}` before streaming any
  bytes (see §5).

### Statement generation

Each transaction's `statements` list is ready to apply, atomically, in
order — the object boundary IS the transaction boundary (no BEGIN/COMMIT in
the list):

1. **DDL**: the original statements, replayed verbatim (a missing trailing
   `;` is appended).
2. **Row changes**, materialized as statements with literal values — never
   the original DML, which may be non-deterministic (`random()`,
   `datetime('now')`, `DEFAULT VALUES`):

   | change | generated statement |
   |---|---|
   | Insert | `INSERT INTO "t" ("c1",...) VALUES (...)` + `ON CONFLICT("pk") DO UPDATE SET "c1" = excluded."c1", ...` (or `DO NOTHING` when only PK columns exist) — upsert, idempotent |
   | Update | `UPDATE "t" SET "c1" = ..., ... WHERE "pk" = ...` — PK taken from the after-image (PKs are never updated) |
   | Delete | `DELETE FROM "t" WHERE "pk" = ...` |

   `pk` is `rowid` for rowid tables without a declared PRIMARY KEY
   (INTEGER PRIMARY KEY tables use their declared column, which equals
   rowid).

3. Literal encoding: `NULL`; integers as decimals; floats as the shortest
   decimal that round-trips the exact f64, forced to a REAL literal with a
   trailing `.0` when integral, `9e999`/`-9e999` for ±Inf, `NULL` for NaN
   (SQLite stores NaN as NULL), `-0.0` preserved; text single-quoted with
   `''` escaping; blobs as `X'<hex>'`. Identifiers are double-quoted,
   embedded quotes doubled.

### Apply rules (the applier)

Run each `data` object in **one SQLite transaction**, with
`PRAGMA foreign_keys = OFF` for the session (replicas must not have
triggers; FK cascades are captured as explicit row events by the primary and
would double-fire otherwise), then `execSQL` each statement in order.

### Idempotency

Re-applying any suffix of a binlog yields the same state:
- INSERT is an upsert keyed by PK;
- UPDATE of a missing row is a no-op; DELETE of a missing row is a no-op;
- DDL is transactional on the replica, and DDL+DML of one source transaction
  apply in one replica transaction.

A source transaction that fails mid-apply can therefore simply be retried.

## 4. Clone (physical snapshot)

`GET /sync/v1/namespaces/{ns}/clone` streams the primary database file as-is
(`application/vnd.sqlite3`), checkpointed so the file alone is complete.

Headers:

| Header | Meaning |
|---|---|
| `Content-Length` | total bytes — clone progress = `bytes_received / Content-Length` |
| `x-lsn` | binlog position the snapshot corresponds to; the client starts incremental sync from this LSN **after** replacing its local file |
| `x-namespace` | namespace |

Client procedure:
1. Write the body to `db.sqlite.tmp` (report progress from Content-Length).
2. `fsync` + atomically rename over the local database.
3. Set `local_lsn = x-lsn`.
4. Resume the sync loop.

Snapshot correctness: the server serializes clones against writes (single
writer per namespace), so `x-lsn` is always ≤ the first un-applied
transaction.

## 5. Retention and lag

Segments are deleted when the namespace's binlog exceeds
`SQLD_MAX_BINLOG_BYTES` (default 256 MiB). `min_lsn` = LSN of the oldest
retained transaction.

If `since < min_lsn` the server answers **409 Conflict** with
`{"code":"BINLOG_LAG","message":"... re-clone ..."}`. The client must then
re-clone (or accept data loss for rows older than the window).

## 6. LSN assignment & durability

- LSNs are per-namespace `u64`s, incremented per committed transaction that
  changed data or schema; read-only commits consume no LSN.
- A transaction's binlog record is written (and fsynced) before the Hrana
  response is sent: a client that saw "committed" can always find the
  transaction in the binlog.
- On restart the server scans segments, truncates torn trailing records, and
  resumes LSN assignment where it left off.

## 7. Constraints and caveats

- **One-way**: writes from replicas are not supported by the protocol. Local
  writes made directly on the replica database will be overwritten by the next
  sync (or lost on re-clone).
- **Savepoints** (`SAVEPOINT`/`ROLLBACK TO`) on the primary are not
  replicated; the transaction's binlog record is dropped (warned in server
  logs). `BEGIN`/`COMMIT`/`ROLLBACK` statements themselves are never emitted
  as events.
- **Replica triggers must not exist**; row events already include trigger
  effects.
- **Without-rowid tables** are fully supported (PK columns identify rows);
  `WITHOUT ROWID` requires a declared PK by SQLite definition.
- Generated columns are captured as part of the after-image; the applier
  should not list them in the INSERT column list if the SQLite version
  rejects writes to them (they are computed again on the replica).
- Binary values are preserved exactly (blob bytes, not base64, on the wire).
