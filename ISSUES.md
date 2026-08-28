# Known Issues

Regression tests for issues #1–#5 live in `tests/data_integrity_regressions.rs`.
They are normal (non-ignored) tests and intentionally FAIL against the current
code; each turns green when its issue is fixed.

## 1. SAVEPOINT / ROLLBACK TO causes silent replica divergence

**Severity:** high (data integrity). **Status:** open. **Test:** `savepoint_rollback_to`.

### Symptom

A transaction that uses `SAVEPOINT` / `ROLLBACK TO` commits successfully on the
primary, but the binlog record does not match the primary's final state. The
replica applies phantom or stale rows and permanently diverges — no error is
surfaced on the server, the client, or the replica.

### Scenario

```sql
BEGIN;                        -- executor opens a CaptureUnit
INSERT INTO t VALUES (1);     -- preupdate_cb records event A
SAVEPOINT sp;
UPDATE t SET v = 2;           -- preupdate_cb records event B (later undone)
ROLLBACK TO sp;               -- SQLite undoes the UPDATE in the btree...
COMMIT;                       -- ...but the capture buffer still has A and B
```

Primary commits `(1, v=1)`; the binlog tells the replica `INSERT (1, v=2)`.
If the row was only written inside the savepoint, the replica gets a phantom
row that does not exist on the primary.

### Root cause

The capture layer is a logical (row-level) tape recorder: `preupdate_cb`
(src/capture.rs:149) appends raw row events to `CaptureUnit`, and
`finalize_unit` (src/capture.rs:337) serializes the whole buffer to the binlog
at COMMIT. It assumes the buffer is an exact transcript of what got committed.

That assumption breaks on savepoint rollback:

- `ROLLBACK TO` does **not** fire `sqlite3_rollback_hook`. In the bundled
  SQLite source the hook is only invoked from `sqlite3RollbackAll`
  (sqlite3.c:188721), and the `OP_Savepoint` / SAVEPOINT_ROLLBACK path never
  calls it — it only calls `sqlite3BtreeSavepoint`.
- Therefore `rollback_cb` (src/capture.rs:219) never runs for `ROLLBACK TO`,
  the unit is not dropped, and the preupdate events captured inside the
  savepoint remain in the buffer and are written to the binlog at COMMIT.

Note: the comment in `rollback_cb` ("including savepoint rollbacks") and the
README's "the transaction's record is dropped with a warning" describe a
different, rarer path (an error-forced full rollback mid-transaction via
`sqlite3RollbackAll`, which does fire the hook and does drop the unit). The
common `ROLLBACK TO` case is undetected phantom/stale data, with no warning at
all.

### Why it is silent and permanent

1. The server returns `OK` — SQLite itself committed successfully.
2. LSNs stay contiguous — the replica cannot see a gap, because a record *was*
   written; it just contains wrong rows.
3. The replica's database is internally consistent — nothing errors, nothing
   re-syncs. Divergence only shows up when a read returns wrong data, and it
   never self-heals: future transactions only fix rows they touch.

This is the fundamental weakness of logical (row-level) capture: it must
re-implement SQL's rollback semantics (statement rollback, savepoint nesting,
`ON CONFLICT` clause rollbacks, triggers) on top of callbacks not designed for
it. Any gap = divergence, and divergence is undetectable by either side.

### How libsql avoids this class of bug (reference)

libsql replicates physical WAL frames; the transaction boundary is SQLite's
own WAL. Frames enter the shadow log in `insert_frames`, but the log's header
`frame_count` (the commit marker) is only bumped on `is_commit`. On savepoint
rollback, SQLite calls the WAL's `xUndo` → `ReplicationLoggerWalWrapper::undo()`
→ `rollback()` (libsql-server/src/replication/primary/replication_logger_wal.rs:120):
buffered frames are discarded and `uncommitted_frame_count` resets to the last
committed count — the undone pages simply never enter the log. No logical
reconciliation is needed because the log stores pages, not rows. See the
`savepoint_and_rollback` test (libsql-server/src/replication/primary/logger.rs:1152).

### Fix options

1. **Reject `SAVEPOINT` / `ROLLBACK TO` server-side** (statement pre-scan or
   authorizer) with a clear error — turns silent divergence into a loud
   failure. Cheapest; acceptable while the clients are our own.
2. **Replicate them properly** (MySQL binlog approach): keep a stack of
   buffer-length snapshots per savepoint (trackable via `record_statement`),
   truncate rows to the savepoint's snapshot on `ROLLBACK TO`, and record
   `SAVEPOINT` / `RELEASE` / `ROLLBACK TO` statements verbatim in the binlog
   so the replica replays the same nesting. Requires server + applier work.
3. Not acceptable: "drop the record with a warning" — a committed transaction
   must never be absent from the log.

## 2. UPDATE of a primary-key column leaves a stale row on the replica

**Severity:** high (data integrity). **Status:** open. **Test:** `update_primary_key`.

UPDATE events carry only the after-image, keyed by the declared PK (or
`rowid`). The "PKs are never updated" rule is a contract, not enforced by
SQL: `UPDATE t SET id = 2 WHERE id = 1` commits on the primary, and the
binlog tells the replica to upsert a row with id=2 — the original row (id=1)
is never deleted, so the replica keeps a stale row and gains a new one.

Fix options:

1. Enforce the contract server-side: reject UPDATE/REPLACE statements that
   assign to a PK column (authorizer or statement pre-scan).
2. Capture before-images for UPDATE and emit the old PK values so the applier
   can delete the old row (requires protocol + applier change).

## 3. Non-UTF-8 TEXT is silently mangled in the binlog

**Severity:** medium (data corruption). **Status:** open. **Test:** `non_utf8_text`.

`sqlite_value_to_value` (src/capture.rs:289) converts TEXT via
`String::from_utf8_lossy`, and the Hrana read path (`value_ref_to_hrana`,
src/executor.rs:344) does the same. SQLite TEXT may contain arbitrary bytes
(e.g. `CAST(X'fffe' AS TEXT)`); the primary stores them exactly, but the
binlog mangles them to U+FFFD, so the replica diverges. The applier also
cannot be sure a `Value::Text` string round-trips through UTF-8.

Fix: carry TEXT as `bytes` (or add a distinct `Value::RawText` variant) in the
binlog and Hrana responses; keep `String` only where the wire format requires
valid UTF-8.

## 4. DDL events include the transaction's DML statements (double apply)

**Severity:** high (data integrity). **Status:** open. **Test:** `ddl_event_includes_dml`.

`finalize_unit` (src/capture.rs:394-401) records a `DdlEvent` whenever the
schema version changed during the transaction, and puts **every** statement of
the transaction into it — only txn-control and `PRAGMA` are filtered
(src/capture.rs:135-143). A transaction like

```sql
BEGIN;
INSERT INTO t VALUES ('b');
CREATE INDEX idx_t_v ON t(v);
COMMIT;
```

produces a record with both `rows = [INSERT 'b']` and
`ddl = ["INSERT INTO t VALUES ('b')", "CREATE INDEX ..."]`. The documented
applier algorithm (docs/sync-protocol.md §3) replays the DDL verbatim **and**
applies the row events, so the DML executes twice.

For most PK'd tables the second application is an idempotent upsert and
happens to converge. It diverges when the DML is not idempotent under replay:

- rowid tables **without** a declared PK: `columns` comes from
  `PRAGMA table_info` and excludes `rowid`, so the row event's upsert cannot
  target the original rowid; the replayed INSERT takes a fresh rowid and the
  replica gains a phantom row (this is exactly what the regression test hits);
- DML with side effects: triggers on the replica, `v = v + 1`-style updates
  (the replay advances the counter, then the row event's after-image
  overwrites — wrong intermediate state if anything depends on it), and
  auto-increment counters.

Fix options:

1. Record only statements that actually changed the schema (track
   `PRAGMA schema_version` per statement) in `DdlEvent.statements`.
2. Emit one `DdlEvent` per DDL statement, interleaved in statement order with
   the row events, and change the applier to walk the mixed stream.
3. When a transaction contains DDL, make verbatim replay authoritative and
   emit no row events for that transaction (simplest; loses fine-grained
   events but is correct — replay is what produced the state).

## 5. Row events for a table dropped in the same transaction are malformed

**Severity:** high. **Status:** open. **Test:** `drop_table_in_same_transaction`.

Column resolution happens at finalize time, after COMMIT, against the
post-transaction schema (src/capture.rs:351-356). If the transaction both
writes to and drops a table,

```sql
BEGIN;
INSERT INTO t VALUES (2, 'b');
DROP TABLE t;
COMMIT;
```

`PRAGMA table_info("t")` fails on the dropped table and `table_schema`
(src/capture.rs:411-432) swallows the error, returning empty `columns`.
The emitted RowEvent has `columns = []` with a populated after-image, and the
applier generates `INSERT INTO "t" () VALUES ()` — invalid SQL. The replica
cannot apply the transaction at all (the applier errors out), while the
primary committed cleanly.

Fix options:

1. Snapshot the schema per statement inside the transaction (capture-time or
   per-statement resolution) so events reference the schema as it existed
   when the row was written.
2. Detect the failure at finalize and refuse to commit the transaction
   (error before COMMIT) instead of emitting a broken record.
3. Reject DDL that drops/alters a table with buffered row events.

## 6. Binlog append/finalize failures are swallowed (silent divergence)

**Severity:** high (data integrity). **Status:** open. **No test** (hard to
inject a write failure through the HTTP path; needs a fault-injection hook).

`after_success` (src/executor.rs:366-401) finalizes the capture unit and
appends the record after SQLite has already committed. Any failure on that
path is only `tracing::error!`'d:

- `finalize_unit` error (e.g. an unexpected `PRAGMA table_info` failure) →
  the record is dropped, the response is still `OK`;
- `binlog.append` error (disk full, I/O error) → the record is dropped, the
  response is still `OK`.

In both cases the primary committed a transaction the binlog never sees, LSNs
stay contiguous (the failed append assigns no LSN), and the replica has no way
to detect the missing record — the divergence is silent and permanent, exactly
like #1.

Fix options:

1. Order the work so the binlog write is the *commit gate*: finalize and
   append the record **before** the transaction commits (write it on
   `commit_hook` via `sqlite3_commit_hook`'s veto return, or use a two-phase
   scheme), so a binlog failure aborts the transaction.
2. At minimum, when append fails, mark the namespace `dirty` and fail all
   subsequent requests on it (503) until an operator resyncs; never return
   `OK` for a commit whose record did not land.

## 7. Binlog records have no checksum

**Severity:** medium. **Status:** open. **No test** (needs bit-flip injection).

Records are `[varint length][protobuf]` with no integrity check. A bit flip
inside a valid-length record decodes silently (prost fills defaults for
missing fields); only torn-record truncation is caught at startup
(src/binlog.rs:198-236). Corrupt data would replicate to every device.

Fix: append a CRC32C per record (or a segment trailer with record count +
CRC) and verify in `read_since`, `scan_state`, and on the wire.

## Verified non-issues (empirically confirmed converging)

These paths were suspected but tested and found correct — do not "fix" them
without adding a failing test first:

- `INSERT OR IGNORE` / `ON CONFLICT ... DO NOTHING`: no preupdate event is
  emitted for discarded rows; nothing leaks into the binlog.
- `INSERT ... ON CONFLICT DO UPDATE`: emits a single UPDATE event with the
  final post-update after-image.
- `INSERT OR REPLACE`: emits the correct DELETE + INSERT pair.
- `CREATE TEMP TABLE` + writes to temp tables: not captured at all (temp
  objects are per-connection and invisible to the hooks).
- `AUTOINCREMENT`: after-images carry the explicit rowid, so replicas stay
  aligned.
- FK cascade actions: captured as explicit row events on the child tables;
  the applier's `foreign_keys = OFF` + event replay converges.
- Statement-level constraint failures mid-statement: partial events are
  truncated (covered by `failed_statement_truncates_partial_events` in
  src/executor.rs).

## Operational / scale issues (no tests; see code)

- **Eager namespace open at startup** (src/namespace.rs:130-157): every
  namespace on disk is opened immediately. At 100k namespaces this means
  ~300k FDs (db + `-wal` + `-shm` each; default ulimit is 1024, so namespaces
  silently fail to open beyond the limit), ~2 MB default page cache per
  connection (~200 GB RAM), and a full decode scan of every retained binlog
  segment before serving. Needs lazy open + idle eviction + bounded
  `cache_size` per connection.
- **`GET /binlog` holds the namespace binlog mutex for the whole read** and
  buffers the entire response in memory (src/sync.rs:79-103): a device
  catching up on a large backlog blocks all writes to that namespace.
- **`GET /clone` runs a blocking `std::fs::read` of the whole DB inside an
  async handler** while holding the namespace lock (src/sync.rs:132-144);
  also unauthenticated and unthrottled — one client can repeatedly download
  entire databases.
- **No auth** anywhere (by design; the backend's bearer token is ignored) —
  the server must only be reachable from trusted networks.
- **No metrics/alerting**: divergence warnings (#1/#6) exist only as logs
  that nobody watches.
