# Known Issues

Regression tests for issues #1–#5 live in `tests/data_integrity_regressions.rs`.
They are normal (non-ignored) tests and intentionally FAIL against the current
code; each turns green when its issue is fixed.

Issues #8–#22 were found by targeted probing (see
[Reproduction notes](#reproduction-notes) at the end) and have **no committed
tests yet**. Every one of them was reproduced against the tree at the time of
writing; the exact failure output is quoted in each entry.

## Severity index

Ordered by how much damage the issue does, not by issue number. Issue numbers
are stable (tests and comments reference them), so new findings are appended
rather than inserted.

| # | Issue | Failure mode | Test |
|---|---|---|---|
| 8 | Binlog LSN order can invert vs. commit order | silent, permanent divergence | — |
| 9 | Tables without a declared PK diverge on first UPDATE/DELETE | silent, permanent divergence | — |
| 1 | `SAVEPOINT` / `ROLLBACK TO` | silent, permanent divergence | `savepoint_rollback_to` |
| 2 | UPDATE of a PK column | silent, permanent divergence | `update_primary_key` |
| 6 | Binlog append/finalize failures are swallowed | silent, permanent divergence | — |
| 3 | Non-UTF-8 TEXT is mangled | silent data corruption | `non_utf8_text` |
| 22 | No divergence detection anywhere | makes all of the above permanent | — |
| 10 | `row_where` panics on UPDATE of a PK-less table | 500 forever + remote DoS | — |
| 11 | Generated columns: columns/values length mismatch | replica apply halts forever | — |
| 12 | `is_txn_control` doesn't strip a trailing `;` | replica apply halts forever | — |
| 13 | Embedded NUL in TEXT truncates the generated SQL | replica apply halts forever | — |
| 14 | DDL replay is not idempotent | replica bricked by a crash | — |
| 16 | Records over 64 MiB brick the binlog on restart | namespace 404s forever | — |
| 4 | DDL events include the transaction's DML | replica apply halts forever (with #12) | `ddl_event_includes_dml` |
| 5 | Row events for a table dropped in the same txn | replica apply halts forever | `drop_table_in_same_transaction` |
| 15 | `ALTER`/`RENAME` after DML in the same txn | replica apply halts forever | — |
| 7 | Binlog records have no checksum | corruption replicates silently | — |
| 17 | `wal_checkpoint` result discarded; clone not isolated | possibly stale/torn snapshot | — |
| 19 | ATTACH guard bypassable; hook ignores the db name | events attributed to the wrong db | — |
| 20 | `PRAGMA user_version` is never replicated | replica migration state wrong | — |
| 18 | `min_lsn` off-by-one | needless full re-clones | — |
| 21 | `CaptureUnit.poisoned` is dead code | misleading; a safeguard that isn't | — |

## Architectural note: the option not yet evaluated

Almost every entry below is the same failure: the capture layer is a *logical*
tape recorder built on callbacks that were not designed to reconstruct SQL
semantics, and the wire format is *rendered SQL text* rather than typed values.
Issue #1 states this correctly ("Any gap = divergence, and divergence is
undetectable by either side") and then offers only two ways out: reject the SQL,
or reimplement MySQL's binlog semantics by hand.

There is a third option that is not discussed anywhere in this repo or in
`docs/sync-protocol.md`: **SQLite's own session extension**
(`sqlite3session_*`, changesets/patchsets — https://sqlite.org/sessionintro.html).
It is part of the vanilla amalgamation and `rusqlite` 0.40 exposes it behind
`features = ["session"]` (the crate already enables `preupdate_hook`, `backup`
and `column_decltype` from the same source).

Relevant properties, mapped onto the issues below:

- Capture is done by SQLite itself, so `SAVEPOINT` / `ROLLBACK TO` are handled
  by SQLite's own undo machinery — **#1**.
- Changesets carry **before- and after-images**, so an UPDATE that moves a PK
  replicates correctly — **#2**, and the same before-image makes rowid-keyed
  tables tractable — **#9**.
- Values are typed and binary-safe end to end (no `String`, no SQL literal
  rendering) — **#3**, **#13**, and the float/blob literal machinery in
  `sql_literal` (src/sync.rs:291-327) disappears entirely.
- `sqlite3changeset_apply` is a real applier with a conflict handler, instead of
  `execSQL` over generated SQL — **#10**, **#11**, **#14**.
- `sqlite3changeset_concat` gives compaction; `sqlite3changeset_invert` gives
  rollback.

Its limits are real and would need their own entries: `apply` requires tables to
have a PRIMARY KEY, DDL still has to be replicated separately (verbatim, with
all the ordering problems of #4/#5/#15), and a session object must be attached
per database/per table on each connection. But it eliminates roughly half of the
open data-integrity issues *by construction* rather than by hand-written
reconciliation.

**This should be evaluated and either adopted or explicitly rejected with a
reason.** As it stands, the project is hand-rolling a mechanism that vanilla
SQLite already ships, which is exactly the kind of decision that should be
written down.

---

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

`CaptureUnit.poisoned` (src/capture.rs:53) reads like the mitigation for this,
but it is **never set to `true` anywhere** — see #21. There is no code path that
drops or flags a savepoint-affected transaction.

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
it. Any gap = divergence, and divergence is undetectable by either side (#22).

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

1. **Reject `SAVEPOINT` / `ROLLBACK TO` server-side** (authorizer, not a
   statement pre-scan — see #19 for why prefix matching is not enough) with a
   clear error — turns silent divergence into a loud failure. Cheapest;
   acceptable while the clients are our own.
2. **Replicate them properly** (MySQL binlog approach): keep a stack of
   buffer-length snapshots per savepoint (trackable via `record_statement`),
   truncate rows to the savepoint's snapshot on `ROLLBACK TO`, and record
   `SAVEPOINT` / `RELEASE` / `ROLLBACK TO` statements verbatim in the binlog
   so the replica replays the same nesting. Requires server + applier work.
3. **Let SQLite do it** — the session extension handles this case natively; see
   the architectural note above.
4. Not acceptable: "drop the record with a warning" — a committed transaction
   must never be absent from the log.

## 2. UPDATE of a primary-key column leaves a stale row on the replica

**Severity:** high (data integrity). **Status:** open. **Test:** `update_primary_key`.

UPDATE events carry only the after-image, keyed by the declared PK (or
`rowid`). The "PKs are never updated" rule is a contract, not enforced by
SQL: `UPDATE t SET id = 2 WHERE id = 1` commits on the primary, and the
binlog tells the replica to upsert a row with id=2 — the original row (id=1)
is never deleted, so the replica keeps a stale row and gains a new one.

Root cause is more general than PKs: **before-images are never captured for
UPDATE.** `preupdate_cb` requests only `PreupdateValue::New` for
`SQLITE_UPDATE` (src/capture.rs:194-200) and `finalize_unit` discards
`pk_values` for INSERT/UPDATE entirely (src/capture.rs:366-370). Any identity
change is therefore unrepresentable. #9 is the same missing before-image seen
from the rowid side.

Fix options:

1. Enforce the contract server-side: reject UPDATE/REPLACE statements that
   assign to a PK column (authorizer, not a statement pre-scan).
2. Capture before-images for UPDATE and emit the old PK values so the applier
   can delete the old row (requires protocol + applier change).
3. Adopt changesets, which carry the before-image by design.

## 3. Non-UTF-8 TEXT is silently mangled in the binlog

**Severity:** medium (data corruption). **Status:** open. **Test:** `non_utf8_text`.

`sqlite_value_to_value` (src/capture.rs:289) converts TEXT via
`String::from_utf8_lossy`, and the Hrana read path (`value_ref_to_hrana`,
src/executor.rs:344) does the same. SQLite TEXT may contain arbitrary bytes
(e.g. `CAST(X'fffe' AS TEXT)`); the primary stores them exactly, but the
binlog mangles them to U+FFFD, so the replica diverges. The applier also
cannot be sure a `Value::Text` string round-trips through UTF-8.

The proto field forces this: `Value::Text` is `#[prost(string, tag = "4")]`
(src/binlog.rs:47-48), and prost rejects non-UTF-8 for `string`.

See #13 for the same root cause with a worse failure mode (embedded NUL bytes
hard-stop the applier rather than corrupting one value).

Fix: carry TEXT as `bytes` (or add a distinct `Value::RawText` variant) in the
binlog and Hrana responses; keep `String` only where the wire format requires
valid UTF-8. Note that this fix is only sufficient if the wire format also
stops being SQL text — see #13.

## 4. DDL events include the transaction's DML statements (double apply)

**Severity:** high (data integrity). **Status:** open. **Test:** `ddl_event_includes_dml`.

> **Severity note:** this entry understates the damage. Because of #12, the
> statements recorded for a mixed DML+DDL transaction include `BEGIN;` and
> `COMMIT;` verbatim, so the applier does not "apply the DML twice and usually
> converge" — it fails outright with
> `cannot start a transaction within a transaction` and the replica stops
> advancing. The regression test `ddl_event_includes_dml` currently fails on
> that error, not on a row diff.

`finalize_unit` (src/capture.rs:394-401) records a `DdlEvent` whenever the
schema version changed during the transaction, and puts **every** statement of
the transaction into it — only txn-control and `PRAGMA` are filtered
(src/capture.rs:135-143), and that filter is itself broken (#12). A transaction
like

```sql
BEGIN;
INSERT INTO t VALUES ('b');
CREATE INDEX idx_t_v ON t(v);
COMMIT;
```

produces a record with both `rows = [INSERT 'b']` and
`ddl = ["BEGIN;", "INSERT INTO t VALUES ('b');", "CREATE INDEX ...;", "COMMIT;"]`.
The documented applier algorithm (docs/sync-protocol.md §3) replays the DDL
verbatim **and** applies the row events, so the DML executes twice — when it
gets that far at all.

Once #12 is fixed, the remaining double-apply diverges when the DML is not
idempotent under replay:

- rowid tables **without** a declared PK: `columns` comes from
  `PRAGMA table_info` and excludes `rowid`, so the row event's upsert cannot
  target the original rowid; the replayed INSERT takes a fresh rowid and the
  replica gains a phantom row (this is exactly what the regression test hits,
  and see #9 — this table shape is broken even without DDL);
- DML with side effects: triggers on the replica, `v = v + 1`-style updates
  (the replay advances the counter, then the row event's after-image
  overwrites — wrong intermediate state if anything depends on it), and
  auto-increment counters.

Fix options:

1. Record only statements that actually changed the schema (track
   `PRAGMA schema_version` per statement) in `DdlEvent.statements`.
2. Emit one `DdlEvent` per DDL statement, interleaved in statement order with
   the row events, and change the applier to walk the mixed stream. This is
   also what #5 and #15 need.
3. When a transaction contains DDL, make verbatim replay authoritative and
   emit no row events for that transaction (simplest; loses fine-grained
   events but is correct — replay is what produced the state). Note this makes
   #14 strictly worse: verbatim DDL replay is not idempotent.
4. Reject DDL and DML in the same transaction (see #15's option 3).

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

`DROP` is not required to trigger this — `ALTER TABLE ... ADD COLUMN` and
`ALTER TABLE ... RENAME TO` in the same transaction produce the same class of
malformed event; see #15. The underlying defect is that column names are
resolved from a schema snapshot taken *after* the transaction, while the values
were captured *during* it, and nothing checks that the two agree in length
(see #11).

Fix options:

1. Snapshot the schema per statement inside the transaction (capture-time or
   per-statement resolution) so events reference the schema as it existed
   when the row was written.
2. Detect the failure at finalize and refuse to commit the transaction
   (error before COMMIT) instead of emitting a broken record.
3. Reject DDL that drops/alters a table with buffered row events.
4. At minimum, assert `columns.len() == values.len()` before appending, so a
   malformed record is never written (see #11).

## 6. Binlog append/finalize failures are swallowed (silent divergence)

**Severity:** high (data integrity). **Status:** open. **No test** (hard to
inject a write failure through the HTTP path; needs a fault-injection hook —
making `Binlog::append` swappable behind a trait would be enough, see
[Test harness gaps](#test-harness-gaps)).

`after_success` (src/executor.rs:366-401) finalizes the capture unit and
appends the record after SQLite has already committed. Any failure on that
path is only `tracing::error!`'d:

- `finalize_unit` error (e.g. an unexpected `PRAGMA table_info` failure) →
  the record is dropped, the response is still `OK`;
- `binlog.append` error (disk full, I/O error) → the record is dropped, the
  response is still `OK`;
- `binlog.lock()` poisoned (another thread panicked while holding it — which
  #10 makes reachable) → the record is dropped, the response is still `OK`,
  **and every subsequent transaction on that namespace is silently unlogged
  for the lifetime of the process.**

In all cases the primary committed a transaction the binlog never sees, LSNs
stay contiguous (the failed append assigns no LSN), and the replica has no way
to detect the missing record — the divergence is silent and permanent, exactly
like #1.

**#8 is the same missing invariant seen from the ordering side:** there is no
lock that spans "SQLite committed" → "record durable". Fixing #6 and #8
together is one change, not two.

Fix options:

1. Order the work so the binlog write is the *commit gate*: finalize and
   append the record **before** the transaction commits (write it on
   `commit_hook` via `sqlite3_commit_hook`'s veto return, or use a two-phase
   scheme), so a binlog failure aborts the transaction.
2. At minimum, when append fails, mark the namespace `dirty` and fail all
   subsequent requests on it (503) until an operator resyncs; never return
   `OK` for a commit whose record did not land.
3. Do not swallow a poisoned mutex — treat it as a fatal namespace fault.

## 7. Binlog records have no checksum

**Severity:** medium. **Status:** open. **No test** (needs bit-flip injection).

Records are `[varint length][protobuf]` with no integrity check. A bit flip
inside a valid-length record decodes silently (prost fills defaults for
missing fields); only torn-record truncation is caught at startup
(src/binlog.rs:198-236). Corrupt data would replicate to every device.

The proto layout amplifies this: `Op::Insert = 0` (src/binlog.rs:91) is
protobuf's default for an integer field, so a corrupted or dropped `op` field
decodes as **INSERT**, silently turning a DELETE into an upsert. The same holds
for `RowEvent.table` (empty string) and `Transaction.lsn` (0). Any enum whose
zero value is a meaningful operation is a hazard in a format with no presence
tracking.

Fix: append a CRC32C per record (or a segment trailer with record count +
CRC) and verify in `read_since`, `scan_state`, and on the wire. Separately,
renumber `Op` so 0 is `UNSPECIFIED` and reject it on decode.

## 8. Binlog LSN order can invert relative to SQLite commit order

**Severity:** critical (data integrity). **Status:** open. **No test.**
**Reproduced.**

### Symptom

Two concurrent writers touching the same row: the primary ends with writer B's
value, the replica ends with writer A's — permanently, silently. Observed
output from the probe (16 side tables to widen the window, two writers on one
row):

```
primary final v = 2
binlog shared-row writes, in LSN order: [("62", 2), ("63", 2), ("64", 1)]
replica final v = 1
>>> DIVERGENCE CONFIRMED: LSN order inverted vs commit order
```

Writer A committed **before** writer B on the primary, but was appended to the
binlog **after** it, so the replica replays B then A and ends on A's value.

### Root cause

Nothing ties the SQLite commit order to the binlog append order.

1. Writes are genuinely concurrent. Every baton-less pipeline request opens its
   **own** connection (`ns.open_stream_handle()`, src/http.rs:176-183,
   src/namespace.rs:80-86) — the namespace's `handle` mutex is *not* taken on
   the pipeline path. Only `POST /` (src/http.rs:244), `/checkpoint` and
   `/clone` use it. `handle_pipeline` is synchronous, so on a multi-threaded
   tokio runtime several requests execute in true parallel.
2. SQLite serializes the *commits* (WAL write lock, `busy_timeout` 10 s), and
   the `Arc<Mutex<Binlog>>` serializes the *appends* — but they are two
   independent locks with a gap between them.
3. The gap is not small. `after_success` (src/executor.rs:366-401) runs, after
   SQLite has already released the write lock:
   `PRAGMA schema_version` (a real query) → `finalize_unit` (per-row
   `table_schema` lookups plus a full clone of every captured value) →
   `SystemTime::now()` → *then* `binlog.lock()`. A transaction touching many
   tables or many rows spends milliseconds there, and the schema cache is
   per-connection, so a fresh connection's cache is always cold
   (src/executor.rs:370, src/capture.rs:353-355).

A writer that commits first and finalizes slowly loses the LSN race to a writer
that commits second and finalizes fast.

### Why it is silent and permanent

Identical to #1: the server returns OK to both clients, LSNs are contiguous,
the replica applies a well-formed stream without error, and nothing ever
re-checks. Only the row's value is wrong, forever.

### Scope

This is not an exotic path — it is the default for any backend with a
connection pool issuing concurrent writes, which is the stated use case
(`@libsql/client` from the ragham backend). It also affects `LSN`-ordered
consumers other than replicas: `x-current-lsn` no longer means "all
transactions up to this point, in commit order".

### Fix options

1. **Single writer per namespace.** Route all writes through one connection (or
   one dedicated writer thread) so commit order and append order are the same
   sequence by construction. This also removes the `busy_timeout` contention
   and makes `docs/sync-protocol.md` §4's "single writer per namespace" claim
   true (it currently is not — see #17).
2. **Make the append the commit gate** — the same fix as #6 option 1: write the
   record from inside `sqlite3_commit_hook`, while SQLite still holds the write
   lock. Correct ordering and durability fall out together.
3. A per-namespace write mutex held across `run_stmt_inner` → `after_success`
   would fix ordering but keeps the #6 durability hole and serializes the
   read-only path too.
4. Not sufficient: assigning the LSN earlier (e.g. at commit-hook time) but
   appending later — the *file* order is what `read_since` returns, so
   out-of-order appends would then also need a reorder buffer.

## 9. Tables without a declared PRIMARY KEY diverge on the first UPDATE or DELETE

**Severity:** critical (data integrity). **Status:** open. **No test.**
**Reproduced.**

### Symptom

```sql
CREATE TABLE t (v TEXT);
INSERT INTO t (rowid, v) VALUES (100, 'a');
INSERT INTO t (rowid, v) VALUES (200, 'b');
DELETE FROM t WHERE rowid = 100;
```

```
primary: [(200, "b")]
replica: [(1, "a"), (2, "b")]
```

The replica keeps the deleted row and has entirely different rowids.

### Root cause

For a rowid table with no declared PK, `finalize_unit` sets
`pk_columns = ["rowid"]` (src/capture.rs:358) — but:

- **INSERT/UPDATE events never carry the rowid.** `columns` comes from
  `PRAGMA table_info`, which does not list `rowid`, and `pk_values` is
  explicitly emptied for INSERT/UPDATE (src/capture.rs:366-370). The generated
  statement is `INSERT INTO "t" ("v") VALUES ('a') ON CONFLICT("rowid") DO
  UPDATE SET ...` — no rowid supplied, so SQLite assigns a fresh one on the
  replica. Primary rowid 100 becomes replica rowid 1.
- **DELETE events are keyed by that rowid** (src/capture.rs:375-377), so
  `DELETE FROM "t" WHERE "rowid" = 100` matches nothing on the replica.
- **The upsert is not idempotent either.** Since the rowid is never supplied,
  `ON CONFLICT("rowid")` can never fire, so re-applying a stream suffix (which
  §3 of the protocol doc says is safe) duplicates the row instead of updating
  it.
- **UPDATE on such a table crashes the server** — see #10.

The rowid is available: `preupdate_cb` already stores it in `RawEvent.rowid`
(src/capture.rs:190, 197, 203). `finalize_unit` throws it away for
INSERT/UPDATE.

### Why the differential test never caught this

`dump_db` (tests/common/mod.rs:321-329) compares tables with `SELECT *`, which
**omits `rowid`**. In the reproduction above `compare_dbs` returns `None` for
the two-INSERT prefix — the divergence is literally invisible to the harness.
The generator (tests/differential_convergence.rs:62-99) also never emits a
PK-less table. See [Test harness gaps](#test-harness-gaps).

### Fix options

1. Emit `rowid` in `columns`/`values` for INSERT and UPDATE on rowid tables
   without a declared PK (SQLite accepts an explicit `rowid` in an INSERT
   column list), so the replica's rowids match and the upsert conflict target
   works.
2. Reject writes to tables without a declared PRIMARY KEY (authorizer or
   `CREATE TABLE` gate) — the honest short-term option, and what
   `sqlite3changeset_apply` requires anyway.
3. Adopt changesets, which carry the rowid for such tables.

Do not "fix" this by only changing the DELETE key — the rowids themselves must
match, or a later UPDATE will target the wrong row.

## 10. `row_where` panics on UPDATE of a PK-less table (500 forever, remote DoS)

**Severity:** high (availability + replication halt). **Status:** open.
**No test.** **Reproduced.**

```sql
CREATE TABLE t (v TEXT);
INSERT INTO t VALUES ('a');
UPDATE t SET v = 'b';
```

`GET /sync/v1/namespaces/{ns}/binlog` then panics:

```
thread 'probe_no_declared_pk' panicked at src/sync.rs:281:22:
PK column missing from after-image
```

For such a table `pk_columns = ["rowid"]` and `pk_values` is empty for UPDATE,
so `row_where` (src/sync.rs:269-288) falls into the after-image branch and
looks for `"rowid"` in `row.columns` — which `PRAGMA table_info` never
contains. The `.expect()` at src/sync.rs:281 fires.

Consequences:

- The record is already durable, so **every** subsequent `GET /binlog` request
  for that namespace hits the same record and panics. Replication for the
  namespace is dead until an operator truncates the binlog.
- The panic is inside an axum handler, reached by an unauthenticated request
  (there is no auth at all), so any client that can write a single `UPDATE` on
  a PK-less table can disable sync for that namespace. With `x-namespace` being
  a plain header, that is trivially reachable.
- `row_sql` has a sibling hazard: `panic!("unknown row opcode {}", row.op)`
  (src/sync.rs:263), reachable from a corrupt record (#7).
- A panic while the binlog mutex is held poisons it, which #6 then swallows
  silently for the rest of the process lifetime.

Fix options:

1. Fix #9 (carry the rowid) — this panic disappears.
2. Independently: never panic in a request path. Return a
   `BINLOG_MALFORMED` error event and log loudly.
3. Add the `columns.len() == values.len()` / "every pk_column resolvable"
   invariant at *append* time (see #11), so a record that cannot be rendered is
   never written in the first place.

## 11. Generated columns produce a columns/values length mismatch

**Severity:** high (replication halt). **Status:** open. **No test.**
**Reproduced.**

```sql
CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT,
                w TEXT GENERATED ALWAYS AS (v || '!') STORED);
INSERT INTO t (id, v) VALUES (1, 'a');
```

Generated statement (note 2 column names, 3 values):

```sql
INSERT INTO "t" ("id", "v") VALUES (1, 'a', 'a!') ON CONFLICT("id") DO UPDATE SET "v" = excluded."v";
```

```
failed to apply binlog statement: 3 values for 2 columns
```

`PRAGMA table_info` **excludes** generated columns (only `table_xinfo` lists
them), but `sqlite3_preupdate_count` / `sqlite3_preupdate_new`
(src/capture.rs:170-185) **include** them. `finalize_unit` pairs the two
without checking their lengths (src/capture.rs:366-370, 384-391), and
`row_sql` zips them blindly (src/sync.rs:227-228).

This is a permanent halt: the replica cannot apply that LSN, and cannot skip it.

Note `docs/sync-protocol.md` §7 currently claims generated columns are
supported and shifts the burden to the client ("the applier should not list
them in the INSERT column list"). That is not implementable — the applier
receives a `columns` list that is already too short and a `values` list that is
already too long, with no way to tell which value belongs to which column. See
[Documentation defects](#documentation-defects).

Fix options:

1. Resolve columns with `PRAGMA table_xinfo` and drop generated columns from
   both `columns` **and** `values` by index before emitting the event.
2. Reject `CREATE TABLE` with generated columns until (1) is done.
3. Add a hard invariant in `Binlog::append`: for INSERT/UPDATE,
   `columns.len() == values.len()`; for DELETE,
   `pk_columns.len() == pk_values.len()`. This turns #5, #11 and #15 into loud
   append-time failures instead of unusable records — which, combined with #6
   option 1, means the offending transaction never commits.

## 12. `is_txn_control` does not strip a trailing semicolon

**Severity:** high (replication halt). **Status:** open. **No test.**
**Reproduced.**

`is_txn_control` (src/capture.rs:135-143) matches the first whitespace-delimited
word against `BEGIN`/`COMMIT`/`END`/`ROLLBACK`/`SAVEPOINT`/`RELEASE`/`PRAGMA`.
It never strips the trailing `;`:

```
            "BEGIN" -> filtered=true
           "BEGIN;" -> filtered=false
           "begin;" -> filtered=false
          "COMMIT;" -> filtered=false
  "BEGIN IMMEDIATE;" -> filtered=true
      "/*c*/ BEGIN;" -> filtered=false
```

`run_sequence` splits with `sqlite3_prepare_v2`'s tail pointer
(src/executor.rs:125-185), which **includes** the semicolon in each statement
slice, so a client sending `BEGIN; ...; COMMIT;` — the shape
`@libsql/client` batches use, and the shape the differential generator emits
(tests/differential_convergence.rs:281) — records `"BEGIN;"` and `"COMMIT;"` in
`unit.stmts`.

Whenever that transaction also changes the schema, they land in
`DdlEvent.statements` and are served verbatim:

```json
{"lsn":2,"statements":["BEGIN;","INSERT INTO t VALUES (1,'a');",
  "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7;","COMMIT;",
  "INSERT INTO \"t\" (\"id\", \"v\", \"c\") VALUES (1, 'a') ON CONFLICT..."]}
```

The applier wraps the list in `BEGIN IMMEDIATE` (docs/sync-protocol.md §3), so:

```
failed to apply binlog statement: cannot start a transaction within a transaction
stmt=BEGIN;
```

Permanent halt. Worse, if the `BEGIN;` were tolerated, the embedded `COMMIT;`
would commit the applier's transaction **mid-list**, so the remaining
statements would run outside any transaction — the protocol's "the object
boundary IS the transaction boundary" guarantee (§3) would be violated and a
crash mid-apply would leave a torn transaction on the replica.

A leading comment defeats the filter too (`/*c*/ BEGIN;`), same as #19.

Fix options:

1. Normalize before matching: trim, strip leading comments, strip the trailing
   `;`, then compare case-insensitively. This is a two-line fix and should
   happen regardless of how #4 is resolved.
2. Better: stop deriving DDL from "every statement in the transaction" (#4
   option 1/2) so txn-control statements are never candidates.
3. Belt-and-braces: have the applier reject any statement in a record whose
   first token is txn-control, rather than executing it.

## 13. Embedded NUL in TEXT truncates the generated SQL

**Severity:** high (replication halt). **Status:** open. **No test.**
**Reproduced.**

```sql
CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO t VALUES (1, CAST(X'6100620063' AS TEXT));   -- 'a\0b\0c'
```

The event serializes fine (JSON escapes it as `\u0000`), but the applier gets a
SQL string with a NUL in the middle of a literal:

```
failed to apply binlog statement: unrecognized token: "'a"
  in INSERT INTO "t" ("id", "v") VALUES (1, 'a<NUL>b<NUL>c') ON CONFLICT... at offset 39
```

Every SQLite/JNI/CGO binding terminates the SQL text at the first NUL, so the
statement is cut mid-literal. This is a **hard, permanent stop**: the replica
can never apply that LSN. On Android it may be worse than an error — JNI's
modified UTF-8 encodes U+0000 as `0xC0 0x80`, so `execSQL` could silently store
a two-byte sequence instead, producing quiet corruption rather than a failure.

Same root cause as #3 (`sqlite_value_to_value` → `String` → literal), different
failure mode. **Fixing #3 alone is not enough**: even with TEXT carried as
bytes, `sql_literal` (src/sync.rs:318) still renders it into a NUL-containing
SQL string. Embedded NULs are only expressible in SQL via `char(0)` or
`CAST(X'..' AS TEXT)` concatenation — i.e. not as a plain literal at all.

Fix options:

1. **Stop shipping SQL text.** Serve the protobuf `Transaction` (which already
   exists, src/binlog.rs:126-136) and have the applier bind typed parameters.
   This is the only fix that closes #3 and #13 together, and it also removes
   the float-literal machinery, the `X'..'` blob hex 2× bloat, and the
   identifier-quoting surface. See also #14.
2. Reject TEXT values containing NUL at capture time (loud, lossy).

## 14. DDL replay is not idempotent (a crashed replica is bricked)

**Severity:** high (protocol contract violated). **Status:** open. **No test.**
**Reproduced.**

```
first apply OK
failed to apply binlog statement: table t already exists
  in CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT); at offset 13
second apply (crash-recovery replay) = false
```

`docs/sync-protocol.md` makes two explicit promises that this breaks:

- §1: "Applying is idempotent by construction (see §3), so a crash between
  'apply succeeded' and 'persist local_lsn' is harmless — the transaction is
  simply re-applied."
- §3: "Re-applying any suffix of a binlog yields the same state: … DDL is
  transactional on the replica, and DDL+DML of one source transaction apply in
  one replica transaction."

Row events are idempotent (upsert / no-op delete). **DDL is not.**
`CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE ADD COLUMN` and `DROP TABLE` all
fail on replay. So a replica that applies a DDL transaction and then loses power
before persisting `local_lsn` will, on every subsequent sync attempt, retry that
transaction and fail — **permanently stuck**, following the documented algorithm
exactly.

This also undermines the clone path: #17's snapshot can legitimately be *ahead*
of `x-lsn`, and the protocol relies on idempotent replay to absorb that. With
non-idempotent DDL, a clone taken just after a schema change fails to sync.

Fix options:

1. Make the generated DDL idempotent where SQLite allows it
   (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`,
   `DROP TABLE IF EXISTS`). Partial: `ALTER TABLE ADD COLUMN` has no
   `IF NOT EXISTS`, and rewriting user DDL is its own risk.
2. Have the applier persist `local_lsn` **inside the same replica transaction**
   that applies the statements (a `sync_state` table updated in the same txn).
   Then the crash window does not exist and idempotency is not needed. This is
   the correct fix and it is a protocol/doc change, not a server change —
   §1 currently tells clients to do the opposite.
3. Ship the schema version alongside each record so the applier can skip
   already-applied DDL.

Whatever the fix, §1 and §3 must be corrected — see
[Documentation defects](#documentation-defects).

## 15. `ALTER TABLE` after DML in the same transaction emits malformed events

**Severity:** high (replication halt). **Status:** open. **No test.**
**Reproduced.** Variant of #5 that does not involve `DROP`.

`ADD COLUMN` — 3 column names, 2 values:

```sql
BEGIN; INSERT INTO t VALUES (1,'a'); ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 7; COMMIT;
```
```sql
INSERT INTO "t" ("id", "v", "c") VALUES (1, 'a') ON CONFLICT("id") DO UPDATE SET ...
```

`RENAME TO` — the event still names the old table, whose `table_info` now
fails, so `columns` is empty (exactly #5's shape):

```sql
BEGIN; INSERT INTO t VALUES (1,'a'); ALTER TABLE t RENAME TO t2; COMMIT;
```
```sql
INSERT INTO "t" () VALUES (1, 'a') ON CONFLICT("rowid") DO NOTHING;
```

Root cause is #5's: `finalize_unit` resolves column names from the
*post-transaction* schema (src/capture.rs:351-356) while the values were
captured against the *pre-ALTER* schema, and nothing checks that they agree.
Note the `RENAME` case also loses the `id` PK (it falls back to `"rowid"`),
compounding with #9.

`ALTER TABLE ... ADD COLUMN` inside a migration transaction that also
backfills data is an extremely common shape, so this is likely to be hit before
#5 is.

Fix: same options as #5, plus the append-time length invariant from #11.

## 16. Records over 64 MiB brick the binlog on restart

**Severity:** high (availability, permanent). **Status:** open. **No test.**
**Reproduced.**

`Binlog::append` (src/binlog.rs:247-291) writes a record of any size.
`scan_state`, which runs on every `Binlog::open`, hard-fails on anything over
64 MiB (src/binlog.rs:216-218):

```
appended oversize record, lsn=1
reopen after oversize record: Err("binlog: implausible record length 73400355 in ".../0000000001.seg")
```

The write and read limits are asymmetric, so a single large transaction (one
70 MiB blob is enough) is accepted at runtime and then makes the namespace
**permanently unopenable**. The failure surfaces badly, too:
`open_namespace` → `Err` → `NamespaceManager::get` returns `None`
(src/namespace.rs:170-181) → the HTTP layer reports **404 "namespace does not
exist"**. An operator sees "namespace missing", not "corrupt binlog".

Note this is also a startup-time landmine for the whole process: the same
`bail!` path is taken by any genuinely corrupt record (#7).

Fix options:

1. Enforce the same ceiling on append (reject the transaction) and make it
   configurable alongside `SQLD_MAX_SEGMENT_BYTES`.
2. Raise the read ceiling and make the sanity check relative to the segment
   size rather than a magic constant.
3. Report a namespace that fails to open as 503 + an explicit reason, never
   404. A failed open should be visible in metrics (#22).

## 17. `wal_checkpoint` result is discarded and clone is not isolated from writers

**Severity:** medium (possibly stale or torn snapshot). **Status:** open.
**No test.** Partially reproduced.

Two problems in `handle_clone` (src/sync.rs:162-196):

**(a) The checkpoint may silently not happen.**
`execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")` throws away the pragma's
result row, which is exactly where SQLite reports failure. Reproduced with one
concurrent reader:

```
execute_batch(wal_checkpoint(TRUNCATE)) = Ok(())  <-- no error surfaced
actual checkpoint result (busy, log, checkpointed) = (1, 3, 3)
wal size after 'successful' TRUNCATE = 12392 bytes (0 == truly truncated)
```

`busy = 1` means the checkpoint did not complete, yet the handler proceeds to
`std::fs::read(ns.db_path())` as if "the db file alone contains the full
snapshot" (src/sync.rs:172). The same discarded-result bug is in
`handle_checkpoint` (src/http.rs:456-459), so `POST /checkpoint` can report
success without checkpointing.

**(b) The clone is not serialized against writers.**
`handle_clone` takes `ns.handle.lock()` — the namespace's *main* handle only.
Baton'd and baton-less pipeline requests write on their **own** connections
(#8), so writes continue throughout the checkpoint and the `fs::read`. Hence:

- New commits between the checkpoint and the read land in the WAL, not the file,
  while `x-lsn` (read at src/sync.rs:178) can already include them → the
  snapshot is **behind** `x-lsn` → those transactions are skipped by the
  client's `since=x-lsn` fetch and lost. Not reproduced in 12 attempts on
  tmpfs, but the window is unconditional.
- Another connection's auto-checkpoint (default 1000 pages ≈ 4 MB WAL) can be
  writing pages into the db file *while* `fs::read` walks it → a torn,
  potentially non-openable snapshot. Not reproduced; `integrity_check` returned
  `ok` in every attempt, but that is timing, not a guarantee.

`docs/sync-protocol.md` §4's justification — "the server serializes clones
against writes (single writer per namespace)" — is therefore false as written.
See [Documentation defects](#documentation-defects).

Fix options:

1. Use SQLite's own consistent-snapshot mechanism: the backup API (the
   `backup` feature is already enabled in Cargo.toml) or `VACUUM INTO`, and read
   `current_lsn` inside the same read transaction. That makes both the
   checkpoint and the exclusion problems moot.
2. Route all writes through a single per-namespace writer (#8 option 1) so
   `ns.handle.lock()` actually excludes them.
3. Always check pragma result rows; `query_row` + assert `busy == 0`, and
   surface a real error otherwise.
4. Stream the snapshot instead of `std::fs::read`-ing it into memory (see
   [Operational / scale issues](#operational--scale-issues)).

## 18. `min_lsn` off-by-one forces needless full re-clones

**Severity:** medium (cost, not correctness). **Status:** open. **No test.**
**Reproduced.**

`min_lsn` is the LSN of the **first record in the oldest retained segment**
(src/binlog.rs:234, 331-335), i.e. that record is still available. A client
whose `local_lsn` equals `min_lsn - 1` therefore has every record it needs
(`min_lsn`, `min_lsn + 1`, …), but `handle_binlog` rejects it
(src/sync.rs:99-109):

```
info = {"current_lsn":40,"min_lsn":30,"schema_version":1}
since=28 -> status=409 Conflict events=0
since=29 -> status=409 Conflict events=0     <-- records 30..40 are all present
since=30 -> status=200 OK events=10
```

The condition `query.since < binlog.min_lsn()` should be
`query.since + 1 < binlog.min_lsn()`. A full database re-clone (potentially
hundreds of MB per device, see #17) is triggered for a client sitting exactly on
the boundary — which is precisely where clients accumulate after a GC.

Two adjacent problems in the same area:

- **GC under-accounts the active segment.** `maybe_gc` (src/binlog.rs:320-343)
  sums the sizes cached in `self.segments`, but the active segment is pushed
  with size `0` (src/binlog.rs:310) and only updated on `rotate`
  (src/binlog.rs:299-301). Retention therefore overshoots
  `SQLD_MAX_BINLOG_BYTES` by up to one segment.
- **Retention has no time floor and no awareness of replica positions.** A
  write burst can evict the whole window in seconds and force every device to
  re-clone. There is no metric for "clients behind retention" (#22).

## 19. The ATTACH guard is bypassable and the hook ignores the database name

**Severity:** medium. **Status:** open. **No test.** **Reproduced.**

`run_stmt_inner` rejects ATTACH with a string prefix test
(src/executor.rs:194-199):

```rust
if sql.trim_start().to_ascii_uppercase().starts_with("ATTACH")
```

A leading comment defeats it (same weakness as #12):

```
ATTACH-with-comment status=200 OK
  (sql: "/*x*/ ATTACH DATABASE ':memory:' AS other")
```

That matters because `preupdate_cb` **ignores the database name** — the
`z_db` parameter is bound to `_z_db` and never read (src/capture.rs:153).
Row events from an attached database are therefore recorded with only the table
name, indistinguishable from `main`, and the applier will write them into the
replica's `main`. That is arbitrary phantom data on every replica, from a single
statement.

Related: `PRAGMA` is filtered wholesale from `DdlEvent.statements`
(src/capture.rs:140) using the same fragile first-word test, and `PRAGMA`
statements that *do* need replicating are dropped (#20).

Fix options:

1. Use `sqlite3_set_authorizer` for every construct that must be forbidden
   (`SQLITE_ATTACH`, `SQLITE_DETACH`, and whatever #1/#2/#9/#11 end up
   rejecting). An authorizer is evaluated by SQLite's parser, so comments,
   casing and whitespace cannot evade it — a string pre-scan structurally
   cannot be made safe.
2. Independently, read `z_db` in `preupdate_cb` and drop (or hard-error on)
   any event whose database is not `main`, so a future ATTACH path cannot
   corrupt replicas silently.

## 20. `PRAGMA user_version` is never replicated

**Severity:** medium. **Status:** open. **No test.** **Reproduced.**

```sql
CREATE TABLE t (id INTEGER PRIMARY KEY);
PRAGMA user_version = 5;
```

Binlog contains only the `CREATE TABLE`; the primary reports
`user_version = 5`, the replica stays at 0.

Two reasons: `user_version` lives in the database header and does **not** bump
`PRAGMA schema_version`, so `finalize_unit`'s DDL detection
(src/capture.rs:394) never fires; and `PRAGMA` is filtered out of
`DdlEvent.statements` regardless (src/capture.rs:140).

This is not cosmetic for the stated deployment. `androidx.sqlite` /
Room — named in the README as the replica runtime — keys its entire migration
state off `user_version`: `SupportSQLiteOpenHelper` compares the file's
`user_version` against the compiled schema version and runs `onUpgrade` /
`onCreate` accordingly. A replica whose data and schema are at v5 but whose
`user_version` reads 0 will attempt to re-run every migration on the cloned
database, on every open.

Note the `clone` path *does* carry `user_version` (it's a byte copy of the
file), so this only bites the incremental path — meaning a replica's
`user_version` silently depends on whether it cloned before or after the
migration.

Fix options:

1. Track `PRAGMA user_version` (and `application_id`) per transaction and emit
   an explicit event when it changes.
2. Whitelist a small set of replicable PRAGMAs instead of dropping all of them.
3. Document `user_version` as server-managed-only and have replicas keep their
   own schema-version bookkeeping in a `sync_state` table (which they need
   anyway for #14 option 2).

## 21. `CaptureUnit.poisoned` is dead code that reads like a safeguard

**Severity:** low (correctness of the code's own documentation). **Status:** open.

`CaptureUnit.poisoned` is declared (src/capture.rs:53), initialized to `false`
(src/capture.rs:64) and checked in `finalize_unit` (src/capture.rs:347-349) —
but **nothing ever sets it to `true`**. Verified: the only other match for
`poisoned` in `src/` is the unrelated mutex message at src/executor.rs:392.

Its doc comment describes a mitigation that does not exist:

```rust
/// True when a savepoint rollback occurred mid-unit: the buffer is no
/// longer trustworthy. Dropped at commit, and a divergence warning is
/// logged (documented limitation: savepoints are not replicated).
```

No savepoint rollback is ever detected (that is #1's whole point — the hook
does not fire), nothing is dropped, and no warning is logged. Anyone reading
`capture.rs` will reasonably conclude savepoints are handled defensively. They
are not.

Fix: delete the field and its comment, or implement it. Do not leave a
half-declared safety net in the file that #1 has to spend a paragraph
contradicting. Note that even if implemented, dropping the record is not an
acceptable resolution (#1 fix option 4).

## 22. There is no divergence detection anywhere

**Severity:** high (design gap; it is what makes every other issue permanent).
**Status:** open.

Read the endings of #1, #2, #6, #8, #9: *silent*, *permanent*, *never
self-heals*, *undetectable by either side*. That is not a coincidence of five
separate bugs — it is a single missing capability. The protocol has no way for a
replica or an operator to answer "is this replica still equal to the primary?"

Concretely:

- Nothing in the wire format lets a replica verify its state. `info` returns
  LSN watermarks and `schema_version`; a diverged replica has correct LSNs.
- The applier's only failure signal is a SQL error, which catches the
  *halting* bugs (#10–#16) and none of the *diverging* ones.
- There are no metrics at all, so the server side cannot see it either. The
  existing `tracing::error!`/`warn!` calls for #1 and #6 go to stdout with no
  alerting path.
- `tests/differential_convergence.rs` is the only divergence detector in the
  project, and it runs in CI-less local test runs against synthetic data — and
  #9 proved it can return "converged" for a diverged pair.

This is worth more than fixing #1–#5 individually, because it changes the
failure mode of the *entire* class from "silent permanent corruption" to
"a re-clone".

Fix options:

1. **Ship a digest with each transaction record** (or every N transactions):
   per-table row count plus an order-independent rolling hash of the rows
   (e.g. XOR/sum of per-row hashes, so it is incrementally maintainable). The
   replica compares after applying; on mismatch it re-clones. This makes every
   capture bug self-healing.
2. **Add a `verify` endpoint**: the replica sends its per-table digests, the
   server compares against its own. Cheaper than (1) on the write path, but
   detection is only as timely as the client's polling.
3. **Metrics + alerting** as a floor: append failures, finalize failures,
   poisoned locks, namespaces that failed to open (#16), replicas behind
   retention (#18), LSN lag distribution, applier error reports from clients.
4. Have replicas report their applied LSN and last error back to the server,
   so "N devices stuck at LSN k" is observable at all.

---

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
  aligned. Re-verified: the generated statement is
  `INSERT INTO "t" ("id", "v") VALUES (1, 'a') ON CONFLICT("id") ...`, and
  applying it on the replica advances the replica's own `sqlite_sequence`
  identically. (`sqlite_sequence` itself is never captured as a row event.)
- FK cascade actions: captured as explicit row events on the child tables;
  the applier's `foreign_keys = OFF` + event replay converges.
- Statement-level constraint failures mid-statement: partial events are
  truncated (covered by `failed_statement_truncates_partial_events` in
  src/executor.rs).
- **Triggers on the primary** (newly verified): an `AFTER INSERT` trigger's
  writes to a second table are captured as explicit row events on that table,
  and the replica converges (with `foreign_keys = OFF` and no replica-side
  triggers, per docs/sync-protocol.md §3).
- **`CREATE TABLE ... AS SELECT`** (newly verified): converges — no preupdate
  events fire for the CTAS row inserts, and the verbatim DDL replay recreates
  the rows on the replica. Note this converges *because* verbatim replay is
  authoritative for that statement; it is fragile in exactly the ways #4
  describes if CTAS is ever mixed with captured DML in one transaction, and it
  assumes the `SELECT` is deterministic (an `ORDER BY`-less CTAS from a table
  whose rowids differ (#9), or one using `random()`/`datetime('now')`, will
  not converge).

## Documentation defects

Claims in the committed docs that are false against the current code. Listed
separately because a wrong guarantee is worse than a missing one — clients are
being told to rely on these.

**`docs/sync-protocol.md`**

| Location | Claim | Reality |
|---|---|---|
| §1 (lines ~44-46) | "a crash between 'apply succeeded' and 'persist local_lsn' is harmless — the transaction is simply re-applied" | False for any transaction containing DDL: replay fails with `table t already exists` and the replica is permanently stuck (#14). |
| §3 "Idempotency" (lines ~150-157) | "Re-applying any suffix of a binlog yields the same state"; "INSERT is an upsert keyed by PK" | False for DDL (#14) and for tables without a declared PK, where the conflict target can never fire so re-apply duplicates rows (#9). |
| §3 "Apply rules" (lines ~144-147) | the statement list is applied in one transaction | Violated in practice: the list can itself contain `BEGIN;`/`COMMIT;` (#12). |
| §4 (lines ~178-180) | "the server serializes clones against writes (single writer per namespace)" | There is no single writer per namespace — every pipeline request opens its own connection (#8), and `handle_clone` locks only the main handle (#17). |
| §4 (line ~162) | "checkpointed so the file alone is complete" | The checkpoint's `busy` result is discarded; it may not have happened (#17). |
| §6 (lines ~196-198) | "A transaction's binlog record is written (and fsynced) before the Hrana response is sent: a client that saw 'committed' can always find the transaction in the binlog" | False on any append/finalize failure (#6) — the response is still OK. Also, the record's *order* is not the commit order (#8). |
| §7 (lines ~207-210) | "Savepoints … are not replicated; the transaction's binlog record is dropped (warned in server logs)" | Nothing is dropped and nothing is warned; the record is written with wrong rows (#1, #21). |
| §7 (lines ~215-217) | "Generated columns are captured as part of the after-image; the applier should not list them in the INSERT column list" | Not implementable by the applier — the server emits mismatched `columns`/`values` lengths (#11). |
| §7 (line ~218) | "Binary values are preserved exactly (blob bytes, not base64, on the wire)" | Blobs are rendered as `X'<hex>'` SQL literals, not raw bytes — 2× size. TEXT is not preserved exactly at all (#3, #13). |
| §5 / §3 | `since < min_lsn` ⇒ 409 | Off by one; `since == min_lsn - 1` is serviceable but rejected (#18). |

**`README.md`**

- "the transaction's record is dropped with a warning" (savepoints) — same
  false claim as §7 above (#1, #21).

**Code comments**

- src/binlog.rs:86-87: "Row mutation opcode, same values as SQLite's
  `SQLITE_INSERT` / `SQLITE_UPDATE` / `SQLITE_DELETE` hook opcodes." They are
  not: SQLite's are 18 / 23 / 9, `Op` is 0 / 1 / 2. Misleading, and the `0`
  default is itself a hazard (#7).
- src/capture.rs:50-53: describes the non-existent `poisoned` mechanism (#21).
- src/capture.rs:220-224 (`rollback_cb`): "Fires on transaction rollback
  (including savepoint rollbacks)" — it does not fire for `ROLLBACK TO` (#1).
- src/capture.rs:6: "UPDATE -> full after-image only (PKs are never updated by
  contract)" — the contract is unenforced (#2) and the same assumption breaks
  for rowid-keyed tables where the "PK" is not in the after-image at all
  (#9, #10).
- src/sync.rs:1-15 (module docs): describes the response as buffered/simple;
  see the stale ISSUES.md bullet corrected under
  [Operational / scale issues](#operational--scale-issues).

## Test harness gaps

The harness deserves its own section because it returned a **false negative**
for #9 — `compare_dbs` reported "converged" for a primary/replica pair that
differed in both row count and every rowid. Gaps, in rough order of how much
they hide:

- **`dump_db` compares with `SELECT *`, which omits `rowid`**
  (tests/common/mod.rs:321-329). Every rowid-keyed divergence is invisible
  (#9). Should select `rowid, *` for rowid tables (detectable via
  `PRAGMA index_list` / a `WITHOUT ROWID` check, or just try `SELECT rowid`).
- **`compare_dbs` ignores database-level state**: `PRAGMA user_version` (#20),
  `application_id`, and `sqlite_sequence`. `dump_schema` filters
  `name NOT LIKE 'sqlite_%'`, so `sqlite_sequence` divergence is invisible too.
- **`dump_schema` does not use `table_xinfo`**, so generated-column
  divergence would not be seen even if #11 didn't hard-fail first.
- **The generator never emits** (tests/differential_convergence.rs:62-99,
  255-342): tables without a declared PRIMARY KEY (#9, #10), generated columns
  (#11), non-UTF-8 or NUL-containing TEXT (#3, #13), DDL *inside* a transaction
  block (#4, #12), `ALTER`/`DROP`/`RENAME` inside a transaction block (#5,
  #15), `SAVEPOINT` (#1), PK-column updates (#2), or oversize values (#16).
  The header comment explains the exclusions as "known divergence", which is
  reasonable for #1–#3, but the excluded region is where all the new bugs live.
- **Everything is single-threaded.** No test issues concurrent pipeline
  requests, so #8 — the most severe issue in this file — could not have been
  found by the existing suite. A convergence test with N concurrent writers on
  one row is a few lines and would have caught it.
- **No re-apply / crash simulation.** The protocol's central idempotency claim
  (§1, §3) is only checked inside one unit test
  (`sync::tests::renders_and_applies_dml`, which re-applies a DML-only stream).
  Re-applying a stream containing DDL is never tested — that is #14.
- **No fault injection for #6.** ISSUES.md calls this "hard to inject", but it
  only requires `DbHandle.binlog` to be swappable (a trait object, or a
  `#[cfg(test)]` failure counter inside `Binlog`). Same hook would let #16 and
  #7 be tested.
- **`GET /binlog` panics are not covered.** No test asserts that a malformed
  record produces an error response rather than a 500/panic (#10).
- **Sloppiness in test helpers**: `src/stream.rs:210` contains a no-op
  `std::mem::forget::<()>(());`, and the same helper drops its `TempDir` while
  the SQLite connection is still open (the directory is deleted underneath the
  live connection).
- **No CI.** The repo has a single commit and no workflow. `cargo test`,
  `clippy -D warnings`, `fmt --check`, and a nightly high-`DIFF_SEEDS`
  differential run would all be useful — but fix the gaps above first, or CI
  will keep certifying divergence as convergence.

## Operational / scale issues (no tests; see code)

- **Eager namespace open at startup** (src/namespace.rs:130-157): every
  namespace on disk is opened immediately. At 100k namespaces this means
  ~300k FDs (db + `-wal` + `-shm` each; default ulimit is 1024, so namespaces
  silently fail to open beyond the limit), ~2 MB default page cache per
  connection (~200 GB RAM), and a full decode scan of every retained binlog
  segment before serving. Needs lazy open + idle eviction + bounded
  `cache_size` per connection.
  Note `load_existing` only scans the top level, so nested namespaces (`a/b`,
  which `NamespaceName` permits) are not preloaded — they happen to work via
  the lazy path in `get`, so the two code paths disagree about what exists.
- **Startup cost is O(total binlog bytes).** `scan_state` (src/binlog.rs:198)
  decodes *every record of every segment* just to recover `current_lsn`, which
  is obtainable from the last record of the last segment. It also aborts the
  whole namespace on the first bad record (#7, #16).
- ~~**`GET /binlog` holds the namespace binlog mutex for the whole read** and
  buffers the entire response in memory (src/sync.rs:79-103)~~ — **fixed**:
  `iter_since` (src/binlog.rs:358-369) opens the segment handles under the lock
  and releases it, and records are decoded lazily. Two problems remain in its
  place:
  - the `BinlogIter` does **blocking file I/O on a tokio worker thread**
    (`futures_util::stream::iter` over a synchronous iterator, src/sync.rs:115),
    so a client catching up on a large backlog occupies a runtime thread for the
    whole stream;
  - it opens **one FD per segment per streaming client** (src/binlog.rs:359-363)
    — with 256 MiB / 16 MiB = 16 segments, 1000 concurrent streams is 16k FDs on
    top of the per-namespace ones.
- **`GET /clone` runs a blocking `std::fs::read` of the whole DB inside an
  async handler** while holding the namespace lock (src/sync.rs:179); also
  unauthenticated and unthrottled — one client can repeatedly download entire
  databases. The lock it holds does not actually exclude writers (#17).
- **All SQLite work runs on async worker threads.** `handle_pipeline` is fully
  synchronous, and includes `busy_timeout(10s)` waits (src/namespace.rs:83, 219)
  and a per-transaction `sync_data()` fsync (src/binlog.rs:287). Under write
  contention this blocks tokio workers for seconds and starves `/health` and
  every in-flight SSE stream. Needs `spawn_blocking` or a dedicated
  per-namespace writer thread (which #8 wants anyway).
- **No graceful shutdown.** `axum::serve(listener, app).await` (src/lib.rs:51-52)
  with no `with_graceful_shutdown` and no signal handler. A SIGTERM (i.e. every
  rolling deploy) can land in the commit→append window and silently lose a
  committed transaction's record — #6 on a schedule.
- **Unbounded stream registry.** `StreamRegistry::acquire(None, ...)`
  (src/stream.rs:111-130) creates a new connection per baton-less request with
  no cap on live streams, so connection/FD/memory growth is client-controlled.
  The `STREAM_TTL` of 10 s (src/stream.rs:30) is also aggressive for mobile
  clients holding a transaction across a flaky link. Minor: the id-collision
  loop at src/stream.rs:114-119 checks the map but does not insert, so two
  concurrent fresh acquires can pick the same id and the second `release`
  overwrites the first.
- **Admin API shares the listener and has no auth.** `/v1/namespaces/*`
  (src/http.rs:44-64), including `DELETE`, is served on the same port as user
  traffic; libsql-server puts it on a separate admin listener. Combined with
  "no auth anywhere", anyone who can reach port 8080 can delete any namespace.
- **No auth** anywhere (by design; the backend's bearer token is ignored) —
  the server must only be reachable from trusted networks. Note `x-namespace`
  is an unauthenticated header, so any reachable client can read or write any
  namespace, and trigger #10 on any of them.
- **Error mapping is coarse.** Every `rusqlite::Error` becomes HTTP 500
  (src/error.rs:44-53); `SQLITE_BUSY` should be 503 + retriable, and a
  namespace that exists but failed to open is reported as 404 (#16).
- **No metrics/alerting**: divergence warnings (#1/#6/#8) exist only as logs
  that nobody watches. See #22.
- **Container/build**: the image runs as root with no `HEALTHCHECK`, and the
  toolchain is pinned only in the Dockerfile (`rust:1.85`) while `Cargo.toml`
  requires `edition = "2024"` — exactly that minimum, with no slack and no
  `rust-toolchain.toml`. `[profile.release]` sets nothing (no LTO, no
  `panic` policy — relevant given the panics in #10).
- **Config surface is thin** (src/config.rs): no listen TLS, no admin port, no
  auth token, no connection/stream caps, no retention *time* floor, no
  `cache_size`, no worker-thread count, no log format switch.

## Reproduction notes

Issues #8–#20 were reproduced with throwaway integration tests placed in
`tests/`, using the existing `tests/common` harness (`TestServer`,
`apply_binlog`, `compare_dbs`) and then deleted — no committed test covers them
yet. To redo any of them:

- Most are a `TestServer::new()` + `POST /v1/namespaces/{ns}/create` + a list of
  `seq(...)` pipeline requests + `fetch_binlog(ns, 0)` + `apply_binlog` into a
  fresh in-memory connection, then `compare_dbs`. Print the raw SSE body — for
  the malformed-statement issues (#11, #12, #13, #15) the bug is visible in the
  generated SQL before you even apply it.
- **#8** needs real parallelism: `#[tokio::test(flavor = "multi_thread")]`, two
  `tokio::spawn`ed writers on one row, and a wide finalize window for one of
  them (a transaction that also inserts into ~60 other tables works — each is a
  cold `PRAGMA table_info` on that request's fresh connection). Compare the
  primary's final value against the replica's.
- **#9** must bypass `dump_db`: query `SELECT rowid, v FROM t` on both sides
  directly, because `compare_dbs` returns `None` for the diverged pair.
- **#10** panics inside the handler, so assert on `fetch_binlog` returning
  rather than on a body.
- **#14** is `apply_binlog(&replica, &binlog)` twice on the same connection,
  with the second call wrapped in `catch_unwind`.
- **#16** is a `Binlog`-level unit test: `append` a `Transaction` holding a
  70 MiB blob, drop it, then `Binlog::open` the same directory.
- **#17(a)** is a plain rusqlite test: hold a read transaction on a second
  connection, then compare `execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")`
  (returns `Ok`) against `query_row` of the same pragma (returns `busy = 1`).
- **#18** needs GC to have run: `TestServer::with_limits(200, 400)` and ~40
  small transactions, then fetch with `since = min_lsn - 1`.

When these become committed tests, `tests/data_integrity_regressions.rs` is the
right home for the convergence ones (#8, #9, #15, #20) and a new
`tests/applier_failures.rs` for the ones that assert the *applier* hard-fails
(#11, #12, #13, #14). Fix the harness gaps first — in particular the `SELECT *`
rowid blindness, or the #9 test will pass while the bug is live.
