# replite

A primary-node SQLite server with **row-level replication** to read
replicas — mobile apps, edge nodes, other servers — built on **vanilla
SQLite** (no libsql fork).

Three pieces of the core idea:

1. **libsql-compatible write path (Hrana)** — existing `@libsql/client`
   SDKs talk to replite unchanged (JSON + protobuf wire formats), so the
   backend stack keeps working.
2. **MySQL-like binlog** — every committed transaction is captured with
   SQLite's C hooks (`preupdate`/`commit`) into a protobuf **binlog**
   (after-images for INSERT/UPDATE, PKs for DELETE, verbatim DDL), LSN'd
   and append-only. Clients pull `GET /sync/v1/.../binlog?since=LSN`.
3. **Embedded replica** — any client is a read replica running plain
   SQLite (e.g. `androidx.sqlite` on Android, or the applier in
   `tests/common/mod.rs`): it `clone`s once, then streams binlog deltas
   and applies them locally (the same algorithm as a MySQL replica).

One-way replication: the server is the only writer; replicas are read-only
mirrors. The client-side contract is in [docs/sync-protocol.md](docs/sync-protocol.md).

```sh
SQLD_DB_PATH=./data cargo run   # listens on 0.0.0.0:8080
```

See [ISSUES.md](ISSUES.md) for known correctness gaps (some with failing
regression tests).
