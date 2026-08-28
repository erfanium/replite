//! Differential convergence tests (property-based).
//!
//! Random statement sequences are executed on a primary; the resulting
//! binlog is fetched and applied to a fresh replica; the two databases must
//! be identical, schema (`sqlite_master`) and rows. This is differential
//! testing: the same input goes through two paths — direct execution vs.
//! capture -> binlog -> apply — and the outputs must converge.
//!
//! Determinism: every sequence is driven by a fixed seed (`DIFF_SEEDS`
//! sequences of `DIFF_STMT` statements by default). On divergence the seed
//! and the shrunk statement list are printed.
//!
//! Shrinking: on failure, statements are removed from the tail as long as
//! the divergence persists, so the panic shows a minimal reproducer.
//!
//! Excluded constructs (known divergence, see ISSUES.md #1):
//! - SAVEPOINT / ROLLBACK TO (the capture unit diverges -> phantom/stale rows)
//! - UPDATE of a primary-key column (after-image only; the "PK never updated"
//!   contract is not enforced by SQL itself)
//! - non-UTF-8 TEXT (captured through String::from_utf8_lossy)
//!
//! Constraint-violating statements are intentionally not generated yet —
//! they must fail without advancing the LSN or leaking row events, but that
//! path is only covered by hand-written tests so far. See the commented
//! "error statements" block below.

mod common;

use std::env;

use common::differential::{check, display, shrink};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

// ---------------------------------------------------------------------------
// Statement generator
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum ColKind {
    Int,
    Text,
    Real,
    Blob,
}

#[derive(Clone)]
struct Col {
    name: String,
    kind: ColKind,
    not_null: bool,
}

#[derive(Clone)]
struct Table {
    name: String,
    cols: Vec<Col>,
    pk: Vec<usize>,
    without_rowid: bool,
}

fn create_table(rng: &mut StdRng, n: u32) -> Table {
    match rng.random_range(0..3) {
        // rowid table, integer PK
        0 => Table {
            name: format!("t{n}"),
            cols: vec![
                Col { name: "id".into(), kind: ColKind::Int, not_null: true },
                Col { name: "v".into(), kind: ColKind::Text, not_null: false },
                Col { name: "n".into(), kind: ColKind::Real, not_null: false },
                Col { name: "b".into(), kind: ColKind::Blob, not_null: false },
            ],
            pk: vec![0],
            without_rowid: false,
        },
        // WITHOUT ROWID, composite text PK
        1 => Table {
            name: format!("t{n}"),
            cols: vec![
                Col { name: "a".into(), kind: ColKind::Text, not_null: true },
                Col { name: "b".into(), kind: ColKind::Text, not_null: true },
                Col { name: "v".into(), kind: ColKind::Int, not_null: false },
            ],
            pk: vec![0, 1],
            without_rowid: true,
        },
        // rowid table with NOT NULL column
        _ => Table {
            name: format!("t{n}"),
            cols: vec![
                Col { name: "id".into(), kind: ColKind::Int, not_null: true },
                Col { name: "v".into(), kind: ColKind::Text, not_null: true },
                Col { name: "n".into(), kind: ColKind::Int, not_null: false },
            ],
            pk: vec![0],
            without_rowid: false,
        },
    }
}

fn create_table_sql(t: &Table) -> String {
    let mut defs: Vec<String> = Vec::new();
    for (i, c) in t.cols.iter().enumerate() {
        let typ = match c.kind {
            ColKind::Int => "INTEGER",
            ColKind::Text => "TEXT",
            ColKind::Real => "REAL",
            ColKind::Blob => "BLOB",
        };
        let mut d = format!("{} {}", c.name, typ);
        if !t.without_rowid && t.pk == vec![i] {
            d.push_str(" PRIMARY KEY");
        }
        if c.not_null {
            d.push_str(" NOT NULL");
        }
        defs.push(d);
    }
    if t.without_rowid {
        let pk: Vec<String> = t.pk.iter().map(|&i| t.cols[i].name.clone()).collect();
        defs.push(format!("PRIMARY KEY ({})", pk.join(", ")));
    }
    let tail = if t.without_rowid { " WITHOUT ROWID" } else { "" };
    format!("CREATE TABLE {} ({}){}", t.name, defs.join(", "), tail)
}

const WORDS: &[&str] = &[
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa",
    "x", "y", "foo", "bar", "baz", "one", "two", "three",
];

fn literal(rng: &mut StdRng, col: &Col) -> String {
    let null_p = if col.not_null { 0.0 } else { 0.15 };
    if rng.random_bool(null_p) {
        return "NULL".into();
    }
    match col.kind {
        ColKind::Int => {
            if rng.random_bool(0.15) {
                rng.random::<i64>().to_string()
            } else {
                rng.random_range(-1000..=1000).to_string()
            }
        }
        ColKind::Text => {
            let n = rng.random_range(1..=3);
            let words: Vec<String> = (0..n)
                .map(|_| WORDS[rng.random_range(0..WORDS.len())].to_string())
                .collect();
            format!("'{}'", words.join(" "))
        }
        ColKind::Real => format!("{}", (rng.random::<f64>() - 0.5) * 2000.0),
        ColKind::Blob => {
            let n = rng.random_range(0..=16);
            let mut s = String::from("X'");
            for _ in 0..n {
                s.push_str(&format!("{:02x}", rng.random_range(0..=255u8)));
            }
            s.push('\'');
            s
        }
    }
}

fn pk_values(rng: &mut StdRng, t: &Table) -> Vec<String> {
    t.pk.iter().map(|&i| literal(rng, &t.cols[i])).collect()
}

fn pk_where(t: &Table, vals: &[String]) -> String {
    t.pk.iter()
        .zip(vals)
        .map(|(&i, v)| format!("{} = {v}", t.cols[i].name))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn insert_sql(rng: &mut StdRng, t: &Table) -> String {
    let names: Vec<String> = t.cols.iter().map(|c| c.name.clone()).collect();
    let rows: Vec<String> = (0..rng.random_range(1..=3))
        .map(|_| {
            let vals: Vec<String> = t.cols.iter().map(|c| literal(rng, c)).collect();
            format!("({})", vals.join(", "))
        })
        .collect();
    format!(
        "INSERT INTO {} ({}) VALUES {}",
        t.name,
        names.join(", "),
        rows.join(", ")
    )
}

fn update_sql(rng: &mut StdRng, t: &Table) -> String {
    // Never update PK columns (contract, see header).
    let non_pk: Vec<usize> = (0..t.cols.len()).filter(|i| !t.pk.contains(i)).collect();
    assert!(!non_pk.is_empty());
    let idx = non_pk[rng.random_range(0..non_pk.len())];
    let col = t.cols[idx].clone();
    let v = literal(rng, &col);
    format!(
        "UPDATE {} SET {} = {} WHERE {}",
        t.name,
        col.name,
        v,
        pk_where(t, &pk_values(rng, t))
    )
}

fn delete_sql(rng: &mut StdRng, t: &Table) -> String {
    format!(
        "DELETE FROM {} WHERE {}",
        t.name,
        pk_where(t, &pk_values(rng, t))
    )
}

fn select_sql(rng: &mut StdRng, t: &Table) -> String {
    let c = &t.cols[rng.random_range(0..t.cols.len())];
    format!("SELECT {} FROM {}", c.name, t.name)
}

fn alter_sql(rng: &mut StdRng, t: &mut Table, n: u32) -> String {
    let name = format!("c{n}");
    let kind = match rng.random_range(0..3) {
        0 => ColKind::Int,
        1 => ColKind::Text,
        _ => ColKind::Real,
    };
    let (typ, default) = match kind {
        ColKind::Int => ("INTEGER", "0"),
        ColKind::Text => ("TEXT", "''"),
        ColKind::Real => ("REAL", "0.0"),
        ColKind::Blob => ("BLOB", "X''"),
    };
    t.cols.push(Col { name: name.clone(), kind, not_null: false });
    format!("ALTER TABLE {} ADD COLUMN {} {} DEFAULT {}", t.name, name, typ, default)
}

fn index_sql(rng: &mut StdRng, t: &Table, n: u32) -> String {
    let c = &t.cols[rng.random_range(0..t.cols.len())];
    format!("CREATE INDEX idx_{n} ON {} ({})", t.name, c.name)
}

/// Generate a random sequence of `n` statements. DDL never lands inside an
/// explicit transaction block (keeps "one source transaction = one record"
/// clean); transaction blocks contain only DML.
///
/// ERROR STATEMENTS — disabled. These deliberately fail (constraint
/// violations) and must not advance the LSN or leak row events; the
/// hand-written tests cover that path today. Enable once the failed-statement
/// path is exercised here:
///   "INSERT INTO {t} (id, v) VALUES ({id}, NULL)"     // NOT NULL violation
///   "INSERT INTO {t} (id, v) VALUES ({dup}, 'x')"     // duplicate PK
///   "UPDATE {t} SET v = NULL"                         // NOT NULL violation
fn generate_sequence(rng: &mut StdRng, n: usize) -> Vec<String> {
    let mut tables: Vec<Table> = Vec::new();
    let mut tbl_n = 0u32;
    let mut col_n = 0u32;
    let mut idx_n = 0u32;
    let mut out: Vec<String> = Vec::new();
    let mut txn_open = false;
    let mut pending: Vec<String> = Vec::new();

    // The first statement always creates a table.
    let t0 = create_table(rng, tbl_n);
    tbl_n += 1;
    out.push(create_table_sql(&t0));
    tables.push(t0);

    while out.len() < n {
        // Maybe open a transaction block.
        if !txn_open && rng.random_bool(0.05) {
            pending.push("BEGIN".into());
            txn_open = true;
            continue;
        }
        // Maybe close it (BEGIN; ...; COMMIT as one pipeline element, so the
        // statements share one connection, like @libsql/client's batch).
        if txn_open && rng.random_bool(0.4) {
            pending.push("COMMIT".into());
            let joined = format!("{};", pending.join("; "));
            out.push(joined);
            pending.clear();
            txn_open = false;
            continue;
        }

        let roll = rng.random_range(0..100);
        let sql: String = if txn_open {
            // DML only inside transaction blocks.
            let i = rng.random_range(0..tables.len());
            let t = &tables[i];
            match roll {
                0..=69 => insert_sql(rng, t),
                70..=89 => update_sql(rng, t),
                _ => delete_sql(rng, t),
            }
        } else if roll < 10 {
            let t = create_table(rng, tbl_n);
            tbl_n += 1;
            let sql = create_table_sql(&t);
            tables.push(t);
            sql
        } else if roll < 14 {
            let i = rng.random_range(0..tables.len());
            let sql = alter_sql(rng, &mut tables[i], col_n);
            col_n += 1;
            sql
        } else if roll < 17 {
            let i = rng.random_range(0..tables.len());
            let sql = index_sql(rng, &tables[i], idx_n);
            idx_n += 1;
            sql
        } else if roll < 62 {
            let i = rng.random_range(0..tables.len());
            insert_sql(rng, &tables[i])
        } else if roll < 78 {
            let i = rng.random_range(0..tables.len());
            update_sql(rng, &tables[i])
        } else if roll < 90 {
            let i = rng.random_range(0..tables.len());
            delete_sql(rng, &tables[i])
        } else {
            let i = rng.random_range(0..tables.len());
            select_sql(rng, &tables[i])
        };

        if txn_open {
            // Wait: ALTER pushes a column into `tables`, so it must not be
            // generated inside txn blocks (guarded above). DML only.
            pending.push(sql);
        } else {
            out.push(sql);
        }
    }

    if txn_open {
        pending.push("COMMIT".into());
        out.push(format!("{};", pending.join("; ")));
    }
    out
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn random_sequences_converge() {
    let seeds: u64 = env::var("DIFF_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let per_seq: usize = env::var("DIFF_STMT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40);

    for seed in 0..seeds {
        let mut rng = StdRng::seed_from_u64(seed);
        let stmts = generate_sequence(&mut rng, per_seq);
        if let Some(diff) = check(&stmts).await {
            let minimal = shrink(&stmts).await;
            panic!(
                "replica diverged from primary at seed {seed}\n\
                 original sequence ({} statements):\n{}\n\
                 shrunk reproducer ({} statements):\n{}\n\
                 diff:\n{diff}",
                stmts.len(),
                display(&stmts),
                minimal.len(),
                display(&minimal),
            );
        }
    }
}
