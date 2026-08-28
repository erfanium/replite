//! Namespace management: one namespace = one SQLite database + its binlog.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use tokio::sync::Mutex;

use crate::binlog::Binlog;
use crate::capture::{Capture, install_hooks};
use crate::executor::DbHandle;

pub const DB_FILE: &str = "db.sqlite";

/// Validated namespace name. libsql-server-compatible charset: alphanumeric
/// plus `-`, `_`, `.`, `:` and `/` (nested namespaces map to nested
/// directories). `..` and leading dots are rejected.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NamespaceName(String);

impl NamespaceName {
    pub fn from_string(name: String) -> Result<Self> {
        if name.is_empty() {
            bail!("namespace name must not be empty");
        }
        if name.len() > 64 {
            bail!("namespace name too long");
        }
        for segment in name.split('/') {
            if segment.is_empty() || segment == "." || segment == ".." {
                bail!("invalid namespace name: {name:?}");
            }
            for c in segment.chars() {
                if !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':')) {
                    bail!("invalid character {c:?} in namespace name: {name:?}");
                }
            }
        }
        Ok(NamespaceName(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NamespaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for NamespaceName {
    fn default() -> Self {
        NamespaceName("default".into())
    }
}

pub struct Namespace {
    pub name: NamespaceName,
    pub dir: PathBuf,
    /// Boxed so the capture cell's address is stable for the SQLite hooks.
    pub handle: Mutex<Box<DbHandle>>,
    /// The binlog, shared by the main handle and every baton'd stream handle.
    pub binlog: Arc<std::sync::Mutex<Binlog>>,
}

impl Namespace {
    pub fn db_path(&self) -> PathBuf {
        self.dir.join(DB_FILE)
    }

    /// Open a new connection to this namespace for a baton'd Hrana stream.
    /// The stream gets its own connection (so its transaction survives across
    /// HTTP requests) and capture, but shares the namespace's binlog.
    pub fn open_stream_handle(&self) -> Result<Box<DbHandle>> {
        let conn = Connection::open(self.db_path())
            .with_context(|| format!("cannot open {}", self.db_path().display()))?;
        conn.busy_timeout(std::time::Duration::from_secs(10))?;
        conn.execute_batch("PRAGMA foreign_keys = ON")?;
        Ok(make_handle(conn, self.binlog.clone()))
    }
}

/// Box the DbHandle BEFORE installing hooks: the preupdate/commit/rollback
/// callbacks capture a raw pointer to the capture RefCell, so the handle must
/// never move afterwards.
fn make_handle(conn: Connection, binlog: Arc<std::sync::Mutex<Binlog>>) -> Box<DbHandle> {
    let schema_version = conn
        .query_row("PRAGMA schema_version", [], |r| r.get(0))
        .unwrap_or(0);
    let handle = Box::new(DbHandle {
        conn,
        capture: RefCell::new(Capture::new(schema_version)),
        binlog,
        sqls: RefCell::new(HashMap::new()),
    });
    install_hooks(&handle.conn, &handle.capture);
    handle
}

/// Owns all namespaces. The map uses a plain `std::sync::RwLock`: guards are
/// never held across an await (opening a namespace is synchronous), so no
/// async-lock deadlock is possible.
pub struct NamespaceManager {
    root: PathBuf,
    max_segment_bytes: u64,
    max_binlog_bytes: u64,
    namespaces: RwLock<HashMap<NamespaceName, Arc<Namespace>>>,
}

impl NamespaceManager {
    pub fn open(root: PathBuf, max_segment_bytes: u64, max_binlog_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        let manager = NamespaceManager {
            root,
            max_segment_bytes,
            max_binlog_bytes,
            namespaces: RwLock::new(HashMap::new()),
        };
        manager.load_existing()?;
        Ok(manager)
    }

    /// Open every existing namespace directory.
    fn load_existing(&self) -> Result<()> {
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let ns_name = match NamespaceName::from_string(name) {
                Ok(n) => n,
                Err(_) => continue,
            };
            if !entry.path().join(DB_FILE).exists() {
                continue;
            }
            match self.open_namespace(ns_name.clone()) {
                Ok(ns) => {
                    let _ = self
                        .namespaces
                        .write()
                        .map(|mut m| m.insert(ns.name.clone(), ns));
                }
                Err(e) => {
                    tracing::warn!("failed to open namespace {ns_name}: {e}");
                }
            }
        }
        Ok(())
    }

    pub async fn get(&self, name: &NamespaceName) -> Option<Arc<Namespace>> {
        {
            let map = self.namespaces.read().ok()?;
            if let Some(ns) = map.get(name) {
                return Some(ns.clone());
            }
        }
        let dir = self.root.join(name.as_str());
        if !dir.join(DB_FILE).exists() {
            return None;
        }
        match self.open_namespace(name.clone()) {
            Ok(ns) => {
                if let Ok(mut map) = self.namespaces.write() {
                    map.insert(name.clone(), ns.clone());
                }
                Some(ns)
            }
            Err(e) => {
                tracing::error!("failed to open namespace {name}: {e}");
                None
            }
        }
    }

    /// Create the namespace if it doesn't exist. Idempotent.
    pub async fn create(&self, name: &NamespaceName) -> Result<Arc<Namespace>> {
        if let Some(ns) = self.get(name).await {
            return Ok(ns);
        }
        let dir = self.root.join(name.as_str());
        std::fs::create_dir_all(&dir)?;
        let ns = self.open_namespace(name.clone())?;
        if let Ok(mut map) = self.namespaces.write() {
            map.insert(name.clone(), ns.clone());
        }
        Ok(ns)
    }

    /// Delete the namespace. Returns false if it didn't exist.
    pub async fn delete(&self, name: &NamespaceName) -> Result<bool> {
        let removed = self
            .namespaces
            .write()
            .map(|mut m| m.remove(name).is_some())
            .unwrap_or(false);
        if !removed {
            return Ok(false);
        }
        let dir = self.root.join(name.as_str());
        std::fs::remove_dir_all(&dir)?;
        Ok(true)
    }

    fn open_namespace(&self, name: NamespaceName) -> Result<Arc<Namespace>> {
        let dir = self.root.join(name.as_str());
        std::fs::create_dir_all(&dir)?;

        let conn = Connection::open(dir.join(DB_FILE))
            .with_context(|| format!("cannot open database for namespace {name}"))?;
        conn.busy_timeout(std::time::Duration::from_secs(10))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

        let binlog = Arc::new(std::sync::Mutex::new(Binlog::open(
            dir.join("binlog"),
            self.max_segment_bytes,
            self.max_binlog_bytes,
        )?));

        let handle = make_handle(conn, binlog.clone());

        Ok(Arc::new(Namespace {
            name,
            dir,
            handle: Mutex::new(handle),
            binlog,
        }))
    }
}
