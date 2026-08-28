//! Server configuration (flags + env).

use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Config {
    /// Listen address, e.g. `0.0.0.0:8080`.
    pub addr: String,
    /// Directory holding all namespaces (each namespace = one subdirectory).
    pub db_path: PathBuf,
    /// Rotate binlog segments at this size.
    pub max_segment_bytes: u64,
    /// Total binlog retention per namespace; oldest segments are deleted
    /// beyond this. Clients older than the retained range must re-clone.
    pub max_binlog_bytes: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let addr = env_or("SQLD_ADDR", "0.0.0.0:8080");
        let db_path = PathBuf::from(env_or("SQLD_DB_PATH", "./data"));
        let max_segment_bytes = env_u64("SQLD_MAX_SEGMENT_BYTES", 16 * 1024 * 1024)?;
        let max_binlog_bytes = env_u64("SQLD_MAX_BINLOG_BYTES", 256 * 1024 * 1024)?;
        Ok(Config {
            addr,
            db_path,
            max_segment_bytes,
            max_binlog_bytes,
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u64(key: &str, default: u64) -> anyhow::Result<u64> {
    match std::env::var(key) {
        Ok(v) => Ok(v
            .parse()
            .map_err(|_| anyhow::anyhow!("{key} must be a number"))?),
        Err(_) => Ok(default),
    }
}
