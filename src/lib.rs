//! replite: vanilla-SQLite primary node with row-level
//! (binlog-style) replication and a libsql-server-compatible HTTP API.

pub mod binlog;
pub mod capture;
pub mod config;
pub mod error;
pub mod executor;
pub mod hrana;
pub mod http;
pub mod namespace;
pub mod pipeline;
pub mod stream;
pub mod sync;

use std::sync::Arc;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

/// Load config, open namespaces, and serve HTTP until the process ends.
pub async fn serve() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = config::Config::from_env()?;
    tracing::info!(
        "replite {} listening on {} (data: {})",
        env!("CARGO_PKG_VERSION"),
        config.addr,
        config.db_path.display(),
    );

    let namespaces = Arc::new(namespace::NamespaceManager::open(
        config.db_path,
        config.max_segment_bytes,
        config.max_binlog_bytes,
    )?);

    let streams = stream::StreamRegistry::new();
    stream::spawn_expiry(streams.clone());

    let app = http::build_router(http::AppState {
        namespaces,
        streams,
    });

    let listener = tokio::net::TcpListener::bind(&config.addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
