//! Hrana-over-HTTP stream state (baton support).
//!
//! A baton is an opaque token that lets a client keep one stream (a database
//! connection with its transaction, stored SQL and prepared statements)
//! alive across HTTP requests. Mirrors libsql-server's protocol:
//!
//! - the request body carries `baton`; the response body returns the next
//!   baton (or none when the stream was closed),
//! - the baton is `base64url-nopad(payload || HMAC-SHA256(payload))` where
//!   payload = `stream_id (8B BE) || baton_seq (8B BE)`, keyed by a random
//!   per-process key (HMAC prevents forging),
//! - the sequence number is incremented per acquire, so a baton can only be
//!   used once and requests are naturally serialized,
//! - streams expire after a short inactivity timeout (their connection is
//!   dropped, which rolls back any open transaction).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::error::AppError;
use crate::executor::DbHandle;

/// How long an idle stream is kept before it expires.
const STREAM_TTL: Duration = Duration::from_secs(10);
/// How often the expiry task sweeps the registry.
const SWEEP_INTERVAL: Duration = Duration::from_secs(2);

pub struct StreamRegistry {
    key: [u8; 32],
    streams: Mutex<HashMap<u64, StreamEntry>>,
}

struct StreamEntry {
    handle: Box<DbHandle>,
    baton_seq: u64,
    last_used: Instant,
}

/// A stream acquired by an in-flight pipeline request. Dropping the guard
/// without [StreamGuard::release] closes the stream (and its connection).
pub struct StreamGuard {
    registry: Arc<StreamRegistry>,
    id: u64,
    entry: Option<StreamEntry>,
}

impl StreamGuard {
    pub fn handle(&self) -> &DbHandle {
        &self.entry.as_ref().unwrap().handle
    }

    /// Release the stream back into the registry and return the next baton,
    /// or close it (returning `None`) when [closed] (the client sent a
    /// `close` request).
    pub fn release(mut self, closed: bool) -> Option<String> {
        let mut entry = self.entry.take()?;
        if closed {
            tracing::debug!("stream {} closed", self.id);
            return None;
        }
        entry.baton_seq = entry.baton_seq.wrapping_add(1);
        entry.last_used = Instant::now();
        let next_seq = entry.baton_seq;
        let mut streams = self.registry.streams.lock().unwrap();
        streams.insert(self.id, entry);
        Some(self.registry.encode_baton(self.id, next_seq))
    }
}

impl StreamRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            key: rand::random(),
            streams: Mutex::new(HashMap::new()),
        })
    }

    /// Acquire the stream for `baton`, or create a fresh one via `make`
    /// when there is no baton.
    pub fn acquire(
        self: &Arc<Self>,
        baton: Option<&str>,
        make: impl FnOnce() -> anyhow::Result<Box<DbHandle>>,
    ) -> Result<StreamGuard, AppError> {
        match baton {
            Some(baton) => {
                let (id, seq) = self.decode_baton(baton)?;
                let mut streams = self.streams.lock().unwrap();
                let entry = streams.remove(&id).ok_or_else(|| {
                    AppError::bad_request("BATON_INVALID", format!("stream {id} not found"))
                })?;
                if entry.baton_seq != seq {
                    return Err(AppError::bad_request(
                        "BATON_REUSED",
                        format!("expected baton seq {}, received {seq}", entry.baton_seq),
                    ));
                }
                tracing::debug!(stream_id = id, baton = true, "stream acquired");
                Ok(StreamGuard {
                    registry: self.clone(),
                    id,
                    entry: Some(entry),
                })
            }
            None => {
                let handle = make().map_err(|e| AppError::internal(e.to_string()))?;
                let streams = self.streams.lock().unwrap();
                let id = loop {
                    let candidate = rand::random::<u64>();
                    if !streams.contains_key(&candidate) {
                        break candidate;
                    }
                };
                tracing::debug!(stream_id = id, baton = false, "stream acquired");
                Ok(StreamGuard {
                    registry: self.clone(),
                    id,
                    entry: Some(StreamEntry {
                        handle,
                        baton_seq: rand::random(),
                        last_used: Instant::now(),
                    }),
                })
            }
        }
    }

    /// Remove expired streams. Runs on a background task.
    pub fn expire(&self) {
        let mut streams = self.streams.lock().unwrap();
        let now = Instant::now();
        let expired: Vec<u64> = streams
            .iter()
            .filter(|(_, e)| now.duration_since(e.last_used) > STREAM_TTL)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            tracing::debug!("stream {id} expired (idle > {STREAM_TTL:?})");
            // dropping the entry drops the connection -> implicit rollback
            streams.remove(&id);
        }
    }

    fn encode_baton(&self, stream_id: u64, baton_seq: u64) -> String {
        let mut payload = [0u8; 16];
        payload[0..8].copy_from_slice(&stream_id.to_be_bytes());
        payload[8..16].copy_from_slice(&baton_seq.to_be_bytes());

        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.key).unwrap();
        mac.update(&payload);
        let mac = mac.finalize().into_bytes();

        let mut data = [0u8; 48];
        data[0..16].copy_from_slice(&payload);
        data[16..48].copy_from_slice(&mac);
        URL_SAFE_NO_PAD.encode(data)
    }

    fn decode_baton(&self, baton: &str) -> Result<(u64, u64), AppError> {
        let data = URL_SAFE_NO_PAD
            .decode(baton)
            .map_err(|_| AppError::bad_request("BATON_INVALID", "cannot base64-decode baton"))?;
        if data.len() != 48 {
            return Err(AppError::bad_request(
                "BATON_INVALID",
                format!("baton has invalid size of {} bytes", data.len()),
            ));
        }
        let payload = &data[0..16];
        let received_mac = &data[16..48];

        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.key).unwrap();
        mac.update(payload);
        mac.verify_slice(received_mac)
            .map_err(|_| AppError::bad_request("BATON_INVALID", "invalid MAC on baton"))?;

        let stream_id = u64::from_be_bytes(payload[0..8].try_into().unwrap());
        let baton_seq = u64::from_be_bytes(payload[8..16].try_into().unwrap());
        Ok((stream_id, baton_seq))
    }
}

/// Spawn the background expiry task.
pub fn spawn_expiry(registry: Arc<StreamRegistry>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SWEEP_INTERVAL).await;
            registry.expire();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::binlog::Binlog;
    use crate::capture::{Capture, install_hooks};

    fn make_handle() -> Box<DbHandle> {
        std::mem::forget::<()>(());
        let dir = tempfile::tempdir().unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("test.sqlite")).unwrap();
        let handle = Box::new(DbHandle {
            conn,
            capture: RefCell::new(Capture::new(0)),
            binlog: Arc::new(Mutex::new(
                Binlog::open(dir.path().join("binlog"), 1024, 10_000).unwrap(),
            )),
            sqls: RefCell::new(HashMap::new()),
        });
        install_hooks(&handle.conn, &handle.capture);
        handle
    }

    #[test]
    fn baton_roundtrip_and_reuse_protection() {
        let registry = StreamRegistry::new();

        let guard = registry.acquire(None, || Ok(make_handle())).unwrap();
        let baton = guard.release(false).unwrap();
        assert!(!baton.is_empty());

        // the returned baton acquires the same stream
        let guard2 = registry.acquire(Some(&baton), || unreachable!()).unwrap();
        // reusing the SAME baton must fail (sequence advanced)
        let err = registry
            .acquire(Some(&baton), || unreachable!())
            .err()
            .unwrap();
        assert!(
            err.message.contains("BATON_REUSED") || err.code == "BATON_INVALID",
            "{err:?}"
        );
        let baton2 = guard2.release(false).unwrap();
        assert_ne!(baton, baton2);

        // closing the stream yields no baton and the stream is gone
        let guard3 = registry.acquire(Some(&baton2), || unreachable!()).unwrap();
        assert!(guard3.release(true).is_none());
        let err = registry
            .acquire(Some(&baton2), || unreachable!())
            .err()
            .unwrap();
        assert_eq!(err.code, "BATON_INVALID");
    }

    #[test]
    fn forged_baton_rejected() {
        let registry = StreamRegistry::new();
        let err = registry
            .acquire(Some("AAAA"), || unreachable!())
            .err()
            .unwrap();
        assert_eq!(err.code, "BATON_INVALID");
    }

    #[test]
    fn expiry_drops_idle_streams() {
        let registry = StreamRegistry::new();
        let guard = registry.acquire(None, || Ok(make_handle())).unwrap();
        let baton = guard.release(false).unwrap();

        // manipulate last_used to simulate idle > TTL
        {
            let mut streams = registry.streams.lock().unwrap();
            for e in streams.values_mut() {
                e.last_used = Instant::now() - STREAM_TTL - Duration::from_secs(1);
            }
        }
        registry.expire();
        let err = registry
            .acquire(Some(&baton), || unreachable!())
            .err()
            .unwrap();
        assert_eq!(err.code, "BATON_INVALID", "expired stream must be gone");
    }
}
