//! Loopback Messages Relay skeleton (stage B).
//!
//! Not yet wired into `start_claude`; the proxy data plane, correlate, tap,
//! and arbiter land in later stages. Remove the dead_code allowance once the
//! boot path consumes this module.
#![allow(dead_code)]

pub mod metrics;

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use tokio::net::TcpListener;

use crate::error::KernelError;

use super::MultiplexConfig;

#[derive(Clone, Debug)]
pub struct RelayHandle {
    pub addr: SocketAddr,
    healthz: Arc<AtomicBool>,
}

impl RelayHandle {
    pub fn healthy(&self) -> bool {
        self.healthz.load(Ordering::Relaxed)
    }
}

pub async fn spawn(cfg: &MultiplexConfig) -> Result<RelayHandle, KernelError> {
    if !cfg.relay_addr.ip().is_loopback() {
        return Err(KernelError::Provider(format!(
            "relay bind address must be loopback: {}",
            cfg.relay_addr
        )));
    }
    let listener = TcpListener::bind(cfg.relay_addr)
        .await
        .map_err(|err| KernelError::Provider(format!("relay bind: {err}")))?;
    let addr = listener
        .local_addr()
        .map_err(|err| KernelError::Provider(err.to_string()))?;
    let healthz = Arc::new(AtomicBool::new(true));
    let app = Router::new()
        .route("/healthz", get(healthz_handler))
        .with_state(Arc::clone(&healthz));
    let serve_healthz = Arc::clone(&healthz);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
        serve_healthz.store(false, Ordering::Relaxed);
    });
    Ok(RelayHandle { addr, healthz })
}

async fn healthz_handler(State(healthz): State<Arc<AtomicBool>>) -> impl IntoResponse {
    if healthz.load(Ordering::Relaxed) {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "unhealthy")
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use crate::config::RelayMode;

    use super::*;

    fn test_cfg() -> MultiplexConfig {
        MultiplexConfig {
            slot_count: 1,
            simulate: true,
            bin: PathBuf::from("simulated"),
            mock_bin: true,
            model: "claude-sonnet-5".into(),
            retire_after_turn: false,
            max_jobs_per_slot: 32,
            slot_max_lifetime: Duration::from_secs(1800),
            session_idle_ttl: Duration::from_secs(600),
            simulate_latency: Duration::from_millis(1),
            continuation_ttl_secs: 600,
            client_stall_timeout: Duration::from_secs(30),
            relay_mode: RelayMode::Observe,
            relay_addr: "127.0.0.1:0".parse().unwrap(),
            relay_upstream: "https://api.anthropic.com".into(),
        }
    }

    #[tokio::test]
    async fn healthz_is_reachable() {
        let handle = spawn(&test_cfg()).await.unwrap();
        assert!(handle.healthy());
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.ends_with("ok"), "{response}");
    }
}
