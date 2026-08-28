//! Loopback Messages Relay data plane plus boot/health helpers.
//!
//! Production wiring lives in `start_claude` and the per-job tap forwarder.
//! Keep this module-level allowance while test-only helpers (`TapQueue::poisoned`,
//! `SseDecoder::poisoned`, scanner `last_valid`) are unused on the bin path.
#![allow(dead_code)]

pub mod arbiter;
pub mod correlate;
pub mod metrics;
pub mod server;
pub mod sse_tap;
pub mod upstream;

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::error::KernelError;

use super::{MultiplexConfig, Runtime};
use metrics::RelayMetrics;
use sse_tap::TapEvent;
use upstream::UpstreamClient;

#[derive(Clone, Debug)]
pub struct RelayHandle {
    pub addr: SocketAddr,
    healthz: Arc<AtomicBool>,
    pub metrics: Arc<RelayMetrics>,
}

impl RelayHandle {
    pub fn healthy(&self) -> bool {
        self.healthz.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct RelayState {
    pub runtime: Arc<Runtime>,
    pub upstream: UpstreamClient,
    pub metrics: Arc<RelayMetrics>,
    pub healthz: Arc<AtomicBool>,
    pub tap_events: Option<mpsc::Sender<TapEvent>>,
}

pub async fn spawn(
    runtime: Arc<Runtime>,
    cfg: &MultiplexConfig,
) -> Result<RelayHandle, KernelError> {
    spawn_with_tap(runtime, cfg, None).await
}

pub async fn spawn_with_tap(
    runtime: Arc<Runtime>,
    cfg: &MultiplexConfig,
    tap_events: Option<mpsc::Sender<TapEvent>>,
) -> Result<RelayHandle, KernelError> {
    if !cfg.relay_addr.ip().is_loopback() {
        return Err(KernelError::Provider(format!(
            "relay bind address must be loopback: {}",
            cfg.relay_addr
        )));
    }
    let upstream = UpstreamClient::new(&cfg.relay_upstream)?;
    let listener = TcpListener::bind(cfg.relay_addr)
        .await
        .map_err(|err| KernelError::Provider(format!("relay bind: {err}")))?;
    let addr = listener
        .local_addr()
        .map_err(|err| KernelError::Provider(err.to_string()))?;
    let healthz = Arc::new(AtomicBool::new(true));
    let metrics = Arc::new(RelayMetrics::default());
    let state = RelayState {
        runtime,
        upstream,
        metrics: Arc::clone(&metrics),
        healthz: Arc::clone(&healthz),
        tap_events,
    };
    let app = Router::new()
        .route("/healthz", get(healthz_handler))
        .fallback(server::proxy)
        .with_state(state);
    let serve_healthz = Arc::clone(&healthz);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
        serve_healthz.store(false, Ordering::Relaxed);
    });
    Ok(RelayHandle {
        addr,
        healthz,
        metrics,
    })
}
pub async fn confirm_healthy(addr: SocketAddr) -> Result<(), KernelError> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|err| KernelError::Provider(format!("relay healthz client: {err}")))?;
    let url = format!("http://{addr}/healthz");
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|err| KernelError::Provider(format!("relay healthz: {err}")))?;
    if !response.status().is_success() {
        return Err(KernelError::Provider(format!(
            "relay healthz status {}",
            response.status()
        )));
    }
    Ok(())
}

async fn healthz_handler(State(state): State<RelayState>) -> impl IntoResponse {
    if state.healthz.load(Ordering::Relaxed) {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "unhealthy")
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf, time::Duration};

    use axum::{
        body::{Body, Bytes, to_bytes},
        http::{HeaderMap, Response, StatusCode, Uri, header::CONTENT_TYPE},
        routing::any,
    };
    use futures_util::stream;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::mpsc,
        time::timeout,
    };

    use crate::config::RelayMode;

    use super::*;
    use crate::{
        model::MessageRequest,
        provider::multiplex_cli::{
            Runtime,
            pending_call::Job,
            relay::correlate::RelayContextToken,
            slot::{Slot, SlotPhase},
        },
    };

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
        let cfg = test_cfg();
        let runtime = Runtime::new(cfg.clone());
        let handle = spawn(runtime, &cfg).await.unwrap();
        assert!(handle.healthy());
        confirm_healthy(handle.addr).await.unwrap();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn proxy_streams_bytes_to_cli_and_taps_filtered_events() {
        let fixture = sse_fixture();
        let upstream_addr = spawn_mock_upstream(fixture.clone()).await;
        let mut cfg = test_cfg();
        cfg.relay_upstream = format!("http://{upstream_addr}");
        let runtime = Runtime::new(cfg.clone());
        insert_running_job(&runtime, "job-relay", "slot-relay").await;
        let token = RelayContextToken {
            job_id: "job-relay".into(),
            slot_id: "slot-relay".into(),
            generation: runtime
                .process_generation
                .load(std::sync::atomic::Ordering::Relaxed),
            nonce: "nonce".into(),
        }
        .encode(runtime.secret())
        .unwrap();
        let (tap_tx, mut tap_rx) = mpsc::channel(16);
        let relay = spawn_with_tap(runtime, &cfg, Some(tap_tx)).await.unwrap();
        let split = token.find('.').unwrap_or(token.len() / 2);
        let chunks = vec![
            Ok::<Bytes, std::io::Error>(Bytes::from(format!(r#"{{"a":"{}"#, &token[..split]))),
            Ok::<Bytes, std::io::Error>(Bytes::from(format!(r#"{}","b":true}}"#, &token[split..]))),
        ];
        let response = reqwest::Client::new()
            .post(format!("http://{}/v1/messages?beta=1", relay.addr))
            .header("authorization", "Bearer test-token")
            .header("anthropic-version", "2023-06-01")
            .body(reqwest::Body::wrap_stream(stream::iter(chunks)))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.bytes().await.unwrap();
        assert_eq!(digest(&body), digest(&fixture));
        assert_eq!(&body[..], &fixture[..]);

        let mut tapped = Vec::new();
        while tapped.len() < 7 {
            let event = timeout(Duration::from_secs(1), tap_rx.recv())
                .await
                .expect("tap event")
                .expect("tap event");
            tapped.push(event.event);
        }
        let types: Vec<_> = tapped
            .iter()
            .map(|event| {
                event
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap()
            })
            .collect();
        assert_eq!(
            types,
            vec![
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "content_block_start",
                "content_block_stop",
                "content_block_start",
                "content_block_stop"
            ]
        );
        assert_eq!(tapped[0]["index"], 0);
        assert_eq!(tapped[3]["index"], 1);
        assert_eq!(tapped[5]["index"], 2);
        assert!(
            tapped
                .iter()
                .all(|event| event.to_string().find("mcp__kin_runtime__").is_none())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upstream_5xx_passes_through_to_cli_and_never_taps() {
        // Mock upstream that always fails with 529 (Anthropic overloaded).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/v1/messages",
            any(|| async {
                Response::builder()
                    .status(StatusCode::from_u16(529).unwrap())
                    .body(Body::from(
                        "{\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"}}",
                    ))
                    .unwrap()
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let mut cfg = test_cfg();
        cfg.relay_upstream = format!("http://{upstream_addr}");
        let runtime = Runtime::new(cfg.clone());
        insert_running_job(&runtime, "job-5xx", "slot-5xx").await;
        let token = RelayContextToken {
            job_id: "job-5xx".into(),
            slot_id: "slot-5xx".into(),
            generation: runtime
                .process_generation
                .load(std::sync::atomic::Ordering::Relaxed),
            nonce: "nonce".into(),
        }
        .encode(runtime.secret())
        .unwrap();
        let (tap_tx, mut tap_rx) = mpsc::channel(16);
        let relay = spawn_with_tap(runtime, &cfg, Some(tap_tx)).await.unwrap();
        let response = reqwest::Client::new()
            .post(format!("http://{}/v1/messages", relay.addr))
            .header("authorization", "Bearer test-token")
            .body(format!(r#"{{"ctx":"{token}"}}"#))
            .send()
            .await
            .unwrap();
        // CLI branch sees the upstream failure untouched so the CLI can retry.
        assert_eq!(response.status().as_u16(), 529);
        let body = response.text().await.unwrap();
        assert!(body.contains("overloaded_error"), "{body}");
        // The failed turn must never reach the user tap.
        assert!(
            timeout(Duration::from_millis(200), tap_rx.recv())
                .await
                .is_err(),
            "tap must stay silent on non-2xx upstream responses"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upstream_midstream_io_error_poisons_current_tap() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/v1/messages",
            any(|| async {
                // Delay the error so hyper flushes the 200 + first chunk
                // first; an immediately-ready Err would short-circuit into a
                // 502 before any body byte reaches the relay.
                let first = Bytes::from(sse(json!({
                    "type":"content_block_start",
                    "index":0,
                    "content_block":{"type":"text","text":""}
                })));
                let chunks = stream::unfold(0u8, move |step| {
                    let first = first.clone();
                    async move {
                        match step {
                            0 => Some((Ok::<Bytes, std::io::Error>(first), 1)),
                            1 => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                Some((Err(std::io::Error::other("broken upstream")), 2))
                            }
                            _ => None,
                        }
                    }
                });
                Response::builder()
                    .status(StatusCode::OK)
                    .header(CONTENT_TYPE, "text/event-stream")
                    .body(Body::from_stream(chunks))
                    .unwrap()
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let mut cfg = test_cfg();
        cfg.relay_upstream = format!("http://{upstream_addr}");
        let runtime = Runtime::new(cfg.clone());
        insert_running_job(&runtime, "job-io", "slot-io").await;
        let token = RelayContextToken {
            job_id: "job-io".into(),
            slot_id: "slot-io".into(),
            generation: runtime
                .process_generation
                .load(std::sync::atomic::Ordering::Relaxed),
            nonce: "nonce".into(),
        }
        .encode(runtime.secret())
        .unwrap();
        let (tap_tx, mut tap_rx) = mpsc::channel(16);
        let relay = spawn_with_tap(runtime, &cfg, Some(tap_tx)).await.unwrap();
        let response = reqwest::Client::new()
            .post(format!("http://{}/v1/messages", relay.addr))
            .body(format!(r#"{{"ctx":"{token}"}}"#))
            .send()
            .await
            .unwrap();
        assert!(response.bytes().await.is_err());

        let mut saw_poison = false;
        while let Ok(Some(event)) = timeout(Duration::from_secs(1), tap_rx.recv()).await {
            if event.is_poisoned() {
                saw_poison = true;
                break;
            }
        }
        assert!(saw_poison, "tap must be poisoned on partial upstream body");
    }

    async fn spawn_mock_upstream(fixture: Vec<u8>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/v1/messages",
            any(move |uri: Uri, headers: HeaderMap, body: Body| {
                let fixture = fixture.clone();
                async move { mock_upstream(uri, headers, body, fixture).await }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    async fn mock_upstream(
        uri: Uri,
        headers: HeaderMap,
        body: Body,
        fixture: Vec<u8>,
    ) -> Response<Body> {
        let body = to_bytes(body, usize::MAX).await.unwrap();
        if uri.path_and_query().map(|pq| pq.as_str()) != Some("/v1/messages?beta=1")
            || headers.get("authorization").is_none()
            || !String::from_utf8_lossy(&body).contains("krc_")
        {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("bad relay request"))
                .unwrap();
        }
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/event-stream")
            .body(Body::from(fixture))
            .unwrap()
    }

    async fn insert_running_job(runtime: &Arc<Runtime>, job_id: &str, slot_id: &str) {
        runtime.jobs.lock().await.insert(
            job_id.to_string(),
            Job {
                job_id: job_id.to_string(),
                tenant_id: "tenant".into(),
                session_id: "session".into(),
                slot_id: slot_id.to_string(),
                generation: runtime
                    .process_generation
                    .load(std::sync::atomic::Ordering::Relaxed),
                request: MessageRequest::default(),
            },
        );
        let mut slot = Slot::new(slot_id);
        slot.phase = SlotPhase::Running;
        slot.job_id = Some(job_id.to_string());
        runtime.slots.lock().await.push(slot);
    }

    fn sse_fixture() -> Vec<u8> {
        [
            sse(json!({"type":"message_start","message":{"id":"inner"}})),
            sse(json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})),
            sse(json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}})),
            sse(json!({"type":"content_block_stop","index":0})),
            sse(json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_internal","name":"mcp__kin_runtime__slot_wait","input":{}}})),
            sse(json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{}"}})),
            sse(json!({"type":"content_block_stop","index":1})),
            sse(json!({"type":"content_block_start","index":2,"content_block":{"type":"server_tool_use","id":"srv","name":"web_search","input":{"query":"q"}}})),
            sse(json!({"type":"content_block_stop","index":2})),
            sse(json!({"type":"content_block_start","index":3,"content_block":{"type":"web_search_tool_result","tool_use_id":"srv","content":[{"title":"result"}]}})),
            sse(json!({"type":"content_block_stop","index":3})),
            sse(json!({"type":"message_delta","usage":{"input_tokens":3,"output_tokens":5}})),
            sse(json!({"type":"message_stop"})),
        ]
        .concat()
    }

    fn sse(value: serde_json::Value) -> Vec<u8> {
        format!("event: message\ndata: {value}\n\n").into_bytes()
    }

    fn digest(bytes: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hasher.finalize().to_vec()
    }
}
