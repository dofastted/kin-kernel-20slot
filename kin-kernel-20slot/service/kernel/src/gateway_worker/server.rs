use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes as RawBytes;
use http_body::Frame;
use http_body_util::StreamBody;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::config::WorkerConfig;
use super::credential::{Credential, now_ms};
use super::error::{WorkerError, truncate};
use super::hop::{HopClient, HopResponse, copyable_response_header};
use super::sse::{PumpOptions, PumpResult, pump};

const TRAILER_NAMES: &str =
    "X-Kin-Terminal-State, X-Kin-Event-Count, X-Kin-Usage, X-Kin-Model, X-Kin-Stop-Reason";

#[derive(Clone)]
pub struct WorkerState {
    pub config: Arc<WorkerConfig>,
    pub hop: Arc<HopClient>,
    pub started: Instant,
    pub shutdown: CancellationToken,
}

#[derive(Debug, Deserialize)]
struct Envelope {
    body: Value,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    delivery_mode: String,
}

pub async fn run(config_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let config = WorkerConfig::load(config_path)?;
    let hop = HopClient::new(&config)?;
    let socket = config.socket_path();
    let shutdown = CancellationToken::new();
    let state = WorkerState {
        config: Arc::new(config),
        hop: Arc::new(hop),
        started: Instant::now(),
        shutdown: shutdown.clone(),
    };
    info!(
        vm_id = %state.config.vm_id,
        socket = %socket.display(),
        "kin-kernel gateway-worker listening"
    );
    prepare_socket(&socket)?;
    let listener = tokio::net::UnixListener::bind(&socket)?;
    chmod_socket(&socket)?;
    let app = router(state.clone());
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            shutdown.cancel();
        })
        .await?;
    let _ = std::fs::remove_file(&socket);
    Ok(())
}

pub fn router(state: WorkerState) -> Router {
    let limit = state.config.max_request_usize();
    Router::new()
        .route("/internal/health", get(health))
        .route("/internal/v1/messages", post(messages))
        .layer(middleware::from_fn_with_state(state.clone(), require_token))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(state)
}

async fn require_token(
    State(state): State<WorkerState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let expected = state.config.internal_token.trim();
    if expected.is_empty() {
        return next.run(request).await;
    }
    let provided = request
        .headers()
        .get("X-Kin-Internal-Token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !token_eq(expected, provided) {
        return WorkerError::new(
            StatusCode::UNAUTHORIZED,
            "internal_auth_failed",
            "Internal worker authentication failed",
        )
        .into_response();
    }
    next.run(request).await
}

async fn health(State(state): State<WorkerState>) -> Json<Value> {
    let (ok, credential_state) = match Credential::load(Path::new(&state.config.credential_path)) {
        Ok(cred) => {
            let state_name = cred.state(now_ms(), state.config.refresh_skew_ms());
            let ok = state_name != "missing" && state_name != "expired";
            (ok, state_name)
        }
        Err(_) => (false, "missing"),
    };
    Json(json!({
        "ok": ok,
        "engine": "rust",
        "version": env!("CARGO_PKG_VERSION"),
        "worker_version": env!("CARGO_PKG_VERSION"),
        "vm_id": state.config.vm_id,
        "proxy_configured": !state.config.proxy_url.trim().is_empty(),
        "proxy_required": state.config.proxy_required,
        "credential_state": credential_state,
        "delivery_mode": state.config.delivery_mode,
        "runtime_kind": state.config.runtime_kind,
        "uptime_seconds": state.started.elapsed().as_secs(),
    }))
}

async fn messages(State(state): State<WorkerState>, body: Bytes) -> Response {
    match process_messages(state, body).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn process_messages(state: WorkerState, body: Bytes) -> Result<Response, WorkerError> {
    let envelope: Envelope = serde_json::from_slice(&body).map_err(|err| {
        WorkerError::new(StatusCode::BAD_REQUEST, "invalid_request", err.to_string())
    })?;
    if envelope.body.is_null() {
        return Err(WorkerError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "body is required",
        ));
    }
    let payload = apply_stream_flag(&envelope.body, envelope.stream);
    let credential = load_live_credential(&state.config)?;
    let upstream = state
        .hop
        .messages(&payload, &envelope.headers, &credential)
        .await?;
    if upstream.status.as_u16() >= 400 {
        return forward_upstream_error(upstream, state.config.max_response_bytes).await;
    }
    if !envelope.stream {
        return buffer_json(upstream, state.config.max_response_bytes).await;
    }
    if delivery_mode(&envelope.delivery_mode, &state.config.delivery_mode) == "verified" {
        return verified_stream(upstream, &state.config, state.shutdown.clone()).await;
    }
    Ok(realtime_stream(
        upstream,
        &state.config,
        state.shutdown.clone(),
    ))
}

fn load_live_credential(config: &WorkerConfig) -> Result<Credential, WorkerError> {
    let credential = Credential::load(Path::new(&config.credential_path)).map_err(|_| {
        WorkerError::new(
            StatusCode::UNAUTHORIZED,
            "needs_refresh",
            "credential needs refresh",
        )
    })?;
    if credential.needs_refresh(now_ms(), config.refresh_skew_ms()) {
        return Err(WorkerError::new(
            StatusCode::UNAUTHORIZED,
            "needs_refresh",
            "credential needs refresh",
        ));
    }
    Ok(credential)
}

fn delivery_mode(envelope: &str, fallback: &str) -> String {
    let mode = envelope.trim();
    if mode.is_empty() {
        fallback.to_string()
    } else {
        mode.to_string()
    }
}

fn apply_stream_flag(body: &Value, stream: bool) -> Vec<u8> {
    let mut object = match body {
        Value::Object(map) => map.clone(),
        _ => return serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec()),
    };
    if object.get("stream").and_then(Value::as_bool) != Some(stream) {
        object.insert("stream".into(), Value::Bool(stream));
    }
    serde_json::to_vec(&object).unwrap_or_else(|_| b"{}".to_vec())
}

async fn buffer_json(
    upstream: HopResponse,
    max_response_bytes: i64,
) -> Result<Response, WorkerError> {
    let data = read_limited(upstream.body, max_response_bytes).await?;
    validate_message_json(&data)?;
    let mut headers = upstream.headers;
    headers.insert(
        HeaderName::from_static("x-kin-terminal-state"),
        HeaderValue::from_static("verified"),
    );
    Ok((upstream.status, headers, data).into_response())
}

async fn verified_stream(
    upstream: HopResponse,
    config: &WorkerConfig,
    shutdown: CancellationToken,
) -> Result<Response, WorkerError> {
    let max = usize::try_from(config.max_response_bytes.max(1)).unwrap_or(usize::MAX);
    let outcome = pump(
        upstream.body.bytes_stream(),
        pump_options(config, shutdown),
        |_event| async { Ok(()) },
    )
    .await;
    if let Some(error) = outcome.error {
        return Err(WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_terminal_invalid",
            error,
        ));
    }
    if outcome.result.body.len() > max {
        return Err(WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_terminal_invalid",
            "verified stream exceeds response limit",
        ));
    }
    let mut headers = upstream.headers;
    headers.insert(
        header_name("content-type"),
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(
        HeaderName::from_static("x-kin-terminal-state"),
        HeaderValue::from_static("verified"),
    );
    apply_stream_meta(&mut headers, &outcome.result);
    Ok((upstream.status, headers, outcome.result.body).into_response())
}

fn realtime_stream(
    upstream: HopResponse,
    config: &WorkerConfig,
    shutdown: CancellationToken,
) -> Response {
    let (tx, rx) = mpsc::channel::<Result<Frame<RawBytes>, std::io::Error>>(16);
    let options = pump_options(config, shutdown);
    tokio::spawn(async move {
        pump_realtime(upstream.body, options, tx).await;
    });
    let mut headers = upstream.headers;
    headers.insert(
        header_name("content-type"),
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(
        header_name("trailer"),
        HeaderValue::from_static(TRAILER_NAMES),
    );
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    let body = Body::new(StreamBody::new(stream));
    (upstream.status, headers, body).into_response()
}

async fn pump_realtime(
    body: reqwest::Response,
    options: PumpOptions,
    tx: mpsc::Sender<Result<Frame<RawBytes>, std::io::Error>>,
) {
    let emit_tx = tx.clone();
    let emit_shutdown = options.shutdown.clone();
    let outcome = pump(body.bytes_stream(), options, |event| {
        let tx = emit_tx.clone();
        let shutdown = emit_shutdown.clone();
        async move {
            tokio::select! {
                _ = shutdown.cancelled() => Err(super::sse::PumpError::Shutdown),
                result = tx.send(Ok(Frame::data(RawBytes::from(event.raw)))) => {
                    result.map_err(|_| super::sse::PumpError::Emit("client gone".into()))
                }
            }
        }
    })
    .await;
    if let Some(error) = &outcome.error
        && error != "client gone"
    {
        let _ = tx
            .send(Ok(Frame::data(RawBytes::from(incomplete_event(error)))))
            .await;
    }
    let mut trailers = HeaderMap::new();
    let state = if outcome.error.is_some() {
        "incomplete"
    } else {
        "verified"
    };
    let _ = trailers.insert(
        HeaderName::from_static("x-kin-terminal-state"),
        HeaderValue::from_str(state).unwrap_or(HeaderValue::from_static("incomplete")),
    );
    apply_stream_meta(&mut trailers, &outcome.result);
    let _ = tx.send(Ok(Frame::trailers(trailers))).await;
}

fn incomplete_event(message: &str) -> Vec<u8> {
    let payload = json!({
        "type": "error",
        "error": {
            "type": "api_error",
            "code": "upstream_stream_incomplete",
            "message": truncate(message),
        }
    });
    format!("event: error\ndata: {payload}\n\n").into_bytes()
}

fn apply_stream_meta(headers: &mut HeaderMap, result: &PumpResult) {
    let _ = headers.insert(
        HeaderName::from_static("x-kin-event-count"),
        HeaderValue::from_str(&result.event_count.to_string())
            .unwrap_or(HeaderValue::from_static("0")),
    );
    if !result.usage.is_empty()
        && let Ok(raw) = serde_json::to_string(&result.usage)
        && let Ok(value) = HeaderValue::from_str(&raw)
    {
        headers.insert(HeaderName::from_static("x-kin-usage"), value);
    }
    if !result.model.is_empty()
        && let Ok(value) = HeaderValue::from_str(&result.model)
    {
        headers.insert(HeaderName::from_static("x-kin-model"), value);
    }
    if !result.stop_reason.is_empty()
        && let Ok(value) = HeaderValue::from_str(&result.stop_reason)
    {
        headers.insert(HeaderName::from_static("x-kin-stop-reason"), value);
    }
}

fn pump_options(config: &WorkerConfig, shutdown: CancellationToken) -> PumpOptions {
    PumpOptions {
        max_event_bytes: config.max_event_bytes(),
        first_byte: config.first_byte(),
        idle: config.idle(),
        shutdown,
    }
}

async fn forward_upstream_error(
    upstream: HopResponse,
    max_response_bytes: i64,
) -> Result<Response, WorkerError> {
    let limit = max_response_bytes.min(1 << 20);
    let data = read_limited(upstream.body, limit).await?;
    let mut headers = HeaderMap::new();
    for (key, value) in upstream.headers.iter() {
        if copyable_response_header(key.as_str()) {
            headers.append(key.clone(), value.clone());
        }
    }
    if headers.get(header_name("content-type")).is_none() {
        headers.insert(
            header_name("content-type"),
            HeaderValue::from_static("application/json"),
        );
    }
    Ok((upstream.status, headers, data).into_response())
}

async fn read_limited(response: reqwest::Response, max: i64) -> Result<Bytes, WorkerError> {
    let max = if max <= 0 { 64 << 20 } else { max as u64 };
    if let Some(len) = response.content_length()
        && len > max
    {
        return Err(WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_response_invalid",
            format!("response exceeds {max} bytes"),
        ));
    }
    let data = response.bytes().await.map_err(|err| {
        WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_response_invalid",
            err.to_string(),
        )
    })?;
    if data.len() as u64 > max {
        return Err(WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_response_invalid",
            format!("response exceeds {max} bytes"),
        ));
    }
    Ok(data)
}

fn validate_message_json(data: &[u8]) -> Result<(), WorkerError> {
    let payload: Value = serde_json::from_slice(data).map_err(|err| {
        WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_terminal_invalid",
            format!("decode Anthropic response: {err}"),
        )
    })?;
    let kind = payload.get("type").and_then(Value::as_str).unwrap_or("");
    if kind != "message" {
        return Err(WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_terminal_invalid",
            format!("unexpected Anthropic response type \"{kind}\""),
        ));
    }
    let id = payload.get("id").and_then(Value::as_str).unwrap_or("");
    let role = payload.get("role").and_then(Value::as_str).unwrap_or("");
    let content_ok = payload
        .get("content")
        .map(|value| match value {
            Value::Array(items) => !items.is_empty(),
            Value::String(text) => !text.is_empty(),
            _ => false,
        })
        .unwrap_or(false);
    if id.is_empty() || role != "assistant" || !content_ok {
        return Err(WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_terminal_invalid",
            "Anthropic message response is incomplete",
        ));
    }
    Ok(())
}

fn token_eq(expected: &str, provided: &str) -> bool {
    let left = expected.as_bytes();
    let right = provided.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

fn header_name(name: &'static str) -> HeaderName {
    HeaderName::from_static(name)
}

fn prepare_socket(path: &Path) -> Result<(), String> {
    if path.exists() {
        std::fs::remove_file(path).map_err(|err| format!("remove stale socket: {err}"))?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| format!("create socket dir: {err}"))?;
    }
    Ok(())
}

fn chmod_socket(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|err| format!("chmod socket: {err}"))?;
    }
    let _ = path;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway_worker::sse::usage_sse_fixture;
    use axum::http::Request as HttpRequest;
    use http_body_util::BodyExt;
    use std::io::ErrorKind;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, UnixListener, UnixStream};
    use tokio::sync::Notify;
    use tower::ServiceExt;

    const TOKEN: &str = "internal-secret";
    const JSON_MESSAGE: &str = r#"{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":1,"output_tokens":1}}"#;

    struct TestEnv {
        _dir: TempDir,
        state: WorkerState,
        hits: Arc<AtomicUsize>,
        last_headers: Arc<Mutex<HeaderMap>>,
        gate: Arc<Notify>,
    }

    async fn spawn_mock(
        mode: MockMode,
        hits: Arc<AtomicUsize>,
        last_headers: Arc<Mutex<HeaderMap>>,
        gate: Arc<Notify>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let hits = hits.clone();
                let last_headers = last_headers.clone();
                let gate = gate.clone();
                let mode = mode;
                tokio::spawn(async move {
                    serve_one(stream, mode, hits, last_headers, gate).await;
                });
            }
        });
        format!("http://{addr}")
    }

    #[derive(Clone, Copy)]
    enum MockMode {
        Json,
        UsageSse,
        IncompleteSse,
        DelayedSse,
    }

    async fn serve_one(
        stream: tokio::net::TcpStream,
        mode: MockMode,
        hits: Arc<AtomicUsize>,
        last_headers: Arc<Mutex<HeaderMap>>,
        gate: Arc<Notify>,
    ) {
        let mut buf = vec![0u8; 8192];
        let mut used = 0usize;
        loop {
            match stream.try_read(&mut buf[used..]) {
                Ok(0) => break,
                Ok(n) => {
                    used += n;
                    if buf[..used].windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    if used == buf.len() {
                        buf.resize(buf.len() * 2, 0);
                    }
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    stream.readable().await.ok();
                }
                Err(_) => return,
            }
        }
        hits.fetch_add(1, Ordering::SeqCst);
        if let Some(headers) = parse_request_headers(&buf[..used]) {
            *last_headers.lock().unwrap_or_else(|err| err.into_inner()) = headers;
        }
        let mut stream = stream;
        match mode {
            MockMode::Json => {
                write_http(
                    &mut stream,
                    200,
                    "application/json",
                    JSON_MESSAGE.as_bytes(),
                )
                .await;
            }
            MockMode::UsageSse => {
                write_sse(&mut stream, &usage_sse_fixture(), false).await;
            }
            MockMode::IncompleteSse => {
                let body = b"data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-haiku-4-5-20251001\"}}\n\n";
                write_sse(&mut stream, body, false).await;
            }
            MockMode::DelayedSse => {
                write_sse_headers(&mut stream).await;
                gate.notified().await;
                let _ = stream.write_all(&usage_sse_fixture()).await;
                let _ = stream.shutdown().await;
            }
        }
    }

    fn parse_request_headers(raw: &[u8]) -> Option<HeaderMap> {
        let text = std::str::from_utf8(raw).ok()?;
        let header_block = text.split("\r\n\r\n").next()?;
        let mut headers = HeaderMap::new();
        for line in header_block.lines().skip(1) {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            if let (Ok(name), Ok(val)) = (
                HeaderName::try_from(key.trim()),
                HeaderValue::from_str(value.trim()),
            ) {
                headers.insert(name, val);
            }
        }
        Some(headers)
    }

    async fn write_http(
        stream: &mut tokio::net::TcpStream,
        status: u16,
        content_type: &str,
        body: &[u8],
    ) {
        let head = format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes()).await;
        let _ = stream.write_all(body).await;
        let _ = stream.shutdown().await;
    }

    async fn write_sse_headers(stream: &mut tokio::net::TcpStream) {
        let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n";
        let _ = stream.write_all(head.as_bytes()).await;
    }

    async fn write_sse(stream: &mut tokio::net::TcpStream, body: &[u8], wait_headers_only: bool) {
        let _ = wait_headers_only;
        write_sse_headers(stream).await;
        let _ = stream.write_all(body).await;
        let _ = stream.shutdown().await;
    }

    async fn build_env(mode: MockMode, expires_at: i64) -> TestEnv {
        let dir = TempDir::new().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let last_headers = Arc::new(Mutex::new(HeaderMap::new()));
        let gate = Arc::new(Notify::new());
        let base = spawn_mock(mode, hits.clone(), last_headers.clone(), gate.clone()).await;
        let cred_path = dir.path().join("credentials.json");
        std::fs::write(
            &cred_path,
            format!(
                r#"{{"claudeAiOauth":{{"accessToken":"access","refreshToken":"refresh","expiresAt":{expires_at}}}}}"#
            ),
        )
        .unwrap();
        let socket = dir.path().join("worker.sock");
        let config_path = dir.path().join("worker.json");
        std::fs::write(
            &config_path,
            format!(
                r#"{{
                    "vm_id": "vm-test",
                    "socket_path": "{}",
                    "credential_path": "{}",
                    "internal_token": "{TOKEN}",
                    "anthropic_base_url": "{base}",
                    "oauth_token_url": "{base}/v1/oauth/token",
                    "test_endpoints": true,
                    "delivery_mode": "realtime",
                    "first_byte_timeout_seconds": 5,
                    "idle_timeout_seconds": 5
                }}"#,
                socket.display(),
                cred_path.display()
            ),
        )
        .unwrap();
        let config = WorkerConfig::load(&config_path).unwrap();
        let hop = HopClient::new(&config).unwrap();
        TestEnv {
            _dir: dir,
            state: WorkerState {
                config: Arc::new(config),
                hop: Arc::new(hop),
                started: Instant::now(),
                shutdown: CancellationToken::new(),
            },
            hits,
            last_headers,
            gate,
        }
    }

    fn envelope(stream: bool, delivery: &str) -> Value {
        json!({
            "body": { "model": "claude-haiku-4-5-20251001", "messages": [{"role":"user","content":"hi"}], "stream": stream },
            "headers": {
                "anthropic-version": "2023-06-01",
                "user-agent": "test/1",
                "cookie": "session=1",
                "x-api-key": "sk-leak",
                "authorization": "Bearer leaked"
            },
            "stream": stream,
            "delivery_mode": delivery
        })
    }

    async fn call(
        state: WorkerState,
        token: Option<&str>,
        body: Option<Value>,
    ) -> (StatusCode, HeaderMap, Bytes, Option<HeaderMap>) {
        let mut builder = HttpRequest::builder()
            .method("POST")
            .uri("/internal/v1/messages");
        if let Some(token) = token {
            builder = builder.header("X-Kin-Internal-Token", token);
        }
        let request = builder
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&body.unwrap_or_else(|| json!({}))).unwrap(),
            ))
            .unwrap();
        let response = router(state).oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let collected = response.into_body().collect().await.unwrap();
        let trailers = collected.trailers().cloned();
        (status, headers, collected.to_bytes(), trailers)
    }

    #[tokio::test]
    async fn rejects_missing_and_wrong_token() {
        let env = build_env(MockMode::Json, 4_102_444_800_000).await;
        let (status, _, body, _) =
            call(env.state.clone(), None, Some(envelope(false, "realtime"))).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "internal_auth_failed");
        let (status, _, body, _) =
            call(env.state, Some("wrong"), Some(envelope(false, "realtime"))).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["type"], "worker_error");
        assert_eq!(json["error"]["code"], "internal_auth_failed");
    }

    #[tokio::test]
    async fn missing_credential_needs_refresh_without_outbound() {
        let env = build_env(MockMode::Json, 4_102_444_800_000).await;
        let mut config = (*env.state.config).clone();
        config.credential_path = env._dir.path().join("missing.json").display().to_string();
        let state = WorkerState {
            config: Arc::new(config),
            hop: env.state.hop.clone(),
            started: Instant::now(),
            shutdown: env.state.shutdown.clone(),
        };
        let (status, _, body, _) = call(state, Some(TOKEN), Some(envelope(true, "realtime"))).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "needs_refresh");
        assert_eq!(env.hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stale_credential_needs_refresh_without_outbound() {
        let env = build_env(MockMode::Json, 1).await;
        let (status, _, body, _) =
            call(env.state, Some(TOKEN), Some(envelope(true, "realtime"))).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "needs_refresh");
        assert_eq!(env.hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn strips_leaked_headers_and_sets_bearer() {
        let env = build_env(MockMode::Json, 4_102_444_800_000).await;
        let (status, _, _, _) = call(
            env.state.clone(),
            Some(TOKEN),
            Some(envelope(false, "realtime")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let headers = env
            .last_headers
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        assert_eq!(
            headers.get("authorization").and_then(|v| v.to_str().ok()),
            Some("Bearer access")
        );
        assert!(headers.get("cookie").is_none());
        assert!(headers.get("x-api-key").is_none());
    }

    #[tokio::test]
    async fn verified_usage_matches_sub2api_fixture() {
        let env = build_env(MockMode::UsageSse, 4_102_444_800_000).await;
        let (status, headers, _body, _) =
            call(env.state, Some(TOKEN), Some(envelope(true, "verified"))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers
                .get("x-kin-terminal-state")
                .and_then(|v| v.to_str().ok()),
            Some("verified")
        );
        let usage: Value = serde_json::from_str(
            headers
                .get("x-kin-usage")
                .and_then(|v| v.to_str().ok())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(usage["input_tokens"], 12);
        assert_eq!(usage["output_tokens"], 4);
        assert_eq!(usage["cache_read_input_tokens"], 3);
        assert_eq!(usage["cache_creation_input_tokens"], 7);
        assert_eq!(usage["cache_creation"]["ephemeral_5m_input_tokens"], 5);
        assert_eq!(usage["cache_creation"]["ephemeral_1h_input_tokens"], 2);
        assert_eq!(
            headers.get("x-kin-model").and_then(|v| v.to_str().ok()),
            Some("claude-haiku-4-5-20251001")
        );
        assert_eq!(
            headers
                .get("x-kin-stop-reason")
                .and_then(|v| v.to_str().ok()),
            Some("end_turn")
        );
    }

    #[tokio::test]
    async fn verified_missing_stop_is_502() {
        let env = build_env(MockMode::IncompleteSse, 4_102_444_800_000).await;
        let (status, _, body, _) =
            call(env.state, Some(TOKEN), Some(envelope(true, "verified"))).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "upstream_terminal_invalid");
    }

    #[tokio::test]
    async fn realtime_commits_headers_then_incomplete_on_missing_stop() {
        let env = build_env(MockMode::IncompleteSse, 4_102_444_800_000).await;
        let (status, headers, body, trailers) =
            call(env.state, Some(TOKEN), Some(envelope(true, "realtime"))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers.get("trailer").and_then(|v| v.to_str().ok()),
            Some(TRAILER_NAMES)
        );
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("upstream_stream_incomplete"), "{text}");
        let state = trailers
            .as_ref()
            .and_then(|t| t.get("x-kin-terminal-state"))
            .and_then(|v| v.to_str().ok());
        assert_eq!(state, Some("incomplete"));
    }

    #[tokio::test]
    async fn realtime_headers_arrive_before_sse_body() {
        let env = build_env(MockMode::DelayedSse, 4_102_444_800_000).await;
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/internal/v1/messages")
            .header("X-Kin-Internal-Token", TOKEN)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&envelope(true, "realtime")).unwrap(),
            ))
            .unwrap();
        let response = router(env.state.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("trailer")
                .and_then(|v| v.to_str().ok()),
            Some(TRAILER_NAMES)
        );
        env.gate.notify_one();
        let collected = response.into_body().collect().await.unwrap();
        let usage = collected
            .trailers()
            .and_then(|t| t.get("x-kin-usage"))
            .and_then(|v| v.to_str().ok())
            .unwrap();
        let usage: Value = serde_json::from_str(usage).unwrap();
        assert_eq!(usage["output_tokens"], 4);
    }

    #[tokio::test]
    async fn realtime_shutdown_finishes_with_incomplete_trailer() {
        let env = build_env(MockMode::DelayedSse, 4_102_444_800_000).await;
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/internal/v1/messages")
            .header("X-Kin-Internal-Token", TOKEN)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&envelope(true, "realtime")).unwrap(),
            ))
            .unwrap();
        let response = router(env.state.clone()).oneshot(request).await.unwrap();
        env.state.shutdown.cancel();
        let collected = response.into_body().collect().await.unwrap();
        let state = collected
            .trailers()
            .and_then(|trailers| trailers.get("x-kin-terminal-state"))
            .and_then(|value| value.to_str().ok());
        assert_eq!(state, Some("incomplete"));
        assert!(
            String::from_utf8_lossy(&collected.to_bytes()).contains("upstream_stream_incomplete")
        );
    }

    #[tokio::test]
    async fn unix_socket_health_and_json_smoke() {
        let env = build_env(MockMode::Json, 4_102_444_800_000).await;
        let socket = env.state.config.socket_path();
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket).unwrap();
        let app = router(env.state.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let mut stream = wait_connect(&socket).await;
        stream
            .write_all(
                format!(
                    "GET /internal/health HTTP/1.1\r\nHost: localhost\r\nX-Kin-Internal-Token: {TOKEN}\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("\"engine\":\"rust\""), "{text}");
        let mut stream = wait_connect(&socket).await;
        let payload = serde_json::to_vec(&envelope(false, "realtime")).unwrap();
        let req = format!(
            "POST /internal/v1/messages HTTP/1.1\r\nHost: localhost\r\nX-Kin-Internal-Token: {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        stream.write_all(&payload).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        assert!(text.contains("\"id\":\"msg_1\""), "{text}");
        assert!(
            text.contains("X-Kin-Terminal-State: verified")
                || text.contains("x-kin-terminal-state: verified"),
            "{text}"
        );
    }

    async fn wait_connect(path: &Path) -> UnixStream {
        for _ in 0..50 {
            if let Ok(stream) = UnixStream::connect(path).await {
                return stream;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("unix socket not ready");
    }
}
