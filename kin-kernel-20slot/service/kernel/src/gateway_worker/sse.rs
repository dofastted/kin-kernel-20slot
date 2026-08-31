use std::future::Future;
use std::time::Duration;

use futures_util::Stream;
use futures_util::StreamExt;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use crate::stream::{event_model, event_stop_reason, event_usage, merge_usage};

#[derive(Debug, Clone)]
pub struct PumpOptions {
    pub max_event_bytes: usize,
    pub max_response_bytes: usize,
    pub capture_body: bool,
    pub first_byte: Duration,
    pub idle: Duration,
    pub shutdown: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct SseEvent {
    pub raw: Vec<u8>,
    pub event_type: String,
    pub usage: Option<Map<String, Value>>,
    pub model: String,
    pub stop_reason: String,
    pub terminal: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PumpResult {
    pub saw_start: bool,
    pub saw_terminal: bool,
    pub event_count: i64,
    pub usage: Map<String, Value>,
    pub model: String,
    pub stop_reason: String,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct PumpOutcome {
    pub result: PumpResult,
    pub error: Option<String>,
}

#[derive(Debug)]
pub enum PumpError {
    FirstByte,
    Idle,
    EventTooLarge,
    ResponseTooLarge(usize),
    Decode(String),
    Shutdown,
    Observe(String),
    Emit(String),
    Read(String),
}

impl std::fmt::Display for PumpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FirstByte => write!(f, "stream first-byte timeout"),
            Self::Idle => write!(f, "stream idle timeout"),
            Self::EventTooLarge => write!(f, "SSE event exceeds limit"),
            Self::ResponseTooLarge(max) => write!(f, "stream response exceeds {max} bytes"),
            Self::Shutdown => write!(f, "worker shutting down"),
            Self::Decode(msg) | Self::Observe(msg) | Self::Emit(msg) | Self::Read(msg) => {
                write!(f, "{msg}")
            }
        }
    }
}

struct Tracker {
    started: bool,
    terminal: bool,
    count: i64,
    capture_body: bool,
    usage: Map<String, Value>,
    model: String,
    stop_reason: String,
    body: Vec<u8>,
}

impl Tracker {
    fn new(capture_body: bool) -> Self {
        Self {
            started: false,
            terminal: false,
            count: 0,
            capture_body,
            usage: Map::new(),
            model: String::new(),
            stop_reason: String::new(),
            body: Vec::new(),
        }
    }

    fn observe(&mut self, event: &SseEvent) -> Result<(), PumpError> {
        self.count += 1;
        if self.capture_body {
            self.body.extend_from_slice(&event.raw);
        }
        if self.terminal {
            return Err(PumpError::Observe(
                "SSE event received after message_stop".into(),
            ));
        }
        match event.event_type.as_str() {
            "message_start" => {
                if self.started {
                    return Err(PumpError::Observe("duplicate message_start".into()));
                }
                self.started = true;
            }
            "error" => {
                return Err(PumpError::Observe("upstream SSE error event".into()));
            }
            "message_stop" => {
                if !self.started {
                    return Err(PumpError::Observe(
                        "message_stop before message_start".into(),
                    ));
                }
                self.terminal = true;
            }
            _ => {}
        }
        if let Some(usage) = &event.usage {
            merge_usage(&mut self.usage, usage);
        }
        if !event.model.is_empty() {
            self.model = event.model.clone();
        }
        if !event.stop_reason.is_empty() {
            self.stop_reason = event.stop_reason.clone();
        }
        Ok(())
    }

    fn result(&self) -> PumpResult {
        PumpResult {
            saw_start: self.started,
            saw_terminal: self.terminal,
            event_count: self.count,
            usage: self.usage.clone(),
            model: self.model.clone(),
            stop_reason: self.stop_reason.clone(),
            body: self.body.clone(),
        }
    }
}

pub async fn pump<S, B, E, F, Fut>(mut body: S, options: PumpOptions, mut emit: F) -> PumpOutcome
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
    E: std::fmt::Display,
    F: FnMut(SseEvent) -> Fut,
    Fut: Future<Output = Result<(), PumpError>>,
{
    let mut buf = Vec::new();
    let mut tracker = Tracker::new(options.capture_body);
    let mut response_bytes = 0usize;
    let mut first = true;
    loop {
        if let Some(outcome) =
            drain_frames(&mut buf, options.max_event_bytes, &mut tracker, &mut emit).await
        {
            return outcome;
        }
        let wait = if first {
            options.first_byte
        } else {
            options.idle
        };
        let next = tokio::select! {
            _ = options.shutdown.cancelled() => {
                return fail(tracker.result(), PumpError::Shutdown);
            }
            next = tokio::time::timeout(wait, body.next()) => next,
        };
        match next {
            Err(_) => {
                let error = if first {
                    PumpError::FirstByte
                } else {
                    PumpError::Idle
                };
                return fail(tracker.result(), error);
            }
            Ok(None) => {
                if !buf.is_empty() {
                    match parse_event(&buf) {
                        Ok(event) => {
                            if let Some(outcome) =
                                observe_emit(&mut tracker, event, &mut emit).await
                            {
                                return outcome;
                            }
                        }
                        Err(err) => return fail(tracker.result(), err),
                    }
                    buf.clear();
                }
                return finish(tracker.result());
            }
            Ok(Some(Err(err))) => return fail(tracker.result(), PumpError::Read(err.to_string())),
            Ok(Some(Ok(chunk))) => {
                first = false;
                let chunk = chunk.as_ref();
                let next_response_bytes = response_bytes.saturating_add(chunk.len());
                if next_response_bytes > options.max_response_bytes {
                    return fail(
                        tracker.result(),
                        PumpError::ResponseTooLarge(options.max_response_bytes),
                    );
                }
                response_bytes = next_response_bytes;
                buf.extend_from_slice(chunk);
                if buf.len() > options.max_event_bytes && find_delim(&buf).is_none() {
                    return fail(tracker.result(), PumpError::EventTooLarge);
                }
            }
        }
    }
}

async fn drain_frames<F, Fut>(
    buf: &mut Vec<u8>,
    max_event_bytes: usize,
    tracker: &mut Tracker,
    emit: &mut F,
) -> Option<PumpOutcome>
where
    F: FnMut(SseEvent) -> Fut,
    Fut: Future<Output = Result<(), PumpError>>,
{
    loop {
        match split_frame(buf, max_event_bytes) {
            Ok(Some(raw)) => match parse_event(&raw) {
                Ok(event) => {
                    if let Some(outcome) = observe_emit(tracker, event, emit).await {
                        return Some(outcome);
                    }
                }
                Err(err) => return Some(fail(tracker.result(), err)),
            },
            Ok(None) => return None,
            Err(err) => return Some(fail(tracker.result(), err)),
        }
    }
}

async fn observe_emit<F, Fut>(
    tracker: &mut Tracker,
    event: SseEvent,
    emit: &mut F,
) -> Option<PumpOutcome>
where
    F: FnMut(SseEvent) -> Fut,
    Fut: Future<Output = Result<(), PumpError>>,
{
    if let Err(err) = tracker.observe(&event) {
        return Some(fail(tracker.result(), err));
    }
    if let Err(err) = emit(event).await {
        return Some(fail(tracker.result(), err));
    }
    None
}

fn finish(result: PumpResult) -> PumpOutcome {
    if !result.saw_start {
        return fail(
            result,
            PumpError::Observe("stream closed before message_start".into()),
        );
    }
    if !result.saw_terminal {
        return fail(
            result,
            PumpError::Observe("stream closed before message_stop".into()),
        );
    }
    PumpOutcome {
        result,
        error: None,
    }
}

fn fail(result: PumpResult, error: PumpError) -> PumpOutcome {
    PumpOutcome {
        result,
        error: Some(error.to_string()),
    }
}

fn split_frame(buf: &mut Vec<u8>, max_event_bytes: usize) -> Result<Option<Vec<u8>>, PumpError> {
    let Some((index, delim)) = find_delim(buf) else {
        return Ok(None);
    };
    if index > max_event_bytes {
        return Err(PumpError::EventTooLarge);
    }
    let raw = buf.drain(..index).collect::<Vec<_>>();
    buf.drain(..delim);
    Ok(Some(trim_ascii(&raw)))
}

fn find_delim(buf: &[u8]) -> Option<(usize, usize)> {
    if let Some(index) = find_subsequence(buf, b"\r\n\r\n") {
        return Some((index, 4));
    }
    find_subsequence(buf, b"\n\n").map(|index| (index, 2))
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn trim_ascii(raw: &[u8]) -> Vec<u8> {
    let mut start = 0;
    let mut end = raw.len();
    while start < end && raw[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && raw[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    raw[start..end].to_vec()
}

fn parse_event(raw: &[u8]) -> Result<SseEvent, PumpError> {
    let mut event = SseEvent {
        raw: {
            let mut out = raw.to_vec();
            out.extend_from_slice(b"\n\n");
            out
        },
        event_type: String::new(),
        usage: None,
        model: String::new(),
        stop_reason: String::new(),
        terminal: false,
    };
    let text = String::from_utf8_lossy(raw);
    let mut data_lines = Vec::new();
    for line in text.split('\n') {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix("event:") {
            event.event_type = rest.trim().to_string();
        }
        if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.trim());
        }
    }
    if data_lines.is_empty() {
        return Ok(event);
    }
    let data = data_lines.join("\n");
    let payload: Value = serde_json::from_str(&data)
        .map_err(|err| PumpError::Decode(format!("decode SSE data: {err}")))?;
    if event.event_type.is_empty() {
        event.event_type = payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
    }
    event.terminal = event.event_type == "message_stop";
    event.usage = event_usage(&payload);
    event.model = event_model(&payload).unwrap_or_default();
    event.stop_reason = event_stop_reason(&payload).unwrap_or_default();
    Ok(event)
}

/// Canned upstream SSE used only by the gateway_worker tests.
#[cfg(test)]
pub fn usage_sse_fixture() -> bytes::Bytes {
    bytes::Bytes::from_static(
        b"data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-haiku-4-5-20251001\",\"usage\":{\"input_tokens\":12,\"cache_read_input_tokens\":3,\"cache_creation_input_tokens\":7,\"cache_creation\":{\"ephemeral_5m_input_tokens\":5,\"ephemeral_1h_input_tokens\":2}}}}\n\n\
data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n\
data: {\"type\":\"message_stop\"}\n\n",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn pump_merges_cache_usage_fixture() {
        let stream = futures_util::stream::iter(vec![Ok::<Bytes, String>(usage_sse_fixture())]);
        let outcome = pump(
            stream,
            PumpOptions {
                max_event_bytes: 32 << 20,
                max_response_bytes: 64 << 20,
                capture_body: true,
                first_byte: Duration::from_secs(1),
                idle: Duration::from_secs(1),
                shutdown: CancellationToken::new(),
            },
            |_event| async { Ok(()) },
        )
        .await;
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(outcome.result.model, "claude-haiku-4-5-20251001");
        assert_eq!(outcome.result.stop_reason, "end_turn");
        assert_eq!(outcome.result.usage["input_tokens"], 12);
        assert_eq!(outcome.result.usage["output_tokens"], 4);
        assert_eq!(outcome.result.usage["cache_read_input_tokens"], 3);
        assert_eq!(
            outcome.result.usage["cache_creation"]["ephemeral_5m_input_tokens"],
            5
        );
        assert_eq!(
            outcome.result.usage["cache_creation"]["ephemeral_1h_input_tokens"],
            2
        );
        assert_eq!(outcome.result.event_count, 3);
    }

    #[tokio::test]
    async fn pump_rejects_missing_stop() {
        let body = Bytes::from_static(
            b"data: {\"type\":\"message_start\",\"message\":{}}\n\n\
data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"x\"}}\n\n",
        );
        let stream = futures_util::stream::iter(vec![Ok::<Bytes, String>(body)]);
        let outcome = pump(
            stream,
            PumpOptions {
                max_event_bytes: 1024,
                max_response_bytes: 64 << 20,
                capture_body: true,
                first_byte: Duration::from_secs(1),
                idle: Duration::from_secs(1),
                shutdown: CancellationToken::new(),
            },
            |_event| async { Ok(()) },
        )
        .await;
        assert!(
            outcome
                .error
                .as_deref()
                .unwrap_or("")
                .contains("message_stop"),
            "{:?}",
            outcome.error
        );
    }

    #[tokio::test]
    async fn pump_redacts_upstream_error_event() {
        let body = Bytes::from_static(
            b"data: {\"type\":\"message_start\",\"message\":{}}\n\n\
data: {\"type\":\"error\",\"error\":{\"message\":\"secret-token\"}}\n\n",
        );
        let stream = futures_util::stream::iter(vec![Ok::<Bytes, String>(body)]);
        let outcome = pump(
            stream,
            PumpOptions {
                max_event_bytes: 1024,
                max_response_bytes: 64 << 20,
                capture_body: true,
                first_byte: Duration::from_secs(1),
                idle: Duration::from_secs(1),
                shutdown: CancellationToken::new(),
            },
            |_event| async { Ok(()) },
        )
        .await;
        assert_eq!(outcome.error.as_deref(), Some("upstream SSE error event"));
    }

    #[tokio::test]
    async fn pump_counts_trimmed_bytes_toward_response_limit() {
        let body = Bytes::from(format!(
            "{}data: {{\"type\":\"message_start\",\"message\":{{}}}}\n\ndata: {{\"type\":\"message_stop\"}}\n\n",
            " ".repeat(200)
        ));
        let stream = futures_util::stream::iter(vec![Ok::<Bytes, String>(body)]);
        let outcome = pump(
            stream,
            PumpOptions {
                max_event_bytes: 1024,
                max_response_bytes: 100,
                capture_body: true,
                first_byte: Duration::from_secs(1),
                idle: Duration::from_secs(1),
                shutdown: CancellationToken::new(),
            },
            |_event| async { Ok(()) },
        )
        .await;
        assert_eq!(
            outcome.error.as_deref(),
            Some("stream response exceeds 100 bytes")
        );
    }
}
