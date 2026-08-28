use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use axum::body::Bytes;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use super::metrics::RelayMetrics;

const TAP_QUEUE_ITEMS: usize = 256;
const TAP_QUEUE_BYTES: usize = 2 * 1024 * 1024;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub(crate) const TAP_POISONED: &str = "kin_tap_poisoned";
pub(crate) const TAP_DRAINED: &str = "kin_tap_drained";
pub(crate) const TAP_USAGE: &str = "kin_tap_usage";

/// Everything the relay server needs to attach an upstream response's tap to
/// an existing job: the event sink, the shared per-job poison flag, the
/// per-job block index allocator, and the turn the response belongs to.
#[derive(Clone)]
pub struct TapBinding {
    pub events: mpsc::Sender<TapEvent>,
    pub poisoned: Option<Arc<AtomicBool>>,
    pub index_allocator: Arc<AtomicUsize>,
    pub turn_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapEvent {
    pub job_id: String,
    pub turn_id: u64,
    pub event: Value,
}

#[derive(Clone)]
pub struct TapQueue {
    tx: mpsc::Sender<Bytes>,
    budget: Arc<TapBudget>,
    poisoned: Arc<AtomicBool>,
    metrics: Arc<RelayMetrics>,
    job_id: String,
    out: mpsc::Sender<TapEvent>,
    index_allocator: Arc<AtomicUsize>,
    turn_id: u64,
}

impl TapQueue {
    pub fn spawn(
        job_id: String,
        out: mpsc::Sender<TapEvent>,
        metrics: Arc<RelayMetrics>,
        job_poisoned: Option<Arc<AtomicBool>>,
        index_allocator: Arc<AtomicUsize>,
        turn_id: u64,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Bytes>(TAP_QUEUE_ITEMS);
        let budget = Arc::new(TapBudget::new(TAP_QUEUE_BYTES));
        let poisoned = job_poisoned.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        let ctx = DrainCtx {
            budget: Arc::clone(&budget),
            poisoned: Arc::clone(&poisoned),
            metrics: Arc::clone(&metrics),
            out: out.clone(),
            job_id: job_id.clone(),
            index_allocator: Arc::clone(&index_allocator),
            turn_id,
        };
        tokio::spawn(async move {
            drain_tap_chunks(&mut rx, ctx).await;
        });
        Self {
            tx,
            budget,
            poisoned,
            metrics,
            job_id,
            out,
            index_allocator,
            turn_id,
        }
    }

    pub fn offer(&self, bytes: Bytes) {
        if self.poisoned.load(Ordering::Relaxed) {
            return;
        }
        if !self.budget.try_reserve(bytes.len()) {
            self.poison();
            return;
        }
        match self.tx.try_send(bytes) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(bytes)) => {
                self.budget.release(bytes.len());
                self.poison();
            }
            Err(mpsc::error::TrySendError::Closed(bytes)) => {
                self.budget.release(bytes.len());
            }
        }
    }

    pub fn poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    pub fn poison(&self) {
        mark_poisoned(
            &self.poisoned,
            &self.metrics,
            &self.out,
            &self.job_id,
            self.turn_id,
        );
    }
}

impl TapEvent {
    pub fn poisoned(job_id: String, turn_id: u64) -> Self {
        Self {
            job_id,
            turn_id,
            event: json!({ "type": TAP_POISONED }),
        }
    }

    pub fn usage(job_id: String, usage: Value, turn_id: u64) -> Self {
        Self {
            job_id,
            turn_id,
            event: json!({ "type": TAP_USAGE, "usage": usage }),
        }
    }

    pub fn drained(job_id: String, turn_id: u64) -> Self {
        Self {
            job_id,
            turn_id,
            event: json!({ "type": TAP_DRAINED }),
        }
    }

    pub fn is_poisoned(&self) -> bool {
        self.event.get("type").and_then(Value::as_str) == Some(TAP_POISONED)
    }

    pub fn is_drained(&self) -> bool {
        self.event.get("type").and_then(Value::as_str) == Some(TAP_DRAINED)
    }

    pub fn usage_value(&self) -> Option<Value> {
        if self.event.get("type").and_then(Value::as_str) != Some(TAP_USAGE) {
            return None;
        }
        self.event.get("usage").cloned()
    }
}

/// Owned context for the per-response tap decode task.
struct DrainCtx {
    budget: Arc<TapBudget>,
    poisoned: Arc<AtomicBool>,
    metrics: Arc<RelayMetrics>,
    out: mpsc::Sender<TapEvent>,
    job_id: String,
    index_allocator: Arc<AtomicUsize>,
    turn_id: u64,
}

async fn drain_tap_chunks(rx: &mut mpsc::Receiver<Bytes>, ctx: DrainCtx) {
    let DrainCtx {
        budget,
        poisoned,
        metrics,
        out,
        job_id,
        index_allocator,
        turn_id,
    } = ctx;
    let mut decoder = SseDecoder::default();
    let mut filter = EventFilter::new(index_allocator);
    while let Some(bytes) = rx.recv().await {
        budget.release(bytes.len());
        if poisoned.load(Ordering::Relaxed) {
            continue;
        }
        let frames = match decoder.push(&bytes) {
            Ok(frames) => frames,
            Err(()) => {
                mark_poisoned(&poisoned, &metrics, &out, &job_id, turn_id);
                continue;
            }
        };
        if !forward_frames(
            frames,
            &mut filter,
            &out,
            &job_id,
            turn_id,
            &poisoned,
            &metrics,
        ) {
            break;
        }
    }
    let _ = out.try_send(TapEvent::drained(job_id, turn_id));
}

fn forward_frames(
    frames: Vec<Value>,
    filter: &mut EventFilter,
    out: &mpsc::Sender<TapEvent>,
    job_id: &str,
    turn_id: u64,
    poisoned: &AtomicBool,
    metrics: &RelayMetrics,
) -> bool {
    for frame in frames {
        let events = filter.apply(frame);
        if let Some(usage) = filter.take_usage_delta()
            && !usage_is_empty(&usage)
        {
            match out.try_send(TapEvent::usage(job_id.to_string(), usage, turn_id)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    mark_poisoned(poisoned, metrics, out, job_id, turn_id);
                    return false;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return false,
            }
        }
        for event in events {
            match out.try_send(TapEvent {
                job_id: job_id.to_string(),
                turn_id,
                event,
            }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    mark_poisoned(poisoned, metrics, out, job_id, turn_id);
                    return false;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return false,
            }
        }
    }
    true
}

fn usage_is_empty(usage: &Value) -> bool {
    usage.as_object().map(Map::is_empty).unwrap_or(true)
}

fn mark_poisoned(
    poisoned: &AtomicBool,
    metrics: &RelayMetrics,
    out: &mpsc::Sender<TapEvent>,
    job_id: &str,
    turn_id: u64,
) {
    if poisoned.swap(true, Ordering::Relaxed) {
        return;
    }
    metrics.inc_tap_dropped();
    let _ = out.try_send(TapEvent::poisoned(job_id.to_string(), turn_id));
}

struct TapBudget {
    used: AtomicUsize,
    max: usize,
}

impl TapBudget {
    fn new(max: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            max,
        }
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        let bytes = bytes.max(1);
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.max {
                return false;
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes.max(1)))
            })
            .ok();
    }
}

#[derive(Default)]
pub struct SseDecoder {
    buf: Vec<u8>,
    poisoned: bool,
}

impl SseDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Value>, ()> {
        if self.poisoned {
            return Err(());
        }
        self.buf.extend_from_slice(bytes);
        if self.buf.len() > MAX_FRAME_BYTES {
            self.poisoned = true;
            return Err(());
        }
        let mut out = Vec::new();
        while let Some((pos, delimiter_len)) = frame_boundary(&self.buf) {
            let frame = self.buf[..pos].to_vec();
            self.buf.drain(..pos + delimiter_len);
            if let Some(value) = parse_frame(&frame) {
                out.push(value);
            }
        }
        Ok(out)
    }

    pub fn poisoned(&self) -> bool {
        self.poisoned
    }
}

#[derive(Default)]
pub struct EventFilter {
    index_allocator: Arc<AtomicUsize>,
    index_map: HashMap<u64, u64>,
    swallowed: Vec<u64>,
    usage: Map<String, Value>,
    response_usage: Map<String, Value>,
    emitted_response_usage: Map<String, Value>,
    pending_usage_delta: Option<Value>,
}

impl EventFilter {
    pub fn new(index_allocator: Arc<AtomicUsize>) -> Self {
        Self {
            index_allocator,
            index_map: HashMap::new(),
            swallowed: Vec::new(),
            usage: Map::new(),
            response_usage: Map::new(),
            emitted_response_usage: Map::new(),
            pending_usage_delta: None,
        }
    }

    pub fn with_start_index(next_index: usize) -> Self {
        Self::new(Arc::new(AtomicUsize::new(next_index)))
    }

    pub fn apply(&mut self, mut event: Value) -> Vec<Value> {
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                if let Some(usage) = event.pointer("/message/usage").and_then(Value::as_object) {
                    self.set_response_usage(usage);
                }
                Vec::new()
            }
            Some("message_stop") => {
                self.response_usage.clear();
                self.emitted_response_usage.clear();
                Vec::new()
            }
            Some("ping") => Vec::new(),
            Some("message_delta") => {
                if let Some(usage) = event.get("usage").and_then(Value::as_object) {
                    self.set_response_usage(usage);
                }
                Vec::new()
            }
            Some("content_block_start") => self.content_block_start(event),
            Some("content_block_delta" | "content_block_stop") => {
                self.rewrite_index(&mut event).into_iter().collect()
            }
            _ => vec![event],
        }
    }

    pub fn usage(&self) -> Value {
        Value::Object(self.usage.clone())
    }

    pub fn take_usage_delta(&mut self) -> Option<Value> {
        self.pending_usage_delta.take()
    }

    fn content_block_start(&mut self, mut event: Value) -> Vec<Value> {
        let Some(old_index) = event.get("index").and_then(Value::as_u64) else {
            return vec![event];
        };
        let block = event.get("content_block").cloned().unwrap_or(Value::Null);
        if is_internal_tool(&block) {
            self.swallowed.push(old_index);
            return Vec::new();
        }
        let new_index = self.index_allocator.fetch_add(1, Ordering::AcqRel) as u64;
        self.index_map.insert(old_index, new_index);
        event["index"] = Value::from(new_index);
        vec![event]
    }

    fn rewrite_index(&self, event: &mut Value) -> Option<Value> {
        let old_index = event.get("index").and_then(Value::as_u64)?;
        if self.swallowed.contains(&old_index) {
            return None;
        }
        if let Some(new_index) = self.index_map.get(&old_index).copied() {
            event["index"] = Value::from(new_index);
        }
        Some(event.clone())
    }

    fn set_response_usage(&mut self, usage: &Map<String, Value>) {
        for (key, value) in usage {
            let Some(value) = value.as_u64() else {
                continue;
            };
            self.response_usage.insert(key.clone(), json!(value));
        }
        let mut delta = Map::new();
        for (key, value) in &self.response_usage {
            let Some(value) = value.as_u64() else {
                continue;
            };
            let emitted = self
                .emitted_response_usage
                .get(key)
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if value > emitted {
                let add = value - emitted;
                add_usage_value(&mut self.usage, key, add);
                delta.insert(key.clone(), json!(add));
            }
        }
        if !delta.is_empty() {
            self.emitted_response_usage = self.response_usage.clone();
            self.pending_usage_delta = Some(Value::Object(delta));
        }
    }
}

fn frame_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let lf = buf
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|pos| (pos, 2));
    let crlf = buf
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| (pos, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn parse_frame(frame: &[u8]) -> Option<Value> {
    let text = std::str::from_utf8(frame).ok()?;
    let mut data = String::new();
    for line in text.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.trim_start());
        }
    }
    if data.is_empty() || data == "[DONE]" {
        return None;
    }
    serde_json::from_str(&data).ok()
}

fn is_internal_tool(block: &Value) -> bool {
    block.get("type").and_then(Value::as_str) == Some("tool_use")
        && block
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| name.starts_with("mcp__kin_runtime__"))
}

fn add_usage(total: &mut Map<String, Value>, usage: &Map<String, Value>) {
    for (key, value) in usage {
        let Some(value) = value.as_u64() else {
            continue;
        };
        add_usage_value(total, key, value);
    }
}

fn add_usage_value(total: &mut Map<String, Value>, key: &str, value: u64) {
    let current = total.get(key).and_then(Value::as_u64).unwrap_or(0);
    total.insert(key.to_string(), json!(current.saturating_add(value)));
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn event(value: Value) -> Vec<u8> {
        format!("event: message\ndata: {value}\n\n").into_bytes()
    }

    #[test]
    fn decoder_handles_cross_chunk_frames() {
        let mut decoder = SseDecoder::default();
        let bytes = event(json!({"type":"ping"}));
        assert!(decoder.push(&bytes[..10]).unwrap().is_empty());
        let out = decoder.push(&bytes[10..]).unwrap();
        assert_eq!(out, vec![json!({"type":"ping"})]);
    }

    #[test]
    fn decoder_handles_crlf_frame_boundaries() {
        let mut decoder = SseDecoder::default();
        let out = decoder
            .push(b"event: message\r\ndata: {\"type\":\"ping\"}\r\n\r\n")
            .unwrap();
        assert_eq!(out, vec![json!({"type":"ping"})]);
    }

    #[test]
    fn decoder_poisoned_on_large_incomplete_frame() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(&vec![b'a'; MAX_FRAME_BYTES + 1]).is_err());
        assert!(decoder.poisoned());
    }

    #[test]
    fn filter_table_and_reindexing() {
        let mut filter = EventFilter::with_start_index(0);
        assert!(
            filter
                .apply(json!({"type":"message_start","message":{"usage":{"input_tokens":3}}}))
                .is_empty()
        );
        assert_eq!(filter.take_usage_delta(), Some(json!({"input_tokens":3})));
        assert!(filter.apply(json!({"type":"ping"})).is_empty());
        assert!(
            filter
                .apply(json!({"type":"message_delta","usage":{"output_tokens":2}}))
                .is_empty()
        );
        assert_eq!(filter.take_usage_delta(), Some(json!({"output_tokens":2})));
        let internal = filter.apply(json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"tool_use","id":"toolu_i","name":"mcp__kin_runtime__slot_wait","input":{}}
        }));
        assert!(internal.is_empty());
        assert!(
            filter
                .apply(json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}))
                .is_empty()
        );
        let start = filter.apply(json!({
            "type":"content_block_start",
            "index":5,
            "content_block":{"type":"text","text":""}
        }));
        assert_eq!(start[0]["index"], 0);
        let delta = filter.apply(json!({
            "type":"content_block_delta",
            "index":5,
            "delta":{"type":"text_delta","text":"hi"}
        }));
        assert_eq!(delta[0]["index"], 0);
        let server = filter.apply(json!({
            "type":"content_block_start",
            "index":9,
            "content_block":{"type":"server_tool_use","id":"srv","name":"web_search","input":{}}
        }));
        assert_eq!(server[0]["content_block"]["type"], "server_tool_use");
        assert_eq!(server[0]["index"], 1);
        assert_eq!(filter.usage(), json!({"input_tokens":3,"output_tokens":2}));
    }

    #[test]
    fn filter_index_continues_across_internal_responses() {
        let indexes = Arc::new(AtomicUsize::new(0));
        let mut first = EventFilter::new(Arc::clone(&indexes));
        let mut second = EventFilter::new(indexes);
        let a = first.apply(json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"text","text":""}
        }));
        let b = second.apply(json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"text","text":""}
        }));
        assert_eq!(a[0]["index"], 0);
        assert_eq!(b[0]["index"], 1);
    }

    #[test]
    fn usage_accumulates_message_start_and_multiple_responses() {
        let mut first = EventFilter::with_start_index(0);
        let mut second = EventFilter::with_start_index(1);
        first.apply(json!({"type":"message_start","message":{"usage":{"input_tokens":4}}}));
        assert_eq!(first.take_usage_delta(), Some(json!({"input_tokens":4})));
        first.apply(json!({"type":"message_delta","usage":{"output_tokens":2}}));
        assert_eq!(first.take_usage_delta(), Some(json!({"output_tokens":2})));
        first.apply(json!({"type":"message_delta","usage":{"output_tokens":5}}));
        assert_eq!(first.take_usage_delta(), Some(json!({"output_tokens":3})));
        second.apply(json!({"type":"message_start","message":{"usage":{"input_tokens":6}}}));
        second.apply(json!({"type":"message_delta","usage":{"output_tokens":1}}));
        assert_eq!(first.usage(), json!({"input_tokens":4,"output_tokens":5}));
        assert_eq!(second.usage(), json!({"input_tokens":6,"output_tokens":1}));
    }

    #[tokio::test]
    async fn tap_overflow_poisoned_and_counts_drop() {
        let (tx, _rx) = mpsc::channel(1);
        let metrics = Arc::new(RelayMetrics::default());
        let tap = TapQueue::spawn(
            "job-1".into(),
            tx,
            Arc::clone(&metrics),
            None,
            Arc::new(AtomicUsize::new(0)),
            0,
        );
        tap.offer(Bytes::from(vec![b'x'; TAP_QUEUE_BYTES + 1]));
        assert!(tap.poisoned());
        assert_eq!(metrics.tap_dropped.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn tap_closed_output_does_not_poison_or_count() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let metrics = Arc::new(RelayMetrics::default());
        let tap = TapQueue::spawn(
            "job-1".into(),
            tx,
            Arc::clone(&metrics),
            None,
            Arc::new(AtomicUsize::new(0)),
            0,
        );
        tap.offer(Bytes::from(event(json!({"type":"ping"}))));
        tokio::task::yield_now().await;
        assert!(!tap.poisoned());
        assert_eq!(metrics.tap_dropped.load(Ordering::Relaxed), 0);
    }
}
