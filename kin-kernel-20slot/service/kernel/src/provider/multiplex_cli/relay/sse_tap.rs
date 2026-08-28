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
pub(crate) const TAP_USAGE: &str = "kin_tap_usage";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapEvent {
    pub job_id: String,
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
}

impl TapQueue {
    pub fn spawn(
        job_id: String,
        out: mpsc::Sender<TapEvent>,
        metrics: Arc<RelayMetrics>,
        job_poisoned: Option<Arc<AtomicBool>>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Bytes>(TAP_QUEUE_ITEMS);
        let budget = Arc::new(TapBudget::new(TAP_QUEUE_BYTES));
        let poisoned = job_poisoned.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        let task_budget = Arc::clone(&budget);
        let task_poisoned = Arc::clone(&poisoned);
        let task_metrics = Arc::clone(&metrics);
        let task_out = out.clone();
        let task_job = job_id.clone();
        tokio::spawn(async move {
            drain_tap_chunks(
                &mut rx,
                task_budget,
                task_poisoned,
                task_metrics,
                task_out,
                task_job,
            )
            .await;
        });
        Self {
            tx,
            budget,
            poisoned,
            metrics,
            job_id,
            out,
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
            Err(mpsc::error::TrySendError::Full(bytes))
            | Err(mpsc::error::TrySendError::Closed(bytes)) => {
                self.budget.release(bytes.len());
                self.poison();
            }
        }
    }

    pub fn poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    fn poison(&self) {
        mark_poisoned(&self.poisoned, &self.metrics, &self.out, &self.job_id);
    }
}

impl TapEvent {
    pub fn poisoned(job_id: String) -> Self {
        Self {
            job_id,
            event: json!({ "type": TAP_POISONED }),
        }
    }

    pub fn usage(job_id: String, usage: Value) -> Self {
        Self {
            job_id,
            event: json!({ "type": TAP_USAGE, "usage": usage }),
        }
    }

    pub fn is_poisoned(&self) -> bool {
        self.event.get("type").and_then(Value::as_str) == Some(TAP_POISONED)
    }

    pub fn usage_value(&self) -> Option<Value> {
        if self.event.get("type").and_then(Value::as_str) != Some(TAP_USAGE) {
            return None;
        }
        self.event.get("usage").cloned()
    }
}

async fn drain_tap_chunks(
    rx: &mut mpsc::Receiver<Bytes>,
    budget: Arc<TapBudget>,
    poisoned: Arc<AtomicBool>,
    metrics: Arc<RelayMetrics>,
    out: mpsc::Sender<TapEvent>,
    job_id: String,
) {
    let mut decoder = SseDecoder::default();
    let mut filter = EventFilter::default();
    let mut last_usage = Value::Null;
    while let Some(bytes) = rx.recv().await {
        budget.release(bytes.len());
        if poisoned.load(Ordering::Relaxed) {
            continue;
        }
        let frames = match decoder.push(&bytes) {
            Ok(frames) => frames,
            Err(()) => {
                mark_poisoned(&poisoned, &metrics, &out, &job_id);
                continue;
            }
        };
        if !forward_frames(
            frames,
            &mut filter,
            &mut last_usage,
            &out,
            &job_id,
            &poisoned,
            &metrics,
        ) {
            break;
        }
    }
}

fn forward_frames(
    frames: Vec<Value>,
    filter: &mut EventFilter,
    last_usage: &mut Value,
    out: &mpsc::Sender<TapEvent>,
    job_id: &str,
    poisoned: &AtomicBool,
    metrics: &RelayMetrics,
) -> bool {
    for frame in frames {
        let events = filter.apply(frame);
        let usage = filter.usage();
        if usage != *last_usage && !usage_is_empty(&usage) {
            *last_usage = usage.clone();
            if out
                .try_send(TapEvent::usage(job_id.to_string(), usage))
                .is_err()
            {
                mark_poisoned(poisoned, metrics, out, job_id);
                return false;
            }
        }
        for event in events {
            if out
                .try_send(TapEvent {
                    job_id: job_id.to_string(),
                    event,
                })
                .is_err()
            {
                mark_poisoned(poisoned, metrics, out, job_id);
                return false;
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
) {
    if poisoned.swap(true, Ordering::Relaxed) {
        return;
    }
    metrics.inc_tap_dropped();
    let _ = out.try_send(TapEvent::poisoned(job_id.to_string()));
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
        while let Some(pos) = frame_boundary(&self.buf) {
            let frame = self.buf[..pos].to_vec();
            self.buf.drain(..pos + 2);
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
    next_index: u64,
    index_map: HashMap<u64, u64>,
    swallowed: Vec<u64>,
    usage: Map<String, Value>,
}

impl EventFilter {
    pub fn apply(&mut self, mut event: Value) -> Vec<Value> {
        match event.get("type").and_then(Value::as_str) {
            Some("message_start" | "message_stop" | "ping") => Vec::new(),
            Some("message_delta") => {
                if let Some(usage) = event.get("usage").and_then(Value::as_object) {
                    add_usage(&mut self.usage, usage);
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

    fn content_block_start(&mut self, mut event: Value) -> Vec<Value> {
        let Some(old_index) = event.get("index").and_then(Value::as_u64) else {
            return vec![event];
        };
        let block = event.get("content_block").cloned().unwrap_or(Value::Null);
        if is_internal_tool(&block) {
            self.swallowed.push(old_index);
            return Vec::new();
        }
        let new_index = self.next_index;
        self.next_index += 1;
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
}

fn frame_boundary(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|window| window == b"\n\n")
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
        let current = total.get(key).and_then(Value::as_u64).unwrap_or(0);
        total.insert(key.clone(), json!(current.saturating_add(value)));
    }
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
    fn decoder_poisoned_on_large_incomplete_frame() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(&vec![b'a'; MAX_FRAME_BYTES + 1]).is_err());
        assert!(decoder.poisoned());
    }

    #[test]
    fn filter_table_and_reindexing() {
        let mut filter = EventFilter::default();
        assert!(filter.apply(json!({"type":"message_start"})).is_empty());
        assert!(filter.apply(json!({"type":"ping"})).is_empty());
        assert!(
            filter
                .apply(json!({"type":"message_delta","usage":{"output_tokens":2}}))
                .is_empty()
        );
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
        assert_eq!(filter.usage(), json!({"output_tokens":2}));
    }

    #[tokio::test]
    async fn tap_overflow_poisoned_and_counts_drop() {
        let (tx, _rx) = mpsc::channel(1);
        let metrics = Arc::new(RelayMetrics::default());
        let tap = TapQueue::spawn("job-1".into(), tx, Arc::clone(&metrics), None);
        tap.offer(Bytes::from(vec![b'x'; TAP_QUEUE_BYTES + 1]));
        assert!(tap.poisoned());
        assert_eq!(metrics.tap_dropped.load(Ordering::Relaxed), 1);
    }
}
