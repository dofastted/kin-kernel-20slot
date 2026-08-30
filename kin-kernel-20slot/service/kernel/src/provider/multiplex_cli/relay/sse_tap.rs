use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use axum::body::Bytes;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use super::super::event_filter::EventFilter;
use super::metrics::RelayMetrics;

const TAP_QUEUE_ITEMS: usize = 256;
const TAP_QUEUE_BYTES: usize = 2 * 1024 * 1024;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Poison threshold for a stream that yields no parseable SSE event. Real SSE
/// produces an event well within the first few KiB; consuming this much with
/// zero events means the bytes are not SSE at all (e.g. a compressed body),
/// and the failure must surface as tap_dropped instead of a silent no-op.
const GARBAGE_BYTES: usize = 64 * 1024;
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

    #[cfg(test)]
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
    consumed_without_event: usize,
    saw_event: bool,
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
        if out.is_empty() && !self.saw_event {
            // Garbage guard: bytes that never produce an event are not SSE
            // (a compressed body, for example). Fail loudly instead of
            // discarding the stream in silence.
            self.consumed_without_event += bytes.len();
            if self.consumed_without_event > GARBAGE_BYTES {
                self.poisoned = true;
                return Err(());
            }
        } else if !out.is_empty() {
            self.saw_event = true;
        }
        Ok(out)
    }

    #[cfg(test)]
    pub fn poisoned(&self) -> bool {
        self.poisoned
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
    fn decoder_poisons_on_non_sse_garbage_stream() {
        // A compressed (or otherwise non-SSE) body used to be discarded in
        // silence: frames containing "\n\n" parsed to nothing, the buffer
        // stayed small, and the tap produced zero events with zero metrics.
        let mut decoder = SseDecoder::default();
        // Pseudo-compressed bytes with newlines so frame_boundary keeps
        // draining the buffer below MAX_FRAME_BYTES.
        let junk: Vec<u8> = (0..4096u32)
            .flat_map(|i| [(i % 251) as u8, b'\n', b'\n'])
            .collect();
        let mut poisoned = false;
        for _ in 0..8 {
            if decoder.push(&junk).is_err() {
                poisoned = true;
                break;
            }
        }
        assert!(poisoned, "garbage stream must poison, not silently no-op");
        assert!(decoder.poisoned());
    }

    #[test]
    fn decoder_tolerates_late_first_event_within_budget() {
        let mut decoder = SseDecoder::default();
        // Comment/heartbeat padding before the first event is legal SSE.
        assert!(decoder.push(&vec![b'\n'; 16 * 1024]).unwrap().is_empty());
        let out = decoder.push(&event(json!({"type":"ping"}))).unwrap();
        assert_eq!(out, vec![json!({"type":"ping"})]);
        // Once a real event arrived, event-free stretches well past the
        // garbage budget are fine (pushed in chunks like a real stream).
        for _ in 0..40 {
            assert!(decoder.push(&vec![b'\n'; 4 * 1024]).is_ok());
        }
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
