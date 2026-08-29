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

    pub fn poisoned(&self) -> bool {
        self.poisoned
    }
}

/// kin-slot agents deliver the final answer inside the kin_done tool call
/// (streamed as input_json_delta), per their prompt. The filter re-streams
/// that argument's `text` field as synthesized text_delta events.
const KIN_DONE_TOOL: &str = "mcp__kin_runtime__kin_done";

/// Per-source policy for `EventFilter`. Relay keeps kin_done text synthesis
/// as a legacy fallback; CLI stdout partials must never invent a body from
/// internal tool arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilterPolicy {
    pub synthesize_kin_done_text: bool,
}

impl FilterPolicy {
    pub const RELAY: Self = Self {
        synthesize_kin_done_text: true,
    };
    pub const CLI: Self = Self {
        synthesize_kin_done_text: false,
    };
}

impl Default for FilterPolicy {
    fn default() -> Self {
        Self::RELAY
    }
}

/// Top-level key marking events synthesized from kin_done arguments. The
/// arbiter uses it to drop a synthesized duplicate when real upstream text
/// already streamed this turn; JobStream strips it before the client.
pub(crate) const KIN_SYNTH_MARKER: &str = "kin_synth";

#[derive(Default)]
pub struct EventFilter {
    policy: FilterPolicy,
    index_allocator: Arc<AtomicUsize>,
    index_map: HashMap<u64, u64>,
    swallowed: Vec<u64>,
    usage: Map<String, Value>,
    response_usage: Map<String, Value>,
    emitted_response_usage: Map<String, Value>,
    pending_usage_delta: Option<Value>,
    /// Legacy: turn kin_done.input_json_delta `text` into synthesized
    /// text_delta. Off for CLI partials — native assistant text is the body.
    kin_done: Option<KinDoneSynth>,
    saw_real_text: bool,
}

struct KinDoneSynth {
    source_index: u64,
    emit_index: Option<u64>,
    extractor: KinDoneTextExtractor,
}

impl EventFilter {
    pub fn new(index_allocator: Arc<AtomicUsize>) -> Self {
        Self::with_policy(index_allocator, FilterPolicy::RELAY)
    }

    pub fn with_policy(index_allocator: Arc<AtomicUsize>, policy: FilterPolicy) -> Self {
        Self {
            policy,
            index_allocator,
            index_map: HashMap::new(),
            swallowed: Vec::new(),
            usage: Map::new(),
            response_usage: Map::new(),
            emitted_response_usage: Map::new(),
            pending_usage_delta: None,
            kin_done: None,
            saw_real_text: false,
        }
    }

    pub fn with_start_index(next_index: usize) -> Self {
        Self::new(Arc::new(AtomicUsize::new(next_index)))
    }

    pub fn apply(&mut self, mut event: Value) -> Vec<Value> {
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                // Each internal Messages response restarts content indexes at 0.
                // Keep the job-level allocator; drop the response-local map.
                self.index_map.clear();
                self.swallowed.clear();
                self.kin_done = None;
                self.saw_real_text = false;
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
                if let Some(out) = self.kin_done_intercept(&event) {
                    return out;
                }
                let out: Vec<Value> = self.rewrite_index(&mut event).into_iter().collect();
                if !out.is_empty()
                    && event.pointer("/delta/type").and_then(Value::as_str) == Some("text_delta")
                {
                    self.saw_real_text = true;
                }
                out
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
            if is_kin_done_tool(&block)
                && self.policy.synthesize_kin_done_text
                && !self.saw_real_text
            {
                self.kin_done = Some(KinDoneSynth {
                    source_index: old_index,
                    emit_index: None,
                    extractor: KinDoneTextExtractor::default(),
                });
            }
            return Vec::new();
        }
        let new_index = self.index_allocator.fetch_add(1, Ordering::AcqRel) as u64;
        self.index_map.insert(old_index, new_index);
        event["index"] = Value::from(new_index);
        vec![event]
    }

    /// Turn a kin_done input_json_delta stream into synthesized text_delta
    /// events. Returns Some(events) when the event belonged to the tracked
    /// kin_done block (even if nothing is emitted for it).
    fn kin_done_intercept(&mut self, event: &Value) -> Option<Vec<Value>> {
        let source_index = self.kin_done.as_ref()?.source_index;
        let index = event.get("index").and_then(Value::as_u64)?;
        if index != source_index {
            return None;
        }
        let kind = event.get("type").and_then(Value::as_str)?;
        if kind == "content_block_stop" {
            let synth = self.kin_done.take()?;
            let Some(index) = synth.emit_index else {
                return Some(Vec::new());
            };
            return Some(vec![json!({
                "type": "content_block_stop",
                "index": index,
                KIN_SYNTH_MARKER: true
            })]);
        }
        if event.pointer("/delta/type").and_then(Value::as_str) != Some("input_json_delta") {
            return Some(Vec::new());
        }
        let partial = event
            .pointer("/delta/partial_json")
            .and_then(Value::as_str)
            .unwrap_or("");
        let (text, emit_index) = {
            let synth = self.kin_done.as_mut()?;
            (synth.extractor.push(partial), synth.emit_index)
        };
        if text.is_empty() {
            return Some(Vec::new());
        }
        let mut out = Vec::new();
        let index = match emit_index {
            Some(index) => index,
            None => {
                let index = self.index_allocator.fetch_add(1, Ordering::AcqRel) as u64;
                self.kin_done.as_mut()?.emit_index = Some(index);
                out.push(json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": { "type": "text", "text": "" },
                    KIN_SYNTH_MARKER: true
                }));
                index
            }
        };
        out.push(json!({
            "type": "content_block_delta",
            "index": index,
            "delta": { "type": "text_delta", "text": text },
            KIN_SYNTH_MARKER: true
        }));
        Some(out)
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

fn is_kin_done_tool(block: &Value) -> bool {
    block.get("type").and_then(Value::as_str) == Some("tool_use")
        && block.get("name").and_then(Value::as_str) == Some(KIN_DONE_TOOL)
}

/// Incremental extractor for the `text` field of a kin_done tool call's
/// argument object, fed arbitrary `input_json_delta.partial_json` fragments.
/// Never buffers the whole document: structure outside the target string is
/// consumed char-by-char, and only unfinished escape sequences carry over
/// between chunks.
#[derive(Default)]
struct KinDoneTextExtractor {
    state: ExtractState,
    depth: u32,
    next_string_is_key: bool,
    key_buf: String,
    /// Pending escape body (chars after the backslash) inside the text value.
    esc: Option<String>,
    /// High surrogate from a `\uD8xx` escape awaiting its low half.
    pending_high: Option<u16>,
    /// Escape flag for strings we skip (keys / other values).
    skip_escape: bool,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum ExtractState {
    #[default]
    Structure,
    InKey,
    InOtherString,
    AwaitColon,
    AwaitValueQuote,
    InText,
    Done,
}

impl KinDoneTextExtractor {
    fn push(&mut self, chunk: &str) -> String {
        let mut out = String::new();
        for c in chunk.chars() {
            match self.state {
                ExtractState::Done => break,
                ExtractState::Structure => self.structure_char(c),
                ExtractState::InKey => {
                    if self.skip_escape {
                        self.skip_escape = false;
                    } else if c == '\\' {
                        self.skip_escape = true;
                    } else if c == '"' {
                        self.state = if self.key_buf == "text" {
                            ExtractState::AwaitColon
                        } else {
                            ExtractState::Structure
                        };
                    } else {
                        self.key_buf.push(c);
                    }
                }
                ExtractState::InOtherString => {
                    if self.skip_escape {
                        self.skip_escape = false;
                    } else if c == '\\' {
                        self.skip_escape = true;
                    } else if c == '"' {
                        self.state = ExtractState::Structure;
                    }
                }
                ExtractState::AwaitColon => {
                    if c == ':' {
                        self.state = ExtractState::AwaitValueQuote;
                    } else if !c.is_whitespace() {
                        self.state = ExtractState::Structure;
                        self.structure_char(c);
                    }
                }
                ExtractState::AwaitValueQuote => {
                    if c == '"' {
                        self.state = ExtractState::InText;
                    } else if !c.is_whitespace() {
                        // `text` is not a string (e.g. null); nothing to stream.
                        self.state = ExtractState::Structure;
                        self.structure_char(c);
                    }
                }
                ExtractState::InText => self.text_char(c, &mut out),
            }
        }
        out
    }

    fn structure_char(&mut self, c: char) {
        match c {
            '{' | '[' => {
                self.depth += 1;
                if self.depth == 1 {
                    self.next_string_is_key = true;
                }
            }
            '}' | ']' => self.depth = self.depth.saturating_sub(1),
            ',' if self.depth == 1 => self.next_string_is_key = true,
            ':' if self.depth == 1 => self.next_string_is_key = false,
            '"' => {
                if self.depth == 1 && self.next_string_is_key {
                    self.key_buf.clear();
                    self.state = ExtractState::InKey;
                } else {
                    self.state = ExtractState::InOtherString;
                }
            }
            _ => {}
        }
    }

    fn text_char(&mut self, c: char, out: &mut String) {
        if let Some(esc) = &mut self.esc {
            esc.push(c);
            match resolve_escape(esc) {
                EscapeStep::Incomplete => {}
                EscapeStep::Unit(unit) => {
                    self.esc = None;
                    self.push_unit(unit, out);
                }
                EscapeStep::Literal(ch) => {
                    self.esc = None;
                    self.flush_pending_high(out);
                    out.push(ch);
                }
                EscapeStep::Invalid => {
                    self.esc = None;
                    self.flush_pending_high(out);
                }
            }
        } else if c == '\\' {
            self.esc = Some(String::new());
        } else if c == '"' {
            self.flush_pending_high(out);
            self.state = ExtractState::Done;
        } else {
            self.flush_pending_high(out);
            out.push(c);
        }
    }

    fn push_unit(&mut self, unit: u16, out: &mut String) {
        if let Some(high) = self.pending_high.take() {
            if (0xDC00..=0xDFFF).contains(&unit) {
                let combined =
                    0x10000 + ((u32::from(high) - 0xD800) << 10) + (u32::from(unit) - 0xDC00);
                out.push(char::from_u32(combined).unwrap_or('\u{FFFD}'));
                return;
            }
            out.push('\u{FFFD}');
        }
        match unit {
            0xD800..=0xDBFF => self.pending_high = Some(unit),
            0xDC00..=0xDFFF => out.push('\u{FFFD}'),
            _ => out.push(char::from_u32(u32::from(unit)).unwrap_or('\u{FFFD}')),
        }
    }

    fn flush_pending_high(&mut self, out: &mut String) {
        if self.pending_high.take().is_some() {
            out.push('\u{FFFD}');
        }
    }
}

enum EscapeStep {
    Incomplete,
    Literal(char),
    Unit(u16),
    Invalid,
}

fn resolve_escape(esc: &str) -> EscapeStep {
    let mut chars = esc.chars();
    let Some(first) = chars.next() else {
        return EscapeStep::Incomplete;
    };
    match first {
        '"' => EscapeStep::Literal('"'),
        '\\' => EscapeStep::Literal('\\'),
        '/' => EscapeStep::Literal('/'),
        'n' => EscapeStep::Literal('\n'),
        't' => EscapeStep::Literal('\t'),
        'r' => EscapeStep::Literal('\r'),
        'b' => EscapeStep::Literal('\u{8}'),
        'f' => EscapeStep::Literal('\u{c}'),
        'u' => {
            let hex: String = chars.collect();
            if hex.len() < 4 {
                return EscapeStep::Incomplete;
            }
            match u16::from_str_radix(&hex[..4], 16) {
                Ok(unit) => EscapeStep::Unit(unit),
                Err(_) => EscapeStep::Invalid,
            }
        }
        _ => EscapeStep::Invalid,
    }
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
    fn kin_done_text_argument_synthesizes_text_deltas() {
        // Real kin-slot responses put the answer in kin_done's `text` arg,
        // streamed as input_json_delta — the filter must re-stream it as
        // marked text_delta events (root cause of "no per-token output").
        let mut filter = EventFilter::with_start_index(0);
        assert!(
            filter
                .apply(json!({
                    "type":"content_block_start",
                    "index":1,
                    "content_block":{"type":"tool_use","id":"toolu_kd","name":"mcp__kin_runtime__kin_done","input":{}}
                }))
                .is_empty()
        );
        let feed = |filter: &mut EventFilter, partial: &str| {
            filter.apply(json!({
                "type":"content_block_delta",
                "index":1,
                "delta":{"type":"input_json_delta","partial_json":partial}
            }))
        };
        assert!(feed(&mut filter, "").is_empty());
        assert!(feed(&mut filter, "{\"job_").is_empty());
        assert!(feed(&mut filter, "id\": \"j1\", \"stop_reason\"").is_empty());
        assert!(feed(&mut filter, ": \"end_turn\", \"te").is_empty());
        let first = feed(&mut filter, "xt\": \"你好，欢");
        assert_eq!(first.len(), 2);
        assert_eq!(first[0]["type"], "content_block_start");
        assert_eq!(first[0]["content_block"]["type"], "text");
        assert_eq!(first[0][KIN_SYNTH_MARKER], true);
        assert_eq!(first[1]["delta"]["text"], "你好，欢");
        let second = feed(&mut filter, "迎\\n光临");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0]["delta"]["text"], "迎\n光临");
        // Rest of the object after text must not leak.
        assert!(feed(&mut filter, "\", \"usage\": {\"output_tokens\": 3}}").is_empty());
        let stop = filter.apply(json!({"type":"content_block_stop","index":1}));
        assert_eq!(stop.len(), 1);
        assert_eq!(stop[0]["type"], "content_block_stop");
        assert_eq!(stop[0][KIN_SYNTH_MARKER], true);
        // Same block index for start/delta/stop.
        assert_eq!(first[0]["index"], stop[0]["index"]);
    }

    #[test]
    fn kin_done_synthesis_skipped_after_real_text() {
        // When the model streamed real text this response, kin_done's text
        // restates it — no synthesized duplicate.
        let mut filter = EventFilter::with_start_index(0);
        let start = filter.apply(json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"text","text":""}
        }));
        assert_eq!(start.len(), 1);
        assert_eq!(
            filter
                .apply(json!({
                    "type":"content_block_delta",
                    "index":0,
                    "delta":{"type":"text_delta","text":"real"}
                }))
                .len(),
            1
        );
        assert!(
            filter
                .apply(json!({
                    "type":"content_block_start",
                    "index":2,
                    "content_block":{"type":"tool_use","id":"toolu_kd","name":"mcp__kin_runtime__kin_done","input":{}}
                }))
                .is_empty()
        );
        assert!(
            filter
                .apply(json!({
                    "type":"content_block_delta",
                    "index":2,
                    "delta":{"type":"input_json_delta","partial_json":"{\"text\": \"dupe\"}"}
                }))
                .is_empty()
        );
        assert!(
            filter
                .apply(json!({"type":"content_block_stop","index":2}))
                .is_empty()
        );
    }

    #[test]
    fn cli_policy_swallows_kin_done_without_synthesis() {
        let mut filter = EventFilter::with_policy(Arc::new(AtomicUsize::new(0)), FilterPolicy::CLI);
        assert!(
            filter
                .apply(json!({
                    "type":"content_block_start",
                    "index":0,
                    "content_block":{"type":"tool_use","id":"toolu_kd","name":"mcp__kin_runtime__kin_done","input":{}}
                }))
                .is_empty()
        );
        assert!(
            filter
                .apply(json!({
                    "type":"content_block_delta",
                    "index":0,
                    "delta":{"type":"input_json_delta","partial_json":"{\"text\":\"secret\"}"}
                }))
                .is_empty()
        );
        assert!(
            filter
                .apply(json!({"type":"content_block_stop","index":0}))
                .is_empty()
        );
    }

    #[test]
    fn message_start_resets_response_local_index_map() {
        let mut filter = EventFilter::with_start_index(0);
        let first = filter.apply(json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"text","text":""}
        }));
        assert_eq!(first[0]["index"], 0);
        filter.apply(json!({"type":"message_start","message":{"id":"r2"}}));
        let second = filter.apply(json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"thinking","thinking":""}
        }));
        assert_eq!(second[0]["index"], 1);
        let delta = filter.apply(json!({
            "type":"content_block_delta",
            "index":0,
            "delta":{"type":"thinking_delta","thinking":"hmm"}
        }));
        assert_eq!(delta[0]["index"], 1, "deltas follow the new response map");
    }

    #[test]
    fn kin_done_extractor_handles_escapes_and_unicode_across_chunks() {
        let mut ex = KinDoneTextExtractor::default();
        let mut out = String::new();
        out.push_str(&ex.push("{\"job_id\": \"j\\\"x\", \"text\": \"a"));
        // Split escape across chunks.
        out.push_str(&ex.push("\\"));
        out.push_str(&ex.push("u4f60"));
        out.push_str(&ex.push("\\ud83d"));
        out.push_str(&ex.push("\\ude00 b\", \"k\": 1}"));
        assert_eq!(out, "a你😀 b");
        // Nothing after the closing quote leaks.
        assert!(ex.push("{\"text\": \"again\"}").is_empty());
    }

    #[test]
    fn kin_done_extractor_ignores_other_keys_and_nested_text() {
        let mut ex = KinDoneTextExtractor::default();
        let out = ex.push(
            "{\"final_digest\": \"text: not this\", \"usage\": {\"text\": \"nested\"}, \"text\": \"real\"}",
        );
        assert_eq!(out, "real");
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
