//! Anthropic SSE event filter shared by every execution path.
//!
//! Swallows internal MCP tool blocks, remaps content-block indexes onto a
//! job-level allocator, and accumulates usage deltas. `FilterPolicy` selects
//! whether kin_done tool arguments may be re-streamed as synthesized text
//! (relay legacy) or dropped (CLI stdout partials).

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use serde_json::{Map, Value, json};

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

    #[cfg(test)]
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

    #[cfg(test)]
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

fn add_usage_value(total: &mut Map<String, Value>, key: &str, value: u64) {
    let current = total.get(key).and_then(Value::as_u64).unwrap_or(0);
    total.insert(key.to_string(), json!(current.saturating_add(value)));
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
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
}
