//! Convert CLI NDJSON frames into Anthropic SSE events.
//!
//! CLI 2.1.241 does not set `parent_tool_use_id` on `stream_event` (token
//! stream is root-only). Subagent output arrives as complete `assistant` /
//! `user` frames. We forward those as stage-level SSE blocks — never by
//! slicing a finished string into fake tokens.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use serde_json::{Value, json};

pub struct JobStream {
    index_allocator: Arc<AtomicUsize>,
    seen: HashSet<String>,
    internal_ids: HashSet<String>,
    web_search_ids: HashSet<String>,
    pub streamed_text: bool,
    pub text: String,
    pub started: bool,
}

impl JobStream {
    pub fn new() -> Self {
        Self::with_index_allocator(Arc::new(AtomicUsize::new(0)))
    }

    pub fn with_index_allocator(index_allocator: Arc<AtomicUsize>) -> Self {
        Self {
            index_allocator,
            seen: HashSet::new(),
            internal_ids: HashSet::new(),
            web_search_ids: HashSet::new(),
            streamed_text: false,
            text: String::new(),
            started: false,
        }
    }

    pub fn message_start(model: &str, id: &str) -> Value {
        json!({
            "type": "message_start",
            "message": {
                "id": id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": model,
                "stop_reason": Value::Null,
                "usage": { "input_tokens": 0, "output_tokens": 0 }
            }
        })
    }

    pub fn ingest(&mut self, frame: &Value) -> Vec<Value> {
        self.started = true;
        match frame.get("type").and_then(Value::as_str) {
            Some("assistant") => self.ingest_blocks(frame.pointer("/message/content")),
            Some("user") => self.ingest_user(frame.pointer("/message/content")),
            Some("stream_event") => self.ingest_stream_event(frame.get("event")),
            _ => Vec::new(),
        }
    }

    fn ingest_stream_event(&mut self, event: Option<&Value>) -> Vec<Value> {
        let Some(event) = event else {
            return Vec::new();
        };
        // Outer message envelope is owned by HTTP (we already sent message_start).
        match event.get("type").and_then(Value::as_str) {
            Some("message_start" | "message_stop" | "message_delta" | "ping") => Vec::new(),
            Some("content_block_delta") => {
                if event.pointer("/delta/type").and_then(Value::as_str) == Some("text_delta")
                    && let Some(text) = event.pointer("/delta/text").and_then(Value::as_str)
                    && !text.is_empty()
                {
                    self.text.push_str(text);
                }
                vec![event.clone()]
            }
            _ => vec![event.clone()],
        }
    }

    fn ingest_blocks(&mut self, content: Option<&Value>) -> Vec<Value> {
        let Some(Value::Array(blocks)) = content else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for block in blocks {
            out.extend(self.emit_complete_block(block));
        }
        out
    }

    fn ingest_user(&mut self, content: Option<&Value>) -> Vec<Value> {
        let Some(Value::Array(blocks)) = content else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for block in blocks {
            if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                continue;
            }
            let id = block
                .get("tool_use_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            if self.internal_ids.contains(id) {
                continue;
            }
            if !self.web_search_ids.contains(id) {
                continue;
            }
            let key = format!("result:{id}");
            if !self.seen.insert(key) {
                continue;
            }
            let mapped = json!({
                "type": "web_search_tool_result",
                "tool_use_id": id,
                "content": block.get("content").cloned().unwrap_or(Value::Null)
            });
            out.extend(self.emit_indexed(mapped));
        }
        out
    }

    pub fn emit_complete_block(&mut self, block: &Value) -> Vec<Value> {
        let kind = block.get("type").and_then(Value::as_str).unwrap_or("");
        let name = block.get("name").and_then(Value::as_str).unwrap_or("");
        if is_internal(name) {
            if let Some(id) = block.get("id").and_then(Value::as_str) {
                self.internal_ids.insert(id.to_string());
            }
            return Vec::new();
        }
        let fp = fingerprint(block);
        if !self.seen.insert(fp) {
            return Vec::new();
        }
        if kind == "tool_use" && is_web_search(name) {
            if let Some(id) = block.get("id").and_then(Value::as_str) {
                self.web_search_ids.insert(id.to_string());
            }
            let mapped = json!({
                "type": "server_tool_use",
                "id": block.get("id"),
                "name": "web_search",
                "input": block.get("input").cloned().unwrap_or(json!({}))
            });
            return self.emit_indexed(mapped);
        }
        match kind {
            "text" => {
                let text = block.get("text").and_then(Value::as_str).unwrap_or("");
                if text.is_empty() {
                    return Vec::new();
                }
                // streamed_text is set by the runtime once these events are
                // actually delivered; marking here counted suppressed/deferred
                // frames as sent and muted the kin_done fallback (empty 200s).
                self.text.push_str(text);
                self.emit_text_block(text, block.get("citations"))
            }
            "thinking" => {
                let thinking = block.get("thinking").and_then(Value::as_str).unwrap_or("");
                self.emit_thinking_block(thinking, block.get("signature").cloned())
            }
            "server_tool_use" | "web_search_tool_result" | "redacted_thinking" | "citations" => {
                self.emit_indexed(block.clone())
            }
            _ => self.emit_indexed(block.clone()),
        }
    }

    fn emit_text_block(&mut self, text: &str, citations: Option<&Value>) -> Vec<Value> {
        let index = self.next();
        let mut out = vec![
            json!({
                "type": "content_block_start",
                "index": index,
                "content_block": { "type": "text", "text": "" }
            }),
            json!({
                "type": "content_block_delta",
                "index": index,
                "delta": { "type": "text_delta", "text": text }
            }),
        ];
        if let Some(Value::Array(items)) = citations {
            for citation in items {
                out.push(json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": { "type": "citations_delta", "citation": citation }
                }));
            }
        }
        out.push(json!({ "type": "content_block_stop", "index": index }));
        out
    }

    fn emit_thinking_block(&mut self, thinking: &str, signature: Option<Value>) -> Vec<Value> {
        let index = self.next();
        let mut start = json!({
            "type": "content_block_start",
            "index": index,
            "content_block": { "type": "thinking", "thinking": "" }
        });
        if let Some(sig) = signature {
            start["content_block"]["signature"] = sig;
        }
        vec![
            start,
            json!({
                "type": "content_block_delta",
                "index": index,
                "delta": { "type": "thinking_delta", "thinking": thinking }
            }),
            json!({ "type": "content_block_stop", "index": index }),
        ]
    }

    fn emit_indexed(&mut self, block: Value) -> Vec<Value> {
        let index = self.next();
        vec![
            json!({
                "type": "content_block_start",
                "index": index,
                "content_block": block
            }),
            json!({ "type": "content_block_stop", "index": index }),
        ]
    }

    pub fn fallback_text(&mut self, text: &str) -> Vec<Value> {
        if self.streamed_text || text.is_empty() {
            return Vec::new();
        }
        self.streamed_text = true;
        self.text.push_str(text);
        self.emit_text_block(text, None)
    }

    pub fn finish(&self, stop_reason: &str, usage: Value) -> Vec<Value> {
        vec![
            json!({
                "type": "message_delta",
                "delta": { "stop_reason": stop_reason, "stop_sequence": Value::Null },
                "usage": usage
            }),
            json!({ "type": "message_stop" }),
        ]
    }

    fn next(&mut self) -> u64 {
        self.index_allocator.fetch_add(1, Ordering::AcqRel) as u64
    }

    pub fn adopt_tap_event(&mut self, event: Value) -> Value {
        let mut event = event;
        // Internal marker for kin_done-synthesized events; never client-visible.
        if let Value::Object(map) = &mut event {
            map.remove(super::relay::sse_tap::KIN_SYNTH_MARKER);
        }
        if let Some(block) = event.get("content_block") {
            let _ = self.seen.insert(fingerprint(block));
        }
        if event.pointer("/delta/type").and_then(Value::as_str) == Some("text_delta") {
            self.streamed_text = true;
        }
        event
    }
}

fn is_internal(name: &str) -> bool {
    name == "Agent"
        || name == "kin-slot"
        || name.starts_with("mcp__kin_runtime__")
        || (name.ends_with("Agent") && name.contains("kin"))
}

fn is_web_search(name: &str) -> bool {
    name.eq_ignore_ascii_case("websearch") || name.eq_ignore_ascii_case("web_search")
}

fn fingerprint(block: &Value) -> String {
    if let Some(id) = block
        .get("id")
        .or_else(|| block.get("tool_use_id"))
        .and_then(Value::as_str)
    {
        return id.to_string();
    }
    format!(
        "{}:{}:{}",
        block.get("type").and_then(Value::as_str).unwrap_or("-"),
        block.get("name").and_then(Value::as_str).unwrap_or("-"),
        block
            .get("text")
            .or_else(|| block.get("thinking"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .chars()
            .take(48)
            .collect::<String>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_web_search_and_result_without_fake_chunks() {
        let mut stream = JobStream::new();
        let assistant = json!({
            "type": "assistant",
            "parent_tool_use_id": "parent_1",
            "message": {"content": [
                {"type":"tool_use","id":"toolu_ws","name":"WebSearch","input":{"query":"iphone"}},
                {"type":"text","text":"full answer"}
            ]}
        });
        let events = stream.ingest(&assistant);
        let types: Vec<_> = events
            .iter()
            .map(|e| e.get("type").and_then(Value::as_str).unwrap())
            .collect();
        assert!(types.contains(&"content_block_start"));
        let starts: Vec<_> = events
            .iter()
            .filter(|e| e.get("type").and_then(Value::as_str) == Some("content_block_start"))
            .map(|e| e["content_block"]["type"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            starts,
            vec!["server_tool_use".to_string(), "text".to_string()]
        );
        let deltas: Vec<_> = events
            .iter()
            .filter(|e| e.pointer("/delta/type").and_then(Value::as_str) == Some("text_delta"))
            .collect();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0]["delta"]["text"], "full answer");

        let user = json!({
            "type": "user",
            "parent_tool_use_id": "parent_1",
            "message": {"content": [
                {"type":"tool_result","tool_use_id":"toolu_ws","content":"Web search results for query"}
            ]}
        });
        let result_events = stream.ingest(&user);
        assert_eq!(
            result_events[0]["content_block"]["type"],
            "web_search_tool_result"
        );
        assert_eq!(result_events[0]["index"], 2);
    }

    #[test]
    fn skips_internal_mcp_tools() {
        let mut stream = JobStream::new();
        let events = stream.ingest(&json!({
            "type": "assistant",
            "message": {"content": [
                {"type":"tool_use","id":"t1","name":"mcp__kin_runtime__slot_wait","input":{}},
                {"type":"tool_use","id":"t2","name":"mcp__kin_runtime__kin_done","input":{"text":"secret"}}
            ]}
        }));
        assert!(events.is_empty());
    }

    #[test]
    fn fallback_only_when_stdout_silent() {
        let mut stream = JobStream::new();
        assert_eq!(stream.fallback_text("hello").len(), 3);
        assert!(stream.fallback_text("again").is_empty());
    }

    #[test]
    fn stdout_then_kin_done_does_not_duplicate() {
        let mut stream = JobStream::new();
        let events = stream.ingest(&json!({
            "type": "assistant",
            "message": {"content": [{"type":"text","text":"from stdout"}]}
        }));
        assert_eq!(events.len(), 3);
        // The runtime marks streamed_text once these events are delivered.
        stream.streamed_text = true;
        assert!(stream.fallback_text("from kin_done").is_empty());
        assert_eq!(stream.text, "from stdout");
    }

    #[test]
    fn stream_event_skips_subagent_envelope() {
        let mut stream = JobStream::new();
        let skipped = stream.ingest(&json!({
            "type": "stream_event",
            "event": { "type": "message_start", "message": { "id": "inner" } }
        }));
        assert!(skipped.is_empty());
        let deltas = stream.ingest(&json!({
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "tok" }
            }
        }));
        assert_eq!(deltas.len(), 1);
        assert_eq!(stream.text, "tok");
    }

    #[test]
    fn text_citations_are_forwarded() {
        let mut stream = JobStream::new();
        let events = stream.ingest(&json!({
            "type": "assistant",
            "message": {"content": [{
                "type": "text",
                "text": "see source",
                "citations": [{"type":"web_search_result_location","url":"https://example.com"}]
            }]}
        }));
        let cites: Vec<_> = events
            .iter()
            .filter(|e| e.pointer("/delta/type").and_then(Value::as_str) == Some("citations_delta"))
            .collect();
        assert_eq!(cites.len(), 1);
        assert_eq!(cites[0]["delta"]["citation"]["url"], "https://example.com");
    }
}
