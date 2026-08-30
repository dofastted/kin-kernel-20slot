//! Anthropic SSE event filter for CLI-sourced streams.
//!
//! Swallows internal `mcp__kin_runtime__*` tool blocks (including kin_done —
//! its arguments are runtime bookkeeping, never a client body), remaps
//! content-block indexes onto a job-level allocator, and accumulates usage.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use serde_json::{Map, Value, json};

#[derive(Default)]
pub struct EventFilter {
    index_allocator: Arc<AtomicUsize>,
    index_map: HashMap<u64, u64>,
    swallowed: Vec<u64>,
    usage: Map<String, Value>,
    response_usage: Map<String, Value>,
    emitted_response_usage: Map<String, Value>,
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

    #[cfg(test)]
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
        let mut advanced = false;
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
                add_usage_value(&mut self.usage, key, value - emitted);
                advanced = true;
            }
        }
        if advanced {
            self.emitted_response_usage = self.response_usage.clone();
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
        assert_eq!(filter.usage(), json!({"input_tokens":3,"output_tokens":2}));
    }

    #[test]
    fn kin_done_block_is_swallowed_without_synthesis() {
        // kin_done's `text` argument is runtime bookkeeping: it must never be
        // re-streamed as a client-visible body.
        let mut filter = EventFilter::new(Arc::new(AtomicUsize::new(0)));
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
        first.apply(json!({"type":"message_delta","usage":{"output_tokens":2}}));
        // Cumulative upstream counters must not be double-counted.
        first.apply(json!({"type":"message_delta","usage":{"output_tokens":5}}));
        second.apply(json!({"type":"message_start","message":{"usage":{"input_tokens":6}}}));
        second.apply(json!({"type":"message_delta","usage":{"output_tokens":1}}));
        assert_eq!(first.usage(), json!({"input_tokens":4,"output_tokens":5}));
        assert_eq!(second.usage(), json!({"input_tokens":6,"output_tokens":1}));
    }
}
