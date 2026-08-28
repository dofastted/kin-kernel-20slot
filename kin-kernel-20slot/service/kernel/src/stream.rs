use serde_json::{Value, json};
use uuid::Uuid;

use crate::model::{ContentBlock, MessageRequest, MessageResponse, StopReason, Usage};

#[derive(Debug)]
pub enum StreamItem {
    Event(Value),
    Finished(MessageResponse),
}

#[derive(Debug)]
pub struct StreamAssembler {
    id: String,
    model: String,
    content: Vec<ContentBlock>,
    stop: StopReason,
    usage: Usage,
    tool_json: Vec<String>,
}

impl StreamAssembler {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            id: format!("msg_{}", Uuid::new_v4().simple()),
            model: model.into(),
            content: Vec::new(),
            stop: StopReason::EndTurn,
            usage: Usage::default(),
            tool_json: Vec::new(),
        }
    }

    pub fn apply_event(&mut self, event: &Value) {
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                if let Some(message) = event.get("message") {
                    if let Some(id) = message.get("id").and_then(Value::as_str) {
                        self.id = id.to_string();
                    }
                    if let Some(model) = message.get("model").and_then(Value::as_str) {
                        self.model = model.to_string();
                    }
                    if let Some(usage) = message.get("usage") {
                        self.apply_usage(usage);
                    }
                }
            }
            Some("content_block_start") => {
                let index = event.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                self.ensure_index(index);
                let block = event.get("content_block").cloned().unwrap_or(json!({}));
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        self.content[index] = ContentBlock::Text {
                            text: block
                                .get("text")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            cache_control: None,
                        };
                    }
                    Some("thinking") => {
                        self.content[index] = ContentBlock::Thinking {
                            thinking: block
                                .get("thinking")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            signature: block
                                .get("signature")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned),
                        };
                    }
                    Some("image") => {
                        self.content[index] = ContentBlock::Image {
                            source: block.get("source").cloned().unwrap_or(json!({})),
                        };
                    }
                    Some("tool_use") => {
                        self.content[index] = ContentBlock::ToolUse {
                            id: block
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            name: block
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            input: block.get("input").cloned().unwrap_or(json!({})),
                        };
                        if let Some(raw) = block.get("input") {
                            if raw.is_string() {
                                self.tool_json[index] = raw.as_str().unwrap_or("").to_string();
                            } else {
                                self.tool_json[index] = raw.to_string();
                            }
                        }
                    }
                    Some("server_tool_use") => {
                        self.content[index] = ContentBlock::ServerToolUse {
                            id: block
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            name: block
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            input: block.get("input").cloned().unwrap_or(json!({})),
                        };
                    }
                    Some("web_search_tool_result") => {
                        self.content[index] = ContentBlock::WebSearchToolResult {
                            tool_use_id: block
                                .get("tool_use_id")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            content: block.get("content").cloned().unwrap_or(Value::Null),
                        };
                    }
                    _ => {}
                }
            }
            Some("content_block_delta") => {
                let index = event.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                self.ensure_index(index);
                let delta = event.get("delta").cloned().unwrap_or(json!({}));
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => {
                        let piece = delta.get("text").and_then(Value::as_str).unwrap_or("");
                        match &mut self.content[index] {
                            ContentBlock::Text { text, .. } => text.push_str(piece),
                            _ => {
                                self.content[index] = ContentBlock::Text {
                                    text: piece.to_string(),
                                    cache_control: None,
                                };
                            }
                        }
                    }
                    Some("thinking_delta") => {
                        let piece = delta.get("thinking").and_then(Value::as_str).unwrap_or("");
                        match &mut self.content[index] {
                            ContentBlock::Thinking { thinking, .. } => thinking.push_str(piece),
                            _ => {
                                self.content[index] = ContentBlock::Thinking {
                                    thinking: piece.to_string(),
                                    signature: None,
                                };
                            }
                        }
                    }
                    Some("input_json_delta") => {
                        if let Some(piece) = delta.get("partial_json").and_then(Value::as_str) {
                            self.tool_json[index].push_str(piece);
                        }
                    }
                    _ => {}
                }
            }
            Some("content_block_stop") => {
                let index = event.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                if let Some(raw) = self.tool_json.get(index) {
                    if !raw.is_empty() {
                        if let Ok(parsed) = serde_json::from_str::<Value>(raw) {
                            if let ContentBlock::ToolUse { input, .. } =
                                &mut self.content[index]
                            {
                                *input = parsed;
                            }
                        }
                    }
                }
            }
            Some("message_delta") => {
                if let Some(usage) = event.get("usage") {
                    if let Some(tokens) = usage.get("output_tokens").and_then(Value::as_u64) {
                        self.usage.output_tokens = tokens;
                    }
                    if let Some(tokens) = usage.get("input_tokens").and_then(Value::as_u64) {
                        self.usage.input_tokens = tokens;
                    }
                }
                if let Some(reason) = event
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                {
                    self.stop = map_stop_reason(reason);
                }
            }
            _ => {}
        }
    }

    pub fn apply_assistant(&mut self, frame: &Value) {
        let mapped = map_assistant(frame);
        if !mapped.is_empty() {
            self.content = mapped;
        }
        if self
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolUse { .. }))
        {
            self.stop = StopReason::ToolUse;
        }
    }

    pub fn apply_result(&mut self, frame: &Value) {
        if let Some(tokens) = frame.get("usage") {
            self.apply_usage(tokens);
        }
        if self.content.is_empty() {
            if let Some(text) = frame.get("result").and_then(Value::as_str) {
                self.content.push(ContentBlock::Text {
                    text: text.to_string(),
                    cache_control: None,
                });
            }
        }
        if !matches!(self.stop, StopReason::ToolUse) {
            self.stop = StopReason::EndTurn;
        }
    }

    pub fn has_tool_use(&self) -> bool {
        self.content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolUse { .. }))
            || matches!(self.stop, StopReason::ToolUse)
    }

    pub fn finish(self, request: &MessageRequest) -> MessageResponse {
        MessageResponse {
            id: self.id,
            r#type: "message",
            role: "assistant",
            model: if self.model.is_empty() {
                request.model.clone()
            } else {
                self.model
            },
            content: self.content,
            stop_reason: self.stop,
            usage: self.usage,
        }
    }

    pub fn parts(self) -> (Vec<ContentBlock>, StopReason, Usage) {
        (self.content, self.stop, self.usage)
    }

    fn ensure_index(&mut self, index: usize) {
        while self.content.len() <= index {
            self.content.push(ContentBlock::Text {
                text: String::new(),
                cache_control: None,
            });
            self.tool_json.push(String::new());
        }
    }

    fn apply_usage(&mut self, usage: &Value) {
        if let Some(tokens) = usage.get("input_tokens").and_then(Value::as_u64) {
            self.usage.input_tokens = tokens;
        }
        if let Some(tokens) = usage.get("output_tokens").and_then(Value::as_u64) {
            self.usage.output_tokens = tokens;
        }
    }
}

pub fn parse_sse_block(block: &str) -> Option<Value> {
    let mut data = String::new();
    for line in block.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
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

pub fn map_assistant(frame: &Value) -> Vec<ContentBlock> {
    let Some(content) = frame
        .pointer("/message/content")
        .or_else(|| frame.get("content"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    content
        .iter()
        .filter_map(|block| match block.get("type").and_then(Value::as_str) {
            Some("text") => Some(ContentBlock::Text {
                text: block
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                cache_control: None,
            }),
            Some("thinking") => Some(ContentBlock::Thinking {
                thinking: block
                    .get("thinking")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                signature: block
                    .get("signature")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            }),
            Some("image") => Some(ContentBlock::Image {
                source: block.get("source").cloned().unwrap_or(json!({})),
            }),
            Some("tool_use") => Some(ContentBlock::ToolUse {
                id: block
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                name: block
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                input: block.get("input").cloned().unwrap_or(json!({})),
            }),
            Some("server_tool_use") => Some(ContentBlock::ServerToolUse {
                id: block
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                name: block
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                input: block.get("input").cloned().unwrap_or(json!({})),
            }),
            Some("web_search_tool_result") => Some(ContentBlock::WebSearchToolResult {
                tool_use_id: block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                content: block.get("content").cloned().unwrap_or(Value::Null),
            }),
            _ => None,
        })
        .collect()
}

pub fn map_stop_reason(value: &str) -> StopReason {
    match value {
        "end_turn" => StopReason::EndTurn,
        "tool_use" => StopReason::ToolUse,
        "max_tokens" => StopReason::MaxTokens,
        "stop_sequence" => StopReason::StopSequence,
        "pause_turn" => StopReason::PauseTurn,
        "refusal" => StopReason::Refusal,
        "model_context_window_exceeded" => StopReason::ModelContextWindowExceeded,
        _ => StopReason::Unknown,
    }
}

pub fn openai_chunk(id: &str, model: &str, delta: Value, finish: Option<&str>) -> Value {
    json!({
        "id": id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish
        }]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concatenates_text_deltas() {
        let mut assembler = StreamAssembler::new("claude-sonnet-5");
        assembler.apply_event(&json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": { "type": "text", "text": "" }
        }));
        assembler.apply_event(&json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": { "type": "text_delta", "text": "Hel" }
        }));
        assembler.apply_event(&json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": { "type": "text_delta", "text": "lo" }
        }));
        let (content, _, _) = assembler.parts();
        match &content[0] {
            ContentBlock::Text { text, .. } => assert_eq!(text, "Hello"),
            _ => panic!("expected text"),
        }
    }

    #[test]
    fn parses_sse_data_block() {
        let event = parse_sse_block("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"ab\"}}\n").unwrap();
        assert_eq!(event["type"], "content_block_delta");
        assert_eq!(event["delta"]["text"], "ab");
    }
}
